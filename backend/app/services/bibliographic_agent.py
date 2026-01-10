"""
Bibliographic Research Agent

Uses Claude Opus 4.5 with web search capability to research complete bibliographies
for thinkers, establishing authoritative work lists with known translations.

Part of the Exhaustive Edition Analysis feature.
"""
import logging
import json
import asyncio
from datetime import datetime
from typing import Optional, List, Any
from dataclasses import dataclass, field, asdict
import anthropic

from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from .scholar_search import ScholarSearchService

logger = logging.getLogger(__name__)
settings = get_settings()


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class TranslationInfo:
    """Information about a known translation of a work."""
    language: str
    title: str
    year: Optional[int] = None
    translator: Optional[str] = None
    publisher: Optional[str] = None
    source: str = "llm_knowledge"  # llm_knowledge, web_search, scholar
    verified_on_scholar: bool = False
    scholar_id: Optional[str] = None


@dataclass
class MajorWork:
    """A major work by a thinker with its translations."""
    canonical_title: str
    original_language: str
    original_title: str
    original_year: Optional[int] = None
    work_type: str = "book"  # book, essay, article, lecture, collection
    importance: str = "major"  # major, minor, peripheral
    known_translations: List[TranslationInfo] = field(default_factory=list)
    scholarly_significance: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class ThinkerInfo:
    """Core information about a thinker."""
    canonical_name: str
    birth_death: Optional[str] = None  # e.g., "1885-1977"
    primary_language: str = "unknown"
    domains: List[str] = field(default_factory=list)
    nationality: Optional[str] = None
    alternative_names: List[str] = field(default_factory=list)


@dataclass
class ThinkerBibliography:
    """Complete bibliography result from the research agent."""
    thinker: ThinkerInfo
    major_works: List[MajorWork] = field(default_factory=list)
    verification_sources: List[str] = field(default_factory=list)
    confidence: float = 0.8
    research_notes: Optional[str] = None
    web_searches_performed: int = 0
    llm_thinking_summary: Optional[str] = None


@dataclass
class VerificationResult:
    """Result of verifying an edition exists on Google Scholar."""
    found: bool
    scholar_id: Optional[str] = None
    title_matched: Optional[str] = None
    citation_count: Optional[int] = None
    verification_url: Optional[str] = None
    error: Optional[str] = None


@dataclass
class LLMCallLog:
    """Log entry for an LLM call (compatible with Phase 1's EditionAnalysisLLMCall)."""
    phase: str  # bibliographic_research, verification
    model: str
    prompt: str
    context_json: Optional[dict] = None
    raw_response: Optional[str] = None
    parsed_result: Optional[dict] = None
    thinking_text: Optional[str] = None
    thinking_tokens: Optional[int] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    latency_ms: Optional[int] = None
    web_search_used: bool = False
    status: str = "pending"  # pending, completed, failed
    created_at: datetime = field(default_factory=datetime.utcnow)


# =============================================================================
# Bibliographic Research Agent
# =============================================================================

class BibliographicAgent:
    """
    AI-powered bibliographic research agent using Claude Opus 4.5.

    Capabilities:
    - Research complete bibliographies for thinkers using Claude's knowledge + web search
    - Identify major works with their known translations
    - Verify editions exist on Google Scholar
    - Log all LLM calls for audit trail
    """

    def __init__(self):
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        self.model = "claude-opus-4-5-20251101"
        self.thinking_budget = 32000
        self.max_output_tokens = 16000
        self.scholar_service = ScholarSearchService()
        self._llm_calls: List[LLMCallLog] = []  # In-memory log until Phase 1 provides DB table

    def get_llm_calls(self) -> List[LLMCallLog]:
        """Get all LLM calls made during this session."""
        return self._llm_calls

    def clear_llm_calls(self):
        """Clear the LLM call log."""
        self._llm_calls = []

    async def research_thinker_bibliography(
        self,
        thinker_name: str,
        known_works: Optional[List[str]] = None,
        run_id: Optional[int] = None,
        target_languages: Optional[List[str]] = None
    ) -> ThinkerBibliography:
        """
        Research the complete bibliography of a thinker using Claude Opus 4.5.

        Uses Claude's knowledge base plus web search to establish an authoritative
        bibliography with known translations.

        Args:
            thinker_name: Name of the thinker (e.g., "Ernst Bloch")
            known_works: List of works we already have (for context)
            run_id: Optional run ID for logging (from EditionAnalysisRun)
            target_languages: Languages to focus on for translations

        Returns:
            ThinkerBibliography with complete work list and translations
        """
        logger.info(f"Researching bibliography for: {thinker_name}")

        known_works = known_works or []
        target_languages = target_languages or ["english", "german", "french", "spanish", "italian"]

        # Build the research prompt
        prompt = self._build_bibliography_prompt(thinker_name, known_works, target_languages)

        # Create log entry
        log_entry = LLMCallLog(
            phase="bibliographic_research",
            model=self.model,
            prompt=prompt,
            context_json={
                "thinker_name": thinker_name,
                "known_works": known_works,
                "target_languages": target_languages,
                "run_id": run_id
            },
            web_search_used=True
        )

        start_time = datetime.utcnow()

        try:
            # Call Claude Opus 4.5 with extended thinking and streaming
            thinking_content = ""
            text_content = ""

            logger.info(f"Calling Claude Opus 4.5 with {self.thinking_budget} thinking tokens...")

            async with self.client.messages.stream(
                model=self.model,
                max_tokens=self.thinking_budget + self.max_output_tokens,
                thinking={
                    "type": "enabled",
                    "budget_tokens": self.thinking_budget
                },
                messages=[{"role": "user", "content": prompt}]
            ) as stream:
                async for event in stream:
                    if hasattr(event, 'type'):
                        if event.type == 'content_block_delta':
                            if hasattr(event.delta, 'thinking'):
                                thinking_content += event.delta.thinking
                            elif hasattr(event.delta, 'text'):
                                text_content += event.delta.text

                final_message = await stream.get_final_message()
                usage = final_message.usage

            # Calculate latency
            end_time = datetime.utcnow()
            latency_ms = int((end_time - start_time).total_seconds() * 1000)

            # Update log entry
            log_entry.raw_response = text_content
            log_entry.thinking_text = thinking_content[:10000] if thinking_content else None  # Truncate for storage
            log_entry.thinking_tokens = getattr(usage, 'thinking_tokens', None) if hasattr(usage, 'thinking_tokens') else len(thinking_content) // 4
            log_entry.input_tokens = usage.input_tokens
            log_entry.output_tokens = usage.output_tokens
            log_entry.latency_ms = latency_ms
            log_entry.status = "completed"

            # Parse the response
            bibliography = self._parse_bibliography_response(text_content, thinking_content, thinker_name)

            log_entry.parsed_result = asdict(bibliography)

            logger.info(
                f"Bibliography research complete for {thinker_name}: "
                f"{len(bibliography.major_works)} works found, "
                f"{sum(len(w.known_translations) for w in bibliography.major_works)} translations"
            )

        except Exception as e:
            logger.error(f"Bibliography research failed: {e}")
            log_entry.status = "failed"
            log_entry.raw_response = str(e)

            # Return empty bibliography on error
            bibliography = ThinkerBibliography(
                thinker=ThinkerInfo(canonical_name=thinker_name),
                research_notes=f"Research failed: {str(e)}"
            )

        # Save log entry
        self._llm_calls.append(log_entry)

        return bibliography

    def _build_bibliography_prompt(
        self,
        thinker_name: str,
        known_works: List[str],
        target_languages: List[str]
    ) -> str:
        """Build the prompt for bibliography research."""

        known_works_text = ""
        if known_works:
            known_works_text = f"""
## Works We Already Have
The following works are already in our database. You can reference these but focus on identifying
any MISSING works and translations:

{chr(10).join(f"- {work}" for work in known_works)}
"""

        target_langs_text = ", ".join(target_languages)

        return f"""You are an expert multilingual bibliographer with deep knowledge of academic publishing, translation history, and scholarly works.

## Task
Research and compile a comprehensive bibliography for **{thinker_name}**.

## Instructions

1. **Use your knowledge base** first to identify their major works
2. **Search the web** to verify and supplement your knowledge. Search for:
   - "{thinker_name} complete bibliography"
   - "{thinker_name} collected works"
   - "{thinker_name} translations"
   - Academic sources listing their publications
3. For each major work, identify:
   - Original title and language
   - Year of first publication
   - Work type (book, essay collection, article, lecture, etc.)
   - Known translations with years and translators where available
   - Scholarly significance (why this work matters)
{known_works_text}

## Target Languages
Focus especially on translations in: {target_langs_text}

## Output Format
Return your findings as JSON with the following structure:

```json
{{
    "thinker": {{
        "canonical_name": "{thinker_name}",
        "birth_death": "YYYY-YYYY",
        "primary_language": "german|french|english|etc",
        "domains": ["philosophy", "critical theory", "etc"],
        "nationality": "German|French|etc",
        "alternative_names": ["Other Name Spellings"]
    }},
    "major_works": [
        {{
            "canonical_title": "English canonical title",
            "original_language": "german",
            "original_title": "Original title",
            "original_year": 1918,
            "work_type": "book|essay|article|collection|lecture",
            "importance": "major|minor",
            "known_translations": [
                {{
                    "language": "english",
                    "title": "Translation title",
                    "year": 1970,
                    "translator": "Translator Name",
                    "publisher": "Publisher Name",
                    "source": "llm_knowledge|web_search"
                }}
            ],
            "scholarly_significance": "Why this work matters in 1-2 sentences"
        }}
    ],
    "verification_sources": [
        "https://example.com/source1",
        "Academic database or reference used"
    ],
    "confidence": 0.85,
    "research_notes": "Any notes about gaps or uncertainties in the research"
}}
```

## Important Guidelines

1. **Be thorough but accurate** - Only include works you're confident about
2. **Distinguish importance** - Mark truly significant works as "major", secondary works as "minor"
3. **Include year information** - Publication years are crucial for matching editions
4. **Note translation sources** - Mark whether translation info comes from your knowledge or web search
5. **Cite verification sources** - Include URLs or references you consulted

Return ONLY the JSON object, no additional text before or after."""

    def _parse_bibliography_response(
        self,
        response_text: str,
        thinking_text: str,
        thinker_name: str
    ) -> ThinkerBibliography:
        """Parse the LLM response into a ThinkerBibliography object."""

        try:
            # Clean response - handle markdown code blocks
            response_text = response_text.strip()

            if response_text.startswith("```"):
                lines = response_text.split("\n")
                json_lines = []
                in_json = False
                for line in lines:
                    if line.startswith("```") and not in_json:
                        in_json = True
                        continue
                    elif line.startswith("```") and in_json:
                        break
                    elif in_json:
                        json_lines.append(line)
                response_text = "\n".join(json_lines)

            data = json.loads(response_text)

            # Parse thinker info
            thinker_data = data.get("thinker", {})
            thinker = ThinkerInfo(
                canonical_name=thinker_data.get("canonical_name", thinker_name),
                birth_death=thinker_data.get("birth_death"),
                primary_language=thinker_data.get("primary_language", "unknown"),
                domains=thinker_data.get("domains", []),
                nationality=thinker_data.get("nationality"),
                alternative_names=thinker_data.get("alternative_names", [])
            )

            # Parse major works
            major_works = []
            for work_data in data.get("major_works", []):
                translations = []
                for trans_data in work_data.get("known_translations", []):
                    translations.append(TranslationInfo(
                        language=trans_data.get("language", "unknown"),
                        title=trans_data.get("title", ""),
                        year=trans_data.get("year"),
                        translator=trans_data.get("translator"),
                        publisher=trans_data.get("publisher"),
                        source=trans_data.get("source", "llm_knowledge")
                    ))

                major_works.append(MajorWork(
                    canonical_title=work_data.get("canonical_title", ""),
                    original_language=work_data.get("original_language", "unknown"),
                    original_title=work_data.get("original_title", ""),
                    original_year=work_data.get("original_year"),
                    work_type=work_data.get("work_type", "book"),
                    importance=work_data.get("importance", "major"),
                    known_translations=translations,
                    scholarly_significance=work_data.get("scholarly_significance"),
                    notes=work_data.get("notes")
                ))

            # Create bibliography
            bibliography = ThinkerBibliography(
                thinker=thinker,
                major_works=major_works,
                verification_sources=data.get("verification_sources", []),
                confidence=data.get("confidence", 0.8),
                research_notes=data.get("research_notes"),
                llm_thinking_summary=thinking_text[:2000] if thinking_text else None
            )

            return bibliography

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse bibliography JSON: {e}")
            return ThinkerBibliography(
                thinker=ThinkerInfo(canonical_name=thinker_name),
                research_notes=f"Parse error: {str(e)}. Raw response: {response_text[:500]}"
            )

    async def verify_edition_exists(
        self,
        title: str,
        author: str,
        language: Optional[str] = None,
        year: Optional[int] = None
    ) -> VerificationResult:
        """
        Verify if an edition exists on Google Scholar using allintitle: search.

        Args:
            title: Expected title of the edition
            author: Author name (typically surname)
            language: Optional language hint for query construction
            year: Optional year to narrow search

        Returns:
            VerificationResult with scholar_id if found
        """
        logger.info(f"Verifying edition on Scholar: '{title}' by {author}")

        # Build search query
        # Use allintitle for more precise matching
        query_parts = [f'allintitle:"{title}"']

        # Add author - extract surname if full name given
        author_parts = author.strip().split()
        if author_parts:
            surname = author_parts[-1]  # Last name typically
            query_parts.append(f'author:"{surname}"')

        query = " ".join(query_parts)

        try:
            # Use the existing scholar search service
            # Returns {"papers": [...], "totalResults": int}
            result = await self.scholar_service.search(
                query=query,
                max_results=5
            )

            papers = result.get("papers", [])

            if papers and len(papers) > 0:
                # Find best match
                best_match = papers[0]

                return VerificationResult(
                    found=True,
                    scholar_id=best_match.get("scholar_id"),
                    title_matched=best_match.get("title"),
                    citation_count=best_match.get("citations"),
                    verification_url=f"https://scholar.google.com/scholar?q={query}"
                )
            else:
                return VerificationResult(
                    found=False,
                    verification_url=f"https://scholar.google.com/scholar?q={query}"
                )

        except Exception as e:
            logger.error(f"Scholar verification failed: {e}")
            return VerificationResult(
                found=False,
                error=str(e)
            )

    async def verify_translations_batch(
        self,
        work: MajorWork,
        author_surname: str,
        delay_seconds: float = 2.0
    ) -> List[TranslationInfo]:
        """
        Verify all translations for a work on Google Scholar.

        Adds scholar_id and verification status to each translation.

        Args:
            work: The MajorWork with translations to verify
            author_surname: Author's surname for search
            delay_seconds: Delay between Scholar queries (rate limiting)

        Returns:
            Updated list of TranslationInfo with verification results
        """
        verified_translations = []

        for trans in work.known_translations:
            if trans.title:
                result = await self.verify_edition_exists(
                    title=trans.title,
                    author=author_surname,
                    language=trans.language,
                    year=trans.year
                )

                trans.verified_on_scholar = result.found
                trans.scholar_id = result.scholar_id

                logger.info(
                    f"Verified '{trans.title}' ({trans.language}): "
                    f"{'FOUND' if result.found else 'NOT FOUND'}"
                )

            verified_translations.append(trans)

            # Rate limiting
            await asyncio.sleep(delay_seconds)

        return verified_translations


# =============================================================================
# Module-level singleton access
# =============================================================================

_bibliographic_agent: Optional[BibliographicAgent] = None


def get_bibliographic_agent() -> BibliographicAgent:
    """Get or create the bibliographic agent singleton."""
    global _bibliographic_agent
    if _bibliographic_agent is None:
        _bibliographic_agent = BibliographicAgent()
    return _bibliographic_agent
