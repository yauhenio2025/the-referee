"""
Thinker Bibliography Service

LLM-driven workflows for discovering and cataloging complete bibliographies of thinkers.

Workflows:
1. Disambiguation - Identify thinker from user input (e.g., "Marcuse" → Herbert Marcuse)
2. Variant Generation - Generate author search query variants
3. Page Filtering - Filter search results to identify works BY the thinker (not ABOUT)
4. Translation Detection - Group works into canonical editions + translations
5. Retrospective Matching - Match existing papers to thinkers
"""
import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

import anthropic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..models import Thinker, ThinkerWork, ThinkerHarvestRun, ThinkerLLMCall

logger = logging.getLogger(__name__)
settings = get_settings()


class ThinkerBibliographyService:
    """Service for managing thinker bibliography harvesting"""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        self.model_sonnet = "claude-sonnet-4-5-20250929"
        self.model_opus = "claude-opus-4-5-20251101"

    # ============== Workflow 1: Disambiguation ==============

    async def disambiguate_thinker(
        self,
        user_input: str,
        thinker_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Disambiguate a thinker from user input.

        Args:
            user_input: User's query like "Marcuse" or "Herbert Marcuse"
            thinker_id: Optional - if provided, associate LLM call with existing thinker

        Returns:
            Dict with disambiguation results
        """
        logger.info(f"[Thinker] Disambiguating: '{user_input}'")

        # Create LLM call record
        llm_call = ThinkerLLMCall(
            thinker_id=thinker_id,
            workflow="disambiguation",
            model=self.model_sonnet,
            prompt=user_input,
            status="running",
            started_at=datetime.utcnow(),
        )
        self.db.add(llm_call)
        await self.db.flush()

        prompt = f"""You are helping identify which specific thinker a user is referring to.

USER INPUT: "{user_input}"

YOUR TASK:
Determine who the user means. Consider:
1. Famous philosophers, theorists, scholars with this name
2. Potential ambiguity (e.g., "James" could be William James or Henry James)
3. Common misspellings or partial names

Return a JSON object with your analysis:

{{
  "is_ambiguous": false,
  "primary_candidate": {{
    "canonical_name": "Herbert Marcuse",
    "birth_death": "1898-1979",
    "bio": "German-American philosopher and sociologist, associated with the Frankfurt School of critical theory",
    "domains": ["critical theory", "Marxism", "Frankfurt School", "social philosophy"],
    "notable_works": ["One-Dimensional Man", "Eros and Civilization", "Reason and Revolution", "An Essay on Liberation"]
  }},
  "alternatives": [],
  "confidence": 0.95,
  "requires_confirmation": false,
  "reasoning": "Marcuse almost certainly refers to Herbert Marcuse, the prominent Frankfurt School philosopher. No other notable scholars with this surname."
}}

If AMBIGUOUS, include alternatives:
{{
  "is_ambiguous": true,
  "primary_candidate": {{ ... most likely candidate ... }},
  "alternatives": [
    {{ ... second candidate ... }},
    {{ ... third candidate ... }}
  ],
  "confidence": 0.6,
  "requires_confirmation": true,
  "reasoning": "The name 'James' could refer to William James (psychologist/pragmatist) or Henry James (literary critic)..."
}}

RULES:
- canonical_name should be the full scholarly name (e.g., "Herbert Marcuse" not "H. Marcuse")
- birth_death format: "YYYY-YYYY" or "YYYY-" for living thinkers
- domains: 3-6 key areas they're known for
- notable_works: 4-6 most cited/influential works
- confidence: 0.0-1.0 based on how certain you are
- requires_confirmation: true if user should verify before proceeding

ONLY return the JSON object, no other text."""

        start_time = datetime.utcnow()

        try:
            response = self.client.messages.create(
                model=self.model_sonnet,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
            llm_call.raw_response = text
            llm_call.input_tokens = response.usage.input_tokens
            llm_call.output_tokens = response.usage.output_tokens

            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                result = json.loads(json_match.group())
                llm_call.parsed_result = json.dumps(result)
                llm_call.status = "completed"
                llm_call.completed_at = datetime.utcnow()
                llm_call.latency_ms = int((llm_call.completed_at - start_time).total_seconds() * 1000)

                logger.info(f"[Thinker] Disambiguation result: {result.get('primary_candidate', {}).get('canonical_name', 'Unknown')}")
                logger.info(f"  Confidence: {result.get('confidence', 0)}, Ambiguous: {result.get('is_ambiguous', False)}")

                await self.db.commit()
                return {
                    "success": True,
                    "llm_call_id": llm_call.id,
                    **result
                }

        except json.JSONDecodeError as e:
            logger.error(f"[Thinker] JSON parse error: {e}")
            llm_call.status = "failed"
            llm_call.parsed_result = json.dumps({"error": f"JSON parse error: {str(e)}"})

        except Exception as e:
            logger.error(f"[Thinker] Disambiguation error: {e}")
            llm_call.status = "failed"
            llm_call.parsed_result = json.dumps({"error": str(e)})

        llm_call.completed_at = datetime.utcnow()
        llm_call.latency_ms = int((llm_call.completed_at - start_time).total_seconds() * 1000)
        await self.db.commit()

        # Fallback response
        return {
            "success": False,
            "llm_call_id": llm_call.id,
            "is_ambiguous": True,
            "primary_candidate": {
                "canonical_name": user_input,
                "birth_death": None,
                "bio": None,
                "domains": [],
                "notable_works": [],
            },
            "alternatives": [],
            "confidence": 0.0,
            "requires_confirmation": True,
            "reasoning": "LLM call failed - please verify thinker identity manually",
        }

    # ============== Workflow 2: Variant Generation ==============

    async def generate_name_variants(
        self,
        thinker: Thinker,
    ) -> Dict[str, Any]:
        """
        Generate search query variants for a thinker's name.

        Uses a simple, reliable programmatic approach:
        1. author:"X* LastName" - wildcard covers initials, middle names, full first names
        2. "Full Name" - catches title/content mentions (combined with variant 1)

        Args:
            thinker: The Thinker model instance

        Returns:
            Dict with generated variants
        """
        logger.info(f"[Thinker] Generating name variants for: {thinker.canonical_name}")

        # Parse the name into parts
        name_parts = thinker.canonical_name.split()

        if len(name_parts) >= 2:
            first_name = name_parts[0]
            last_name = name_parts[-1]
            first_initial = first_name[0]

            # Strategy: wildcard initial is more capacious than plain initial
            # author:"C* Durand" matches: C Durand, Cedric Durand, C J Durand, etc.
            variants = [
                {
                    "query": f'author:"{first_initial}* {last_name}"',
                    "type": "wildcard_initial",
                    "priority": 1,
                    "description": f"Wildcard search: matches {first_initial}, {first_name}, {first_initial} J, etc."
                },
                {
                    "query": f'"{thinker.canonical_name}"',
                    "type": "full_name",
                    "priority": 2,
                    "description": "Full name in quotes for title/content matching"
                },
            ]
        else:
            # Single name (like "Plato" or mononymous person)
            variants = [
                {
                    "query": f'author:"{thinker.canonical_name}"',
                    "type": "single_name",
                    "priority": 1,
                    "description": "Single name author search"
                },
            ]

        # Extract just the query strings for storage on thinker
        query_strings = [v["query"] for v in variants]

        # Update thinker with variants
        thinker.name_variants = json.dumps(query_strings)
        await self.db.commit()

        logger.info(f"[Thinker] Generated {len(variants)} name variants (programmatic)")
        for v in variants:
            logger.info(f"  - {v['query']} ({v['type']})")

        return {
            "success": True,
            "llm_call_id": None,  # No LLM call needed
            "thinker_id": thinker.id,
            "canonical_name": thinker.canonical_name,
            "variants": variants,
            "variant_count": len(variants),
        }

    # ============== Workflow 3: Page Filtering ==============

    async def filter_page_results(
        self,
        thinker: Thinker,
        papers: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Filter a page of search results to identify works BY the thinker.

        Args:
            thinker: The Thinker model instance
            papers: List of paper dicts from Scholar search (title, authors, year, etc.)

        Returns:
            Dict with filtering decisions per paper
        """
        if not papers:
            return {"success": True, "decisions": [], "accepted": 0, "rejected": 0}

        logger.info(f"[Thinker] Filtering {len(papers)} results for: {thinker.canonical_name}")

        # Create LLM call record
        llm_call = ThinkerLLMCall(
            thinker_id=thinker.id,
            workflow="page_filtering",
            model=self.model_sonnet,
            prompt=f"Filter {len(papers)} papers for {thinker.canonical_name}",
            status="running",
            started_at=datetime.utcnow(),
        )
        self.db.add(llm_call)
        await self.db.flush()

        # Parse domains
        domains = self._parse_json_list(thinker.domains)
        notable_works = self._parse_json_list(thinker.notable_works)

        # Format papers for prompt
        papers_text = ""
        for i, p in enumerate(papers):
            papers_text += f"""
PAPER {i+1}:
  Title: {p.get('title', 'Unknown')}
  Authors: {p.get('authors', 'Unknown')}
  Year: {p.get('year', 'Unknown')}
  Snippet: {p.get('snippet', '')[:200] if p.get('snippet') else 'N/A'}
  Scholar ID: {p.get('scholar_id', 'N/A')}
"""

        prompt = f"""You are filtering Google Scholar search results to identify works AUTHORED BY a specific thinker.

THINKER TO IDENTIFY:
- Name: {thinker.canonical_name}
- Life: {thinker.birth_death or 'Unknown'}
- Domains: {', '.join(domains) if domains else 'Not specified'}
- Notable works: {', '.join(notable_works[:5]) if notable_works else 'Not specified'}
- Bio: {thinker.bio or 'Not provided'}

SEARCH RESULTS TO FILTER:
{papers_text}

YOUR TASK:
For each paper, determine if this is a work AUTHORED BY {thinker.canonical_name}.

ACCEPT if:
- The author field contains {thinker.canonical_name} or a name variant
- The paper is in a domain this thinker works in
- It appears to be written BY them (not just ABOUT them or their ideas)

REJECT if:
- The paper is ABOUT the thinker (e.g., "Analysis of Marcuse's critique...")
- Different author with similar name (different first name, different field)
- The thinker is only cited/referenced, not the author
- Clearly wrong field (e.g., physics paper for a philosopher)

UNCERTAIN if:
- Can't determine authorship from available info
- Could be a translated edition or variant title
- Multiple authors and unclear if thinker is primary

Return a JSON array with one decision per paper (same order as input):
[
  {{
    "paper_index": 1,
    "decision": "accept",
    "confidence": 0.95,
    "reason": "Herbert Marcuse is listed as author, title matches known work"
  }},
  {{
    "paper_index": 2,
    "decision": "reject",
    "confidence": 0.90,
    "reason": "Paper is ABOUT Marcuse's philosophy, not BY him - author is J. Smith"
  }},
  {{
    "paper_index": 3,
    "decision": "uncertain",
    "confidence": 0.50,
    "reason": "Author listed as 'H. Marcuse' but appears to be chemistry paper"
  }}
]

RULES:
- decision: "accept", "reject", or "uncertain"
- confidence: 0.0-1.0
- reason: Brief explanation (one sentence)
- paper_index: 1-based index matching input order

ONLY return the JSON array, no other text."""

        start_time = datetime.utcnow()

        try:
            response = self.client.messages.create(
                model=self.model_sonnet,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
            llm_call.raw_response = text
            llm_call.input_tokens = response.usage.input_tokens
            llm_call.output_tokens = response.usage.output_tokens

            json_match = re.search(r"\[[\s\S]*\]", text)
            if json_match:
                decisions = json.loads(json_match.group())
                llm_call.parsed_result = json.dumps(decisions)
                llm_call.status = "completed"
                llm_call.completed_at = datetime.utcnow()
                llm_call.latency_ms = int((llm_call.completed_at - start_time).total_seconds() * 1000)

                # Count results
                accepted = sum(1 for d in decisions if d.get("decision") == "accept")
                rejected = sum(1 for d in decisions if d.get("decision") == "reject")
                uncertain = sum(1 for d in decisions if d.get("decision") == "uncertain")

                logger.info(f"[Thinker] Page filtering: {accepted} accepted, {rejected} rejected, {uncertain} uncertain")

                await self.db.commit()
                return {
                    "success": True,
                    "llm_call_id": llm_call.id,
                    "decisions": decisions,
                    "accepted": accepted,
                    "rejected": rejected,
                    "uncertain": uncertain,
                }

        except json.JSONDecodeError as e:
            logger.error(f"[Thinker] JSON parse error in page filtering: {e}")
            llm_call.status = "failed"
            llm_call.parsed_result = json.dumps({"error": f"JSON parse error: {str(e)}"})

        except Exception as e:
            logger.error(f"[Thinker] Page filtering error: {e}")
            llm_call.status = "failed"
            llm_call.parsed_result = json.dumps({"error": str(e)})

        llm_call.completed_at = datetime.utcnow()
        llm_call.latency_ms = int((llm_call.completed_at - start_time).total_seconds() * 1000)
        await self.db.commit()

        # Fallback: mark all as uncertain
        return {
            "success": False,
            "llm_call_id": llm_call.id,
            "decisions": [
                {"paper_index": i+1, "decision": "uncertain", "confidence": 0.0, "reason": "LLM filtering failed"}
                for i in range(len(papers))
            ],
            "accepted": 0,
            "rejected": 0,
            "uncertain": len(papers),
            "error": "LLM call failed - all papers marked as uncertain",
        }

    # ============== Workflow 4: Translation Detection ==============

    async def detect_translations(
        self,
        thinker: Thinker,
        works: Optional[List[ThinkerWork]] = None,
    ) -> Dict[str, Any]:
        """
        Detect translations and group works into canonical editions.

        Uses Claude Opus with extended thinking (32k budget) for complex analysis.

        Args:
            thinker: The Thinker model instance
            works: Optional list of works to analyze (defaults to all accepted works)

        Returns:
            Dict with work groups (canonical + translations)
        """
        # Load works if not provided
        if works is None:
            result = await self.db.execute(
                select(ThinkerWork)
                .where(ThinkerWork.thinker_id == thinker.id)
                .where(ThinkerWork.decision == "accepted")
                .order_by(ThinkerWork.year, ThinkerWork.title)
            )
            works = list(result.scalars().all())

        if not works:
            return {"success": True, "work_groups": [], "message": "No accepted works to analyze"}

        logger.info(f"[Thinker] Detecting translations among {len(works)} works for: {thinker.canonical_name}")

        # Create LLM call record
        llm_call = ThinkerLLMCall(
            thinker_id=thinker.id,
            workflow="translation_detection",
            model=self.model_opus,
            prompt=f"Detect translations among {len(works)} works for {thinker.canonical_name}",
            status="running",
            started_at=datetime.utcnow(),
        )
        self.db.add(llm_call)
        await self.db.flush()

        # Format works for prompt
        works_text = ""
        for w in works:
            works_text += f"""
WORK ID {w.id}:
  Title: {w.title}
  Year: {w.year or 'Unknown'}
  Authors: {w.authors_raw or 'Unknown'}
  Citations: {w.citation_count}
  Language: {w.original_language or 'Unknown'}
"""

        prompt = f"""You are a scholarly expert analyzing the complete bibliography of a major thinker to identify translations and group related works.

THINKER: {thinker.canonical_name}
BIO: {thinker.bio or 'Not provided'}
DOMAINS: {', '.join(self._parse_json_list(thinker.domains)) or 'Not specified'}

WORKS TO ANALYZE ({len(works)} total):
{works_text}

YOUR TASK:
Group these works into "work groups" where each group represents a single intellectual work in all its editions and translations.

EXAMPLE:
If you see:
- "One-Dimensional Man" (1964, English)
- "Der eindimensionale Mensch" (1967, German)
- "L'homme unidimensionnel" (1968, French)
- "El hombre unidimensional" (1969, Spanish)

These are ALL the same work. Group them together with the original as "canonical".

WORK GROUP STRUCTURE:
{{
  "canonical_work_id": 123,  // Work ID of the original/primary edition
  "canonical_title": "One-Dimensional Man",
  "original_language": "english",
  "original_year": 1964,
  "translations": [
    {{
      "work_id": 124,
      "title": "Der eindimensionale Mensch",
      "language": "german",
      "year": 1967
    }},
    ...
  ],
  "confidence": 0.95,
  "reasoning": "All titles are translations of 'One-Dimensional Man', published after 1964 original"
}}

RULES FOR GROUPING:
1. Same work = same intellectual content, just different language/edition
2. Canonical = original publication (usually earliest, in author's primary language)
3. For {thinker.canonical_name}, primary language is likely: {self._guess_primary_language(thinker)}
4. Similar titles in different languages → likely translations
5. Republications/new editions in SAME language are NOT translations (still group them)
6. Edited volumes, anthologies, collected works → usually separate works
7. Articles vs books → usually different works even if similar title

Return a JSON object:
{{
  "work_groups": [
    {{
      "canonical_work_id": 123,
      "canonical_title": "One-Dimensional Man",
      "original_language": "english",
      "original_year": 1964,
      "translations": [...],
      "same_language_editions": [...],
      "confidence": 0.95,
      "reasoning": "..."
    }},
    ...
  ],
  "standalone_works": [456, 789],  // Work IDs that don't belong to any group
  "analysis_notes": "Brief notes about patterns observed, ambiguous cases, etc."
}}

Think carefully about linguistic relationships between titles. Use your knowledge of {thinker.canonical_name}'s bibliography.

ONLY return the JSON object, no other text."""

        start_time = datetime.utcnow()

        try:
            # Use streaming for extended thinking (per CLAUDE.md)
            thinking_text = ""
            response_text = ""
            input_tokens = 0
            output_tokens = 0
            thinking_tokens = 0

            with self.client.messages.stream(
                model=self.model_opus,
                max_tokens=16000,
                thinking={
                    "type": "enabled",
                    "budget_tokens": 32000,
                },
                messages=[{"role": "user", "content": prompt}]
            ) as stream:
                for event in stream:
                    if hasattr(event, 'type'):
                        if event.type == 'content_block_delta':
                            if hasattr(event.delta, 'thinking'):
                                thinking_text += event.delta.thinking
                            elif hasattr(event.delta, 'text'):
                                response_text += event.delta.text

                # Get final message for usage stats
                final_message = stream.get_final_message()
                input_tokens = final_message.usage.input_tokens
                output_tokens = final_message.usage.output_tokens
                if hasattr(final_message.usage, 'thinking_tokens'):
                    thinking_tokens = final_message.usage.thinking_tokens

            llm_call.raw_response = response_text
            llm_call.thinking_text = thinking_text if thinking_text else None
            llm_call.thinking_tokens = thinking_tokens if thinking_tokens else None
            llm_call.input_tokens = input_tokens
            llm_call.output_tokens = output_tokens

            json_match = re.search(r"\{[\s\S]*\}", response_text)
            if json_match:
                result = json.loads(json_match.group())
                llm_call.parsed_result = json.dumps(result)
                llm_call.status = "completed"
                llm_call.completed_at = datetime.utcnow()
                llm_call.latency_ms = int((llm_call.completed_at - start_time).total_seconds() * 1000)

                work_groups = result.get("work_groups", [])
                standalone = result.get("standalone_works", [])

                logger.info(f"[Thinker] Translation detection: {len(work_groups)} groups, {len(standalone)} standalone")
                if thinking_tokens:
                    logger.info(f"[Thinker] Extended thinking used {thinking_tokens} tokens")

                # Update ThinkerWork records with translation info
                for group in work_groups:
                    canonical_id = group.get("canonical_work_id")
                    original_lang = group.get("original_language")

                    # Mark translations
                    for trans in group.get("translations", []):
                        work_id = trans.get("work_id")
                        if work_id:
                            work = await self.db.get(ThinkerWork, work_id)
                            if work:
                                work.is_translation = True
                                work.canonical_work_id = canonical_id
                                work.original_language = trans.get("language")

                    # Mark same-language editions
                    for edition in group.get("same_language_editions", []):
                        work_id = edition.get("work_id")
                        if work_id and work_id != canonical_id:
                            work = await self.db.get(ThinkerWork, work_id)
                            if work:
                                work.is_translation = False  # Same language, different edition
                                work.canonical_work_id = canonical_id
                                work.original_language = original_lang

                    # Mark canonical work
                    if canonical_id:
                        canonical_work = await self.db.get(ThinkerWork, canonical_id)
                        if canonical_work:
                            canonical_work.is_translation = False
                            canonical_work.canonical_work_id = None  # It IS the canonical
                            canonical_work.original_language = original_lang

                await self.db.commit()

                return {
                    "success": True,
                    "llm_call_id": llm_call.id,
                    "work_groups": work_groups,
                    "standalone_works": standalone,
                    "analysis_notes": result.get("analysis_notes", ""),
                    "thinking_tokens": thinking_tokens,
                }

        except json.JSONDecodeError as e:
            logger.error(f"[Thinker] JSON parse error in translation detection: {e}")
            llm_call.status = "failed"
            llm_call.parsed_result = json.dumps({"error": f"JSON parse error: {str(e)}"})

        except Exception as e:
            logger.error(f"[Thinker] Translation detection error: {e}")
            llm_call.status = "failed"
            llm_call.parsed_result = json.dumps({"error": str(e)})

        llm_call.completed_at = datetime.utcnow()
        llm_call.latency_ms = int((llm_call.completed_at - start_time).total_seconds() * 1000)
        await self.db.commit()

        return {
            "success": False,
            "llm_call_id": llm_call.id,
            "work_groups": [],
            "standalone_works": [w.id for w in works],
            "error": "Translation detection failed",
        }

    def _guess_primary_language(self, thinker: Thinker) -> str:
        """Guess the thinker's primary writing language from bio/domains"""
        bio = (thinker.bio or "").lower()
        name = thinker.canonical_name.lower()

        if "german" in bio or "frankfurt" in bio or "berlin" in bio:
            return "German"
        elif "french" in bio or "paris" in bio:
            return "French"
        elif "american" in bio or "usa" in bio or "united states" in bio:
            return "English"
        elif "british" in bio or "oxford" in bio or "cambridge" in bio:
            return "English"
        elif "japanese" in bio or "tokyo" in bio:
            return "Japanese"
        elif "chinese" in bio or "beijing" in bio:
            return "Chinese"
        else:
            return "likely German or English based on common thinker origins"

    # ============== Workflow 5: Retrospective Matching ==============

    async def retrospective_match(
        self,
        thinker_ids: Optional[List[int]] = None,
        paper_ids: Optional[List[int]] = None,
        batch_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Match existing papers to thinkers (retrospective assignment).

        Analyzes existing papers in the database and determines which ones
        were authored by known thinkers.

        Args:
            thinker_ids: List of thinker IDs to match (defaults to all)
            paper_ids: List of paper IDs to analyze (defaults to unassigned papers)
            batch_size: Number of papers per LLM call

        Returns:
            Dict with matching results
        """
        from ..models import Paper

        # Load thinkers
        if thinker_ids:
            result = await self.db.execute(
                select(Thinker).where(Thinker.id.in_(thinker_ids))
            )
        else:
            result = await self.db.execute(select(Thinker))
        thinkers = list(result.scalars().all())

        if not thinkers:
            return {"success": False, "error": "No thinkers to match against"}

        # Load papers (those not already linked to a thinker)
        if paper_ids:
            result = await self.db.execute(
                select(Paper).where(Paper.id.in_(paper_ids))
            )
        else:
            # Get papers not already in thinker_works
            subquery = select(ThinkerWork.paper_id).where(ThinkerWork.paper_id.isnot(None))
            result = await self.db.execute(
                select(Paper)
                .where(Paper.id.notin_(subquery))
                .where(Paper.deleted_at.is_(None))
                .limit(500)  # Safety limit
            )
        papers = list(result.scalars().all())

        if not papers:
            return {"success": True, "matches": [], "message": "No papers to analyze"}

        logger.info(f"[Thinker] Retrospective matching: {len(papers)} papers against {len(thinkers)} thinkers")

        # Build thinker context for prompt
        thinkers_text = ""
        for t in thinkers:
            domains = self._parse_json_list(t.domains)
            thinkers_text += f"""
THINKER {t.id}: {t.canonical_name}
  Life: {t.birth_death or 'Unknown'}
  Domains: {', '.join(domains[:5]) if domains else 'Not specified'}
"""

        all_matches = []
        total_matched = 0

        # Process in batches
        for batch_start in range(0, len(papers), batch_size):
            batch = papers[batch_start:batch_start + batch_size]

            # Create LLM call record
            llm_call = ThinkerLLMCall(
                thinker_id=thinkers[0].id if len(thinkers) == 1 else None,
                workflow="retrospective_matching",
                model=self.model_sonnet,
                prompt=f"Match {len(batch)} papers to {len(thinkers)} thinkers",
                status="running",
                started_at=datetime.utcnow(),
            )
            self.db.add(llm_call)
            await self.db.flush()

            # Format papers for prompt
            papers_text = ""
            for p in batch:
                papers_text += f"""
PAPER {p.id}:
  Title: {p.title or 'Unknown'}
  Authors: {p.primary_author or 'Unknown'}
  Year: {p.year or 'Unknown'}
"""

            prompt = f"""You are matching papers to their authors from a list of known thinkers.

KNOWN THINKERS:
{thinkers_text}

PAPERS TO MATCH:
{papers_text}

YOUR TASK:
For each paper, determine if any of the known thinkers is the PRIMARY author.

Return a JSON array of MATCHES ONLY (papers that match a thinker):
[
  {{
    "paper_id": 123,
    "thinker_id": 1,
    "confidence": 0.95,
    "reason": "Herbert Marcuse is listed as primary author"
  }},
  ...
]

RULES:
- Only include papers where you're confident a known thinker is the PRIMARY author
- Papers ABOUT a thinker should NOT be matched
- Minimum confidence threshold: 0.7
- If no papers match any thinker, return empty array: []

ONLY return the JSON array, no other text."""

            start_time = datetime.utcnow()

            try:
                response = self.client.messages.create(
                    model=self.model_sonnet,
                    max_tokens=4096,
                    messages=[{"role": "user", "content": prompt}]
                )

                text = response.content[0].text
                llm_call.raw_response = text
                llm_call.input_tokens = response.usage.input_tokens
                llm_call.output_tokens = response.usage.output_tokens

                json_match = re.search(r"\[[\s\S]*\]", text)
                if json_match:
                    batch_matches = json.loads(json_match.group())
                    llm_call.parsed_result = json.dumps(batch_matches)
                    llm_call.status = "completed"

                    # Create ThinkerWork entries for matches
                    for match in batch_matches:
                        paper_id = match.get("paper_id")
                        thinker_id_match = match.get("thinker_id")
                        confidence = match.get("confidence", 0.8)

                        # Get the paper
                        paper = await self.db.get(Paper, paper_id)
                        if paper:
                            # Check if already linked
                            existing = await self.db.execute(
                                select(ThinkerWork)
                                .where(ThinkerWork.thinker_id == thinker_id_match)
                                .where(ThinkerWork.paper_id == paper_id)
                            )
                            if not existing.scalars().first():
                                work = ThinkerWork(
                                    thinker_id=thinker_id_match,
                                    paper_id=paper_id,
                                    title=paper.title or "Unknown",
                                    authors_raw=paper.primary_author,
                                    year=paper.year,
                                    decision="accepted",
                                    confidence=confidence,
                                    reason=match.get("reason", "Retrospective match"),
                                    created_at=datetime.utcnow(),
                                )
                                self.db.add(work)
                                total_matched += 1

                    all_matches.extend(batch_matches)
                else:
                    llm_call.status = "completed"
                    llm_call.parsed_result = json.dumps([])

            except Exception as e:
                logger.error(f"[Thinker] Retrospective matching batch error: {e}")
                llm_call.status = "failed"
                llm_call.parsed_result = json.dumps({"error": str(e)})

            llm_call.completed_at = datetime.utcnow()
            llm_call.latency_ms = int((llm_call.completed_at - start_time).total_seconds() * 1000)

        await self.db.commit()

        logger.info(f"[Thinker] Retrospective matching complete: {total_matched} matches created")

        return {
            "success": True,
            "matches": all_matches,
            "total_papers_analyzed": len(papers),
            "total_matches": total_matched,
            "thinkers_checked": len(thinkers),
        }

    # ============== CRUD Operations ==============

    async def create_thinker(
        self,
        user_input: str,
        auto_disambiguate: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a new thinker, optionally with disambiguation.

        Args:
            user_input: User's query like "Marcuse"
            auto_disambiguate: If True, run disambiguation workflow

        Returns:
            Dict with created thinker and disambiguation results
        """
        # Check for existing thinker with similar name
        result = await self.db.execute(
            select(Thinker).where(
                Thinker.canonical_name.ilike(f"%{user_input}%")
            )
        )
        existing = result.scalars().first()
        if existing:
            return {
                "success": False,
                "error": f"Thinker '{existing.canonical_name}' already exists",
                "existing_thinker_id": existing.id,
            }

        # Run disambiguation first (without thinker_id)
        disambiguation = None
        if auto_disambiguate:
            disambiguation = await self.disambiguate_thinker(user_input)

        # Determine canonical name and info
        if disambiguation and disambiguation.get("success"):
            primary = disambiguation.get("primary_candidate", {})
            canonical_name = primary.get("canonical_name", user_input)
            birth_death = primary.get("birth_death")
            bio = primary.get("bio")
            domains = primary.get("domains", [])
            notable_works = primary.get("notable_works", [])
        else:
            canonical_name = user_input
            birth_death = None
            bio = None
            domains = []
            notable_works = []

        # Create thinker
        thinker = Thinker(
            canonical_name=canonical_name,
            birth_death=birth_death,
            bio=bio,
            domains=json.dumps(domains) if domains else None,
            notable_works=json.dumps(notable_works) if notable_works else None,
            status="pending" if disambiguation and disambiguation.get("requires_confirmation") else "disambiguated",
            created_at=datetime.utcnow(),
            disambiguated_at=datetime.utcnow() if disambiguation and not disambiguation.get("requires_confirmation") else None,
        )
        self.db.add(thinker)
        await self.db.flush()

        # Update LLM call with thinker_id if we ran disambiguation
        if disambiguation and disambiguation.get("llm_call_id"):
            llm_call = await self.db.get(ThinkerLLMCall, disambiguation["llm_call_id"])
            if llm_call:
                llm_call.thinker_id = thinker.id

        await self.db.commit()

        logger.info(f"[Thinker] Created thinker: {thinker.canonical_name} (id={thinker.id})")

        return {
            "success": True,
            "thinker_id": thinker.id,
            "canonical_name": thinker.canonical_name,
            "status": thinker.status,
            "disambiguation": disambiguation,
            "requires_confirmation": disambiguation.get("requires_confirmation", False) if disambiguation else False,
        }

    async def confirm_disambiguation(
        self,
        thinker_id: int,
        candidate_index: int = 0,
        custom_domains: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Confirm disambiguation choice for a thinker.

        Args:
            thinker_id: The thinker to confirm
            candidate_index: 0 for primary, 1+ for alternatives
            custom_domains: Optional override for domains
        """
        thinker = await self.db.get(Thinker, thinker_id)
        if not thinker:
            return {"success": False, "error": "Thinker not found"}

        if thinker.status != "pending":
            return {"success": False, "error": f"Thinker already {thinker.status}"}

        # Get the most recent disambiguation call
        result = await self.db.execute(
            select(ThinkerLLMCall)
            .where(ThinkerLLMCall.thinker_id == thinker_id)
            .where(ThinkerLLMCall.workflow == "disambiguation")
            .order_by(ThinkerLLMCall.id.desc())
        )
        llm_call = result.scalars().first()

        if llm_call and llm_call.parsed_result:
            parsed = json.loads(llm_call.parsed_result)
            candidates = [parsed.get("primary_candidate", {})] + parsed.get("alternatives", [])

            if candidate_index < len(candidates):
                selected = candidates[candidate_index]
                thinker.canonical_name = selected.get("canonical_name", thinker.canonical_name)
                thinker.birth_death = selected.get("birth_death")
                thinker.bio = selected.get("bio")
                thinker.domains = json.dumps(custom_domains or selected.get("domains", []))
                thinker.notable_works = json.dumps(selected.get("notable_works", []))

        thinker.status = "disambiguated"
        thinker.disambiguated_at = datetime.utcnow()
        await self.db.commit()

        logger.info(f"[Thinker] Confirmed disambiguation: {thinker.canonical_name}")

        return {
            "success": True,
            "thinker_id": thinker.id,
            "canonical_name": thinker.canonical_name,
            "status": thinker.status,
        }

    async def seed_works_from_profile(
        self,
        thinker_id: int,
        profile_url: str,
    ) -> Dict[str, Any]:
        """
        Fetch all publications from a Google Scholar profile and create ThinkerWorks.

        Publications from the author's profile are auto-accepted since they are
        definitively by this author. This seeds the bibliography before running
        author: search discovery (which will then find any works NOT on the profile).

        Args:
            thinker_id: The thinker to seed works for
            profile_url: Google Scholar profile URL (e.g., https://scholar.google.com/citations?user=zKHBVTkAAAAJ)

        Returns:
            Dict with seeding results including count of works created
        """
        from .scholar_search import get_scholar_service

        logger.info(f"[Thinker] Seeding works from Scholar profile: {profile_url}")

        # Get thinker
        thinker = await self.db.get(Thinker, thinker_id)
        if not thinker:
            return {"success": False, "error": "Thinker not found", "works_seeded": 0}

        # Fetch profile with ALL publications (paginated)
        scholar = get_scholar_service()
        profile_data = await scholar.fetch_author_profile_with_all_publications(profile_url)

        if not profile_data:
            logger.error(f"[Thinker] Failed to fetch Scholar profile: {profile_url}")
            return {"success": False, "error": "Failed to fetch profile", "works_seeded": 0}

        # Extract user ID and update thinker
        thinker.scholar_user_id = profile_data.get("scholar_user_id")
        thinker.scholar_profile_url = profile_url

        publications = profile_data.get("publications", [])
        logger.info(f"[Thinker] Found {len(publications)} publications in profile")

        # Get existing scholar_ids to avoid duplicates
        result = await self.db.execute(
            select(ThinkerWork.scholar_id)
            .where(ThinkerWork.thinker_id == thinker_id)
            .where(ThinkerWork.scholar_id.isnot(None))
        )
        existing_scholar_ids = set(row[0] for row in result.fetchall())

        # Create ThinkerWorks for each publication
        works_created = 0
        works_skipped = 0

        for pub in publications:
            scholar_id = pub.get("scholar_id")

            # Skip if already exists (by scholar_id)
            if scholar_id and scholar_id in existing_scholar_ids:
                works_skipped += 1
                continue

            # Parse year safely
            year = None
            if pub.get("year"):
                try:
                    year = int(pub["year"])
                except (ValueError, TypeError):
                    pass

            # Parse citation count safely
            citation_count = 0
            if pub.get("citations"):
                try:
                    citation_count = int(pub["citations"])
                except (ValueError, TypeError):
                    pass

            work = ThinkerWork(
                thinker_id=thinker_id,
                scholar_id=scholar_id,
                title=pub.get("title", "Unknown"),
                authors_raw=pub.get("authors"),
                year=year,
                venue=pub.get("venue"),
                citation_count=citation_count,
                link=pub.get("link"),
                decision="accepted",  # Profile works are auto-accepted
                confidence=1.0,  # Maximum confidence - from author's own profile
                reason="From author's Google Scholar profile",
                found_by_variant="scholar_profile",
                created_at=datetime.utcnow(),
            )
            self.db.add(work)
            works_created += 1

            if scholar_id:
                existing_scholar_ids.add(scholar_id)

        # Update thinker's works count
        thinker.works_discovered = (thinker.works_discovered or 0) + works_created
        await self.db.commit()

        logger.info(
            f"[Thinker] Seeded {works_created} works from profile "
            f"(skipped {works_skipped} duplicates)"
        )

        return {
            "success": True,
            "works_seeded": works_created,
            "works_skipped": works_skipped,
            "profile_name": profile_data.get("full_name"),
            "affiliation": profile_data.get("affiliation"),
            "total_in_profile": len(publications),
        }

    async def get_thinker(self, thinker_id: int) -> Optional[Thinker]:
        """Get a thinker by ID"""
        return await self.db.get(Thinker, thinker_id)

    async def list_thinkers(self) -> List[Thinker]:
        """List all thinkers"""
        result = await self.db.execute(
            select(Thinker).order_by(Thinker.canonical_name)
        )
        return list(result.scalars().all())

    async def delete_thinker(self, thinker_id: int) -> Dict[str, Any]:
        """Delete a thinker and all related data"""
        thinker = await self.db.get(Thinker, thinker_id)
        if not thinker:
            return {"success": False, "error": "Thinker not found"}

        canonical_name = thinker.canonical_name
        await self.db.delete(thinker)
        await self.db.commit()

        logger.info(f"[Thinker] Deleted thinker: {canonical_name}")
        return {"success": True, "deleted": canonical_name}

    # ============== Helper Methods ==============

    def _parse_json_list(self, value: Optional[str]) -> List[str]:
        """Parse a JSON array string or return empty list"""
        if not value:
            return []
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return [value] if value else []

    async def thinker_to_response(self, thinker: Thinker) -> Dict[str, Any]:
        """Convert Thinker model to response dict"""
        return {
            "id": thinker.id,
            "canonical_name": thinker.canonical_name,
            "birth_death": thinker.birth_death,
            "bio": thinker.bio,
            "domains": self._parse_json_list(thinker.domains),
            "notable_works": self._parse_json_list(thinker.notable_works),
            "name_variants": self._parse_json_list(thinker.name_variants),
            "status": thinker.status,
            "works_discovered": thinker.works_discovered,
            "works_harvested": thinker.works_harvested,
            "total_citations": thinker.total_citations,
            "created_at": thinker.created_at,
            "disambiguated_at": thinker.disambiguated_at,
            "harvest_started_at": thinker.harvest_started_at,
            "harvest_completed_at": thinker.harvest_completed_at,
        }


# Singleton accessor
def get_thinker_service(db: AsyncSession) -> ThinkerBibliographyService:
    """Get thinker bibliography service instance"""
    return ThinkerBibliographyService(db)
