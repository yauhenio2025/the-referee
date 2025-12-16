"""
LLM-Driven Edition Discovery Service

Ported from the original gs-harvester JavaScript implementation.

Uses Claude Sonnet to:
1. Generate optimal search queries to find ALL editions of a work
2. Evaluate search results to identify genuine editions
3. Be fully transparent about decisions

The LLM does ALL the thinking - no hardcoded logic for query building.
"""
import asyncio
import json
import logging
import re
from typing import Optional, Dict, Any, List
import anthropic

from ..config import get_settings
from .scholar_search import get_scholar_service

logger = logging.getLogger(__name__)
settings = get_settings()


# Available languages for edition discovery
AVAILABLE_LANGUAGES = [
    {"code": "english", "name": "English", "icon": "🇬🇧"},
    {"code": "german", "name": "German", "icon": "🇩🇪"},
    {"code": "french", "name": "French", "icon": "🇫🇷"},
    {"code": "spanish", "name": "Spanish", "icon": "🇪🇸"},
    {"code": "portuguese", "name": "Portuguese", "icon": "🇧🇷"},
    {"code": "italian", "name": "Italian", "icon": "🇮🇹"},
    {"code": "russian", "name": "Russian", "icon": "🇷🇺"},
    {"code": "chinese", "name": "Chinese", "icon": "🇨🇳"},
    {"code": "japanese", "name": "Japanese", "icon": "🇯🇵"},
    {"code": "korean", "name": "Korean", "icon": "🇰🇷"},
    {"code": "arabic", "name": "Arabic", "icon": "🇸🇦"},
    {"code": "dutch", "name": "Dutch", "icon": "🇳🇱"},
    {"code": "polish", "name": "Polish", "icon": "🇵🇱"},
    {"code": "turkish", "name": "Turkish", "icon": "🇹🇷"},
    {"code": "persian", "name": "Persian/Farsi", "icon": "🇮🇷"},
    {"code": "hindi", "name": "Hindi", "icon": "🇮🇳"},
    {"code": "hebrew", "name": "Hebrew", "icon": "🇮🇱"},
    {"code": "greek", "name": "Greek", "icon": "🇬🇷"},
]

# Map language names to Google Scholar hl parameter codes
LANGUAGE_TO_HL_CODE = {
    "english": "en",
    "german": "de",
    "french": "fr",
    "spanish": "es",
    "portuguese": "pt",
    "italian": "it",
    "russian": "ru",
    "chinese": "zh-CN",
    "japanese": "ja",
    "korean": "ko",
    "arabic": "ar",
    "dutch": "nl",
    "polish": "pl",
    "turkish": "tr",
    "persian": "fa",
    "hindi": "hi",
    "hebrew": "iw",  # Google uses iw for Hebrew
    "greek": "el",
    "swedish": "sv",
    "danish": "da",
    "norwegian": "no",
    "finnish": "fi",
    "czech": "cs",
    "hungarian": "hu",
    "romanian": "ro",
    "ukrainian": "uk",
    "vietnamese": "vi",
    "thai": "th",
    "indonesian": "id",
}


class EditionDiscoveryService:
    """LLM-driven edition discovery service"""

    def __init__(
        self,
        language_strategy: str = "major_languages",
        custom_languages: Optional[List[str]] = None,
    ):
        self.client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        self.model = "claude-sonnet-4-5-20250929"
        self.scholar = get_scholar_service()
        self.language_strategy = language_strategy
        self.custom_languages = custom_languages or []

    async def discover_editions(
        self,
        paper: Dict[str, Any],
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point: Discover all editions of a work

        Args:
            paper: Dict with 'title', 'author', 'year' fields
            progress_callback: Optional callback for progress updates

        Returns:
            Dict with discovery results
        """
        title = paper.get("title", "")
        author = paper.get("author") or paper.get("authors", "")
        year = paper.get("year")

        logger.info(f"═" * 80)
        logger.info(f"[LLM-Discovery] Finding ALL editions of: \"{title}\"")
        logger.info(f"  Author: {author or 'not specified'}")
        logger.info(f"  Language strategy: {self.language_strategy}")
        logger.info(f"═" * 80)

        # Step 1: Generate search queries using LLM
        queries = await self._generate_queries(paper)
        logger.info(f"[LLM-Discovery] Generated {len(queries)} queries")

        # Step 2: Execute all queries and collect results
        all_results = []
        query_results = []

        for i, q in enumerate(queries):
            query_text = q.get("query", "")
            rationale = q.get("rationale", "")
            query_lang = q.get("lang", "english").lower()
            # Convert language name to Google Scholar hl code
            hl_code = LANGUAGE_TO_HL_CODE.get(query_lang, "en")

            if progress_callback:
                await progress_callback({
                    "stage": "searching",
                    "query": i + 1,
                    "total_queries": len(queries),
                    "current_query": query_text[:60],
                })

            logger.info(f"[LLM-Discovery] Executing query {i+1}/{len(queries)} [{query_lang}]: {query_text[:60]}...")

            try:
                results = await self.scholar.search(query_text, language=hl_code, max_results=30)
                papers = results.get("papers", [])

                logger.info(f"  Found: {len(papers)} results")

                # Retry with reformulated query if low results
                if len(papers) < 5:
                    reformulated = await self._reformulate_query(paper, query_text, rationale, len(papers), query_lang)
                    if reformulated and reformulated.get("query") != query_text:
                        logger.info(f"  [RETRY] New query [{query_lang}]: {reformulated['query'][:60]}...")
                        retry_results = await self.scholar.search(reformulated["query"], language=hl_code, max_results=30)
                        retry_papers = retry_results.get("papers", [])
                        if len(retry_papers) > len(papers):
                            papers = retry_papers
                            query_text = reformulated["query"]
                            rationale = f"{rationale} → Reformulated: {reformulated.get('rationale', '')}"

                query_results.append({
                    "query": query_text,
                    "rationale": rationale,
                    "resultCount": len(papers),
                    "results": papers,
                })

                # Add to combined results with deduplication
                for p in papers:
                    existing_idx = next(
                        (j for j, r in enumerate(all_results)
                         if r.get("title", "").lower() == p.get("title", "").lower() or
                         r.get("scholarId") == p.get("scholarId")),
                        None
                    )
                    if existing_idx is None:
                        all_results.append({**p, "foundBy": [query_text]})
                    else:
                        all_results[existing_idx]["foundBy"].append(query_text)

            except Exception as e:
                logger.error(f"  ERROR: {e}")
                query_results.append({
                    "query": query_text,
                    "rationale": rationale,
                    "error": str(e),
                })

            # Rate limiting between queries
            await asyncio.sleep(2)

        logger.info(f"[LLM-Discovery] Total unique results: {len(all_results)}")

        # Step 3: Evaluate results using LLM
        if progress_callback:
            await progress_callback({
                "stage": "evaluating",
                "total_results": len(all_results),
            })

        evaluation = await self._evaluate_results(paper, all_results)

        logger.info(f"[LLM-Discovery] Evaluation complete:")
        logger.info(f"  High confidence: {len(evaluation.get('highConfidence', []))} (auto-selected)")
        logger.info(f"  Uncertain: {len(evaluation.get('uncertain', []))} (needs review)")
        logger.info(f"  Rejected: {len(evaluation.get('rejected', []))}")

        return {
            "targetWork": paper,
            "queries": query_results,
            "allResults": all_results,
            "genuineEditions": evaluation.get("genuineEditions", []),
            "highConfidence": evaluation.get("highConfidence", []),
            "uncertain": evaluation.get("uncertain", []),
            "rejected": evaluation.get("rejected", []),
            "llmReasoning": evaluation.get("reasoning", ""),
            "summary": {
                "queriesGenerated": len(queries),
                "totalResults": len(all_results),
                "genuineEditions": len(evaluation.get("genuineEditions", [])),
                "highConfidence": len(evaluation.get("highConfidence", [])),
                "uncertain": len(evaluation.get("uncertain", [])),
                "rejected": len(evaluation.get("rejected", [])),
            }
        }

    async def _generate_queries(self, paper: Dict[str, Any]) -> List[Dict[str, str]]:
        """Ask LLM to generate optimal search queries"""
        title = paper.get("title", "")
        author = paper.get("author") or paper.get("authors", "")
        year = paper.get("year")

        # Build language-specific instructions
        if self.language_strategy == "english_only":
            language_instructions = """
LANGUAGE FOCUS: English Only
- Focus on editions published in English
- Look for translations INTO English"""
        elif self.language_strategy == "major_languages":
            language_instructions = """
LANGUAGE FOCUS: Major World Languages
REQUIRED: Generate separate queries for EACH of these languages:
1. English - original/translation
2. German - e.g., "Der/Die/Das..." title patterns
3. French - e.g., "Le/La/Les..." title patterns
4. Spanish - e.g., "El/La/Los..." title patterns
5. Portuguese - e.g., "O/A/Os..." title patterns
6. Italian - e.g., "Il/La/I..." title patterns
7. Russian - use Cyrillic script for title keywords
8. Chinese - use Chinese characters for title
9. Japanese - use Japanese script for title

IMPORTANT: Generate at least 8-12 queries covering ALL major languages!"""
        else:
            language_instructions = """
LANGUAGE FOCUS: All Languages
- Find editions in ANY language worldwide
- Include all regional translations and local editions"""

        # Add custom languages
        custom_lang_instruction = ""
        if self.custom_languages:
            lang_names = [l.get("name", l) for l in self.custom_languages] if isinstance(self.custom_languages[0], dict) else self.custom_languages
            custom_lang_instruction = f"\n\nADDITIONAL LANGUAGES: Also include queries for: {', '.join(lang_names)}"

        prompt = f"""You are helping find ALL editions of an academic work on Google Scholar.

TARGET WORK:
- Title: "{title}"
- Author: "{author or 'Unknown'}"
- Year (if known): {year or 'Unknown'}
{language_instructions}{custom_lang_instruction}

YOUR TASK:
Generate Google Scholar search queries to find editions based on the language focus above.

CONTEXT:
- We want to catch editions with slight title variations
- Classic works often have many editions: original, translations, reprints, collected works
- Different editions may be cited separately, so we need ALL of them
- Google Scholar supports: allintitle:"...", author:"*name*" (wildcards), "exact phrase"

AUTHOR NAME HANDLING:
1. ALPHABETIC SCRIPTS (Latin, Cyrillic, Greek, Arabic):
   - Use SURNAME ONLY with wildcards: author:"*surname*"
2. CJK SCRIPTS (Chinese, Japanese, Korean):
   - DO NOT use author: operator - search by TITLE ONLY

QUERY STRATEGIES TO MIX:
- STRICT: allintitle:"exact phrase" author:"*surname*" - precise but may miss results
- MEDIUM: "quoted phrase" author:"*surname*" - without allintitle, catches more
- LOOSE: just keywords no quotes - catches everything, needs filtering

Generate 15-25 queries total, mixing strict and loose for each language!

Return a JSON array of queries with LANGUAGE CODE for each:
[
  {{ "query": "allintitle:\\"eighteenth brumaire\\" author:\\"*marx*\\"", "rationale": "English STRICT", "lang": "english" }},
  {{ "query": "\\"eighteenth brumaire\\" marx", "rationale": "English MEDIUM", "lang": "english" }},
  {{ "query": "Der achtzehnte Brumaire marx", "rationale": "German MEDIUM", "lang": "german" }},
  {{ "query": "Le dix-huit brumaire marx", "rationale": "French MEDIUM", "lang": "french" }},
  {{ "query": "雾月十八日 马克思", "rationale": "Chinese LOOSE - keywords", "lang": "chinese" }},
  {{ "query": "восемнадцатое брюмера маркс", "rationale": "Russian LOOSE", "lang": "russian" }},
  ...
]

IMPORTANT: Include "lang" field with lowercase language name (english, german, french, spanish, portuguese, italian, russian, chinese, japanese, korean, arabic, dutch, polish, turkish, etc.)

ONLY return the JSON array, no other text."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
            json_match = re.search(r"\[[\s\S]*\]", text)
            if json_match:
                return json.loads(json_match.group())

        except Exception as e:
            logger.error(f"[LLM-Discovery] Query generation error: {e}")

        # Fallback: simple queries
        return [
            {"query": f'allintitle:"{title}"', "rationale": "Fallback: exact title"},
            {"query": f'"{title}" {author}', "rationale": "Fallback: quoted title + author"},
        ]

    async def _evaluate_results(
        self,
        target_paper: Dict[str, Any],
        results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Ask LLM to evaluate results and identify genuine editions"""
        if not results:
            return {"genuineEditions": [], "highConfidence": [], "uncertain": [], "rejected": [], "reasoning": "No results to evaluate"}

        title = target_paper.get("title", "")
        author = target_paper.get("author") or target_paper.get("authors", "")

        # Format results for the LLM
        results_text = "\n\n".join([
            f"[{i}] \"{r.get('title', 'Unknown')}\" by {r.get('authorsRaw', 'Unknown')} ({r.get('year', '?')}) - {r.get('citationCount', 0)} citations\n      {r.get('abstract', 'No abstract')[:200]}"
            for i, r in enumerate(results[:80])  # Limit to 80 to avoid token limits
        ])

        prompt = f"""You are evaluating Google Scholar search results to identify genuine editions of a specific work.

TARGET WORK:
- Title: "{title}"
- Author: "{author or 'Unknown'}"

SEARCH RESULTS TO EVALUATE:
{results_text}

YOUR TASK:
Categorize each result into THREE categories:

HIGH CONFIDENCE GENUINE EDITION - Clearly IS the target work:
- Author matches (or obvious variants like initials)
- Title is clearly the same work (original, translation, reprint)
- TRANSLATIONS COUNT AS GENUINE EDITIONS!

UNCERTAIN - Needs human review:
- Author unclear or might be editor/translator
- Title similar but might be different work

REJECTED - Clearly NOT a genuine edition:
- Different author entirely (e.g., a dissertation BY someone studying the work)
- Commentaries, analyses, works ABOUT the target work
- Book reviews

CLASSIFY THE LANGUAGE of each result based on its title.

Return a JSON object:
{{
  "highConfidence": [0, 2, 5, ...],
  "uncertain": [7, 12, ...],
  "rejected": [
    {{ "index": 1, "reason": "Dissertation BY someone about the work" }},
    ...
  ],
  "languages": {{
    "0": "English",
    "1": "German",
    "2": "Chinese",
    ...
  }},
  "reasoning": "Brief explanation of evaluation approach"
}}

ONLY return the JSON object, no other text."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                evaluation = json.loads(json_match.group())
                languages = evaluation.get("languages", {})

                # Map high confidence editions
                high_confidence = [
                    {**results[idx], "editionIndex": idx, "confidence": "high", "autoSelected": True, "language": languages.get(str(idx), "Unknown")}
                    for idx in evaluation.get("highConfidence", [])
                    if idx < len(results)
                ]

                # Map uncertain editions
                uncertain = [
                    {**results[idx], "editionIndex": idx, "confidence": "uncertain", "autoSelected": False, "language": languages.get(str(idx), "Unknown")}
                    for idx in evaluation.get("uncertain", [])
                    if idx < len(results)
                ]

                # Map rejected
                rejected = [
                    {**results[r["index"]], "rejectionReason": r["reason"], "editionIndex": r["index"], "language": languages.get(str(r["index"]), "Unknown")}
                    for r in evaluation.get("rejected", [])
                    if r["index"] < len(results)
                ]

                genuine_editions = high_confidence + uncertain

                return {
                    "genuineEditions": genuine_editions,
                    "highConfidence": high_confidence,
                    "uncertain": uncertain,
                    "rejected": rejected,
                    "reasoning": evaluation.get("reasoning", ""),
                }

        except Exception as e:
            logger.error(f"[LLM-Discovery] Evaluation error: {e}")

        # Fallback: include all results with basic language classification
        return {
            "genuineEditions": [{**r, "editionIndex": i, "confidence": "uncertain", "autoSelected": False, "language": self._detect_language(r.get("title", ""))} for i, r in enumerate(results)],
            "highConfidence": [],
            "uncertain": [{**r, "editionIndex": i, "confidence": "uncertain", "autoSelected": False, "language": self._detect_language(r.get("title", ""))} for i, r in enumerate(results)],
            "rejected": [],
            "reasoning": "Fallback: included all results due to evaluation error",
        }

    async def _reformulate_query(
        self,
        paper: Dict[str, Any],
        failed_query: str,
        original_rationale: str,
        result_count: int,
        target_language: str = "english",
    ) -> Optional[Dict[str, str]]:
        """Ask LLM to reformulate a query that returned few results"""
        title = paper.get("title", "")
        author = paper.get("author") or paper.get("authors", "")

        prompt = f"""A Google Scholar query returned only {result_count} results. Help reformulate to find MORE results.

TARGET WORK:
- Title: "{title}"
- Author: "{author or 'Unknown'}"
- TARGET LANGUAGE: {target_language.upper()}

UNDERPERFORMING QUERY: {failed_query}
ORIGINAL INTENT: {original_rationale}
RESULTS FOUND: {result_count}

Likely causes:
1. Title translated differently than expected in {target_language}
2. Author name format different in that language
3. Search terms too restrictive (allintitle: is very strict!)

REFORMULATION STRATEGIES:
- REMOVE allintitle: operator - use regular search instead
- REMOVE author: restriction - editions often have editors/translators listed
- KEEP the query IN {target_language.upper()} - do NOT switch to English!
- Try alternate {target_language} translations of the title

Return a JSON object with ONE reformulated query:
{{
  "query": "the new broader query",
  "rationale": "why this should find more results"
}}

If you cannot think of a good alternative, return:
{{ "query": null, "rationale": "no good alternative" }}

ONLY return the JSON object, no other text."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                result = json.loads(json_match.group())
                if result.get("query") and result["query"] != failed_query:
                    return result

        except Exception as e:
            logger.error(f"[LLM-Discovery] Reformulation error: {e}")

        return None

    def _detect_language(self, title: str) -> str:
        """Simple pattern-based language detection"""
        if not title:
            return "Unknown"

        # Non-Latin scripts
        if re.search(r"[\u4e00-\u9fff]", title):
            return "Chinese"
        if re.search(r"[\u3040-\u30ff]", title):
            return "Japanese"
        if re.search(r"[\u0400-\u04ff]", title):
            return "Russian"
        if re.search(r"[\uac00-\ud7af]", title):
            return "Korean"
        if re.search(r"[\u0600-\u06ff]", title):
            return "Arabic"
        if re.search(r"[\u0590-\u05ff]", title):
            return "Hebrew"

        # Language-specific patterns
        t = title.lower()
        if re.search(r"\b(der|die|das|und|eine?r?s?)\b", t):
            return "German"
        if re.search(r"\b(le|la|les|du|de la|l'|une?)\b", t):
            return "French"
        if re.search(r"\b(el|la|los|las|del|una?)\b", t):
            return "Spanish"
        if re.search(r"\b(il|lo|la|gli|le|della?|un[ao]?)\b", t):
            return "Italian"
        if re.search(r"\b(o|a|os|as|do|da|uma?)\b", t):
            return "Portuguese"

        return "English"

    @staticmethod
    async def recommend_languages(paper: Dict[str, Any]) -> Dict[str, Any]:
        """Get LLM recommendation for languages to search for a paper"""
        if not settings.anthropic_api_key:
            return {
                "success": False,
                "recommended": ["english", "german", "french", "spanish"],
                "reasoning": "Fallback - no API key configured",
                "authorLanguage": None,
                "primaryMarkets": ["english"],
            }

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        title = paper.get("title", "")
        author = paper.get("author") or paper.get("authors", "")
        year = paper.get("year")

        prompt = f"""You are helping determine which languages would contain the most important citations and editions of an academic work.

WORK TO ANALYZE:
- Title: "{title}"
- Author: "{author or 'Unknown'}"
- Year: {year or 'Unknown'}

Consider:
1. The author's native language and country
2. The work's subject matter and which scholarly traditions engage with it
3. Historical context (e.g., Marxist works have important Russian/Chinese editions)
4. The work's influence in different regions

Return a JSON object:
{{
  "recommended": ["english", "german", "french", ...],
  "reasoning": "Brief explanation",
  "authorLanguage": "german",
  "primaryMarkets": ["english", "german"]
}}

ONLY return the JSON object, no other text."""

        try:
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                result = json.loads(json_match.group())
                return {
                    "success": True,
                    "recommended": result.get("recommended", []),
                    "reasoning": result.get("reasoning", ""),
                    "authorLanguage": result.get("authorLanguage"),
                    "primaryMarkets": result.get("primaryMarkets", []),
                }

        except Exception as e:
            logger.error(f"[LLM-Discovery] Language recommendation error: {e}")

        return {
            "success": False,
            "recommended": ["english", "german", "french", "spanish"],
            "reasoning": "Fallback - LLM call failed",
            "authorLanguage": None,
            "primaryMarkets": ["english"],
        }
