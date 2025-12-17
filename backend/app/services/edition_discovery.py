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
                        # Tag with the language of the query that found it
                        all_results.append({**p, "foundBy": [query_text], "queryLanguage": query_lang})
                    else:
                        all_results[existing_idx]["foundBy"].append(query_text)
                        # Keep first language found (more specific query usually runs first)

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

        # Sort by citation count (highest first) so best editions get evaluated regardless of query order
        all_results.sort(key=lambda r: r.get("citationCount", 0), reverse=True)

        # Log language distribution before evaluation
        lang_counts = {}
        for r in all_results:
            lang = r.get("queryLanguage", "unknown")
            lang_counts[lang] = lang_counts.get(lang, 0) + 1
        logger.info(f"[LLM-Discovery] Language distribution: {lang_counts}")

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

        prompt = f"""You are helping find ALL EDITIONS (translations, reprints, collected works) of an academic work on Google Scholar.

TARGET WORK:
- Title: "{title}"
- Author: "{author or 'Unknown'}"
- Year (if known): {year or 'Unknown'}
{language_instructions}{custom_lang_instruction}

YOUR TASK:
Generate Google Scholar search queries to find ACTUAL EDITIONS (not papers ABOUT the work).

CRITICAL - FINDING EDITIONS vs PAPERS ABOUT THE WORK:
- We want to find EDITIONS: translations, reprints, original texts
- We do NOT want: dissertations about, analyses of, critiques of the work
- Key difference: An EDITION has the author as Marx/Dostoevsky/etc
  A paper ABOUT has a different author who is studying the work

QUERY STRATEGIES:
1. ALWAYS include author name to find editions BY the author
2. For CJK: use the author's name in that script (马克思, マルクス, 맑스)
3. For Arabic: use transliterated author name (ماركس)
4. Combine: [translated title keywords] + [author name in that script]

AUTHOR NAME HANDLING:
- Latin/Cyrillic/Greek: author:"*surname*" OR just "surname" in query
- Chinese: 马克思 (Marx), 恩格斯 (Engels), 列宁 (Lenin)
- Japanese: マルクス (Marx), エンゲルス (Engels)
- Arabic: ماركس (Marx), إنجلز (Engels)
- Korean: 마르크스 (Marx)

EXAMPLE QUERIES FOR "The Eighteenth Brumaire of Louis Bonaparte" by Marx:
- English: "eighteenth brumaire" author:"*marx*"
- German: "achtzehnte Brumaire" marx
- French: "dix-huit brumaire" marx
- Italian: "diciotto brumaio" marx
- Spanish: "dieciocho brumario" marx
- Portuguese: "dezoito brumário" marx
- Russian: "Восемнадцатое брюмера" Маркс
- Chinese: "路易·波拿巴的雾月十八日" 马克思 OR 雾月十八日 马克思
- Arabic: "الثامن عشر من برومير" ماركس OR برومير ماركس
- Japanese: "ブリュメール十八日" マルクス

Generate 20-30 queries total covering ALL target languages!

Return a JSON array:
[
  {{ "query": "\\"eighteenth brumaire\\" author:\\"*marx*\\"", "rationale": "English - author restricted", "lang": "english" }},
  {{ "query": "路易波拿巴的雾月十八日 马克思", "rationale": "Chinese - full title + author", "lang": "chinese" }},
  {{ "query": "雾月十八日 马克思", "rationale": "Chinese - short title + author", "lang": "chinese" }},
  {{ "query": "برومير ماركس", "rationale": "Arabic - Brumaire + Marx", "lang": "arabic" }},
  {{ "query": "\\"diciotto brumaio\\" marx", "rationale": "Italian - title + author", "lang": "italian" }},
  ...
]

Include "lang" field: english, german, french, spanish, portuguese, italian, russian, chinese, japanese, korean, arabic, dutch, etc.

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
        """Ask LLM to evaluate results and identify genuine editions - processes ALL results in batches"""
        if not results:
            return {"genuineEditions": [], "highConfidence": [], "uncertain": [], "rejected": [], "reasoning": "No results to evaluate"}

        # Process ALL results in batches of 80 (like gs-harvester)
        BATCH_SIZE = 80
        if len(results) > BATCH_SIZE:
            logger.info(f"[LLM-Discovery] Large result set ({len(results)}) - processing in batches of {BATCH_SIZE}")
            return await self._evaluate_results_in_batches(target_paper, results, BATCH_SIZE)

        # Single batch processing for smaller result sets
        return await self._evaluate_single_batch(target_paper, results, 0)

    async def _evaluate_results_in_batches(
        self,
        target_paper: Dict[str, Any],
        results: List[Dict[str, Any]],
        batch_size: int,
    ) -> Dict[str, Any]:
        """Process large result sets in batches"""
        all_high_confidence = []
        all_uncertain = []
        all_rejected = []
        all_reasoning = []

        num_batches = (len(results) + batch_size - 1) // batch_size

        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(results))
            batch = results[start_idx:end_idx]

            logger.info(f"[LLM-Discovery] Processing batch {batch_idx + 1}/{num_batches} (indices {start_idx}-{end_idx - 1})")

            batch_result = await self._evaluate_single_batch(target_paper, batch, start_idx)

            all_high_confidence.extend(batch_result.get("highConfidence", []))
            all_uncertain.extend(batch_result.get("uncertain", []))
            all_rejected.extend(batch_result.get("rejected", []))
            all_reasoning.append(f"Batch {batch_idx + 1}: {batch_result.get('reasoning', '')}")

            # Small delay between batches
            if batch_idx < num_batches - 1:
                await asyncio.sleep(1)

        genuine_editions = all_high_confidence + all_uncertain

        # Log language breakdown
        lang_counts = {}
        for e in genuine_editions:
            lang = e.get("language", "Unknown")
            lang_counts[lang] = lang_counts.get(lang, 0) + 1
        logger.info(f"[LLM-Discovery] Batched evaluation complete: {len(all_high_confidence)} high, {len(all_uncertain)} uncertain, {len(all_rejected)} rejected")
        logger.info(f"[LLM-Discovery] Languages: {lang_counts}")

        return {
            "genuineEditions": genuine_editions,
            "highConfidence": all_high_confidence,
            "uncertain": all_uncertain,
            "rejected": all_rejected,
            "reasoning": "; ".join(all_reasoning),
        }

    async def _evaluate_single_batch(
        self,
        target_paper: Dict[str, Any],
        batch: List[Dict[str, Any]],
        start_idx: int,
    ) -> Dict[str, Any]:
        """Evaluate a single batch of results"""
        title = target_paper.get("title", "")
        author = target_paper.get("author") or target_paper.get("authors", "")

        # Format results - include query language context
        results_text = "\n\n".join([
            f"[{i}] \"{r.get('title', 'Unknown')}\" by {r.get('authorsRaw', 'Unknown')} ({r.get('year', '?')}) - {r.get('citationCount', 0)} citations [found via {r.get('queryLanguage', 'unknown')} query]"
            for i, r in enumerate(batch)
        ])

        prompt = f"""You are evaluating Google Scholar search results to identify editions of a work that scholars might cite.

TARGET WORK:
- Title: "{title}"
- Author: "{author or 'Unknown'}"

SEARCH RESULTS TO EVALUATE:
{results_text}

CRITICAL CONTEXT - THE USE CASE:
We are gathering ALL versions of this work that scholars have cited. Our goal is to find every
paper that could be a citation to this work - translations, reprints, collected works, anthologies,
variant titles, etc. When we later extract citations, we need ALL these sources to get complete
citation coverage.

KEY INSIGHT: Authors rarely write TWO books with similar titles. If the title looks related
and the author matches (or is a variant), it's almost certainly the same work. For example:
- "Il 18 brumaio di Napoleone Bonaparte" and "Il 18 brumaio di Luigi Bonaparte" = SAME WORK
  (Marx wrote only ONE essay about the 18th Brumaire - title variations happen in translations)
- "Rivoluzione e reazione in Francia: 1848-1850: le lotte di classe, il 18 brumaio..." = VALID
  (This anthology CONTAINS the 18 Brumaire - scholars cite it, so we need it!)

YOUR TASK - BE INCLUSIVE:
Categorize each result (indices 0-{len(batch) - 1}):

HIGH CONFIDENCE - Include if ANY of these apply:
- Author field shows {author or 'the target author'} (any variant: Marx/K Marx/M Karl/马克思/Маркс/ماركس)
- Title contains key terms from the target work in any language
- It's a translation, reprint, collected works, or anthology CONTAINING the work
- Title has minor variations (different transliterations of names, year variations, etc.)

UNCERTAIN - When there's doubt but it COULD be the work:
- Author unclear but title is close
- Non-Latin script where you can't verify
- Anthology that MIGHT contain the work

REJECTED - ONLY reject when you are ABSOLUTELY CERTAIN:
- Author is CLEARLY a different scholar writing ABOUT the work (e.g., "Il bonapartismo nel Diciotto Brumaio di Marx" by F. Antonini - this is ABOUT the work, not BY Marx)
- Title is completely unrelated to the target work
- It's obviously a commentary, dissertation, or analysis rather than the work itself

REJECTION TEST: Ask yourself "Could a scholar who cited this be citing {author}'s work?"
If YES or MAYBE → HIGH CONFIDENCE or UNCERTAIN
If DEFINITELY NO → REJECTED

LANGUAGE CLASSIFICATION - Based on title:
- "Der achtzehnte Brumaire" → German
- "El dieciocho brumario" → Spanish
- "Le dix-huit brumaire" → French
- "Il diciotto brumaio" → Italian
- Cyrillic → Russian, Chinese chars → Chinese, Arabic script → Arabic

Return JSON:
{{
  "highConfidence": [0, 2, 5, ...],
  "uncertain": [7, 12, ...],
  "rejected": [
    {{ "index": 1, "reason": "Article BY F. Antonini analyzing Marx's work - not an edition" }},
    ...
  ],
  "languages": {{
    "0": "Italian",
    "1": "German",
    ...
  }},
  "reasoning": "Brief explanation"
}}

ONLY return the JSON object."""

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

                # Map indices back to global (adding start_idx)
                high_confidence = [
                    {**batch[idx], "editionIndex": start_idx + idx, "confidence": "high", "autoSelected": True, "language": languages.get(str(idx), self._detect_language(batch[idx].get("title", "")))}
                    for idx in evaluation.get("highConfidence", [])
                    if idx < len(batch)
                ]

                uncertain = [
                    {**batch[idx], "editionIndex": start_idx + idx, "confidence": "uncertain", "autoSelected": False, "language": languages.get(str(idx), self._detect_language(batch[idx].get("title", "")))}
                    for idx in evaluation.get("uncertain", [])
                    if idx < len(batch)
                ]

                rejected = [
                    {**batch[r["index"]], "rejectionReason": r["reason"], "editionIndex": start_idx + r["index"], "language": languages.get(str(r["index"]), "Unknown")}
                    for r in evaluation.get("rejected", [])
                    if r["index"] < len(batch)
                ]

                return {
                    "highConfidence": high_confidence,
                    "uncertain": uncertain,
                    "rejected": rejected,
                    "reasoning": evaluation.get("reasoning", ""),
                }

        except Exception as e:
            logger.error(f"[LLM-Discovery] Batch evaluation error: {e}")

        # Fallback: include all results with basic language classification
        def fallback_language(r: dict) -> str:
            """Use queryLanguage if available, else detect from title"""
            query_lang = r.get("queryLanguage", "").capitalize()
            if query_lang:
                return query_lang
            return self._detect_language(r.get("title", ""))

        return {
            "genuineEditions": [{**r, "editionIndex": i, "confidence": "uncertain", "autoSelected": False, "language": fallback_language(r)} for i, r in enumerate(results)],
            "highConfidence": [],
            "uncertain": [{**r, "editionIndex": i, "confidence": "uncertain", "autoSelected": False, "language": fallback_language(r)} for i, r in enumerate(results)],
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

    async def fetch_more_in_language(
        self,
        paper: Dict[str, Any],
        target_language: str,
        max_results: int = 50,
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, Any]:
        """
        Fetch more editions in a specific language.
        Used when user clicks "Fetch more" for a language filter.

        Args:
            progress_callback: Optional async callback for progress updates
        """
        title = paper.get("title", "")
        author = paper.get("author") or paper.get("authors", "")
        year = paper.get("year")

        logger.info(f"═" * 80)
        logger.info(f"[LLM-Discovery] Fetching more editions in {target_language.upper()}")
        logger.info(f"  Title: {title}")
        logger.info(f"═" * 80)

        # Generate targeted queries for this specific language
        if progress_callback:
            await progress_callback({
                "stage": "generating_queries",
                "message": f"Generating {target_language} queries...",
            })

        queries = await self._generate_targeted_queries(paper, target_language)
        logger.info(f"[LLM-Discovery] Generated {len(queries)} {target_language} queries")

        # Execute queries
        all_results = []
        queries_used = []
        hl_code = LANGUAGE_TO_HL_CODE.get(target_language, "en")

        for i, q in enumerate(queries):
            query_text = q.get("query", "")
            queries_used.append(query_text)

            if progress_callback:
                await progress_callback({
                    "stage": "searching",
                    "query": i + 1,
                    "total_queries": len(queries),
                    "current_query": query_text[:50],
                })

            logger.info(f"[LLM-Discovery] Query {i+1}/{len(queries)}: {query_text[:60]}...")

            try:
                results = await self.scholar.search(query_text, language=hl_code, max_results=30)
                papers = results.get("papers", [])
                logger.info(f"  Found: {len(papers)} results")

                for p in papers:
                    existing_idx = next(
                        (j for j, r in enumerate(all_results)
                         if r.get("title", "").lower() == p.get("title", "").lower() or
                         r.get("scholarId") == p.get("scholarId")),
                        None
                    )
                    if existing_idx is None:
                        all_results.append({**p, "queryLanguage": target_language})

            except Exception as e:
                logger.error(f"  ERROR: {e}")

            await asyncio.sleep(2)

        logger.info(f"[LLM-Discovery] Total unique results for {target_language}: {len(all_results)}")

        # Evaluate results
        if not all_results:
            return {
                "genuineEditions": [],
                "highConfidence": [],
                "uncertain": [],
                "rejected": [],
                "queriesUsed": queries_used,
                "totalSearched": 0,
            }

        if progress_callback:
            await progress_callback({
                "stage": "evaluating",
                "total_results": len(all_results),
                "message": f"Evaluating {len(all_results)} results...",
            })

        evaluation = await self._evaluate_results(paper, all_results)

        return {
            "genuineEditions": evaluation.get("genuineEditions", []),
            "highConfidence": evaluation.get("highConfidence", []),
            "uncertain": evaluation.get("uncertain", []),
            "rejected": evaluation.get("rejected", []),
            "queriesUsed": queries_used,
            "totalSearched": len(all_results),
            # Raw results for debugging - before LLM processing
            "rawResults": all_results,
            "llmClassification": {
                "highCount": len(evaluation.get("highConfidence", [])),
                "uncertainCount": len(evaluation.get("uncertain", [])),
                "rejectedCount": len(evaluation.get("rejected", [])),
                "reasoning": evaluation.get("reasoning", ""),
            },
        }

    async def _generate_targeted_queries(
        self,
        paper: Dict[str, Any],
        target_language: str,
    ) -> List[Dict[str, str]]:
        """Generate queries specifically for one language"""
        title = paper.get("title", "")
        author = paper.get("author") or paper.get("authors", "")

        prompt = f"""Generate Google Scholar queries to find EDITIONS of this work in {target_language.upper()}:

TARGET WORK:
- Title: "{title}"
- Author: "{author or 'Unknown'}"
- Target Language: {target_language.upper()}

CRITICAL: Generate queries that will find ACTUAL EDITIONS (translations/reprints), not papers ABOUT the work.

STRATEGIES FOR {target_language.upper()}:
1. Include the author's name (in appropriate script for this language)
2. Use the translated title keywords
3. Mix strict and loose queries

AUTHOR NAMES IN DIFFERENT SCRIPTS:
- Chinese: 马克思 (Marx), 恩格斯 (Engels)
- Arabic: ماركس (Marx), إنجلز (Engels)
- Russian: Маркс (Marx)
- Japanese: マルクス (Marx)

Generate 5-8 queries specifically for {target_language}.

Return JSON array:
[
  {{ "query": "translated title keywords + author name", "rationale": "explanation" }},
  ...
]

ONLY return the JSON array."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text
            json_match = re.search(r"\[[\s\S]*\]", text)
            if json_match:
                queries = json.loads(json_match.group())
                # Add language tag to each query
                for q in queries:
                    q["lang"] = target_language
                return queries

        except Exception as e:
            logger.error(f"[LLM-Discovery] Targeted query generation error: {e}")

        # Fallback: simple query with author
        return [
            {"query": f'"{title}" {author}', "rationale": f"Fallback: title + author", "lang": target_language},
        ]
