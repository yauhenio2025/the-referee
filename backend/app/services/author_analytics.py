"""
Author Analytics Service

Uses Claude Haiku to:
- Detect self-citations (comparing citing authors with thinker name)
- Disaggregate multi-author strings into individual authors
- Normalize author name variants (WW Gasparski = W Gasparski)
"""
import logging
import json
from typing import Dict, Any, List, Optional
import anthropic

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def process_citing_authors(
    thinker_name: str,
    raw_author_groups: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Process raw author groups using Claude Haiku to:
    1. Disaggregate multi-author entries into individual authors
    2. Normalize author name variants
    3. Detect which authors are likely the thinker themselves (self-citations)

    Args:
        thinker_name: The canonical name of the thinker (e.g., "Wojciech Gasparski")
        raw_author_groups: List of dicts with 'authors', 'citation_count', 'papers_count', 'citation_ids'

    Returns:
        Dict with 'individual_authors' list, each with:
        - normalized_name: cleaned author name
        - is_self_citation: bool
        - citation_count: int
        - papers_count: int
        - citation_ids: list of citation IDs for fetching papers
    """
    if not settings.anthropic_api_key:
        logger.warning("No Anthropic API key - returning raw author groups without LLM processing")
        return {"individual_authors": raw_author_groups, "llm_processed": False}

    if not raw_author_groups:
        return {"individual_authors": [], "llm_processed": True}

    logger.info(f"Processing {len(raw_author_groups)} author groups for thinker: {thinker_name}")

    try:
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        logger.info("Anthropic client created, calling API...")

        # Format the author groups for the prompt
        author_entries = []
        for i, group in enumerate(raw_author_groups):
            author_entries.append({
                "id": i,
                "raw_authors": group.get("authors", "Unknown"),
                "citation_count": group.get("citation_count", 0),
                "papers_count": group.get("papers_count", 0),
                "citation_ids": group.get("citation_ids", [])
            })

        prompt = f"""Analyze citing authors for thinker: "{thinker_name}"

You have raw author strings from Google Scholar citations. Each entry may contain:
- Multiple authors (e.g., "MW Bukała , WW Gasparski")
- Name variants of the same person (e.g., "WW Gasparski" and "W Gasparski")
- Self-citations (the thinker citing their own work)

INPUT DATA:
{json.dumps(author_entries, indent=2)}

YOUR TASKS:
1. DISAGGREGATE: Split multi-author entries into individual authors
   - "MW Bukała , WW Gasparski" → two separate authors
   - When disaggregating, allocate the full citation_count and papers_count to EACH author

2. NORMALIZE: Identify name variants that are the same person
   - "WW Gasparski", "W Gasparski", "W. W. Gasparski" → all same person
   - Use the most complete/formal version as the normalized name

3. DETECT SELF-CITATIONS: Identify authors who are likely the thinker "{thinker_name}"
   - Match initials to full name (WW Gasparski = Wojciech W. Gasparski)
   - Consider Polish naming conventions if applicable
   - Be conservative: only mark as self-citation if confident

OUTPUT JSON (no markdown, just pure JSON):
{{
  "individual_authors": [
    {{
      "normalized_name": "Full Author Name",
      "is_self_citation": true/false,
      "confidence": 0.0-1.0,
      "merged_from": ["variant1", "variant2"],
      "total_citation_count": 123,
      "total_papers_count": 45,
      "source_entry_ids": [0, 3, 5]
    }}
  ],
  "reasoning": "Brief explanation of key decisions"
}}

Rules:
- Merge variants of the same person
- Sum citation counts when merging
- Use union of papers when merging (don't double count)
- source_entry_ids links back to input entry IDs for fetching papers later"""

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",  # Using Sonnet as Haiku may not be available
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text.strip()
        logger.info(f"Got LLM response, length: {len(response_text)} chars")

        # Parse JSON from response
        # Handle potential markdown code blocks
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.startswith("```json") or line.startswith("```"):
                    in_json = not in_json
                    continue
                if in_json:
                    json_lines.append(line)
            response_text = "\n".join(json_lines)

        result = json.loads(response_text)

        # Map source_entry_ids to actual citation_ids
        for author in result.get("individual_authors", []):
            all_citation_ids = []
            for entry_id in author.get("source_entry_ids", []):
                if entry_id < len(author_entries):
                    all_citation_ids.extend(author_entries[entry_id].get("citation_ids", []))
            author["citation_ids"] = list(set(all_citation_ids))  # dedupe

        result["llm_processed"] = True
        logger.info(f"Processed {len(raw_author_groups)} author groups into {len(result.get('individual_authors', []))} individual authors")

        return result

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {e}")
        logger.error(f"Raw response (first 500 chars): {response_text[:500] if response_text else 'empty'}")
        return {"individual_authors": raw_author_groups, "llm_processed": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Error in author analytics LLM call: {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"individual_authors": raw_author_groups, "llm_processed": False, "error": str(e)}
