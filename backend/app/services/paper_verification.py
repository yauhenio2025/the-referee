"""
Paper Verification Service

Uses Claude to verify that search results match the target paper.
Handles:
- Title matching across languages
- Author verification
- Edition detection
"""
import logging
from typing import Optional, Dict, Any, List
import anthropic

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def verify_scholar_match(
    target_title: str,
    target_author: Optional[str],
    target_year: Optional[int],
    primary_result: Dict[str, Any],
    alternatives: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Use LLM to verify that a Scholar search result matches the target paper

    Args:
        target_title: The title we're searching for
        target_author: Optional target author
        target_year: Optional target year
        primary_result: The top search result
        alternatives: Alternative results to consider

    Returns:
        Dict with 'verified', 'confidence', 'reason', 'betterMatch' (if found)
    """
    if not settings.anthropic_api_key:
        logger.warning("No Anthropic API key - skipping LLM verification")
        return {"verified": True, "confidence": 0.5, "reason": "LLM verification skipped (no API key)"}

    try:
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        # Format primary result
        primary_text = f"""
Title: "{primary_result.get('title', 'Unknown')}"
Authors: {primary_result.get('authorsRaw', 'Unknown')}
Year: {primary_result.get('year', 'Unknown')}
Citations: {primary_result.get('citationCount', 0)}
"""

        # Format alternatives
        alt_text = ""
        for i, alt in enumerate(alternatives, 1):
            alt_text += f"""
[{i}] "{alt.get('title', 'Unknown')}"
    Authors: {alt.get('authorsRaw', 'Unknown')}
    Year: {alt.get('year', 'Unknown')}
    Citations: {alt.get('citationCount', 0)}
"""

        prompt = f"""You are verifying that a Google Scholar search result matches the paper we're looking for.

TARGET PAPER:
- Title: "{target_title}"
- Author: "{target_author or 'Not specified'}"
- Year: {target_year or 'Not specified'}

PRIMARY SEARCH RESULT:
{primary_text}

ALTERNATIVE RESULTS:
{alt_text if alt_text else "None"}

YOUR TASK:
1. Determine if the PRIMARY result is the same work as the TARGET
2. Consider: title variations, translations, author name formats, year differences
3. If PRIMARY doesn't match but an ALTERNATIVE does, identify the better match

Return a JSON object:
{{
  "verified": true/false,
  "confidence": 0.0-1.0,
  "reason": "Brief explanation",
  "betterMatchIndex": null or 1/2/3 (if an alternative is a better match)
}}

Important:
- Same work in different language/edition = MATCH (verified: true)
- Book review, commentary, or work ABOUT the target = NOT a match
- Different author entirely = NOT a match
- Slight title variations are OK (e.g., "The Eighteenth Brumaire" vs "18th Brumaire")

ONLY return the JSON object, no other text."""

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )

        text = response.content[0].text.strip()

        # Extract JSON
        import json
        import re
        json_match = re.search(r"\{[\s\S]*\}", text)
        if json_match:
            result = json.loads(json_match.group())

            # If better match found, include the actual result object
            better_match = None
            if result.get("betterMatchIndex") is not None:
                idx = result["betterMatchIndex"] - 1  # Convert to 0-indexed
                if 0 <= idx < len(alternatives):
                    better_match = alternatives[idx]

            return {
                "verified": result.get("verified", True),
                "confidence": result.get("confidence", 0.8),
                "reason": result.get("reason", ""),
                "betterMatch": better_match,
            }

    except Exception as e:
        logger.warning(f"LLM verification error: {e}")

    # Fallback - assume match with moderate confidence
    return {"verified": True, "confidence": 0.6, "reason": f"LLM verification failed: {str(e)}"}


async def classify_edition_language(title: str) -> str:
    """
    Detect the language of a paper title using pattern matching

    Args:
        title: Paper title

    Returns:
        Language name (e.g., "English", "German", "Chinese")
    """
    if not title:
        return "Unknown"

    # Check for non-Latin scripts first
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
    if re.search(r"[\u0900-\u097f]", title):
        return "Hindi"

    # Check for language-specific articles/words
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
    if re.search(r"\b(de|het|een|van)\b", t):
        return "Dutch"
    if re.search(r"\b(och|en|ett|av)\b", t):
        return "Swedish"

    # Default to English for Latin-script titles
    return "English"


import re
