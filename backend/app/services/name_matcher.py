"""
Name Matcher Service

Reusable module for intelligent name matching with:
1. Heuristics for fast candidate generation
2. LLM validation for accuracy

Use cases:
- Author name merging in citation analysis
- Author disambiguation in paper harvesting
- Matching authors across different sources
"""
import logging
import json
import re
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from difflib import SequenceMatcher
import anthropic

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class NameMatch:
    """A proposed name match/merge."""
    canonical: str
    variants: List[str]
    confidence: float = 0.0
    reason: str = ""
    approved: bool = False


@dataclass
class NameMatchResult:
    """Result of name matching process."""
    matches: List[NameMatch]
    unmatched: List[str]
    llm_validated: bool = False
    error: Optional[str] = None


# =============================================================================
# Core Name Utilities
# =============================================================================

def normalize_name(name: str) -> str:
    """
    Normalize a name for comparison.
    - Removes extra whitespace
    - Removes periods after initials
    - Removes 'et al' variations
    """
    name = " ".join(name.split())
    name = re.sub(r'\.(?=\s|$)', '', name)
    name = re.sub(r'\s*et\s*al\.?\s*$', '', name, flags=re.IGNORECASE)
    return name.strip()


def extract_name_parts(name: str) -> Dict[str, Any]:
    """
    Extract structured parts from a name.

    Returns:
        {
            "surname": "gasparski",
            "initials": ["W", "W"],
            "given_names": [],  # if full names present
            "raw": "WW Gasparski"
        }
    """
    normalized = normalize_name(name)
    parts = normalized.split()

    if not parts:
        return {"surname": "", "initials": [], "given_names": [], "raw": name}

    # Last part is usually surname
    surname = parts[-1].lower()

    # Everything before surname
    prefixes = parts[:-1]
    initials = []
    given_names = []

    for p in prefixes:
        if len(p) <= 2 or p.isupper():
            # Likely initials (single letters or all caps like "WW")
            initials.extend([c.upper() for c in p if c.isalpha()])
        else:
            # Likely a full given name
            given_names.append(p)
            initials.append(p[0].upper())

    return {
        "surname": surname,
        "initials": initials,
        "given_names": given_names,
        "raw": name
    }


def split_author_string(author_string: str) -> List[str]:
    """
    Split a multi-author string into individual authors.
    Handles: commas, semicolons, " and ", " & "
    """
    authors = re.split(r'\s*[,;]\s*|\s+and\s+|\s*&\s*', author_string)
    authors = [a.strip() for a in authors if a.strip()]
    # Filter out suffixes
    authors = [a for a in authors if not re.match(r'^(Jr\.?|Sr\.?|I{1,3}|IV|V)$', a, re.IGNORECASE)]
    return authors if authors else [author_string]


# =============================================================================
# Heuristic Matching
# =============================================================================

def names_might_match(name1: str, name2: str, surname_threshold: float = 0.8) -> Tuple[bool, float]:
    """
    Check if two names might refer to the same person using heuristics.

    Returns:
        (might_match: bool, confidence: float)
    """
    n1 = normalize_name(name1).lower()
    n2 = normalize_name(name2).lower()

    # Exact match
    if n1 == n2:
        return True, 1.0

    parts1 = extract_name_parts(name1)
    parts2 = extract_name_parts(name2)

    if not parts1["surname"] or not parts2["surname"]:
        return False, 0.0

    # Surnames must be similar
    surname_sim = SequenceMatcher(None, parts1["surname"], parts2["surname"]).ratio()
    if surname_sim < surname_threshold:
        return False, 0.0

    # Check initials
    initials1 = set(parts1["initials"])
    initials2 = set(parts2["initials"])

    if initials1 and initials2:
        # At least one initial should match
        if not initials1 & initials2:
            return False, 0.0
        # Higher confidence if initials are subset
        if initials1 <= initials2 or initials2 <= initials1:
            return True, 0.8
        # Some overlap
        overlap = len(initials1 & initials2) / max(len(initials1), len(initials2))
        return True, 0.5 + (overlap * 0.3)

    # One has initials, other doesn't - possible match
    return True, 0.5


def find_match_candidates(names: List[str]) -> List[NameMatch]:
    """
    Find potential name matches from a list using heuristics.

    Returns list of NameMatch objects with proposed merges.
    """
    candidates = []
    processed = set()

    for i, name1 in enumerate(names):
        if name1 in processed:
            continue

        variants = [name1]
        best_confidence = 0.0

        for j, name2 in enumerate(names):
            if i >= j or name2 in processed:
                continue

            might_match, confidence = names_might_match(name1, name2)
            if might_match:
                variants.append(name2)
                processed.add(name2)
                best_confidence = max(best_confidence, confidence)

        processed.add(name1)

        if len(variants) > 1:
            # Use longest variant as canonical candidate
            canonical = max(variants, key=len)
            candidates.append(NameMatch(
                canonical=canonical,
                variants=variants,
                confidence=best_confidence
            ))

    return candidates


def check_name_against_reference(
    name: str,
    reference_name: str,
    strict: bool = False
) -> Tuple[bool, float]:
    """
    Check if a name matches a reference name (e.g., for self-citation detection).

    Args:
        name: The name to check
        reference_name: The reference to match against
        strict: If True, require higher confidence for match

    Returns:
        (is_match: bool, confidence: float)
    """
    name_parts = extract_name_parts(name)
    ref_parts = extract_name_parts(reference_name)

    if not name_parts["surname"] or not ref_parts["surname"]:
        return False, 0.0

    # Surname must match
    surname_sim = SequenceMatcher(None, name_parts["surname"], ref_parts["surname"]).ratio()
    threshold = 0.9 if strict else 0.85
    if surname_sim < threshold:
        return False, 0.0

    # Check initials against reference's given names/initials
    name_initials = set(name_parts["initials"])
    ref_initials = set(ref_parts["initials"])

    if name_initials and ref_initials:
        if name_initials <= ref_initials or ref_initials <= name_initials:
            return True, 0.9
        if name_initials & ref_initials:
            return True, 0.7
    elif not name_initials:
        # Just surname match
        return True, 0.6

    return False, 0.0


# =============================================================================
# LLM Validation
# =============================================================================

async def validate_matches_with_llm(
    candidates: List[NameMatch],
    context: str = "",
    reference_name: Optional[str] = None,
    reference_matches: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Use Claude Sonnet to validate proposed name matches.

    Args:
        candidates: List of NameMatch objects to validate
        context: Additional context for the LLM (e.g., "citation analysis for academic papers")
        reference_name: Optional reference name for identity matching (e.g., thinker name)
        reference_matches: Names flagged as potentially matching the reference

    Returns:
        {
            "approved_matches": [NameMatch, ...],
            "rejected_matches": [NameMatch, ...],
            "confirmed_reference_matches": [str, ...],  # if reference_name provided
            "llm_validated": bool,
            "error": str or None
        }
    """
    if not settings.anthropic_api_key:
        logger.warning("No Anthropic API key - approving all heuristic suggestions")
        for c in candidates:
            c.approved = True
        return {
            "approved_matches": candidates,
            "rejected_matches": [],
            "confirmed_reference_matches": reference_matches or [],
            "llm_validated": False
        }

    if not candidates and not reference_matches:
        return {
            "approved_matches": [],
            "rejected_matches": [],
            "confirmed_reference_matches": [],
            "llm_validated": True
        }

    try:
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        # Build prompt
        prompt_parts = [f"You are validating name matches{f' for {context}' if context else ''}."]

        if candidates:
            candidates_data = [
                {"canonical": c.canonical, "variants": c.variants, "confidence": c.confidence}
                for c in candidates
            ]
            prompt_parts.append(f"""
TASK 1 - VALIDATE NAME MERGES:
These name pairs were flagged as potentially the same person. Approve or reject each.

{json.dumps(candidates_data, indent=2)}
""")

        if reference_name and reference_matches:
            prompt_parts.append(f"""
TASK 2 - VALIDATE IDENTITY MATCHES:
Reference person: "{reference_name}"
These names were flagged as potentially being this person:

{json.dumps(reference_matches, indent=2)}
""")

        prompt_parts.append("""
RESPOND WITH VALID JSON ONLY:
{
  "approved_merges": [
    {"canonical": "Best Name Form", "variants": ["variant1", "variant2"], "reason": "brief reason"}
  ],
  "rejected_merges": [
    {"variants": ["name1", "name2"], "reason": "why different people"}
  ],
  "confirmed_identity_matches": ["name1", "name2"],
  "rejected_identity_matches": ["name3"]
}

Rules:
- Approve merge if clearly same person
- Reject merge if possibly different people
- Use most complete/formal name as canonical
- Be conservative - when in doubt, reject""")

        prompt = "\n".join(prompt_parts)

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text.strip()

        # Clean up JSON
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.startswith("```"):
                    in_json = not in_json
                    continue
                if in_json:
                    json_lines.append(line)
            response_text = "\n".join(json_lines)

        response_text = re.sub(r',\s*([}\]])', r'\1', response_text)
        result = json.loads(response_text)

        # Map back to NameMatch objects
        approved_variants = {
            tuple(sorted(m["variants"])): m
            for m in result.get("approved_merges", [])
        }

        approved_matches = []
        rejected_matches = []

        for candidate in candidates:
            key = tuple(sorted(candidate.variants))
            if key in approved_variants:
                approved = approved_variants[key]
                candidate.canonical = approved.get("canonical", candidate.canonical)
                candidate.approved = True
                candidate.reason = approved.get("reason", "")
                approved_matches.append(candidate)
            else:
                candidate.approved = False
                rejected_matches.append(candidate)

        logger.info(f"LLM approved {len(approved_matches)}/{len(candidates)} merges")

        return {
            "approved_matches": approved_matches,
            "rejected_matches": rejected_matches,
            "confirmed_reference_matches": result.get("confirmed_identity_matches", []),
            "llm_validated": True
        }

    except Exception as e:
        logger.error(f"LLM validation failed: {e}")
        # Fall back to approving heuristics
        for c in candidates:
            c.approved = True
        return {
            "approved_matches": candidates,
            "rejected_matches": [],
            "confirmed_reference_matches": reference_matches or [],
            "llm_validated": False,
            "error": str(e)
        }


# =============================================================================
# High-Level API
# =============================================================================

async def match_and_merge_names(
    names: List[str],
    context: str = "",
    reference_name: Optional[str] = None,
    validate_with_llm: bool = True
) -> NameMatchResult:
    """
    Complete pipeline: find candidates with heuristics, validate with LLM.

    Args:
        names: List of names to match/merge
        context: Context for LLM (e.g., "academic author names")
        reference_name: Optional reference for identity matching
        validate_with_llm: Whether to validate with LLM (default True)

    Returns:
        NameMatchResult with approved matches and unmatched names
    """
    if not names:
        return NameMatchResult(matches=[], unmatched=[])

    # Find candidates with heuristics
    candidates = find_match_candidates(names)

    # Find reference matches if provided
    reference_matches = []
    if reference_name:
        for name in names:
            is_match, conf = check_name_against_reference(name, reference_name)
            if is_match:
                reference_matches.append(name)

    # Validate with LLM
    if validate_with_llm and (candidates or reference_matches):
        validation = await validate_matches_with_llm(
            candidates=candidates,
            context=context,
            reference_name=reference_name,
            reference_matches=reference_matches
        )

        approved = validation["approved_matches"]
        confirmed_ref = set(validation.get("confirmed_reference_matches", []))
        llm_validated = validation.get("llm_validated", False)
        error = validation.get("error")
    else:
        # Just use heuristics
        approved = candidates
        for c in approved:
            c.approved = True
        confirmed_ref = set(reference_matches)
        llm_validated = False
        error = None

    # Figure out unmatched names
    matched_names = set()
    for match in approved:
        matched_names.update(match.variants)

    unmatched = [n for n in names if n not in matched_names]

    # Add reference match info to approved matches
    for match in approved:
        if any(v in confirmed_ref for v in match.variants):
            match.reason = (match.reason + " [REFERENCE MATCH]").strip()

    return NameMatchResult(
        matches=approved,
        unmatched=unmatched,
        llm_validated=llm_validated,
        error=error
    )
