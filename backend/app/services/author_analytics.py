"""
Author Analytics Service

Two-stage approach:
1. Heuristics generate merge candidates (fast)
2. Claude Sonnet 4.5 validates/approves merges (intelligent)
"""
import logging
import json
import re
from typing import Dict, Any, List, Set, Tuple
from difflib import SequenceMatcher
import anthropic

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def normalize_name(name: str) -> str:
    """Normalize an author name for comparison."""
    name = " ".join(name.split())
    name = re.sub(r'\.(?=\s|$)', '', name)
    name = re.sub(r'\s*et\s*al\.?\s*$', '', name, flags=re.IGNORECASE)
    return name.strip()


def expand_initials(name: str) -> List[str]:
    """Extract possible surname and initials from a name."""
    parts = name.split()
    if not parts:
        return []
    surname = parts[-1] if parts else ""
    initials = [p[0].upper() for p in parts[:-1] if p]
    return [surname.lower()] + initials


def names_might_match(name1: str, name2: str) -> bool:
    """Check if two names MIGHT refer to same person (for candidate generation)."""
    n1 = normalize_name(name1).lower()
    n2 = normalize_name(name2).lower()

    if n1 == n2:
        return True

    parts1 = expand_initials(name1)
    parts2 = expand_initials(name2)

    if not parts1 or not parts2:
        return False

    # Surnames must be similar
    if SequenceMatcher(None, parts1[0], parts2[0]).ratio() < 0.8:
        return False

    # Check initials overlap
    initials1 = set(parts1[1:])
    initials2 = set(parts2[1:])

    if initials1 and initials2:
        if not initials1 & initials2:
            return False

    return True


def is_likely_self_citation(author_name: str, thinker_name: str) -> bool:
    """Quick check if author might be the thinker."""
    author_parts = expand_initials(author_name)
    thinker_parts = thinker_name.lower().split()

    if not author_parts or not thinker_parts:
        return False

    thinker_surname = thinker_parts[-1]
    author_surname = author_parts[0]

    if SequenceMatcher(None, thinker_surname, author_surname).ratio() < 0.85:
        return False

    return True


def split_authors(author_string: str) -> List[str]:
    """Split a multi-author string into individual authors."""
    authors = re.split(r'\s*[,;]\s*|\s+and\s+|\s*&\s*', author_string)
    authors = [a.strip() for a in authors if a.strip()]
    authors = [a for a in authors if not re.match(r'^(Jr\.?|Sr\.?|I{1,3}|IV|V)$', a, re.IGNORECASE)]
    return authors if authors else [author_string]


async def validate_merges_with_llm(
    thinker_name: str,
    merge_candidates: List[Dict],
    self_citation_candidates: List[str]
) -> Dict[str, Any]:
    """
    Use Claude Sonnet to validate merge candidates and self-citation detection.

    Returns dict with:
    - approved_merges: list of {"canonical": str, "variants": list}
    - self_citations: list of author names confirmed as self-citations
    """
    if not settings.anthropic_api_key:
        logger.warning("No Anthropic API key - approving all heuristic suggestions")
        return {
            "approved_merges": merge_candidates,
            "self_citations": self_citation_candidates,
            "llm_validated": False
        }

    try:
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        prompt = f"""You are validating author name merges for citation analysis.

THINKER: "{thinker_name}"

TASK 1 - VALIDATE MERGE CANDIDATES:
These name pairs were flagged as potentially the same person. Approve or reject each.

{json.dumps(merge_candidates, indent=2)}

TASK 2 - VALIDATE SELF-CITATIONS:
These authors were flagged as potentially being the thinker "{thinker_name}". Confirm or reject.

{json.dumps(self_citation_candidates, indent=2)}

RESPOND WITH VALID JSON ONLY:
{{
  "approved_merges": [
    {{"canonical": "Best Name Form", "variants": ["variant1", "variant2"], "reason": "brief reason"}}
  ],
  "rejected_merges": [
    {{"variants": ["name1", "name2"], "reason": "why these are different people"}}
  ],
  "confirmed_self_citations": ["name1", "name2"],
  "rejected_self_citations": ["name3"]
}}

Rules:
- For merges: approve if clearly same person, reject if possibly different people
- For self-citations: confirm if author is clearly the thinker, reject if uncertain
- Use the most complete/formal name as canonical
- Be conservative - when in doubt, reject"""

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
        result["llm_validated"] = True

        logger.info(f"LLM approved {len(result.get('approved_merges', []))} merges, "
                   f"confirmed {len(result.get('confirmed_self_citations', []))} self-citations")

        return result

    except Exception as e:
        logger.error(f"LLM validation failed: {e}")
        # Fall back to approving heuristic suggestions
        return {
            "approved_merges": merge_candidates,
            "self_citations": self_citation_candidates,
            "llm_validated": False,
            "error": str(e)
        }


async def process_citing_authors(
    thinker_name: str,
    raw_author_groups: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Process citing authors with heuristic candidate generation + LLM validation.
    """
    if not raw_author_groups:
        return {"individual_authors": [], "llm_processed": True}

    logger.info(f"Processing {len(raw_author_groups)} author groups for thinker: {thinker_name}")

    # Step 1: Disaggregate multi-author entries
    individual_entries = []

    for idx, group in enumerate(raw_author_groups):
        author_string = group.get("authors", "Unknown")
        citation_count = group.get("citation_count", 0)
        papers_count = group.get("papers_count", 0)
        citation_ids = group.get("citation_ids", [])

        authors = split_authors(author_string)

        for author in authors:
            individual_entries.append({
                "name": normalize_name(author),
                "citation_count": citation_count,
                "papers_count": papers_count,
                "citation_ids": citation_ids,
                "original_idx": idx
            })

    # Step 2: Generate merge candidates using heuristics
    merge_candidates = []
    self_citation_candidates = []
    processed_names = set()

    for i, entry1 in enumerate(individual_entries):
        name1 = entry1["name"]
        if name1 in processed_names:
            continue

        variants = [name1]

        for j, entry2 in enumerate(individual_entries):
            if i >= j:
                continue
            name2 = entry2["name"]
            if name2 in processed_names:
                continue

            if names_might_match(name1, name2):
                variants.append(name2)
                processed_names.add(name2)

        processed_names.add(name1)

        if len(variants) > 1:
            # Pick longest as canonical candidate
            canonical = max(variants, key=len)
            merge_candidates.append({
                "canonical": canonical,
                "variants": variants
            })

        # Check for self-citation
        if is_likely_self_citation(name1, thinker_name):
            self_citation_candidates.append(name1)

    # Step 3: Validate with LLM
    validation = await validate_merges_with_llm(
        thinker_name, merge_candidates, self_citation_candidates
    )

    # Step 4: Apply approved merges
    approved_merges = {
        tuple(sorted(m["variants"])): m["canonical"]
        for m in validation.get("approved_merges", [])
    }
    confirmed_self = set(validation.get("confirmed_self_citations", validation.get("self_citations", [])))

    # Build merge map: variant -> canonical
    variant_to_canonical = {}
    for variants, canonical in approved_merges.items():
        for v in variants:
            variant_to_canonical[v.lower()] = canonical

    # Aggregate with approved merges
    merged_authors = {}

    for entry in individual_entries:
        name = entry["name"]
        canonical = variant_to_canonical.get(name.lower(), name)

        if canonical not in merged_authors:
            merged_authors[canonical] = {
                "normalized_name": canonical,
                "citation_count": 0,
                "papers_count": 0,
                "citation_ids": [],
                "variants": set(),
                "source_indices": set()
            }

        merged = merged_authors[canonical]
        merged["citation_count"] += entry["citation_count"]
        merged["papers_count"] += entry["papers_count"]
        merged["citation_ids"].extend(entry["citation_ids"])
        merged["variants"].add(name)
        merged["source_indices"].add(entry["original_idx"])

    # Step 5: Build final list with self-citation flags
    individual_authors = []

    for canonical, data in merged_authors.items():
        # Check if any variant is a confirmed self-citation
        is_self = any(v.lower() in [s.lower() for s in confirmed_self] for v in data["variants"])
        is_self = is_self or canonical.lower() in [s.lower() for s in confirmed_self]

        individual_authors.append({
            "normalized_name": data["normalized_name"],
            "is_self_citation": is_self,
            "confidence": 0.9 if is_self else 0.0,
            "total_citation_count": data["citation_count"],
            "total_papers_count": data["papers_count"],
            "citation_ids": list(set(data["citation_ids"])),
            "merged_from": list(data["variants"]),
            "source_entry_ids": list(data["source_indices"])
        })

    individual_authors.sort(key=lambda x: x["total_citation_count"], reverse=True)

    logger.info(f"Processed {len(raw_author_groups)} groups into {len(individual_authors)} authors "
               f"(LLM validated: {validation.get('llm_validated', False)})")

    return {
        "individual_authors": individual_authors,
        "llm_processed": validation.get("llm_validated", False),
        "error": validation.get("error")
    }
