"""
Author Analytics Service

Uses the name_matcher module for intelligent author merging and self-citation detection.
"""
import logging
from typing import Dict, Any, List

from .name_matcher import (
    normalize_name,
    split_author_string,
    find_match_candidates,
    check_name_against_reference,
    validate_matches_with_llm,
)

logger = logging.getLogger(__name__)


async def process_citing_authors(
    thinker_name: str,
    raw_author_groups: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Process citing authors with heuristic candidate generation + LLM validation.

    Uses name_matcher module for the heavy lifting.
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

        authors = split_author_string(author_string)

        for author in authors:
            individual_entries.append({
                "name": normalize_name(author),
                "citation_count": citation_count,
                "papers_count": papers_count,
                "citation_ids": citation_ids,
                "original_idx": idx
            })

    # Step 2: Extract unique names for matching
    unique_names = list(set(e["name"] for e in individual_entries))

    # Step 3: Find merge candidates with heuristics
    merge_candidates = find_match_candidates(unique_names)

    # Step 4: Find self-citation candidates
    self_citation_candidates = []
    for name in unique_names:
        is_match, conf = check_name_against_reference(name, thinker_name)
        if is_match:
            self_citation_candidates.append(name)

    # Step 5: Validate with LLM
    validation = await validate_matches_with_llm(
        candidates=merge_candidates,
        context="academic citation analysis",
        reference_name=thinker_name,
        reference_matches=self_citation_candidates
    )

    # Step 6: Build merge map from approved matches
    variant_to_canonical = {}
    for match in validation.get("approved_matches", []):
        canonical = match.canonical
        for v in match.variants:
            variant_to_canonical[v.lower()] = canonical

    confirmed_self = set(
        s.lower() for s in validation.get("confirmed_reference_matches", [])
    )

    # Step 7: Aggregate with approved merges
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

    # Step 8: Build final list with self-citation flags
    individual_authors = []

    for canonical, data in merged_authors.items():
        # Check if any variant is a confirmed self-citation
        is_self = any(v.lower() in confirmed_self for v in data["variants"])
        is_self = is_self or canonical.lower() in confirmed_self

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

    llm_validated = validation.get("llm_validated", False)
    logger.info(f"Processed {len(raw_author_groups)} groups into {len(individual_authors)} authors "
               f"(LLM validated: {llm_validated})")

    return {
        "individual_authors": individual_authors,
        "llm_processed": llm_validated,
        "error": validation.get("error")
    }
