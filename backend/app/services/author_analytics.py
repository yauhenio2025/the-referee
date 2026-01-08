"""
Author Analytics Service

Uses heuristics to:
- Detect self-citations (comparing citing authors with thinker name)
- Disaggregate multi-author strings into individual authors
- Normalize author name variants (WW Gasparski = W Gasparski)
"""
import logging
import re
from typing import Dict, Any, List, Set, Tuple
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Normalize an author name for comparison."""
    # Remove extra whitespace
    name = " ".join(name.split())
    # Remove periods after initials
    name = re.sub(r'\.(?=\s|$)', '', name)
    # Remove 'et al' variations
    name = re.sub(r'\s*et\s*al\.?\s*$', '', name, flags=re.IGNORECASE)
    return name.strip()


def expand_initials(name: str) -> List[str]:
    """Extract possible surname and initials from a name."""
    parts = name.split()
    if not parts:
        return []

    # Last part is usually surname
    surname = parts[-1] if parts else ""
    initials = [p[0].upper() for p in parts[:-1] if p]

    return [surname.lower()] + initials


def names_match(name1: str, name2: str, threshold: float = 0.7) -> bool:
    """Check if two author names likely refer to the same person."""
    n1 = normalize_name(name1).lower()
    n2 = normalize_name(name2).lower()

    # Exact match
    if n1 == n2:
        return True

    # Extract surname and initials
    parts1 = expand_initials(name1)
    parts2 = expand_initials(name2)

    if not parts1 or not parts2:
        return False

    # Same surname
    if parts1[0] != parts2[0]:
        # Check if surnames are similar (typos, transliteration)
        if SequenceMatcher(None, parts1[0], parts2[0]).ratio() < 0.8:
            return False

    # Check initials - one should be subset of other or match
    initials1 = set(parts1[1:])
    initials2 = set(parts2[1:])

    # If both have initials, check they don't conflict
    if initials1 and initials2:
        # At least one initial should match
        if not initials1 & initials2:
            return False

    return True


def is_self_citation(author_name: str, thinker_name: str) -> Tuple[bool, float]:
    """Check if an author name is likely the thinker (self-citation)."""
    author_norm = normalize_name(author_name).lower()
    thinker_norm = normalize_name(thinker_name).lower()

    # Direct match
    if author_norm == thinker_norm:
        return True, 1.0

    # Extract parts
    author_parts = expand_initials(author_name)
    thinker_parts = thinker_name.lower().split()

    if not author_parts or not thinker_parts:
        return False, 0.0

    # Get thinker's surname (last part)
    thinker_surname = thinker_parts[-1]
    author_surname = author_parts[0]

    # Surname must match
    if thinker_surname != author_surname:
        if SequenceMatcher(None, thinker_surname, author_surname).ratio() < 0.85:
            return False, 0.0

    # Check if initials match thinker's first names
    author_initials = set(author_parts[1:])
    thinker_initials = set(p[0].upper() for p in thinker_parts[:-1] if p)

    if author_initials and thinker_initials:
        # Initials should be subset or match
        if author_initials <= thinker_initials or thinker_initials <= author_initials:
            return True, 0.9
        # At least some overlap
        if author_initials & thinker_initials:
            return True, 0.7
    elif not author_initials:
        # Just surname match
        return True, 0.6

    return False, 0.0


def split_authors(author_string: str) -> List[str]:
    """Split a multi-author string into individual authors."""
    # Common separators: comma, semicolon, " and ", " & "
    # But be careful with "J Smith, Jr" patterns

    # First split by common separators
    authors = re.split(r'\s*[,;]\s*|\s+and\s+|\s*&\s*', author_string)

    # Filter out empty strings and trim
    authors = [a.strip() for a in authors if a.strip()]

    # Filter out things that look like suffixes (Jr, III, etc)
    authors = [a for a in authors if not re.match(r'^(Jr\.?|Sr\.?|I{1,3}|IV|V)$', a, re.IGNORECASE)]

    return authors if authors else [author_string]


async def process_citing_authors(
    thinker_name: str,
    raw_author_groups: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Process raw author groups using heuristics to:
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
    if not raw_author_groups:
        return {"individual_authors": [], "llm_processed": True}

    logger.info(f"Processing {len(raw_author_groups)} author groups for thinker: {thinker_name}")

    # Step 1: Disaggregate multi-author entries
    individual_entries = []  # (name, citation_count, papers_count, citation_ids, original_idx)

    for idx, group in enumerate(raw_author_groups):
        author_string = group.get("authors", "Unknown")
        citation_count = group.get("citation_count", 0)
        papers_count = group.get("papers_count", 0)
        citation_ids = group.get("citation_ids", [])

        # Split into individual authors
        authors = split_authors(author_string)

        for author in authors:
            individual_entries.append({
                "name": normalize_name(author),
                "citation_count": citation_count,
                "papers_count": papers_count,
                "citation_ids": citation_ids,
                "original_idx": idx
            })

    # Step 2: Merge variants of the same person
    merged_authors = {}  # normalized_key -> merged data

    for entry in individual_entries:
        name = entry["name"]

        # Find if this matches an existing author
        matched_key = None
        for existing_key in merged_authors:
            if names_match(name, existing_key):
                matched_key = existing_key
                break

        if matched_key:
            # Merge with existing
            merged = merged_authors[matched_key]
            merged["citation_count"] += entry["citation_count"]
            merged["papers_count"] += entry["papers_count"]
            merged["citation_ids"].extend(entry["citation_ids"])
            merged["variants"].add(name)
            merged["source_indices"].add(entry["original_idx"])
            # Keep longer name as normalized
            if len(name) > len(merged["normalized_name"]):
                merged["normalized_name"] = name
        else:
            # New author
            merged_authors[name] = {
                "normalized_name": name,
                "citation_count": entry["citation_count"],
                "papers_count": entry["papers_count"],
                "citation_ids": list(entry["citation_ids"]),
                "variants": {name},
                "source_indices": {entry["original_idx"]}
            }

    # Step 3: Detect self-citations and build final list
    individual_authors = []

    for key, data in merged_authors.items():
        is_self, confidence = is_self_citation(data["normalized_name"], thinker_name)

        individual_authors.append({
            "normalized_name": data["normalized_name"],
            "is_self_citation": is_self,
            "confidence": confidence,
            "total_citation_count": data["citation_count"],
            "total_papers_count": data["papers_count"],
            "citation_ids": list(set(data["citation_ids"])),  # dedupe
            "merged_from": list(data["variants"]),
            "source_entry_ids": list(data["source_indices"])
        })

    # Sort by citation count
    individual_authors.sort(key=lambda x: x["total_citation_count"], reverse=True)

    logger.info(f"Processed {len(raw_author_groups)} groups into {len(individual_authors)} individual authors")

    return {
        "individual_authors": individual_authors,
        "llm_processed": True  # Using heuristics, but compatible with existing code
    }
