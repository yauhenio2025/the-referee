"""
Overflow Harvester Service - Partition Strategy

Handles the case when a single year has >1000 citations, exceeding Google Scholar's
limit per query. Uses a PARTITION strategy to guarantee complete coverage:

Strategy:
1. Detect overflow (>1000 results for a year)
2. LLM suggests common terms to exclude based on the topic/domain
3. Keep adding -intitle:"term" exclusions until result count < 1000
4. Harvest the exclusion set (items WITHOUT those terms)
5. Build OR inclusion query: intitle:"term1" OR intitle:"term2" OR ...
6. Harvest the inclusion set (items WITH at least one term)
7. If inclusion set >1000, recursively partition it

This guarantees: exclusion_set + inclusion_set = all_items
"""
import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Dict, Any, List, Set, Optional, Callable
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Citation

logger = logging.getLogger(__name__)

# Constants
GOOGLE_SCHOLAR_LIMIT = 1000
TARGET_THRESHOLD = 950  # Aim to get below this to have safety margin
LLM_MODEL = "claude-sonnet-4-5-20250929"


def log_now(msg: str, level: str = "info"):
    """Log message and immediately flush to stdout"""
    import sys
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    print(f"{timestamp} | overflow | {level.upper()} | {msg}", flush=True)
    sys.stdout.flush()


async def get_result_count_for_query(
    scholar_service,
    scholar_id: str,
    year: int,
    query_suffix: str = ""
) -> int:
    """
    Get the result count for a query WITHOUT harvesting.
    Makes a single page request to get the total count.
    """
    # Use the scholar service to make a single request
    # We'll fetch just page 0 with max_results=10 to get the count
    result = await scholar_service.get_cited_by(
        scholar_id=scholar_id,
        max_results=10,  # Just get first page
        year_low=year,
        year_high=year,
        additional_query=query_suffix if query_suffix else None,
    )

    return result.get('totalResults', 0) if isinstance(result, dict) else 0


async def suggest_exclusion_terms_llm(
    edition_title: str,
    year: int,
    current_count: int,
    already_excluded: List[str] = None
) -> List[str]:
    """
    Use LLM to suggest terms to exclude from titles to reduce result count.

    Returns list of terms to try excluding, ordered by expected impact.
    """
    import anthropic
    import os

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log_now("No ANTHROPIC_API_KEY - using fallback terms", "warning")
        return get_fallback_exclusion_terms(edition_title)

    client = anthropic.Anthropic(api_key=api_key)

    already_excluded = already_excluded or []
    excluded_str = ", ".join([f'"{t}"' for t in already_excluded]) if already_excluded else "none yet"

    prompt = f"""You are helping harvest academic citations from Google Scholar. We have a paper with {current_count} citations in year {year}, exceeding the 1000 result limit. We need to partition the results by excluding common title terms.

SEED PAPER: "{edition_title}"
YEAR: {year}
CURRENT RESULT COUNT: {current_count}
TERMS ALREADY EXCLUDED: {excluded_str}

YOUR TASK: Suggest 10-15 single-word terms that are likely to appear frequently in titles of papers citing this work. These should be:

1. Common academic/domain terms related to the paper's topic
2. Generic scholarly terms (like "analysis", "theory", "study")
3. Key concepts from the paper's domain

We will use these as -intitle:"term" exclusions to reduce the result count below 1000.

IMPORTANT:
- Return ONLY single words (no phrases)
- Return terms that are NOT already excluded
- Order by expected frequency (most common first)
- Include both domain-specific and generic academic terms

OUTPUT FORMAT: Return a JSON array of strings, nothing else.
Example: ["corporate", "governance", "firm", "organization", "management", "theory", "analysis", "study", "business", "market"]

Return ONLY the JSON array:"""

    try:
        log_now(f"Calling LLM for exclusion term suggestions...")

        response = client.messages.create(
            model=LLM_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text.strip()

        # Extract JSON from response
        json_match = re.search(r'\[.*?\]', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)

        terms = json.loads(response_text)

        # Filter out already excluded terms
        terms = [t for t in terms if t.lower() not in [e.lower() for e in already_excluded]]

        log_now(f"LLM suggested {len(terms)} terms: {terms[:5]}...")
        return terms

    except Exception as e:
        log_now(f"LLM term suggestion failed: {e}, using fallback", "warning")
        return get_fallback_exclusion_terms(edition_title, already_excluded)


def get_fallback_exclusion_terms(edition_title: str, already_excluded: List[str] = None) -> List[str]:
    """Fallback terms when LLM is unavailable."""
    already_excluded = set(t.lower() for t in (already_excluded or []))

    # Generic academic terms that appear in many titles
    generic_terms = [
        "analysis", "study", "theory", "research", "review", "approach",
        "model", "framework", "perspective", "evidence", "impact", "effects",
        "role", "case", "empirical", "development", "performance", "management",
        "relationship", "strategy", "value", "market", "social", "economic",
        "political", "organizational", "institutional", "financial", "corporate",
        "governance", "firm", "business", "industry", "policy", "regulation"
    ]

    # Extract potential domain terms from the title
    title_words = re.findall(r'\b[a-zA-Z]{4,}\b', edition_title.lower())
    domain_terms = [w for w in title_words if w not in {'the', 'and', 'for', 'with', 'from'}]

    # Combine and filter
    all_terms = domain_terms[:10] + generic_terms
    filtered = [t for t in all_terms if t.lower() not in already_excluded]

    return filtered[:20]


async def find_exclusion_set(
    scholar_service,
    scholar_id: str,
    year: int,
    edition_title: str,
    initial_count: int,
    max_iterations: int = 20
) -> Dict[str, Any]:
    """
    Find a set of exclusion terms that brings the result count below 1000.

    Returns:
        {
            "excluded_terms": ["term1", "term2", ...],
            "exclusion_query": "-intitle:\"term1\" -intitle:\"term2\" ...",
            "result_count": 892,
            "success": True
        }
    """
    log_now(f"Finding exclusion set for year {year} (initial count: {initial_count})")

    excluded_terms = []
    current_count = initial_count

    # Get initial term suggestions from LLM
    suggested_terms = await suggest_exclusion_terms_llm(
        edition_title, year, current_count, excluded_terms
    )

    term_index = 0

    for iteration in range(max_iterations):
        if current_count < TARGET_THRESHOLD:
            log_now(f"✓ Achieved target: {current_count} < {TARGET_THRESHOLD}")
            break

        # Get next term to try
        if term_index >= len(suggested_terms):
            # Need more terms from LLM
            log_now(f"Requesting more terms from LLM...")
            more_terms = await suggest_exclusion_terms_llm(
                edition_title, year, current_count, excluded_terms
            )
            if not more_terms:
                log_now(f"No more terms available, stopping at {current_count}")
                break
            suggested_terms.extend(more_terms)

        next_term = suggested_terms[term_index]
        term_index += 1

        # Build test query with this term added
        test_excluded = excluded_terms + [next_term]
        test_query = " ".join([f'-intitle:"{t}"' for t in test_excluded])

        # Get result count
        try:
            new_count = await get_result_count_for_query(
                scholar_service, scholar_id, year, test_query
            )

            reduction = current_count - new_count
            log_now(f"  [{iteration+1}] Adding -{next_term}: {current_count} → {new_count} (reduction: {reduction})")

            if new_count < current_count:
                # Term was useful, keep it
                excluded_terms.append(next_term)
                current_count = new_count
            else:
                log_now(f"    Skipping '{next_term}' - no reduction")

            # Rate limit
            await asyncio.sleep(1)

        except Exception as e:
            log_now(f"    Error testing '{next_term}': {e}", "warning")
            continue

    exclusion_query = " ".join([f'-intitle:"{t}"' for t in excluded_terms])

    return {
        "excluded_terms": excluded_terms,
        "exclusion_query": exclusion_query,
        "result_count": current_count,
        "success": current_count < GOOGLE_SCHOLAR_LIMIT
    }


def build_inclusion_query(excluded_terms: List[str]) -> str:
    """Build an OR query to match items containing any of the excluded terms."""
    if not excluded_terms:
        return ""
    return " OR ".join([f'intitle:"{t}"' for t in excluded_terms])


async def harvest_partition(
    scholar_service,
    db: AsyncSession,
    edition_id: int,
    scholar_id: str,
    year: int,
    edition_title: str,
    paper_id: int,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    total_for_year: int,
    depth: int = 0,
    max_depth: int = 3
) -> Dict[str, Any]:
    """
    Recursively partition and harvest citations for a year with >1000 results.

    Uses the exclusion/inclusion partition strategy:
    1. Find terms to exclude until count < 1000
    2. Harvest exclusion set (items WITHOUT those terms)
    3. Harvest inclusion set (items WITH at least one term)
    4. If inclusion set > 1000, recursively partition
    """
    indent = "  " * depth
    log_now(f"{indent}═══ PARTITION depth={depth}, year={year}, total={total_for_year} ═══")

    if depth >= max_depth:
        log_now(f"{indent}Max depth reached, harvesting what we can", "warning")
        # Just harvest up to 1000
        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=GOOGLE_SCHOLAR_LIMIT,
            year_low=year,
            year_high=year,
            on_page_complete=on_page_complete,
        )
        return {
            "depth": depth,
            "harvested": result.get('pages_fetched', 0) * 10,
            "truncated": True
        }

    stats = {
        "depth": depth,
        "year": year,
        "initial_count": total_for_year,
        "exclusion_harvested": 0,
        "inclusion_harvested": 0,
        "total_new": 0,
        "partitions": []
    }

    # Step 1: Find exclusion terms to get below 1000
    log_now(f"{indent}Step 1: Finding exclusion set...")
    exclusion_result = await find_exclusion_set(
        scholar_service, scholar_id, year, edition_title, total_for_year
    )

    if not exclusion_result["success"]:
        log_now(f"{indent}Could not reduce below 1000, harvesting partial", "warning")
        # Harvest what we can with current exclusions
        if exclusion_result["exclusion_query"]:
            result = await scholar_service.get_cited_by(
                scholar_id=scholar_id,
                max_results=GOOGLE_SCHOLAR_LIMIT,
                year_low=year,
                year_high=year,
                additional_query=exclusion_result["exclusion_query"],
                on_page_complete=on_page_complete,
            )
            stats["exclusion_harvested"] = result.get('pages_fetched', 0) * 10
        return stats

    excluded_terms = exclusion_result["excluded_terms"]
    exclusion_query = exclusion_result["exclusion_query"]

    log_now(f"{indent}Exclusion set: {len(excluded_terms)} terms, {exclusion_result['result_count']} results")
    log_now(f"{indent}Terms: {excluded_terms}")

    # Step 2: Harvest the EXCLUSION set (items WITHOUT those terms)
    log_now(f"{indent}Step 2: Harvesting exclusion set ({exclusion_result['result_count']} items)...")

    start_count = len(existing_scholar_ids)

    result = await scholar_service.get_cited_by(
        scholar_id=scholar_id,
        max_results=GOOGLE_SCHOLAR_LIMIT,
        year_low=year,
        year_high=year,
        additional_query=exclusion_query,
        on_page_complete=on_page_complete,
    )

    exclusion_new = len(existing_scholar_ids) - start_count
    stats["exclusion_harvested"] = exclusion_new
    log_now(f"{indent}✓ Exclusion set harvested: {exclusion_new} new citations")

    # Rate limit between phases
    await asyncio.sleep(3)

    # Step 3: Build and check INCLUSION query (items WITH at least one term)
    inclusion_query = build_inclusion_query(excluded_terms)
    log_now(f"{indent}Step 3: Checking inclusion set...")
    log_now(f"{indent}Query: {inclusion_query[:100]}...")

    inclusion_count = await get_result_count_for_query(
        scholar_service, scholar_id, year, inclusion_query
    )

    log_now(f"{indent}Inclusion set has {inclusion_count} results")

    # Step 4: Handle inclusion set
    if inclusion_count == 0:
        log_now(f"{indent}Inclusion set empty, done!")

    elif inclusion_count < GOOGLE_SCHOLAR_LIMIT:
        # Can harvest directly
        log_now(f"{indent}Step 4: Harvesting inclusion set ({inclusion_count} items)...")

        start_count = len(existing_scholar_ids)

        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=GOOGLE_SCHOLAR_LIMIT,
            year_low=year,
            year_high=year,
            additional_query=inclusion_query,
            on_page_complete=on_page_complete,
        )

        inclusion_new = len(existing_scholar_ids) - start_count
        stats["inclusion_harvested"] = inclusion_new
        log_now(f"{indent}✓ Inclusion set harvested: {inclusion_new} new citations")

    else:
        # Inclusion set also >1000, need to recursively partition
        log_now(f"{indent}Step 4: Inclusion set too large ({inclusion_count}), recursively partitioning...")

        # For recursive partition, we need to work within the inclusion set
        # We'll add the inclusion query as a base and then add more exclusions
        sub_result = await harvest_partition_with_base(
            scholar_service=scholar_service,
            db=db,
            edition_id=edition_id,
            scholar_id=scholar_id,
            year=year,
            edition_title=edition_title,
            paper_id=paper_id,
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            total_for_year=inclusion_count,
            base_query=inclusion_query,
            depth=depth + 1,
            max_depth=max_depth
        )

        stats["partitions"].append(sub_result)
        stats["inclusion_harvested"] = sub_result.get("total_new", 0)

    stats["total_new"] = stats["exclusion_harvested"] + stats["inclusion_harvested"]

    log_now(f"{indent}═══ PARTITION COMPLETE: {stats['total_new']} total new citations ═══")

    return stats


async def harvest_partition_with_base(
    scholar_service,
    db: AsyncSession,
    edition_id: int,
    scholar_id: str,
    year: int,
    edition_title: str,
    paper_id: int,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    total_for_year: int,
    base_query: str,
    depth: int = 0,
    max_depth: int = 3
) -> Dict[str, Any]:
    """
    Partition within an existing query constraint (for recursive partitioning).
    Similar to harvest_partition but adds exclusions ON TOP of base_query.
    """
    indent = "  " * depth
    log_now(f"{indent}═══ SUB-PARTITION depth={depth}, total={total_for_year} ═══")
    log_now(f"{indent}Base query: {base_query[:80]}...")

    if depth >= max_depth:
        log_now(f"{indent}Max depth reached, harvesting what we can", "warning")
        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=GOOGLE_SCHOLAR_LIMIT,
            year_low=year,
            year_high=year,
            additional_query=base_query,
            on_page_complete=on_page_complete,
        )
        return {"depth": depth, "harvested": result.get('pages_fetched', 0) * 10, "truncated": True}

    stats = {
        "depth": depth,
        "initial_count": total_for_year,
        "exclusion_harvested": 0,
        "inclusion_harvested": 0,
        "total_new": 0
    }

    # Find additional exclusion terms
    # Get terms to exclude within this subset
    additional_terms = await suggest_exclusion_terms_llm(
        edition_title, year, total_for_year, []
    )

    # Test adding exclusions until we're below 1000
    excluded_terms = []
    current_count = total_for_year

    for term in additional_terms:
        if current_count < TARGET_THRESHOLD:
            break

        test_exclusions = " ".join([f'-intitle:"{t}"' for t in excluded_terms + [term]])
        test_query = f"{base_query} {test_exclusions}"

        try:
            new_count = await get_result_count_for_query(
                scholar_service, scholar_id, year, test_query
            )

            if new_count < current_count:
                excluded_terms.append(term)
                current_count = new_count
                log_now(f"{indent}  Adding -{term}: {current_count}")

            await asyncio.sleep(1)

        except Exception as e:
            log_now(f"{indent}  Error: {e}", "warning")
            continue

    if current_count >= GOOGLE_SCHOLAR_LIMIT:
        log_now(f"{indent}Could not reduce below 1000, harvesting partial", "warning")
        # Just harvest what we can
        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=GOOGLE_SCHOLAR_LIMIT,
            year_low=year,
            year_high=year,
            additional_query=base_query,
            on_page_complete=on_page_complete,
        )
        stats["total_new"] = len(existing_scholar_ids)
        return stats

    # Harvest exclusion set within base query
    exclusion_additions = " ".join([f'-intitle:"{t}"' for t in excluded_terms])
    full_exclusion_query = f"{base_query} {exclusion_additions}"

    log_now(f"{indent}Harvesting exclusion subset ({current_count} items)...")
    start_count = len(existing_scholar_ids)

    result = await scholar_service.get_cited_by(
        scholar_id=scholar_id,
        max_results=GOOGLE_SCHOLAR_LIMIT,
        year_low=year,
        year_high=year,
        additional_query=full_exclusion_query,
        on_page_complete=on_page_complete,
    )

    stats["exclusion_harvested"] = len(existing_scholar_ids) - start_count

    await asyncio.sleep(3)

    # Harvest inclusion subset (items matching base_query AND containing excluded terms)
    if excluded_terms:
        inclusion_part = " OR ".join([f'intitle:"{t}"' for t in excluded_terms])
        # We need items that match base_query AND have at least one excluded term
        # Since base_query is already an OR of terms, we need to be careful
        # Actually we can just harvest the original base_query and deduplicate

        log_now(f"{indent}Harvesting remaining from base query...")
        start_count = len(existing_scholar_ids)

        # This will get duplicates but they'll be filtered by existing_scholar_ids
        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=GOOGLE_SCHOLAR_LIMIT,
            year_low=year,
            year_high=year,
            additional_query=base_query,  # Just the base, let dedup handle it
            on_page_complete=on_page_complete,
        )

        stats["inclusion_harvested"] = len(existing_scholar_ids) - start_count

    stats["total_new"] = stats["exclusion_harvested"] + stats["inclusion_harvested"]

    return stats


async def detect_and_handle_overflow(
    scholar_service,
    db: AsyncSession,
    edition_id: int,
    scholar_id: str,
    year: int,
    edition_title: str,
    paper_id: int,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    year_result: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Check if a year has overflow and handle it using partition strategy.

    Called after the initial year fetch completes.
    """
    papers_fetched = year_result.get("pages_fetched", 0) * 10
    total_results = year_result.get("totalResults", 0)

    # Check if there's overflow
    if papers_fetched < 950 or total_results <= papers_fetched:
        # No overflow, we got everything
        return None

    log_now(f"🚨 OVERFLOW DETECTED: Year {year} has {total_results} citations, only fetched {papers_fetched}")

    # Use partition strategy to get the rest
    return await harvest_partition(
        scholar_service=scholar_service,
        db=db,
        edition_id=edition_id,
        scholar_id=scholar_id,
        year=year,
        edition_title=edition_title,
        paper_id=paper_id,
        existing_scholar_ids=existing_scholar_ids,
        on_page_complete=on_page_complete,
        total_for_year=total_results,
    )
