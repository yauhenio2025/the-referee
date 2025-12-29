"""
Overflow Harvester Service - Partition Strategy with FULL TRACEABILITY

Handles the case when a single year has >1000 citations, exceeding Google Scholar's
limit per query. Uses a PARTITION strategy to guarantee complete coverage.

CRITICAL: This module now persists EVERYTHING to the database:
- Every partition attempt (PartitionRun)
- Every term we try to exclude (PartitionTermAttempt)
- Every query we execute (PartitionQuery)
- Every LLM call for term suggestions (PartitionLLMCall)

Strategy:
1. Detect overflow (>1000 results for a year)
2. Create PartitionRun record (status: pending)
3. LLM suggests common terms to exclude - ALL calls logged to PartitionLLMCall
4. Test each term with -intitle:"term" - EACH test logged to PartitionTermAttempt
5. CRITICAL: Do NOT start harvesting until exclusion_set_count < 1000
6. Harvest the exclusion set - Logged to PartitionQuery
7. Build OR inclusion query and get count
8. If inclusion_set_count < 1000: harvest it - Logged to PartitionQuery
9. If inclusion_set_count >= 1000: RECURSIVELY partition (create child PartitionRun)

This guarantees: exclusion_set + inclusion_set = all_items
"""
import asyncio
import json
import logging
import re
import time
from datetime import datetime
from typing import Dict, Any, List, Set, Optional, Callable, Tuple
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import PartitionRun, PartitionTermAttempt, PartitionQuery, PartitionLLMCall, Citation

logger = logging.getLogger(__name__)

# Constants
GOOGLE_SCHOLAR_LIMIT = 1000
TARGET_THRESHOLD = 950  # Aim to get below this to have safety margin
MAX_RECURSION_DEPTH = 3
MAX_TERM_ATTEMPTS = 200  # Safety limit - keep trying until below 1000
MAX_CONSECUTIVE_ZERO_REDUCTIONS = 15  # Give up if 15 terms in a row have 0 reduction
LLM_MODEL = "claude-sonnet-4-5-20250929"


def log_now(msg: str, level: str = "info"):
    """Log message and immediately flush to stdout"""
    import sys
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    print(f"{timestamp} | overflow | {level.upper()} | {msg}", flush=True)
    sys.stdout.flush()


# ============== PARTITION RUN MANAGEMENT ==============


async def create_partition_run(
    db: AsyncSession,
    edition_id: int,
    job_id: Optional[int],
    year: int,
    initial_count: int,
    parent_partition_id: Optional[int] = None,
    base_query: Optional[str] = None,
    depth: int = 0
) -> PartitionRun:
    """Create a new PartitionRun record to track this partition attempt."""
    run = PartitionRun(
        edition_id=edition_id,
        job_id=job_id,
        year=year,
        initial_count=initial_count,
        parent_partition_id=parent_partition_id,
        base_query=base_query,
        depth=depth,
        status="pending",
        target_threshold=TARGET_THRESHOLD,
    )
    db.add(run)
    await db.flush()  # Get the ID
    log_now(f"Created PartitionRun #{run.id} for year {year}, initial_count={initial_count}, depth={depth}")
    return run


async def update_partition_status(
    db: AsyncSession,
    run: PartitionRun,
    status: str,
    error_message: Optional[str] = None,
    error_stage: Optional[str] = None
):
    """Update the status of a partition run."""
    run.status = status
    if error_message:
        run.error_message = error_message
    if error_stage:
        run.error_stage = error_stage
    if status == "completed":
        run.completed_at = datetime.utcnow()
    await db.flush()
    log_now(f"PartitionRun #{run.id} status -> {status}")


# ============== LLM CALLS WITH FULL LOGGING ==============


async def suggest_exclusion_terms_llm(
    db: AsyncSession,
    partition_run: PartitionRun,
    edition_title: str,
    year: int,
    current_count: int,
    already_excluded: List[str] = None,
    call_number: int = 1
) -> Tuple[List[str], Optional[PartitionLLMCall]]:
    """
    Use LLM to suggest terms to exclude from titles to reduce result count.

    EVERYTHING is logged to PartitionLLMCall.

    Returns tuple of (terms_list, llm_call_record)
    """
    import anthropic
    import os

    already_excluded = already_excluded or []
    excluded_str = ", ".join([f'"{t}"' for t in already_excluded]) if already_excluded else "none yet"

    prompt = f"""You are helping harvest academic citations from Google Scholar. We have a paper with {current_count} citations in year {year}, exceeding the 1000 result limit. We need to partition the results by excluding common title terms.

SEED PAPER: "{edition_title}"
YEAR: {year}
CURRENT RESULT COUNT: {current_count}
TERMS ALREADY EXCLUDED: {excluded_str}

YOUR TASK: Suggest 15-20 single-word terms that are likely to appear frequently in titles of papers citing this work. These should be:

1. Common academic/domain terms related to the paper's topic
2. Generic scholarly terms (like "analysis", "theory", "study")
3. Key concepts from the paper's domain

We will use these as -intitle:"term" exclusions to reduce the result count below 1000.

IMPORTANT:
- Return ONLY single words (no phrases)
- Return terms that are NOT already excluded
- Order by expected frequency (most common first)
- Include both domain-specific and generic academic terms
- Be creative - think about what words commonly appear in academic paper titles

OUTPUT FORMAT: Return a JSON array of strings, nothing else.
Example: ["corporate", "governance", "firm", "organization", "management", "theory", "analysis", "study", "business", "market", "social", "political", "cultural", "economic", "power"]

Return ONLY the JSON array:"""

    # Create LLM call record BEFORE making the call
    llm_call = PartitionLLMCall(
        partition_run_id=partition_run.id,
        call_number=call_number,
        purpose="suggest_exclusion_terms",
        model=LLM_MODEL,
        prompt=prompt,
        edition_title=edition_title[:500],
        year=year,
        current_count=current_count,
        already_excluded_terms=json.dumps(already_excluded) if already_excluded else None,
        status="pending",
        started_at=datetime.utcnow(),
    )
    db.add(llm_call)
    await db.flush()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log_now("No ANTHROPIC_API_KEY - using fallback terms", "warning")
        llm_call.status = "failed"
        llm_call.error_message = "No ANTHROPIC_API_KEY"
        llm_call.completed_at = datetime.utcnow()
        await db.flush()
        terms = get_fallback_exclusion_terms(edition_title, already_excluded)
        return terms, llm_call

    try:
        client = anthropic.Anthropic(api_key=api_key)
        start_time = time.time()

        log_now(f"LLM call #{call_number} for PartitionRun #{partition_run.id}...")

        response = client.messages.create(
            model=LLM_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        latency_ms = int((time.time() - start_time) * 1000)
        response_text = response.content[0].text.strip()

        # Update LLM call record with response
        llm_call.raw_response = response_text
        llm_call.latency_ms = latency_ms
        llm_call.input_tokens = response.usage.input_tokens if hasattr(response, 'usage') else None
        llm_call.output_tokens = response.usage.output_tokens if hasattr(response, 'usage') else None
        llm_call.completed_at = datetime.utcnow()

        # Parse JSON from response
        json_match = re.search(r'\[.*?\]', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)

        try:
            terms = json.loads(response_text)
            # Filter out already excluded terms
            terms = [t for t in terms if isinstance(t, str) and t.lower() not in [e.lower() for e in already_excluded]]

            llm_call.parsed_terms = json.dumps(terms)
            llm_call.terms_count = len(terms)
            llm_call.status = "completed"

            log_now(f"LLM suggested {len(terms)} terms: {terms[:5]}...")
            await db.flush()
            return terms, llm_call

        except json.JSONDecodeError as e:
            llm_call.status = "parse_error"
            llm_call.error_message = f"JSON parse error: {e}"
            await db.flush()
            log_now(f"LLM response parse error: {e}", "warning")
            return get_fallback_exclusion_terms(edition_title, already_excluded), llm_call

    except Exception as e:
        llm_call.status = "failed"
        llm_call.error_message = str(e)[:1000]
        llm_call.completed_at = datetime.utcnow()
        await db.flush()
        log_now(f"LLM call failed: {e}", "warning")
        return get_fallback_exclusion_terms(edition_title, already_excluded), llm_call


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
        "governance", "firm", "business", "industry", "policy", "regulation",
        "culture", "cultural", "identity", "discourse", "power", "media",
        "urban", "space", "global", "international", "national", "local"
    ]

    # Extract potential domain terms from the title
    title_words = re.findall(r'\b[a-zA-Z]{4,}\b', edition_title.lower())
    domain_terms = [w for w in title_words if w not in {'the', 'and', 'for', 'with', 'from'}]

    # Combine and filter
    all_terms = domain_terms[:10] + generic_terms
    filtered = [t for t in all_terms if t.lower() not in already_excluded]

    return filtered[:25]


# ============== QUERY EXECUTION WITH FULL LOGGING ==============


async def execute_count_query(
    db: AsyncSession,
    partition_run: PartitionRun,
    scholar_service,
    scholar_id: str,
    year: int,
    query_suffix: str,
    query_type: str,
    purpose: str
) -> Tuple[int, PartitionQuery]:
    """
    Execute a count query and log it to PartitionQuery.

    Returns tuple of (count, query_record)
    """
    # Create query record
    query_record = PartitionQuery(
        partition_run_id=partition_run.id,
        query_type=query_type,
        scholar_id=scholar_id,
        year=year,
        additional_query=query_suffix if query_suffix else None,
        purpose=purpose,
        status="pending",
        started_at=datetime.utcnow(),
    )
    db.add(query_record)
    await db.flush()

    try:
        start_time = time.time()

        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=10,  # Just first page for count
            year_low=year,
            year_high=year,
            additional_query=query_suffix if query_suffix else None,
        )

        latency_ms = int((time.time() - start_time) * 1000)
        count = result.get('totalResults', 0) if isinstance(result, dict) else 0

        query_record.actual_count = count
        query_record.latency_ms = latency_ms
        query_record.status = "completed"
        query_record.completed_at = datetime.utcnow()
        await db.flush()

        return count, query_record

    except Exception as e:
        query_record.status = "failed"
        query_record.error_message = str(e)[:1000]
        query_record.completed_at = datetime.utcnow()
        await db.flush()
        raise


async def execute_harvest_query(
    db: AsyncSession,
    partition_run: PartitionRun,
    scholar_service,
    scholar_id: str,
    year: int,
    query_suffix: str,
    query_type: str,
    purpose: str,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    max_results: int = GOOGLE_SCHOLAR_LIMIT
) -> Tuple[int, int, PartitionQuery]:
    """
    Execute a harvest query and log it to PartitionQuery.

    Returns tuple of (citations_new, citations_total, query_record)
    """
    # Create query record
    query_record = PartitionQuery(
        partition_run_id=partition_run.id,
        query_type=query_type,
        scholar_id=scholar_id,
        year=year,
        additional_query=query_suffix if query_suffix else None,
        purpose=purpose,
        status="running",
        started_at=datetime.utcnow(),
    )
    db.add(query_record)
    await db.flush()

    start_count = len(existing_scholar_ids)

    try:
        start_time = time.time()

        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=max_results,
            year_low=year,
            year_high=year,
            additional_query=query_suffix if query_suffix else None,
            on_page_complete=on_page_complete,
        )

        latency_ms = int((time.time() - start_time) * 1000)

        new_count = len(existing_scholar_ids) - start_count

        if isinstance(result, dict):
            query_record.actual_count = result.get('totalResults', 0)
            query_record.pages_fetched = result.get('pages_fetched', 0)
            query_record.pages_succeeded = result.get('pages_succeeded', 0)
            query_record.pages_failed = result.get('pages_failed', 0)

        query_record.citations_new = new_count
        query_record.citations_harvested = result.get('pages_fetched', 0) * 10 if isinstance(result, dict) else 0
        query_record.latency_ms = latency_ms
        query_record.status = "completed"
        query_record.completed_at = datetime.utcnow()
        await db.flush()

        return new_count, query_record.citations_harvested, query_record

    except Exception as e:
        query_record.status = "failed"
        query_record.error_message = str(e)[:1000]
        query_record.completed_at = datetime.utcnow()
        await db.flush()
        raise


# ============== TERM TESTING WITH FULL LOGGING ==============


async def test_exclusion_term(
    db: AsyncSession,
    partition_run: PartitionRun,
    scholar_service,
    scholar_id: str,
    year: int,
    term: str,
    current_exclusions: List[str],
    count_before: int,
    order_tried: int,
    source: str,
    llm_call_id: Optional[int] = None
) -> Tuple[int, bool, PartitionTermAttempt]:
    """
    Test adding a term to the exclusion list.

    Returns tuple of (new_count, kept, term_attempt_record)
    """
    # Build test query
    test_exclusions = current_exclusions + [term]
    test_query = " ".join([f'-intitle:"{t}"' for t in test_exclusions])

    # Create term attempt record
    term_attempt = PartitionTermAttempt(
        partition_run_id=partition_run.id,
        term=term,
        order_tried=order_tried,
        source=source,
        llm_call_id=llm_call_id,
        test_query=test_query,
        count_before=count_before,
        count_after=0,
        reduction=0,
        reduction_percent=0.0,
        kept=False,
    )
    db.add(term_attempt)
    await db.flush()

    try:
        start_time = time.time()

        count_after, query_record = await execute_count_query(
            db=db,
            partition_run=partition_run,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            year=year,
            query_suffix=test_query,
            query_type="term_test",
            purpose=f"Testing exclusion of '{term}'"
        )

        latency_ms = int((time.time() - start_time) * 1000)
        reduction = count_before - count_after
        reduction_pct = (reduction / count_before * 100) if count_before > 0 else 0

        term_attempt.count_after = count_after
        term_attempt.reduction = reduction
        term_attempt.reduction_percent = reduction_pct
        term_attempt.latency_ms = latency_ms

        # Decide whether to keep this term
        kept = count_after < count_before  # Only keep if it actually reduces count
        term_attempt.kept = kept

        if not kept:
            term_attempt.skip_reason = "no_reduction" if reduction == 0 else "negative_reduction"

        await db.flush()

        log_now(f"  Term '{term}': {count_before} -> {count_after} (reduction: {reduction}, {'KEPT' if kept else 'SKIPPED'})")

        return count_after, kept, term_attempt

    except Exception as e:
        term_attempt.skip_reason = f"error: {str(e)[:80]}"
        await db.flush()
        log_now(f"  Term '{term}': ERROR - {e}", "warning")
        return count_before, False, term_attempt


# ============== MAIN PARTITION LOGIC ==============


async def find_exclusion_set(
    db: AsyncSession,
    partition_run: PartitionRun,
    scholar_service,
    scholar_id: str,
    year: int,
    edition_title: str,
    initial_count: int
) -> Tuple[List[str], int, bool]:
    """
    Find a set of exclusion terms that brings the result count below TARGET_THRESHOLD.

    CRITICAL: This function will NOT return success=True unless we actually get below threshold.

    Returns tuple of (excluded_terms, final_count, success)
    """
    log_now(f"Finding exclusion set for year {year} (initial count: {initial_count})")

    partition_run.status = "finding_terms"
    partition_run.terms_started_at = datetime.utcnow()
    await db.flush()

    excluded_terms = []
    current_count = initial_count
    term_order = 0
    llm_call_number = 0
    consecutive_zero_reductions = 0  # Track when we're stuck

    # Get initial term suggestions from LLM
    llm_call_number += 1
    suggested_terms, llm_call = await suggest_exclusion_terms_llm(
        db=db,
        partition_run=partition_run,
        edition_title=edition_title,
        year=year,
        current_count=current_count,
        already_excluded=excluded_terms,
        call_number=llm_call_number
    )

    term_index = 0

    # CRITICAL: Keep trying until we're below the HARD LIMIT (1000), not the ideal target (950)
    # The target threshold is a safety margin, but if we can't reach it, being below 1000 is still OK
    # Only stop if: (1) we succeed (<1000), (2) LLM gives no terms, or (3) truly stuck
    while current_count >= GOOGLE_SCHOLAR_LIMIT and term_order < MAX_TERM_ATTEMPTS:
        # Check if we're stuck (too many consecutive terms with no reduction)
        if consecutive_zero_reductions >= MAX_CONSECUTIVE_ZERO_REDUCTIONS:
            log_now(f"STUCK: {MAX_CONSECUTIVE_ZERO_REDUCTIONS} consecutive terms with 0 reduction. Requesting fresh batch from LLM...")
            consecutive_zero_reductions = 0  # Reset and try a fresh batch
            # Clear remaining suggested terms to force new LLM call
            suggested_terms = suggested_terms[:term_index]  # Keep only already-tried terms

        # Get next term to try
        if term_index >= len(suggested_terms):
            # Need more terms from LLM
            log_now(f"Requesting more terms from LLM (attempt {term_order + 1}, count={current_count})...")
            llm_call_number += 1
            more_terms, llm_call = await suggest_exclusion_terms_llm(
                db=db,
                partition_run=partition_run,
                edition_title=edition_title,
                year=year,
                current_count=current_count,
                already_excluded=excluded_terms,
                call_number=llm_call_number
            )
            if not more_terms:
                log_now(f"LLM returned no new terms after {llm_call_number} calls. Stopping at {current_count}", "warning")
                break
            suggested_terms.extend(more_terms)
            log_now(f"LLM provided {len(more_terms)} new terms to try")

        next_term = suggested_terms[term_index]
        term_index += 1
        term_order += 1

        # Test this term
        new_count, kept, term_attempt = await test_exclusion_term(
            db=db,
            partition_run=partition_run,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            year=year,
            term=next_term,
            current_exclusions=excluded_terms,
            count_before=current_count,
            order_tried=term_order,
            source="llm" if llm_call else "fallback",
            llm_call_id=llm_call.id if llm_call else None
        )

        if kept:
            excluded_terms.append(next_term)
            current_count = new_count
            consecutive_zero_reductions = 0  # Reset - we made progress
        else:
            consecutive_zero_reductions += 1

        # Rate limit
        await asyncio.sleep(1)

    # Log final status
    if current_count < GOOGLE_SCHOLAR_LIMIT:
        log_now(f"SUCCESS: Achieved harvestable count: {current_count} < {GOOGLE_SCHOLAR_LIMIT} after {term_order} attempts")

    # Update partition run with term discovery results
    partition_run.terms_tried_count = term_order
    partition_run.terms_kept_count = len(excluded_terms)
    partition_run.final_exclusion_terms = json.dumps(excluded_terms)
    partition_run.final_exclusion_query = " ".join([f'-intitle:"{t}"' for t in excluded_terms])
    partition_run.exclusion_set_count = current_count
    partition_run.terms_completed_at = datetime.utcnow()

    success = current_count < GOOGLE_SCHOLAR_LIMIT
    if success:
        partition_run.status = "terms_found"
    else:
        partition_run.status = "terms_failed"
        partition_run.error_message = f"Could not reduce count below {GOOGLE_SCHOLAR_LIMIT}. Final count: {current_count}"

    await db.flush()

    return excluded_terms, current_count, success


def build_inclusion_query(excluded_terms: List[str]) -> str:
    """Build an OR query to match items containing any of the excluded terms."""
    if not excluded_terms:
        return ""
    return " OR ".join([f'intitle:"{t}"' for t in excluded_terms])


async def harvest_partition(
    db: AsyncSession,
    scholar_service,
    edition_id: int,
    scholar_id: str,
    year: int,
    edition_title: str,
    paper_id: int,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    total_for_year: int,
    job_id: Optional[int] = None,
    parent_partition_id: Optional[int] = None,
    base_query: Optional[str] = None,
    depth: int = 0
) -> Dict[str, Any]:
    """
    Recursively partition and harvest citations for a year with >1000 results.

    EVERYTHING is logged to the database.

    Uses the exclusion/inclusion partition strategy:
    1. Find terms to exclude until count < 1000
    2. Harvest exclusion set (items WITHOUT those terms)
    3. Harvest inclusion set (items WITH at least one term)
    4. If inclusion set > 1000, recursively partition

    TRANSACTION HANDLING:
    - We commit after creating PartitionRun to ensure traceability persists even if harvest fails
    - We commit periodically during term testing to save progress
    - On any error, we rollback to recover the transaction state
    """
    indent = "  " * depth
    log_now(f"{indent}=== PARTITION depth={depth}, year={year}, total={total_for_year} ===")

    # Create partition run record and COMMIT immediately to ensure traceability persists
    partition_run = await create_partition_run(
        db=db,
        edition_id=edition_id,
        job_id=job_id,
        year=year,
        initial_count=total_for_year,
        parent_partition_id=parent_partition_id,
        base_query=base_query,
        depth=depth
    )
    # Commit the partition run creation immediately so it's visible even if harvest fails
    try:
        await db.commit()
        log_now(f"{indent}Committed PartitionRun #{partition_run.id}")
    except Exception as commit_err:
        log_now(f"{indent}Failed to commit PartitionRun: {commit_err}", "error")
        await db.rollback()
        raise

    stats = {
        "partition_run_id": partition_run.id,
        "depth": depth,
        "year": year,
        "initial_count": total_for_year,
        "exclusion_harvested": 0,
        "inclusion_harvested": 0,
        "total_new": 0,
        "success": False
    }

    if depth >= MAX_RECURSION_DEPTH:
        log_now(f"{indent}Max depth reached, harvesting what we can", "warning")
        partition_run.status = "failed"
        partition_run.error_message = f"Max recursion depth ({MAX_RECURSION_DEPTH}) reached"
        partition_run.error_stage = "depth_limit"
        await db.flush()

        # Just harvest up to 1000
        try:
            new_count, total_harvested, query_record = await execute_harvest_query(
                db=db,
                partition_run=partition_run,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                year=year,
                query_suffix=base_query,
                query_type="depth_limit_harvest",
                purpose=f"Fallback harvest at max depth (base: {base_query[:50] if base_query else 'none'}...)",
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
            )
            stats["total_new"] = new_count
            partition_run.total_harvested = total_harvested
            partition_run.total_new_unique = new_count
        except Exception as e:
            log_now(f"{indent}Fallback harvest failed: {e}", "error")

        await db.flush()
        return stats

    # Step 1: Find exclusion terms to get below 1000
    # CRITICAL: We do NOT proceed to harvesting unless this succeeds
    log_now(f"{indent}Step 1: Finding exclusion set...")

    try:
        excluded_terms, exclusion_count, terms_success = await find_exclusion_set(
            db=db,
            partition_run=partition_run,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            year=year,
            edition_title=edition_title,
            initial_count=total_for_year
        )
        # Commit term discovery results before starting harvest
        await db.commit()
        log_now(f"{indent}Committed term discovery results")
    except Exception as term_err:
        log_now(f"{indent}Term discovery error: {term_err}", "error")
        try:
            await db.rollback()
            log_now(f"{indent}Rolled back transaction after term discovery error")
        except Exception:
            pass
        stats["error"] = f"Term discovery exception: {term_err}"
        return stats

    if not terms_success:
        log_now(f"{indent}FAILED: Could not reduce below {GOOGLE_SCHOLAR_LIMIT}, final count: {exclusion_count}", "error")
        stats["error"] = f"Term discovery failed. Count still at {exclusion_count}. Cannot proceed with harvest until below {GOOGLE_SCHOLAR_LIMIT}."

        # CRITICAL: Do NOT proceed with harvest until we're below 1000
        # Proceeding with partial harvest defeats the entire purpose of the partition strategy
        await update_partition_status(db, partition_run, "failed",
            error_message=f"Could not reduce count below {GOOGLE_SCHOLAR_LIMIT}. Final count: {exclusion_count}. Need more effective exclusion terms.",
            error_stage="term_discovery")
        await db.commit()  # Commit the failed status

        log_now(f"{indent}Harvest BLOCKED - must find terms to reduce below {GOOGLE_SCHOLAR_LIMIT} before scraping", "error")
        return stats

    exclusion_query = partition_run.final_exclusion_query
    if base_query:
        exclusion_query = f"{base_query} {exclusion_query}"

    log_now(f"{indent}Exclusion set: {len(excluded_terms)} terms, {exclusion_count} results")
    log_now(f"{indent}Terms: {excluded_terms}")

    # Step 2: Harvest the EXCLUSION set (items WITHOUT those terms)
    log_now(f"{indent}Step 2: Harvesting exclusion set ({exclusion_count} items)...")

    partition_run.status = "harvesting_exclusion"
    partition_run.exclusion_started_at = datetime.utcnow()
    await db.flush()
    await db.commit()

    try:
        exclusion_new, exclusion_total, query_record = await execute_harvest_query(
            db=db,
            partition_run=partition_run,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            year=year,
            query_suffix=exclusion_query,
            query_type="exclusion_harvest",
            purpose=f"Harvest exclusion set ({len(excluded_terms)} terms excluded)",
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
        )

        stats["exclusion_harvested"] = exclusion_new
        partition_run.exclusion_harvested = exclusion_new
        partition_run.exclusion_completed_at = datetime.utcnow()
        await db.commit()  # Commit exclusion harvest progress
        log_now(f"{indent}Exclusion set harvested: {exclusion_new} new citations")

    except Exception as e:
        log_now(f"{indent}Exclusion harvest failed: {e}", "error")
        try:
            await db.rollback()  # Rollback to recover transaction state
            log_now(f"{indent}Rolled back after exclusion harvest error")
        except Exception:
            pass
        # Try to update the partition status with the error
        try:
            await update_partition_status(db, partition_run, "failed",
                error_message=str(e),
                error_stage="exclusion_harvest")
            await db.commit()
        except Exception:
            pass
        return stats

    # Rate limit between phases
    await asyncio.sleep(3)

    # Step 3: Build and check INCLUSION query (items WITH at least one term)
    inclusion_query = build_inclusion_query(excluded_terms)
    if base_query:
        inclusion_query = f"({inclusion_query}) {base_query}"

    partition_run.final_inclusion_query = inclusion_query

    log_now(f"{indent}Step 3: Checking inclusion set...")
    log_now(f"{indent}Query: {inclusion_query[:100]}...")

    try:
        inclusion_count, query_record = await execute_count_query(
            db=db,
            partition_run=partition_run,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            year=year,
            query_suffix=inclusion_query,
            query_type="inclusion_count",
            purpose="Count inclusion set"
        )

        partition_run.inclusion_set_count = inclusion_count
        await db.commit()  # Commit the inclusion count

        log_now(f"{indent}Inclusion set has {inclusion_count} results")
    except Exception as count_err:
        log_now(f"{indent}Inclusion count failed: {count_err}", "error")
        try:
            await db.rollback()
        except Exception:
            pass
        stats["error"] = f"Inclusion count failed: {count_err}"
        return stats

    # Step 4: Handle inclusion set
    if inclusion_count == 0:
        log_now(f"{indent}Inclusion set empty, done!")
        partition_run.status = "completed"
        partition_run.inclusion_harvested = 0

    elif inclusion_count < GOOGLE_SCHOLAR_LIMIT:
        # Can harvest directly
        log_now(f"{indent}Step 4: Harvesting inclusion set ({inclusion_count} items)...")

        partition_run.status = "harvesting_inclusion"
        partition_run.inclusion_started_at = datetime.utcnow()
        await db.flush()
        await db.commit()

        try:
            inclusion_new, inclusion_total, query_record = await execute_harvest_query(
                db=db,
                partition_run=partition_run,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                year=year,
                query_suffix=inclusion_query,
                query_type="inclusion_harvest",
                purpose=f"Harvest inclusion set ({inclusion_count} items)",
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
            )

            stats["inclusion_harvested"] = inclusion_new
            partition_run.inclusion_harvested = inclusion_new
            partition_run.inclusion_completed_at = datetime.utcnow()
            partition_run.status = "completed"
            await db.commit()  # Commit inclusion harvest results
            log_now(f"{indent}Inclusion set harvested: {inclusion_new} new citations")

        except Exception as e:
            log_now(f"{indent}Inclusion harvest failed: {e}", "error")
            try:
                await db.rollback()
                log_now(f"{indent}Rolled back after inclusion harvest error")
            except Exception:
                pass
            try:
                await update_partition_status(db, partition_run, "failed",
                    error_message=str(e),
                    error_stage="inclusion_harvest")
                await db.commit()
            except Exception:
                pass

    else:
        # Inclusion set also >1000, need to recursively partition
        log_now(f"{indent}Step 4: Inclusion set too large ({inclusion_count}), RECURSIVELY PARTITIONING...")

        partition_run.status = "needs_recursive"
        await db.flush()
        await db.commit()

        try:
            # Recursive call with inclusion_query as base
            sub_result = await harvest_partition(
                db=db,
                scholar_service=scholar_service,
                edition_id=edition_id,
                scholar_id=scholar_id,
                year=year,
                edition_title=edition_title,
                paper_id=paper_id,
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
                total_for_year=inclusion_count,
                job_id=job_id,
                parent_partition_id=partition_run.id,
                base_query=inclusion_query,
                depth=depth + 1
            )

            stats["inclusion_harvested"] = sub_result.get("total_new", 0)
            partition_run.inclusion_harvested = stats["inclusion_harvested"]
            partition_run.status = "completed"
        except Exception as recursive_err:
            log_now(f"{indent}Recursive partition failed: {recursive_err}", "error")
            try:
                await db.rollback()
            except Exception:
                pass
            try:
                await update_partition_status(db, partition_run, "failed",
                    error_message=str(recursive_err),
                    error_stage="recursive_partition")
                await db.commit()
            except Exception:
                pass
            return stats

    # Finalize
    partition_run.total_harvested = (partition_run.exclusion_harvested or 0) + (partition_run.inclusion_harvested or 0)
    partition_run.total_new_unique = stats["exclusion_harvested"] + stats["inclusion_harvested"]
    partition_run.completed_at = datetime.utcnow()

    try:
        await db.commit()  # Final commit
        log_now(f"{indent}Committed final partition results")
    except Exception as final_err:
        log_now(f"{indent}Failed to commit final results: {final_err}", "warning")
        try:
            await db.rollback()
        except Exception:
            pass

    stats["total_new"] = partition_run.total_new_unique
    stats["success"] = True

    log_now(f"{indent}=== PARTITION COMPLETE: {stats['total_new']} total new citations ===")

    return stats


async def detect_and_handle_overflow(
    db: AsyncSession,
    scholar_service,
    edition_id: int,
    scholar_id: str,
    year: int,
    edition_title: str,
    paper_id: int,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    year_result: Dict[str, Any],
    job_id: Optional[int] = None
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

    log_now(f"OVERFLOW DETECTED: Year {year} has {total_results} citations, only fetched {papers_fetched}")

    # Use partition strategy to get the rest
    return await harvest_partition(
        db=db,
        scholar_service=scholar_service,
        edition_id=edition_id,
        scholar_id=scholar_id,
        year=year,
        edition_title=edition_title,
        paper_id=paper_id,
        existing_scholar_ids=existing_scholar_ids,
        on_page_complete=on_page_complete,
        total_for_year=total_results,
        job_id=job_id,
    )
