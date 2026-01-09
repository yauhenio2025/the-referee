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

from sqlalchemy.exc import DBAPIError, OperationalError

from ..models import PartitionRun, PartitionTermAttempt, PartitionQuery, PartitionLLMCall, Citation

logger = logging.getLogger(__name__)


async def db_retry(db: AsyncSession, operation_name: str = "db_operation", max_retries: int = 3):
    """
    Retry decorator context for database operations.

    Usage:
        async with db_retry(db, "flush partition run"):
            await safe_flush(db)

    On connection error, rolls back and retries up to max_retries times.
    """
    class DbRetryContext:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                return False

            # Check if it's a connection error we can retry
            if isinstance(exc_val, (DBAPIError, OperationalError)):
                error_msg = str(exc_val).lower()
                if any(x in error_msg for x in ['connection', 'closed', 'timeout', 'reset']):
                    log_now(f"[DB RETRY] {operation_name} failed with connection error, will retry", "warn")
                    try:
                        await db.rollback()
                    except Exception:
                        pass
                    return False  # Don't suppress, let caller handle retry
            return False

    return DbRetryContext()


async def safe_flush(db: AsyncSession, context: str = ""):
    """Flush with retry on connection errors"""
    for attempt in range(3):
        try:
            await db.flush()
            return
        except (DBAPIError, OperationalError) as e:
            error_msg = str(e).lower()
            if any(x in error_msg for x in ['connection', 'closed', 'timeout', 'reset']):
                log_now(f"[DB RETRY] Flush failed ({context}), attempt {attempt + 1}/3: {e}", "warn")
                try:
                    await db.rollback()
                except Exception:
                    pass
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
            raise


async def safe_commit(db: AsyncSession, context: str = ""):
    """Commit with retry on connection errors"""
    for attempt in range(3):
        try:
            await db.commit()
            return
        except (DBAPIError, OperationalError) as e:
            error_msg = str(e).lower()
            if any(x in error_msg for x in ['connection', 'closed', 'timeout', 'reset']):
                log_now(f"[DB RETRY] Commit failed ({context}), attempt {attempt + 1}/3: {e}", "warn")
                try:
                    await db.rollback()
                except Exception:
                    pass
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
            raise


async def db_keepalive(db: AsyncSession):
    """
    Ping the database to keep the connection alive.
    Call this before DB operations after long waits (like LLM calls).
    If connection is dead, this will fail and trigger pool reconnection.
    """
    try:
        from sqlalchemy import text
        await db.execute(text("SELECT 1"))
        return True
    except Exception as e:
        log_now(f"[DB KEEPALIVE] Connection check failed: {e}", "warn")
        # Try to rollback to clear any bad state
        try:
            await db.rollback()
        except Exception:
            pass
        return False


# Constants
GOOGLE_SCHOLAR_LIMIT = 1000
TARGET_THRESHOLD = 990  # Just need to be below 1000 - previous 850 was too aggressive
MAX_RECURSION_DEPTH = 3
MAX_TERM_ATTEMPTS = 200  # Safety limit - keep trying until below 1000
MAX_CONSECUTIVE_ZERO_REDUCTIONS = 15  # Give up if 15 terms in a row have 0 reduction
LLM_MODEL = "claude-sonnet-4-5-20250929"

# Language filter constants for stratified harvesting
# Non-English: Chinese (Simplified & Traditional), Dutch, French, German, Italian, Japanese, Korean, Polish, Portuguese, Spanish, Turkish
# NOTE: Oxylabs cannot handle pipe-separated multi-language filters - they cause ReadTimeout
# So we harvest each language SEPARATELY with individual requests
NON_ENGLISH_LANGUAGE_LIST = [
    "lang_zh-CN",  # Chinese Simplified
    "lang_zh-TW",  # Chinese Traditional
    "lang_ja",     # Japanese
    "lang_ko",     # Korean
    "lang_de",     # German
    "lang_fr",     # French
    "lang_es",     # Spanish
    "lang_pt",     # Portuguese
    "lang_it",     # Italian
    "lang_nl",     # Dutch
    "lang_pl",     # Polish
    "lang_tr",     # Turkish
]
ENGLISH_ONLY = "lang_en"

# Legacy constant for backwards compatibility (but don't use with Oxylabs!)
NON_ENGLISH_LANGUAGES = "|".join(NON_ENGLISH_LANGUAGE_LIST)


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
    await safe_flush(db)  # Get the ID
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
    await safe_flush(db)
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

    prompt = f"""You are helping harvest academic citations from Google Scholar. We have a paper with {current_count} citations in year {year}, exceeding the 1000 result limit. We need to QUICKLY reduce the count below 1000 by excluding common title terms.

SEED PAPER: "{edition_title}"
YEAR: {year}
CURRENT RESULT COUNT: {current_count}
TARGET: Get below 1000 as FAST as possible
TERMS ALREADY EXCLUDED: {excluded_str}

YOUR TASK: Suggest 25-30 HIGH-FREQUENCY single-word terms. PRIORITIZE terms that will exclude the MOST papers per term. Think about:

1. EXTREMELY common academic words that appear in almost every paper title (the, of, and, in, for, on, to, with, from, by, an, as - NO, these are too short. Think: study, analysis, research, review, case, approach, perspective, between, through, toward, understanding, examining, exploring, impact, effect, role, development, practice, process, system, using, based, new, modern, contemporary)

2. Domain-specific BROAD terms from this paper's field that will match MANY papers (for postmodernism/cultural theory: cultural, culture, social, political, economic, theory, critical, modern, contemporary, media, identity, global, discourse, power, narrative, space, history, art, literature, film, urban, aesthetic)

3. AVOID overly specific/niche terms that only match a few papers

GOAL: Each term should ideally exclude 50+ papers. We need to get from {current_count} to below 1000.

IMPORTANT:
- Return ONLY single words (no phrases)
- Return terms NOT already excluded
- Order by EXPECTED IMPACT (highest reduction first)
- Include generic academic terms that appear in MANY titles

OUTPUT FORMAT: Return a JSON array of strings, nothing else.
Example: ["cultural", "social", "political", "theory", "critical", "modern", "contemporary", "analysis", "study", "identity", "media", "global", "discourse", "power", "narrative", "space", "history", "practice", "development", "economic", "urban", "art", "literature", "perspective", "approach"]

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
    await safe_flush(db)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log_now("No ANTHROPIC_API_KEY - using fallback terms", "warning")
        llm_call.status = "failed"
        llm_call.error_message = "No ANTHROPIC_API_KEY"
        llm_call.completed_at = datetime.utcnow()
        await safe_flush(db)
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

        # Keep DB connection alive after LLM call (can take 30-60 seconds)
        await db_keepalive(db)

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
            await safe_flush(db)
            return terms, llm_call

        except json.JSONDecodeError as e:
            llm_call.status = "parse_error"
            llm_call.error_message = f"JSON parse error: {e}"
            await safe_flush(db)
            log_now(f"LLM response parse error: {e}", "warning")
            return get_fallback_exclusion_terms(edition_title, already_excluded), llm_call

    except Exception as e:
        llm_call.status = "failed"
        llm_call.error_message = str(e)[:1000]
        llm_call.completed_at = datetime.utcnow()
        await safe_flush(db)
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
    purpose: str,
    language_filter: Optional[str] = None
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
    await safe_flush(db)

    try:
        start_time = time.time()

        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=10,  # Just first page for count
            year_low=year,
            year_high=year,
            additional_query=query_suffix if query_suffix else None,
            language_filter=language_filter,
        )

        latency_ms = int((time.time() - start_time) * 1000)

        # Keep DB connection alive after Scholar query (can take 10-20 seconds)
        await db_keepalive(db)

        count = result.get('totalResults', 0) if isinstance(result, dict) else 0

        query_record.actual_count = count
        query_record.latency_ms = latency_ms
        query_record.status = "completed"
        query_record.completed_at = datetime.utcnow()
        await safe_flush(db, "test_exclusion_query completion")

        return count, query_record

    except Exception as e:
        query_record.status = "failed"
        query_record.error_message = str(e)[:1000]
        query_record.completed_at = datetime.utcnow()
        await safe_flush(db)
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
    max_results: int = GOOGLE_SCHOLAR_LIMIT,
    language_filter: Optional[str] = None
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
    await safe_flush(db)

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
            language_filter=language_filter,
        )

        latency_ms = int((time.time() - start_time) * 1000)

        # Keep DB connection alive after harvest (can take several minutes)
        await db_keepalive(db)

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
        await safe_flush(db)

        return new_count, query_record.citations_harvested, query_record

    except Exception as e:
        query_record.status = "failed"
        query_record.error_message = str(e)[:1000]
        query_record.completed_at = datetime.utcnow()
        await safe_flush(db)
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
    llm_call_id: Optional[int] = None,
    language_filter: Optional[str] = None
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
    await safe_flush(db)

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
            purpose=f"Testing exclusion of '{term}'",
            language_filter=language_filter,
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

        await safe_flush(db)

        log_now(f"  Term '{term}': {count_before} -> {count_after} (reduction: {reduction}, {'KEPT' if kept else 'SKIPPED'})")

        return count_after, kept, term_attempt

    except Exception as e:
        term_attempt.skip_reason = f"error: {str(e)[:80]}"
        await safe_flush(db)
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
    initial_count: int,
    language_filter: Optional[str] = None
) -> Tuple[List[str], int, bool]:
    """
    Find a set of exclusion terms that brings the result count below TARGET_THRESHOLD.

    IMPORTANT: When used for stratified harvesting, pass language_filter=ENGLISH_ONLY
    to ensure term testing happens within the English subset, not all languages.

    CRITICAL: This function will NOT return success=True unless we actually get below threshold.

    Returns tuple of (excluded_terms, final_count, success)
    """
    log_now(f"Finding exclusion set for year {year} (initial count: {initial_count})")

    partition_run.status = "finding_terms"
    partition_run.terms_started_at = datetime.utcnow()
    await safe_flush(db)

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
    # Keep DB connection alive after LLM call (can take 30-60 seconds)
    await db_keepalive(db)

    term_index = 0

    # CRITICAL: Keep trying until we're below the HARD LIMIT (1000), not the ideal target (950)
    # The target threshold is a safety margin, but if we can't reach it, being below 1000 is still OK
    # Only stop if: (1) we succeed (<1000), (2) LLM gives no terms, or (3) truly stuck
    while current_count >= TARGET_THRESHOLD and term_order < MAX_TERM_ATTEMPTS:
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
            # Keep DB connection alive after LLM call
            await db_keepalive(db)
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
            llm_call_id=llm_call.id if llm_call else None,
            language_filter=language_filter,
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
    if current_count < TARGET_THRESHOLD:
        log_now(f"SUCCESS: Achieved harvestable count: {current_count} < {TARGET_THRESHOLD} (target) after {term_order} attempts")

    # Update partition run with term discovery results
    partition_run.terms_tried_count = term_order
    partition_run.terms_kept_count = len(excluded_terms)
    partition_run.final_exclusion_terms = json.dumps(excluded_terms)
    partition_run.final_exclusion_query = " ".join([f'-intitle:"{t}"' for t in excluded_terms])
    partition_run.exclusion_set_count = current_count
    partition_run.terms_completed_at = datetime.utcnow()

    success = current_count < TARGET_THRESHOLD
    if success:
        partition_run.status = "terms_found"
    else:
        partition_run.status = "terms_failed"
        partition_run.error_message = f"Could not reduce count below {TARGET_THRESHOLD}. Final count: {current_count}"

    await safe_flush(db)

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
        await safe_commit(db)
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
        await safe_flush(db)

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

        await safe_flush(db)
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
        await safe_commit(db)
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
        log_now(f"{indent}FAILED: Could not reduce below {TARGET_THRESHOLD}, final count: {exclusion_count}", "error")
        stats["error"] = f"Term discovery failed. Count still at {exclusion_count}. Cannot proceed with harvest until below {TARGET_THRESHOLD}."

        # CRITICAL: Do NOT proceed with harvest until we're below 1000
        # Proceeding with partial harvest defeats the entire purpose of the partition strategy
        await update_partition_status(db, partition_run, "failed",
            error_message=f"Could not reduce count below {TARGET_THRESHOLD}. Final count: {exclusion_count}. Need more effective exclusion terms.",
            error_stage="term_discovery")
        await safe_commit(db)  # Commit the failed status

        log_now(f"{indent}Harvest BLOCKED - must find terms to reduce below {TARGET_THRESHOLD} before scraping", "error")
        return stats

    exclusion_query = partition_run.final_exclusion_query
    if base_query:
        exclusion_query = f"{base_query} {exclusion_query}"

    log_now(f"{indent}Exclusion set: {len(excluded_terms)} terms, {exclusion_count} results")
    log_now(f"{indent}Terms: {excluded_terms}")

    # VERIFICATION: Re-check count before harvesting (Google Scholar counts fluctuate!)
    log_now(f"{indent}Verifying exclusion count before harvest...")
    verify_result = await scholar_service.get_cited_by(
        scholar_id=scholar_id,
        max_results=10,
        year_low=year,
        year_high=year,
        additional_query=exclusion_query,
    )
    verified_count = verify_result.get('totalResults', 0)

    if verified_count != exclusion_count:
        log_now(f"{indent}WARNING: Count changed! Was {exclusion_count}, now {verified_count}", "warn")
        exclusion_count = verified_count
        partition_run.exclusion_set_count = verified_count

    if verified_count >= GOOGLE_SCHOLAR_LIMIT:
        log_now(f"{indent}ERROR: Verified count {verified_count} >= {GOOGLE_SCHOLAR_LIMIT}! Google Scholar lied to us.", "error")
        log_now(f"{indent}Will harvest what we can (max 1000), but coverage will be incomplete.", "warn")

    # Step 2: Harvest the EXCLUSION set (items WITHOUT those terms)
    log_now(f"{indent}Step 2: Harvesting exclusion set ({exclusion_count} items)...")

    partition_run.status = "harvesting_exclusion"
    partition_run.exclusion_started_at = datetime.utcnow()
    await safe_flush(db)
    await safe_commit(db)

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
        await safe_commit(db)  # Commit exclusion harvest progress
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
            await safe_commit(db)
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
        await safe_commit(db)  # Commit the inclusion count

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
        await safe_flush(db)
        await safe_commit(db)

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
            await safe_commit(db)  # Commit inclusion harvest results
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
                await safe_commit(db)
            except Exception:
                pass

    else:
        # Inclusion set also >1000, need to recursively partition
        log_now(f"{indent}Step 4: Inclusion set too large ({inclusion_count}), RECURSIVELY PARTITIONING...")

        partition_run.status = "needs_recursive"
        await safe_flush(db)
        await safe_commit(db)

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
                await safe_commit(db)
            except Exception:
                pass
            return stats

    # Finalize
    partition_run.total_harvested = (partition_run.exclusion_harvested or 0) + (partition_run.inclusion_harvested or 0)
    partition_run.total_new_unique = stats["exclusion_harvested"] + stats["inclusion_harvested"]
    partition_run.completed_at = datetime.utcnow()

    try:
        await safe_commit(db)  # Final commit
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


async def harvest_with_language_stratification(
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
    job_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    STRATIFIED HARVESTING: First harvest non-English papers, then handle English.

    This approach reduces the need for aggressive exclusion terms by first
    carving out a predictable subset (non-English papers).

    Strategy:
    1. Check non-English count and harvest if < 1000
    2. Check English-only count
    3. If English < 1000: harvest directly
    4. If English >= 1000: use exclusion term strategy (but needs fewer terms now)

    Returns stats dict with harvest results.
    """
    log_now(f"╔{'═'*60}╗")
    log_now(f"║  STRATIFIED LANGUAGE HARVEST - Year {year}")
    log_now(f"║  Total for year: {total_for_year}")
    log_now(f"╚{'═'*60}╝")

    stats = {
        "year": year,
        "total_reported": total_for_year,
        "non_english_harvested": 0,
        "english_harvested": 0,
        "total_new": 0,
        "strategy_used": "stratified_language",
        "success": False
    }

    # Create partition run for tracking
    partition_run = await create_partition_run(
        db=db,
        edition_id=edition_id,
        job_id=job_id,
        year=year,
        initial_count=total_for_year,
        parent_partition_id=None,
        base_query=None,
        depth=0
    )
    await safe_commit(db)

    # ========== STEP 1: Harvest NON-ENGLISH papers (per-language) ==========
    # NOTE: Oxylabs cannot handle pipe-separated multi-language filters
    # So we harvest each language SEPARATELY with individual requests
    log_now(f"Step 1: Harvesting non-English papers ({len(NON_ENGLISH_LANGUAGE_LIST)} languages)...")

    partition_run.status = "harvesting_non_english"
    await safe_flush(db)

    total_non_english_new = 0

    for lang_code in NON_ENGLISH_LANGUAGE_LIST:
        try:
            # First check count for this language
            lang_result = await scholar_service.get_cited_by(
                scholar_id=scholar_id,
                max_results=10,  # Just first page for count
                year_low=year,
                year_high=year,
                language_filter=lang_code,
            )
            lang_count = lang_result.get('totalResults', 0)

            if lang_count == 0:
                log_now(f"  {lang_code}: 0 papers (skipping)")
                continue

            if lang_count >= GOOGLE_SCHOLAR_LIMIT:
                log_now(f"  {lang_code}: {lang_count} papers (>= 1000, would need partition)", "warn")
                # For now just harvest first 1000 - could add recursion later
                lang_count = GOOGLE_SCHOLAR_LIMIT

            log_now(f"  {lang_code}: {lang_count} papers - harvesting...")

            lang_new, lang_total, _ = await execute_harvest_query(
                db=db,
                partition_run=partition_run,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                year=year,
                query_suffix=None,
                query_type="non_english_harvest",
                purpose=f"Harvest {lang_code} papers ({lang_count})",
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
                language_filter=lang_code,
            )

            total_non_english_new += lang_new
            log_now(f"  {lang_code}: +{lang_new} new papers (total non-English: {total_non_english_new})")

            # Rate limit between languages
            await asyncio.sleep(2)

        except Exception as e:
            log_now(f"  {lang_code}: ERROR - {e}", "error")
            # Continue with next language
            await asyncio.sleep(3)

    stats["non_english_harvested"] = total_non_english_new
    log_now(f"✓ Total non-English harvested: {total_non_english_new} new papers")
    await safe_commit(db)

    # Rate limit before English phase
    await asyncio.sleep(3)

    # ========== STEP 2: Check ENGLISH-ONLY count ==========
    log_now(f"Step 2: Checking English-only papers...")

    try:
        english_result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=10,  # Just first page for count
            year_low=year,
            year_high=year,
            language_filter=ENGLISH_ONLY,
        )
        english_count = english_result.get('totalResults', 0)
        log_now(f"English-only papers: {english_count}")

        # ========== STEP 3: Handle English papers ==========
        if english_count == 0:
            log_now(f"No English papers found - done!")
            partition_run.status = "completed"
            stats["success"] = True

        elif english_count < GOOGLE_SCHOLAR_LIMIT:
            # Can harvest English directly!
            log_now(f"Step 3a: English count ({english_count}) < 1000 - harvesting directly!")

            partition_run.status = "harvesting_english"
            await safe_flush(db)

            english_new, english_total, _ = await execute_harvest_query(
                db=db,
                partition_run=partition_run,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                year=year,
                query_suffix=None,
                query_type="english_harvest",
                purpose=f"Harvest English papers ({english_count})",
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
                language_filter=ENGLISH_ONLY,
            )

            stats["english_harvested"] = english_new
            partition_run.status = "completed"
            stats["success"] = True
            log_now(f"✓ Harvested {english_new} new English papers")

        else:
            # English still >= 1000, need exclusion term strategy
            log_now(f"Step 3b: English count ({english_count}) >= 1000 - using exclusion term strategy")
            stats["strategy_used"] = "stratified_language_plus_exclusion"

            # Update partition run for exclusion-based harvesting on English subset
            partition_run.status = "finding_terms"
            partition_run.base_query = f"lr={ENGLISH_ONLY}"  # Document that we're working on English subset
            await safe_flush(db)

            # Find exclusion terms to reduce English count below 1000
            excluded_terms, exclusion_count, terms_success = await find_exclusion_set(
                db=db,
                partition_run=partition_run,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                year=year,
                edition_title=edition_title,
                initial_count=english_count,  # Start from English count, not total
                language_filter=ENGLISH_ONLY,  # CRITICAL: Test terms within English subset only!
            )
            await safe_commit(db)

            if not terms_success:
                log_now(f"FAILED: Could not reduce English count below {TARGET_THRESHOLD}", "error")
                partition_run.status = "failed"
                partition_run.error_message = f"Could not reduce English count. Final: {exclusion_count}"
                await safe_commit(db)
                stats["error"] = f"Term discovery failed for English subset"
                return stats

            # Build exclusion query for English subset
            exclusion_query = partition_run.final_exclusion_query

            # Harvest EXCLUSION set (English papers WITHOUT those terms)
            log_now(f"Harvesting English exclusion set ({exclusion_count} papers)...")

            partition_run.status = "harvesting_exclusion"
            await safe_flush(db)

            exclusion_new, _, _ = await execute_harvest_query(
                db=db,
                partition_run=partition_run,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                year=year,
                query_suffix=exclusion_query,
                query_type="english_exclusion_harvest",
                purpose=f"Harvest English exclusion set ({len(excluded_terms)} terms)",
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
                language_filter=ENGLISH_ONLY,
            )

            stats["english_harvested"] += exclusion_new
            log_now(f"✓ Harvested {exclusion_new} new English papers (exclusion set)")
            await safe_commit(db)

            await asyncio.sleep(3)

            # Build and harvest INCLUSION set (English papers WITH at least one term)
            inclusion_query = build_inclusion_query(excluded_terms)

            # Check inclusion count
            inclusion_result = await scholar_service.get_cited_by(
                scholar_id=scholar_id,
                max_results=10,
                year_low=year,
                year_high=year,
                additional_query=inclusion_query,
                language_filter=ENGLISH_ONLY,
            )
            inclusion_count = inclusion_result.get('totalResults', 0)
            log_now(f"English inclusion set: {inclusion_count} papers")

            if inclusion_count > 0 and inclusion_count < GOOGLE_SCHOLAR_LIMIT:
                log_now(f"Harvesting English inclusion set ({inclusion_count} papers)...")

                partition_run.status = "harvesting_inclusion"
                await safe_flush(db)

                inclusion_new, _, _ = await execute_harvest_query(
                    db=db,
                    partition_run=partition_run,
                    scholar_service=scholar_service,
                    scholar_id=scholar_id,
                    year=year,
                    query_suffix=inclusion_query,
                    query_type="english_inclusion_harvest",
                    purpose=f"Harvest English inclusion set ({inclusion_count})",
                    existing_scholar_ids=existing_scholar_ids,
                    on_page_complete=on_page_complete,
                    language_filter=ENGLISH_ONLY,
                )

                stats["english_harvested"] += inclusion_new
                log_now(f"✓ Harvested {inclusion_new} new English papers (inclusion set)")

            elif inclusion_count >= GOOGLE_SCHOLAR_LIMIT:
                log_now(f"WARNING: English inclusion set ({inclusion_count}) >= 1000 - would need recursion", "warn")
                # Could add recursive handling here

            partition_run.status = "completed"
            stats["success"] = True

    except Exception as e:
        log_now(f"English harvest error: {e}", "error")
        partition_run.status = "failed"
        partition_run.error_message = str(e)[:1000]
        stats["error"] = str(e)

    # Finalize
    stats["total_new"] = stats["non_english_harvested"] + stats["english_harvested"]
    partition_run.total_new_unique = stats["total_new"]
    partition_run.completed_at = datetime.utcnow()
    await safe_commit(db)

    log_now(f"╔{'═'*60}╗")
    log_now(f"║  STRATIFIED HARVEST COMPLETE")
    log_now(f"║  Non-English: {stats['non_english_harvested']}")
    log_now(f"║  English: {stats['english_harvested']}")
    log_now(f"║  Total new: {stats['total_new']}")
    log_now(f"║  Strategy: {stats['strategy_used']}")
    log_now(f"╚{'═'*60}╝")

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
    Check if a year has overflow and handle it using STRATIFIED LANGUAGE HARVESTING.

    NEW STRATEGY (2024-12):
    1. First harvest non-English papers (usually < 1000)
    2. Then check English-only count
    3. If English < 1000: harvest directly (most common case after non-English carved out)
    4. If English >= 1000: use exclusion term strategy (but fewer terms needed)

    This reduces the need for aggressive exclusion terms and improves coverage.

    Called after the initial year fetch completes.
    """
    papers_fetched = year_result.get("pages_fetched", 0) * 10
    total_results = year_result.get("totalResults", 0)

    # Check if there's overflow
    if papers_fetched < 950 or total_results <= papers_fetched:
        # No overflow, we got everything
        return None

    log_now(f"OVERFLOW DETECTED: Year {year} has {total_results} citations, only fetched {papers_fetched}")

    # Use stratified language harvesting (non-English first, then English)
    return await harvest_with_language_stratification(
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


# ============== AUTHOR-LETTER PARTITIONING STRATEGY ==============
# This strategy replaces year-by-year harvesting for overflow cases.
# It partitions by author letter (a-z) instead of year, which captures
# papers without year metadata that year-by-year misses.

# Author letters ordered by approximate frequency in academic publishing
AUTHOR_LETTERS = [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
    "u", "v", "w", "x", "y", "z"
]

# Default source exclusion terms for splitting overflow letters
# These are common academic sources that can be excluded to reduce count
DEFAULT_SOURCE_EXCLUSIONS = [
    "Available", "open", "historical", "work", "marine", "change",
    "antipode", "frontiers", "geoforum", "urban", "sociology", "theory",
    "world", "global", "education", "cogent", "geography", "geographer",
    "Companion", "routledge", "rivista", "Encyclopedia", "Revista",
    "palgrave", "post", "Transactions", "affairs", "economy", "political",
    "compartaive", "literature", "language", "journalism", "communication",
    "sociological", "anthropological", "cultural", "forum", "social",
    "science", "modern", "studies", "review", "handbook", "research",
    "annals", "society", "culture", "journal", "press", "sage", "quarterly"
]

# Extended source terms for deeper subdivision (when > 2000)
EXTENDED_SOURCE_EXCLUSIONS = [
    "Race", "Globalizations", "Democratization", "Environment", "Contemporary",
    "space", "Environmental", "middle", "Curriculum", "local", "economic",
    "public", "critical", "Critique", "Convergence", "gender", "Cambridge",
    "third", "East", "feminist", "international", "european", "american",
    "national", "human", "new", "development", "policy", "management"
]


def build_letter_exclusion_query(exclude_all_letters: bool = True, include_letter: str = None) -> str:
    """
    Build the author letter exclusion query.

    Args:
        exclude_all_letters: If True, excludes ALL letters (for harvesting non-letter items)
        include_letter: If set, includes this letter while excluding all others

    Returns:
        Query string like: -author:"a*" -author:"b*" ... OR author:"a*" -author:"b*" ...
    """
    if exclude_all_letters:
        # Harvest items with no author letter (rare edge case)
        return " ".join([f'-author:"{letter}*"' for letter in AUTHOR_LETTERS])
    elif include_letter:
        # Harvest items for a specific letter, excluding all others
        parts = [f'author:"{include_letter}*"']
        for letter in AUTHOR_LETTERS:
            if letter != include_letter:
                parts.append(f'-author:"{letter}*"')
        return " ".join(parts)
    else:
        return ""


def build_source_exclusion_query(excluded_sources: List[str]) -> str:
    """Build source exclusion query from list of source terms."""
    return " ".join([f'-source:{source}' for source in excluded_sources])


def build_source_inclusion_query(included_sources: List[str]) -> str:
    """Build source inclusion query (OR) from list of source terms."""
    return " OR ".join([f'source:{source}' for source in included_sources])


async def get_query_count(
    scholar_service,
    scholar_id: str,
    additional_query: str = "",
    language_filter: str = None,
) -> int:
    """Get the count of results for a query without fetching all papers."""
    result = await scholar_service.get_cited_by(
        scholar_id=scholar_id,
        max_results=10,  # Just first page for count
        additional_query=additional_query if additional_query else None,
        language_filter=language_filter,
    )
    return result.get('totalResults', 0)


async def harvest_query_partition(
    db: AsyncSession,
    scholar_service,
    scholar_id: str,
    edition_id: int,
    paper_id: int,
    partition_key: str,  # e.g., "a", "_", "a_excl", "a_incl"
    additional_query: str,
    language_filter: str,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    expected_count: int,
    partition_run: Optional[PartitionRun] = None,
) -> Tuple[int, int]:
    """
    Harvest a single partition and track it in harvest_targets.

    Returns: (new_citations, total_citations)
    """
    from ..models import HarvestTarget

    # Create or update harvest target for this partition
    target_result = await db.execute(
        select(HarvestTarget)
        .where(HarvestTarget.edition_id == edition_id)
        .where(HarvestTarget.letter == partition_key)
    )
    target = target_result.scalar_one_or_none()

    if not target:
        target = HarvestTarget(
            edition_id=edition_id,
            letter=partition_key,
            expected_count=expected_count,
            actual_count=0,
            status="harvesting",
            pages_attempted=0,
            pages_succeeded=0,
            pages_failed=0,
        )
        db.add(target)
        await safe_flush(db)
    else:
        target.expected_count = expected_count
        target.status = "harvesting"
        await safe_flush(db)

    log_now(f"  Harvesting partition '{partition_key}': {expected_count} expected")

    # Track pages
    pages_succeeded = 0
    pages_failed = 0
    new_citations = 0

    async def wrapped_on_page_complete(page_num: int, papers: List[Dict]):
        nonlocal pages_succeeded, new_citations
        pages_succeeded += 1
        # Call the original callback
        result = await on_page_complete(page_num, papers)
        if isinstance(result, int):
            new_citations += result
        return result

    try:
        result = await scholar_service.get_cited_by(
            scholar_id=scholar_id,
            max_results=min(expected_count + 50, 1000),  # Cap at 1000
            additional_query=additional_query if additional_query else None,
            language_filter=language_filter,
            on_page_complete=wrapped_on_page_complete,
        )

        pages_failed = result.get("pages_failed", 0)
        actual_count = result.get("totalResults", 0)

        # Update target
        target.actual_count = new_citations
        target.pages_attempted = pages_succeeded + pages_failed
        target.pages_succeeded = pages_succeeded
        target.pages_failed = pages_failed
        target.status = "complete" if pages_failed == 0 else "partial"
        await safe_flush(db)

        log_now(f"  Partition '{partition_key}': {new_citations} new citations (pages: {pages_succeeded} ok, {pages_failed} failed)")

        return new_citations, actual_count

    except Exception as e:
        log_now(f"  Partition '{partition_key}' FAILED: {e}", "error")
        target.status = "failed"
        target.gap_reason = str(e)[:50]
        await safe_flush(db)
        return 0, 0


async def find_source_exclusions_with_llm(
    edition_title: str,
    current_count: int,
    target_count: int,
    language: str,
    existing_exclusions: List[str],
) -> List[str]:
    """
    Use Claude Opus 4.5 with extended thinking to find source exclusion terms
    that will reduce the result count below target_count.

    Returns: List of additional source terms to exclude
    """
    import anthropic
    import os

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    prompt = f"""You are helping partition Google Scholar citation results to get below 1000 results.

CONTEXT:
- Paper being cited: "{edition_title}"
- Language filter: {language}
- Current result count: {current_count}
- Target: Below {target_count}
- Already excluded sources: {existing_exclusions}

TASK:
Suggest additional academic journal/source name patterns to exclude that will reduce the count.
Focus on common academic publishers, journal names, and venue types.

For {language} papers, suggest terms in that language if not English.

Return ONLY a JSON array of source terms to exclude, like:
["term1", "term2", "term3"]

Suggest 10-20 terms that are likely to have significant coverage in this citation set.
"""

    try:
        response = client.messages.create(
            model="claude-opus-4-5-20251101",
            max_tokens=16000,
            thinking={
                "type": "enabled",
                "budget_tokens": 32000
            },
            messages=[{"role": "user", "content": prompt}]
        )

        # Extract the text response (after thinking)
        text_response = ""
        for block in response.content:
            if block.type == "text":
                text_response = block.text
                break

        # Parse JSON array from response
        import json
        # Find JSON array in response
        match = re.search(r'\[.*?\]', text_response, re.DOTALL)
        if match:
            terms = json.loads(match.group())
            log_now(f"  LLM suggested {len(terms)} additional source exclusions")
            return terms

        log_now(f"  LLM response did not contain valid JSON array", "warn")
        return []

    except Exception as e:
        log_now(f"  LLM source exclusion failed: {e}", "error")
        return []


async def harvest_letter_with_subdivision(
    db: AsyncSession,
    scholar_service,
    scholar_id: str,
    edition_id: int,
    paper_id: int,
    edition_title: str,
    letter: str,
    letter_count: int,
    language_filter: str,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    partition_run: Optional[PartitionRun] = None,
) -> int:
    """
    Harvest a letter partition that has > 1000 results using source-based subdivision.

    Strategy:
    - If 1000-2000: Split into 2 pools using default source exclusions
    - If 2000-3000: Split into 3 pools using extended exclusions
    - If > 3000: Use LLM to find more exclusion terms

    Returns: Total new citations harvested
    """
    total_new = 0
    letter_query = build_letter_exclusion_query(exclude_all_letters=False, include_letter=letter)

    log_now(f"Letter '{letter}' has {letter_count} results - using source subdivision")

    # Determine how many pools we need
    if letter_count < 2000:
        # 2 pools: exclusion set + inclusion set
        exclusions = DEFAULT_SOURCE_EXCLUSIONS[:20]  # Start with first 20
    elif letter_count < 3000:
        # 3 pools: need more exclusions
        exclusions = DEFAULT_SOURCE_EXCLUSIONS + EXTENDED_SOURCE_EXCLUSIONS[:15]
    else:
        # Need LLM to help find more terms
        exclusions = DEFAULT_SOURCE_EXCLUSIONS + EXTENDED_SOURCE_EXCLUSIONS
        additional = await find_source_exclusions_with_llm(
            edition_title=edition_title,
            current_count=letter_count,
            target_count=900,
            language=language_filter or "English",
            existing_exclusions=exclusions,
        )
        exclusions = exclusions + additional

    # Build exclusion query (Pool A: everything NOT in excluded sources)
    exclusion_source_query = build_source_exclusion_query(exclusions)
    pool_a_query = f"{letter_query} {exclusion_source_query}"

    # Check Pool A count
    pool_a_count = await get_query_count(
        scholar_service, scholar_id, pool_a_query, language_filter
    )
    log_now(f"  Pool A (exclusion): {pool_a_count} results")

    if pool_a_count >= 1000:
        # Need to exclude more - try LLM
        log_now(f"  Pool A still >= 1000, requesting LLM help...")
        additional = await find_source_exclusions_with_llm(
            edition_title=edition_title,
            current_count=pool_a_count,
            target_count=900,
            language=language_filter or "English",
            existing_exclusions=exclusions,
        )
        exclusions = exclusions + additional
        exclusion_source_query = build_source_exclusion_query(exclusions)
        pool_a_query = f"{letter_query} {exclusion_source_query}"
        pool_a_count = await get_query_count(
            scholar_service, scholar_id, pool_a_query, language_filter
        )
        log_now(f"  Pool A after LLM: {pool_a_count} results")

    # Harvest Pool A if under 1000
    if pool_a_count < 1000 and pool_a_count > 0:
        new, _ = await harvest_query_partition(
            db=db,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            edition_id=edition_id,
            paper_id=paper_id,
            partition_key=f"{letter}_excl",
            additional_query=pool_a_query,
            language_filter=language_filter,
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            expected_count=pool_a_count,
            partition_run=partition_run,
        )
        total_new += new
        await asyncio.sleep(3)

    # Build inclusion query (Pool B: everything IN excluded sources)
    inclusion_source_query = build_source_inclusion_query(exclusions)
    pool_b_query = f"{letter_query} ({inclusion_source_query})"

    # Check Pool B count
    pool_b_count = await get_query_count(
        scholar_service, scholar_id, pool_b_query, language_filter
    )
    log_now(f"  Pool B (inclusion): {pool_b_count} results")

    if pool_b_count < 1000 and pool_b_count > 0:
        new, _ = await harvest_query_partition(
            db=db,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            edition_id=edition_id,
            paper_id=paper_id,
            partition_key=f"{letter}_incl",
            additional_query=pool_b_query,
            language_filter=language_filter,
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            expected_count=pool_b_count,
            partition_run=partition_run,
        )
        total_new += new
    elif pool_b_count >= 1000:
        # Pool B still too large - need to recursively subdivide
        # For now, just harvest first 1000 and log warning
        log_now(f"  WARNING: Pool B has {pool_b_count} results, harvesting first 1000", "warn")
        new, _ = await harvest_query_partition(
            db=db,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            edition_id=edition_id,
            paper_id=paper_id,
            partition_key=f"{letter}_incl",
            additional_query=pool_b_query,
            language_filter=language_filter,
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            expected_count=min(pool_b_count, 1000),
            partition_run=partition_run,
        )
        total_new += new

    return total_new


async def harvest_with_author_letter_strategy(
    db: AsyncSession,
    scholar_service,
    edition_id: int,
    scholar_id: str,
    edition_title: str,
    paper_id: int,
    total_citation_count: int,
    existing_scholar_ids: Set[str],
    on_page_complete: Callable,
    job_id: Optional[int] = None,
    language_filter: str = None,
) -> Dict[str, Any]:
    """
    MAIN ENTRY POINT: Harvest citations using author-letter partitioning strategy.

    This replaces year-by-year harvesting for overflow cases.

    Strategy:
    1. If total < 1000: harvest directly (no strategy needed)
    2. If total >= 1000:
       a. Check each language - if all < 1000, harvest by language
       b. If any language >= 1000 (usually English):
          - Harvest non-letter items first (rare)
          - For each letter a-z:
            - If < 1000: harvest directly
            - If >= 1000: use source-based subdivision

    Returns: Stats dict with harvest results
    """
    log_now(f"╔{'═'*60}╗")
    log_now(f"║  AUTHOR-LETTER HARVEST STRATEGY")
    log_now(f"║  Edition: {edition_id} ({edition_title[:40]}...)")
    log_now(f"║  Total citations: {total_citation_count}")
    log_now(f"╚{'═'*60}╝")

    stats = {
        "edition_id": edition_id,
        "total_expected": total_citation_count,
        "total_harvested": 0,
        "strategy_used": "author_letter",
        "letters_processed": [],
        "success": False,
    }

    # Create partition run for tracking
    partition_run = await create_partition_run(
        db=db,
        edition_id=edition_id,
        job_id=job_id,
        year=None,  # No year for author-letter strategy
        initial_count=total_citation_count,
        parent_partition_id=None,
        base_query=None,
        depth=0
    )
    await safe_commit(db)

    # === LEVEL 0: Check if direct harvest is possible ===
    if total_citation_count < GOOGLE_SCHOLAR_LIMIT:
        log_now(f"Total ({total_citation_count}) < 1000 - harvesting directly")
        new, _ = await harvest_query_partition(
            db=db,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            edition_id=edition_id,
            paper_id=paper_id,
            partition_key="_all",
            additional_query="",
            language_filter=language_filter,
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            expected_count=total_citation_count,
            partition_run=partition_run,
        )
        stats["total_harvested"] = new
        stats["strategy_used"] = "direct"
        stats["success"] = True
        partition_run.status = "completed"
        await safe_commit(db)
        return stats

    # === LEVEL 1: Try language stratification first ===
    log_now(f"Total >= 1000 - checking language stratification...")

    # Check non-English languages
    non_english_total = 0
    non_english_harvested = 0

    for lang_code in NON_ENGLISH_LANGUAGE_LIST:
        lang_count = await get_query_count(scholar_service, scholar_id, "", lang_code)
        if lang_count > 0:
            log_now(f"  {lang_code}: {lang_count} results")
            non_english_total += lang_count

            if lang_count < GOOGLE_SCHOLAR_LIMIT:
                new, _ = await harvest_query_partition(
                    db=db,
                    scholar_service=scholar_service,
                    scholar_id=scholar_id,
                    edition_id=edition_id,
                    paper_id=paper_id,
                    partition_key=f"lang_{lang_code}",
                    additional_query="",
                    language_filter=lang_code,
                    existing_scholar_ids=existing_scholar_ids,
                    on_page_complete=on_page_complete,
                    expected_count=lang_count,
                    partition_run=partition_run,
                )
                non_english_harvested += new
                await asyncio.sleep(2)

    log_now(f"Non-English: {non_english_harvested} harvested of {non_english_total} expected")
    stats["non_english_harvested"] = non_english_harvested

    # Check English
    english_count = await get_query_count(scholar_service, scholar_id, "", ENGLISH_ONLY)
    log_now(f"English: {english_count} results")

    if english_count < GOOGLE_SCHOLAR_LIMIT:
        # Can harvest English directly
        log_now(f"English < 1000 - harvesting directly")
        new, _ = await harvest_query_partition(
            db=db,
            scholar_service=scholar_service,
            scholar_id=scholar_id,
            edition_id=edition_id,
            paper_id=paper_id,
            partition_key="lang_en",
            additional_query="",
            language_filter=ENGLISH_ONLY,
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            expected_count=english_count,
            partition_run=partition_run,
        )
        stats["english_harvested"] = new
        stats["total_harvested"] = non_english_harvested + new
        stats["success"] = True
        partition_run.status = "completed"
        await safe_commit(db)
        return stats

    # === LEVEL 2: English >= 1000 - Use author-letter partitioning ===
    log_now(f"English >= 1000 - using author-letter partitioning")
    stats["strategy_used"] = "author_letter"

    english_harvested = 0

    # Step 1: Harvest non-letter items (rare edge case)
    no_letter_query = build_letter_exclusion_query(exclude_all_letters=True)
    no_letter_count = await get_query_count(
        scholar_service, scholar_id, no_letter_query, ENGLISH_ONLY
    )

    if no_letter_count > 0:
        log_now(f"Non-letter items: {no_letter_count}")
        if no_letter_count < GOOGLE_SCHOLAR_LIMIT:
            new, _ = await harvest_query_partition(
                db=db,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                edition_id=edition_id,
                paper_id=paper_id,
                partition_key="_",
                additional_query=no_letter_query,
                language_filter=ENGLISH_ONLY,
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
                expected_count=no_letter_count,
                partition_run=partition_run,
            )
            english_harvested += new
            await asyncio.sleep(2)

    # Step 2: Process each letter a-z
    for letter in AUTHOR_LETTERS:
        letter_query = build_letter_exclusion_query(exclude_all_letters=False, include_letter=letter)
        letter_count = await get_query_count(
            scholar_service, scholar_id, letter_query, ENGLISH_ONLY
        )

        if letter_count == 0:
            log_now(f"Letter '{letter}': 0 results - skipping")
            continue

        log_now(f"Letter '{letter}': {letter_count} results")
        stats["letters_processed"].append({"letter": letter, "count": letter_count})

        if letter_count < GOOGLE_SCHOLAR_LIMIT:
            # Direct harvest for this letter
            new, _ = await harvest_query_partition(
                db=db,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                edition_id=edition_id,
                paper_id=paper_id,
                partition_key=letter,
                additional_query=letter_query,
                language_filter=ENGLISH_ONLY,
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
                expected_count=letter_count,
                partition_run=partition_run,
            )
            english_harvested += new
        else:
            # Need subdivision for this letter
            new = await harvest_letter_with_subdivision(
                db=db,
                scholar_service=scholar_service,
                scholar_id=scholar_id,
                edition_id=edition_id,
                paper_id=paper_id,
                edition_title=edition_title,
                letter=letter,
                letter_count=letter_count,
                language_filter=ENGLISH_ONLY,
                existing_scholar_ids=existing_scholar_ids,
                on_page_complete=on_page_complete,
                partition_run=partition_run,
            )
            english_harvested += new

        await asyncio.sleep(3)  # Rate limit between letters

    stats["english_harvested"] = english_harvested
    stats["total_harvested"] = non_english_harvested + english_harvested
    stats["success"] = True

    partition_run.status = "completed"
    partition_run.final_exclusion_query = f"author_letter_strategy"
    await safe_commit(db)

    log_now(f"╔{'═'*60}╗")
    log_now(f"║  AUTHOR-LETTER HARVEST COMPLETE")
    log_now(f"║  Total harvested: {stats['total_harvested']}")
    log_now(f"║  Non-English: {non_english_harvested}")
    log_now(f"║  English: {english_harvested}")
    log_now(f"╚{'═'*60}╝")

    return stats
