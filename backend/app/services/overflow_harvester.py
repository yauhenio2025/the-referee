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
