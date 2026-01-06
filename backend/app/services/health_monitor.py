"""
Autonomous Health Monitor with LLM Diagnostics

Monitors harvest activity and uses Claude Sonnet 4.5 to diagnose
and automatically fix stalls when:
- Active jobs exist (running/pending > 0)
- BUT 0 citations saved in last 15 minutes
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import anthropic
from sqlalchemy import select, update, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..models import Job, Edition, Paper, FailedFetch, HealthMonitorLog

logger = logging.getLogger(__name__)
settings = get_settings()

# Constants
CHECK_INTERVAL_SECONDS = settings.health_monitor_interval_minutes * 60
ACTION_COOLDOWN_MINUTES = 15  # Don't repeat same action within this time
LLM_MODEL = "claude-sonnet-4-5-20250929"
LLM_MAX_TOKENS = 1500

# Global state
_monitor_task: Optional[asyncio.Task] = None
_last_action_times: Dict[str, datetime] = {}  # Track action cooldowns


async def collect_diagnostics(db: AsyncSession) -> Dict[str, Any]:
    """
    Collect comprehensive diagnostic data for LLM analysis.
    """
    now = datetime.utcnow()
    diagnostics = {}

    # 1. Activity stats (citations saved in time periods)
    try:
        from .api_logger import get_activity_stats
        activity = await get_activity_stats(db)
        diagnostics["activity_stats"] = activity
    except Exception as e:
        logger.warning(f"Failed to get activity stats: {e}")
        diagnostics["activity_stats"] = {"error": str(e)}

    # 2. Active jobs (running + pending)
    try:
        running_result = await db.execute(
            select(Job).where(Job.status == "running")
        )
        running_jobs = running_result.scalars().all()

        pending_result = await db.execute(
            select(Job).where(Job.status == "pending")
        )
        pending_jobs = pending_result.scalars().all()

        diagnostics["running_jobs"] = [
            {
                "id": j.id,
                "type": j.job_type,
                "paper_id": j.paper_id,
                "progress": j.progress,
                "progress_message": j.progress_message,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "duration_minutes": round((now - j.started_at).total_seconds() / 60, 1) if j.started_at else None,
            }
            for j in running_jobs
        ]
        diagnostics["pending_jobs_count"] = len(pending_jobs)
        diagnostics["oldest_pending"] = min(
            (j.created_at for j in pending_jobs), default=None
        )
        if diagnostics["oldest_pending"]:
            diagnostics["oldest_pending"] = diagnostics["oldest_pending"].isoformat()
    except Exception as e:
        logger.warning(f"Failed to get job data: {e}")
        diagnostics["running_jobs"] = []
        diagnostics["pending_jobs_count"] = 0

    # 3. Recent failed jobs (last 10)
    try:
        failed_result = await db.execute(
            select(Job)
            .where(Job.status == "failed")
            .order_by(Job.completed_at.desc())
            .limit(10)
        )
        failed_jobs = failed_result.scalars().all()
        diagnostics["recent_failures"] = [
            {
                "id": j.id,
                "type": j.job_type,
                "error": j.error[:500] if j.error else None,  # Truncate
                "failed_at": j.completed_at.isoformat() if j.completed_at else None,
            }
            for j in failed_jobs
        ]
    except Exception as e:
        logger.warning(f"Failed to get failed jobs: {e}")
        diagnostics["recent_failures"] = []

    # 4. Stuck editions (being harvested but not progressing)
    try:
        # Editions with active jobs but high stall count
        stuck_result = await db.execute(
            select(Edition, Paper.title)
            .join(Paper, Edition.paper_id == Paper.id)
            .where(
                Edition.selected == True,
                Edition.harvest_stall_count > 3,
            )
            .limit(10)
        )
        stuck_editions = stuck_result.all()
        diagnostics["stuck_editions"] = [
            {
                "edition_id": e.id,
                "paper_title": title[:100] if title else "Unknown",
                "expected": e.citation_count,
                "harvested": e.harvested_citation_count,
                "gap_percent": round(100 * (e.citation_count - e.harvested_citation_count) / e.citation_count, 1) if e.citation_count > 0 else 0,
                "stall_count": e.harvest_stall_count,
                "last_harvested": e.last_harvested_at.isoformat() if e.last_harvested_at else None,
            }
            for e, title in stuck_editions
        ]
    except Exception as e:
        logger.warning(f"Failed to get stuck editions: {e}")
        diagnostics["stuck_editions"] = []

    # 5. Failed fetches (pending retries)
    try:
        failed_fetch_result = await db.execute(
            select(func.count(FailedFetch.id))
            .where(FailedFetch.status == "pending")
        )
        pending_fetches = failed_fetch_result.scalar() or 0

        # Get error patterns
        error_result = await db.execute(
            select(FailedFetch.last_error, func.count(FailedFetch.id))
            .where(FailedFetch.status == "pending")
            .group_by(FailedFetch.last_error)
            .order_by(func.count(FailedFetch.id).desc())
            .limit(5)
        )
        error_patterns = error_result.all()

        diagnostics["failed_fetches"] = {
            "pending_count": pending_fetches,
            "error_patterns": [
                {"error": err[:100] if err else "Unknown", "count": cnt}
                for err, cnt in error_patterns
            ]
        }
    except Exception as e:
        logger.warning(f"Failed to get failed fetches: {e}")
        diagnostics["failed_fetches"] = {"pending_count": 0, "error_patterns": []}

    # 6. DB health (blocking locks, long queries)
    try:
        # Check for blocking locks
        locks_result = await db.execute(text("""
            SELECT
                blocked_locks.pid AS blocked_pid,
                blocking_locks.pid AS blocking_pid,
                blocked_activity.query AS blocked_query
            FROM pg_catalog.pg_locks blocked_locks
            JOIN pg_catalog.pg_locks blocking_locks
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.pid != blocked_locks.pid
            JOIN pg_catalog.pg_stat_activity blocked_activity
                ON blocked_activity.pid = blocked_locks.pid
            WHERE NOT blocked_locks.granted
            LIMIT 5
        """))
        locks = locks_result.fetchall()

        # Check for long-running queries
        activity_result = await db.execute(text("""
            SELECT pid, state, query,
                   EXTRACT(EPOCH FROM (NOW() - query_start)) as duration_seconds
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query NOT LIKE '%pg_stat_activity%'
              AND EXTRACT(EPOCH FROM (NOW() - query_start)) > 30
            ORDER BY duration_seconds DESC
            LIMIT 5
        """))
        long_queries = activity_result.fetchall()

        diagnostics["db_health"] = {
            "blocking_locks": [
                {"blocked_pid": r[0], "blocking_pid": r[1], "query": r[2][:100] if r[2] else None}
                for r in locks
            ],
            "long_queries": [
                {"pid": r[0], "state": r[1], "query": r[2][:100] if r[2] else None, "duration_seconds": round(r[3], 1)}
                for r in long_queries
            ]
        }
    except Exception as e:
        logger.warning(f"Failed to get DB health: {e}")
        diagnostics["db_health"] = {"blocking_locks": [], "long_queries": []}

    return diagnostics


def build_llm_prompt(diagnostics: Dict[str, Any]) -> str:
    """
    Build the structured prompt for Claude Sonnet 4.5.
    """
    activity = diagnostics.get("activity_stats", {})
    stats_15m = activity.get("15min", {})
    stats_1h = activity.get("1hr", {})

    running_jobs = diagnostics.get("running_jobs", [])
    pending_count = diagnostics.get("pending_jobs_count", 0)
    recent_failures = diagnostics.get("recent_failures", [])
    stuck_editions = diagnostics.get("stuck_editions", [])
    failed_fetches = diagnostics.get("failed_fetches", {})
    db_health = diagnostics.get("db_health", {})

    prompt = f"""You are a harvest system diagnostician for a Google Scholar citation harvester.

PROBLEM: We have active jobs but ZERO citations saved in the last 15 minutes.

CURRENT STATE:
- Running jobs: {len(running_jobs)}
- Pending jobs: {pending_count}
- Citations saved last 15min: {stats_15m.get('citations_saved', 0)} (THIS IS THE PROBLEM - should not be 0)
- Citations saved last 1hr: {stats_1h.get('citations_saved', 0)}
- Oxylabs API calls last 15min: {stats_15m.get('oxylabs_calls', 0)}
- Pages fetched last 15min: {stats_15m.get('pages_fetched', 0)}

RUNNING JOBS:
"""
    if running_jobs:
        for job in running_jobs[:10]:
            prompt += f"- Job {job['id']}: type={job['type']}, progress={job['progress']}, duration={job['duration_minutes']}min\n"
    else:
        prompt += "- None\n"

    prompt += f"""
RECENT FAILURES (last 10):
"""
    if recent_failures:
        for fail in recent_failures[:5]:
            prompt += f"- Job {fail['id']}: type={fail['type']}, error={fail['error'][:200] if fail['error'] else 'None'}\n"
    else:
        prompt += "- None\n"

    prompt += f"""
STUCK EDITIONS (high stall count):
"""
    if stuck_editions:
        for ed in stuck_editions[:5]:
            prompt += f"- Edition {ed['edition_id']}: {ed['paper_title'][:50]}, gap={ed['gap_percent']}%, stalls={ed['stall_count']}\n"
    else:
        prompt += "- None\n"

    prompt += f"""
FAILED PAGE FETCHES:
- Pending retries: {failed_fetches.get('pending_count', 0)}
- Common errors: {json.dumps(failed_fetches.get('error_patterns', [])[:3])}

DB HEALTH:
- Blocking locks: {len(db_health.get('blocking_locks', []))}
- Long-running queries (>30s): {len(db_health.get('long_queries', []))}
"""
    if db_health.get('long_queries'):
        for q in db_health['long_queries'][:2]:
            prompt += f"  - PID {q['pid']}: {q['duration_seconds']}s - {q['query'][:50]}...\n"

    prompt += """
Based on this, identify the root cause and recommend ONE action.

AVAILABLE ACTIONS:
1. RESTART_ZOMBIE_JOBS - Reset jobs stuck in "running" for >30min back to "pending"
2. CANCEL_STUCK_JOBS - Cancel jobs making no progress (params: job_ids to cancel)
3. RESET_STALL_COUNTS - Reset stall counters on editions to allow retries
4. RETRY_FAILED_FETCHES - Force queue a job to retry failed page fetches
5. KILL_BLOCKING_QUERY - Kill a specific blocking DB query (params: pid)
6. PAUSE_ALL_HARVESTS - Emergency stop if rate limited (sets harvest_paused=True on all papers)
7. NO_ACTION - If the issue is transient or data is still flowing, wait for next check

IMPORTANT: If pages are being fetched but no citations saved, it likely means:
- All fetched pages contain citations we already have (deduplication)
- OR there's a bug in the citation save code
- OR the harvests are for papers with very few remaining citations

Respond ONLY with valid JSON (no markdown, no explanation outside JSON):
{
  "diagnosis": "Brief explanation of what's wrong (1-2 sentences)",
  "root_cause": "RATE_LIMIT|ZOMBIE_JOBS|DB_LOCK|PARSE_ERROR|DEDUPLICATION|NETWORK|TRANSIENT|UNKNOWN",
  "action": "ACTION_NAME",
  "params": {},
  "confidence": "HIGH|MEDIUM|LOW",
  "reasoning": "Why this action will help (1 sentence)"
}
"""
    return prompt


async def call_llm_diagnosis(prompt: str) -> Dict[str, Any]:
    """
    Call Claude Sonnet 4.5 for diagnosis.
    """
    if not settings.anthropic_api_key:
        logger.error("No Anthropic API key configured")
        return {"error": "No API key", "action": "NO_ACTION"}

    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    try:
        start_time = time.time()
        response = await client.messages.create(
            model=LLM_MODEL,
            max_tokens=LLM_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}]
        )
        duration_ms = int((time.time() - start_time) * 1000)

        # Extract text content
        response_text = ""
        for block in response.content:
            if hasattr(block, 'text'):
                response_text += block.text

        # Parse JSON response
        try:
            # Clean up any markdown code fences
            clean_text = response_text.strip()
            if clean_text.startswith("```"):
                clean_text = clean_text.split("```")[1]
                if clean_text.startswith("json"):
                    clean_text = clean_text[4:]
            clean_text = clean_text.strip()

            result = json.loads(clean_text)
            result["_duration_ms"] = duration_ms
            result["_raw_response"] = response_text
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            return {
                "error": f"JSON parse error: {e}",
                "action": "NO_ACTION",
                "_raw_response": response_text,
                "_duration_ms": duration_ms
            }

    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return {"error": str(e), "action": "NO_ACTION"}


async def execute_action(db: AsyncSession, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute the recommended action.
    """
    global _last_action_times

    # Check cooldown
    if action in _last_action_times:
        time_since = datetime.utcnow() - _last_action_times[action]
        if time_since.total_seconds() < ACTION_COOLDOWN_MINUTES * 60:
            return {
                "executed": False,
                "reason": f"Action {action} on cooldown ({ACTION_COOLDOWN_MINUTES - int(time_since.total_seconds()/60)} min remaining)"
            }

    result = {"executed": False, "details": None}

    try:
        if action == "RESTART_ZOMBIE_JOBS":
            # Reset jobs stuck in "running" for >30min
            timeout_threshold = datetime.utcnow() - timedelta(minutes=30)
            update_result = await db.execute(
                update(Job)
                .where(
                    Job.status == "running",
                    Job.started_at < timeout_threshold
                )
                .values(status="pending", started_at=None)
                .returning(Job.id)
            )
            reset_ids = [r[0] for r in update_result.fetchall()]
            await db.commit()
            result = {"executed": True, "details": f"Reset {len(reset_ids)} zombie jobs: {reset_ids}"}

        elif action == "CANCEL_STUCK_JOBS":
            job_ids = params.get("job_ids", [])
            if job_ids:
                await db.execute(
                    update(Job)
                    .where(Job.id.in_(job_ids))
                    .values(status="cancelled", error="Cancelled by health monitor")
                )
                await db.commit()
                result = {"executed": True, "details": f"Cancelled jobs: {job_ids}"}
            else:
                result = {"executed": False, "reason": "No job_ids provided"}

        elif action == "RESET_STALL_COUNTS":
            # Reset stall counts on editions with high stall counts
            update_result = await db.execute(
                update(Edition)
                .where(Edition.harvest_stall_count > 5)
                .values(harvest_stall_count=0)
                .returning(Edition.id)
            )
            reset_ids = [r[0] for r in update_result.fetchall()]
            await db.commit()
            result = {"executed": True, "details": f"Reset stall counts for {len(reset_ids)} editions"}

        elif action == "RETRY_FAILED_FETCHES":
            # Create a retry_failed_fetches job
            new_job = Job(
                job_type="retry_failed_fetches",
                status="pending",
                params=json.dumps({"max_retries": 50, "source": "health_monitor"}),
            )
            db.add(new_job)
            await db.commit()
            result = {"executed": True, "details": f"Created retry job {new_job.id}"}

        elif action == "KILL_BLOCKING_QUERY":
            pid = params.get("pid")
            if pid:
                await db.execute(text(f"SELECT pg_terminate_backend({int(pid)})"))
                await db.commit()
                result = {"executed": True, "details": f"Killed query with PID {pid}"}
            else:
                result = {"executed": False, "reason": "No PID provided"}

        elif action == "PAUSE_ALL_HARVESTS":
            # Emergency stop - pause all papers
            await db.execute(
                update(Paper)
                .where(Paper.harvest_paused == False)
                .values(harvest_paused=True)
            )
            await db.commit()
            result = {"executed": True, "details": "Paused all harvests (emergency stop)"}

        elif action == "NO_ACTION":
            result = {"executed": True, "details": "No action taken (by design)"}

        else:
            result = {"executed": False, "reason": f"Unknown action: {action}"}

        # Record action time for cooldown
        if result.get("executed"):
            _last_action_times[action] = datetime.utcnow()

    except Exception as e:
        logger.error(f"Action execution failed: {e}")
        result = {"executed": False, "error": str(e)}

    return result


async def run_health_check() -> Optional[HealthMonitorLog]:
    """
    Run a single health check cycle.
    Returns the log entry if a diagnosis was made, None otherwise.
    """
    from ..database import async_session

    async with async_session() as db:
        # 1. Check if we should trigger
        diagnostics = await collect_diagnostics(db)

        activity = diagnostics.get("activity_stats", {})
        stats_15m = activity.get("15min", {})
        citations_15m = stats_15m.get("citations_saved", 0)

        running_count = len(diagnostics.get("running_jobs", []))
        pending_count = diagnostics.get("pending_jobs_count", 0)
        active_jobs = running_count + pending_count

        # Trigger condition: active jobs > 0 AND citations_15m == 0
        if active_jobs == 0:
            logger.debug("Health monitor: No active jobs, skipping")
            return None

        if citations_15m > 0:
            logger.debug(f"Health monitor: {citations_15m} citations in 15min, all good")
            return None

        logger.warning(f"Health monitor TRIGGERED: {active_jobs} active jobs but 0 citations in 15min")

        # 2. Create log entry
        log_entry = HealthMonitorLog(
            trigger_reason="zero_citations_15min",
            active_jobs_count=active_jobs,
            citations_15min=citations_15m,
            diagnostic_data=json.dumps(diagnostics, default=str),
        )

        # 3. Call LLM for diagnosis
        prompt = build_llm_prompt(diagnostics)
        llm_response = await call_llm_diagnosis(prompt)

        log_entry.llm_model = LLM_MODEL
        log_entry.llm_diagnosis = llm_response.get("diagnosis")
        log_entry.llm_root_cause = llm_response.get("root_cause")
        log_entry.llm_confidence = llm_response.get("confidence")
        log_entry.llm_raw_response = llm_response.get("_raw_response")
        log_entry.llm_call_duration_ms = llm_response.get("_duration_ms")

        action = llm_response.get("action", "NO_ACTION")
        params = llm_response.get("params", {})
        log_entry.action_type = action
        log_entry.action_params = json.dumps(params) if params else None

        logger.info(f"Health monitor diagnosis: {llm_response.get('diagnosis')} -> Action: {action}")

        # 4. Execute action (unless dry run)
        if settings.health_monitor_dry_run:
            log_entry.action_executed = False
            log_entry.action_result = "Dry run - action not executed"
            logger.info("Health monitor: Dry run mode, skipping action execution")
        else:
            start_time = time.time()
            action_result = await execute_action(db, action, params)
            log_entry.action_duration_ms = int((time.time() - start_time) * 1000)
            log_entry.action_executed = action_result.get("executed", False)
            log_entry.action_result = json.dumps(action_result.get("details")) if action_result.get("details") else None
            log_entry.action_error = action_result.get("error") or action_result.get("reason")

            if log_entry.action_executed:
                logger.info(f"Health monitor action executed: {action} -> {action_result.get('details')}")
            else:
                logger.warning(f"Health monitor action not executed: {action_result.get('reason') or action_result.get('error')}")

        # 5. Save log entry
        db.add(log_entry)
        await db.commit()
        await db.refresh(log_entry)

        return log_entry


async def health_monitor_loop():
    """
    Main loop that runs health checks periodically.
    """
    logger.info(f"Health monitor started (interval: {settings.health_monitor_interval_minutes} min)")

    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

            if not settings.health_monitor_enabled:
                logger.debug("Health monitor disabled, skipping check")
                continue

            await run_health_check()

        except asyncio.CancelledError:
            logger.info("Health monitor cancelled")
            raise
        except Exception as e:
            logger.error(f"Health monitor error: {e}")
            await asyncio.sleep(60)  # Wait a bit before retrying


async def start_health_monitor():
    """Start the health monitor background task."""
    global _monitor_task

    if not settings.health_monitor_enabled:
        logger.info("Health monitor disabled by config")
        return

    if _monitor_task is None or _monitor_task.done():
        _monitor_task = asyncio.create_task(health_monitor_loop())
        logger.info("Health monitor task started")


async def stop_health_monitor():
    """Stop the health monitor background task."""
    global _monitor_task

    if _monitor_task:
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass
        _monitor_task = None
        logger.info("Health monitor task stopped")


async def trigger_manual_check() -> Optional[HealthMonitorLog]:
    """
    Manually trigger a health check (for API endpoint).
    Bypasses the normal trigger conditions.
    """
    from ..database import async_session

    async with async_session() as db:
        diagnostics = await collect_diagnostics(db)

        activity = diagnostics.get("activity_stats", {})
        stats_15m = activity.get("15min", {})
        citations_15m = stats_15m.get("citations_saved", 0)

        running_count = len(diagnostics.get("running_jobs", []))
        pending_count = diagnostics.get("pending_jobs_count", 0)
        active_jobs = running_count + pending_count

        # Create log entry
        log_entry = HealthMonitorLog(
            trigger_reason="manual_trigger",
            active_jobs_count=active_jobs,
            citations_15min=citations_15m,
            diagnostic_data=json.dumps(diagnostics, default=str),
        )

        # Call LLM for diagnosis
        prompt = build_llm_prompt(diagnostics)
        llm_response = await call_llm_diagnosis(prompt)

        log_entry.llm_model = LLM_MODEL
        log_entry.llm_diagnosis = llm_response.get("diagnosis")
        log_entry.llm_root_cause = llm_response.get("root_cause")
        log_entry.llm_confidence = llm_response.get("confidence")
        log_entry.llm_raw_response = llm_response.get("_raw_response")
        log_entry.llm_call_duration_ms = llm_response.get("_duration_ms")

        action = llm_response.get("action", "NO_ACTION")
        params = llm_response.get("params", {})
        log_entry.action_type = action
        log_entry.action_params = json.dumps(params) if params else None

        # Execute action (unless dry run)
        if settings.health_monitor_dry_run:
            log_entry.action_executed = False
            log_entry.action_result = "Dry run - action not executed"
        else:
            start_time = time.time()
            action_result = await execute_action(db, action, params)
            log_entry.action_duration_ms = int((time.time() - start_time) * 1000)
            log_entry.action_executed = action_result.get("executed", False)
            log_entry.action_result = json.dumps(action_result.get("details")) if action_result.get("details") else None
            log_entry.action_error = action_result.get("error") or action_result.get("reason")

        # Save log entry
        db.add(log_entry)
        await db.commit()
        await db.refresh(log_entry)

        return log_entry


async def get_recent_logs(db: AsyncSession, limit: int = 20) -> List[HealthMonitorLog]:
    """Get recent health monitor logs."""
    result = await db.execute(
        select(HealthMonitorLog)
        .order_by(HealthMonitorLog.created_at.desc())
        .limit(limit)
    )
    return result.scalars().all()
