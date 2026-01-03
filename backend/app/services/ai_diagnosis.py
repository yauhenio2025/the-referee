"""
AI-Powered Harvest Diagnosis Service

Uses Claude Opus 4.5 with extended thinking to analyze stalled harvests
and provide actionable recommendations.
"""
import logging
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import anthropic

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func

from ..config import get_settings
from ..models import (
    Paper, Edition, Job, HarvestTarget, FailedFetch,
    PartitionRun, PartitionQuery, Citation
)

logger = logging.getLogger(__name__)
settings = get_settings()


class HarvestDiagnosisService:
    """AI-powered diagnosis of harvest issues"""

    def __init__(self):
        # Use AsyncAnthropic for async context
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        # Use Opus 4.5 for complex reasoning
        self.model = "claude-opus-4-5-20251101"

    async def diagnose_edition(
        self,
        db: AsyncSession,
        edition_id: int,
        thinking_budget: int = 32000
    ) -> Dict[str, Any]:
        """
        Perform comprehensive AI diagnosis of an edition's harvest status.

        Collects all relevant data and sends to Claude Opus 4.5 for analysis.
        Returns actionable recommendations.
        """
        logger.info(f"Starting AI diagnosis for edition {edition_id}")

        # Collect all relevant data
        context = await self._collect_diagnosis_context(db, edition_id)

        if "error" in context:
            return context

        # Build the analysis prompt
        prompt = self._build_diagnosis_prompt(context)

        # Call Opus 4.5 with extended thinking
        try:
            logger.info(f"Calling Opus 4.5 with {thinking_budget} thinking tokens...")

            # Use streaming for extended thinking (as per CLAUDE.md guidance)
            full_thinking = ""
            full_response = ""

            async with self.client.messages.stream(
                model=self.model,
                max_tokens=thinking_budget + 16000,  # Must be > thinking budget
                thinking={
                    "type": "enabled",
                    "budget_tokens": thinking_budget
                },
                messages=[{"role": "user", "content": prompt}]
            ) as stream:
                async for event in stream:
                    if hasattr(event, 'type'):
                        if event.type == 'content_block_delta':
                            if hasattr(event.delta, 'thinking'):
                                full_thinking += event.delta.thinking
                            elif hasattr(event.delta, 'text'):
                                full_response += event.delta.text

            # Parse the response
            analysis = self._parse_analysis_response(full_response, full_thinking)

            return {
                "success": True,
                "edition_id": edition_id,
                "paper_title": context["paper"]["title"],
                "edition_title": context["edition"]["title"],
                "context_summary": {
                    "expected": context["edition"]["expected_citations"],
                    "harvested": context["edition"]["harvested_citations"],
                    "gap": context["edition"]["gap"],
                    "gap_percent": context["edition"]["gap_percent"],
                    "years_total": len(context["harvest_targets"]),
                    "years_complete": sum(1 for ht in context["harvest_targets"] if ht["status"] == "complete"),
                    "recent_jobs": len(context["recent_jobs"]),
                },
                "analysis": analysis,
                "raw_thinking": full_thinking[:5000] + "..." if len(full_thinking) > 5000 else full_thinking,
            }

        except Exception as e:
            logger.error(f"AI diagnosis failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "edition_id": edition_id,
            }

    async def _collect_diagnosis_context(
        self,
        db: AsyncSession,
        edition_id: int
    ) -> Dict[str, Any]:
        """Collect all relevant data for diagnosis"""

        # Get edition with paper
        edition_result = await db.execute(
            select(Edition).where(Edition.id == edition_id)
        )
        edition = edition_result.scalar_one_or_none()

        if not edition:
            return {"error": f"Edition {edition_id} not found"}

        # Get paper
        paper_result = await db.execute(
            select(Paper).where(Paper.id == edition.paper_id)
        )
        paper = paper_result.scalar_one_or_none()

        # Get all harvest targets for this edition
        ht_result = await db.execute(
            select(HarvestTarget)
            .where(HarvestTarget.edition_id == edition_id)
            .order_by(HarvestTarget.year)
        )
        harvest_targets = ht_result.scalars().all()

        # Get recent jobs for this paper (last 30 jobs)
        jobs_result = await db.execute(
            select(Job)
            .where(Job.paper_id == edition.paper_id)
            .order_by(desc(Job.created_at))
            .limit(30)
        )
        recent_jobs = jobs_result.scalars().all()

        # Get failed fetches for this edition
        ff_result = await db.execute(
            select(FailedFetch)
            .where(FailedFetch.edition_id == edition_id)
            .order_by(desc(FailedFetch.created_at))
            .limit(20)
        )
        failed_fetches = ff_result.scalars().all()

        # Get partition runs for this edition
        pr_result = await db.execute(
            select(PartitionRun)
            .where(PartitionRun.edition_id == edition_id)
            .order_by(desc(PartitionRun.created_at))
            .limit(10)
        )
        partition_runs = pr_result.scalars().all()

        # Get citation distribution by year
        citation_dist = await db.execute(
            select(
                Citation.year,
                func.count(Citation.id).label('count')
            )
            .where(Citation.edition_id == edition_id)
            .group_by(Citation.year)
            .order_by(Citation.year)
        )
        citation_by_year = {row.year: row.count for row in citation_dist.fetchall()}

        # Parse resume state if exists
        resume_state = None
        if edition.harvest_resume_state:
            try:
                resume_state = json.loads(edition.harvest_resume_state)
            except:
                pass

        return {
            "paper": {
                "id": paper.id,
                "title": paper.title,
                "authors": paper.authors,
                "year": paper.year,
                "total_harvested": paper.total_harvested_citations,
                "harvest_paused": paper.harvest_paused,
            },
            "edition": {
                "id": edition.id,
                "title": edition.title,
                "scholar_id": edition.scholar_id,
                "language": edition.language,
                "expected_citations": edition.citation_count,
                "harvested_citations": edition.harvested_citation_count,
                "gap": edition.citation_count - edition.harvested_citation_count,
                "gap_percent": round((edition.citation_count - edition.harvested_citation_count) / max(edition.citation_count, 1) * 100, 1),
                "last_harvested_at": edition.last_harvested_at.isoformat() if edition.last_harvested_at else None,
                "stall_count": edition.harvest_stall_count,
                "harvest_complete": edition.harvest_complete,
                "harvest_complete_reason": edition.harvest_complete_reason,
                "resume_state": resume_state,
            },
            "harvest_targets": [
                {
                    "year": ht.year,
                    "expected": ht.expected_count,
                    "actual": ht.actual_count,
                    "gap": ht.expected_count - ht.actual_count,
                    "status": ht.status,
                    "pages_attempted": ht.pages_attempted,
                    "pages_succeeded": ht.pages_succeeded,
                    "pages_failed": ht.pages_failed,
                    "is_overflow": ht.expected_count > 1000,
                    "updated_at": ht.updated_at.isoformat() if ht.updated_at else None,
                }
                for ht in harvest_targets
            ],
            "citation_distribution": citation_by_year,
            "recent_jobs": [
                {
                    "id": job.id,
                    "type": job.job_type,
                    "status": job.status,
                    "progress": job.progress,
                    "progress_message": job.progress_message,
                    "error": job.error,
                    "created_at": job.created_at.isoformat(),
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    "duration_minutes": (
                        (job.completed_at - job.started_at).total_seconds() / 60
                        if job.completed_at and job.started_at else None
                    ),
                    "params": json.loads(job.params) if job.params else None,
                    "result": self._parse_job_result(job.result),
                }
                for job in recent_jobs
            ],
            "failed_fetches": [
                {
                    "year": ff.year,
                    "page_number": ff.page_number,
                    "retry_count": ff.retry_count,
                    "status": ff.status,
                    "last_error": ff.last_error,
                    "created_at": ff.created_at.isoformat(),
                }
                for ff in failed_fetches
            ],
            "partition_runs": [
                {
                    "year": pr.year,
                    "status": pr.status,
                    "initial_count": pr.initial_count,
                    "exclusion_harvested": pr.exclusion_harvested,
                    "inclusion_harvested": pr.inclusion_harvested,
                    "total_new_unique": pr.total_new_unique,
                    "error_message": pr.error_message,
                    "created_at": pr.created_at.isoformat(),
                }
                for pr in partition_runs
            ],
        }

    def _parse_job_result(self, result_str: Optional[str]) -> Optional[Dict]:
        """Parse job result JSON, extracting key metrics"""
        if not result_str:
            return None
        try:
            result = json.loads(result_str)
            # Extract only the most relevant fields
            return {
                "citations_saved": result.get("citations_saved"),
                "citations_found": result.get("citations_found"),
                "duplicates": result.get("duplicates"),
                "duplicate_rate": result.get("duplicate_rate"),
                "pages_processed": result.get("pages_processed"),
                "editions_processed": result.get("editions_processed"),
                "years_processed": result.get("years_processed"),
            }
        except:
            return None

    def _build_diagnosis_prompt(self, context: Dict[str, Any]) -> str:
        """Build comprehensive diagnosis prompt"""

        # Format harvest targets as a table
        ht_table = "Year | Expected | Actual | Gap | Status | Pages | Overflow\n"
        ht_table += "-" * 70 + "\n"
        for ht in context["harvest_targets"]:
            ht_table += f"{ht['year'] or 'ALL'} | {ht['expected']} | {ht['actual']} | {ht['gap']} | {ht['status']} | {ht['pages_attempted']}/{ht['pages_succeeded']}/{ht['pages_failed']} | {'YES' if ht['is_overflow'] else 'no'}\n"

        # Format recent jobs
        jobs_info = ""
        for job in context["recent_jobs"][:15]:  # Last 15 jobs
            jobs_info += f"\n  Job #{job['id']} ({job['type']}) - {job['status']}"
            if job['duration_minutes']:
                jobs_info += f" - {job['duration_minutes']:.1f}min"
            if job['result']:
                r = job['result']
                jobs_info += f"\n    Saved: {r.get('citations_saved', '?')}, Dups: {r.get('duplicates', '?')}, Rate: {r.get('duplicate_rate', '?')}"
            if job['error']:
                jobs_info += f"\n    ERROR: {job['error'][:200]}"

        # Format failed fetches
        ff_info = ""
        if context["failed_fetches"]:
            ff_info = "\nFailed Page Fetches:\n"
            for ff in context["failed_fetches"][:10]:
                ff_info += f"  Year {ff['year']} Page {ff['page_number']}: {ff['status']} (retries: {ff['retry_count']})\n"
                if ff['last_error']:
                    ff_info += f"    Error: {ff['last_error'][:100]}\n"

        # Format partition runs
        pr_info = ""
        if context["partition_runs"]:
            pr_info = "\nPartition Harvest Attempts:\n"
            for pr in context["partition_runs"]:
                pr_info += f"  Year {pr['year']}: {pr['status']} - Initial: {pr['initial_count']}, Harvested: {pr['total_new_unique']} new\n"
                if pr['error_message']:
                    pr_info += f"    Error: {pr['error_message'][:150]}\n"

        prompt = f"""You are an expert at diagnosing Google Scholar citation harvesting issues.

CONTEXT: I'm harvesting citations for an academic paper. The harvest is STALLED - we have a significant gap between expected and actual citations, and recent jobs aren't making progress.

YOUR TASK: Analyze ALL the data below and tell me:
1. What is the ROOT CAUSE of the stall? (Be specific - which years, which pages, what pattern)
2. What SPECIFIC ACTION should I take? (Include exact year, page number to resume from)
3. Is the gap likely RECOVERABLE or is it Google Scholar data inconsistency?

=== EDITION INFO ===
Paper: {context['paper']['title']}
Edition: {context['edition']['title']} (ID: {context['edition']['id']})
Scholar ID: {context['edition']['scholar_id']}
Language: {context['edition']['language']}

Expected Citations: {context['edition']['expected_citations']}
Harvested Citations: {context['edition']['harvested_citations']}
GAP: {context['edition']['gap']} ({context['edition']['gap_percent']}%)

Stall Count: {context['edition']['stall_count']} (consecutive zero-progress jobs)
Harvest Complete: {context['edition']['harvest_complete']} ({context['edition']['harvest_complete_reason'] or 'N/A'})
Last Harvested: {context['edition']['last_harvested_at'] or 'Never'}

Resume State: {json.dumps(context['edition']['resume_state'], indent=2) if context['edition']['resume_state'] else 'None'}

=== PER-YEAR HARVEST STATUS ===
{ht_table}

=== CITATION DISTRIBUTION BY YEAR ===
(What we actually have in database)
{json.dumps(context['citation_distribution'], indent=2)}

=== RECENT JOB HISTORY ===
{jobs_info}
{ff_info}
{pr_info}

=== ANALYSIS REQUIRED ===

Think through this step by step:

1. COMPLETENESS ANALYSIS
   - Which years are marked complete but might have gaps?
   - Which years are incomplete and have room for more citations?
   - Any overflow years (>1000 citations) that need special handling?

2. JOB HISTORY ANALYSIS
   - What pattern do you see in recent jobs?
   - High duplicate rates? (indicates resume position bug)
   - Frequent failures? (indicates blocking or network issues)
   - Jobs completing with 0 new citations?

3. PAGE/POSITION ANALYSIS
   - Where exactly did harvesting stop?
   - Can we calculate the correct resume page?
   - Are there any years we haven't tried at all?

4. ROOT CAUSE DETERMINATION
   One of:
   - RESUME_BUG: Job is restarting from wrong position
   - RATE_LIMITING: Google Scholar is blocking us
   - OVERFLOW_YEAR: Year has >1000 citations, needs partitioning
   - GS_INCONSISTENCY: GS's reported count is inflated/wrong
   - INCOMPLETE_YEARS: We haven't tried all years yet
   - NETWORK_ISSUES: Page fetches failing
   - OTHER: Something else

5. RECOMMENDED ACTION
   Be VERY SPECIFIC. Don't say "restart the harvest". Say:
   - "Resume from year 2016, page 47"
   - "Run partition harvest for year 2020"
   - "Mark as complete - gap is GS data issue"
   - "Reset resume state and start from year X"

Output your analysis as JSON:
{{
    "root_cause": "RESUME_BUG | RATE_LIMITING | OVERFLOW_YEAR | GS_INCONSISTENCY | INCOMPLETE_YEARS | NETWORK_ISSUES | OTHER",
    "root_cause_explanation": "Detailed explanation of what's happening",
    "gap_recoverable": true/false,
    "gap_recoverable_explanation": "Why you think the gap is or isn't recoverable",
    "recommended_action": {{
        "action_type": "RESUME | PARTITION | RESET | MARK_COMPLETE | WAIT | MANUAL_REVIEW",
        "action_description": "Human-readable description",
        "specific_params": {{
            "start_year": 2016,
            "start_page": 47,
            "skip_years": [2010, 2011, 2012],
            "partition_years": [2020]
        }}
    }},
    "confidence": "HIGH | MEDIUM | LOW",
    "additional_notes": "Any other observations or caveats"
}}

IMPORTANT: Output ONLY the JSON object, no other text."""

        return prompt

    def _parse_analysis_response(
        self,
        response_text: str,
        thinking_text: str
    ) -> Dict[str, Any]:
        """Parse the AI analysis response"""
        try:
            # Try to extract JSON from response
            response_text = response_text.strip()

            # Handle markdown code blocks
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                # Remove first and last lines (``` markers)
                json_lines = []
                in_json = False
                for line in lines:
                    if line.startswith("```") and not in_json:
                        in_json = True
                        continue
                    elif line.startswith("```") and in_json:
                        break
                    elif in_json:
                        json_lines.append(line)
                response_text = "\n".join(json_lines)

            analysis = json.loads(response_text)

            # Add thinking summary
            analysis["thinking_summary"] = thinking_text[:2000] if thinking_text else None

            return analysis

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse AI response as JSON: {e}")
            return {
                "parse_error": True,
                "raw_response": response_text[:3000],
                "thinking_summary": thinking_text[:2000] if thinking_text else None,
            }


# Singleton instance
_diagnosis_service: Optional[HarvestDiagnosisService] = None


def get_diagnosis_service() -> HarvestDiagnosisService:
    """Get or create diagnosis service singleton"""
    global _diagnosis_service
    if _diagnosis_service is None:
        _diagnosis_service = HarvestDiagnosisService()
    return _diagnosis_service
