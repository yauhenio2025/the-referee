"""
Edition Analysis Orchestrator

This module coordinates the full edition analysis pipeline for a dossier:
1. Inventory - analyze all papers/editions in the dossier
2. Bibliographic Research - use Claude to research thinker's bibliography
3. Edition Linking - match papers to Works
4. Gap Analysis - identify missing translations
5. Job Generation - create scraper jobs for gaps

This is the main entry point called by the API routes.

Created by RECONCILER to wire together Phase 1-5 services.
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..models import (
    Dossier, Paper, Edition, Job,
    Work, WorkEdition, MissingEdition, EditionAnalysisRun, EditionAnalysisLLMCall
)
from .inventory_service import InventoryService
from .bibliographic_agent import BibliographicAgent
from .edition_linking_service import EditionLinkingService
from .gap_analysis_service import GapAnalysisService

logger = logging.getLogger(__name__)


class EditionAnalysisOrchestrator:
    """
    Coordinates the full edition analysis pipeline.

    Manages the lifecycle of an EditionAnalysisRun and orchestrates
    all Phase 1-5 services to complete the analysis.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize orchestrator with database session.

        Args:
            db: Async database session
        """
        self.db = db

    async def start_analysis(
        self,
        dossier_id: int,
        force_refresh: bool = False
    ) -> EditionAnalysisRun:
        """
        Start a new edition analysis run for a dossier.

        Args:
            dossier_id: The dossier to analyze
            force_refresh: If True, ignores existing analysis and starts fresh

        Returns:
            The created EditionAnalysisRun record

        Raises:
            ValueError: If dossier not found or analysis already in progress
        """
        # Verify dossier exists
        result = await self.db.execute(
            select(Dossier).where(Dossier.id == dossier_id)
        )
        dossier = result.scalar_one_or_none()
        if not dossier:
            raise ValueError(f"Dossier {dossier_id} not found")

        # Check for existing in-progress run
        existing = await self.db.execute(
            select(EditionAnalysisRun)
            .where(EditionAnalysisRun.dossier_id == dossier_id)
            .where(EditionAnalysisRun.status.in_(['pending', 'inventorying', 'researching', 'linking', 'analyzing_gaps', 'generating_jobs']))
        )
        existing_run = existing.scalar_one_or_none()
        if existing_run and not force_refresh:
            raise ValueError(f"Analysis already in progress (run_id={existing_run.id}, status={existing_run.status})")

        # Create new run
        run = EditionAnalysisRun(
            dossier_id=dossier_id,
            thinker_name=dossier.name,
            status="pending",
            phase="Initializing",
            phase_progress=0.0,
        )
        self.db.add(run)
        await self.db.commit()
        await self.db.refresh(run)

        logger.info(f"Created edition analysis run {run.id} for dossier {dossier_id} ({dossier.name})")
        return run

    async def run_analysis(self, run_id: int) -> EditionAnalysisRun:
        """
        Execute the full analysis pipeline.

        This is the main entry point that runs all phases sequentially.
        Updates the run status throughout and handles errors gracefully.

        Args:
            run_id: The EditionAnalysisRun to execute

        Returns:
            The completed (or failed) EditionAnalysisRun
        """
        # Fetch run with dossier
        result = await self.db.execute(
            select(EditionAnalysisRun)
            .options(selectinload(EditionAnalysisRun.dossier))
            .where(EditionAnalysisRun.id == run_id)
        )
        run = result.scalar_one_or_none()
        if not run:
            raise ValueError(f"EditionAnalysisRun {run_id} not found")

        run.started_at = datetime.utcnow()
        await self.db.commit()

        try:
            # Phase 1: Inventory
            inventory = await self._run_inventory_phase(run)

            # Phase 2: Bibliographic Research
            bibliography = await self._run_bibliographic_phase(run)

            # Phase 3: Edition Linking
            linking_result = await self._run_linking_phase(run, inventory, bibliography)

            # Phase 4: Gap Analysis
            gaps = await self._run_gap_analysis_phase(run, bibliography)

            # Phase 5: Job Generation
            await self._run_job_generation_phase(run, gaps)

            # Complete
            run.status = "completed"
            run.phase = "Complete"
            run.phase_progress = 1.0
            run.completed_at = datetime.utcnow()
            await self.db.commit()

            logger.info(f"Edition analysis run {run_id} completed successfully")

        except Exception as e:
            logger.error(f"Edition analysis run {run_id} failed: {e}", exc_info=True)
            run.status = "failed"
            run.error = str(e)
            run.error_phase = run.phase
            await self.db.commit()

        return run

    async def _run_inventory_phase(self, run: EditionAnalysisRun) -> Dict[str, Any]:
        """
        Phase 1: Build inventory of papers/editions in the dossier.

        Uses InventoryService to analyze what's in the dossier.
        """
        run.status = "inventorying"
        run.phase = "Building inventory"
        run.phase_progress = 0.0
        await self.db.commit()

        # Create service with db session
        inventory_service = InventoryService(self.db)

        # Analyze dossier
        inventory = await inventory_service.analyze_dossier(run.dossier_id)

        # Update run stats
        run.papers_analyzed = inventory.paper_count
        run.editions_analyzed = inventory.edition_count
        run.phase_progress = 1.0
        await self.db.commit()

        logger.info(f"Inventory phase complete: {run.papers_analyzed} papers, {run.editions_analyzed} editions")

        # Convert to dict format for other phases
        return {
            'thinker_name': inventory.thinker_name,
            'papers': [
                {
                    'paper_id': p.paper_id,
                    'title': p.title,
                    'detected_language': p.detected_language,
                    'authors': p.authors,
                    'year': p.year,
                    'editions': [
                        {
                            'edition_id': e.edition_id,
                            'title': e.title,
                            'detected_language': e.detected_language,
                        }
                        for e in p.editions
                    ] if hasattr(p, 'editions') else []
                }
                for p in inventory.papers
            ],
            'title_clusters': [
                {
                    'cluster_id': c.cluster_id,
                    'representative_title': c.representative_title,
                    'titles': c.titles,
                    'paper_ids': c.paper_ids,
                    'languages': c.languages,
                }
                for c in inventory.title_clusters
            ] if inventory.title_clusters else [],
            'language_distribution': inventory.language_distribution,
            'total_papers': inventory.total_papers,
            'total_editions': inventory.total_editions,
        }

    async def _run_bibliographic_phase(self, run: EditionAnalysisRun) -> Dict[str, Any]:
        """
        Phase 2: Research thinker's bibliography using Claude.

        Uses BibliographicAgent to get comprehensive bibliography with extended thinking.
        """
        run.status = "researching"
        run.phase = "Researching bibliography"
        run.phase_progress = 0.0
        await self.db.commit()

        # Create agent
        agent = BibliographicAgent()

        # Research bibliography
        bibliography = await agent.research_thinker(run.thinker_name)

        # Store LLM call logs
        for log_entry in agent.get_llm_calls():
            llm_call = EditionAnalysisLLMCall(
                run_id=run.id,
                phase="bibliographic_research",
                call_number=log_entry.get('call_number', 1),
                purpose=log_entry.get('purpose', 'Bibliography research'),
                model=log_entry.get('model', 'claude-opus-4-5-20251101'),
                prompt=log_entry.get('prompt', ''),
                context_json=log_entry.get('context_json'),
                raw_response=log_entry.get('response', ''),
                thinking_text=log_entry.get('thinking', ''),
                thinking_tokens=log_entry.get('thinking_tokens', 0),
                input_tokens=log_entry.get('input_tokens', 0),
                output_tokens=log_entry.get('output_tokens', 0),
                web_search_used=log_entry.get('web_search_used', False),
                latency_ms=log_entry.get('latency_ms', 0),
                status='completed',
            )
            self.db.add(llm_call)

            # Update aggregate stats
            run.llm_calls_count += 1
            run.total_input_tokens += log_entry.get('input_tokens', 0)
            run.total_output_tokens += log_entry.get('output_tokens', 0)
            run.thinking_tokens += log_entry.get('thinking_tokens', 0)
            if log_entry.get('web_search_used', False):
                run.web_searches_count += 1

        run.works_identified = len(bibliography.get('major_works', []))
        run.phase_progress = 1.0
        await self.db.commit()

        logger.info(f"Bibliographic phase complete: {run.works_identified} major works identified")
        return bibliography

    async def _run_linking_phase(
        self,
        run: EditionAnalysisRun,
        inventory: Dict[str, Any],
        bibliography: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Phase 3: Link inventory items to Works.

        Uses EditionLinkingService to match papers/editions to abstract Works.
        """
        run.status = "linking"
        run.phase = "Linking editions to works"
        run.phase_progress = 0.0
        await self.db.commit()

        # Create service with db session
        linking_service = EditionLinkingService(self.db)

        # Link editions to works
        linking_result = await linking_service.link_editions_to_works(
            inventory=inventory,
            bibliography=bibliography,
            run_id=run.id
        )

        run.links_created = linking_result.get('links_created', 0)
        run.works_identified = linking_result.get('works_created', run.works_identified)
        run.phase_progress = 1.0
        await self.db.commit()

        logger.info(f"Linking phase complete: {run.links_created} links created")
        return linking_result

    async def _run_gap_analysis_phase(
        self,
        run: EditionAnalysisRun,
        bibliography: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Phase 4: Identify gaps (missing translations).

        Uses GapAnalysisService to find what's missing.
        """
        run.status = "analyzing_gaps"
        run.phase = "Identifying gaps"
        run.phase_progress = 0.0
        await self.db.commit()

        # Create service with db session
        gap_service = GapAnalysisService(self.db)

        # Analyze gaps
        gaps = await gap_service.analyze_gaps(
            dossier_id=run.dossier_id,
            bibliography=bibliography,
            run_id=run.id,
        )

        run.gaps_found = len(gaps.get('missing_translations', [])) + len(gaps.get('missing_works', []))
        run.phase_progress = 1.0
        await self.db.commit()

        logger.info(f"Gap analysis phase complete: {run.gaps_found} gaps found")
        return gaps

    async def _run_job_generation_phase(
        self,
        run: EditionAnalysisRun,
        gaps: Dict[str, Any]
    ) -> None:
        """
        Phase 5: Generate scraper jobs for gaps.

        Uses GapAnalysisService to create jobs.
        """
        run.status = "generating_jobs"
        run.phase = "Generating jobs"
        run.phase_progress = 0.0
        await self.db.commit()

        # Create service with db session
        gap_service = GapAnalysisService(self.db)

        # Generate jobs
        jobs = await gap_service.generate_scraper_jobs(
            gaps=gaps,
            dossier_id=run.dossier_id,
            thinker_name=run.thinker_name
        )

        run.jobs_created = len(jobs)
        run.phase_progress = 1.0
        await self.db.commit()

        logger.info(f"Job generation phase complete: {run.jobs_created} jobs created")

    async def get_run_status(self, run_id: int) -> Optional[EditionAnalysisRun]:
        """
        Get the current status of an analysis run.
        """
        result = await self.db.execute(
            select(EditionAnalysisRun)
            .where(EditionAnalysisRun.id == run_id)
        )
        return result.scalar_one_or_none()

    async def get_latest_run(self, dossier_id: int) -> Optional[EditionAnalysisRun]:
        """
        Get the most recent analysis run for a dossier.
        """
        result = await self.db.execute(
            select(EditionAnalysisRun)
            .where(EditionAnalysisRun.dossier_id == dossier_id)
            .order_by(EditionAnalysisRun.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


async def run_edition_analysis_background(
    db: AsyncSession,
    run_id: int,
) -> None:
    """
    Background task to run edition analysis.

    This is called by the API route as a background task so the
    POST returns immediately with the run_id.
    """
    orchestrator = EditionAnalysisOrchestrator(db)
    await orchestrator.run_analysis(run_id)
