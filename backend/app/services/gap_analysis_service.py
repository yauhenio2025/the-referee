"""
Gap Analysis & Job Generation Service - Phase 5

This service:
1. Compares linked Works against bibliography expectations
2. Identifies missing translations
3. Identifies missing major works
4. Generates scraper jobs to fill gaps

Dependencies:
- Phase 1: Work, WorkEdition, MissingEdition models
- Phase 2: DossierInventory data structure
- Phase 3: ThinkerBibliography data structure
- Phase 4: EditionLinkingService (creates Work/WorkEdition records)
"""
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, TypedDict, Set
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Job, Paper, Edition, Dossier

logger = logging.getLogger(__name__)


# ============== Type Definitions ==============
# These mirror the interfaces from Phase 2, 3, 4

class ThinkerInfo(TypedDict):
    canonical_name: str
    birth_death: Optional[str]
    primary_language: str
    domains: List[str]


class KnownTranslation(TypedDict):
    language: str
    title: str
    year: Optional[int]
    translator: Optional[str]
    source: str  # "llm_knowledge", "web_search", "scholar"


class MajorWork(TypedDict):
    canonical_title: str
    original_language: str
    original_title: str
    original_year: Optional[int]
    work_type: str  # book, essay, etc.
    importance: str  # major, minor
    known_translations: List[KnownTranslation]
    scholarly_significance: Optional[str]


class ThinkerBibliography(TypedDict):
    """Output from Phase 3 BibliographicAgent"""
    thinker: ThinkerInfo
    major_works: List[MajorWork]
    verification_sources: List[str]
    confidence: float


class EditionInfo(TypedDict):
    edition_id: int
    title: str
    language: Optional[str]
    year: Optional[int]


class PaperInfo(TypedDict):
    paper_id: int
    title: str
    authors: List[str]
    editions: List[EditionInfo]


class TitleCluster(TypedDict):
    canonical_title: str
    papers: List[int]  # paper_ids
    languages: List[str]
    years: List[int]


class DossierInventory(TypedDict):
    """Output from Phase 2 InventoryService"""
    thinker_name: str
    papers: List[PaperInfo]
    title_clusters: List[TitleCluster]


class MissingTranslation(TypedDict):
    work_canonical_title: str
    original_language: str
    missing_language: str
    expected_title: str
    expected_year: Optional[int]
    priority: str  # high, medium, low
    source: str  # "llm_knowledge", "web_search", "scholar"


class MissingWork(TypedDict):
    canonical_title: str
    importance: str
    reason_missing: str  # "never_scraped", "not_on_scholar"


class OrphanEdition(TypedDict):
    edition_id: int
    title: str


class GapAnalysisResult(TypedDict):
    missing_translations: List[MissingTranslation]
    missing_works: List[MissingWork]
    orphan_editions: List[OrphanEdition]


# ============== Priority Languages ==============
# Languages we prioritize for translations based on scholarly importance
PRIORITY_LANGUAGES = {
    "high": ["english", "german", "french"],
    "medium": ["spanish", "italian", "russian", "portuguese"],
    "low": ["chinese", "japanese", "korean", "arabic", "dutch", "polish"]
}

def get_translation_priority(language: str, importance: str) -> str:
    """
    Determine priority for a missing translation based on language and work importance.

    For major works:
      - English/German/French missing -> high priority
      - Spanish/Italian/Russian missing -> medium priority
      - Others -> low priority

    For minor works:
      - All translations are low priority
    """
    lang_lower = language.lower()

    if importance != "major":
        return "low"

    if lang_lower in PRIORITY_LANGUAGES["high"]:
        return "high"
    elif lang_lower in PRIORITY_LANGUAGES["medium"]:
        return "medium"
    else:
        return "low"


class GapAnalysisService:
    """
    Service for analyzing gaps in a dossier's coverage of a thinker's works.

    Compares what we have (from Phase 2 inventory + Phase 4 linking)
    against what should exist (from Phase 3 bibliographic research)
    to identify missing translations and works.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def analyze_gaps(
        self,
        dossier_id: int,
        bibliography: ThinkerBibliography,
        run_id: int,
        linked_works: Optional[List[Dict[str, Any]]] = None,
    ) -> GapAnalysisResult:
        """
        Analyze gaps between what we have and what should exist.

        Args:
            dossier_id: The dossier being analyzed
            bibliography: ThinkerBibliography from Phase 3
            run_id: EditionAnalysisRun ID for audit logging
            linked_works: Optional list of Work records with their WorkEditions
                         (from Phase 4 linking). If not provided, queries DB directly.

        Returns:
            GapAnalysisResult with missing_translations, missing_works, orphan_editions
        """
        logger.info(f"[GapAnalysis] Starting gap analysis for dossier {dossier_id}")
        logger.info(f"[GapAnalysis] Bibliography has {len(bibliography['major_works'])} major works")

        # Build a map of what we have: canonical_title -> set of languages
        existing_coverage = await self._build_coverage_map(dossier_id, linked_works)

        missing_translations: List[MissingTranslation] = []
        missing_works: List[MissingWork] = []

        # For each major work in the bibliography, check what translations we're missing
        for work in bibliography["major_works"]:
            canonical_title = work["canonical_title"]
            original_language = work["original_language"].lower()
            importance = work.get("importance", "major")

            # Check if we have this work at all
            if canonical_title not in existing_coverage:
                # We're missing this work entirely
                missing_works.append({
                    "canonical_title": canonical_title,
                    "importance": importance,
                    "reason_missing": "never_scraped"  # Most likely reason
                })
                logger.info(f"[GapAnalysis] Missing work: {canonical_title}")
                continue

            # We have some editions - check for missing translations
            existing_languages = existing_coverage[canonical_title]

            # Check original language
            if original_language not in existing_languages:
                priority = get_translation_priority(original_language, importance)
                missing_translations.append({
                    "work_canonical_title": canonical_title,
                    "original_language": original_language,
                    "missing_language": original_language,
                    "expected_title": work.get("original_title", canonical_title),
                    "expected_year": work.get("original_year"),
                    "priority": priority,
                    "source": "llm_knowledge"
                })
                logger.info(f"[GapAnalysis] Missing original ({original_language}): {canonical_title}")

            # Check each known translation
            for translation in work.get("known_translations", []):
                trans_lang = translation["language"].lower()

                if trans_lang not in existing_languages:
                    priority = get_translation_priority(trans_lang, importance)
                    missing_translations.append({
                        "work_canonical_title": canonical_title,
                        "original_language": original_language,
                        "missing_language": trans_lang,
                        "expected_title": translation["title"],
                        "expected_year": translation.get("year"),
                        "priority": priority,
                        "source": translation.get("source", "llm_knowledge")
                    })
                    logger.info(f"[GapAnalysis] Missing translation ({trans_lang}): {translation['title']}")

        # Find orphan editions (editions we have but couldn't place in a work)
        orphan_editions = await self._find_orphan_editions(dossier_id, linked_works)

        result: GapAnalysisResult = {
            "missing_translations": missing_translations,
            "missing_works": missing_works,
            "orphan_editions": orphan_editions
        }

        logger.info(f"[GapAnalysis] Complete: {len(missing_translations)} missing translations, "
                   f"{len(missing_works)} missing works, {len(orphan_editions)} orphan editions")

        return result

    async def _build_coverage_map(
        self,
        dossier_id: int,
        linked_works: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Set[str]]:
        """
        Build a map of canonical_title -> set of languages we have.

        If linked_works is provided (from Phase 4), use those.
        Otherwise, fall back to querying editions directly and doing simple grouping.
        """
        coverage: Dict[str, Set[str]] = {}

        if linked_works:
            # Use the linked works from Phase 4
            for work_record in linked_works:
                canonical_title = work_record.get("canonical_title", "")
                if not canonical_title:
                    continue

                if canonical_title not in coverage:
                    coverage[canonical_title] = set()

                # Each work_record should have work_editions with language info
                for work_edition in work_record.get("work_editions", []):
                    lang = work_edition.get("language", "").lower()
                    if lang:
                        coverage[canonical_title].add(lang)
        else:
            # Fallback: Query editions directly from the dossier
            # This is a simple approach that may not perfectly match works
            result = await self.db.execute(
                select(Paper, Edition)
                .join(Edition, Edition.paper_id == Paper.id)
                .where(Paper.dossier_id == dossier_id)
                .where(Paper.deleted_at.is_(None))
            )
            rows = result.all()

            for paper, edition in rows:
                # Use paper title as a simple proxy for canonical title
                title = paper.title or ""
                if not title:
                    continue

                if title not in coverage:
                    coverage[title] = set()

                lang = (edition.language or "").lower()
                if lang:
                    coverage[title].add(lang)

        return coverage

    async def _find_orphan_editions(
        self,
        dossier_id: int,
        linked_works: Optional[List[Dict[str, Any]]] = None,
    ) -> List[OrphanEdition]:
        """
        Find editions that exist but couldn't be linked to any Work.

        An orphan edition is one that:
        - Exists in the database
        - But has no WorkEdition record linking it to a Work
        """
        orphans: List[OrphanEdition] = []

        if linked_works:
            # Collect all edition_ids that ARE linked
            linked_edition_ids: Set[int] = set()
            for work_record in linked_works:
                for work_edition in work_record.get("work_editions", []):
                    ed_id = work_edition.get("edition_id")
                    if ed_id:
                        linked_edition_ids.add(ed_id)

            # Query all editions in dossier
            result = await self.db.execute(
                select(Edition)
                .join(Paper, Paper.id == Edition.paper_id)
                .where(Paper.dossier_id == dossier_id)
                .where(Paper.deleted_at.is_(None))
            )
            all_editions = result.scalars().all()

            # Find those not in linked set
            for edition in all_editions:
                if edition.id not in linked_edition_ids:
                    orphans.append({
                        "edition_id": edition.id,
                        "title": edition.title or ""
                    })

        return orphans

    async def generate_scraper_jobs(
        self,
        gaps: GapAnalysisResult,
        dossier_id: int,
        thinker_name: str,
    ) -> List[Job]:
        """
        Generate scraper jobs to fill the identified gaps.

        Creates 'discover_editions' jobs with appropriate parameters
        to find the missing translations and works.

        Args:
            gaps: GapAnalysisResult from analyze_gaps
            dossier_id: The dossier we're filling gaps for
            thinker_name: Name of the thinker for search queries

        Returns:
            List of created Job records (persisted to DB)
        """
        logger.info(f"[GapAnalysis] Generating scraper jobs for {len(gaps['missing_translations'])} "
                   f"missing translations and {len(gaps['missing_works'])} missing works")

        created_jobs: List[Job] = []

        # Priority scores for job ordering
        priority_scores = {"high": 100, "medium": 50, "low": 10}

        # Create jobs for missing translations
        for missing in gaps["missing_translations"]:
            priority = priority_scores.get(missing["priority"], 10)

            # Build search query
            search_query = self._build_translation_search_query(
                canonical_title=missing["work_canonical_title"],
                expected_title=missing["expected_title"],
                thinker_name=thinker_name,
                target_language=missing["missing_language"]
            )

            job_params = {
                "gap_type": "missing_translation",
                "search_query": search_query,
                "target_language": missing["missing_language"],
                "expected_title": missing["expected_title"],
                "expected_year": missing["expected_year"],
                "work_canonical_title": missing["work_canonical_title"],
                "original_language": missing["original_language"],
                "source": missing["source"],
                "dossier_id": dossier_id,
                "thinker_name": thinker_name,
            }

            job = Job(
                paper_id=None,  # Not tied to a specific paper
                job_type="discover_editions",
                status="pending",
                priority=priority,
                params=json.dumps(job_params),
            )

            self.db.add(job)
            created_jobs.append(job)
            logger.info(f"[GapAnalysis] Created job for missing {missing['missing_language']} "
                       f"translation of '{missing['work_canonical_title']}' (priority={priority})")

        # Create jobs for missing works (entire works not found)
        for missing_work in gaps["missing_works"]:
            # Major works get higher priority
            priority = 100 if missing_work["importance"] == "major" else 30

            search_query = self._build_work_search_query(
                canonical_title=missing_work["canonical_title"],
                thinker_name=thinker_name
            )

            job_params = {
                "gap_type": "missing_work",
                "search_query": search_query,
                "canonical_title": missing_work["canonical_title"],
                "importance": missing_work["importance"],
                "reason_missing": missing_work["reason_missing"],
                "dossier_id": dossier_id,
                "thinker_name": thinker_name,
            }

            job = Job(
                paper_id=None,
                job_type="discover_editions",
                status="pending",
                priority=priority,
                params=json.dumps(job_params),
            )

            self.db.add(job)
            created_jobs.append(job)
            logger.info(f"[GapAnalysis] Created job for missing work '{missing_work['canonical_title']}' "
                       f"(importance={missing_work['importance']}, priority={priority})")

        # Flush to get job IDs
        await self.db.flush()

        # Refresh to get assigned IDs
        for job in created_jobs:
            await self.db.refresh(job)

        logger.info(f"[GapAnalysis] Created {len(created_jobs)} scraper jobs")
        return created_jobs

    def _build_translation_search_query(
        self,
        canonical_title: str,
        expected_title: str,
        thinker_name: str,
        target_language: str,
    ) -> str:
        """
        Build a Google Scholar search query to find a missing translation.

        Strategy:
        - Use allintitle: with key words from expected title
        - Add author name for disambiguation
        """
        # Extract author surname for query
        surname = thinker_name.split()[-1] if thinker_name else ""

        # Use expected title if available, else canonical
        title_to_search = expected_title if expected_title else canonical_title

        # Extract key words (remove common articles/prepositions)
        stop_words = {"the", "a", "an", "of", "to", "in", "on", "for", "and", "or", "le", "la", "les",
                      "der", "die", "das", "el", "los", "las", "il", "lo", "gli", "de", "du", "des"}

        words = title_to_search.lower().split()
        key_words = [w for w in words if w not in stop_words and len(w) > 2][:4]  # Max 4 key words

        if key_words:
            # Use allintitle with key words + author
            query = f'allintitle:{" ".join(key_words)} author:{surname}'
        else:
            # Fallback: quoted title + author
            query = f'"{title_to_search}" {surname}'

        return query

    def _build_work_search_query(
        self,
        canonical_title: str,
        thinker_name: str,
    ) -> str:
        """
        Build a Google Scholar search query to find a missing work.
        """
        surname = thinker_name.split()[-1] if thinker_name else ""

        # For a missing work, try both the canonical title and author
        # allintitle is too restrictive for long titles
        query = f'"{canonical_title}" author:"{surname}"'

        return query

    async def persist_gap_analysis(
        self,
        dossier_id: int,
        run_id: int,
        gaps: GapAnalysisResult,
        jobs: List[Job],
    ) -> Dict[str, Any]:
        """
        Persist gap analysis results to the database.

        This creates MissingEdition records for each gap found.
        These records track the gap status (pending, job_created, found, dismissed).

        Note: Requires MissingEdition model from Phase 1.
        If Phase 1 models aren't available yet, this is a no-op that logs a warning.
        """
        logger.info(f"[GapAnalysis] Persisting gap analysis results for run {run_id}")

        try:
            # Try to import the MissingEdition model from Phase 1
            from ..models import MissingEdition, Work

            # Create MissingEdition records for missing translations
            for i, missing in enumerate(gaps["missing_translations"]):
                # Find the Work record for this gap (if Phase 4 created it)
                work_id = None
                work_result = await self.db.execute(
                    select(Work).where(
                        Work.thinker_name == missing.get("thinker_name", ""),
                        Work.canonical_title == missing["work_canonical_title"]
                    )
                )
                work = work_result.scalar_one_or_none()
                if work:
                    work_id = work.id

                # Find the corresponding job (if created)
                job_id = None
                if i < len(jobs):
                    job_id = jobs[i].id

                missing_edition = MissingEdition(
                    work_id=work_id,
                    language=missing["missing_language"],
                    expected_title=missing["expected_title"],
                    expected_year=missing.get("expected_year"),
                    source=missing["source"],
                    priority=missing["priority"],
                    status="job_created" if job_id else "pending",
                    job_id=job_id,
                )
                self.db.add(missing_edition)

            await self.db.flush()

            return {
                "persisted": True,
                "missing_translations_saved": len(gaps["missing_translations"]),
                "missing_works_saved": len(gaps["missing_works"]),
            }

        except ImportError:
            logger.warning("[GapAnalysis] MissingEdition model not available (Phase 1 incomplete). "
                          "Gap analysis results will not be persisted to MissingEdition table.")
            return {
                "persisted": False,
                "reason": "MissingEdition model not available",
            }
        except Exception as e:
            logger.error(f"[GapAnalysis] Error persisting gap analysis: {e}")
            return {
                "persisted": False,
                "reason": str(e),
            }


# ============== Convenience Functions ==============

async def analyze_and_generate_jobs(
    db: AsyncSession,
    dossier_id: int,
    bibliography: ThinkerBibliography,
    run_id: int,
    linked_works: Optional[List[Dict[str, Any]]] = None,
    auto_persist: bool = True,
) -> Dict[str, Any]:
    """
    Convenience function to run full gap analysis workflow.

    1. Analyze gaps between what we have and what should exist
    2. Generate scraper jobs for each gap
    3. Optionally persist results to database

    Returns:
        {
            "gaps": GapAnalysisResult,
            "jobs_created": int,
            "job_ids": List[int],
            "persistence_result": Dict
        }
    """
    service = GapAnalysisService(db)

    # Analyze gaps
    thinker_name = bibliography["thinker"]["canonical_name"]
    gaps = await service.analyze_gaps(
        dossier_id=dossier_id,
        bibliography=bibliography,
        run_id=run_id,
        linked_works=linked_works,
    )

    # Generate jobs
    jobs = await service.generate_scraper_jobs(
        gaps=gaps,
        dossier_id=dossier_id,
        thinker_name=thinker_name,
    )

    # Persist if requested
    persistence_result = {}
    if auto_persist:
        persistence_result = await service.persist_gap_analysis(
            dossier_id=dossier_id,
            run_id=run_id,
            gaps=gaps,
            jobs=jobs,
        )

    return {
        "gaps": gaps,
        "jobs_created": len(jobs),
        "job_ids": [j.id for j in jobs],
        "persistence_result": persistence_result,
    }
