"""
Edition Linking Service - Phase 4

Links papers/editions from inventory to abstract Works from bibliographic research.
Creates Work and WorkEdition records to establish the connection between
physical editions in our database and logical works from the thinker's bibliography.

Dependencies:
- Phase 1: Work, WorkEdition models
- Phase 2: DossierInventory (from InventoryService)
- Phase 3: ThinkerBibliography (from BibliographicAgent)
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional, TypedDict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# =============================================================================
# Type Definitions - Expected inputs from Phase 2 (Inventory Service)
# =============================================================================

class EditionInfo(TypedDict):
    """Single edition from inventory"""
    edition_id: int
    title: str
    language: Optional[str]
    year: Optional[int]


class PaperInfo(TypedDict):
    """Paper from inventory with its editions"""
    paper_id: int
    title: str
    authors: list[str]
    editions: list[EditionInfo]


class TitleCluster(TypedDict):
    """Fuzzy-grouped papers by title similarity"""
    canonical_title: str
    papers: list[int]  # paper_ids
    languages: list[str]
    years: list[int]


class DossierInventory(TypedDict):
    """Output from Phase 2 InventoryService.analyze_dossier()"""
    thinker_name: str
    papers: list[PaperInfo]
    title_clusters: list[TitleCluster]


# =============================================================================
# Type Definitions - Expected inputs from Phase 3 (Bibliographic Agent)
# =============================================================================

class TranslationInfo(TypedDict):
    """A known translation of a work"""
    language: str
    title: str
    year: Optional[int]
    translator: Optional[str]
    source: str  # "llm_knowledge", "web_search", "scholar"


class MajorWork(TypedDict):
    """A major work from bibliographic research"""
    canonical_title: str
    original_language: str
    original_title: str
    original_year: Optional[int]
    work_type: str  # book, essay, article, lecture, etc.
    importance: str  # major, minor
    known_translations: list[TranslationInfo]
    scholarly_significance: Optional[str]


class ThinkerInfo(TypedDict):
    """Thinker metadata from bibliographic research"""
    canonical_name: str
    birth_death: Optional[str]
    primary_language: str
    domains: list[str]


class ThinkerBibliography(TypedDict):
    """Output from Phase 3 BibliographicAgent.research_thinker_bibliography()"""
    thinker: ThinkerInfo
    major_works: list[MajorWork]
    verification_sources: list[str]
    confidence: float


# =============================================================================
# Type Definitions - Outputs for Phase 5 and Reconciler
# =============================================================================

class UncertainMatch(TypedDict):
    """A paper that couldn't be confidently linked"""
    paper_id: int
    paper_title: str
    possible_works: list[str]  # List of work canonical titles
    confidence: float
    reason: str


class LinkingResult(TypedDict):
    """Result from link_editions_to_works()"""
    works_created: int
    works_existing: int
    links_created: int
    links_existing: int
    uncertain_matches: list[UncertainMatch]
    papers_unmatched: int
    editions_linked: int


# =============================================================================
# Internal data structures
# =============================================================================

@dataclass
class MatchCandidate:
    """A potential match between an inventory item and a bibliographic work"""
    work: MajorWork
    score: float
    match_type: str  # "exact", "fuzzy", "translation", "partial"
    matched_field: str  # Which field matched: "original_title", "canonical_title", "translation"


@dataclass
class LinkingContext:
    """Context accumulated during linking process for logging"""
    run_id: int
    thinker_name: str
    works_in_bibliography: int
    papers_in_inventory: int
    decisions: list[dict] = field(default_factory=list)


# =============================================================================
# Title Normalization Utilities
# =============================================================================

def normalize_title(title: str) -> str:
    """
    Normalize a title for comparison.
    - Lowercase
    - Remove articles (the, a, an, der, die, das, le, la, el, etc.)
    - Remove punctuation
    - Collapse whitespace
    """
    if not title:
        return ""

    # Lowercase
    normalized = title.lower()

    # Remove common articles in multiple languages
    articles = [
        r'\bthe\b', r'\ba\b', r'\ban\b',  # English
        r'\bder\b', r'\bdie\b', r'\bdas\b', r'\bein\b', r'\beine\b',  # German
        r'\ble\b', r'\bla\b', r'\bles\b', r'\bun\b', r'\bune\b',  # French
        r'\bel\b', r'\bla\b', r'\blos\b', r'\blas\b', r'\bun\b', r'\buna\b',  # Spanish
        r'\bil\b', r'\blo\b', r'\bla\b', r'\bi\b', r'\bgli\b', r'\ble\b',  # Italian
    ]
    for article in articles:
        normalized = re.sub(article, '', normalized)

    # Remove punctuation
    normalized = re.sub(r'[^\w\s]', '', normalized)

    # Collapse whitespace
    normalized = ' '.join(normalized.split())

    return normalized.strip()


def title_similarity(title1: str, title2: str) -> float:
    """
    Calculate similarity between two titles using SequenceMatcher.
    Returns a score between 0.0 and 1.0.
    """
    norm1 = normalize_title(title1)
    norm2 = normalize_title(title2)

    if not norm1 or not norm2:
        return 0.0

    # Exact match after normalization
    if norm1 == norm2:
        return 1.0

    # Use SequenceMatcher for fuzzy matching
    return SequenceMatcher(None, norm1, norm2).ratio()


def extract_key_terms(title: str) -> set[str]:
    """
    Extract key terms from a title for partial matching.
    Filters out common words and returns significant terms.
    """
    stopwords = {
        'the', 'a', 'an', 'of', 'and', 'in', 'on', 'to', 'for', 'with', 'as', 'by', 'from',
        'der', 'die', 'das', 'und', 'in', 'von', 'zu', 'mit', 'als', 'auf',
        'le', 'la', 'les', 'de', 'du', 'et', 'en', 'pour', 'avec', 'sur',
        'el', 'la', 'los', 'las', 'de', 'del', 'y', 'en', 'para', 'con',
    }

    normalized = normalize_title(title)
    terms = normalized.split()

    # Return terms that are not stopwords and have at least 3 characters
    return {term for term in terms if term not in stopwords and len(term) >= 3}


# =============================================================================
# Edition Linking Service
# =============================================================================

class EditionLinkingService:
    """
    Service to link papers/editions from dossier inventory to abstract Works.

    Takes:
    - DossierInventory from Phase 2 (what we have)
    - ThinkerBibliography from Phase 3 (what should exist)

    Creates:
    - Work records for each bibliographic work
    - WorkEdition links connecting papers/editions to Works

    Handles:
    - Exact title matches
    - Fuzzy title matches (variant spellings)
    - Translation detection (linking e.g., "Spuren" to "Traces")
    - Uncertain matches flagged for review
    """

    # Thresholds for matching confidence
    EXACT_MATCH_THRESHOLD = 0.95
    FUZZY_MATCH_THRESHOLD = 0.75
    TRANSLATION_CONFIDENCE_MIN = 0.60

    def __init__(self, session: AsyncSession):
        self.session = session

    async def link_editions_to_works(
        self,
        inventory: DossierInventory,
        bibliography: ThinkerBibliography,
        run_id: int
    ) -> LinkingResult:
        """
        Link papers/editions from inventory to Works from bibliography.

        For each paper/edition in inventory:
        1. Find matching Work from bibliography (or create new Work)
        2. Create WorkEdition link
        3. Flag uncertain matches for review

        Args:
            inventory: Dossier inventory from Phase 2
            bibliography: Thinker bibliography from Phase 3
            run_id: EditionAnalysisRun ID for audit logging

        Returns:
            LinkingResult with counts and uncertain matches
        """
        # Import models here to avoid circular imports
        # These models come from Phase 1
        from app.models import Work, WorkEdition, EditionAnalysisRun

        context = LinkingContext(
            run_id=run_id,
            thinker_name=inventory['thinker_name'],
            works_in_bibliography=len(bibliography['major_works']),
            papers_in_inventory=len(inventory['papers'])
        )

        logger.info(
            f"Starting edition linking for {context.thinker_name}: "
            f"{context.papers_in_inventory} papers, {context.works_in_bibliography} bibliographic works"
        )

        # Track results
        works_created = 0
        works_existing = 0
        links_created = 0
        links_existing = 0
        uncertain_matches: list[UncertainMatch] = []
        papers_unmatched = 0
        editions_linked = 0

        # First, ensure all bibliographic works exist in database
        work_map: dict[str, int] = {}  # canonical_title -> work_id

        for major_work in bibliography['major_works']:
            # Check if work already exists
            existing = await self.session.execute(
                select(Work).where(
                    Work.thinker_name == inventory['thinker_name'],
                    Work.canonical_title == major_work['canonical_title']
                )
            )
            existing_work = existing.scalar_one_or_none()

            if existing_work:
                work_map[major_work['canonical_title']] = existing_work.id
                works_existing += 1
                logger.debug(f"Found existing Work: {major_work['canonical_title']}")
            else:
                # Create new Work
                new_work = Work(
                    thinker_name=inventory['thinker_name'],
                    canonical_title=major_work['canonical_title'],
                    original_language=major_work['original_language'],
                    original_title=major_work['original_title'],
                    original_year=major_work.get('original_year'),
                    work_type=major_work['work_type'],
                    importance=major_work['importance'],
                    notes=major_work.get('scholarly_significance')
                )
                self.session.add(new_work)
                await self.session.flush()  # Get the ID

                work_map[major_work['canonical_title']] = new_work.id
                works_created += 1
                logger.info(f"Created Work: {major_work['canonical_title']} (id={new_work.id})")

        # Now link each paper/edition to a Work
        for paper_info in inventory['papers']:
            paper_matched = False

            # Try to match the paper's title
            matches = self._find_work_matches(paper_info['title'], bibliography['major_works'])

            if matches:
                best_match = matches[0]

                if best_match.score >= self.EXACT_MATCH_THRESHOLD:
                    # High confidence - create link
                    work_id = work_map[best_match.work['canonical_title']]
                    created = await self._create_paper_link(
                        paper_info, work_id, best_match, context
                    )
                    if created:
                        links_created += 1
                    else:
                        links_existing += 1
                    paper_matched = True

                elif best_match.score >= self.FUZZY_MATCH_THRESHOLD:
                    # Medium confidence - check if translation or uncertain
                    if best_match.match_type == 'translation':
                        # Translation match is acceptable
                        work_id = work_map[best_match.work['canonical_title']]
                        created = await self._create_paper_link(
                            paper_info, work_id, best_match, context
                        )
                        if created:
                            links_created += 1
                        else:
                            links_existing += 1
                        paper_matched = True
                    else:
                        # Flag as uncertain
                        uncertain_matches.append({
                            'paper_id': paper_info['paper_id'],
                            'paper_title': paper_info['title'],
                            'possible_works': [m.work['canonical_title'] for m in matches[:3]],
                            'confidence': best_match.score,
                            'reason': f"{best_match.match_type} match (score: {best_match.score:.2f})"
                        })
                        paper_matched = True  # Matched but uncertain
                else:
                    # Low confidence matches - flag as uncertain if any candidates
                    if matches:
                        uncertain_matches.append({
                            'paper_id': paper_info['paper_id'],
                            'paper_title': paper_info['title'],
                            'possible_works': [m.work['canonical_title'] for m in matches[:3]],
                            'confidence': best_match.score,
                            'reason': f"low confidence {best_match.match_type} match (score: {best_match.score:.2f})"
                        })

            if not paper_matched and not matches:
                papers_unmatched += 1
                logger.debug(f"No match found for paper: {paper_info['title']}")

            # Also link editions
            for edition in paper_info.get('editions', []):
                # Try to match edition title (might be a translation)
                edition_matches = self._find_work_matches(
                    edition['title'],
                    bibliography['major_works']
                )

                if edition_matches and edition_matches[0].score >= self.FUZZY_MATCH_THRESHOLD:
                    best = edition_matches[0]
                    work_id = work_map[best.work['canonical_title']]
                    created = await self._create_edition_link(
                        edition, work_id, best, context
                    )
                    if created:
                        editions_linked += 1

        # Update run status
        run = await self.session.get(EditionAnalysisRun, run_id)
        if run:
            run.works_identified = works_created + works_existing
            run.links_created = links_created

        await self.session.commit()

        result: LinkingResult = {
            'works_created': works_created,
            'works_existing': works_existing,
            'links_created': links_created,
            'links_existing': links_existing,
            'uncertain_matches': uncertain_matches,
            'papers_unmatched': papers_unmatched,
            'editions_linked': editions_linked
        }

        logger.info(
            f"Edition linking complete: {works_created} works created, "
            f"{links_created} links created, {len(uncertain_matches)} uncertain, "
            f"{papers_unmatched} unmatched"
        )

        return result

    def _find_work_matches(
        self,
        title: str,
        major_works: list[MajorWork]
    ) -> list[MatchCandidate]:
        """
        Find matching works for a given title.

        Checks against:
        - canonical_title (English standard)
        - original_title (original language)
        - known_translations titles

        Returns matches sorted by score (highest first).
        """
        candidates: list[MatchCandidate] = []

        for work in major_works:
            best_score = 0.0
            best_type = "none"
            best_field = ""

            # Check against canonical title
            canonical_score = title_similarity(title, work['canonical_title'])
            if canonical_score > best_score:
                best_score = canonical_score
                best_type = "exact" if canonical_score >= self.EXACT_MATCH_THRESHOLD else "fuzzy"
                best_field = "canonical_title"

            # Check against original title
            original_score = title_similarity(title, work['original_title'])
            if original_score > best_score:
                best_score = original_score
                best_type = "exact" if original_score >= self.EXACT_MATCH_THRESHOLD else "fuzzy"
                best_field = "original_title"

            # Check against known translations
            for translation in work.get('known_translations', []):
                trans_score = title_similarity(title, translation['title'])
                if trans_score > best_score:
                    best_score = trans_score
                    best_type = "translation"
                    best_field = f"translation_{translation['language']}"

            # Also check key term overlap for partial matches
            if best_score < self.FUZZY_MATCH_THRESHOLD:
                title_terms = extract_key_terms(title)
                canonical_terms = extract_key_terms(work['canonical_title'])
                original_terms = extract_key_terms(work['original_title'])

                if title_terms and (canonical_terms or original_terms):
                    all_work_terms = canonical_terms | original_terms
                    overlap = len(title_terms & all_work_terms)
                    max_terms = max(len(title_terms), len(all_work_terms))
                    if max_terms > 0:
                        partial_score = overlap / max_terms
                        if partial_score > best_score and overlap >= 2:
                            best_score = partial_score
                            best_type = "partial"
                            best_field = "key_terms"

            if best_score > 0.1:  # Only include non-trivial matches
                candidates.append(MatchCandidate(
                    work=work,
                    score=best_score,
                    match_type=best_type,
                    matched_field=best_field
                ))

        # Sort by score descending
        candidates.sort(key=lambda c: c.score, reverse=True)
        return candidates

    async def _create_paper_link(
        self,
        paper_info: PaperInfo,
        work_id: int,
        match: MatchCandidate,
        context: LinkingContext
    ) -> bool:
        """
        Create a WorkEdition link for a paper.

        Returns True if link was created, False if already existed.
        """
        from app.models import WorkEdition, Paper

        # Check for existing link
        existing = await self.session.execute(
            select(WorkEdition).where(
                WorkEdition.work_id == work_id,
                WorkEdition.paper_id == paper_info['paper_id']
            )
        )
        if existing.scalar_one_or_none():
            return False

        # Get paper to determine language
        paper = await self.session.get(Paper, paper_info['paper_id'])
        language = paper.language if paper else None

        # Determine edition type based on match
        edition_type = self._infer_edition_type(match, language)

        link = WorkEdition(
            work_id=work_id,
            paper_id=paper_info['paper_id'],
            edition_id=None,  # This is a paper-level link
            language=language or "unknown",
            edition_type=edition_type,
            year=paper.year if paper else None,
            verified=False,
            auto_linked=True,
            confidence=match.score
        )
        self.session.add(link)

        context.decisions.append({
            'paper_id': paper_info['paper_id'],
            'work_id': work_id,
            'match_type': match.match_type,
            'match_field': match.matched_field,
            'confidence': match.score
        })

        logger.debug(
            f"Linked paper {paper_info['paper_id']} to work {work_id} "
            f"({match.match_type}, score={match.score:.2f})"
        )

        return True

    async def _create_edition_link(
        self,
        edition: EditionInfo,
        work_id: int,
        match: MatchCandidate,
        context: LinkingContext
    ) -> bool:
        """
        Create a WorkEdition link for an edition.

        Returns True if link was created, False if already existed.
        """
        from app.models import WorkEdition, Edition

        # Check for existing link
        existing = await self.session.execute(
            select(WorkEdition).where(
                WorkEdition.work_id == work_id,
                WorkEdition.edition_id == edition['edition_id']
            )
        )
        if existing.scalar_one_or_none():
            return False

        # Determine edition type based on match
        edition_type = self._infer_edition_type(match, edition.get('language'))

        link = WorkEdition(
            work_id=work_id,
            paper_id=None,  # This is an edition-level link
            edition_id=edition['edition_id'],
            language=edition.get('language') or "unknown",
            edition_type=edition_type,
            year=edition.get('year'),
            verified=False,
            auto_linked=True,
            confidence=match.score
        )
        self.session.add(link)

        logger.debug(
            f"Linked edition {edition['edition_id']} to work {work_id} "
            f"({match.match_type}, score={match.score:.2f})"
        )

        return True

    def _infer_edition_type(
        self,
        match: MatchCandidate,
        language: Optional[str]
    ) -> str:
        """
        Infer the edition type from the match and language.

        Returns: 'original', 'translation', 'abridged', 'anthology_excerpt'
        """
        # If matched against original_title in original_language
        if match.matched_field == 'original_title':
            if language and language.lower() == match.work['original_language'].lower():
                return 'original'

        # If matched against translation
        if match.match_type == 'translation':
            return 'translation'

        # If language differs from original
        if language and match.work.get('original_language'):
            if language.lower() != match.work['original_language'].lower():
                return 'translation'

        # Default to translation unless we're sure it's original
        return 'translation'

    async def link_orphan_papers(
        self,
        inventory: DossierInventory,
        run_id: int
    ) -> int:
        """
        Create Works for papers that don't match any bibliographic work.

        These are papers in the dossier that the bibliographic agent didn't know about,
        or that couldn't be matched. We create Works for them to track them.

        Returns: Number of Works created for orphan papers
        """
        from app.models import Work, WorkEdition, Paper

        # Find papers without WorkEdition links
        orphan_count = 0

        for paper_info in inventory['papers']:
            # Check if paper already has a link
            existing = await self.session.execute(
                select(WorkEdition).where(
                    WorkEdition.paper_id == paper_info['paper_id']
                )
            )
            if existing.scalar_one_or_none():
                continue  # Already linked

            # Get paper details
            paper = await self.session.get(Paper, paper_info['paper_id'])
            if not paper:
                continue

            # Create a new Work for this orphan
            work = Work(
                thinker_name=inventory['thinker_name'],
                canonical_title=paper.title,  # Use paper title as canonical
                original_language=paper.language or 'unknown',
                original_title=paper.title,
                original_year=paper.year,
                work_type='unknown',
                importance='unknown',  # Needs classification
                notes=f"Auto-created from orphan paper {paper.id}"
            )
            self.session.add(work)
            await self.session.flush()

            # Link the paper
            link = WorkEdition(
                work_id=work.id,
                paper_id=paper.id,
                edition_id=None,
                language=paper.language or 'unknown',
                edition_type='original',  # Assume original
                year=paper.year,
                verified=False,
                auto_linked=True,
                confidence=0.5  # Low confidence since we couldn't match it
            )
            self.session.add(link)

            orphan_count += 1
            logger.info(f"Created orphan Work for paper {paper.id}: {paper.title}")

        if orphan_count > 0:
            await self.session.commit()

        return orphan_count
