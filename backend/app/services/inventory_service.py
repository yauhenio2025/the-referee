"""
Dossier Inventory Service

Phase 2 of Edition Analysis: Pure data extraction service that analyzes
all papers/editions in a dossier without LLM calls.

Capabilities:
- Extract all papers and their editions from a dossier
- Detect language from titles
- Group papers by title similarity (fuzzy clustering)
- Identify obvious original/translation pairs
"""
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Any, Set

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Dossier, Paper, Edition

logger = logging.getLogger(__name__)


# ============== Data Structures ==============

@dataclass
class EditionInfo:
    """Information about a single edition"""
    edition_id: int
    title: str
    language: Optional[str]
    year: Optional[int]
    venue: Optional[str]
    citation_count: int
    scholar_id: Optional[str]
    selected: bool
    confidence: str


@dataclass
class PaperInfo:
    """Information about a paper and its editions"""
    paper_id: int
    title: str
    authors: List[str]
    year: Optional[int]
    language: Optional[str]
    citation_count: int
    scholar_id: Optional[str]
    editions: List[EditionInfo] = field(default_factory=list)


@dataclass
class TitleCluster:
    """A group of papers/editions that appear to be the same work"""
    canonical_title: str
    papers: List[int]  # paper_ids
    editions: List[int]  # edition_ids
    languages: List[str]
    years: List[int]
    similarity_scores: Dict[str, float] = field(default_factory=dict)  # title -> score


@dataclass
class DossierInventory:
    """Complete inventory of a dossier's papers and editions"""
    dossier_id: int
    dossier_name: str
    thinker_name: str  # Inferred from dossier name or papers
    paper_count: int
    edition_count: int
    papers: List[PaperInfo] = field(default_factory=list)
    title_clusters: List[TitleCluster] = field(default_factory=list)
    languages_detected: List[str] = field(default_factory=list)
    year_range: Optional[tuple] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "dossier_id": self.dossier_id,
            "dossier_name": self.dossier_name,
            "thinker_name": self.thinker_name,
            "paper_count": self.paper_count,
            "edition_count": self.edition_count,
            "papers": [asdict(p) for p in self.papers],
            "title_clusters": [asdict(c) for c in self.title_clusters],
            "languages_detected": self.languages_detected,
            "year_range": list(self.year_range) if self.year_range else None,
        }


# ============== Language Detection ==============

# Common words and patterns for language detection
# These are high-precision indicators (if present, strong signal)
LANGUAGE_MARKERS = {
    "german": {
        "words": {"der", "die", "das", "und", "ein", "eine", "zur", "zum", "vom", "des", "dem", "den",
                  "über", "für", "mit", "ist", "nicht", "sind", "aber", "oder", "als", "nach",
                  "geist", "wesen", "geschichte", "philosophie", "kritik", "theorie", "prinzip"},
        "patterns": [r"\bund\b", r"\bder\b", r"\bdie\b", r"\bzur\b", r"ß", r"ü", r"ö", r"ä"],
        "suffixes": ["ung", "heit", "keit", "ismus", "schaft"],
    },
    "french": {
        "words": {"le", "la", "les", "de", "du", "des", "et", "en", "un", "une", "est", "sont",
                  "dans", "sur", "pour", "avec", "par", "que", "qui", "ce", "cette", "aux",
                  "philosophie", "histoire", "théorie", "critique", "esprit", "principe",
                  "homme", "raison", "révolution", "société", "libération"},
        "patterns": [r"\bde la\b", r"\bdu\b", r"\bdes\b", r"\bet\b", r"\bl'", r"é", r"è", r"ê", r"ç", r"œ"],
        "suffixes": ["tion", "isme", "ité", "ment", "ionnel"],
    },
    "spanish": {
        "words": {"el", "la", "los", "las", "de", "del", "en", "un", "una", "es", "son",
                  "con", "por", "para", "que", "como", "pero", "más", "sobre", "entre",
                  "filosofía", "historia", "teoría", "crítica", "principio", "espíritu",
                  "revolución", "teólogo", "teología"},
        "patterns": [r"\bdel\b", r"\bde la\b", r"\by\b", r"\bel\b", r"ñ", r"á", r"é", r"í", r"ó", r"ú", r"¿", r"¡"],
        "suffixes": ["ción", "dad", "ismo", "miento", "ólogo"],
    },
    "italian": {
        "words": {"il", "la", "lo", "i", "gli", "le", "di", "del", "della", "e", "ed", "un", "una",
                  "che", "per", "con", "non", "sono", "nel", "nella", "sul", "sulla",
                  "filosofia", "storia", "teoria", "critica", "principio", "spirito"},
        "patterns": [r"\bdella\b", r"\bdel\b", r"\bil\b", r"\be\b(?!\w)", r"à", r"ì", r"ò", r"ù"],
        "suffixes": ["zione", "ità", "ismo", "mento"],
    },
    "portuguese": {
        "words": {"o", "a", "os", "as", "de", "do", "da", "dos", "das", "em", "no", "na",
                  "um", "uma", "que", "para", "com", "por", "sobre", "entre", "mais",
                  "filosofia", "história", "teoria", "crítica", "princípio", "espírito"},
        "patterns": [r"\bda\b", r"\bdo\b", r"\bdos\b", r"\bdas\b", r"\bno\b", r"\bna\b", r"ã", r"õ", r"ç"],
        "suffixes": ["ção", "dade", "ismo", "mento"],
    },
    "english": {
        "words": {"the", "of", "and", "in", "to", "a", "for", "on", "with", "is", "are", "by",
                  "from", "an", "as", "at", "this", "that", "which", "or", "but", "not",
                  "philosophy", "history", "theory", "critique", "principle", "spirit",
                  "man", "society", "reason", "liberation", "revolution", "essays"},
        "patterns": [r"\bthe\b", r"\bof\b", r"\band\b", r"\bin\b", r"-dimensional\b"],
        "suffixes": ["tion", "ness", "ism", "ment", "ing", "ed"],
    },
    "dutch": {
        "words": {"de", "het", "een", "van", "en", "in", "op", "te", "met", "voor", "naar",
                  "zijn", "hebben", "over", "tot", "aan", "bij", "uit",
                  "filosofie", "geschiedenis", "theorie", "kritiek", "beginsel", "geest"},
        "patterns": [r"\bhet\b", r"\been\b", r"\bvan\b", r"\bde\b", r"ij", r"oe"],
        "suffixes": ["heid", "ing", "isme", "tie"],
    },
    "russian": {
        "words": set(),  # Cyrillic detection instead
        "patterns": [r"[а-яА-ЯёЁ]+"],
        "suffixes": [],
    },
    "chinese": {
        "words": set(),  # Character range detection
        "patterns": [r"[\u4e00-\u9fff]+"],
        "suffixes": [],
    },
    "japanese": {
        "words": set(),  # Character range detection (hiragana, katakana, kanji)
        "patterns": [r"[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff]+"],
        "suffixes": [],
    },
    "korean": {
        "words": set(),  # Hangul detection
        "patterns": [r"[\uac00-\ud7af]+"],
        "suffixes": [],
    },
}


def detect_language(title: str) -> Optional[str]:
    """
    Detect the language of a title based on word patterns and character sets.

    Returns the most likely language or None if uncertain.
    """
    if not title:
        return None

    title_lower = title.lower()
    words = set(re.findall(r'\b\w+\b', title_lower))

    scores: Dict[str, float] = {}

    # First check for non-Latin scripts (high confidence)
    for lang in ["russian", "chinese", "japanese", "korean"]:
        markers = LANGUAGE_MARKERS[lang]
        for pattern in markers["patterns"]:
            if re.search(pattern, title):
                return lang

    # Score Latin-script languages
    for lang, markers in LANGUAGE_MARKERS.items():
        if lang in ["russian", "chinese", "japanese", "korean"]:
            continue

        score = 0.0

        # Word matches (strongest signal)
        word_matches = len(words & markers["words"])
        if word_matches > 0:
            score += word_matches * 2.0

        # Pattern matches
        for pattern in markers["patterns"]:
            if re.search(pattern, title_lower):
                score += 1.5

        # Suffix matches (weaker signal)
        for suffix in markers.get("suffixes", []):
            for word in words:
                if word.endswith(suffix) and len(word) > len(suffix) + 2:
                    score += 0.5

        if score > 0:
            scores[lang] = score

    if not scores:
        return None

    # Return highest scoring language if it's clearly ahead
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_lang, best_score = sorted_scores[0]

    # Require minimum score and margin over second place
    if best_score < 2.0:
        return None

    if len(sorted_scores) > 1:
        second_score = sorted_scores[1][1]
        if best_score < second_score * 1.5:  # Require 50% margin
            return None

    return best_lang


# ============== Fuzzy Title Matching ==============

def normalize_title(title: str) -> str:
    """
    Normalize a title for comparison.

    - Lowercase
    - Remove common subtitle separators
    - Remove punctuation
    - Collapse whitespace
    """
    if not title:
        return ""

    normalized = title.lower()

    # Remove common subtitle patterns
    normalized = re.sub(r'[:\-–—]\s*.*$', '', normalized)  # Remove subtitles
    normalized = re.sub(r'\([^)]*\)', '', normalized)  # Remove parenthetical
    normalized = re.sub(r'\[[^\]]*\]', '', normalized)  # Remove brackets

    # Remove punctuation except hyphens in compound words
    normalized = re.sub(r'[^\w\s-]', ' ', normalized)

    # Collapse whitespace
    normalized = ' '.join(normalized.split())

    return normalized.strip()


def title_similarity(title1: str, title2: str) -> float:
    """
    Calculate similarity between two titles.

    Returns a score between 0.0 and 1.0.
    """
    norm1 = normalize_title(title1)
    norm2 = normalize_title(title2)

    if not norm1 or not norm2:
        return 0.0

    # Use SequenceMatcher for fuzzy matching
    return SequenceMatcher(None, norm1, norm2).ratio()


def cluster_titles(papers: List[PaperInfo], threshold: float = 0.6) -> List[TitleCluster]:
    """
    Cluster papers by title similarity.

    Papers/editions with similar titles (likely translations or re-editions)
    are grouped together.

    Args:
        papers: List of PaperInfo objects
        threshold: Minimum similarity score to cluster (0.0-1.0)

    Returns:
        List of TitleCluster objects
    """
    # Collect all titles with their sources
    title_sources: List[tuple] = []  # (title, paper_id, edition_id or None)

    for paper in papers:
        title_sources.append((paper.title, paper.paper_id, None))
        for edition in paper.editions:
            title_sources.append((edition.title, paper.paper_id, edition.edition_id))

    if not title_sources:
        return []

    # Build clusters using Union-Find approach
    n = len(title_sources)
    parent = list(range(n))

    def find(x: int) -> int:
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x: int, y: int):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # Compare all pairs (O(n^2) but n is typically small for a dossier)
    for i in range(n):
        for j in range(i + 1, n):
            title_i = title_sources[i][0]
            title_j = title_sources[j][0]

            sim = title_similarity(title_i, title_j)
            if sim >= threshold:
                union(i, j)

    # Group by cluster
    clusters_map: Dict[int, List[int]] = {}
    for i in range(n):
        root = find(i)
        if root not in clusters_map:
            clusters_map[root] = []
        clusters_map[root].append(i)

    # Build TitleCluster objects
    clusters: List[TitleCluster] = []

    for indices in clusters_map.values():
        if len(indices) < 1:
            continue

        paper_ids: Set[int] = set()
        edition_ids: List[int] = []
        languages: Set[str] = set()
        years: Set[int] = set()
        titles: List[str] = []

        for idx in indices:
            title, paper_id, edition_id = title_sources[idx]
            titles.append(title)
            paper_ids.add(paper_id)

            if edition_id:
                edition_ids.append(edition_id)

            # Get language and year from the paper
            paper = next((p for p in papers if p.paper_id == paper_id), None)
            if paper:
                if paper.language:
                    languages.add(paper.language)
                if paper.year:
                    years.add(paper.year)

                # Also check editions for language/year
                for ed in paper.editions:
                    if ed.edition_id == edition_id:
                        if ed.language:
                            languages.add(ed.language)
                        if ed.year:
                            years.add(ed.year)

        # Choose the shortest title as canonical (usually the original)
        canonical = min(titles, key=len)

        # Calculate similarity scores for all titles in cluster
        sim_scores = {}
        for t in titles:
            if t != canonical:
                sim_scores[t] = title_similarity(canonical, t)

        cluster = TitleCluster(
            canonical_title=canonical,
            papers=list(paper_ids),
            editions=edition_ids,
            languages=list(languages),
            years=sorted(years) if years else [],
            similarity_scores=sim_scores,
        )
        clusters.append(cluster)

    # Sort clusters by number of associated papers (descending)
    clusters.sort(key=lambda c: len(c.papers), reverse=True)

    return clusters


# ============== Main Service ==============

class InventoryService:
    """
    Service to analyze all papers/editions in a dossier.

    This is Phase 2 of the Edition Analysis pipeline:
    - No LLM calls - pure data extraction
    - Extracts titles, languages, years, authors
    - Groups by title similarity (fuzzy matching)
    - Detects languages from titles
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def analyze_dossier(self, dossier_id: int) -> DossierInventory:
        """
        Analyze all papers and editions in a dossier.

        Args:
            dossier_id: The dossier to analyze

        Returns:
            DossierInventory with all extracted information
        """
        logger.info(f"[InventoryService] Analyzing dossier {dossier_id}")

        # Get dossier
        result = await self.db.execute(
            select(Dossier).where(Dossier.id == dossier_id)
        )
        dossier = result.scalar_one_or_none()

        if not dossier:
            raise ValueError(f"Dossier {dossier_id} not found")

        # Get all papers in the dossier
        papers_result = await self.db.execute(
            select(Paper)
            .where(Paper.dossier_id == dossier_id)
            .where(Paper.deleted_at.is_(None))  # Exclude soft-deleted
            .order_by(Paper.year.desc().nullslast(), Paper.title)
        )
        papers = papers_result.scalars().all()

        logger.info(f"[InventoryService] Found {len(papers)} papers in dossier '{dossier.name}'")

        # Get all editions for these papers in one query (avoid N+1)
        paper_ids = [p.id for p in papers]
        editions_by_paper: Dict[int, List[Edition]] = {}

        if paper_ids:
            editions_result = await self.db.execute(
                select(Edition)
                .where(Edition.paper_id.in_(paper_ids))
                .order_by(Edition.paper_id, Edition.citation_count.desc())
            )
            for edition in editions_result.scalars().all():
                if edition.paper_id not in editions_by_paper:
                    editions_by_paper[edition.paper_id] = []
                editions_by_paper[edition.paper_id].append(edition)

        # Build PaperInfo objects
        paper_infos: List[PaperInfo] = []
        all_languages: Set[str] = set()
        all_years: List[int] = []
        total_editions = 0

        for paper in papers:
            # Parse authors
            authors = []
            if paper.authors:
                try:
                    authors = json.loads(paper.authors)
                except json.JSONDecodeError:
                    # Fallback: treat as comma-separated
                    authors = [a.strip() for a in paper.authors.split(",") if a.strip()]

            # Detect language from title if not already set
            paper_language = paper.language
            if not paper_language:
                paper_language = detect_language(paper.title)

            if paper_language:
                all_languages.add(paper_language)

            if paper.year:
                all_years.append(paper.year)

            # Build edition infos
            edition_infos: List[EditionInfo] = []
            paper_editions = editions_by_paper.get(paper.id, [])
            total_editions += len(paper_editions)

            for edition in paper_editions:
                # Detect language for edition
                ed_language = edition.language
                if not ed_language:
                    ed_language = detect_language(edition.title)

                if ed_language:
                    all_languages.add(ed_language)

                if edition.year:
                    all_years.append(edition.year)

                edition_infos.append(EditionInfo(
                    edition_id=edition.id,
                    title=edition.title,
                    language=ed_language,
                    year=edition.year,
                    venue=edition.venue,
                    citation_count=edition.citation_count,
                    scholar_id=edition.scholar_id,
                    selected=edition.selected,
                    confidence=edition.confidence,
                ))

            paper_infos.append(PaperInfo(
                paper_id=paper.id,
                title=paper.title,
                authors=authors,
                year=paper.year,
                language=paper_language,
                citation_count=paper.citation_count,
                scholar_id=paper.scholar_id,
                editions=edition_infos,
            ))

        # Cluster titles
        title_clusters = cluster_titles(paper_infos, threshold=0.6)
        logger.info(f"[InventoryService] Created {len(title_clusters)} title clusters")

        # Infer thinker name from dossier name
        # Common pattern: dossier name is the thinker's name
        thinker_name = dossier.name

        # Calculate year range
        year_range = None
        if all_years:
            year_range = (min(all_years), max(all_years))

        inventory = DossierInventory(
            dossier_id=dossier_id,
            dossier_name=dossier.name,
            thinker_name=thinker_name,
            paper_count=len(papers),
            edition_count=total_editions,
            papers=paper_infos,
            title_clusters=title_clusters,
            languages_detected=sorted(all_languages),
            year_range=year_range,
        )

        logger.info(f"[InventoryService] Inventory complete: {len(papers)} papers, "
                    f"{total_editions} editions, {len(title_clusters)} clusters, "
                    f"languages: {all_languages}")

        return inventory

    async def get_papers_by_title(self, dossier_id: int, title_pattern: str) -> List[PaperInfo]:
        """
        Get papers matching a title pattern (for targeted analysis).

        Args:
            dossier_id: The dossier to search in
            title_pattern: SQL LIKE pattern (e.g., '%Utopia%')

        Returns:
            List of matching PaperInfo objects
        """
        result = await self.db.execute(
            select(Paper)
            .where(Paper.dossier_id == dossier_id)
            .where(Paper.deleted_at.is_(None))
            .where(Paper.title.ilike(title_pattern))
        )
        papers = result.scalars().all()

        # Get editions
        paper_ids = [p.id for p in papers]
        editions_by_paper: Dict[int, List[Edition]] = {}

        if paper_ids:
            editions_result = await self.db.execute(
                select(Edition)
                .where(Edition.paper_id.in_(paper_ids))
            )
            for edition in editions_result.scalars().all():
                if edition.paper_id not in editions_by_paper:
                    editions_by_paper[edition.paper_id] = []
                editions_by_paper[edition.paper_id].append(edition)

        paper_infos: List[PaperInfo] = []
        for paper in papers:
            authors = []
            if paper.authors:
                try:
                    authors = json.loads(paper.authors)
                except json.JSONDecodeError:
                    authors = [a.strip() for a in paper.authors.split(",") if a.strip()]

            edition_infos = [
                EditionInfo(
                    edition_id=ed.id,
                    title=ed.title,
                    language=ed.language or detect_language(ed.title),
                    year=ed.year,
                    venue=ed.venue,
                    citation_count=ed.citation_count,
                    scholar_id=ed.scholar_id,
                    selected=ed.selected,
                    confidence=ed.confidence,
                )
                for ed in editions_by_paper.get(paper.id, [])
            ]

            paper_infos.append(PaperInfo(
                paper_id=paper.id,
                title=paper.title,
                authors=authors,
                year=paper.year,
                language=paper.language or detect_language(paper.title),
                citation_count=paper.citation_count,
                scholar_id=paper.scholar_id,
                editions=edition_infos,
            ))

        return paper_infos
