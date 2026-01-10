# Phase 2 Final Report

> Implementor Session: 2026-01-10
> Phase: 2 - Dossier Inventory Service
> Status: COMPLETE

## What Was Built

A pure data extraction service that analyzes all papers/editions in a dossier without any LLM calls. This is Phase 2 of the Edition Analysis pipeline.

### Key Components

1. **Data Structures** (dataclasses for type safety):
   - `EditionInfo` - Information about a single edition
   - `PaperInfo` - Information about a paper and its editions
   - `TitleCluster` - Group of papers/editions that appear to be the same work
   - `DossierInventory` - Complete inventory with all extracted information

2. **Language Detection** (`detect_language()`):
   - Rule-based detection using word patterns and diacritical marks
   - Supports: German, French, Spanish, Italian, Portuguese, English, Dutch, Russian, Chinese, Japanese, Korean
   - Returns `None` for uncertain cases (conservative approach)
   - Test results: 12/12 representative titles correctly detected

3. **Fuzzy Title Clustering** (`cluster_titles()`):
   - Uses normalized title comparison
   - SequenceMatcher for fuzzy matching
   - Union-Find algorithm for clustering
   - Configurable similarity threshold (default 0.6)
   - Groups translations/re-editions together

4. **InventoryService class**:
   - `analyze_dossier(dossier_id)` - Main entry point
   - `get_papers_by_title(dossier_id, pattern)` - Targeted search
   - Efficient N+1 avoidance (bulk edition loading)
   - Excludes soft-deleted papers

## Files Changed

- `backend/app/services/inventory_service.py` - NEW (complete implementation)

## Deviations from Memo

1. **Additional helper method**: Added `get_papers_by_title()` for targeted searches, which will be useful for Phase 4's edition linking.

2. **Title normalization**: Added `normalize_title()` helper that strips subtitles, parentheticals, and brackets before comparison.

3. **Conservative language detection**: Returns `None` rather than guessing when confidence is low. This is intentional - the LLM in Phase 3 will handle ambiguous cases.

## Interface Provided

```python
from app.services.inventory_service import (
    InventoryService,
    DossierInventory,
    PaperInfo,
    EditionInfo,
    TitleCluster,
    detect_language,
    title_similarity,
    cluster_titles,
    normalize_title,
)

# Main usage
service = InventoryService(db_session)
inventory: DossierInventory = await service.analyze_dossier(dossier_id)

# Inventory structure
{
    "dossier_id": int,
    "dossier_name": str,
    "thinker_name": str,           # Inferred from dossier name
    "paper_count": int,
    "edition_count": int,
    "papers": [
        {
            "paper_id": int,
            "title": str,
            "authors": list[str],
            "editions": [
                {"edition_id": int, "title": str, "language": str, ...}
            ]
        }
    ],
    "title_clusters": [            # Fuzzy-grouped by title similarity
        {
            "canonical_title": str,
            "papers": [paper_ids],
            "editions": [edition_ids],
            "languages": [str],
            "years": [int]
        }
    ],
    "languages_detected": [str],
    "year_range": (min, max)
}

# Also provides standalone utilities
detect_language("Geist der Utopie") -> "german"
title_similarity("Spuren", "Traces") -> 0.17
cluster_titles(papers, threshold=0.6) -> List[TitleCluster]
```

## Interface Expected

```python
# From Phase 1 (models) - confirmed existing in codebase
from app.models import Dossier, Paper, Edition

# Standard SQLAlchemy async patterns
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
```

## Known Issues

1. **Title similarity across languages**: `title_similarity("Spuren", "Traces")` returns 0.17 (low) because they're completely different strings. The LLM in Phase 3/4 will need to establish these cross-language links using bibliographic knowledge.

2. **Language detection limitations**: Very short titles (3-4 words) without distinctive markers may return `None`. This is by design - uncertain cases should be handled by the LLM.

## Testing Done

- [x] Import test - all classes and functions importable
- [x] Language detection - 12/12 representative titles correctly detected
  - German: "Geist der Utopie", "Thomas Münzer als Theologe der Revolution", "Der eindimensionale Mensch"
  - English: "The Spirit of Utopia", "One-Dimensional Man", "Reason and Revolution", "Eros and Civilization"
  - French: "L'esprit de l'utopie", "L'homme unidimensionnel"
  - Spanish: "El principio esperanza", "Thomas Müntzer, teólogo de la revolución"
  - Italian: "Il principio speranza"
- [x] Title normalization - correctly strips subtitles and parentheticals
- [x] Title similarity - fuzzy matching working with SequenceMatcher
- [ ] Full integration test with actual dossier (needs Phase 7)

## Questions for Reconciler

1. **Thinker name inference**: Currently using dossier name as thinker name. Is this sufficient, or should we also analyze paper author fields?

2. **Cluster threshold**: Using 0.6 as default. May need tuning based on real dossier data. Should this be configurable via the API?

3. **Database connection**: Using async session passed to constructor. Confirm this pattern matches the rest of the codebase (it does based on my reading of thinker_service.py).

## Performance Notes

- Bulk loads all editions in a single query (avoids N+1)
- O(n²) title comparison for clustering, but n is typically small for a single dossier (usually <100 papers)
- No database writes in this phase - read-only analysis
