# Phase 4 Final Report

> Implementor Session: 2026-01-10
> Phase: 4 - Edition Linking Service
> Status: COMPLETE

## What Was Built

A service to link papers/editions from dossier inventory to abstract Works from bibliographic research. The service:

1. Ensures all bibliographic works exist as Work records in the database
2. Links papers to Works using multi-strategy title matching
3. Links editions to Works (handling translations)
4. Tracks uncertain matches for human review
5. Handles orphan papers (creates Works for unmatched items)

## Files Changed

- `backend/app/services/edition_linking_service.py` - NEW: Complete EditionLinkingService implementation (~450 lines)

## Deviations from Memo

1. **Added `link_orphan_papers()` method**: The memo didn't specify how to handle papers that don't match any bibliographic work. Added a method to create Works for these orphans with low confidence (0.5).

2. **TypedDict for interfaces**: Used TypedDict instead of Pydantic for data structures. This reduces overhead and makes it easier to pass plain dicts between services. Reconciler can convert to Pydantic if needed.

3. **Configurable thresholds**: Match thresholds are class constants that can be tuned:
   - EXACT_MATCH_THRESHOLD = 0.95
   - FUZZY_MATCH_THRESHOLD = 0.75
   - TRANSLATION_CONFIDENCE_MIN = 0.60

## Interface Provided

```python
from app.services.edition_linking_service import (
    EditionLinkingService,
    # Input types (from Phase 2)
    DossierInventory,
    PaperInfo,
    EditionInfo,
    TitleCluster,
    # Input types (from Phase 3)
    ThinkerBibliography,
    ThinkerInfo,
    MajorWork,
    TranslationInfo,
    # Output types
    LinkingResult,
    UncertainMatch,
)

class EditionLinkingService:
    def __init__(self, session: AsyncSession): ...

    async def link_editions_to_works(
        self,
        inventory: DossierInventory,
        bibliography: ThinkerBibliography,
        run_id: int
    ) -> LinkingResult: ...

    async def link_orphan_papers(
        self,
        inventory: DossierInventory,
        run_id: int
    ) -> int: ...
```

## Interface Expected

From **Phase 1** (models):
```python
from app.models import Work, WorkEdition, EditionAnalysisRun, Paper, Edition
```

Expected Work model attributes:
- `thinker_name: str`
- `canonical_title: str`
- `original_language: str`
- `original_title: str`
- `original_year: Optional[int]`
- `work_type: str`
- `importance: str`
- `notes: Optional[str]`

Expected WorkEdition model attributes:
- `work_id: int`
- `paper_id: Optional[int]`
- `edition_id: Optional[int]`
- `language: str`
- `edition_type: str`
- `year: Optional[int]`
- `verified: bool`
- `auto_linked: bool`
- `confidence: float`

From **Phase 2** (inventory): `DossierInventory` dict with `thinker_name`, `papers`, `title_clusters`

From **Phase 3** (bibliography): `ThinkerBibliography` dict with `thinker`, `major_works`, `verification_sources`, `confidence`

## Known Issues

1. **Model imports will fail**: The service imports `Work`, `WorkEdition`, `EditionAnalysisRun` which don't exist yet (Phase 1). Reconciler needs to ensure Phase 1 creates these models.

2. **Translation matching by string similarity only**: "Geist der Utopie" → "Spirit of Utopia" has low string similarity because the words are completely different. Could benefit from LLM-assisted matching for difficult cases.

3. **Language field may be empty**: The service relies on `paper.language` and `edition.language` being populated. If Phase 2's inventory doesn't detect languages, linking quality degrades.

## Testing Done

- [x] Code compiles without syntax errors
- [x] Type hints are consistent
- [x] Logic for each matching strategy verified by inspection
- [ ] Unit tests - NOT YET (waiting for Phase 1 models)
- [ ] Integration test with real data - NOT YET (waiting for all phases)

## Questions for Reconciler

1. **Model location**: Are Phase 1 models in `app.models` or a separate file? Update imports if needed.

2. **Phase 2 language detection**: Does InventoryService detect paper/edition languages? If not, linking will be less accurate.

3. **Uncertain match threshold**: Currently papers with 0.75-0.95 similarity that aren't translations are flagged as uncertain. Should this threshold be adjustable per-thinker?

4. **LLM enhancement**: Should uncertain matches be sent to an LLM for verification? Would add latency but improve accuracy for edge cases.
