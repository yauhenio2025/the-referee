# Phase 4 Handoff: Edition Linking Service

> Implementor Session: 2026-01-10
> Phase: 4 - Edition Linking Service
> Status: COMPLETE

## Notes for Reconciler

### Dependencies Needed
- Phase 1: Schema & Models (Work, WorkEdition, EditionAnalysisRun models)
- Phase 2: Inventory Service (DossierInventory structure)
- Phase 3: Bibliographic Agent (ThinkerBibliography structure)

### What Phase 4 Delivers

Created `backend/app/services/edition_linking_service.py` with:

1. **TypedDict definitions** for interface contracts:
   - `DossierInventory` - expected input from Phase 2
   - `ThinkerBibliography` - expected input from Phase 3
   - `LinkingResult` - output for Phase 5

2. **EditionLinkingService class** with methods:
   - `link_editions_to_works()` - main linking method
   - `link_orphan_papers()` - creates Works for unmatched papers
   - `_find_work_matches()` - multi-strategy title matching
   - `_create_paper_link()` / `_create_edition_link()` - database linking

3. **Title matching strategies**:
   - Exact match (normalized, >95% similarity)
   - Fuzzy match (>75% similarity)
   - Translation detection (via known_translations from bibliography)
   - Partial match (key term overlap with 2+ shared terms)

## Deviations from Memo

1. **Added `link_orphan_papers()` method**: Not in original spec but needed to handle papers that don't match any bibliographic work. Creates Works for them with low confidence.

2. **TypedDict over Pydantic**: Used TypedDict for interface definitions instead of Pydantic models. This makes it easier to pass data between services without serialization overhead. Reconciler can convert to Pydantic if needed.

3. **Thresholds as class constants**: Made match thresholds configurable class constants:
   - EXACT_MATCH_THRESHOLD = 0.95
   - FUZZY_MATCH_THRESHOLD = 0.75
   - TRANSLATION_CONFIDENCE_MIN = 0.60

## Questions / Complications

1. **Model imports**: The service imports `Work`, `WorkEdition`, `EditionAnalysisRun` from `app.models`. Phase 1 needs to add these models. If models are in a different location, reconciler should update imports.

2. **Language detection**: The service relies on papers/editions having a `language` field. If this isn't populated, linking quality degrades. Phase 2 should detect language.

3. **Translation matching**: Currently matches translations by title similarity. Could be enhanced with LLM for ambiguous cases (e.g., "Geist der Utopie" → "Spirit of Utopia" has low string similarity).

## Interface for Phase 5

Phase 5 (Gap Analysis) should call:

```python
from app.services.edition_linking_service import EditionLinkingService, LinkingResult

service = EditionLinkingService(session)
result: LinkingResult = await service.link_editions_to_works(inventory, bibliography, run_id)

# result contains:
# - works_created: int
# - works_existing: int
# - links_created: int
# - links_existing: int
# - uncertain_matches: list[UncertainMatch]
# - papers_unmatched: int
# - editions_linked: int
```

After linking, Phase 5 can query Works and their WorkEditions to find gaps.
