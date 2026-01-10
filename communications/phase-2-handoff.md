# Phase 2 Handoff: Dossier Inventory Service

> Implementor Session: 2026-01-10
> Phase: 2 - Dossier Inventory Service
> Status: COMPLETE

## Understanding

Phase 2 builds a service to analyze all papers/editions in a dossier:
- Extract titles, languages, years, authors
- Group by apparent work (fuzzy title matching)
- Detect obvious original/translation pairs
- **No LLM calls** - pure data extraction

## Interface Delivered

```python
class InventoryService:
    async def analyze_dossier(self, dossier_id: int) -> DossierInventory
    async def get_papers_by_title(self, dossier_id: int, title_pattern: str) -> List[PaperInfo]
```

Plus standalone utilities:
- `detect_language(title)` - Rule-based language detection
- `title_similarity(t1, t2)` - Fuzzy title matching
- `cluster_titles(papers, threshold)` - Group by title similarity
- `normalize_title(title)` - Strip subtitles/brackets for comparison

## Implementation Decisions

1. **Conservative language detection**: Returns `None` when uncertain rather than guessing. The LLM in Phase 3 handles ambiguous cases.

2. **Union-Find clustering**: O(n²) comparison but efficient grouping. Fine for typical dossier sizes (<100 papers).

3. **Thinker name = dossier name**: Simple inference that works for the expected use case (dossiers named after thinkers like "Bloch").

4. **Dataclasses for type safety**: All data structures are dataclasses with `to_dict()` for JSON serialization.

## Notes for Reconciler

- File location: `backend/app/services/inventory_service.py`
- No database schema changes required
- Uses existing `Paper`, `Edition`, `Dossier` models
- Follows async patterns from existing services (e.g., `thinker_service.py`)

## Dependencies

- Phase 1 models (existing - already in codebase)
- Python standard library only (no new pip dependencies)
