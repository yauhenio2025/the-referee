# Phase 5 Handoff: Gap Analysis & Job Generation

> Implementor Session: 2026-01-10
> Phase: 5 - Gap Analysis & Job Generation
> Status: COMPLETE

## What Was Built

Created `backend/app/services/gap_analysis_service.py` with:

### GapAnalysisService Class
- `analyze_gaps()` - Compares linked Works against bibliography expectations
- `generate_scraper_jobs()` - Creates Job records for each gap found
- `persist_gap_analysis()` - Saves MissingEdition records (requires Phase 1 models)

### Convenience Function
- `analyze_and_generate_jobs()` - Full workflow in one call

## Dependencies from Other Phases

- **Phase 1**: Need `Work`, `WorkEdition`, `MissingEdition` models (gracefully handles absence)
- **Phase 2**: Uses `DossierInventory` structure (defined as TypedDict locally)
- **Phase 3**: Uses `ThinkerBibliography` structure (defined as TypedDict locally)
- **Phase 4**: Uses linked_works output (list of Work dicts with work_editions)

## TypedDicts Defined Locally

Since other phases are still in progress, I defined these TypedDicts in the service file:
- `ThinkerBibliography` - matches Phase 3 interface
- `DossierInventory` - matches Phase 2 interface
- `GapAnalysisResult` - output structure
- `MissingTranslation`, `MissingWork`, `OrphanEdition` - result item types

The reconciler should verify these match the actual implementations from Phases 2-4.

## Key Implementation Decisions

1. **Priority System**: Translation priorities based on:
   - Language importance (English/German/French = high, Spanish/Italian/Russian = medium, others = low)
   - Work importance (major works get higher priority)

2. **Job Type**: Uses existing `discover_editions` job type with extended params:
   - `gap_type`: "missing_translation" or "missing_work"
   - `target_language`: For translation searches
   - `expected_title`, `expected_year`: Search hints
   - `dossier_id`, `thinker_name`: Context

3. **Graceful Degradation**: If Phase 1 models aren't available, `persist_gap_analysis()`
   logs a warning and returns without error.

4. **Orphan Detection**: Identifies editions that exist but couldn't be linked to Works.

## Interface Provided

```python
class GapAnalysisService:
    async def analyze_gaps(
        self,
        dossier_id: int,
        bibliography: ThinkerBibliography,
        run_id: int,
        linked_works: Optional[List[Dict[str, Any]]] = None,
    ) -> GapAnalysisResult

    async def generate_scraper_jobs(
        self,
        gaps: GapAnalysisResult,
        dossier_id: int,
        thinker_name: str,
    ) -> List[Job]
```

## Interface Expected from Other Phases

### From Phase 3 (ThinkerBibliography):
```python
{
    "thinker": {"canonical_name": str, "primary_language": str, ...},
    "major_works": [
        {
            "canonical_title": str,
            "original_language": str,
            "original_title": str,
            "original_year": int,
            "importance": str,  # "major" or "minor"
            "known_translations": [
                {"language": str, "title": str, "year": int, "source": str}
            ]
        }
    ]
}
```

### From Phase 4 (linked_works):
```python
[
    {
        "canonical_title": str,
        "work_editions": [
            {"edition_id": int, "language": str, ...}
        ]
    }
]
```

## Questions for Reconciler

1. Should the gap analysis update the `EditionAnalysisRun` record with counts?
   I didn't add this since the run_id is passed but the model wasn't available.

2. Should orphan editions generate jobs or just be reported?
   Currently just reported - reconciler can add job generation if needed.

3. The existing `discover_editions` job type may need worker code to handle
   the new params (`gap_type`, `target_language`, etc.). The job worker
   should be updated to process these gap-fill jobs.
