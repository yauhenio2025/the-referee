# Phase 5 Final Report

> Implementor Session: 2026-01-10
> Phase: 5 - Gap Analysis & Job Generation
> Status: **COMPLETE**

## What Was Built

Created a complete Gap Analysis service at `backend/app/services/gap_analysis_service.py`:

1. **GapAnalysisService class** with:
   - `analyze_gaps()` - Compares linked Works against bibliography expectations
   - `generate_scraper_jobs()` - Creates Job records to fill identified gaps
   - `persist_gap_analysis()` - Saves MissingEdition records (requires Phase 1 models)

2. **Convenience function**:
   - `analyze_and_generate_jobs()` - Full workflow in one call

3. **Supporting infrastructure**:
   - TypedDict definitions for all data structures
   - Priority calculation system for translations
   - Query builders for Google Scholar searches

## Files Changed

- `backend/app/services/gap_analysis_service.py` - **NEW** (350+ lines)

## Deviations from Memo

### 1. TypedDicts Instead of Direct Imports
Since other phases are still in progress, I defined the expected interfaces as TypedDicts locally:
- `ThinkerBibliography` - matches Phase 3 interface from MASTER_MEMO
- `DossierInventory` - matches Phase 2 interface from MASTER_MEMO
- `GapAnalysisResult` - output structure per MASTER_MEMO

The reconciler should verify these match and potentially move them to a shared types module.

### 2. Graceful Degradation for Phase 1 Models
The `persist_gap_analysis()` method tries to import Phase 1's `MissingEdition` model:
- If available: Creates proper MissingEdition records
- If not available: Logs warning and returns without error

This allows the service to work even before Phase 1 is complete.

### 3. Using Existing Job Type
Used the existing `discover_editions` job type with extended params rather than creating a new job type. The params distinguish gap-fill jobs:
```json
{
  "gap_type": "missing_translation",  // or "missing_work"
  "target_language": "english",
  "expected_title": "Thomas Müntzer as Theologian of Revolution",
  "dossier_id": 123,
  "thinker_name": "Ernst Bloch"
}
```

## Interface Provided

```python
# Main service
class GapAnalysisService:
    async def analyze_gaps(
        self,
        dossier_id: int,
        bibliography: ThinkerBibliography,
        run_id: int,
        linked_works: Optional[List[Dict[str, Any]]] = None,
    ) -> GapAnalysisResult:
        """
        Compare what we have against what should exist.
        Returns missing translations, missing works, orphan editions.
        """

    async def generate_scraper_jobs(
        self,
        gaps: GapAnalysisResult,
        dossier_id: int,
        thinker_name: str,
    ) -> List[Job]:
        """
        Create Job records for each gap found.
        Jobs use 'discover_editions' type with gap-specific params.
        """

    async def persist_gap_analysis(
        self,
        dossier_id: int,
        run_id: int,
        gaps: GapAnalysisResult,
        jobs: List[Job],
    ) -> Dict[str, Any]:
        """
        Save MissingEdition records to database.
        Requires Phase 1 models; gracefully handles their absence.
        """

# Convenience function
async def analyze_and_generate_jobs(
    db: AsyncSession,
    dossier_id: int,
    bibliography: ThinkerBibliography,
    run_id: int,
    linked_works: Optional[List[Dict[str, Any]]] = None,
    auto_persist: bool = True,
) -> Dict[str, Any]:
    """
    Full workflow: analyze gaps → generate jobs → persist results.
    """
```

## Interface Expected

### From Phase 3 (ThinkerBibliography):
```python
{
    "thinker": {
        "canonical_name": str,
        "primary_language": str,
        "birth_death": str,
        "domains": List[str]
    },
    "major_works": [
        {
            "canonical_title": str,
            "original_language": str,
            "original_title": str,
            "original_year": int,
            "work_type": str,
            "importance": str,  # "major" or "minor"
            "known_translations": [
                {
                    "language": str,
                    "title": str,
                    "year": int,
                    "translator": str,
                    "source": str  # "llm_knowledge", "web_search", "scholar"
                }
            ],
            "scholarly_significance": str
        }
    ],
    "verification_sources": List[str],
    "confidence": float
}
```

### From Phase 4 (linked_works):
```python
[
    {
        "canonical_title": str,
        "work_editions": [
            {"edition_id": int, "language": str, "title": str, "year": int}
        ]
    }
]
```

## Known Issues

1. **Phase 1 Dependency**: The `persist_gap_analysis()` method requires Phase 1's `MissingEdition` model. Currently gracefully handles its absence but the reconciler should ensure it works once Phase 1 is complete.

2. **Job Worker Integration**: The job worker (`job_worker.py`) needs to be updated to handle the new gap-fill job params (`gap_type`, `target_language`, etc.). Currently it will run `discover_editions` jobs but may not use all the params.

3. **Run Tracking**: The `run_id` parameter is passed but not used to update `EditionAnalysisRun` counts since that model isn't available. Reconciler should add this.

## Testing Done

- [x] Service file created and compiles without syntax errors
- [x] All imports resolve correctly (models, typing, etc.)
- [ ] Integration test with real dossier (needs Phase 1-4 complete)
- [ ] End-to-end test with Bloch dossier (needs full integration)

## Questions for Reconciler

1. **EditionAnalysisRun updates**: Should `analyze_gaps` update the run record with `gaps_found`, `jobs_created` counts? I have the run_id but didn't have access to the model.

2. **Orphan handling**: Should orphan editions (ones we have but can't link to Works) also generate jobs? Currently just reported.

3. **Job worker updates**: The `discover_editions` job handler needs to be updated to:
   - Check for `gap_type` in params
   - Use `target_language` for focused searches
   - Use `expected_title` and `expected_year` as search hints

4. **TypedDict consolidation**: The TypedDicts defined here should probably move to a shared types module and be verified against Phases 2-4 implementations.

## Example Usage

```python
from backend.app.services.gap_analysis_service import (
    GapAnalysisService,
    analyze_and_generate_jobs,
    ThinkerBibliography
)

async def run_gap_analysis(db: AsyncSession, dossier_id: int):
    # Assume bibliography comes from Phase 3
    bibliography: ThinkerBibliography = await bibliographic_agent.research(...)

    # Assume linked_works comes from Phase 4
    linked_works = await edition_linking_service.link_editions(...)

    # Run full workflow
    result = await analyze_and_generate_jobs(
        db=db,
        dossier_id=dossier_id,
        bibliography=bibliography,
        run_id=run.id,
        linked_works=linked_works,
    )

    print(f"Found {len(result['gaps']['missing_translations'])} missing translations")
    print(f"Created {result['jobs_created']} scraper jobs")
```

## Example Output (Bloch Dossier)

```json
{
  "gaps": {
    "missing_translations": [
      {
        "work_canonical_title": "Thomas Müntzer as Theologian of Revolution",
        "original_language": "german",
        "missing_language": "english",
        "expected_title": "Thomas Müntzer as Theologian of Revolution",
        "expected_year": 1989,
        "priority": "high",
        "source": "web_search"
      }
    ],
    "missing_works": [],
    "orphan_editions": []
  },
  "jobs_created": 1,
  "job_ids": [456],
  "persistence_result": {
    "persisted": true,
    "missing_translations_saved": 1
  }
}
```
