# Phase 1 Handoff Notes

> Implementor Session: 2026-01-10
> Phase: 1 - Schema Extensions & Models
> Status: COMPLETE

## Progress Log

### Completed
1. Read and understood MASTER_MEMO.md
2. Analyzed existing models.py and database.py structure
3. Created all 5 new SQLAlchemy models
4. Added relationships to existing models (Paper, Edition, Dossier)
5. Added migration SQL to database.py
6. Verified all imports work correctly

## Key Decisions

### 1. No Alembic - Using Custom Migration System
The project doesn't use Alembic. Instead, it uses a custom `run_migrations()` function in `database.py` that runs raw SQL with `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`. I followed this pattern for consistency.

### 2. Work.thinker_name as String (Not FK)
The MASTER_MEMO showed `thinker_name VARCHAR(255)` as a string, not a foreign key to the Thinker table. This is correct because:
- Works can exist for thinkers that don't have a Thinker record yet
- Keeps the schema flexible for different analysis contexts
- Matches the memo specification exactly

### 3. Partial Unique Indexes for WorkEdition
Used PostgreSQL partial unique indexes (`WHERE edition_id IS NOT NULL`) instead of SQLAlchemy's `postgresql_where` in model definition because:
- The migration SQL runs via raw execution
- More explicit and easier to debug
- Partial indexes only enforced when values are non-null

### 4. Relationships Added to Existing Models
- `Paper.work_edition` - One-to-one (uselist=False) to WorkEdition
- `Edition.work_edition` - One-to-one (uselist=False) to WorkEdition
- `Dossier.edition_analysis_runs` - One-to-many to EditionAnalysisRun

## Deviations from Memo

### Minor Schema Enhancements
1. **Added `link_reason` to WorkEdition** - Stores why the link was made, useful for debugging
2. **Added `phase_progress` to EditionAnalysisRun** - 0.0-1.0 float for progress bar UI
3. **Added `expected_translator` and `expected_publisher` to MissingEdition** - More complete gap tracking
4. **Added `source_details` to MissingEdition** - JSON for additional verification info
5. **Added `web_search_queries` and `web_sources_cited` to EditionAnalysisLLMCall** - Better audit trail

### Removed from Memo Spec
1. **Work.unique constraint** - Changed from `UNIQUE(thinker_name, canonical_title)` table constraint to unique index (same effect, better for PostgreSQL)

## Files Changed

- `backend/app/models.py` - Added 5 new model classes at end of file (~290 new lines)
  - `Work` (lines 1105-1152)
  - `WorkEdition` (lines 1155-1199)
  - `MissingEdition` (lines 1202-1251)
  - `EditionAnalysisRun` (lines 1254-1335)
  - `EditionAnalysisLLMCall` (lines 1338-1399)

- `backend/app/models.py` - Added relationships to existing models
  - `Paper.work_edition` relationship (lines 132-137)
  - `Edition.work_edition` relationship (lines 225-230)
  - `Dossier.edition_analysis_runs` relationship (lines 49-54)

- `backend/app/database.py` - Added migration SQL (lines 230-353)
  - CREATE TABLE for works, work_editions, missing_editions, edition_analysis_runs, edition_analysis_llm_calls
  - CREATE INDEX for all performance-critical columns
  - Partial unique indexes for work_editions

## Interface Provided

```python
# New models available for import
from app.models import (
    Work,                    # Abstract intellectual work
    WorkEdition,             # Links Work to Paper/Edition
    MissingEdition,          # Gap identified in coverage
    EditionAnalysisRun,      # Analysis run audit trail
    EditionAnalysisLLMCall,  # LLM call audit trail
)

# New relationships on existing models
paper.work_edition         # Optional[WorkEdition] - if linked to a Work
edition.work_edition       # Optional[WorkEdition] - if linked to a Work
dossier.edition_analysis_runs  # List[EditionAnalysisRun]

# Accessing Work from Paper or Edition
if paper.work_edition:
    work = paper.work_edition.work
    print(f"Paper linked to: {work.canonical_title}")
```

## Testing Done

- [x] All new models import without error
- [x] All relationships properly defined and accessible
- [x] No circular import issues
- [x] Python syntax validated

## Questions for Reconciler

1. Should we add a method to Paper/Edition for getting the Work directly? (e.g., `paper.work` property that follows the relationship chain)

2. The EditionAnalysisRun has `dossier_id` as NOT NULL but the ForeignKey allows SET NULL on delete. Should it be CASCADE instead to match the cascade delete pattern?

## Notes for Other Phases

### Phase 2 (Inventory Service)
- Use `Paper.editions` relationship to get all editions for a paper
- Use `Paper.dossier` relationship to navigate from paper to dossier
- The `Edition.language` field exists but may be null - inventory service should detect language from title

### Phase 4 (Edition Linking)
- Create `WorkEdition` records to link papers/editions to works
- The unique constraints prevent double-linking (one paper/edition can only link to one work)
- Set `auto_linked=True` for algorithmic links, `verified=False` initially

### Phase 5 (Gap Analysis)
- Create `MissingEdition` records for identified gaps
- The unique constraint on (work_id, language) prevents duplicate gap entries
- Use `Job` model for creating scraper jobs, link via `MissingEdition.job_id`
