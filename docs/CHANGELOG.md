# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- **Exhaustive Edition Analysis** - Full implementation of thinker-based bibliographic analysis
  - New tables: `works`, `work_editions`, `missing_editions`, `edition_analysis_runs`, `edition_analysis_llm_calls`
  - 5 new services: inventory, bibliographic agent, edition linking, gap analysis, job generation
  - Uses Claude Opus 4.5 with 32k thinking tokens + web search
  - 10 API endpoints for triggering analysis, reviewing results, creating jobs
  - Frontend component for analysis UI ([frontend/src/components/EditionAnalysis.jsx](../frontend/src/components/EditionAnalysis.jsx))

### Fixed
- **Duplicate index creation error** - Removed `__table_args__` Index definitions from edition analysis models ([backend/app/models.py](../backend/app/models.py)). Indexes are created via raw SQL migrations with `IF NOT EXISTS` in database.py; SQLAlchemy's `create_all()` was attempting to create them again without `IF NOT EXISTS`.
- **Closure scope error** - Initialize `effective_year_low` before callback definition in job_worker.py ([backend/app/services/job_worker.py:1370](../backend/app/services/job_worker.py))
- Dossier paper counts now correctly filter by collection_id ([backend/app/main.py](../backend/app/main.py)) - Previously, dossiers showed paper counts that included papers from other collections
- Dossier paper counts now exclude soft-deleted papers

### Added
- Stable URLs for dossiers in collection view ([frontend/src/App.jsx](../frontend/src/App.jsx), [frontend/src/components/CollectionDetail.jsx](../frontend/src/components/CollectionDetail.jsx))
  - URLs now include dossier selection: `/collections/3?dossier=5` or `/collections/3?dossier=unassigned`
  - Browser back/forward navigation preserves dossier selection
  - URLs can be shared/bookmarked to link directly to a specific dossier

---
