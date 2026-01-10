# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- **Exhaustive Edition Analysis** - Multi-session implementation plan for thinker-based bibliographic analysis ([communications/MASTER_MEMO.md](../communications/MASTER_MEMO.md))
  - Schema design for Work, WorkEdition, MissingEdition tables
  - Bibliographic Research Agent using Claude Opus 4.5 with 32k thinking + web search
  - Gap analysis to identify missing translations and major works
  - Job generation to create scraper tasks for missing literature

### Fixed
- Dossier paper counts now correctly filter by collection_id ([backend/app/main.py](../backend/app/main.py)) - Previously, dossiers showed paper counts that included papers from other collections
- Dossier paper counts now exclude soft-deleted papers

### Added
- Stable URLs for dossiers in collection view ([frontend/src/App.jsx](../frontend/src/App.jsx), [frontend/src/components/CollectionDetail.jsx](../frontend/src/components/CollectionDetail.jsx))
  - URLs now include dossier selection: `/collections/3?dossier=5` or `/collections/3?dossier=unassigned`
  - Browser back/forward navigation preserves dossier selection
  - URLs can be shared/bookmarked to link directly to a specific dossier

---
