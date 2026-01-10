# Feature Inventory

> Auto-maintained by Claude Code. Last updated: 2026-01-10

## Collections & Dossiers

### Collection Management
- **Status**: Active
- **Description**: Organize papers into named collections with optional color coding
- **Entry Points**:
  - `backend/app/main.py:509-528` - Create collection endpoint
  - `backend/app/main.py:534-580` - List collections endpoint
  - `backend/app/main.py:581-628` - Get collection with papers endpoint
  - `frontend/src/components/Collections.jsx` - Collections list UI
  - `frontend/src/components/CollectionDetail.jsx` - Collection detail view
- **Dependencies**: Paper model, SQLAlchemy
- **Added**: 2024 | **Modified**: 2026-01-10

### Dossier System
- **Status**: Active
- **Description**: Sub-organize papers within collections into dossiers (folders)
- **Entry Points**:
  - `backend/app/main.py:752-786` - List/create dossiers endpoints
  - `backend/app/main.py:785-834` - Get dossier with papers endpoint
  - `backend/app/main.py:906-938` - Assign papers to dossier endpoint
  - `frontend/src/components/CollectionDetail.jsx:302-439` - Dossier sidebar UI
  - `frontend/src/components/CollectionDetail.jsx:177-194` - Paper filtering by dossier
- **Dependencies**: Collection, Paper models
- **Added**: 2024 | **Modified**: 2026-01-10

### Dossier Stable URLs
- **Status**: Active
- **Description**: URL reflects selected dossier for bookmarking and sharing
- **Entry Points**:
  - `frontend/src/App.jsx:266-292` - CollectionDetailRoute with URL params
  - `frontend/src/components/CollectionDetail.jsx:17-25` - Dossier state sync with URL
- **Dependencies**: react-router-dom useSearchParams
- **Added**: 2026-01-10

## Paper Management

### Paper Input & Resolution
- **Status**: Active
- **Description**: Add papers by title/DOI, resolve via Google Scholar
- **Entry Points**:
  - `frontend/src/components/PaperInput.jsx` - Paper input form
  - `backend/app/main.py` - Paper CRUD endpoints
- **Dependencies**: Google Scholar scraping
- **Added**: 2024

### Edition Discovery
- **Status**: Active
- **Description**: Find foreign editions and translations of papers
- **Entry Points**:
  - `frontend/src/components/PaperEditions.jsx` - Edition management UI
  - `backend/app/services/` - Edition discovery services
- **Dependencies**: Google Scholar API
- **Added**: 2024

## Citation Harvesting

### Citation Job Queue
- **Status**: Active
- **Description**: Background job system for harvesting paper citations
- **Entry Points**:
  - `frontend/src/components/Jobs.jsx` - Job queue UI
  - `backend/app/services/job_worker.py` - Job processing
- **Dependencies**: Redis/Database for job queue
- **Added**: 2024
