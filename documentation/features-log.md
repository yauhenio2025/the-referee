# The Referee - Features Log

A chronological log of major features introduced to the project.

---

## 2025-12-22: Bug Fix - Dossier Creation When Adding Seeds

**Commit:** `b8ba68b` on `main`

**Description:** Fixed two bugs that prevented dossiers from being created when adding seeds from the Citations page.

**Bugs Fixed:**
1. **Backend**: `create_paper` endpoint ignored `dossier_id` from request (line was missing)
2. **Frontend**: `Citations.jsx` didn't create new dossiers before creating papers

**Root Cause:** When user selected "Create new dossier" in the modal, the dossier was never actually created. Papers were added with `dossier_id: null`.

**Files Modified:**
- `backend/app/main.py` - Added `dossier_id=paper.dossier_id` to create_paper
- `frontend/src/components/Citations.jsx` - Added dossier creation before paper creation

---

## 2025-12-22: Dossiers & Terminology Cleanup

**Commit:** `9be6494` on `main`

**Description:** Added Dossiers as an organizational layer between Collections and Papers. Dossiers allow grouping related Works (seeds) within a Collection. Also created terminology documentation and updated UI labels to use clearer language.

**Key Changes:**
- Backend: Added `Dossier` model (belongs to Collection, has many Papers)
- Backend: Added `dossier_id` to Paper model
- Backend: CRUD endpoints for Dossiers (`/api/dossiers`)
- Backend: Updated `add_edition_as_seed` to support dossier selection
- Frontend: Created `DossierSelectModal.jsx` component
- Frontend: Editions page - "Add as Seed" now prompts for dossier selection
- Frontend: Citations page - Added "Track this paper" button with dossier selection
- Frontend: Updated tooltips and messages for clarity
- Documentation: Created `terminology.md` with ontology definitions

**Organizational Hierarchy:**
```
Collection → Dossier → Work (Seed) → Editions → Citing Papers
```

**Files Modified:**
- `backend/app/models.py`
- `backend/app/schemas.py`
- `backend/app/database.py`
- `backend/app/main.py`
- `frontend/src/lib/api.js`
- `frontend/src/components/DossierSelectModal.jsx` (new)
- `frontend/src/components/EditionDiscovery.jsx`
- `frontend/src/components/Citations.jsx`
- `documentation/terminology.md` (new)

---

## 2025-12-22: Per-Edition Harvest Button

**Commit:** `499901c` on `main`

**Description:** Added ability to manually start/resume harvest for a specific edition, useful when auto-resume fails or user wants to prioritize a particular edition.

**Key Changes:**
- Backend: `POST /api/editions/{edition_id}/harvest` endpoint
- Frontend: Harvest button (📥) on each edition row
- Frontend: Visual feedback for harvesting state

**Files Modified:**
- `backend/app/main.py`
- `frontend/src/lib/api.js`
- `frontend/src/components/EditionDiscovery.jsx`

---

## 2025-12-22: Smart Resume Fix & Pause/Unpause Harvest

**Commit:** `bf3a9ae`, `3bdf66e` on `main`

**Description:** Fixed year-by-year harvest resume logic to only trust saved state (not re-derive from citations). Added ability to pause/unpause automatic harvest resume for specific papers.

**Key Changes:**
- Backend: Fixed `harvest_resume_state` handling in job worker
- Backend: Added `harvest_paused` field to Paper model
- Backend: `POST /api/papers/{id}/pause-harvest` and `unpause-harvest` endpoints
- Auto-resume skips paused papers

**Files Modified:**
- `backend/app/models.py`
- `backend/app/database.py`
- `backend/app/services/job_worker.py`
- `backend/app/main.py`
- `frontend/src/lib/api.js`

---

## 2025-12-20: Edition Management - Add as Seed, Exclude, Finalize

**Commit:** `700fa60` on `main`

**Description:** Three new capabilities for managing edition candidates on the EditionDiscovery page. Users can now: convert interesting candidates into independent seed papers, exclude irrelevant candidates from view, and finalize the edition selection to show only selected editions.

**Key Changes:**
- Backend: Added `excluded` field to Edition model
- Backend: Added `editions_finalized` field to Paper model
- Backend: `POST /api/editions/exclude` - Exclude/unexclude editions
- Backend: `POST /api/editions/{id}/add-as-seed` - Convert edition to new seed paper
- Backend: `POST /api/papers/{id}/finalize-editions` - Finalize edition view
- Backend: `POST /api/papers/{id}/reopen-editions` - Reopen for editing
- Frontend: Add as Seed button (🌱) on each edition row
- Frontend: Exclude button (⊘) on each edition row
- Frontend: "Finalize Editions" / "Reopen Editions" toggle in toolbar
- Frontend: "Show/Hide Excluded" toggle for viewing excluded editions
- Frontend: Finalized banner with status message
- Frontend: Excluded editions group with strikethrough styling
- CSS: Comprehensive styling for new states and buttons

**Files Modified:**
- `backend/app/models.py`
- `backend/app/schemas.py`
- `backend/app/database.py`
- `backend/app/main.py`
- `frontend/src/lib/api.js`
- `frontend/src/components/EditionDiscovery.jsx`
- `frontend/src/App.css`

---

## 2025-12-20: Advanced Job Queue Monitor

**Commit:** `d96ab70` on `main`

**Description:** Complete overhaul of the Job Queue UI to provide real-time detailed progress tracking for running jobs. Users can now see exactly what's happening during citation harvesting including edition details, citation counts, current page, and year-by-year mode status.

**Key Changes:**
- Backend: Extended `update_job_progress()` to accept detailed progress data stored in job params
- Backend: Citation extraction callback now passes rich progress details including edition info, harvest stats, current year
- Frontend: Complete rewrite of JobQueue.jsx with card-based active jobs and compact recent jobs table
- Frontend: Detailed harvest info panel showing edition title, language, citations saved/total, current page, year
- Frontend: Large animated progress bar with percentage overlay
- Frontend: Year-by-year mode badge indicator
- CSS: Comprehensive styling for job cards, stat boxes, harvest details, progress animations

**Files Modified:**
- `backend/app/services/job_worker.py`
- `frontend/src/components/JobQueue.jsx`
- `frontend/src/App.css`

---

## 2025-12-20: Auto-Resume Incomplete Harvests

**Commit:** `c9e48ff` on `main`

**Description:** When citation harvesting stops with a significant gap (at least 100 missing citations or 10% of total), the system now automatically queues resume jobs. The worker checks for incomplete harvests every 60 seconds when idle and queues continuation jobs to eventually get all citations.

**Key Changes:**
- Backend: `find_incomplete_harvests()` detects editions with significant gaps
- Backend: `auto_resume_incomplete_harvests()` queues continuation jobs
- Worker loop integration: checks every 60s when no pending jobs
- Schema: Added `is_incomplete` and `missing_citations` computed fields
- Frontend: New red pulsing "incomplete" status badge showing missing count
- Tooltip explains auto-resume will handle the completion

**Files Modified:**
- `backend/app/services/job_worker.py`
- `backend/app/main.py`
- `backend/app/schemas.py`
- `frontend/src/components/EditionDiscovery.jsx`
- `frontend/src/App.css`

---

## 2025-12-20: Quick Harvest

**Commit:** `174a67d` on `main`

**Description:** Quick Harvest allows users to skip the edition discovery workflow and immediately start harvesting citations for a resolved paper. The button appears on paper cards that have been resolved but don't yet have any editions or harvested citations.

**Key Changes:**
- Backend: `POST /api/papers/{paper_id}/quick-harvest` endpoint
- Creates an edition from the paper's Scholar data if one doesn't exist
- Marks edition as selected and queues citation extraction job
- Frontend: Purple ⚡ Quick Harvest button on paper cards
- CSS: Quick harvest button styling with hover effects

**Files Modified:**
- `backend/app/main.py`
- `frontend/src/lib/api.js`
- `frontend/src/components/PaperList.jsx`
- `frontend/src/App.css`

---

## 2025-12-20: Citation Auto-Updater (Frontend)

**Commit:** `e89061b` on `main`

**Description:** Frontend implementation for the Citation Auto-Updater feature, providing visual staleness indicators and refresh controls throughout the UI.

**Key Changes:**
- Paper Cards: Harvested count display, staleness badges, Refresh button
- Edition Table: Status column with staleness indicators, Refresh Citations button
- Collection View: Refresh All button with progress tracking
- API Client: New methods for refresh and staleness endpoints
- CSS: Staleness badge styles, refresh button variants, progress animations

**Files Modified:**
- `frontend/src/lib/api.js`
- `frontend/src/components/PaperList.jsx`
- `frontend/src/components/EditionDiscovery.jsx`
- `frontend/src/components/CollectionDetail.jsx`
- `frontend/src/App.css`

---

## 2025-12-20: Citation Auto-Updater (Backend)

**Commit:** `d85019b` on `main`

**Description:** Backend functionality to track citation harvest freshness and refresh citations at paper, collection, or global scope. Uses year-aware re-harvesting to only fetch new citations since the last harvest.

**Key Changes:**
- Schema: Added `last_harvested_at`, `last_harvest_year`, `harvested_citation_count` to Edition model
- Schema: Added `any_edition_harvested_at`, `total_harvested_citations` to Paper model
- Job Worker: Extended `extract_citations` job to support refresh mode with `year_low` filtering
- Job Worker: Added `update_edition_harvest_stats()` and `update_paper_harvest_stats()` helpers
- API: `POST /api/refresh/paper/{id}` - Refresh single paper
- API: `POST /api/refresh/collection/{id}` - Refresh all papers in collection
- API: `POST /api/refresh/global` - Refresh all (optionally stale-only)
- API: `GET /api/refresh/status?batch_id={id}` - Track refresh progress
- API: `GET /api/staleness` - Report on stale papers/editions
- UI: Added `is_stale` and `days_since_harvest` computed fields to responses
- 90-day staleness threshold

**Files Modified:**
- `backend/app/models.py`
- `backend/app/schemas.py`
- `backend/app/database.py`
- `backend/app/services/job_worker.py`
- `backend/app/main.py`

---

## 2025-12-20: Add as Seed from Reconciliation Modal

**Commit:** `a1f955e` on `main`

**Description:** When reconciling papers with multiple Scholar matches, users can now add interesting candidates as new seeds even if they're not the right match for the current paper.

**Key Changes:**
- Added "Add as Seed" button to each candidate card in reconciliation modal
- Creates new paper with title, authors, year, venue from candidate
- New paper starts in "pending" status for later resolution
- CSS styling for new button

**Files Modified:**
- `frontend/src/components/PaperList.jsx`
- `frontend/src/App.css`

---

## 2025-12-20: Parallel Job Processing

**Commit:** `7966dfb` on `main`

**Description:** Implemented parallel job processing to allow multiple citation harvesting jobs to run simultaneously. This makes better use of Oxylabs credits and speeds up bulk operations.

**Key Changes:**
- `MAX_CONCURRENT_JOBS = 5` configurable limit
- asyncio.Semaphore for concurrency control
- Running jobs tracked with `_running_jobs` set
- Worker loop grabs multiple pending jobs at once
- Staggered job starts (0.5s) to avoid race conditions
- Slot acquisition/release logged for monitoring

**Files Modified:**
- `backend/app/services/job_worker.py`

---

## 2025-12-19: Year-by-Year Citation Harvesting

**Description:** For editions with >1000 citations (Google Scholar's limit), implemented year-by-year fetching from current year backwards to 1990 to bypass the limit and harvest all citations.

**Key Changes:**
- Raised citation cap from 500 to 1000
- Added YEAR_BY_YEAR_THRESHOLD = 1000
- Fetches from current year backwards
- Stops after 3 consecutive empty years
- Successfully tested: English edition 5268 citations -> 5076 harvested (96%)

**Files Modified:**
- `backend/app/main.py`
- `backend/app/services/job_worker.py`

---

## 2025-12-19: Edition-Based Citation Filtering

**Description:** Added ability to filter citations by specific edition with URL query param support for shareable links.

**Key Changes:**
- Added `harvested_citations` field to EditionResponse schema
- Editions API computes harvested count per edition
- "Harvested" column in EditionDiscovery table (clickable)
- Citations component reads `?edition=` query param
- URL updates when edition filter changes

**Files Modified:**
- `backend/app/schemas.py`
- `backend/app/main.py`
- `frontend/src/components/EditionDiscovery.jsx`
- `frontend/src/components/Citations.jsx`
- `frontend/src/App.css`

---

## Earlier Features

- Citation harvesting with Google Scholar integration
- Edition discovery across multiple languages
- Theme switching (light/dark mode)
- Job queue system with progress tracking
- Resume capability for interrupted jobs
