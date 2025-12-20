# The Referee - Features Log

A chronological log of major features introduced to the project.

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
