# The Referee - Features Log

A chronological log of major features introduced to the project.

---

## 2026-01-10: Citation Harvesting Audit & Critical Fixes

**Description:** Thorough audit of the citation harvesting system revealed and fixed three critical issues affecting letter-based partitioning, resume logic, and query traceability.

**Issues Found & Fixed:**

### 1. Letter-Based Partitioning Callback Bug (HIGH PRIORITY)
- **Problem:** `save_page_citations()` didn't return a value, so `wrapped_on_page_complete()` in overflow_harvester.py couldn't update `new_citations`, leaving all `harvest_targets.actual_count = 0`
- **Evidence:** 1,018 letter-based HarvestTargets with `actual_count = 0` despite citations being harvested
- **Fix:** Added `return new_count` at end of `save_page_citations()` function

### 2. Resume Logic Not Using harvest_resume_state (HIGH PRIORITY)
- **Problem:** `harvest_resume_state` column existed but was never consulted during harvesting - resume logic only counted existing citations to calculate page offset, re-fetching all years from beginning
- **Evidence:** Harvey's book showed 34,077 unique citations but 125,252 total encounters (3.7x duplicates)
- **Fix:** Added full resume state management in `harvest_with_author_letter_strategy()`:
  - Load resume state from edition at start
  - Track `completed_partitions` set (lang codes, letters)
  - Skip already-completed partitions
  - Update resume state after each partition completes

### 3. Query Traceability Gap (MEDIUM PRIORITY)
- **Problem:** Overflow harvesting had full traceability via PartitionRun/PartitionQuery tables, but standard harvesting had NO query logging
- **Fix:** Added universal `HarvestQuery` model and `log_harvest_query()` helper for all harvesting operations

**New Database Table:**
```sql
CREATE TABLE harvest_queries (
    id SERIAL PRIMARY KEY,
    edition_id INTEGER REFERENCES editions(id) ON DELETE CASCADE,
    job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
    query_string TEXT NOT NULL,
    partition_type VARCHAR(20),  -- 'standard', 'year', 'letter', 'lang'
    partition_value VARCHAR(50), -- null, '2020', 'A', 'zh-CN'
    page_number INTEGER DEFAULT 0,
    results_count INTEGER,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Files Modified:**
- `backend/app/services/job_worker.py` - Fixed callback return, added query logging
- `backend/app/services/overflow_harvester.py` - Added resume state management, query logging
- `backend/app/models.py` - Added HarvestQuery model
- `backend/app/database.py` - Added migration for harvest_queries table
- `backend/app/services/api_logger.py` - Added log_harvest_query() helper

---

## 2025-12-22: Citation Search & Faceted Filtering

**Commit:** `492fa3e` on `main`

**Description:** Added search and faceted filtering to the Citations page for finding specific citing papers.

**Key Changes:**

### Search Box
- Full-text search across title, author, and venue
- Instant filtering as you type
- Clear button to reset search

### Top Authors Facet
- Shows authors ranked by citation count
- Click to filter citations by that author
- Expandable to show all authors (top 8 by default)
- Authors extracted and normalized from citation data

### Top Venues Facet
- Shows venues ranked by citation count
- Click to filter by venue
- Expandable list
- Venue names normalized for grouping

### UI/UX
- Responsive two-column layout for facets
- Active filters shown in results count
- Toggle selected facet to clear filter

**Files Modified:**
- `frontend/src/components/Citations.jsx` - Search, facets, filtering logic
- `frontend/src/App.css` - Search box and facet styling

---

## 2025-12-22: Bug Fix - Auto-Resume Creating Duplicate Jobs

**Commit:** `3f4d6fb` on `main`

**Description:** Fixed critical bug where auto-resume was creating multiple jobs for the same paper, wasting API credits.

**The Bug:**
- When a paper had multiple editions (e.g., English, German, French versions)
- `auto_resume_incomplete_harvests()` created a SEPARATE job for EACH edition
- Each job fetched the same citations from Scholar API
- Citations de-duplicated in DB, but API calls already made = wasted credits

**The Fix:**
- Group incomplete editions by paper_id
- Create ONE job per paper with ALL edition_ids included
- Citations fetched once, not N times

**Files Modified:**
- `backend/app/services/job_worker.py` - Fixed `auto_resume_incomplete_harvests()`

---

## 2025-12-22: Collection Badges & Auto-Hide Processed Papers

**Commit:** `e92cd9d` on `main`

**Description:** Added visual indicators for papers already in collections and automatic hiding of processed papers from the landing page.

**Key Changes:**

### Collection/Dossier Badges
- Paper cards show collection/dossier badge when assigned
- Badge displays collection name and dossier (if any)
- Left border uses collection's color for visual consistency
- Truncated display with full path in tooltip

### Hide Processed Papers
- Processed papers hidden from landing page by default
- "Processed" = has collection_id + total_harvested_citations > 0
- Toggle checkbox to show/hide processed papers
- Counter shows how many papers are hidden
- Dimmed styling for processed papers when shown

**Files Modified:**
- `frontend/src/components/PaperList.jsx` - Badges, filtering, toggle
- `frontend/src/App.css` - Badge and toggle styling

---

## 2025-12-22: Add to Collection & Expandable Editions

**Commit:** `a18a5dd` on `main`

**Description:** Added ability to add papers to collections from anywhere in the app, plus expandable editions view in collection detail.

**Key Changes:**

### Add to Collection
- PaperList: Added "📁 Add to Collection" button on each paper card
- EditionDiscovery: Added "📁 Add to Collection" button in action bar
- Both use DossierSelectModal for consistent UX
- Can select existing collection/dossier or create new

### Expandable Editions in Collection Detail
- New "Editions" column shows count badge for each paper
- Click to expand and see all editions inline
- Nested table shows: Language, Title, Citations, Harvested count, Confidence
- Lazy-loads editions on first expand for performance
- Visual hierarchy with indentation and color coding
- Confidence badges (high/uncertain/rejected)

**Files Modified:**
- `frontend/src/components/PaperList.jsx` - Added Add to Collection button
- `frontend/src/components/EditionDiscovery.jsx` - Added Add to Collection button
- `frontend/src/components/CollectionDetail.jsx` - Added expandable editions
- `frontend/src/App.css` - Added CSS for expandable editions panel

---

## 2025-12-22: Soft Delete with Undo & Dossier Management UI

**Commit:** `fbeba6e` on `main`

**Description:** Major UI enhancement adding soft delete with undo for papers and a complete dossier management interface with power-user features.

**Key Changes:**

### Soft Delete with Undo
- Backend: Added `deleted_at` timestamp to Paper model
- Backend: Modified delete endpoint to soft delete by default, with `permanent=true` option
- Backend: Added `POST /api/papers/{id}/restore` endpoint
- Backend: All paper listing queries now exclude deleted papers
- Frontend: Toast notifications now support action buttons
- Frontend: Delete actions show "Undo" toast for 8 seconds before permanent deletion
- Frontend: Toast component enhanced with close button and action support

### Dossier Management UI
- Complete rewrite of CollectionDetail.jsx with dossier sidebar
- Dossier sidebar shows all dossiers with paper counts
- Create/edit/delete dossier functionality
- "Unassigned" virtual dossier for papers without dossier
- Multi-select papers with shift-click and cmd-click
- Drag-and-drop paper assignment to dossiers
- Search/filter papers within collection
- Keyboard shortcuts: ⌘N (new dossier), ⌘A (select all), Esc (clear selection)
- Power-user friendly with visual keyboard hints

**Files Modified:**
- `backend/app/models.py` - Added deleted_at field
- `backend/app/database.py` - Added migration for deleted_at
- `backend/app/main.py` - Modified delete, added restore, updated queries
- `frontend/src/lib/api.js` - Added restorePaper method
- `frontend/src/components/Toast.jsx` - Enhanced with action buttons
- `frontend/src/components/PaperList.jsx` - Updated delete with undo
- `frontend/src/components/CollectionDetail.jsx` - Complete rewrite
- `frontend/src/App.css` - Added extensive styling for dossier UI

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
