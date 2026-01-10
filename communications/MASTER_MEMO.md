# Master Implementation Memo: Exhaustive Edition Analysis

> Project: The Referee - Edition Analysis Feature
> Created: 2026-01-10
> Strategizer Session: Main session

## Project Overview

Build an **Exhaustive Edition Analysis** system for thinker-based dossiers that acts like an experienced multilingual bibliography expert. The system will:

1. **Analyze what a dossier has** - Inventory all papers/editions for a thinker
2. **Determine what SHOULD exist** - Using bibliographic knowledge + web search + Google Scholar
3. **Link editions** - Connect originals to translations (Spuren ↔ Traces)
4. **Identify gaps** - Missing major works, missing translations
5. **Generate scraper jobs** - Actionable tasks to fill the gaps

**Example scenario**: Bloch dossier has German "Thomas Münzer" and Spanish "Thomas Müntzer, teólogo de la revolución" — but where's the English translation? The system should detect this gap and create a job to find it.

## Architecture Decisions

### AI Model Selection
- **Primary Analysis**: Claude Opus 4.5 (`claude-opus-4-5-20251101`) with 32k extended thinking tokens
- **Web Search**: Claude Opus 4.5 built-in web search for bibliographic research
- **Lighter Tasks**: Claude Sonnet 4.5 for simpler classification tasks

### Key Design Principles

1. **Work-Centric Model**: A "Work" is the abstract intellectual work (e.g., "The Spirit of Utopia"). Multiple Papers/Editions can represent the same Work in different languages/editions.

2. **Bibliographic Authority**: Use Claude's knowledge + web search + Google Scholar verification to establish authoritative bibliographies for major thinkers.

3. **Incremental Persistence**: Save analysis results immediately to database, don't accumulate in memory.

4. **Job-Oriented Output**: Final deliverable is a set of scraper jobs, not just a report.

### Database Schema Extensions Required

```sql
-- New table: Work (abstract intellectual work)
CREATE TABLE work (
    id SERIAL PRIMARY KEY,
    thinker_name VARCHAR(255) NOT NULL,           -- e.g., "Ernst Bloch"
    canonical_title VARCHAR(500) NOT NULL,        -- e.g., "The Spirit of Utopia"
    original_language VARCHAR(50),                -- e.g., "german"
    original_title VARCHAR(500),                  -- e.g., "Geist der Utopie"
    original_year INTEGER,
    work_type VARCHAR(50),                        -- book, article, essay, lecture, etc.
    importance VARCHAR(20),                       -- major, minor, peripheral
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(thinker_name, canonical_title)
);

-- New table: WorkEdition (links Work to Paper/Edition)
CREATE TABLE work_edition (
    id SERIAL PRIMARY KEY,
    work_id INTEGER REFERENCES work(id) ON DELETE CASCADE,
    paper_id INTEGER REFERENCES paper(id) ON DELETE SET NULL,
    edition_id INTEGER REFERENCES edition(id) ON DELETE SET NULL,
    language VARCHAR(50) NOT NULL,
    edition_type VARCHAR(50),                     -- original, translation, abridged, anthology_excerpt
    year INTEGER,
    verified BOOLEAN DEFAULT FALSE,               -- manually verified link
    auto_linked BOOLEAN DEFAULT TRUE,             -- linked by LLM
    confidence FLOAT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(work_id, edition_id)
);

-- New table: MissingEdition (gaps identified)
CREATE TABLE missing_edition (
    id SERIAL PRIMARY KEY,
    work_id INTEGER REFERENCES work(id) ON DELETE CASCADE,
    language VARCHAR(50) NOT NULL,                -- missing language
    expected_title VARCHAR(500),                  -- expected title in that language
    expected_year INTEGER,
    source VARCHAR(100),                          -- how we know it exists: "llm_knowledge", "web_search", "google_scholar"
    source_url TEXT,                              -- verification URL if found
    priority VARCHAR(20),                         -- high, medium, low
    status VARCHAR(20) DEFAULT 'pending',         -- pending, job_created, found, dismissed
    job_id INTEGER REFERENCES job(id),            -- scraper job if created
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- New table: EditionAnalysisRun (audit trail)
CREATE TABLE edition_analysis_run (
    id SERIAL PRIMARY KEY,
    dossier_id INTEGER REFERENCES dossier(id) ON DELETE CASCADE,
    thinker_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',         -- pending, analyzing, web_searching, verifying, completed, failed
    phase VARCHAR(50),                            -- current phase
    papers_analyzed INTEGER DEFAULT 0,
    editions_analyzed INTEGER DEFAULT 0,
    works_identified INTEGER DEFAULT 0,
    links_created INTEGER DEFAULT 0,
    gaps_found INTEGER DEFAULT 0,
    jobs_created INTEGER DEFAULT 0,
    llm_calls_count INTEGER DEFAULT 0,
    web_searches_count INTEGER DEFAULT 0,
    total_input_tokens INTEGER DEFAULT 0,
    total_output_tokens INTEGER DEFAULT 0,
    thinking_tokens INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- New table: EditionAnalysisLLMCall (detailed audit)
CREATE TABLE edition_analysis_llm_call (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES edition_analysis_run(id) ON DELETE CASCADE,
    phase VARCHAR(50),                            -- inventory, bibliographic_research, gap_analysis, verification
    model VARCHAR(100),
    prompt TEXT,
    context_json JSONB,
    raw_response TEXT,
    parsed_result JSONB,
    thinking_text TEXT,
    thinking_tokens INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    latency_ms INTEGER,
    web_search_used BOOLEAN DEFAULT FALSE,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Phase Overview

| Phase | Description | Dependencies | Parallelizable |
|-------|-------------|--------------|----------------|
| 1     | Schema & Models | None | Yes |
| 2     | Inventory Service | Phase 1 | Yes |
| 3     | Bibliographic Research Agent | Phase 1 | Yes |
| 4     | Edition Linking Service | Phase 1, 2 | No (needs 2) |
| 5     | Gap Analysis & Job Generation | Phases 2, 3, 4 | No (needs all) |
| 6     | API Routes & UI | Phase 5 | No (needs 5) |
| 7     | Integration & Testing | All | No (final) |

**Parallel Execution**: Phases 1, 2, 3 can run in parallel. Phases 4-7 are sequential.

---

## Phase 1: Schema Extensions & Models

### Scope
- Create new SQLAlchemy models: `Work`, `WorkEdition`, `MissingEdition`, `EditionAnalysisRun`, `EditionAnalysisLLMCall`
- Add Alembic migration
- Add relationships to existing models

### Files to Create/Modify
- `backend/models.py` - Add new model classes
- `backend/alembic/versions/xxx_edition_analysis_schema.py` - Migration
- Add indexes for performance

### Interface Contract
- Exports: SQLAlchemy model classes, migration script
- Expects: Existing Paper, Edition, Dossier models

### Success Criteria
- [ ] All new tables created via migration
- [ ] Models have proper relationships defined
- [ ] Can create/query Work, WorkEdition, MissingEdition records
- [ ] Indexes on frequently queried columns

---

## Phase 2: Dossier Inventory Service

### Scope
- Build service to analyze all papers/editions in a dossier
- Extract: titles, languages, years, authors
- Group by apparent work (fuzzy title matching)
- Detect obvious original/translation pairs

### Files to Create/Modify
- `backend/services/inventory_service.py` - New service
- No LLM calls in this phase - pure data extraction

### Interface Contract
```python
class InventoryService:
    async def analyze_dossier(self, dossier_id: int) -> DossierInventory:
        """
        Returns:
        {
            "thinker_name": str,                    # Inferred from dossier
            "papers": [
                {
                    "paper_id": int,
                    "title": str,
                    "authors": list[str],
                    "editions": [
                        {"edition_id": int, "title": str, "language": str, "year": int}
                    ]
                }
            ],
            "title_clusters": [                     # Fuzzy-grouped by title similarity
                {
                    "canonical_title": str,
                    "papers": [paper_ids],
                    "languages": [str],
                    "years": [int]
                }
            ]
        }
        """
```

### Success Criteria
- [ ] Can extract all papers/editions from a dossier
- [ ] Groups by title similarity (Levenshtein/fuzzy)
- [ ] Detects language from title
- [ ] Returns structured inventory

---

## Phase 3: Bibliographic Research Agent

### Scope
- Build Claude Opus 4.5 agent with web search capability
- For a given thinker, establish their authoritative bibliography
- Uses: Claude's knowledge, web search, Google Scholar allintitle: queries
- Returns: List of major works with expected translations

### Files to Create/Modify
- `backend/services/bibliographic_agent.py` - New agent
- Uses Anthropic SDK with streaming, extended thinking, web search

### Interface Contract
```python
class BibliographicAgent:
    async def research_thinker_bibliography(
        self,
        thinker_name: str,
        known_works: list[str],  # What we already have
        run_id: int              # For audit logging
    ) -> ThinkerBibliography:
        """
        Returns:
        {
            "thinker": {
                "canonical_name": str,
                "birth_death": str,
                "primary_language": str,
                "domains": [str]
            },
            "major_works": [
                {
                    "canonical_title": str,
                    "original_language": str,
                    "original_title": str,
                    "original_year": int,
                    "work_type": str,              # book, essay, etc.
                    "importance": str,             # major, minor
                    "known_translations": [
                        {
                            "language": str,
                            "title": str,
                            "year": int,
                            "translator": str,
                            "source": str          # "llm_knowledge", "web_search", "scholar"
                        }
                    ],
                    "scholarly_significance": str  # Why this work matters
                }
            ],
            "verification_sources": [str],         # URLs consulted
            "confidence": float
        }
        """

    async def verify_edition_exists(
        self,
        title: str,
        author: str,
        language: str
    ) -> VerificationResult:
        """
        Uses Google Scholar allintitle: to verify edition exists.
        Returns scholar_id if found.
        """
```

### Key Implementation Details

**Claude API Call Structure**:
```python
client = anthropic.Anthropic()

response = client.messages.stream(
    model="claude-opus-4-5-20251101",
    max_tokens=16000,
    thinking={
        "type": "enabled",
        "budget_tokens": 32000
    },
    # Web search is built into Claude - just ask it to search
    messages=[
        {
            "role": "user",
            "content": f"""You are an expert multilingual bibliographer specializing in {domain}.

Research the complete bibliography of {thinker_name}.

Use web search to verify your knowledge. Search for:
1. {thinker_name} complete bibliography
2. {thinker_name} works translations
3. Academic sources listing their publications

For each major work, identify:
- Original language and title
- Known translations with years
- Publisher information where available

{context_about_what_we_have}

Return JSON with the structure shown..."""
        }
    ]
)
```

**Google Scholar Verification**:
```python
async def verify_on_scholar(self, title: str, author: str) -> Optional[str]:
    """
    Search Google Scholar with: allintitle:{title} author:"{author}"
    Parse results to find scholar_id
    """
    query = f'allintitle:{title} author:"{author}"'
    # Use existing scraper infrastructure
```

### Success Criteria
- [ ] Can query Claude Opus 4.5 with extended thinking
- [ ] Web search integration working
- [ ] Returns structured bibliography for test thinker (Bloch)
- [ ] Verification queries to Google Scholar work
- [ ] All calls logged to EditionAnalysisLLMCall

---

## Phase 4: Edition Linking Service

### Scope
- Take inventory (Phase 2) + bibliography (Phase 3)
- Match papers/editions to Works
- Create WorkEdition links
- Handle fuzzy matching for variant titles

### Files to Create/Modify
- `backend/services/edition_linking_service.py` - New service

### Interface Contract
```python
class EditionLinkingService:
    async def link_editions_to_works(
        self,
        inventory: DossierInventory,
        bibliography: ThinkerBibliography,
        run_id: int
    ) -> LinkingResult:
        """
        For each paper/edition in inventory:
        1. Find matching Work from bibliography
        2. Create Work if new
        3. Create WorkEdition link
        4. Flag uncertain matches for review

        Returns:
        {
            "works_created": int,
            "links_created": int,
            "uncertain_matches": [
                {"paper_id": int, "possible_works": [str], "confidence": float}
            ]
        }
        """
```

### Success Criteria
- [ ] Links obvious matches (exact title match)
- [ ] Handles language variants (Spuren → Traces)
- [ ] Creates new Works for unknown papers
- [ ] Flags uncertain matches

---

## Phase 5: Gap Analysis & Job Generation

### Scope
- Compare linked Works against bibliography expectations
- Identify missing translations
- Identify missing major works
- Generate scraper jobs to fill gaps

### Files to Create/Modify
- `backend/services/gap_analysis_service.py` - New service

### Interface Contract
```python
class GapAnalysisService:
    async def analyze_gaps(
        self,
        dossier_id: int,
        bibliography: ThinkerBibliography,
        run_id: int
    ) -> GapAnalysisResult:
        """
        Returns:
        {
            "missing_translations": [
                {
                    "work_canonical_title": str,
                    "original_language": str,
                    "missing_language": str,
                    "expected_title": str,
                    "expected_year": int,
                    "priority": str,
                    "source": str
                }
            ],
            "missing_works": [
                {
                    "canonical_title": str,
                    "importance": str,
                    "reason_missing": str         # Never scraped? Not on Scholar?
                }
            ],
            "orphan_editions": [                   # Editions we have but can't place
                {"edition_id": int, "title": str}
            ]
        }
        """

    async def generate_scraper_jobs(
        self,
        gaps: GapAnalysisResult,
        dossier_id: int
    ) -> list[Job]:
        """
        Creates jobs of type 'discover_editions' for each gap.
        Priority based on importance of work.
        """
```

### Success Criteria
- [ ] Identifies missing translations
- [ ] Identifies missing major works
- [ ] Creates properly formatted scraper jobs
- [ ] Jobs have appropriate priority ordering

---

## Phase 6: API Routes & UI Components

### Scope
- REST endpoints for triggering analysis
- Endpoints for reviewing/approving results
- Frontend components for displaying analysis

### Files to Create/Modify
- `backend/main.py` - Add routes
- `frontend/src/components/EditionAnalysis/` - New components
- `frontend/src/pages/` - Analysis page if needed

### API Endpoints
```
POST /api/dossiers/{dossier_id}/analyze-editions
    - Triggers full analysis run
    - Returns run_id

GET /api/edition-analysis-runs/{run_id}
    - Returns run status and results

GET /api/dossiers/{dossier_id}/edition-analysis
    - Returns latest analysis for dossier
    - Includes works, links, gaps

POST /api/edition-analysis/missing/{missing_id}/create-job
    - Creates scraper job for specific gap

POST /api/edition-analysis/missing/{missing_id}/dismiss
    - Marks gap as dismissed (not actually missing)

GET /api/works?thinker={name}
    - Returns all Works for a thinker

GET /api/works/{work_id}/editions
    - Returns all linked editions for a Work
```

### Success Criteria
- [ ] Can trigger analysis from UI
- [ ] Can view analysis results
- [ ] Can create jobs for gaps
- [ ] Can dismiss false positives

---

## Phase 7: Integration & Testing

### Scope
- End-to-end testing with real dossier (Bloch)
- Performance optimization
- Error handling
- Documentation

### Files to Create/Modify
- `backend/tests/test_edition_analysis.py`
- `docs/FEATURES.md` - Update
- `docs/CHANGELOG.md` - Update

### Test Scenarios
1. **Bloch Dossier Analysis**
   - Should detect Spuren ↔ Traces link
   - Should identify missing English Thomas Münzer
   - Should identify major works like Das Prinzip Hoffnung

2. **Edge Cases**
   - Thinker with no translations
   - Work with 10+ translations
   - Anthology excerpts vs full works

### Success Criteria
- [ ] Full analysis runs on Bloch dossier
- [ ] Correct identification of known gaps
- [ ] Jobs generated are valid and runnable
- [ ] UI displays results correctly

---

## Integration Points

### Between Phases
- Phase 2 → Phase 4: `DossierInventory` data structure
- Phase 3 → Phase 4: `ThinkerBibliography` data structure
- Phase 4 → Phase 5: Work and WorkEdition records in DB
- Phase 5 → Phase 6: Jobs in database, analysis results

### With Existing Systems
- Uses existing `Paper`, `Edition`, `Dossier` models
- Uses existing `Job` model and worker
- Uses existing scraper infrastructure
- Uses existing Anthropic client patterns

## Out of Scope

- Automatic job execution (jobs are created, not run)
- Modifying existing edition discovery logic
- Handling non-thinker dossiers (topic-based collections)
- Real-time analysis (this is a batch operation)
- UI for managing Works directly (only via analysis results)

## Example Output: Bloch Dossier Analysis

```json
{
  "run_id": 1,
  "dossier": "Bloch",
  "thinker": "Ernst Bloch",

  "works_identified": [
    {
      "canonical_title": "The Spirit of Utopia",
      "original": {"language": "german", "title": "Geist der Utopie", "year": 1918},
      "editions_found": [
        {"paper_id": 45, "language": "german", "title": "Geist der Utopie"},
        {"paper_id": 67, "language": "english", "title": "The Spirit of Utopia"}
      ],
      "missing_translations": []
    },
    {
      "canonical_title": "Traces",
      "original": {"language": "german", "title": "Spuren", "year": 1930},
      "editions_found": [
        {"paper_id": 23, "language": "german", "title": "Spuren"},
        {"paper_id": 89, "language": "english", "title": "Traces"}
      ],
      "missing_translations": [
        {"language": "french", "expected_title": "Traces", "priority": "medium"}
      ]
    },
    {
      "canonical_title": "Thomas Müntzer as Theologian of Revolution",
      "original": {"language": "german", "title": "Thomas Münzer als Theologe der Revolution", "year": 1921},
      "editions_found": [
        {"paper_id": 34, "language": "german", "title": "Thomas Münzer als Theologe..."},
        {"paper_id": 56, "language": "spanish", "title": "Thomas Müntzer, teólogo de la revolución"}
      ],
      "missing_translations": [
        {"language": "english", "expected_title": "Thomas Müntzer as Theologian of Revolution", "year": 1989, "priority": "high"}
      ]
    }
  ],

  "jobs_created": [
    {
      "job_type": "discover_editions",
      "params": {
        "search_query": "Thomas Müntzer Bloch English translation",
        "target_language": "english",
        "expected_title": "Thomas Müntzer as Theologian of Revolution"
      },
      "priority": 10
    }
  ]
}
```

## Technical Notes

### Claude Opus 4.5 API Usage

```python
import anthropic

client = anthropic.Anthropic()

# For streaming with extended thinking (REQUIRED for long operations)
with client.messages.stream(
    model="claude-opus-4-5-20251101",
    max_tokens=16000,
    thinking={
        "type": "enabled",
        "budget_tokens": 32000  # 32k thinking tokens
    },
    messages=[...]
) as stream:
    thinking_content = ""
    text_content = ""

    for event in stream:
        if event.type == "content_block_delta":
            if hasattr(event.delta, "thinking"):
                thinking_content += event.delta.thinking
            elif hasattr(event.delta, "text"):
                text_content += event.delta.text

    final = stream.get_final_message()
    usage = final.usage
```

### Web Search in Claude

Claude Opus 4.5 has built-in web search. To trigger it, include explicit search instructions:

```
Search the web for "{query}" and report what you find...
```

The model will automatically search and cite sources.

### Google Scholar Verification Pattern

```python
async def verify_on_scholar(title: str, author_surname: str) -> dict:
    """
    Uses allintitle: for precise matching.
    """
    query = f'allintitle:{title} author:"{author_surname}"'
    encoded = urllib.parse.quote(query)
    url = f"https://scholar.google.com/scholar?q={encoded}"

    # Use existing scraper with proper rate limiting
    results = await scraper.search(query)

    if results:
        return {
            "found": True,
            "scholar_id": results[0].scholar_id,
            "title": results[0].title,
            "citations": results[0].citations
        }
    return {"found": False}
```
