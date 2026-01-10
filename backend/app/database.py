"""
Database connection and session management
"""
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool
from .config import get_settings
from .models import Base

logger = logging.getLogger(__name__)
settings = get_settings()

# Create async engine with connection timeout
# command_timeout: max time for a query (in seconds)
# timeout: connection timeout (in seconds)
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    poolclass=NullPool,  # Better for serverless/Render
    connect_args={
        "command_timeout": 120,  # 120 second query timeout (increased for remote DB latency)
        "timeout": 30,  # 30 second connection timeout
    },
)

# Session factory
async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """Initialize database tables"""
    logger.info("init_db: Starting database initialization...")
    try:
        logger.info("init_db: Connecting to database...")
        async with engine.begin() as conn:
            logger.info("init_db: Connected! Creating tables...")
            await conn.run_sync(Base.metadata.create_all)
            logger.info("init_db: Tables created successfully")
    except Exception as e:
        logger.error(f"init_db: Database connection failed: {e}")
        raise

    # Run migrations for new columns
    logger.info("init_db: Running migrations...")
    await run_migrations()
    logger.info("init_db: Database initialization complete!")


async def run_migrations():
    """Add any missing columns to existing tables.

    Each migration runs in its own transaction to avoid cascading failures
    when one migration fails (e.g., column already exists).
    """
    from sqlalchemy import text

    migrations = [
        # Add candidates column to papers table (for reconciliation feature)
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS candidates TEXT",
        # Add is_supplementary column to editions table (for fetch more feature)
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS is_supplementary BOOLEAN DEFAULT FALSE",
        # Add params column to jobs table (for job-specific parameters)
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS params TEXT",
        # Add collection_id to papers (for collection feature)
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS collection_id INTEGER REFERENCES collections(id) ON DELETE SET NULL",
        "CREATE INDEX IF NOT EXISTS ix_papers_collection ON papers(collection_id)",
        # Citation auto-updater feature: harvest tracking on editions
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS last_harvested_at TIMESTAMP NULL",
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS last_harvest_year INTEGER NULL",
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS harvested_citation_count INTEGER DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS ix_editions_last_harvested ON editions(last_harvested_at)",
        # Citation auto-updater feature: aggregate tracking on papers
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS any_edition_harvested_at TIMESTAMP NULL",
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS total_harvested_citations INTEGER DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS ix_papers_any_harvested ON papers(any_edition_harvested_at)",
        # Edition management feature: exclude editions and finalize view
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS excluded BOOLEAN DEFAULT FALSE",
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS editions_finalized BOOLEAN DEFAULT FALSE",
        # Year-by-year harvest resume state for proper resume without re-fetching
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS harvest_resume_state TEXT NULL",
        # Pause auto-resume for specific papers
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS harvest_paused BOOLEAN DEFAULT FALSE",
        # Dossiers feature: papers belong to dossiers, dossiers belong to collections
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS dossier_id INTEGER REFERENCES dossiers(id) ON DELETE SET NULL",
        "CREATE INDEX IF NOT EXISTS ix_papers_dossier ON papers(dossier_id)",
        # Soft delete feature: papers can be soft deleted and restored
        "ALTER TABLE papers ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP NULL",
        "CREATE INDEX IF NOT EXISTS ix_papers_deleted ON papers(deleted_at)",
        # Stall detection: track consecutive zero-progress jobs to stop infinite auto-resume loops
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS harvest_stall_count INTEGER DEFAULT 0",
        # Performance: index on editions.paper_id for N+1 query fixes
        "CREATE INDEX IF NOT EXISTS ix_editions_paper ON editions(paper_id)",
        # CRITICAL: Unique constraint for ON CONFLICT (paper_id, scholar_id) DO NOTHING in citation upserts
        # Must be NON-partial index (no WHERE clause) to match ON CONFLICT clause exactly
        # Drop any existing indexes first (both old non-unique and old partial unique)
        "DROP INDEX IF EXISTS ix_citations_paper_scholar",
        "DROP INDEX IF EXISTS ix_citations_paper_scholar_unique",
        "CREATE UNIQUE INDEX ix_citations_paper_scholar_unique ON citations(paper_id, scholar_id)",
        # External API feature: webhook callback support for jobs
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS callback_url TEXT NULL",
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS callback_secret VARCHAR(256) NULL",
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS callback_sent_at TIMESTAMP NULL",
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS callback_error TEXT NULL",
        # Duplicate tracking: count how many times we encounter the same paper in GS
        # This helps reconcile our count vs GS count (GS tolerates duplicates, we don't)
        # SUM(encounter_count) = GS-equivalent count, COUNT(*) = our deduplicated count
        "ALTER TABLE citations ADD COLUMN IF NOT EXISTS encounter_count INTEGER DEFAULT 1",
        # Harvest completion tracking - when we've verified we can't get more citations
        # This stops auto-resume even if there's a gap (the gap is GS's fault, not ours)
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS harvest_complete BOOLEAN DEFAULT FALSE",
        "ALTER TABLE editions ADD COLUMN IF NOT EXISTS harvest_complete_reason VARCHAR(50) NULL",
        # API call logging for activity statistics (Oxylabs calls, pages fetched, citations saved)
        """CREATE TABLE IF NOT EXISTS api_call_logs (
            id SERIAL PRIMARY KEY,
            call_type VARCHAR(30) NOT NULL,
            job_id INTEGER,
            edition_id INTEGER,
            count INTEGER DEFAULT 1,
            success BOOLEAN DEFAULT TRUE,
            extra_info TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS ix_api_call_logs_call_type ON api_call_logs(call_type)",
        "CREATE INDEX IF NOT EXISTS ix_api_call_logs_created_at ON api_call_logs(created_at)",
        "CREATE INDEX IF NOT EXISTS ix_api_call_logs_type_created ON api_call_logs(call_type, created_at)",
        # Health monitor logs: track LLM-powered autonomous diagnoses and actions
        """CREATE TABLE IF NOT EXISTS health_monitor_logs (
            id SERIAL PRIMARY KEY,
            trigger_reason VARCHAR(100) NOT NULL,
            active_jobs_count INTEGER DEFAULT 0,
            citations_15min INTEGER DEFAULT 0,
            diagnostic_data TEXT,
            llm_model VARCHAR(100),
            llm_diagnosis TEXT,
            llm_root_cause VARCHAR(50),
            llm_confidence VARCHAR(20),
            llm_raw_response TEXT,
            action_type VARCHAR(50),
            action_params TEXT,
            action_executed BOOLEAN DEFAULT FALSE,
            action_result TEXT,
            action_error TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            llm_call_duration_ms INTEGER,
            action_duration_ms INTEGER
        )""",
        "CREATE INDEX IF NOT EXISTS ix_health_monitor_logs_created_at ON health_monitor_logs(created_at)",
        "CREATE INDEX IF NOT EXISTS ix_health_monitor_logs_action_type ON health_monitor_logs(action_type)",
        # Thinker Bibliographies feature: tables created by create_all, indexes here for safety
        "CREATE INDEX IF NOT EXISTS ix_thinkers_status ON thinkers(status)",
        "CREATE INDEX IF NOT EXISTS ix_thinker_works_thinker ON thinker_works(thinker_id)",
        "CREATE INDEX IF NOT EXISTS ix_thinker_works_scholar_id ON thinker_works(scholar_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_thinker_works_thinker_scholar ON thinker_works(thinker_id, scholar_id)",
        "CREATE INDEX IF NOT EXISTS ix_thinker_harvest_runs_thinker ON thinker_harvest_runs(thinker_id)",
        "CREATE INDEX IF NOT EXISTS ix_thinker_harvest_runs_status ON thinker_harvest_runs(status)",
        "CREATE INDEX IF NOT EXISTS ix_thinker_llm_calls_thinker ON thinker_llm_calls(thinker_id)",
        "CREATE INDEX IF NOT EXISTS ix_thinker_llm_calls_workflow ON thinker_llm_calls(workflow)",
        # Make thinker_id nullable in thinker_llm_calls to allow disambiguation before thinker exists
        "ALTER TABLE thinker_llm_calls ALTER COLUMN thinker_id DROP NOT NULL",
        # Scholar Author Profiles cache table
        """CREATE TABLE IF NOT EXISTS scholar_author_profiles (
            id SERIAL PRIMARY KEY,
            scholar_user_id VARCHAR(50) NOT NULL UNIQUE,
            profile_url TEXT NOT NULL,
            full_name VARCHAR(255),
            affiliation VARCHAR(500),
            homepage_url TEXT,
            topics TEXT,
            fetched_at TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW()
        )""",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_scholar_author_profiles_user_id ON scholar_author_profiles(scholar_user_id)",
        # Author profiles in citations (JSON array of author profile data)
        "ALTER TABLE citations ADD COLUMN IF NOT EXISTS author_profiles TEXT",
        # Publications cache for scholar author profiles
        "ALTER TABLE scholar_author_profiles ADD COLUMN IF NOT EXISTS publications TEXT",
        "ALTER TABLE scholar_author_profiles ADD COLUMN IF NOT EXISTS publications_count INTEGER DEFAULT 0",
        # Google Scholar profile URL support for thinkers
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS scholar_profile_url TEXT NULL",
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS scholar_user_id VARCHAR(50) NULL",
        # Thinker harvest batch tracking for profile pre-fetching
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS harvest_batch_id VARCHAR(36) NULL",
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS harvest_batch_jobs_total INTEGER DEFAULT 0",
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS harvest_batch_jobs_completed INTEGER DEFAULT 0",
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS harvest_batch_jobs_failed INTEGER DEFAULT 0",
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS profiles_prefetch_status VARCHAR(20) NULL",
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS profiles_prefetch_count INTEGER DEFAULT 0",
        "ALTER TABLE thinkers ADD COLUMN IF NOT EXISTS profiles_prefetched_at TIMESTAMP NULL",
        # Add cluster_id to thinker_works for citation harvesting (extracted from "Cited by" link)
        "ALTER TABLE thinker_works ADD COLUMN IF NOT EXISTS cluster_id VARCHAR(50) NULL",
        "CREATE INDEX IF NOT EXISTS idx_thinker_works_cluster_id ON thinker_works(cluster_id)",
        # Author-letter partitioning for harvest_targets (replaces year-by-year strategy)
        "ALTER TABLE harvest_targets ADD COLUMN IF NOT EXISTS letter VARCHAR(5) NULL",
        "CREATE INDEX IF NOT EXISTS idx_harvest_targets_letter ON harvest_targets(edition_id, letter)",
        # Author-letter partitioning for partition_runs (year is now nullable)
        "ALTER TABLE partition_runs ALTER COLUMN year DROP NOT NULL",
        "ALTER TABLE partition_runs ADD COLUMN IF NOT EXISTS letter VARCHAR(5) NULL",
        "CREATE INDEX IF NOT EXISTS idx_partition_runs_letter ON partition_runs(edition_id, letter)",
        # Expand letter columns to VARCHAR(20) for longer partition keys like 'lang_zh-CN', 'a_excl'
        "ALTER TABLE harvest_targets ALTER COLUMN letter TYPE VARCHAR(20)",
        "ALTER TABLE partition_runs ALTER COLUMN letter TYPE VARCHAR(20)",
        # CRITICAL: Prevent duplicate active jobs for same paper + job_type
        # This partial unique index enforces that only ONE pending/running job can exist per paper+type
        # Solves race conditions where multiple requests create duplicate jobs simultaneously
        """CREATE UNIQUE INDEX IF NOT EXISTS ix_jobs_active_paper_type
           ON jobs(paper_id, job_type)
           WHERE status IN ('pending', 'running')""",
        # Universal query logging for ALL harvesting operations (standard and overflow)
        # Provides full traceability for debugging and auditing
        """CREATE TABLE IF NOT EXISTS harvest_queries (
            id SERIAL PRIMARY KEY,
            edition_id INTEGER REFERENCES editions(id) ON DELETE CASCADE,
            job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
            query_string TEXT NOT NULL,
            partition_type VARCHAR(20),
            partition_value VARCHAR(50),
            page_number INTEGER DEFAULT 0,
            results_count INTEGER,
            success BOOLEAN DEFAULT TRUE,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS ix_harvest_queries_edition ON harvest_queries(edition_id)",
        "CREATE INDEX IF NOT EXISTS ix_harvest_queries_job ON harvest_queries(job_id)",
        "CREATE INDEX IF NOT EXISTS ix_harvest_queries_created ON harvest_queries(created_at)",
        # ============== EXHAUSTIVE EDITION ANALYSIS TABLES ==============
        # Work table - abstract intellectual works (books, essays, etc.)
        """CREATE TABLE IF NOT EXISTS works (
            id SERIAL PRIMARY KEY,
            thinker_name VARCHAR(255) NOT NULL,
            canonical_title VARCHAR(500) NOT NULL,
            original_language VARCHAR(50),
            original_title VARCHAR(500),
            original_year INTEGER,
            work_type VARCHAR(50),
            importance VARCHAR(20),
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS ix_works_thinker ON works(thinker_name)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_works_thinker_title ON works(thinker_name, canonical_title)",
        # WorkEdition table - links Work to Paper/Edition
        """CREATE TABLE IF NOT EXISTS work_editions (
            id SERIAL PRIMARY KEY,
            work_id INTEGER REFERENCES works(id) ON DELETE CASCADE,
            paper_id INTEGER REFERENCES papers(id) ON DELETE SET NULL,
            edition_id INTEGER REFERENCES editions(id) ON DELETE SET NULL,
            language VARCHAR(50) NOT NULL,
            edition_type VARCHAR(50),
            year INTEGER,
            translator VARCHAR(255),
            publisher VARCHAR(255),
            verified BOOLEAN DEFAULT FALSE,
            auto_linked BOOLEAN DEFAULT TRUE,
            confidence FLOAT,
            link_reason TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS ix_work_editions_work ON work_editions(work_id)",
        "CREATE INDEX IF NOT EXISTS ix_work_editions_paper ON work_editions(paper_id)",
        "CREATE INDEX IF NOT EXISTS ix_work_editions_edition ON work_editions(edition_id)",
        "CREATE INDEX IF NOT EXISTS ix_work_editions_language ON work_editions(work_id, language)",
        # Partial unique indexes for edition and paper (PostgreSQL syntax)
        """CREATE UNIQUE INDEX IF NOT EXISTS ix_work_editions_edition_unique
           ON work_editions(edition_id) WHERE edition_id IS NOT NULL""",
        """CREATE UNIQUE INDEX IF NOT EXISTS ix_work_editions_paper_unique
           ON work_editions(paper_id) WHERE paper_id IS NOT NULL""",
        # MissingEdition table - gaps identified in edition coverage
        """CREATE TABLE IF NOT EXISTS missing_editions (
            id SERIAL PRIMARY KEY,
            work_id INTEGER REFERENCES works(id) ON DELETE CASCADE,
            language VARCHAR(50) NOT NULL,
            expected_title VARCHAR(500),
            expected_year INTEGER,
            expected_translator VARCHAR(255),
            expected_publisher VARCHAR(255),
            source VARCHAR(100),
            source_url TEXT,
            source_details TEXT,
            priority VARCHAR(20) DEFAULT 'medium',
            status VARCHAR(20) DEFAULT 'pending',
            job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
            dismissed_reason TEXT,
            found_edition_id INTEGER REFERENCES editions(id) ON DELETE SET NULL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            resolved_at TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS ix_missing_editions_work ON missing_editions(work_id)",
        "CREATE INDEX IF NOT EXISTS ix_missing_editions_status ON missing_editions(status)",
        "CREATE INDEX IF NOT EXISTS ix_missing_editions_priority ON missing_editions(priority)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_missing_editions_work_lang ON missing_editions(work_id, language)",
        # EditionAnalysisRun table - audit trail for analysis runs
        """CREATE TABLE IF NOT EXISTS edition_analysis_runs (
            id SERIAL PRIMARY KEY,
            dossier_id INTEGER REFERENCES dossiers(id) ON DELETE CASCADE,
            thinker_name VARCHAR(255) NOT NULL,
            status VARCHAR(30) DEFAULT 'pending',
            phase VARCHAR(50),
            phase_progress FLOAT DEFAULT 0.0,
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
            results_summary TEXT,
            error TEXT,
            error_phase VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW(),
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS ix_edition_analysis_runs_dossier ON edition_analysis_runs(dossier_id)",
        "CREATE INDEX IF NOT EXISTS ix_edition_analysis_runs_status ON edition_analysis_runs(status)",
        # EditionAnalysisLLMCall table - detailed LLM call audit
        """CREATE TABLE IF NOT EXISTS edition_analysis_llm_calls (
            id SERIAL PRIMARY KEY,
            run_id INTEGER REFERENCES edition_analysis_runs(id) ON DELETE CASCADE,
            phase VARCHAR(50) NOT NULL,
            call_number INTEGER DEFAULT 1,
            purpose VARCHAR(200),
            model VARCHAR(100) NOT NULL,
            prompt TEXT NOT NULL,
            context_json TEXT,
            raw_response TEXT,
            parsed_result TEXT,
            thinking_text TEXT,
            thinking_tokens INTEGER,
            web_search_used BOOLEAN DEFAULT FALSE,
            web_search_queries TEXT,
            web_sources_cited TEXT,
            input_tokens INTEGER,
            output_tokens INTEGER,
            latency_ms INTEGER,
            status VARCHAR(20) DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS ix_edition_analysis_llm_calls_run ON edition_analysis_llm_calls(run_id)",
        "CREATE INDEX IF NOT EXISTS ix_edition_analysis_llm_calls_phase ON edition_analysis_llm_calls(phase)",
    ]

    # Run each migration in its own transaction to avoid cascading failures
    # Set short lock timeout to fail fast if table is locked by another process
    for i, migration in enumerate(migrations, 1):
        try:
            logger.info(f"Migration {i}/{len(migrations)}: {migration[:50]}...")
            async with engine.begin() as conn:
                # Set 2 second lock timeout to fail fast
                await conn.execute(text("SET lock_timeout = '2s'"))
                await conn.execute(text(migration))
            logger.info(f"Migration {i} completed")
        except Exception as e:
            # Column might already exist, lock timeout, or other non-fatal error
            logger.info(f"Migration {i} skipped: {type(e).__name__}: {e}")


async def get_db() -> AsyncSession:
    """Dependency for getting database sessions"""
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
