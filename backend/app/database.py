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
        "command_timeout": 30,  # 30 second query timeout
        "timeout": 15,  # 15 second connection timeout
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
        # First drop the old non-unique index if it exists, then create unique version
        "DROP INDEX IF EXISTS ix_citations_paper_scholar",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_citations_paper_scholar_unique ON citations(paper_id, scholar_id)",
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
