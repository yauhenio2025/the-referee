"""
Database connection and session management
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool
from .config import get_settings
from .models import Base

settings = get_settings()

# Create async engine
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    poolclass=NullPool,  # Better for serverless/Render
)

# Session factory
async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """Initialize database tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Run migrations for new columns
    await run_migrations()


async def run_migrations():
    """Add any missing columns to existing tables"""
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
    ]

    async with engine.begin() as conn:
        for migration in migrations:
            try:
                await conn.execute(text(migration))
            except Exception as e:
                # Column might already exist or other non-fatal error
                print(f"Migration note: {e}")


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
