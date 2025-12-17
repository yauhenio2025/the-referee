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
