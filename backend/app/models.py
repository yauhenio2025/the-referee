"""
Database models for The Referee
"""
from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, Integer, Text, DateTime, Boolean, ForeignKey, JSON, Float, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Collection(Base):
    """A collection of related papers for grouped analysis"""
    __tablename__ = "collections"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    color: Mapped[Optional[str]] = mapped_column(String(20))  # For UI display (hex color)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    papers: Mapped[List["Paper"]] = relationship(back_populates="collection")


class Paper(Base):
    """A seed paper to analyze for citations"""
    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(primary_key=True)
    collection_id: Mapped[Optional[int]] = mapped_column(ForeignKey("collections.id", ondelete="SET NULL"), index=True)
    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), unique=True, index=True)
    cluster_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)  # JSON array as string
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    abstract_source: Mapped[Optional[str]] = mapped_column(String(50))  # 'scholar_search', 'allintitle_scrape', 'manual'
    link: Mapped[Optional[str]] = mapped_column(Text)

    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    language: Mapped[Optional[str]] = mapped_column(String(50))

    # Status: pending, needs_reconciliation, resolved, error
    status: Mapped[str] = mapped_column(String(50), default="pending")
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Candidate papers for reconciliation (JSON array)
    candidates: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Aggregate harvest tracking (computed from editions, for quick staleness checks)
    any_edition_harvested_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, default=None)
    total_harvested_citations: Mapped[int] = mapped_column(Integer, default=0)

    # Edition management
    editions_finalized: Mapped[bool] = mapped_column(Boolean, default=False)  # User finalized edition selection

    # Relationships
    collection: Mapped[Optional["Collection"]] = relationship(back_populates="papers")
    editions: Mapped[List["Edition"]] = relationship(back_populates="paper", cascade="all, delete-orphan")
    citations: Mapped[List["Citation"]] = relationship(back_populates="paper", cascade="all, delete-orphan")
    jobs: Mapped[List["Job"]] = relationship(back_populates="paper", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_papers_title", "title"),  # Regular index for title lookups
    )


class Edition(Base):
    """An edition/translation of a paper found via edition discovery"""
    __tablename__ = "editions"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))

    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    cluster_id: Mapped[Optional[str]] = mapped_column(String(50))

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    link: Mapped[Optional[str]] = mapped_column(Text)

    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    language: Mapped[Optional[str]] = mapped_column(String(50))

    # Discovery metadata
    confidence: Mapped[str] = mapped_column(String(20), default="uncertain")  # high, uncertain
    auto_selected: Mapped[bool] = mapped_column(Boolean, default=False)
    found_by_query: Mapped[Optional[str]] = mapped_column(Text)

    # Selection status
    selected: Mapped[bool] = mapped_column(Boolean, default=False)  # User selected for citation extraction
    excluded: Mapped[bool] = mapped_column(Boolean, default=False)  # User excluded from view

    # Supplementary flag - True if added via "Fetch more" button
    is_supplementary: Mapped[bool] = mapped_column(Boolean, default=False)

    # Track which job added this edition (for NEW badge - null means not new)
    added_by_job_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)

    # Citation harvest tracking (for auto-updater feature)
    last_harvested_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, default=None)
    last_harvest_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    harvested_citation_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    paper: Mapped["Paper"] = relationship(back_populates="editions")


class Citation(Base):
    """A paper that cites a seed paper (or one of its editions)"""
    __tablename__ = "citations"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))
    edition_id: Mapped[Optional[int]] = mapped_column(ForeignKey("editions.id", ondelete="SET NULL"))

    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    link: Mapped[Optional[str]] = mapped_column(Text)

    citation_count: Mapped[int] = mapped_column(Integer, default=0)

    # For cross-citation analysis
    intersection_count: Mapped[int] = mapped_column(Integer, default=1)  # How many seeds this cites

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    paper: Mapped["Paper"] = relationship(back_populates="citations")

    __table_args__ = (
        Index("ix_citations_paper_scholar", "paper_id", "scholar_id"),
    )


class Job(Base):
    """Background processing job"""
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[Optional[int]] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))

    job_type: Mapped[str] = mapped_column(String(50))  # resolve, discover_editions, extract_citations, fetch_more_editions
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, running, completed, failed
    priority: Mapped[int] = mapped_column(Integer, default=0)

    # Job parameters (JSON) - e.g., {"language": "italian", "max_results": 50}
    params: Mapped[Optional[str]] = mapped_column(Text)

    # Progress
    progress: Mapped[float] = mapped_column(Float, default=0.0)
    progress_message: Mapped[Optional[str]] = mapped_column(Text)

    # Results
    result: Mapped[Optional[str]] = mapped_column(Text)  # JSON
    error: Mapped[Optional[str]] = mapped_column(Text)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    paper: Mapped[Optional["Paper"]] = relationship(back_populates="jobs")

    __table_args__ = (
        Index("ix_jobs_status_priority", "status", "priority"),
    )


class RawSearchResult(Base):
    """Raw search results before LLM processing - for debugging/auditing"""
    __tablename__ = "raw_search_results"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))
    job_id: Mapped[Optional[int]] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"))

    # Search context
    search_type: Mapped[str] = mapped_column(String(50))  # discover_editions, fetch_more
    target_language: Mapped[Optional[str]] = mapped_column(String(50))
    query: Mapped[str] = mapped_column(Text)

    # Raw results from Scholar (before LLM)
    raw_results: Mapped[str] = mapped_column(Text)  # JSON array
    result_count: Mapped[int] = mapped_column(Integer, default=0)

    # LLM classification results
    llm_classification: Mapped[Optional[str]] = mapped_column(Text)  # JSON with high/uncertain/rejected

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_raw_search_paper", "paper_id"),
        Index("ix_raw_search_job", "job_id"),
    )


class SearchCache(Base):
    """Cache for Google Scholar search results"""
    __tablename__ = "search_cache"

    id: Mapped[int] = mapped_column(primary_key=True)
    query_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    query: Mapped[str] = mapped_column(Text)

    results: Mapped[str] = mapped_column(Text)  # JSON array of papers
    result_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)

    __table_args__ = (
        Index("ix_cache_expires", "expires_at"),
    )
