"""
Database models for The Referee
"""
from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, Integer, Text, DateTime, Boolean, ForeignKey, JSON, Float, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Paper(Base):
    """A seed paper to analyze for citations"""
    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(primary_key=True)
    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), unique=True, index=True)
    cluster_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)  # JSON array as string
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    abstract: Mapped[Optional[str]] = mapped_column(Text)
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

    # Relationships
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

    job_type: Mapped[str] = mapped_column(String(50))  # resolve, discover_editions, extract_citations
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, running, completed, failed
    priority: Mapped[int] = mapped_column(Integer, default=0)

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
