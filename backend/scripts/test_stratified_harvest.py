#!/usr/bin/env python3
"""
REAL TEST of the STRATIFIED LANGUAGE HARVESTING solution.

This script ACTUALLY CALLS the real harvest_with_language_stratification function
and saves to the DB. No placeholders - we test the actual code including
exclusion term strategy for English when >= 1000.

Tests for scholar ID 15603705792201309427 (Jameson), Year 2014:
1. Harvest NON-ENGLISH papers per-language (~282)
2. Check ENGLISH count
3. If English < 1000: harvest directly
4. If English >= 1000: use exclusion term strategy on English subset

Usage:
    python scripts/test_stratified_harvest.py
    python scripts/test_stratified_harvest.py --paper-id 71 --year 2014
"""

import argparse
import asyncio
import os
import sys
import logging
from datetime import datetime
from typing import Set, Dict, Any

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env BEFORE any other imports
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Global log file handle
LOG_FILE = None

# Import the REAL stratified harvester
from app.services.overflow_harvester import (
    harvest_with_language_stratification,
    GOOGLE_SCHOLAR_LIMIT,
    TARGET_THRESHOLD,
    NON_ENGLISH_LANGUAGE_LIST,
    ENGLISH_ONLY,
)
from app.services.scholar_search import ScholarSearchService
from app.models import Citation, Edition, Paper


def log(msg: str, level: str = "INFO"):
    """Log with timestamp - writes to stdout AND log file"""
    global LOG_FILE
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] [{level:5}] {msg}"
    print(line, flush=True)
    if LOG_FILE:
        LOG_FILE.write(line + "\n")
        LOG_FILE.flush()


async def get_edition_for_paper(db: AsyncSession, paper_id: int) -> Dict[str, Any]:
    """Get edition details from database"""
    result = await db.execute(text("""
        SELECT
            e.id as edition_id,
            e.title as edition_title,
            e.scholar_id,
            e.language,
            p.id as paper_id,
            p.title as paper_title
        FROM editions e
        JOIN papers p ON e.paper_id = p.id
        WHERE p.id = :paper_id
        LIMIT 1
    """), {"paper_id": paper_id})
    row = result.fetchone()

    if row:
        return {
            "edition_id": row.edition_id,
            "edition_title": row.edition_title,
            "scholar_id": row.scholar_id,
            "language": row.language,
            "paper_id": row.paper_id,
            "paper_title": row.paper_title,
        }
    return None


async def get_existing_citations(db: AsyncSession, edition_id: int) -> Set[str]:
    """Get existing scholar IDs for an edition"""
    result = await db.execute(text("""
        SELECT scholar_id FROM citations
        WHERE edition_id = :edition_id AND scholar_id IS NOT NULL
    """), {"edition_id": edition_id})
    return {row.scholar_id for row in result.fetchall()}


async def count_citations_for_year(db: AsyncSession, edition_id: int, year: int) -> int:
    """Count existing citations for a specific year"""
    result = await db.execute(text("""
        SELECT COUNT(*) FROM citations
        WHERE edition_id = :edition_id AND year = :year
    """), {"edition_id": edition_id, "year": year})
    return result.scalar() or 0


async def main():
    global LOG_FILE

    parser = argparse.ArgumentParser(description='Test stratified language harvesting')
    parser.add_argument('--paper-id', type=int, default=71, help='Paper ID to test (default: 71 = Jameson)')
    parser.add_argument('--year', type=int, default=2014, help='Year to harvest (default: 2014)')
    parser.add_argument('--dry-run', action='store_true', help='Dry run - do not save to DB')
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Open log file
    log_filename = f"stratified_test_{timestamp}.log"
    LOG_FILE = open(log_filename, 'w')

    log("="*70)
    log("STRATIFIED LANGUAGE HARVEST TEST - REAL DB VERSION")
    log(f"Paper ID: {args.paper_id}")
    log(f"Year: {args.year}")
    log(f"Dry run: {args.dry_run}")
    log(f"Log file: {log_filename}")
    log("="*70)

    # Connect to database
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        log("ERROR: DATABASE_URL not set", "ERROR")
        return

    # Convert to async URL if needed
    if database_url.startswith('postgresql://'):
        database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)

    log(f"Connecting to database...")
    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as db:
        # Get edition details
        edition = await get_edition_for_paper(db, args.paper_id)
        if not edition:
            log(f"ERROR: No edition found for paper_id {args.paper_id}", "ERROR")
            return

        log(f"Edition: {edition['edition_title'][:50]}...")
        log(f"Scholar ID: {edition['scholar_id']}")
        log(f"Edition ID: {edition['edition_id']}")

        # Get existing citations
        existing_scholar_ids = await get_existing_citations(db, edition['edition_id'])
        existing_for_year = await count_citations_for_year(db, edition['edition_id'], args.year)
        log(f"Existing citations for edition: {len(existing_scholar_ids)} total, {existing_for_year} for year {args.year}")

        # Initialize scholar service
        scholar_service = ScholarSearchService()

        # Get total count for the year
        log(f"\nGetting total count for year {args.year}...")
        total_result = await scholar_service.get_cited_by(
            scholar_id=edition['scholar_id'],
            max_results=10,
            year_low=args.year,
            year_high=args.year,
        )
        total_count = total_result.get('totalResults', 0)
        log(f"Total papers for year {args.year}: {total_count}")

        if total_count < GOOGLE_SCHOLAR_LIMIT:
            log(f"Total ({total_count}) < 1000 - no need for stratified harvesting!")
            log("A simple harvest would suffice. Exiting.")
            return

        log(f"\nTotal ({total_count}) >= 1000 - STRATIFIED HARVESTING REQUIRED")
        log(f"Non-English languages to harvest: {len(NON_ENGLISH_LANGUAGE_LIST)}")
        log(f"Languages: {', '.join(NON_ENGLISH_LANGUAGE_LIST)}")

        # Counters for on_page_complete callback
        papers_saved = {"count": 0, "new": 0}

        async def on_page_complete(page_num: int, papers: list):
            """Callback to save papers as they're harvested.

            Uses ORM model like job_worker does - this is the correct pattern.
            """
            new_count = 0
            for paper in papers:
                scholar_id = paper.get('scholarId') or paper.get('id')
                if not scholar_id or scholar_id in existing_scholar_ids:
                    continue

                if not args.dry_run:
                    # Handle authors - can be string or list
                    authors = paper.get('authorsRaw') or paper.get('authors')
                    if isinstance(authors, list):
                        authors = ', '.join(authors)

                    # Create citation using ORM model
                    citation = Citation(
                        edition_id=edition['edition_id'],
                        paper_id=edition['paper_id'],
                        scholar_id=scholar_id,
                        title=paper.get('title', '')[:500] if paper.get('title') else '',
                        authors=authors[:500] if authors else '',
                        year=paper.get('year'),
                        venue=paper.get('venue', '')[:500] if paper.get('venue') else '',
                        abstract=paper.get('snippet', '')[:2000] if paper.get('snippet') else '',
                        citation_count=paper.get('citationCount') or paper.get('citation_count') or 0,
                        link=paper.get('url', '')[:1000] if paper.get('url') else '',
                    )
                    db.add(citation)

                existing_scholar_ids.add(scholar_id)
                new_count += 1
                papers_saved["new"] += 1
                papers_saved["count"] += 1

            if not args.dry_run and new_count > 0:
                await db.commit()

            log(f"Page {page_num + 1}: +{len(papers)} papers (new: {new_count}, total new: {papers_saved['new']})")

        # Call the REAL stratified harvester
        log("\n" + "="*70)
        log("CALLING harvest_with_language_stratification()")
        log("="*70 + "\n")

        stats = await harvest_with_language_stratification(
            db=db,
            scholar_service=scholar_service,
            edition_id=edition['edition_id'],
            scholar_id=edition['scholar_id'],
            year=args.year,
            edition_title=edition['edition_title'],
            paper_id=edition['paper_id'],
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            total_for_year=total_count,
            job_id=None,
        )

        # Final summary
        log("\n" + "="*70)
        log("FINAL SUMMARY")
        log("="*70)
        log(f"Total for year (Scholar):     {total_count}")
        log(f"Non-English harvested:        {stats.get('non_english_harvested', 0)}")
        log(f"English harvested:            {stats.get('english_harvested', 0)}")
        log(f"Total new unique:             {stats.get('total_new', 0)}")
        log(f"Strategy used:                {stats.get('strategy_used', 'unknown')}")
        log(f"Success:                      {stats.get('success', False)}")

        if stats.get('error'):
            log(f"Error:                        {stats['error']}", "ERROR")

        # Calculate coverage
        final_count = await count_citations_for_year(db, edition['edition_id'], args.year)
        coverage = final_count / total_count * 100 if total_count else 0
        log(f"\nCitations in DB for {args.year}: {final_count}")
        log(f"Coverage: {coverage:.1f}%")

        log("="*70)

    # Close connections
    await engine.dispose()

    log(f"\nLog saved to: {log_filename}")
    LOG_FILE.close()


if __name__ == "__main__":
    asyncio.run(main())
