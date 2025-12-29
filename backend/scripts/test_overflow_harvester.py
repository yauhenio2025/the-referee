#!/usr/bin/env python3
"""
REAL TEST of overflow_harvester.py functionality.

This script ACTUALLY CALLS the real overflow harvester functions and saves to the DB.
No reimplementation bullshit - we test the actual code.

Usage:
    python scripts/test_overflow_harvester.py --paper-id 71 --year 2014

What it does:
1. Connects to the real database
2. Gets edition details for the specified paper
3. Calls the REAL harvest_partition() function from overflow_harvester.py
4. Everything gets saved to the DB (PartitionRun, PartitionTermAttempt, PartitionQuery, PartitionLLMCall)
5. Logs EVERYTHING to stdout AND a log file for post-mortem analysis
"""

import argparse
import asyncio
import os
import sys
import logging
from datetime import datetime
from typing import Set, Dict, Any, List

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env BEFORE any other imports
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Global log file handle
LOG_FILE = None

# Import the REAL overflow harvester
from app.services.overflow_harvester import (
    harvest_partition,
    GOOGLE_SCHOLAR_LIMIT,
    TARGET_THRESHOLD,
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
            "edition_id": row[0],
            "edition_title": row[1],
            "scholar_id": row[2],
            "language": row[3],
            "paper_id": row[4],
            "paper_title": row[5],
        }
    return None


async def get_existing_citation_ids(db: AsyncSession, edition_id: int, year: int) -> Set[str]:
    """Get scholar IDs of citations we already have for this edition/year"""
    result = await db.execute(text("""
        SELECT scholar_id FROM citations
        WHERE edition_id = :edition_id AND year = :year AND scholar_id IS NOT NULL
    """), {"edition_id": edition_id, "year": year})
    return {row[0] for row in result.fetchall()}


async def get_year_count(scholar: ScholarSearchService, scholar_id: str, year: int) -> int:
    """Get citation count for a specific year"""
    result = await scholar.get_cited_by(
        scholar_id=scholar_id,
        max_results=10,
        year_low=year,
        year_high=year,
    )
    return result.get('totalResults', 0)


async def run_test(paper_id: int, year: int, dry_run: bool = False, log_file_path: str = None):
    """
    Run the REAL overflow harvester test.

    Args:
        paper_id: The paper ID to test (e.g., 71 for Jameson's Postmodernism)
        year: The year to harvest (e.g., 2014)
        dry_run: If True, only check counts but don't actually harvest
        log_file_path: Path to write detailed log file
    """
    global LOG_FILE

    # Setup file logging
    if log_file_path:
        LOG_FILE = open(log_file_path, 'w')
        log(f"LOG FILE: {log_file_path}")

    # Also capture ALL Python logging to our log file
    if LOG_FILE:
        class LogFileHandler(logging.Handler):
            def emit(self, record):
                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                line = f"[{ts}] [{record.levelname:5}] [{record.name}] {record.getMessage()}"
                LOG_FILE.write(line + "\n")
                LOG_FILE.flush()

        root_logger = logging.getLogger()
        root_logger.addHandler(LogFileHandler())

    log("=" * 80)
    log(f"OVERFLOW HARVESTER TEST - Paper ID: {paper_id}, Year: {year}")
    log(f"Dry run: {dry_run}")
    log("=" * 80)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log("ERROR: DATABASE_URL not set in environment", "ERROR")
        return

    log(f"Database: {db_url[:50]}...")

    # Create async engine and session
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as db:
        # Step 1: Get edition details
        log("")
        log("STEP 1: Getting edition details from database...")
        edition = await get_edition_for_paper(db, paper_id)

        if not edition:
            log(f"ERROR: No edition found for paper_id={paper_id}", "ERROR")
            return

        log(f"  Paper:     {edition['paper_title']}")
        log(f"  Edition:   {edition['edition_title']}")
        log(f"  Scholar ID: {edition['scholar_id']}")
        log(f"  Edition ID: {edition['edition_id']}")

        scholar_id = edition['scholar_id']
        edition_id = edition['edition_id']
        edition_title = edition['edition_title']

        # Step 2: Get existing citations for this year
        log("")
        log(f"STEP 2: Checking existing citations for year {year}...")
        existing_ids = await get_existing_citation_ids(db, edition_id, year)
        log(f"  Already have {len(existing_ids)} citations in DB for {year}")

        # Step 3: Initialize scholar service and get current count
        log("")
        log("STEP 3: Querying Google Scholar for current count...")
        scholar = ScholarSearchService()

        try:
            total_count = await get_year_count(scholar, scholar_id, year)
            log(f"  Google Scholar reports: {total_count} citations for {year}")

            if total_count <= GOOGLE_SCHOLAR_LIMIT:
                log(f"  Count is below {GOOGLE_SCHOLAR_LIMIT} - no overflow, direct harvest possible")
                if not dry_run:
                    log("  (But we'll still test the partition logic for demonstration)")
            else:
                log(f"  OVERFLOW DETECTED: {total_count} > {GOOGLE_SCHOLAR_LIMIT}")
                log(f"  Will need to partition to harvest all citations")

            if dry_run:
                log("")
                log("DRY RUN - Not actually harvesting. Use --run to execute.")
                log("")
                log("Summary:")
                log(f"  Total on Scholar: {total_count}")
                log(f"  Already in DB:    {len(existing_ids)}")
                log(f"  Potentially new:  {total_count - len(existing_ids)}")
                await scholar.close()
                return

            # Step 4: Run the REAL harvest_partition
            log("")
            log("STEP 4: Running REAL harvest_partition()...")
            log("=" * 80)
            log("  All progress below is from the actual overflow_harvester.py")
            log("  Everything is being saved to the database!")
            log("=" * 80)
            log("")

            citations_collected = []

            async def on_page_complete(page_num: int, papers: List[Dict]):
                """Callback for each page of results"""
                new_count = 0
                skipped_duplicates = 0
                committed_count = 0

                for p in papers:
                    pid = p.get("scholarId") or p.get("id")
                    if pid and pid not in existing_ids:
                        existing_ids.add(pid)

                        # Save to database
                        authors_raw = p.get("authors")
                        if isinstance(authors_raw, list):
                            authors_str = ", ".join(authors_raw)
                        else:
                            authors_str = authors_raw

                        citation = Citation(
                            paper_id=paper_id,
                            edition_id=edition_id,
                            scholar_id=pid,
                            title=p.get("title"),
                            authors=authors_str,
                            year=p.get("year") or year,
                            venue=p.get("venue"),
                            citation_count=p.get("citationCount"),
                            link=p.get("link"),
                        )

                        try:
                            db.add(citation)
                            await db.commit()
                            citations_collected.append(p)
                            new_count += 1
                            committed_count += 1
                        except IntegrityError:
                            await db.rollback()
                            skipped_duplicates += 1
                            log(f"    DUPLICATE: {pid[:20]}... already in DB", "WARN")

                # Log with explicit commit confirmation
                log(f"  Page {page_num + 1}: {len(papers)} papers | {committed_count} COMMITTED to DB | {skipped_duplicates} duplicates | Total in DB: {len(citations_collected)}")

            # Call the REAL function
            result = await harvest_partition(
                db=db,
                scholar_service=scholar,
                edition_id=edition_id,
                scholar_id=scholar_id,
                year=year,
                edition_title=edition_title,
                paper_id=paper_id,
                existing_scholar_ids=existing_ids,
                on_page_complete=on_page_complete,
                total_for_year=total_count,
                job_id=None,  # No job, this is a test
            )

            # Final commit
            await db.commit()

            # Step 5: Summary
            log("")
            log("=" * 80)
            log("HARVEST COMPLETE - SUMMARY")
            log("=" * 80)
            log(f"  Partition Run ID:     {result.get('partition_run_id')}")
            log(f"  Success:              {result.get('success')}")
            log(f"  Depth:                {result.get('depth')}")
            log(f"  Initial count:        {result.get('initial_count')}")
            log(f"  Exclusion harvested:  {result.get('exclusion_harvested')}")
            log(f"  Inclusion harvested:  {result.get('inclusion_harvested')}")
            log(f"  Total new:            {result.get('total_new')}")

            if result.get('error'):
                log(f"  Error:                {result.get('error')}", "ERROR")

            log("")
            log("Database records created:")
            log("  - PartitionRun: Tracks the overall partition attempt")
            log("  - PartitionTermAttempt: Every term we tried to exclude")
            log("  - PartitionQuery: Every Scholar query we made")
            log("  - PartitionLLMCall: Every LLM call for term suggestions")
            log("  - Citations: The actual papers we harvested")
            log("")
            log("You can inspect these in the database to see exactly what happened.")

        finally:
            await scholar.close()

    await engine.dispose()

    # Close log file
    if LOG_FILE:
        log("=" * 80)
        log("LOG FILE COMPLETE")
        log("=" * 80)
        LOG_FILE.close()


def main():
    parser = argparse.ArgumentParser(
        description="Test the REAL overflow harvester functionality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run - just check counts
  python scripts/test_overflow_harvester.py --paper-id 71 --year 2014

  # Actually run the harvest with logging
  python scripts/test_overflow_harvester.py --paper-id 71 --year 2014 --run --log harvest.log

  # Test a different year
  python scripts/test_overflow_harvester.py --paper-id 71 --year 2013 --run
"""
    )
    parser.add_argument("--paper-id", type=int, required=True,
                        help="Paper ID to test (e.g., 71 for Jameson's Postmodernism)")
    parser.add_argument("--year", type=int, required=True,
                        help="Year to harvest (e.g., 2014)")
    parser.add_argument("--run", action="store_true",
                        help="Actually run the harvest (default is dry-run)")
    parser.add_argument("--log", type=str, default=None,
                        help="Log file path for detailed output (e.g., harvest.log)")

    args = parser.parse_args()

    dry_run = not args.run

    # Auto-generate log file name if --run but no --log specified
    log_file = args.log
    if args.run and not log_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"harvest_p{args.paper_id}_y{args.year}_{timestamp}.log"
        print(f"Auto-logging to: {log_file}")

    asyncio.run(run_test(args.paper_id, args.year, dry_run=dry_run, log_file_path=log_file))


if __name__ == "__main__":
    main()
