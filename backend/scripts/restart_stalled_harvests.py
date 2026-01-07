#!/usr/bin/env python3
"""
Script to restart stalled year-by-year harvests.

This script:
1. Finds editions with incomplete harvest_targets
2. For each incomplete year, fetches citations using year filters
3. Updates harvest_targets and saves citations to database
"""

import os
import sys
import asyncio
import json
import logging
from datetime import datetime

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))
    DATABASE_URL = os.getenv("DATABASE_URL")

# Oxylabs credentials
OXYLABS_USERNAME = os.getenv("OXYLABS_USERNAME")
OXYLABS_PASSWORD = os.getenv("OXYLABS_PASSWORD")

# Stalled edition IDs (from our analysis)
STALLED_EDITIONS = [1369, 1639, 2117, 2125, 2148, 2405, 2589]

# Map edition_id to paper_id
EDITION_TO_PAPER = {
    1369: 85,    # The use of knowledge in society
    1639: 354,   # Keywords
    2117: 888,   # Theory of communicative action
    2125: 897,   # Structural transformation
    2148: 919,   # Between facts and norms
    2405: 1176,  # Birth of biopolitics
    2589: 1360,  # Modern world-system I
}


async def get_incomplete_years(db_url: str, edition_id: int) -> list:
    """Get list of incomplete years for an edition from harvest_targets"""
    # Use sync connection for simplicity
    sync_url = db_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT year, expected_count, actual_count
            FROM harvest_targets
            WHERE edition_id = :edition_id AND status = 'incomplete'
            ORDER BY year DESC
        """), {"edition_id": edition_id})
        years = [(row[0], row[1], row[2]) for row in result]
    engine.dispose()
    return years


async def get_edition_info(db_url: str, edition_id: int) -> dict:
    """Get edition info including scholar_id"""
    sync_url = db_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, paper_id, scholar_id, title, citation_count
            FROM editions WHERE id = :edition_id
        """), {"edition_id": edition_id})
        row = result.fetchone()
        if row:
            return {
                "id": row[0],
                "paper_id": row[1],
                "scholar_id": row[2],
                "title": row[3],
                "citation_count": row[4]
            }
    engine.dispose()
    return None


async def get_existing_citations(db_url: str, paper_id: int) -> set:
    """Get existing citation scholar_ids for a paper"""
    sync_url = db_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT scholar_id FROM citations WHERE paper_id = :paper_id AND scholar_id IS NOT NULL
        """), {"paper_id": paper_id})
        ids = {row[0] for row in result}
    engine.dispose()
    return ids


async def fetch_citations_for_year(scholar_id: str, year: int, max_results: int = 200) -> list:
    """Fetch citations for a specific year using Oxylabs"""
    base_url = f"https://scholar.google.com/scholar?cites={scholar_id}&hl=en&as_ylo={year}&as_yhi={year}"

    all_papers = []
    current_page = 0
    max_pages = (max_results + 9) // 10
    consecutive_failures = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        while len(all_papers) < max_results and current_page < max_pages:
            page_url = base_url if current_page == 0 else f"{base_url}&start={current_page * 10}"

            try:
                # Fetch via Oxylabs
                payload = {
                    "source": "google_scholar",
                    "url": page_url,
                    "render": "html",
                    "geo_location": "United States"
                }

                response = await client.post(
                    "https://realtime.oxylabs.io/v1/queries",
                    auth=(OXYLABS_USERNAME, OXYLABS_PASSWORD),
                    json=payload
                )
                response.raise_for_status()
                data = response.json()

                html = data.get("results", [{}])[0].get("content", "")
                if not html:
                    logger.warning(f"Empty response for page {current_page + 1}")
                    break

                # Parse results (simplified parser)
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')
                results = soup.select('.gs_r.gs_or.gs_scl')

                if not results:
                    logger.info(f"No more results on page {current_page + 1}")
                    break

                papers = []
                for result in results:
                    try:
                        # Get title and link
                        title_elem = result.select_one('h3.gs_rt a')
                        title = title_elem.get_text(strip=True) if title_elem else "Unknown"
                        link = title_elem.get('href') if title_elem else None

                        # Get authors
                        authors_elem = result.select_one('.gs_a')
                        authors = authors_elem.get_text(strip=True) if authors_elem else None

                        # Get citation count and scholar_id
                        citation_count = 0
                        scholar_id_found = None
                        for a in result.select('.gs_fl a'):
                            text = a.get_text()
                            if 'Cited by' in text:
                                try:
                                    citation_count = int(text.replace('Cited by', '').strip())
                                except:
                                    pass
                                href = a.get('href', '')
                                if 'cites=' in href:
                                    scholar_id_found = href.split('cites=')[1].split('&')[0]

                        papers.append({
                            "title": title,
                            "link": link,
                            "authorsRaw": authors,
                            "year": year,
                            "citationCount": citation_count,
                            "scholarId": scholar_id_found
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing result: {e}")
                        continue

                all_papers.extend(papers)
                current_page += 1
                consecutive_failures = 0

                logger.info(f"  Page {current_page}: got {len(papers)} papers (total: {len(all_papers)})")

                if current_page < max_pages and len(all_papers) < max_results:
                    await asyncio.sleep(2)  # Rate limit

            except Exception as e:
                consecutive_failures += 1
                logger.error(f"  Page {current_page + 1} failed: {e}")

                if consecutive_failures >= 3:
                    logger.error(f"  Too many failures, stopping at {len(all_papers)} papers")
                    break

                current_page += 1
                await asyncio.sleep(5)

    return all_papers


async def save_citations(db_url: str, paper_id: int, edition_id: int, papers: list, existing_ids: set) -> int:
    """Save new citations to database"""
    sync_url = db_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)

    new_count = 0
    with engine.connect() as conn:
        for paper in papers:
            scholar_id = paper.get("scholarId")
            if not scholar_id or scholar_id in existing_ids:
                continue

            conn.execute(text("""
                INSERT INTO citations (paper_id, edition_id, scholar_id, title, authors, year, link, citation_count, intersection_count, created_at)
                VALUES (:paper_id, :edition_id, :scholar_id, :title, :authors, :year, :link, :citation_count, 1, NOW())
                ON CONFLICT (scholar_id, paper_id) DO NOTHING
            """), {
                "paper_id": paper_id,
                "edition_id": edition_id,
                "scholar_id": scholar_id,
                "title": paper.get("title", "Unknown"),
                "authors": paper.get("authorsRaw"),
                "year": paper.get("year"),
                "link": paper.get("link"),
                "citation_count": paper.get("citationCount", 0)
            })
            existing_ids.add(scholar_id)
            new_count += 1
        conn.commit()

    engine.dispose()
    return new_count


async def update_harvest_target(db_url: str, edition_id: int, year: int, actual_count: int, status: str):
    """Update harvest_target for a year"""
    sync_url = db_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE harvest_targets
            SET actual_count = :actual_count, status = :status, updated_at = NOW()
            WHERE edition_id = :edition_id AND year = :year
        """), {
            "edition_id": edition_id,
            "year": year,
            "actual_count": actual_count,
            "status": status
        })
        conn.commit()
    engine.dispose()


async def update_edition_stats(db_url: str, edition_id: int, total_harvested: int):
    """Update edition harvested_citation_count"""
    sync_url = db_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE editions
            SET harvested_citation_count = :count, last_harvested_at = NOW(), harvest_stall_count = 0
            WHERE id = :edition_id
        """), {"edition_id": edition_id, "count": total_harvested})
        conn.commit()
    engine.dispose()


async def process_edition(db_url: str, edition_id: int, max_years: int = 10):
    """Process incomplete years for one edition"""
    edition = await get_edition_info(db_url, edition_id)
    if not edition:
        logger.error(f"Edition {edition_id} not found")
        return

    paper_id = edition["paper_id"]
    scholar_id = edition["scholar_id"]

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing: {edition['title'][:50]}...")
    logger.info(f"Edition ID: {edition_id}, Scholar ID: {scholar_id}")

    # Get incomplete years
    incomplete_years = await get_incomplete_years(db_url, edition_id)
    if not incomplete_years:
        logger.info("No incomplete years found!")
        return

    logger.info(f"Found {len(incomplete_years)} incomplete years")

    # Get existing citations
    existing_ids = await get_existing_citations(db_url, paper_id)
    logger.info(f"Existing citations: {len(existing_ids)}")

    # Process each incomplete year (limited by max_years)
    total_new = 0
    for year, expected, current in incomplete_years[:max_years]:
        logger.info(f"\n  Year {year}: expected={expected}, current={current}")

        # Fetch citations for this year
        papers = await fetch_citations_for_year(scholar_id, year, max_results=expected + 50)

        if not papers:
            logger.warning(f"  No papers found for year {year}")
            await update_harvest_target(db_url, edition_id, year, current, "complete")
            continue

        # Save new citations
        new_count = await save_citations(db_url, paper_id, edition_id, papers, existing_ids)
        total_new += new_count

        # Update harvest_target
        new_actual = current + new_count
        status = "complete" if new_actual >= expected * 0.9 else "incomplete"  # 90% threshold
        await update_harvest_target(db_url, edition_id, year, new_actual, status)

        logger.info(f"  Saved {new_count} new citations (total for year: {new_actual}/{expected})")

        # Rate limit between years
        await asyncio.sleep(3)

    # Update edition stats
    total_harvested = len(existing_ids) + total_new
    await update_edition_stats(db_url, edition_id, total_harvested)

    logger.info(f"\nEdition complete: added {total_new} new citations")


async def main():
    """Main entry point"""
    logger.info("Starting stalled harvest restart")
    logger.info(f"Database: {DATABASE_URL[:50]}...")

    if not OXYLABS_USERNAME or not OXYLABS_PASSWORD:
        logger.error("OXYLABS credentials not found!")
        return

    # Process each stalled edition
    for edition_id in STALLED_EDITIONS:
        try:
            await process_edition(DATABASE_URL, edition_id, max_years=5)  # Do 5 years per edition
        except Exception as e:
            logger.error(f"Error processing edition {edition_id}: {e}")
            continue

        # Rate limit between editions
        await asyncio.sleep(5)

    logger.info("\n" + "="*60)
    logger.info("Stalled harvest restart complete!")


if __name__ == "__main__":
    asyncio.run(main())
