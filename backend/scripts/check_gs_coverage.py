#!/usr/bin/env python3
"""
Check Google Scholar coverage for a paper.

Shows:
- GS reported count
- Our unique citation count
- GS-equivalent count (with duplicate encounters)
- Gap analysis

Usage:
    python scripts/check_gs_coverage.py 71          # Check paper ID 71
    python scripts/check_gs_coverage.py 71 --year 2025  # Check specific year
"""
import asyncio
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


async def check_coverage(paper_id: int, year: int = None):
    engine = create_async_engine(DATABASE_URL)

    async with engine.connect() as conn:
        # Get paper info
        result = await conn.execute(text(
            'SELECT title, citation_count FROM papers WHERE id = :id'
        ), {"id": paper_id})
        paper = result.fetchone()

        if not paper:
            print(f"Paper {paper_id} not found")
            return

        print(f"Paper: {paper[0]}")
        print(f"Google Scholar total: {paper[1]:,}")
        print()

        # Year filter
        year_clause = "AND c.year = :year" if year else ""
        year_label = f" (year {year})" if year else ""

        # Get coverage stats
        result = await conn.execute(text(f'''
            SELECT
                COUNT(*) as unique_count,
                SUM(COALESCE(c.encounter_count, 1)) as gs_equivalent,
                SUM(COALESCE(c.encounter_count, 1)) - COUNT(*) as duplicate_encounters
            FROM citations c
            WHERE c.paper_id = :paper_id
            {year_clause}
        '''), {"paper_id": paper_id, "year": year} if year else {"paper_id": paper_id})
        row = result.fetchone()

        print(f"{'=' * 60}")
        print(f"Coverage{year_label}:")
        print(f"  Our unique citations:      {row[0]:>10,}")
        print(f"  GS-equivalent count:       {row[1]:>10,} (includes duplicates)")
        print(f"  Duplicate encounters:      {row[2]:>10,}")
        print(f"{'=' * 60}")

        if not year:
            print()
            print(f"Gap from GS (unique):        {paper[1] - row[0]:>10,}")
            print(f"Gap from GS (w/ dupes):      {paper[1] - row[1]:>10,}")

        # Top years breakdown if no year filter
        if not year:
            print()
            print("Top years by citation count:")
            result = await conn.execute(text('''
                SELECT
                    year,
                    COUNT(*) as unique_count,
                    SUM(COALESCE(encounter_count, 1)) as gs_equivalent
                FROM citations
                WHERE paper_id = :paper_id AND year IS NOT NULL
                GROUP BY year
                ORDER BY year DESC
                LIMIT 10
            '''), {"paper_id": paper_id})
            rows = result.fetchall()
            print(f"  {'Year':<8} {'Unique':>10} {'GS-equiv':>10} {'Dupes':>8}")
            print(f"  {'-' * 40}")
            for yr, unique, gs_eq in rows:
                dupes = gs_eq - unique
                print(f"  {yr:<8} {unique:>10,} {gs_eq:>10,} {dupes:>8,}")

    await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description='Check GS coverage for a paper')
    parser.add_argument('paper_id', type=int, help='Paper ID to check')
    parser.add_argument('--year', type=int, help='Filter to specific year')
    args = parser.parse_args()

    asyncio.run(check_coverage(args.paper_id, args.year))


if __name__ == "__main__":
    main()
