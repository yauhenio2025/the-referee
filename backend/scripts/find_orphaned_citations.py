#!/usr/bin/env python3
"""
Find citations that belong to deleted papers (orphaned citations).
These likely came from papers that were linked as foreign editions.
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL environment variable must be set")
    sys.exit(1)

sync_url = DATABASE_URL.replace("+asyncpg", "")

def analyze():
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        # Find citations linked to deleted papers
        print("=" * 70)
        print("ORPHANED CITATIONS (linked to deleted papers)")
        print("=" * 70)

        result = conn.execute(text("""
            SELECT
                c.paper_id,
                p.title as paper_title,
                p.deleted_at,
                COUNT(c.id) as citation_count
            FROM citations c
            JOIN papers p ON c.paper_id = p.id
            WHERE p.deleted_at IS NOT NULL
            GROUP BY c.paper_id, p.title, p.deleted_at
            ORDER BY citation_count DESC
            LIMIT 20
        """))

        rows = result.fetchall()
        if rows:
            for row in rows:
                print(f"Paper #{row[0]}: '{row[1][:50]}...' - {row[3]} orphaned citations (deleted: {row[2]})")
        else:
            print("No orphaned citations found.")

        # Check paper 617's editions
        print("\n" + "=" * 70)
        print("PAPER #617 EDITIONS ANALYSIS")
        print("=" * 70)

        result = conn.execute(text("""
            SELECT
                e.id,
                e.title,
                e.scholar_id,
                e.citation_count,
                e.harvested_citation_count,
                e.merged_into_edition_id,
                (SELECT COUNT(*) FROM citations c WHERE c.edition_id = e.id) as actual_citations
            FROM editions e
            WHERE e.paper_id = 617
            ORDER BY e.id
        """))

        for row in result.fetchall():
            print(f"\nEdition #{row[0]}: {row[1][:50]}...")
            print(f"  Scholar ID: {row[2]}")
            print(f"  citation_count (GS): {row[3]}")
            print(f"  harvested_citation_count: {row[4]}")
            print(f"  merged_into_edition_id: {row[5]}")
            print(f"  actual citations in DB: {row[6]}")

        # Check if there are any citations for paper 617 not linked to an edition
        print("\n" + "=" * 70)
        print("PAPER #617 CITATION BREAKDOWN")
        print("=" * 70)

        result = conn.execute(text("""
            SELECT
                edition_id,
                COUNT(*) as count
            FROM citations
            WHERE paper_id = 617
            GROUP BY edition_id
            ORDER BY count DESC
        """))

        total = 0
        for row in result.fetchall():
            ed_label = f"Edition #{row[0]}" if row[0] else "No edition (NULL)"
            print(f"  {ed_label}: {row[1]} citations")
            total += row[1]
        print(f"  TOTAL: {total} citations for paper 617")

        # Find recently deleted papers that might have been foreign editions
        print("\n" + "=" * 70)
        print("RECENTLY DELETED PAPERS (potential foreign editions)")
        print("=" * 70)

        result = conn.execute(text("""
            SELECT
                p.id,
                p.title,
                p.citation_count,
                p.deleted_at,
                (SELECT COUNT(*) FROM citations c WHERE c.paper_id = p.id) as orphaned_citations
            FROM papers p
            WHERE p.deleted_at IS NOT NULL
            AND p.citation_count > 0
            ORDER BY p.deleted_at DESC
            LIMIT 20
        """))

        for row in result.fetchall():
            print(f"Paper #{row[0]}: {row[1][:50]}...")
            print(f"  citation_count: {row[2]}, orphaned_citations: {row[4]}, deleted: {row[3]}")

if __name__ == "__main__":
    analyze()
