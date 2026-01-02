#!/usr/bin/env python3
"""
Recover orphaned citations from deleted foreign edition papers.

When papers are linked as editions and then deleted, their citations
become orphaned. This script:
1. Finds orphaned citations (on deleted papers)
2. Identifies which paper they should now belong to (via editions)
3. Moves them to the correct paper and edition
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

# Mapping of deleted paper ID -> (target paper ID, target edition ID)
# Based on analysis: these foreign editions were linked to paper 617
ORPHAN_MAPPING = {
    627: (617, 1876),  # El hombre unidimensional -> paper 617, edition 1876
    620: (617, 1877),  # L'uomo a una dimensione -> paper 617, edition 1877
    650: (617, 1881),  # One-dimensional man (1976) -> paper 617, edition 1881
}

def recover():
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        print("=" * 70)
        print("ORPHANED CITATION RECOVERY")
        print("=" * 70)

        total_recovered = 0

        for source_paper_id, (target_paper_id, target_edition_id) in ORPHAN_MAPPING.items():
            # Count orphaned citations
            result = conn.execute(text("""
                SELECT COUNT(*) FROM citations WHERE paper_id = :source_id
            """), {"source_id": source_paper_id})
            count = result.scalar()

            if count == 0:
                print(f"\nPaper #{source_paper_id}: No orphaned citations found")
                continue

            print(f"\nPaper #{source_paper_id}: {count} orphaned citations")
            print(f"  Moving to paper #{target_paper_id}, edition #{target_edition_id}")

            # Get source paper info for updating edition
            result = conn.execute(text("""
                SELECT scholar_id, citation_count, title
                FROM papers
                WHERE id = :source_id
            """), {"source_id": source_paper_id})
            source = result.fetchone()

            if source:
                print(f"  Source paper: '{source[2][:50]}...'")
                print(f"  Source scholar_id: {source[0]}")
                print(f"  Source citation_count: {source[1]}")

            # Update edition with source paper's metadata (if missing)
            if source and source[0]:  # Has scholar_id
                conn.execute(text("""
                    UPDATE editions
                    SET scholar_id = COALESCE(scholar_id, :scholar_id),
                        citation_count = CASE WHEN citation_count = 0 THEN :citation_count ELSE citation_count END
                    WHERE id = :edition_id
                """), {
                    "scholar_id": source[0],
                    "citation_count": source[1] or 0,
                    "edition_id": target_edition_id
                })
                print(f"  Updated edition with scholar_id and citation_count")

            # First, find which citations already exist in target paper (duplicates)
            result = conn.execute(text("""
                SELECT c_orphan.id
                FROM citations c_orphan
                WHERE c_orphan.paper_id = :source_paper_id
                AND c_orphan.scholar_id IN (
                    SELECT scholar_id FROM citations WHERE paper_id = :target_paper_id
                )
            """), {
                "source_paper_id": source_paper_id,
                "target_paper_id": target_paper_id
            })
            duplicate_ids = [row[0] for row in result.fetchall()]

            if duplicate_ids:
                print(f"  Found {len(duplicate_ids)} duplicate citations (will delete)")
                # Delete duplicates instead of moving
                conn.execute(text("""
                    DELETE FROM citations WHERE id = ANY(:ids)
                """), {"ids": duplicate_ids})

            # Now move remaining unique citations
            result = conn.execute(text("""
                UPDATE citations
                SET paper_id = :target_paper_id,
                    edition_id = :target_edition_id
                WHERE paper_id = :source_paper_id
            """), {
                "target_paper_id": target_paper_id,
                "target_edition_id": target_edition_id,
                "source_paper_id": source_paper_id
            })

            moved = result.rowcount
            total_recovered += moved
            print(f"  ✓ Moved {moved} unique citations, deleted {len(duplicate_ids)} duplicates")

        # Commit all changes
        conn.commit()

        print("\n" + "=" * 70)
        print(f"TOTAL RECOVERED: {total_recovered} citations")
        print("=" * 70)

        # Update harvested_citation_count for affected editions
        print("\nUpdating harvested_citation_count for affected editions...")
        for _, (target_paper_id, target_edition_id) in ORPHAN_MAPPING.items():
            result = conn.execute(text("""
                UPDATE editions
                SET harvested_citation_count = (
                    SELECT COUNT(*) FROM citations WHERE edition_id = :edition_id
                )
                WHERE id = :edition_id
            """), {"edition_id": target_edition_id})

        conn.commit()
        print("Done!")

        # Verify final state
        print("\n" + "=" * 70)
        print("VERIFICATION - Paper #617 Editions After Recovery")
        print("=" * 70)

        result = conn.execute(text("""
            SELECT
                e.id,
                e.title,
                e.scholar_id,
                e.citation_count,
                e.harvested_citation_count,
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
            print(f"  actual citations in DB: {row[5]}")

if __name__ == "__main__":
    recover()
