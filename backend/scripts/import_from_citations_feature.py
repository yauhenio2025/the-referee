#!/usr/bin/env python3
"""
Import seeds and citations from the old citations-feature session files
into the-referee database.
"""
import json
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Database URL - must be set via environment variable
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL environment variable must be set")
    print("Example: export DATABASE_URL='postgresql://user:pass@host/db'")
    sys.exit(1)

# Session files to import
CITATIONS_FEATURE_PATH = "/home/evgeny/projects/google_scholar/citations-feature/backend/storage/sessions"

# Mapping of seeds to collections
SEED_COLLECTION_MAP = {
    "To save everything, click here": 3,  # EM Capitalism Book
    "From counterculture to cyberculture": 2,  # History of Cybernetics
    "The fascist offensive and the tasks of the Communist International": 1,  # National Liberation Marxism
    "Dimitrov and Stalin: 1934-1943": 1,  # National Liberation Marxism
    "The darker nations": 1,  # National Liberation Marxism
    "The poorer nations": 1,  # National Liberation Marxism
}

# Session files containing our target seeds
SESSION_FILES = [
    "session_1765586347337_0n37vllzg.json",  # Morozov + Turner
    "session_1765351262398_yb5mnb3qm.json",  # Dimitrov + Prashad
]


def get_collection_for_seed(title: str) -> int:
    """Get collection ID for a seed based on partial title match"""
    title_lower = title.lower()
    for pattern, collection_id in SEED_COLLECTION_MAP.items():
        if pattern.lower() in title_lower:
            return collection_id
    return None


def import_session(session_path: str, db_session):
    """Import seeds and citations from a session file"""
    print(f"\n{'='*60}")
    print(f"Importing from: {os.path.basename(session_path)}")
    print('='*60)

    with open(session_path, 'r') as f:
        session_data = json.load(f)

    if 'crossCitationResults' not in session_data:
        print("  No cross-citation results found")
        return

    # Get the most recent cross-citation result
    cc_result = session_data['crossCitationResults'][-1]
    result = cc_result.get('result', {})
    seeds = result.get('seeds', [])
    intersections = result.get('intersections', [])

    print(f"  Seeds: {len(seeds)}")
    print(f"  Cross-citations: {len(intersections)}")

    paper_ids = {}  # Map seed index to paper_id

    # Import seeds as papers
    for seed in seeds:
        title = seed.get('title', 'Unknown')
        collection_id = get_collection_for_seed(title)

        print(f"\n  Seed: {title[:60]}...")
        print(f"    Citations: {seed.get('citationCount', 0)}, Fetched: {seed.get('fetched', 0)}")
        print(f"    Collection: {collection_id}")

        # Check if paper already exists
        existing = db_session.execute(
            text("SELECT id FROM papers WHERE LOWER(title) LIKE :pattern"),
            {"pattern": f"%{title[:40].lower()}%"}
        ).fetchone()

        if existing:
            print(f"    -> Paper already exists (id={existing[0]})")
            paper_ids[seed.get('index', len(paper_ids))] = existing[0]

            # Update collection assignment if needed
            if collection_id:
                db_session.execute(
                    text("UPDATE papers SET collection_id = :cid WHERE id = :pid"),
                    {"cid": collection_id, "pid": existing[0]}
                )
            continue

        # Insert new paper
        result = db_session.execute(
            text("""
                INSERT INTO papers (title, citation_count, status, collection_id, created_at, updated_at)
                VALUES (:title, :citations, 'resolved', :collection_id, NOW(), NOW())
                RETURNING id
            """),
            {
                "title": title,
                "citations": seed.get('citationCount', 0),
                "collection_id": collection_id
            }
        )
        paper_id = result.fetchone()[0]
        paper_ids[seed.get('index', len(paper_ids))] = paper_id
        print(f"    -> Created paper (id={paper_id})")

        # Create an edition for this paper (the main edition)
        db_session.execute(
            text("""
                INSERT INTO editions (paper_id, title, citation_count, confidence, selected, auto_selected, is_supplementary, created_at)
                VALUES (:paper_id, :title, :citations, 'high', true, false, false, NOW())
            """),
            {
                "paper_id": paper_id,
                "title": title,
                "citations": seed.get('citationCount', 0)
            }
        )
        print(f"    -> Created main edition")

    db_session.commit()

    # Import citations (intersections)
    print(f"\n  Importing {len(intersections)} cross-citations...")
    imported = 0
    skipped = 0

    for citation in intersections:
        scholar_id = citation.get('scholarId') or citation.get('id')
        # Truncate long scholar_ids (some are generated strings)
        if scholar_id and len(scholar_id) > 50:
            scholar_id = scholar_id[:50]
        title = citation.get('title', 'Unknown')
        seeds_cited = citation.get('seedsCited', [])

        if not seeds_cited:
            continue

        # Insert citation for each seed it cites
        for seed_idx in seeds_cited:
            paper_id = paper_ids.get(seed_idx)
            if not paper_id:
                continue

            # Check if citation already exists for this paper
            existing = db_session.execute(
                text("""
                    SELECT id FROM citations
                    WHERE paper_id = :pid AND (scholar_id = :sid OR LOWER(title) = LOWER(:title))
                """),
                {"pid": paper_id, "sid": scholar_id, "title": title}
            ).fetchone()

            if existing:
                skipped += 1
                continue

            # Insert citation
            db_session.execute(
                text("""
                    INSERT INTO citations (
                        paper_id, scholar_id, title, authors, year, venue,
                        abstract, link, citation_count, intersection_count, created_at
                    )
                    VALUES (
                        :paper_id, :scholar_id, :title, :authors, :year, :venue,
                        :abstract, :link, :citation_count, :intersection_count, NOW()
                    )
                """),
                {
                    "paper_id": paper_id,
                    "scholar_id": scholar_id,
                    "title": title,
                    "authors": citation.get('authorsRaw', ''),
                    "year": citation.get('year'),
                    "venue": citation.get('venue', ''),
                    "abstract": citation.get('abstract', ''),
                    "link": citation.get('link', ''),
                    "citation_count": citation.get('citationCount', 0),
                    "intersection_count": citation.get('intersectionCount', 1)
                }
            )
            imported += 1

    db_session.commit()
    print(f"  -> Imported {imported} citations, skipped {skipped} duplicates")


def main():
    print("=" * 60)
    print("IMPORT FROM CITATIONS-FEATURE")
    print("=" * 60)

    # Connect to database
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    db_session = Session()

    try:
        for session_file in SESSION_FILES:
            session_path = os.path.join(CITATIONS_FEATURE_PATH, session_file)
            if os.path.exists(session_path):
                import_session(session_path, db_session)
            else:
                print(f"Session file not found: {session_path}")

        # Show final counts
        print("\n" + "=" * 60)
        print("FINAL COUNTS")
        print("=" * 60)

        result = db_session.execute(text("""
            SELECT c.name, COUNT(p.id) as paper_count
            FROM collections c
            LEFT JOIN papers p ON p.collection_id = c.id
            GROUP BY c.id, c.name
            ORDER BY c.id
        """))
        for row in result:
            print(f"  {row[0]}: {row[1]} papers")

        paper_count = db_session.execute(text("SELECT COUNT(*) FROM papers")).fetchone()[0]
        citation_count = db_session.execute(text("SELECT COUNT(*) FROM citations")).fetchone()[0]
        edition_count = db_session.execute(text("SELECT COUNT(*) FROM editions")).fetchone()[0]

        print(f"\n  Total papers: {paper_count}")
        print(f"  Total editions: {edition_count}")
        print(f"  Total citations: {citation_count}")

    except Exception as e:
        print(f"Error: {e}")
        db_session.rollback()
        raise
    finally:
        db_session.close()


if __name__ == "__main__":
    main()
