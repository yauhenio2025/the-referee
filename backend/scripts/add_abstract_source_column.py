#!/usr/bin/env python3
"""
Migration: Add abstract_source column to papers table

This migration adds the abstract_source column to track where the abstract came from:
- 'scholar_search': From regular Google Scholar search results
- 'allintitle_scrape': From allintitle: query which often returns full abstracts
- 'manual': Manually entered by user

Run this script to add the column to an existing database:
    python scripts/add_abstract_source_column.py
"""
import sqlite3
import sys
from pathlib import Path

# Find the database file
BACKEND_DIR = Path(__file__).parent.parent
DB_PATH = BACKEND_DIR / "referee.db"


def migrate():
    """Add abstract_source column to papers table if it doesn't exist"""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("This migration only applies to existing databases.")
        return False

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Check if papers table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='papers'")
        if not cursor.fetchone():
            print("Papers table doesn't exist yet. The column will be created when the app starts.")
            print("SQLAlchemy will auto-create tables with the new abstract_source field.")
            return True

        # Check if column already exists
        cursor.execute("PRAGMA table_info(papers)")
        columns = {row[1] for row in cursor.fetchall()}

        if "abstract_source" in columns:
            print("Column 'abstract_source' already exists in papers table. Skipping.")
            return True

        # Add the column
        print("Adding 'abstract_source' column to papers table...")
        cursor.execute("""
            ALTER TABLE papers
            ADD COLUMN abstract_source VARCHAR(50)
        """)

        # Update existing papers that have abstracts to mark them as 'scholar_search'
        cursor.execute("""
            UPDATE papers
            SET abstract_source = 'scholar_search'
            WHERE abstract IS NOT NULL AND abstract != ''
        """)

        updated_count = cursor.rowcount
        conn.commit()

        print(f"✓ Column added successfully!")
        print(f"✓ Updated {updated_count} existing papers with abstract_source = 'scholar_search'")
        return True

    except Exception as e:
        conn.rollback()
        print(f"✗ Migration failed: {e}")
        return False

    finally:
        conn.close()


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
