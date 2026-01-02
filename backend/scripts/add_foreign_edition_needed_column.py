#!/usr/bin/env python3
"""
Add foreign_edition_needed column to papers table.
This migration adds a boolean column to track papers that need foreign edition lookup.
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL environment variable must be set")
    sys.exit(1)

# Convert asyncpg to psycopg2 for synchronous migration
sync_url = DATABASE_URL.replace("+asyncpg", "")

def run_migration():
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        # Check if column already exists
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'papers'
            AND column_name = 'foreign_edition_needed'
        """))

        if result.fetchone():
            print("Column 'foreign_edition_needed' already exists. Nothing to do.")
            return

        # Add the column
        print("Adding 'foreign_edition_needed' column to papers table...")
        conn.execute(text("""
            ALTER TABLE papers
            ADD COLUMN foreign_edition_needed BOOLEAN DEFAULT FALSE
        """))
        conn.commit()
        print("✅ Column added successfully!")

        # Verify
        result = conn.execute(text("""
            SELECT column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = 'papers'
            AND column_name = 'foreign_edition_needed'
        """))
        row = result.fetchone()
        if row:
            print(f"   Column: {row[0]}, Type: {row[1]}, Default: {row[2]}")

if __name__ == "__main__":
    run_migration()
