#!/usr/bin/env python3
"""
Generate comprehensive Excel report of all unfinished harvest jobs.
Includes diagnostics, Google Scholar URLs for debugging, and stall analysis.
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://referee_db_user:hFe0kHlN4BRAwPEvDZg3j1nZxNPQuvXc@dpg-d50iffje5dus73dfpe1g-a.singapore-postgres.render.com/referee_db")
sync_url = DATABASE_URL.replace("+asyncpg", "")

def generate_gs_url(scholar_id: str, year: int = None) -> str:
    """Generate Google Scholar URL for an edition, optionally filtered by year."""
    base = f"https://scholar.google.com/scholar?cites={scholar_id}&hl=en"
    if year:
        base += f"&as_ylo={year}&as_yhi={year}"
    return base

def get_harvest_diagnostics():
    """Get comprehensive diagnostics for all editions with harvesting activity."""
    engine = create_engine(sync_url)

    # Simplified query without jobs table join (too slow)
    edition_query = """
    WITH target_summary AS (
        SELECT
            edition_id,
            SUM(expected_count) as total_expected,
            SUM(actual_count) as total_actual,
            COUNT(*) FILTER (WHERE status = 'complete') as complete_years,
            COUNT(*) FILTER (WHERE status != 'complete' AND expected_count > 0) as incomplete_years_with_expected,
            COUNT(*) FILTER (WHERE status != 'complete' AND expected_count = 0) as incomplete_years_no_expected,
            MIN(CASE WHEN status != 'complete' AND expected_count > 0 THEN year END) as min_incomplete_year,
            MAX(CASE WHEN status != 'complete' AND expected_count > 0 THEN year END) as max_incomplete_year,
            MAX(updated_at) as last_target_update
        FROM harvest_targets
        GROUP BY edition_id
    )
    SELECT
        e.id,
        e.title,
        e.scholar_id,
        e.year as publication_year,
        e.citation_count,
        e.harvest_stall_count,
        e.harvest_complete,
        e.last_harvested_at,
        ts.total_expected,
        ts.total_actual,
        ts.complete_years,
        ts.incomplete_years_with_expected,
        ts.incomplete_years_no_expected,
        ts.min_incomplete_year,
        ts.max_incomplete_year,
        ts.last_target_update,
        CASE
            WHEN e.harvest_complete THEN 'Complete'
            WHEN e.harvest_stall_count >= 5 THEN 'Stalled'
            WHEN ts.incomplete_years_with_expected > 0 THEN 'Has Work'
            ELSE 'Unknown'
        END as status_category,
        ROUND(100.0 * ts.total_actual / NULLIF(ts.total_expected, 0), 1) as pct_complete
    FROM editions e
    LEFT JOIN target_summary ts ON ts.edition_id = e.id
    WHERE e.citation_count > 0
      AND (
          e.harvest_stall_count > 0
          OR ts.incomplete_years_with_expected > 0
          OR (e.harvest_complete = false AND ts.total_expected > 0)
      )
    ORDER BY
        CASE
            WHEN e.harvest_stall_count >= 5 THEN 0
            ELSE 1
        END,
        e.harvest_stall_count DESC,
        (ts.total_expected - ts.total_actual) DESC
    """

    with engine.connect() as conn:
        editions_df = pd.read_sql(text(edition_query), conn)

    # Get detailed year breakdown for stalled editions
    stalled_ids = editions_df[editions_df['harvest_stall_count'] >= 5]['id'].tolist()

    if stalled_ids:
        year_query = f"""
        SELECT
            ht.edition_id,
            ht.year,
            ht.expected_count,
            ht.actual_count,
            ht.status,
            ht.pages_attempted,
            ht.pages_succeeded,
            ht.pages_failed,
            ht.updated_at
        FROM harvest_targets ht
        WHERE ht.edition_id IN ({','.join(map(str, stalled_ids))})
          AND ht.status != 'complete'
          AND ht.expected_count > 0
        ORDER BY ht.edition_id, ht.year DESC
        """

        with engine.connect() as conn:
            stalled_years_df = pd.read_sql(text(year_query), conn)
    else:
        stalled_years_df = pd.DataFrame()

    return editions_df, stalled_years_df

def create_excel_report(editions_df, stalled_years_df, output_path):
    """Create Excel report with multiple sheets."""

    # Add Google Scholar URLs
    editions_df['gs_url'] = editions_df.apply(
        lambda r: generate_gs_url(r['scholar_id']) if pd.notna(r['scholar_id']) else '',
        axis=1
    )

    # Calculate time since last activity
    now = datetime.utcnow()
    editions_df['hours_since_activity'] = editions_df['last_harvested_at'].apply(
        lambda x: round((now - x).total_seconds() / 3600, 1) if pd.notna(x) else None
    )

    # Determine likely stall reason
    def get_likely_reason(row):
        if row['harvest_stall_count'] < 5:
            return ''
        pct = row['pct_complete'] or 0
        if pct > 90:
            return 'Near complete - likely duplicates/inaccessible citations'
        elif pct > 70:
            return 'High progress - may need pagination adjustment'
        elif pct > 50:
            return 'Medium progress - possible rate limiting or parsing issues'
        else:
            return 'Low progress - needs investigation (rate limit? blocked?)'

    editions_df['likely_stall_reason'] = editions_df.apply(get_likely_reason, axis=1)

    # Reorder columns for readability
    main_cols = [
        'id', 'title', 'status_category', 'harvest_stall_count', 'pct_complete',
        'total_actual', 'total_expected', 'incomplete_years_with_expected',
        'min_incomplete_year', 'max_incomplete_year',
        'hours_since_activity', 'likely_stall_reason',
        'gs_url', 'scholar_id', 'citation_count', 'publication_year',
        'last_harvested_at', 'last_target_update'
    ]
    editions_df = editions_df[[c for c in main_cols if c in editions_df.columns]]

    # Create stalled editions sheet with URLs for each year
    if not stalled_years_df.empty:
        stalled_years_df = stalled_years_df.merge(
            editions_df[['id', 'scholar_id', 'title']].rename(columns={'id': 'edition_id'}),
            on='edition_id'
        )
        stalled_years_df['gs_year_url'] = stalled_years_df.apply(
            lambda r: generate_gs_url(r['scholar_id'], r['year']) if pd.notna(r['scholar_id']) else '',
            axis=1
        )
        stalled_years_df['gap'] = stalled_years_df['expected_count'] - stalled_years_df['actual_count']
        stalled_years_df['pct'] = round(100 * stalled_years_df['actual_count'] / stalled_years_df['expected_count'].replace(0, 1), 1)

    # Write to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = {
            'Metric': [
                'Total Editions with Activity',
                'Stalled (stall_count >= 5)',
                'Has Work (not stalled)',
                'Complete',
                'Total Citations Expected',
                'Total Citations Harvested',
                'Overall Completion %'
            ],
            'Value': [
                len(editions_df),
                len(editions_df[editions_df['harvest_stall_count'] >= 5]),
                len(editions_df[editions_df['status_category'] == 'Has Work']),
                len(editions_df[editions_df['status_category'] == 'Complete']),
                int(editions_df['total_expected'].sum()) if pd.notna(editions_df['total_expected'].sum()) else 0,
                int(editions_df['total_actual'].sum()) if pd.notna(editions_df['total_actual'].sum()) else 0,
                round(100 * editions_df['total_actual'].sum() / max(editions_df['total_expected'].sum(), 1), 1) if pd.notna(editions_df['total_expected'].sum()) else 0
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        # All editions
        editions_df.to_excel(writer, sheet_name='All Editions', index=False)

        # Stalled editions only
        stalled_df = editions_df[editions_df['harvest_stall_count'] >= 5].copy()
        if not stalled_df.empty:
            stalled_df.to_excel(writer, sheet_name='Stalled Editions', index=False)

        # Stalled year details
        if not stalled_years_df.empty:
            year_cols = ['edition_id', 'title', 'year', 'expected_count', 'actual_count',
                        'gap', 'pct', 'status', 'pages_attempted', 'gs_year_url']
            stalled_years_df[[c for c in year_cols if c in stalled_years_df.columns]].to_excel(
                writer, sheet_name='Stalled Year Details', index=False
            )

        # Editions with work to do
        has_work_df = editions_df[editions_df['status_category'] == 'Has Work'].copy()
        if not has_work_df.empty:
            has_work_df.to_excel(writer, sheet_name='Has Work', index=False)

    return output_path

def main():
    print("Fetching harvest diagnostics...")
    editions_df, stalled_years_df = get_harvest_diagnostics()

    print(f"Found {len(editions_df)} editions with harvesting activity")
    print(f"  - Stalled: {len(editions_df[editions_df['harvest_stall_count'] >= 5])}")
    print(f"  - Has Work: {len(editions_df[editions_df['status_category'] == 'Has Work'])}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"/home/evgeny/projects/referee/the-referee/harvest_report_{timestamp}.xlsx"

    print(f"\nGenerating Excel report: {output_path}")
    create_excel_report(editions_df, stalled_years_df, output_path)

    print(f"\nReport saved to: {output_path}")

    # Also print stalled summary to console
    stalled = editions_df[editions_df['harvest_stall_count'] >= 5]
    if not stalled.empty:
        print("\n" + "="*80)
        print("STALLED EDITIONS SUMMARY:")
        print("="*80)
        for _, row in stalled.head(15).iterrows():
            print(f"\nEdition {row['id']}: {row['title'][:50]}...")
            print(f"  Progress: {row['total_actual']}/{row['total_expected']} ({row['pct_complete']}%)")
            print(f"  Stall count: {row['harvest_stall_count']}")
            print(f"  Incomplete years: {row['min_incomplete_year']}-{row['max_incomplete_year']} ({row['incomplete_years_with_expected']} years)")
            print(f"  Likely reason: {row['likely_stall_reason']}")
            print(f"  GS URL: {row['gs_url']}")

if __name__ == "__main__":
    main()
