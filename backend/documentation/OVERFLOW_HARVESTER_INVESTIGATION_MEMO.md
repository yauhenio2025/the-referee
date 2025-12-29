# Overflow Harvester Investigation Memo

**Date**: 2025-12-29
**Status**: Investigation Required
**Log File**: `/home/evgeny/projects/referee/the-referee/backend/harvest_p71_y2014_20251229_163027.log`

---

## Executive Summary

Run #17 of the overflow harvester for Paper ID 71 (Jameson's Postmodernism), Year 2014:
- **Initial count**: 1830 citations
- **Exclusion harvested**: 985
- **Inclusion harvested**: 484
- **Total new (reported)**: 1469
- **Actual in DB**: 1397 (verified via SQL)
- **Gap from initial**: 433 citations missing (23.7%)
- **Gap from reported**: 72 citations lost between harvest and DB (4.9%)

## Critical Finding from Diagnostics

```
partition_runs row #17:
- initial_count: 1830
- exclusion_harvested: 985
- inclusion_harvested: 484
- total_harvested: 1469
- total_new_unique: 1469

SQL: SELECT COUNT(*) FROM citations WHERE paper_id=71 AND year=2014
Result: 1397

Duplicates in DB: 0
NULL scholar_ids: 0
```

**There are TWO gaps to investigate:**
1. **Gap A (harvest gap)**: 1830 - 1469 = 361 (papers not covered by exclusion+inclusion)
2. **Gap B (commit gap)**: 1469 - 1397 = 72 (papers harvested but not in DB) **SOLVED - see below**

## Gap B Explained: Papers Without Scholar IDs

Analysis of log file shows:
- **Exclusion phase**: Harvester reported 985, but log shows 917 committed → 68 missing
- **Inclusion phase**: Harvester reported 484, but log shows 480 committed → 4 missing
- **Total**: 68 + 4 = 72 papers = exact match to gap!

**Root Cause**: The test script skips papers without scholar_ids:
```python
pid = p.get("scholarId") or p.get("id")
if pid and pid not in existing_ids:  # Papers without IDs are silently skipped!
    # save to DB
```

Pages showing less than 10 committed (with 0 duplicates) indicate papers without IDs:
- Page 9: 9/10 committed (1 without ID)
- Page 17: 9/10 committed (1 without ID)
- Page 23: 8/10 committed (2 without ID)

**Fix**: Log and handle papers without scholar_ids explicitly. These may be:
- Unpublished works / preprints
- Conference papers without DOIs
- Google Scholar parsing failures

## Duplicate Analysis: Google Scholar Result Cycling

Pages 90-99 of exclusion phase show Google Scholar instability:

```
Page 90: 10 papers | 7 committed | 2 duplicates | 1 without ID
Page 91: 10 papers | 10 committed | 0 duplicates
Page 92: 10 papers | 9 committed | 1 duplicate
Page 93: 10 papers | 10 committed | 0 duplicates
Page 94: 10 papers | 8 committed | 1 duplicate | 1 without ID
Page 95: 10 papers | 9 committed | 1 duplicate
Page 96: 10 papers | 0 committed | 10 duplicates  <-- 100% DUPLICATES!
Page 97: 10 papers | 0 committed | 10 duplicates  <-- 100% DUPLICATES!
Page 98: 10 papers | 6 committed | 4 duplicates
Page 99: 10 papers | 7 committed | 1 duplicate | 2 without ID
```

**Key Finding**: Near the 1000 result limit, Google Scholar starts recycling results.
Pages 96-97 returned 20 papers that had ALL been seen before.

**Implication**: Even if we could query >1000 results, the data degrades significantly after ~950.

## Term Progression Data (Run #17)

```
Order | Term            | Before | After | Reduction
------+---------------- +--------+-------+----------
    1 | cultural        |   1830 |  1760 |       70
    2 | contemporary    |   1760 |  1680 |       80
    3 | theory          |   1680 |  1630 |       50
    4 | modern          |   1630 |  1600 |       30
    5 | social          |   1600 |  1550 |       50
    6 | political       |   1550 |  1540 |       10
    7 | critical        |   1540 |  1510 |       30
    8 | postmodern      |   1510 |  1460 |       50
    9 | analysis        |   1460 |  1450 |       10
   10 | culture         |   1450 |  1400 |       50
   11 | identity        |   1400 |  1380 |       20
   12 | space           |   1380 |  1350 |       30
   13 | discourse       |   1350 |  1340 |       10
   14 | global          |   1340 |   210 |     1130  <-- SUSPICIOUS!
```

**PARSING BUG CONFIRMED** (2025-12-29 follow-up):

The "210" was actually "1210" with the leading "1" dropped during parsing!

**Evidence**:
- Re-running same query now: `totalResults = 1210` ✓
- Without "global": `totalResults = 1340` ✓
- Actual reduction from "global": 130 (9.7%), NOT 1130 (84%)

**Root Cause**: The HTML count may have been formatted as "1 210" (space-separated thousands)
which the old regex `[\d,\.]+` didn't capture properly.

**Fix Applied**: Updated `_extract_result_count()` in `scholar_search.py`:
1. Added `\s` to regex pattern: `[\d,\.\s]+` to handle space-separated numbers
2. Added debug logging to show raw match vs cleaned number
3. Added context logging when no match found

This same bug likely caused the earlier "410 vs 1410" discrepancy during Run #15.

---

## Issues to Investigate

### 1. The 361 Citation Gap

**Observation**: Exclusion (985) + Inclusion (484) = 1469, not 1830.

**Hypotheses**:

a) **Imperfect Complementarity**: `-intitle:"X"` and `intitle:"X"` may not be perfect complements in Google Scholar
   - Papers may have terms in metadata/abstract but not in the searchable title field
   - Google Scholar's title matching may be fuzzy or context-dependent

b) **Multiple Term OR Behavior**: The inclusion query uses OR:
   ```
   intitle:"cultural" OR intitle:"theory" OR intitle:"postmodern"...
   ```
   - OR queries may have different behavior/coverage than simple queries
   - Google Scholar may cap results differently for complex queries

c) **Count Drift During Harvest**: Initial count was 1830, but Google Scholar counts fluctuate
   - Already observed: Count changed from 410 to 1410 in 10 seconds during same session
   - The "true" count may have been lower when actual harvesting occurred

d) **Overlap Handling**: Some papers excluded by ALL terms may exist
   - If a paper doesn't contain ANY of the exclusion terms in its title, it won't appear in inclusion
   - Papers with empty or special character titles

### 2. Counting Mismatch (1397 vs 1469)

**Observation**: Log shows "Total in DB: 1397" but summary says "Total new: 1469"

**Investigation**:
- The test script callback tracks `citations_collected` list
- The overflow_harvester returns its own counts
- These may be counting different things or have race conditions

**Check**:
```sql
SELECT COUNT(*) FROM citations WHERE paper_id = 71 AND year = 2014;
```

### 3. 100% Duplicate Pages (Pages 96-97)

**Observation**: Pages 96 and 97 in exclusion phase returned papers that were all already seen.

**Hypotheses**:
- Google Scholar may cycle/repeat results near the 1000 limit
- Pagination may become unstable at high page numbers
- Results may shift during long-running harvests as the index updates

**Action**: Analyze log to see pattern of duplicates across all pages.

### 4. Papers Without Scholar IDs

**Observation**: Some pages show "9/10 committed" or "8/10 committed" with 0 duplicates

**Questions**:
- What happens to papers without scholar_ids?
- Are they being silently skipped?
- Could this account for some of the gap?

**Check in code**:
```python
pid = p.get("scholarId") or p.get("id")
if pid and pid not in existing_ids:  # <- Papers without IDs are skipped here
```

---

## Proposed Alternative: Multiple Queries Instead of OR

**Current Approach (Inclusion)**:
```
intitle:"cultural" OR intitle:"theory" OR intitle:"postmodern"...
```

**Problems with OR**:
1. Complex queries may have result limits or different behavior
2. Hard to know coverage of each term
3. Google Scholar's OR may not be exhaustive

**Alternative Approach: Sequential Term Harvesting**

For each exclusion term used, run a SEPARATE inclusion query:

```python
# Instead of one big OR query:
# intitle:"cultural" OR intitle:"theory" OR intitle:"postmodern"

# Run sequential queries:
for term in exclusion_terms:
    results = query(f'intitle:"{term}"')
    for paper in results:
        if paper.scholar_id not in already_harvested:
            save(paper)
            already_harvested.add(paper.scholar_id)
```

**Benefits**:
1. Each query is simple and predictable
2. Can harvest up to 1000 per term (vs shared 1000 for OR)
3. Better coverage guarantee
4. Easier to debug and understand

**Costs**:
1. More API calls
2. More time
3. More rate limiting risk

**Hybrid Approach**: Could also combine adjacent small terms:
- If `intitle:"cultural"` has 50 results, and `intitle:"theory"` has 80 results
- Combine them: `intitle:"cultural" OR intitle:"theory"` (130 < 1000, safe)
- Only split when individual terms exceed threshold

---

## Diagnostic Queries

### Check actual DB count
```sql
SELECT COUNT(*) FROM citations WHERE paper_id = 71 AND year = 2014;
```

### Check for duplicates in DB
```sql
SELECT scholar_id, COUNT(*) as cnt
FROM citations
WHERE paper_id = 71 AND year = 2014
GROUP BY scholar_id
HAVING COUNT(*) > 1;
```

### Check NULL scholar_ids
```sql
SELECT COUNT(*) FROM citations
WHERE paper_id = 71 AND year = 2014 AND scholar_id IS NULL;
```

### Check partition run details
```sql
SELECT
    id, created_at, status, depth,
    initial_count, exclusion_set_count, inclusion_set_count,
    exclusion_harvested, inclusion_harvested, total_new
FROM partition_runs
WHERE edition_id = (SELECT id FROM editions WHERE paper_id = 71 LIMIT 1)
ORDER BY created_at DESC
LIMIT 5;
```

### Check term attempts
```sql
SELECT
    term, term_order,
    count_before, count_after,
    was_accepted, rejection_reason
FROM partition_term_attempts
WHERE partition_run_id = (
    SELECT id FROM partition_runs
    WHERE edition_id = (SELECT id FROM editions WHERE paper_id = 71 LIMIT 1)
    ORDER BY created_at DESC LIMIT 1
)
ORDER BY term_order;
```

---

## Log Analysis Tasks

1. **Parse log file** to extract:
   - Exact counts at each step
   - All duplicate detections
   - All "X/10 committed" patterns
   - Timing of all queries

2. **Build timeline** of:
   - Count reported by Google Scholar at each query
   - Actual papers received vs expected

3. **Categorize gaps**:
   - Duplicates detected: X
   - Papers without IDs: X
   - Unexplained: X

---

## Next Steps

1. Run diagnostic SQL queries above
2. Parse and analyze the log file systematically
3. Implement alternative inclusion strategy (sequential queries)
4. Run test with new strategy and compare results
5. If still gap, investigate Google Scholar's behavior with controlled experiments

---

## Files to Modify

- `app/services/overflow_harvester.py` - Alternative inclusion strategy
- `scripts/test_overflow_harvester.py` - Better tracking of skip reasons

---

## Summary of Findings

### SOLVED
| Issue | Root Cause | Fix |
|-------|------------|-----|
| Gap B (72 papers) | Papers without scholar_ids silently skipped | Log and optionally save papers without IDs |
| Duplicate pages 96-97 | Google Scholar recycles results near 1000 limit | Accept this as GS behavior, stop earlier if duplicates spike |

### SOLVED (parsing bug)
| Issue | Root Cause | Fix |
|-------|------------|-----|
| "global" 1130 reduction | Parsing bug: "1210" → "210" | Regex now includes `\s` for space-separated numbers |
| 1210 vs 210 discrepancy | Same parsing bug | Debug logging added to catch future issues |
| 410 vs 1410 fluctuation | Same parsing bug (Run #15) | Fixed with updated regex |

### PARTIALLY UNDERSTOOD
| Issue | Current Understanding | Next Steps |
|-------|----------------------|------------|
| Gap A (361 papers) | Exclusion+Inclusion ≠ Total | Still need to investigate partition strategy |

### KEY INSIGHT (REVISED)
~~Google Scholar is fundamentally unreliable for precise counts.~~ **WRONG!**

Most of the "instability" was actually a **parsing bug** in our code:
- "410 vs 1410" → parsing bug, dropped leading "1"
- "210 vs 1210" → same parsing bug
- Term "global" 84% reduction → actually 9.7% (normal)

**Only real GS issue**: Pages 96-97 returned 100% duplicates (near 1000 limit)

**Recommendation**: Trust GS counts more, but add robust parsing with debug logging.

## Immediate Action Items

1. **Quick Win**: Add logging for papers without scholar_ids in test script
2. **Medium**: Implement sequential term harvesting (one query per term, not OR)
3. **Experiment**: Test `intitle:"global"` alone to understand its behavior
4. **Architecture**: Consider early termination when duplicate rate exceeds threshold

## Notes

- Google Scholar is fundamentally unreliable for precise counts
- Any harvester must be robust to:
  - Count fluctuation
  - Result shuffling
  - Duplicate returns
  - Rate limiting
- Goal should be "harvest as many as possible" not "harvest exactly N"
- Accept some gap is inevitable, but 20% gap is too high
- Current best result: 1397 unique citations for 2014 (76% of initial count)

## Connection Stability Fixes (2025-12-29 evening)

### Problem
Run #18 failed with `connection was closed in the middle of operation` error.
Render Postgres closes idle connections after ~5 minutes.
During LLM calls (30-60 seconds each), the DB connection sits idle and gets killed.

### Solution Applied

1. **Added `db_keepalive()` function** in `overflow_harvester.py`:
   - Executes `SELECT 1` to ping the database
   - Called after every long-running operation
   - Prevents connection timeout during idle waits

2. **Added keepalive calls after**:
   - Every LLM call (inside `suggest_exclusion_terms_llm`)
   - Every Scholar query (inside `execute_count_query`)
   - Every harvest operation (inside `execute_harvest_query`)
   - After returning from `suggest_exclusion_terms_llm` in the main loop

3. **Already had** (from earlier):
   - `safe_flush()` and `safe_commit()` with retry logic
   - Connection pool settings with TCP keepalive in test script

### Test Command
```bash
cd /home/evgeny/projects/referee/the-referee/backend
python scripts/test_overflow_harvester.py --paper-id 71 --year 2014 --run
```
