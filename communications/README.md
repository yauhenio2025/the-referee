# Communications Folder

This folder is used for multi-phase parallel execution sessions.

## How to Use

### 1. Strategizer Session
Run Claude Code and ask it to analyze a task and create a parallel execution plan:
```
"Analyze this spec and create a parallel execution plan in /communications"
```
This creates `MASTER_MEMO.md` with all phases defined.

### 2. Implementor Sessions (run in parallel terminals)
```bash
# Terminal 1
claude
> "You are implementor for Phase 1. Read /communications/MASTER_MEMO.md and implement Phase 1 only."

# Terminal 2
claude
> "You are implementor for Phase 2. Read /communications/MASTER_MEMO.md and implement Phase 2 only."

# Terminal 3
claude
> "You are implementor for Phase 3. Read /communications/MASTER_MEMO.md and implement Phase 3 only."
```

### 3. Reconciler Session (after all implementors finish)
```
claude
> "You are the reconciler. Read all files in /communications/ and wire the phases together."
```

## File Structure

```
communications/
├── README.md                     # This file
├── MASTER_MEMO.md               # Created by strategizer, read by ALL
├── phase-1-handoff.md           # Implementor 1's notes during work
├── phase-1-final-report.md      # Implementor 1's completion report
├── phase-2-handoff.md           # Implementor 2's notes during work
├── phase-2-final-report.md      # Implementor 2's completion report
├── phase-3-handoff.md           # Implementor 3's notes during work
├── phase-3-final-report.md      # Implementor 3's completion report
└── reconciler-report.md         # Final integration report
```

## Rules

1. **Implementors MUST write final reports** - no exceptions
2. **Implementors stay in their lane** - only modify files in their phase scope
3. **All communication via /communications folder** - not via CLI
4. **Reconciler runs AFTER all implementors complete**
5. **MASTER_MEMO is read-only for implementors** - only strategizer modifies it
