# The Referee - Terminology & Ontology

This document defines the core concepts and workflow of The Referee citation analysis engine.

## Core Concepts

### Work (aka Seed)

A **Work** is the abstract intellectual work we want to track citations for. This could be:
- A book (e.g., "The Modern Corporation and Private Property")
- An academic paper
- A seminal article

**Lifecycle:**
```
Draft → Resolved → Editions Finalized
```

- **Draft**: Just added, not yet matched to Google Scholar
- **Resolved**: Matched to a Scholar cluster/entry
- **Editions Finalized**: User has confirmed which editions to track

### Edition

An **Edition** is a specific publication of a Work. A single Work may have multiple editions:
- Different years (1932 first edition, 1968 revised edition)
- Different languages (English original, French translation, Japanese translation)
- Different publishers

#### Canonical Edition
The **Canonical Edition** is the original publication in its original language. For "The Modern Corporation and Private Property" (1932, English), the canonical edition is the 1932 English publication.

#### Foreign Editions
**Foreign Editions** are translations or foreign-language versions. For a French work like Foucault's "Surveiller et punir", the French edition is canonical, and the English "Discipline and Punish" is a foreign edition.

> **Note**: For 99% of academic papers, there's only one edition (the original publication). The edition system primarily matters for influential books that have been translated.

### Edition Candidate

An **Edition Candidate** is a potential edition discovered via Google Scholar search. During edition discovery, the system searches for the work across multiple languages and returns candidates.

**Confidence levels:**
- **High**: Strong match (exact title, same author, significant citations)
- **Uncertain**: Possible match (similar title, needs human review)
- **Rejected**: Not a match (different work entirely)

**User actions on candidates:**
| Action | When to use | Result |
|--------|-------------|--------|
| **Confirm** | This is an edition of the work | Becomes a Confirmed Edition |
| **Exclude** | This is unrelated (most common) | Hidden from view |
| **Add as New Seed** | Unrelated but interesting work | Creates a new Work to track separately |

### Confirmed Edition (Selected Edition)

A **Confirmed Edition** is an edition candidate that the user has verified belongs to this Work. Only confirmed editions are harvested for citations.

### Citing Paper (Citation)

A **Citing Paper** is a paper that cites one of our confirmed editions. These are harvested from Google Scholar's "Cited by" feature.

**User actions on citations:**
| Action | When to use | Result |
|--------|-------------|--------|
| **View** | Browse the citing paper | Opens Scholar link |
| **Add as New Seed** | This citing paper is worth tracking | Creates a new Work |

> **Note**: Most citations are just browsed, not converted to seeds. Creating a new seed is the exception for particularly influential or interesting citing papers.

## Organizational Hierarchy

```
Collection
└── Dossier
    └── Work (Seed)
        └── Confirmed Editions
            └── Citing Papers
```

### Collection
Top-level organizational unit. Examples: "Management Theory", "Philosophy of Science"

### Dossier
A folder within a Collection for grouping related Works. Examples: "Berle & Means Cluster", "Frankfurt School"

### Work
The intellectual work being tracked (as defined above).

## Workflow Summary

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. ADD WORK                                                            │
│     User enters: Title, Author, Year                                    │
│     System resolves against Google Scholar                              │
│     Work status: Draft → Resolved                                       │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. DISCOVER EDITIONS                                                   │
│     System searches Scholar in multiple languages                       │
│     Returns edition candidates with confidence scores                   │
│                                                                         │
│     User reviews each candidate:                                        │
│     ├── ✓ Confirm → becomes Confirmed Edition                          │
│     ├── ✗ Exclude → hidden (most common for mismatches)                │
│     └── 🌱 Add as Seed → creates new Work (rare, for interesting finds)│
│                                                                         │
│     User can mark one edition as CANONICAL (original language)          │
│     Work status: Resolved → Editions Finalized                          │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. HARVEST CITATIONS                                                   │
│     System extracts citing papers from Confirmed Editions               │
│     Harvests year-by-year to capture full citation history              │
│                                                                         │
│     User browses citing papers:                                         │
│     ├── Filter by edition, year, cross-citation count                  │
│     ├── Sort by citation count, year                                   │
│     └── 🌱 Add as Seed → creates new Work (rare, for key papers)       │
└─────────────────────────────────────────────────────────────────────────┘
```

## Cross-Citations

When a paper cites multiple editions of the same Work, it has a **cross-citation count** > 1. These are often the most important citing papers as they engage with the Work across different contexts (e.g., citing both the English and German editions).

## Glossary

| Term | Definition |
|------|------------|
| Work | The abstract intellectual work being tracked |
| Seed | Synonym for Work (emphasizes it's the starting point for citation tracking) |
| Edition | A specific publication of a Work |
| Canonical Edition | The original publication in its original language |
| Foreign Edition | A translation or foreign-language version |
| Edition Candidate | A potential edition found via Scholar search |
| Confirmed Edition | An edition verified to belong to this Work |
| Citing Paper | A paper that cites one of our Confirmed Editions |
| Cross-citation | A paper that cites multiple editions of the same Work |
| Dossier | Organizational folder within a Collection |
| Collection | Top-level organizational unit |
| Harvest | The process of extracting citing papers from Scholar |

---

*Last updated: 2024-12-22*
