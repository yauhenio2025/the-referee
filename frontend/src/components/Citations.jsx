import { useState, useMemo, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { api } from '../lib/api'
import { useToast } from './Toast'
import DossierSelectModal from './DossierSelectModal'

/**
 * Citations View - Shows extracted citations for a paper
 * With edition filtering (by specific edition, not just language)
 */
export default function Citations({ paper, onBack }) {
  const [searchParams, setSearchParams] = useSearchParams()
  const [sortBy, setSortBy] = useState('citations') // intersection, citations, year
  const [minIntersection, setMinIntersection] = useState(1)
  const [showDossierModal, setShowDossierModal] = useState(false)
  const [pendingCitationSeed, setPendingCitationSeed] = useState(null) // Citation to add as seed
  const [searchTitle, setSearchTitle] = useState('') // Search in title
  const [searchAuthor, setSearchAuthor] = useState('') // Search in author
  const [searchVenue, setSearchVenue] = useState('') // Search in venue
  const [selectedAuthor, setSelectedAuthor] = useState(null) // Filter by author facet
  const [selectedVenue, setSelectedVenue] = useState(null) // Filter by venue facet
  const [showAllAuthors, setShowAllAuthors] = useState(false) // Expand authors list
  const [showAllVenues, setShowAllVenues] = useState(false) // Expand venues list
  const toast = useToast()
  const queryClient = useQueryClient()

  // Initialize selected edition from URL param
  const editionParam = searchParams.get('edition')
  const [selectedEdition, setSelectedEdition] = useState(
    editionParam ? parseInt(editionParam) : null
  )

  // Update URL when edition selection changes
  const handleEditionSelect = (editionId) => {
    setSelectedEdition(editionId)
    if (editionId !== null) {
      setSearchParams({ edition: editionId.toString() })
    } else {
      setSearchParams({})
    }
  }

  const { data: citations, isLoading } = useQuery({
    queryKey: ['citations', paper.id],
    queryFn: () => api.getPaperCitations(paper.id),
  })

  // Create seed paper from citation - supports multiple dossiers
  const createSeedFromCitation = useMutation({
    mutationFn: async ({ citation, selections }) => {
      // selections is an array of dossier options from the multi-dossier modal
      // Each selection can be: { dossierId, collectionId } or { createNewDossier, newDossierName, collectionId }

      if (!selections || selections.length === 0) {
        throw new Error('No dossier selections provided')
      }

      // Resolve all dossier IDs (create new ones if needed)
      const resolvedDossierIds = []
      for (const sel of selections) {
        if (sel.createNewDossier && sel.newDossierName && sel.collectionId) {
          // Create the new dossier
          const newDossier = await api.createDossier({
            name: sel.newDossierName,
            collection_id: sel.collectionId,
          })
          resolvedDossierIds.push(newDossier.id)
        } else if (sel.dossierId) {
          resolvedDossierIds.push(sel.dossierId)
        }
      }

      if (resolvedDossierIds.length === 0) {
        throw new Error('No valid dossiers resolved')
      }

      // Create the paper with the first (primary) dossier
      const primaryDossierId = resolvedDossierIds[0]
      const primarySelection = selections[0]
      const collectionId = primarySelection.collectionId || paper.collection_id || null

      const newPaper = await api.createPaper({
        title: citation.title,
        authors: citation.authors,
        year: citation.year,
        venue: citation.venue,
        dossier_id: primaryDossierId,
        collection_id: collectionId,
      })

      // If there are additional dossiers, add the paper to them
      if (resolvedDossierIds.length > 1) {
        await api.addPaperToDossiers(newPaper.id, resolvedDossierIds)
      }

      return { newPaper, dossierCount: resolvedDossierIds.length }
    },
    onSuccess: ({ newPaper, dossierCount }) => {
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['dossiers'])
      const suffix = dossierCount > 1 ? ` (${dossierCount} dossiers)` : ''
      toast.success(`🌱 Now tracking: ${newPaper.title.substring(0, 50)}...${suffix}`)
    },
    onError: (error) => {
      toast.error(`Failed to create seed: ${error.message}`)
    },
  })

  // Handle adding citation as seed
  const handleAddAsSeed = useCallback((citation) => {
    setPendingCitationSeed(citation)
    setShowDossierModal(true)
  }, [])

  // Handle dossier selection from modal (now receives array of selections)
  const handleDossierSelected = useCallback((selections) => {
    if (pendingCitationSeed && selections && selections.length > 0) {
      createSeedFromCitation.mutate({
        citation: pendingCitationSeed,
        selections,
      })
    }
    setPendingCitationSeed(null)
  }, [pendingCitationSeed, createSeedFromCitation])

  // Extract unique editions from citations (by edition_id)
  const editionGroups = useMemo(() => {
    if (!citations) return []
    const groups = {}
    citations.forEach(c => {
      const editionId = c.edition_id || 'unknown'
      if (!groups[editionId]) {
        groups[editionId] = {
          id: c.edition_id,
          title: c.edition_title || 'Unknown Edition',
          language: c.edition_language || 'Unknown',
          count: 0
        }
      }
      groups[editionId].count++
    })
    // Sort by count descending
    return Object.values(groups).sort((a, b) => b.count - a.count)
  }, [citations])

  // Extract top authors with counts
  // Author string format: "MC Jensen, WH Meckling - Corporate governance, 2019 - taylorfrancis.com"
  // Or: "A Smith, B Jones, C Lee - Journal name, 2020 - publisher.com"
  const topAuthors = useMemo(() => {
    if (!citations) return []
    const authorCounts = {}
    citations.forEach(c => {
      if (!c.authors) return
      // First split on " - " to separate authors from venue/year/source
      const authorsPart = c.authors.split(' - ')[0]
      if (!authorsPart) return

      // Split authors on ", " but be careful with initials like "MC Jensen"
      // Authors are typically: "FirstInitial LastName" or "F LastName"
      const authorList = authorsPart.split(/,\s*/)

      authorList.forEach(author => {
        // Clean up the author name
        let cleaned = author.trim()
        // Skip if it looks like a year (4 digits)
        if (/^\d{4}$/.test(cleaned)) return
        // Skip if too short
        if (cleaned.length < 3) return
        // Skip if it contains URL patterns
        if (cleaned.includes('.com') || cleaned.includes('.org') || cleaned.includes('.edu')) return
        // Skip common non-author patterns
        if (/^(vol|no|pp|ed|eds|et al)\.?$/i.test(cleaned)) return

        // Normalize: capitalize properly
        if (cleaned.length > 2) {
          authorCounts[cleaned] = (authorCounts[cleaned] || 0) + 1
        }
      })
    })
    return Object.entries(authorCounts)
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count)
  }, [citations])

  // Extract top venues with counts
  const topVenues = useMemo(() => {
    if (!citations) return []
    const venueCounts = {}
    citations.forEach(c => {
      if (!c.venue) return
      // Normalize venue name (lowercase for grouping, keep original for display)
      const normalized = c.venue.toLowerCase().trim()
      if (!venueCounts[normalized]) {
        venueCounts[normalized] = { name: c.venue, count: 0 }
      }
      venueCounts[normalized].count++
    })
    return Object.values(venueCounts)
      .sort((a, b) => b.count - a.count)
  }, [citations])

  // Filter and sort citations
  const filteredCitations = useMemo(() => {
    if (!citations) return []
    const titleSearch = searchTitle.toLowerCase().trim()
    const authorSearch = searchAuthor.toLowerCase().trim()
    const venueSearch = searchVenue.toLowerCase().trim()

    return citations
      .filter(c => c.intersection_count >= minIntersection)
      .filter(c => selectedEdition === null || c.edition_id === selectedEdition)
      // Title search
      .filter(c => {
        if (!titleSearch) return true
        return (c.title || '').toLowerCase().includes(titleSearch)
      })
      // Author search (free text)
      .filter(c => {
        if (!authorSearch) return true
        return (c.authors || '').toLowerCase().includes(authorSearch)
      })
      // Venue search (free text)
      .filter(c => {
        if (!venueSearch) return true
        return (c.venue || '').toLowerCase().includes(venueSearch)
      })
      // Author facet filter (exact match on parsed author)
      .filter(c => {
        if (!selectedAuthor) return true
        return (c.authors || '').toLowerCase().includes(selectedAuthor.toLowerCase())
      })
      // Venue facet filter
      .filter(c => {
        if (!selectedVenue) return true
        return (c.venue || '').toLowerCase() === selectedVenue.toLowerCase()
      })
      .sort((a, b) => {
        if (sortBy === 'intersection') return b.intersection_count - a.intersection_count
        if (sortBy === 'citations') return (b.citation_count || 0) - (a.citation_count || 0)
        if (sortBy === 'year') return (b.year || 0) - (a.year || 0)
        return 0
      })
  }, [citations, minIntersection, selectedEdition, sortBy, searchTitle, searchAuthor, searchVenue, selectedAuthor, selectedVenue])

  // Group by intersection count for summary
  const intersectionGroups = useMemo(() => {
    if (!citations) return {}
    return citations.reduce((acc, c) => {
      const count = c.intersection_count || 1
      acc[count] = (acc[count] || 0) + 1
      return acc
    }, {})
  }, [citations])

  const maxIntersection = Math.max(...Object.keys(intersectionGroups).map(Number), 1)

  // Get selected edition info for display
  const selectedEditionInfo = editionGroups.find(e => e.id === selectedEdition)

  return (
    <div className="citations-view">
      <header className="citations-header">
        <button onClick={onBack} className="btn-text">← Back</button>
        <div className="citations-title">
          <h2>Citations for: {paper.title}</h2>
          <span className="meta">{citations?.length || 0} total citations extracted</span>
        </div>
      </header>

      {isLoading ? (
        <div className="loading">Loading citations...</div>
      ) : !citations?.length ? (
        <div className="empty">
          <p>No citations extracted yet.</p>
          <p>Go to Editions tab and click "Extract Citations" to harvest citing papers.</p>
        </div>
      ) : (
        <>
          {/* Edition Filter */}
          {editionGroups.length > 0 && (
            <div className="edition-filter">
              <span className="label">Filter by Edition:</span>
              <div className="edition-buttons">
                <button
                  className={`edition-btn ${selectedEdition === null ? 'active' : ''}`}
                  onClick={() => handleEditionSelect(null)}
                >
                  All Editions ({citations.length})
                </button>
                {editionGroups.map(edition => (
                  <button
                    key={edition.id || 'unknown'}
                    className={`edition-btn ${selectedEdition === edition.id ? 'active' : ''}`}
                    onClick={() => handleEditionSelect(edition.id)}
                    title={edition.title}
                  >
                    <span className="edition-flag">{getLangEmoji(edition.language)}</span>
                    <span className="edition-info">
                      <span className="edition-lang">{edition.language}</span>
                      <span className="edition-title">{truncate(edition.title, 40)}</span>
                    </span>
                    <span className="edition-count">{edition.count}</span>
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Search and Facets */}
          <div className="citations-search-facets">
            {/* Search Fields */}
            <div className="search-fields">
              <div className="search-field">
                <label>Title</label>
                <input
                  type="text"
                  placeholder="Search in title..."
                  value={searchTitle}
                  onChange={e => setSearchTitle(e.target.value)}
                />
                {searchTitle && (
                  <button className="field-clear" onClick={() => setSearchTitle('')}>×</button>
                )}
              </div>
              <div className="search-field">
                <label>Author</label>
                <input
                  type="text"
                  placeholder="Search by author..."
                  value={searchAuthor}
                  onChange={e => setSearchAuthor(e.target.value)}
                />
                {searchAuthor && (
                  <button className="field-clear" onClick={() => setSearchAuthor('')}>×</button>
                )}
              </div>
              <div className="search-field">
                <label>Venue</label>
                <input
                  type="text"
                  placeholder="Search by venue..."
                  value={searchVenue}
                  onChange={e => setSearchVenue(e.target.value)}
                />
                {searchVenue && (
                  <button className="field-clear" onClick={() => setSearchVenue('')}>×</button>
                )}
              </div>
            </div>

            {/* Facets Row */}
            <div className="facets-row">
              {/* Top Authors Facet */}
              <div className="facet">
                <div className="facet-header">
                  <span className="facet-title">Top Authors</span>
                  {selectedAuthor && (
                    <button className="facet-clear" onClick={() => setSelectedAuthor(null)}>Clear</button>
                  )}
                </div>
                <div className="facet-items">
                  {(showAllAuthors ? topAuthors : topAuthors.slice(0, 8)).map(author => (
                    <button
                      key={author.name}
                      className={`facet-item ${selectedAuthor === author.name ? 'active' : ''}`}
                      onClick={() => setSelectedAuthor(selectedAuthor === author.name ? null : author.name)}
                      title={author.name}
                    >
                      <span className="facet-name">{truncate(author.name, 25)}</span>
                      <span className="facet-count">{author.count}</span>
                    </button>
                  ))}
                  {topAuthors.length > 8 && (
                    <button
                      className="facet-toggle"
                      onClick={() => setShowAllAuthors(!showAllAuthors)}
                    >
                      {showAllAuthors ? 'Show less' : `+${topAuthors.length - 8} more`}
                    </button>
                  )}
                </div>
              </div>

              {/* Top Venues Facet */}
              <div className="facet">
                <div className="facet-header">
                  <span className="facet-title">Top Venues</span>
                  {selectedVenue && (
                    <button className="facet-clear" onClick={() => setSelectedVenue(null)}>Clear</button>
                  )}
                </div>
                <div className="facet-items">
                  {(showAllVenues ? topVenues : topVenues.slice(0, 8)).map(venue => (
                    <button
                      key={venue.name}
                      className={`facet-item ${selectedVenue === venue.name ? 'active' : ''}`}
                      onClick={() => setSelectedVenue(selectedVenue === venue.name ? null : venue.name)}
                      title={venue.name}
                    >
                      <span className="facet-name">{truncate(venue.name, 30)}</span>
                      <span className="facet-count">{venue.count}</span>
                    </button>
                  ))}
                  {topVenues.length > 8 && (
                    <button
                      className="facet-toggle"
                      onClick={() => setShowAllVenues(!showAllVenues)}
                    >
                      {showAllVenues ? 'Show less' : `+${topVenues.length - 8} more`}
                    </button>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* Summary Stats */}
          <div className="citations-summary">
            <div className="intersection-bars">
              <span className="label">Cross-citations:</span>
              {Object.entries(intersectionGroups)
                .sort((a, b) => Number(b[0]) - Number(a[0]))
                .map(([count, num]) => (
                  <button
                    key={count}
                    className={`intersection-bar ${minIntersection === Number(count) ? 'active' : ''}`}
                    onClick={() => setMinIntersection(Number(count))}
                    title={`${num} papers cite ${count} edition(s)`}
                  >
                    <span className="bar-label">{count}+</span>
                    <span className="bar-count">{num}</span>
                  </button>
                ))}
              <button
                className={`intersection-bar ${minIntersection === 1 ? 'active' : ''}`}
                onClick={() => setMinIntersection(1)}
              >
                All
              </button>
            </div>

            <div className="sort-controls">
              <label>Sort by:</label>
              <select value={sortBy} onChange={e => setSortBy(e.target.value)}>
                <option value="citations">Citation count</option>
                <option value="intersection">Cross-citation count</option>
                <option value="year">Year (newest first)</option>
              </select>
            </div>
          </div>

          {/* Results count */}
          <div className="results-count">
            Showing {filteredCitations.length} of {citations.length} citations
            {selectedEditionInfo && ` • Edition: ${selectedEditionInfo.language} - ${truncate(selectedEditionInfo.title, 30)}`}
            {minIntersection > 1 && ` • Citing ${minIntersection}+ editions`}
            {searchTitle && ` • Title: "${searchTitle}"`}
            {searchAuthor && ` • Author: "${searchAuthor}"`}
            {searchVenue && ` • Venue: "${searchVenue}"`}
            {selectedAuthor && ` • Author facet: ${truncate(selectedAuthor, 20)}`}
            {selectedVenue && ` • Venue facet: ${truncate(selectedVenue, 20)}`}
          </div>

          {/* Citations Table */}
          <table className="citations-table">
            <thead>
              <tr>
                <th className="col-cross">Cross</th>
                <th className="col-title">Title / Authors</th>
                <th className="col-year">Year</th>
                <th className="col-edition">Edition</th>
                <th className="col-venue">Venue</th>
                <th className="col-cites">Cited by</th>
                <th className="col-actions">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredCitations.map(citation => (
                <CitationRow
                  key={citation.id}
                  citation={citation}
                  maxIntersection={maxIntersection}
                  onAddAsSeed={handleAddAsSeed}
                />
              ))}
            </tbody>
          </table>
        </>
      )}

      {/* Dossier Selection Modal */}
      <DossierSelectModal
        isOpen={showDossierModal}
        onClose={() => {
          setShowDossierModal(false)
          setPendingCitationSeed(null)
        }}
        onSelect={handleDossierSelected}
        defaultCollectionId={paper.collection_id}
        defaultDossierId={paper.dossier_id}
        title="Track Citing Paper"
        subtitle="This citing paper is interesting - track its citations as a new seed in:"
      />
    </div>
  )
}

// Helper function to truncate text
function truncate(text, maxLen) {
  if (!text) return ''
  return text.length > maxLen ? text.substring(0, maxLen - 3) + '...' : text
}

// Language flag emoji based on language
function getLangEmoji(lang) {
  const flags = {
    'English': '🇬🇧',
    'Italian': '🇮🇹',
    'Spanish': '🇪🇸',
    'French': '🇫🇷',
    'German': '🇩🇪',
    'Portuguese': '🇵🇹',
    'Russian': '🇷🇺',
    'Chinese': '🇨🇳',
    'Japanese': '🇯🇵',
    'Korean': '🇰🇷',
    'Dutch': '🇳🇱',
    'Polish': '🇵🇱',
    'Turkish': '🇹🇷',
    'Arabic': '🇸🇦',
  }
  return flags[lang] || '🌐'
}

function CitationRow({ citation, maxIntersection, onAddAsSeed }) {
  const crossWidth = (citation.intersection_count / maxIntersection) * 100

  return (
    <tr className={citation.intersection_count > 1 ? 'multi-cross' : ''}>
      <td className="col-cross">
        <div className="cross-indicator">
          <span className="cross-num">{citation.intersection_count}</span>
          <div className="cross-bar" style={{ width: `${crossWidth}%` }} />
        </div>
      </td>
      <td className="col-title">
        <div className="title-cell">
          {citation.link ? (
            <a href={citation.link} target="_blank" rel="noopener noreferrer" title={citation.title}>
              {truncate(citation.title, 100)}
            </a>
          ) : (
            <span title={citation.title}>
              {truncate(citation.title, 100)}
            </span>
          )}
          <span className="authors-line">{citation.authors || 'Unknown'}</span>
        </div>
      </td>
      <td className="col-year">{citation.year || '–'}</td>
      <td className="col-edition">
        <span className="edition-badge" title={citation.edition_title || 'Unknown'}>
          {getLangEmoji(citation.edition_language)}
          <span className="edition-badge-text">
            {truncate(citation.edition_language || '?', 3)}
          </span>
        </span>
      </td>
      <td className="col-venue">
        <span className="venue-tag" title={citation.venue}>
          {citation.venue ? truncate(citation.venue, 30) : '–'}
        </span>
      </td>
      <td className="col-cites">{citation.citation_count?.toLocaleString() || 0}</td>
      <td className="col-actions">
        <button
          className="btn-icon btn-seed"
          onClick={() => onAddAsSeed(citation)}
          title="Track this paper's citations"
        >
          🌱
        </button>
      </td>
    </tr>
  )
}
