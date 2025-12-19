import { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { api } from '../lib/api'

/**
 * Citations View - Shows extracted citations for a paper
 * With edition filtering (by specific edition, not just language)
 */
export default function Citations({ paper, onBack }) {
  const [searchParams, setSearchParams] = useSearchParams()
  const [sortBy, setSortBy] = useState('citations') // intersection, citations, year
  const [minIntersection, setMinIntersection] = useState(1)

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

  // Filter and sort citations
  const filteredCitations = useMemo(() => {
    if (!citations) return []
    return citations
      .filter(c => c.intersection_count >= minIntersection)
      .filter(c => selectedEdition === null || c.edition_id === selectedEdition)
      .sort((a, b) => {
        if (sortBy === 'intersection') return b.intersection_count - a.intersection_count
        if (sortBy === 'citations') return (b.citation_count || 0) - (a.citation_count || 0)
        if (sortBy === 'year') return (b.year || 0) - (a.year || 0)
        return 0
      })
  }, [citations, minIntersection, selectedEdition, sortBy])

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
              </tr>
            </thead>
            <tbody>
              {filteredCitations.map(citation => (
                <CitationRow key={citation.id} citation={citation} maxIntersection={maxIntersection} />
              ))}
            </tbody>
          </table>
        </>
      )}
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

function CitationRow({ citation, maxIntersection }) {
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
    </tr>
  )
}
