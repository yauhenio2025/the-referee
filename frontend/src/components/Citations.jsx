import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../lib/api'

/**
 * Citations View - Shows extracted citations for a paper
 */
export default function Citations({ paper, onBack }) {
  const [sortBy, setSortBy] = useState('intersection') // intersection, citations, year
  const [minIntersection, setMinIntersection] = useState(1)

  const { data: citations, isLoading } = useQuery({
    queryKey: ['citations', paper.id],
    queryFn: () => api.getPaperCitations(paper.id),
  })

  // Sort and filter citations
  const filteredCitations = citations
    ?.filter(c => c.intersection_count >= minIntersection)
    ?.sort((a, b) => {
      if (sortBy === 'intersection') return b.intersection_count - a.intersection_count
      if (sortBy === 'citations') return (b.citation_count || 0) - (a.citation_count || 0)
      if (sortBy === 'year') return (b.year || 0) - (a.year || 0)
      return 0
    }) || []

  // Group by intersection count for summary
  const intersectionGroups = citations?.reduce((acc, c) => {
    const count = c.intersection_count || 1
    acc[count] = (acc[count] || 0) + 1
    return acc
  }, {}) || {}

  const maxIntersection = Math.max(...Object.keys(intersectionGroups).map(Number), 1)

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
                <option value="intersection">Cross-citation count</option>
                <option value="citations">Citation count</option>
                <option value="year">Year (newest first)</option>
              </select>
            </div>
          </div>

          {/* Results count */}
          <div className="results-count">
            Showing {filteredCitations.length} of {citations.length} citations
            {minIntersection > 1 && ` (citing ${minIntersection}+ editions)`}
          </div>

          {/* Citations Table */}
          <table className="citations-table">
            <thead>
              <tr>
                <th className="col-cross">Cross</th>
                <th className="col-title">Title / Authors</th>
                <th className="col-year">Year</th>
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
              {citation.title.length > 100 ? citation.title.substring(0, 97) + '...' : citation.title}
            </a>
          ) : (
            <span title={citation.title}>
              {citation.title.length > 100 ? citation.title.substring(0, 97) + '...' : citation.title}
            </span>
          )}
          <span className="authors-line">{citation.authors || 'Unknown'}</span>
        </div>
      </td>
      <td className="col-year">{citation.year || '–'}</td>
      <td className="col-venue">
        <span className="venue-tag" title={citation.venue}>
          {citation.venue ? (citation.venue.length > 30 ? citation.venue.substring(0, 27) + '...' : citation.venue) : '–'}
        </span>
      </td>
      <td className="col-cites">{citation.citation_count?.toLocaleString() || 0}</td>
    </tr>
  )
}
