import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../lib/api'

/**
 * Citations View - Shows extracted citations for a paper
 * With language filtering and advanced sorting
 */
export default function Citations({ paper, onBack }) {
  const [sortBy, setSortBy] = useState('citations') // intersection, citations, year
  const [minIntersection, setMinIntersection] = useState(1)
  const [selectedLanguage, setSelectedLanguage] = useState(null) // null = all languages

  const { data: citations, isLoading } = useQuery({
    queryKey: ['citations', paper.id],
    queryFn: () => api.getPaperCitations(paper.id),
  })

  // Extract unique languages from citations
  const languageGroups = useMemo(() => {
    if (!citations) return {}
    return citations.reduce((acc, c) => {
      const lang = c.edition_language || 'Unknown'
      acc[lang] = (acc[lang] || 0) + 1
      return acc
    }, {})
  }, [citations])

  // Sort languages by count
  const sortedLanguages = useMemo(() => {
    return Object.entries(languageGroups)
      .sort((a, b) => b[1] - a[1])
      .map(([lang, count]) => ({ lang, count }))
  }, [languageGroups])

  // Filter and sort citations
  const filteredCitations = useMemo(() => {
    if (!citations) return []
    return citations
      .filter(c => c.intersection_count >= minIntersection)
      .filter(c => !selectedLanguage || (c.edition_language || 'Unknown') === selectedLanguage)
      .sort((a, b) => {
        if (sortBy === 'intersection') return b.intersection_count - a.intersection_count
        if (sortBy === 'citations') return (b.citation_count || 0) - (a.citation_count || 0)
        if (sortBy === 'year') return (b.year || 0) - (a.year || 0)
        return 0
      })
  }, [citations, minIntersection, selectedLanguage, sortBy])

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
          {/* Language Filter */}
          {sortedLanguages.length > 1 && (
            <div className="language-filter">
              <span className="label">Filter by Edition Language:</span>
              <div className="language-buttons">
                <button
                  className={`language-btn ${selectedLanguage === null ? 'active' : ''}`}
                  onClick={() => setSelectedLanguage(null)}
                >
                  All ({citations.length})
                </button>
                {sortedLanguages.map(({ lang, count }) => (
                  <button
                    key={lang}
                    className={`language-btn ${selectedLanguage === lang ? 'active' : ''}`}
                    onClick={() => setSelectedLanguage(lang)}
                  >
                    {lang} ({count})
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
            {selectedLanguage && ` • Language: ${selectedLanguage}`}
            {minIntersection > 1 && ` • Citing ${minIntersection}+ editions`}
          </div>

          {/* Citations Table */}
          <table className="citations-table">
            <thead>
              <tr>
                <th className="col-cross">Cross</th>
                <th className="col-title">Title / Authors</th>
                <th className="col-year">Year</th>
                <th className="col-lang">Lang</th>
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

  // Language flag emoji based on language
  const getLangEmoji = (lang) => {
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
      <td className="col-lang">
        <span className="lang-badge" title={citation.edition_language || 'Unknown'}>
          {getLangEmoji(citation.edition_language)} {(citation.edition_language || '?').substring(0, 3)}
        </span>
      </td>
      <td className="col-venue">
        <span className="venue-tag" title={citation.venue}>
          {citation.venue ? (citation.venue.length > 30 ? citation.venue.substring(0, 27) + '...' : citation.venue) : '–'}
        </span>
      </td>
      <td className="col-cites">{citation.citation_count?.toLocaleString() || 0}</td>
    </tr>
  )
}
