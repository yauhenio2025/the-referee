import { useState, useEffect, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

/**
 * Edition Discovery - Tufte-inspired compact data view
 *
 * Design principles:
 * - High data-ink ratio: maximize information per pixel
 * - Small multiples: compact table rows, not cards
 * - Quick batch actions: one-click select by confidence/language
 * - Minimal chrome: no decorative elements
 */
export default function EditionDiscovery({ paper, onBack }) {
  const [languageStrategy, setLanguageStrategy] = useState('recommended')
  const [customLanguages, setCustomLanguages] = useState([])
  const [showLanguageModal, setShowLanguageModal] = useState(false)
  const [isLoadingRecs, setIsLoadingRecs] = useState(false)
  const [recommendations, setRecommendations] = useState(null)
  const [discoveryProgress, setDiscoveryProgress] = useState(null)
  const [expandedGroups, setExpandedGroups] = useState({ high: true, uncertain: true, rejected: false })
  const [languageFilter, setLanguageFilter] = useState(null)
  const queryClient = useQueryClient()

  const { data: editions, isLoading } = useQuery({
    queryKey: ['editions', paper.id],
    queryFn: () => api.getPaperEditions(paper.id),
  })

  const { data: languages } = useQuery({
    queryKey: ['languages'],
    queryFn: () => api.getAvailableLanguages(),
  })

  // Fetch LLM recommendations when modal opens
  useEffect(() => {
    if (showLanguageModal && !recommendations && !isLoadingRecs) {
      setIsLoadingRecs(true)
      api.recommendLanguages({
        title: paper.title,
        author: paper.authors,
        year: paper.year,
      }).then(recs => {
        setRecommendations(recs)
        if (recs?.recommended) {
          setCustomLanguages(recs.recommended)
        }
        setIsLoadingRecs(false)
      }).catch(err => {
        console.error('Failed to get language recommendations:', err)
        setIsLoadingRecs(false)
      })
    }
  }, [showLanguageModal, recommendations, isLoadingRecs, paper])

  const discoverEditions = useMutation({
    mutationFn: async () => {
      let langsToUse = customLanguages
      if (languageStrategy === 'english_only') {
        langsToUse = ['english']
      } else if (languageStrategy === 'major_languages') {
        langsToUse = ['english', 'german', 'french', 'spanish', 'portuguese', 'italian', 'russian', 'chinese', 'japanese']
      } else if (languageStrategy === 'recommended' && recommendations?.recommended) {
        langsToUse = recommendations.recommended
      }

      setShowLanguageModal(false)
      setDiscoveryProgress({ stage: 'searching', message: 'Generating queries...', progress: 10 })

      const progressInterval = setInterval(() => {
        setDiscoveryProgress(prev => {
          if (!prev || prev.progress >= 90) return prev
          const newProgress = Math.min(prev.progress + Math.random() * 15, 90)
          const messages = ['Searching Scholar...', 'Analyzing results...', 'Identifying editions...', 'Classifying...']
          return { ...prev, progress: newProgress, message: messages[Math.floor(newProgress / 25)] }
        })
      }, 1500)

      try {
        const result = await api.discoverEditions(paper.id, { languageStrategy, customLanguages: langsToUse })
        clearInterval(progressInterval)
        setDiscoveryProgress({ stage: 'complete', message: `Found ${result.total_found} editions`, progress: 100 })
        setTimeout(() => setDiscoveryProgress(null), 2000)
        return result
      } catch (error) {
        clearInterval(progressInterval)
        setDiscoveryProgress(null)
        throw error
      }
    },
    onSuccess: () => queryClient.invalidateQueries(['editions', paper.id]),
  })

  const selectEditions = useMutation({
    mutationFn: ({ ids, selected }) => api.selectEditions(ids, selected),
    onSuccess: () => queryClient.invalidateQueries(['editions', paper.id]),
  })

  const extractCitations = useMutation({
    mutationFn: () => api.extractCitations(paper.id),
    onSuccess: () => queryClient.invalidateQueries(['jobs']),
  })

  // Computed data
  const { highConfidence, uncertain, rejected, languageGroups, selectedCount, totalCitations } = useMemo(() => {
    if (!editions) return { highConfidence: [], uncertain: [], rejected: [], languageGroups: {}, selectedCount: 0, totalCitations: 0 }

    const filtered = languageFilter ? editions.filter(e => e.language === languageFilter) : editions

    return {
      highConfidence: filtered.filter(e => e.confidence === 'high'),
      uncertain: filtered.filter(e => e.confidence === 'uncertain'),
      rejected: filtered.filter(e => e.confidence === 'rejected'),
      languageGroups: editions.reduce((acc, e) => {
        const lang = e.language || 'Unknown'
        acc[lang] = (acc[lang] || 0) + 1
        return acc
      }, {}),
      selectedCount: editions.filter(e => e.selected).length,
      totalCitations: editions.filter(e => e.selected).reduce((sum, e) => sum + (e.citation_count || 0), 0),
    }
  }, [editions, languageFilter])

  // Batch actions
  const selectByConfidence = (confidence) => {
    const ids = editions.filter(e => e.confidence === confidence).map(e => e.id)
    if (ids.length) selectEditions.mutate({ ids, selected: true })
  }

  const deselectByConfidence = (confidence) => {
    const ids = editions.filter(e => e.confidence === confidence).map(e => e.id)
    if (ids.length) selectEditions.mutate({ ids, selected: false })
  }

  const selectByLanguage = (lang) => {
    const ids = editions.filter(e => e.language === lang).map(e => e.id)
    if (ids.length) selectEditions.mutate({ ids, selected: true })
  }

  const selectAll = () => {
    const ids = editions.map(e => e.id)
    selectEditions.mutate({ ids, selected: true })
  }

  const deselectAll = () => {
    const ids = editions.map(e => e.id)
    selectEditions.mutate({ ids, selected: false })
  }

  const toggleGroup = (group) => {
    setExpandedGroups(prev => ({ ...prev, [group]: !prev[group] }))
  }

  const toggleLanguage = (code) => {
    if (customLanguages.includes(code)) {
      setCustomLanguages(customLanguages.filter(c => c !== code))
    } else {
      setCustomLanguages([...customLanguages, code])
    }
  }

  return (
    <div className="edition-discovery tufte">
      {/* Compact Header */}
      <header className="ed-header">
        <button onClick={onBack} className="btn-text">← Papers</button>
        <div className="ed-title">
          <h2>{paper.title}</h2>
          <span className="meta">{paper.authors} {paper.year && `(${paper.year})`}</span>
        </div>
      </header>

      {/* Action Bar */}
      <div className="ed-actions">
        <button onClick={() => setShowLanguageModal(true)} disabled={discoverEditions.isPending} className="btn-primary">
          Discover Editions
        </button>
        <button
          onClick={() => extractCitations.mutate()}
          disabled={selectedCount === 0 || extractCitations.isPending}
          className="btn-success"
        >
          Extract Citations ({selectedCount} selected, ~{totalCitations.toLocaleString()} citing papers)
        </button>
      </div>

      {/* Progress */}
      {discoveryProgress && (
        <div className="ed-progress">
          <div className="progress-bar" style={{ width: `${discoveryProgress.progress}%` }} />
          <span>{discoveryProgress.message}</span>
        </div>
      )}

      {/* Stats + Batch Actions */}
      {editions?.length > 0 && (
        <div className="ed-toolbar">
          <div className="stats-row">
            <span className="stat" onClick={() => selectByConfidence('high')} title="Click to select all">
              <strong>{highConfidence.length}</strong> high
            </span>
            <span className="stat uncertain" onClick={() => selectByConfidence('uncertain')} title="Click to select all">
              <strong>{uncertain.length}</strong> uncertain
            </span>
            <span className="stat rejected">
              <strong>{rejected.length}</strong> rejected
            </span>
            <span className="stat-sep">|</span>
            <span className="stat selected">
              <strong>{selectedCount}</strong>/{editions.length} selected
            </span>
          </div>

          <div className="batch-actions">
            <button onClick={selectAll} className="btn-sm">Select All</button>
            <button onClick={deselectAll} className="btn-sm">Clear</button>
            <button onClick={() => selectByConfidence('high')} className="btn-sm btn-high">+ High</button>
            <button onClick={() => deselectByConfidence('uncertain')} className="btn-sm">− Uncertain</button>
          </div>

          {/* Language chips - click to filter, double-click to select */}
          <div className="lang-chips">
            <span className="chip-label">Languages:</span>
            {Object.entries(languageGroups).map(([lang, count]) => (
              <button
                key={lang}
                className={`lang-chip ${languageFilter === lang ? 'active' : ''}`}
                onClick={() => setLanguageFilter(languageFilter === lang ? null : lang)}
                onDoubleClick={() => selectByLanguage(lang)}
                title="Click to filter, double-click to select all"
              >
                {lang} <span className="count">{count}</span>
              </button>
            ))}
            {languageFilter && (
              <button className="lang-chip clear" onClick={() => setLanguageFilter(null)}>
                × Clear filter
              </button>
            )}
          </div>
        </div>
      )}

      {/* Editions Table */}
      {isLoading ? (
        <div className="loading">Loading editions...</div>
      ) : editions?.length === 0 ? (
        <div className="empty">No editions yet. Click "Discover Editions" to search.</div>
      ) : (
        <div className="ed-table">
          {/* High Confidence */}
          {highConfidence.length > 0 && (
            <EditionGroup
              title="High Confidence"
              editions={highConfidence}
              expanded={expandedGroups.high}
              onToggle={() => toggleGroup('high')}
              onSelect={(id, selected) => selectEditions.mutate({ ids: [id], selected })}
              onSelectAll={() => selectByConfidence('high')}
              className="group-high"
            />
          )}

          {/* Uncertain */}
          {uncertain.length > 0 && (
            <EditionGroup
              title="Uncertain"
              editions={uncertain}
              expanded={expandedGroups.uncertain}
              onToggle={() => toggleGroup('uncertain')}
              onSelect={(id, selected) => selectEditions.mutate({ ids: [id], selected })}
              onSelectAll={() => selectByConfidence('uncertain')}
              className="group-uncertain"
            />
          )}

          {/* Rejected */}
          {rejected.length > 0 && (
            <EditionGroup
              title="Rejected"
              editions={rejected}
              expanded={expandedGroups.rejected}
              onToggle={() => toggleGroup('rejected')}
              onSelect={(id, selected) => selectEditions.mutate({ ids: [id], selected })}
              onSelectAll={() => selectByConfidence('rejected')}
              className="group-rejected"
            />
          )}
        </div>
      )}

      {/* Language Modal */}
      {showLanguageModal && (
        <div className="modal-overlay" onClick={() => setShowLanguageModal(false)}>
          <div className="modal compact" onClick={e => e.stopPropagation()}>
            <h3>Search Languages</h3>

            {isLoadingRecs ? (
              <div className="loading-rec">Getting AI recommendations...</div>
            ) : recommendations && (
              <div className="ai-rec">
                <strong>AI suggests:</strong> {recommendations.recommended?.join(', ')}
                <p className="rec-reason">{recommendations.reasoning}</p>
              </div>
            )}

            <div className="strategy-options">
              {[
                { value: 'recommended', label: 'AI Recommended', desc: 'Based on author/title' },
                { value: 'major_languages', label: 'Major Languages', desc: 'EN, DE, FR, ES, PT, IT, RU, ZH, JA' },
                { value: 'english_only', label: 'English Only', desc: 'Fast, limited coverage' },
                { value: 'custom', label: 'Custom', desc: 'Choose below' },
              ].map(opt => (
                <label key={opt.value} className={languageStrategy === opt.value ? 'selected' : ''}>
                  <input
                    type="radio"
                    value={opt.value}
                    checked={languageStrategy === opt.value}
                    onChange={e => setLanguageStrategy(e.target.value)}
                  />
                  <span className="opt-label">{opt.label}</span>
                  <span className="opt-desc">{opt.desc}</span>
                </label>
              ))}
            </div>

            {languageStrategy === 'custom' && (
              <div className="custom-langs">
                {languages?.languages?.map(lang => (
                  <label key={lang.code} className={customLanguages.includes(lang.code) ? 'selected' : ''}>
                    <input
                      type="checkbox"
                      checked={customLanguages.includes(lang.code)}
                      onChange={() => toggleLanguage(lang.code)}
                    />
                    {lang.icon} {lang.name}
                  </label>
                ))}
              </div>
            )}

            <div className="modal-footer">
              <button onClick={() => setShowLanguageModal(false)}>Cancel</button>
              <button
                onClick={() => discoverEditions.mutate()}
                disabled={languageStrategy === 'custom' && customLanguages.length === 0}
                className="btn-primary"
              >
                Start Discovery
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

/**
 * Edition Group - collapsible section with table rows
 */
function EditionGroup({ title, editions, expanded, onToggle, onSelect, onSelectAll, className }) {
  const selectedCount = editions.filter(e => e.selected).length
  const totalCitations = editions.reduce((sum, e) => sum + (e.citation_count || 0), 0)

  return (
    <div className={`ed-group ${className}`}>
      <div className="group-header" onClick={onToggle}>
        <span className="toggle">{expanded ? '▼' : '▶'}</span>
        <span className="group-title">{title}</span>
        <span className="group-stats">
          {selectedCount}/{editions.length} selected · {totalCitations.toLocaleString()} citations
        </span>
        <button className="btn-xs" onClick={(e) => { e.stopPropagation(); onSelectAll(); }}>
          Select all
        </button>
      </div>

      {expanded && (
        <table className="edition-table">
          <thead>
            <tr>
              <th className="col-check"></th>
              <th className="col-title">Title / Authors</th>
              <th className="col-year">Year</th>
              <th className="col-lang">Lang</th>
              <th className="col-cites">Citations</th>
            </tr>
          </thead>
          <tbody>
            {editions.map(ed => (
              <EditionRow key={ed.id} edition={ed} onSelect={onSelect} />
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

/**
 * Edition Row - single compact row
 */
function EditionRow({ edition, onSelect }) {
  const maxCites = 5000 // for bar scaling
  const barWidth = Math.min(100, (edition.citation_count / maxCites) * 100)

  return (
    <tr className={edition.selected ? 'selected' : ''}>
      <td className="col-check">
        <input
          type="checkbox"
          checked={edition.selected}
          onChange={e => onSelect(edition.id, e.target.checked)}
        />
      </td>
      <td className="col-title">
        <div className="title-cell">
          {edition.link ? (
            <a href={edition.link} target="_blank" rel="noopener noreferrer" title={edition.title}>
              {edition.title.length > 80 ? edition.title.substring(0, 77) + '...' : edition.title}
            </a>
          ) : (
            <span title={edition.title}>
              {edition.title.length > 80 ? edition.title.substring(0, 77) + '...' : edition.title}
            </span>
          )}
          <span className="authors-line">{edition.authors || 'Unknown'}</span>
        </div>
      </td>
      <td className="col-year">{edition.year || '–'}</td>
      <td className="col-lang">
        <span className="lang-tag">{edition.language?.substring(0, 3) || '?'}</span>
      </td>
      <td className="col-cites">
        <div className="cite-cell">
          <span className="cite-num">{edition.citation_count?.toLocaleString() || 0}</span>
          <div className="cite-bar" style={{ width: `${barWidth}%` }} />
        </div>
      </td>
    </tr>
  )
}
