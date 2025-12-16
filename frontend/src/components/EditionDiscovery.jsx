import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function EditionDiscovery({ paper, onBack }) {
  const [languageStrategy, setLanguageStrategy] = useState('recommended')
  const [customLanguages, setCustomLanguages] = useState([])
  const [showLanguageModal, setShowLanguageModal] = useState(false)
  const [isLoadingRecs, setIsLoadingRecs] = useState(false)
  const [recommendations, setRecommendations] = useState(null)
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
        // Pre-select recommended languages
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
    mutationFn: () => {
      // Build language list based on strategy
      let langsToUse = customLanguages
      if (languageStrategy === 'english_only') {
        langsToUse = ['english']
      } else if (languageStrategy === 'major_languages') {
        langsToUse = ['english', 'german', 'french', 'spanish', 'portuguese', 'italian', 'russian', 'chinese', 'japanese']
      }
      return api.discoverEditions(paper.id, {
        languageStrategy,
        customLanguages: langsToUse
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries(['editions', paper.id])
      setShowLanguageModal(false)
    },
  })

  const selectEditions = useMutation({
    mutationFn: ({ ids, selected }) => api.selectEditions(ids, selected),
    onSuccess: () => {
      queryClient.invalidateQueries(['editions', paper.id])
    },
  })

  const extractCitations = useMutation({
    mutationFn: () => api.extractCitations(paper.id),
    onSuccess: () => {
      queryClient.invalidateQueries(['jobs'])
    },
  })

  const selectedCount = editions?.filter(e => e.selected).length || 0
  const languageGroups = editions?.reduce((acc, e) => {
    const lang = e.language || 'Unknown'
    acc[lang] = (acc[lang] || 0) + 1
    return acc
  }, {}) || {}

  // Group editions by confidence
  const highConfidence = editions?.filter(e => e.confidence === 'high') || []
  const uncertain = editions?.filter(e => e.confidence === 'uncertain') || []
  const rejected = editions?.filter(e => e.confidence === 'rejected') || []

  const toggleLanguage = (code) => {
    if (customLanguages.includes(code)) {
      setCustomLanguages(customLanguages.filter(c => c !== code))
    } else {
      setCustomLanguages([...customLanguages, code])
    }
  }

  return (
    <div className="edition-discovery">
      <div className="edition-header">
        <button onClick={onBack} className="btn-back">← Back to Papers</button>
        <h2>Editions of: {paper.title}</h2>
        {paper.authors && <p className="paper-meta">{paper.authors} {paper.year && `(${paper.year})`}</p>}
      </div>

      <div className="edition-actions">
        <button onClick={() => setShowLanguageModal(true)} className="btn-primary">
          🔍 Discover Editions
        </button>
        <button
          onClick={() => extractCitations.mutate()}
          disabled={selectedCount === 0 || extractCitations.isPending}
          className="btn-success"
        >
          📊 Extract Citations ({selectedCount} selected)
        </button>
      </div>

      {/* Language selection modal with LLM recommendations */}
      {showLanguageModal && (
        <div className="modal-overlay">
          <div className="modal language-modal">
            <h3>🌍 Select Languages to Search</h3>

            {/* LLM Recommendations Section */}
            {isLoadingRecs ? (
              <div className="llm-recommendations loading">
                <div className="spinner"></div>
                <p>🤖 Getting AI language recommendations...</p>
              </div>
            ) : recommendations ? (
              <div className="llm-recommendations">
                <div className="rec-header">
                  <span className="rec-icon">🤖</span>
                  <span className="rec-title">AI Recommendation</span>
                </div>
                <p className="rec-reasoning">{recommendations.reasoning}</p>
                {recommendations.author_language && (
                  <p className="rec-author-lang">
                    <strong>Author's likely language:</strong> {recommendations.author_language}
                  </p>
                )}
                {recommendations.primary_markets?.length > 0 && (
                  <p className="rec-markets">
                    <strong>Primary markets:</strong> {recommendations.primary_markets.join(', ')}
                  </p>
                )}
                <div className="rec-languages">
                  <strong>Recommended languages:</strong>
                  <div className="rec-lang-chips">
                    {recommendations.recommended?.map(lang => {
                      const langInfo = languages?.languages?.find(l => l.code === lang)
                      return (
                        <span key={lang} className="rec-lang-chip">
                          {langInfo?.icon || '🌐'} {langInfo?.name || lang}
                        </span>
                      )
                    })}
                  </div>
                </div>
              </div>
            ) : null}

            {/* Language Strategy Selection */}
            <div className="language-strategies">
              <h4>Search Strategy</h4>
              <label className={languageStrategy === 'recommended' ? 'selected' : ''}>
                <input
                  type="radio"
                  value="recommended"
                  checked={languageStrategy === 'recommended'}
                  onChange={(e) => setLanguageStrategy(e.target.value)}
                />
                <span className="strategy-name">🤖 Use AI Recommendations</span>
                <span className="strategy-desc">Search in languages recommended by AI based on author and title</span>
              </label>
              <label className={languageStrategy === 'major_languages' ? 'selected' : ''}>
                <input
                  type="radio"
                  value="major_languages"
                  checked={languageStrategy === 'major_languages'}
                  onChange={(e) => setLanguageStrategy(e.target.value)}
                />
                <span className="strategy-name">🌍 Major Languages</span>
                <span className="strategy-desc">EN, DE, FR, ES, PT, IT, RU, ZH, JA</span>
              </label>
              <label className={languageStrategy === 'english_only' ? 'selected' : ''}>
                <input
                  type="radio"
                  value="english_only"
                  checked={languageStrategy === 'english_only'}
                  onChange={(e) => setLanguageStrategy(e.target.value)}
                />
                <span className="strategy-name">🇬🇧 English Only</span>
                <span className="strategy-desc">Only search for English editions</span>
              </label>
              <label className={languageStrategy === 'custom' ? 'selected' : ''}>
                <input
                  type="radio"
                  value="custom"
                  checked={languageStrategy === 'custom'}
                  onChange={(e) => setLanguageStrategy(e.target.value)}
                />
                <span className="strategy-name">✏️ Custom Selection</span>
                <span className="strategy-desc">Choose languages manually below</span>
              </label>
            </div>

            {/* Custom Language Selection */}
            {languageStrategy === 'custom' && (
              <div className="custom-languages">
                <h4>Select Languages ({customLanguages.length} selected)</h4>
                <div className="language-chips">
                  {languages?.languages?.map(lang => (
                    <label
                      key={lang.code}
                      className={`language-chip ${customLanguages.includes(lang.code) ? 'selected' : ''}`}
                    >
                      <input
                        type="checkbox"
                        checked={customLanguages.includes(lang.code)}
                        onChange={() => toggleLanguage(lang.code)}
                      />
                      {lang.icon} {lang.name}
                    </label>
                  ))}
                </div>
              </div>
            )}

            {/* Selected languages preview for recommended strategy */}
            {languageStrategy === 'recommended' && recommendations?.recommended && (
              <div className="selected-preview">
                <h4>Will search in: {recommendations.recommended.length} languages</h4>
                <div className="language-chips">
                  {recommendations.recommended.map(lang => {
                    const langInfo = languages?.languages?.find(l => l.code === lang)
                    return (
                      <span key={lang} className="language-chip selected">
                        {langInfo?.icon || '🌐'} {langInfo?.name || lang}
                      </span>
                    )
                  })}
                </div>
              </div>
            )}

            <div className="modal-actions">
              <button onClick={() => setShowLanguageModal(false)} className="btn-secondary">
                Cancel
              </button>
              <button
                onClick={() => discoverEditions.mutate()}
                disabled={discoverEditions.isPending || (languageStrategy === 'custom' && customLanguages.length === 0)}
                className="btn-primary"
              >
                {discoverEditions.isPending ? '🔄 Discovering...' : '🚀 Start Discovery'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edition stats summary */}
      {editions?.length > 0 && (
        <div className="edition-stats">
          <div className="stat high">
            <span className="stat-count">{highConfidence.length}</span>
            <span className="stat-label">High Confidence</span>
          </div>
          <div className="stat uncertain">
            <span className="stat-count">{uncertain.length}</span>
            <span className="stat-label">Uncertain</span>
          </div>
          <div className="stat rejected">
            <span className="stat-count">{rejected.length}</span>
            <span className="stat-label">Rejected</span>
          </div>
        </div>
      )}

      {/* Language filter */}
      {Object.keys(languageGroups).length > 0 && (
        <div className="language-filter">
          <span>Filter by language: </span>
          {Object.entries(languageGroups).map(([lang, count]) => (
            <span key={lang} className="lang-badge">
              {lang} ({count})
            </span>
          ))}
        </div>
      )}

      {/* Editions list */}
      {isLoading ? (
        <div className="loading">Loading editions...</div>
      ) : editions?.length === 0 ? (
        <div className="empty">No editions discovered yet. Click "Discover Editions" to start.</div>
      ) : (
        <div className="editions-list">
          <div className="editions-header">
            <label>
              <input
                type="checkbox"
                checked={selectedCount === editions?.length}
                onChange={(e) => {
                  const ids = editions.map(e => e.id)
                  selectEditions.mutate({ ids, selected: e.target.checked })
                }}
              />
              Select All ({editions?.length})
            </label>
          </div>

          {/* High Confidence Editions */}
          {highConfidence.length > 0 && (
            <div className="edition-group">
              <h3 className="group-header high">✓ High Confidence ({highConfidence.length})</h3>
              {highConfidence.map(edition => (
                <EditionCard
                  key={edition.id}
                  edition={edition}
                  onSelect={(selected) => selectEditions.mutate({ ids: [edition.id], selected })}
                />
              ))}
            </div>
          )}

          {/* Uncertain Editions */}
          {uncertain.length > 0 && (
            <div className="edition-group">
              <h3 className="group-header uncertain">? Uncertain ({uncertain.length})</h3>
              {uncertain.map(edition => (
                <EditionCard
                  key={edition.id}
                  edition={edition}
                  onSelect={(selected) => selectEditions.mutate({ ids: [edition.id], selected })}
                />
              ))}
            </div>
          )}

          {/* Rejected Editions */}
          {rejected.length > 0 && (
            <div className="edition-group">
              <h3 className="group-header rejected">✗ Rejected ({rejected.length})</h3>
              {rejected.map(edition => (
                <EditionCard
                  key={edition.id}
                  edition={edition}
                  onSelect={(selected) => selectEditions.mutate({ ids: [edition.id], selected })}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function EditionCard({ edition, onSelect }) {
  return (
    <div className={`edition-card ${edition.selected ? 'selected' : ''} confidence-${edition.confidence}`}>
      <input
        type="checkbox"
        checked={edition.selected}
        onChange={(e) => onSelect(e.target.checked)}
      />
      <div className="edition-info">
        <h4>
          {edition.link ? (
            <a href={edition.link} target="_blank" rel="noopener noreferrer">
              {edition.title}
            </a>
          ) : (
            edition.title
          )}
        </h4>
        <div className="edition-meta">
          {edition.authors && <span className="authors">{edition.authors}</span>}
          {edition.year && <span className="year">({edition.year})</span>}
          {edition.venue && <span className="venue">{edition.venue}</span>}
        </div>
        <div className="edition-badges">
          <span className="citations">📚 {edition.citation_count.toLocaleString()} citations</span>
          <span className={`confidence-badge ${edition.confidence}`}>
            {edition.confidence === 'high' ? '✓' : edition.confidence === 'uncertain' ? '?' : '✗'} {edition.confidence}
          </span>
          {edition.language && <span className="language-badge">{edition.language}</span>}
          {edition.auto_selected && <span className="auto-badge">Auto</span>}
        </div>
        {edition.abstract && (
          <p className="edition-abstract">{edition.abstract.substring(0, 200)}...</p>
        )}
      </div>
    </div>
  )
}
