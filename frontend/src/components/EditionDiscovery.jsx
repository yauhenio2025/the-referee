import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function EditionDiscovery({ paper, onBack }) {
  const [languageStrategy, setLanguageStrategy] = useState('major_languages')
  const [customLanguages, setCustomLanguages] = useState([])
  const [showLanguageModal, setShowLanguageModal] = useState(false)
  const queryClient = useQueryClient()

  const { data: editions, isLoading } = useQuery({
    queryKey: ['editions', paper.id],
    queryFn: () => api.getPaperEditions(paper.id),
  })

  const { data: languages } = useQuery({
    queryKey: ['languages'],
    queryFn: () => api.getAvailableLanguages(),
  })

  const discoverEditions = useMutation({
    mutationFn: () => api.discoverEditions(paper.id, { languageStrategy, customLanguages }),
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

  return (
    <div className="edition-discovery">
      <div className="edition-header">
        <button onClick={onBack} className="btn-back">← Back to Papers</button>
        <h2>Editions of: {paper.title}</h2>
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

      {/* Language selection modal */}
      {showLanguageModal && (
        <div className="modal-overlay">
          <div className="modal">
            <h3>Select Languages to Search</h3>
            <div className="language-strategies">
              <label>
                <input
                  type="radio"
                  value="major_languages"
                  checked={languageStrategy === 'major_languages'}
                  onChange={(e) => setLanguageStrategy(e.target.value)}
                />
                Major Languages (EN, DE, FR, ES, PT, IT, RU, ZH, JA)
              </label>
              <label>
                <input
                  type="radio"
                  value="english_only"
                  checked={languageStrategy === 'english_only'}
                  onChange={(e) => setLanguageStrategy(e.target.value)}
                />
                English Only
              </label>
            </div>

            <div className="custom-languages">
              <h4>Additional Languages:</h4>
              <div className="language-chips">
                {languages?.languages?.map(lang => (
                  <label key={lang.code} className="language-chip">
                    <input
                      type="checkbox"
                      checked={customLanguages.includes(lang.code)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setCustomLanguages([...customLanguages, lang.code])
                        } else {
                          setCustomLanguages(customLanguages.filter(c => c !== lang.code))
                        }
                      }}
                    />
                    {lang.icon} {lang.name}
                  </label>
                ))}
              </div>
            </div>

            <div className="modal-actions">
              <button onClick={() => setShowLanguageModal(false)} className="btn-secondary">
                Cancel
              </button>
              <button
                onClick={() => discoverEditions.mutate()}
                disabled={discoverEditions.isPending}
                className="btn-primary"
              >
                {discoverEditions.isPending ? 'Discovering...' : 'Start Discovery'}
              </button>
            </div>
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

          {editions?.map(edition => (
            <div key={edition.id} className={`edition-card ${edition.selected ? 'selected' : ''}`}>
              <input
                type="checkbox"
                checked={edition.selected}
                onChange={(e) => selectEditions.mutate({ ids: [edition.id], selected: e.target.checked })}
              />
              <div className="edition-info">
                <h4>{edition.title}</h4>
                <div className="edition-meta">
                  {edition.authors && <span>{edition.authors}</span>}
                  {edition.year && <span>({edition.year})</span>}
                  <span className="citations">{edition.citation_count} citations</span>
                  <span className={`confidence ${edition.confidence}`}>{edition.confidence}</span>
                  {edition.language && <span className="language">{edition.language}</span>}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
