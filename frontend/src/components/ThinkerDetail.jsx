/**
 * Thinker Detail Component - Shows thinker info, works, and actions
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import { useToast } from './Toast'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import DossierSelectModal from './DossierSelectModal'

function ThinkerDetail({ thinkerId, onBack }) {
  const [activeTab, setActiveTab] = useState('works')
  const [workFilter, setWorkFilter] = useState('accepted')
  const [selectedAuthor, setSelectedAuthor] = useState(null)  // For author papers modal
  const [authorPapers, setAuthorPapers] = useState([])
  const [loadingAuthorPapers, setLoadingAuthorPapers] = useState(false)
  // Author search modal (for clicking author names in Top Citing Papers)
  const [authorSearchQuery, setAuthorSearchQuery] = useState(null)
  const [authorSearchResults, setAuthorSearchResults] = useState(null)
  const [loadingAuthorSearch, setLoadingAuthorSearch] = useState(false)
  // Dossier selection for make-seed (using DossierSelectModal)
  const [showDossierModal, setShowDossierModal] = useState(false)
  const [pendingSeedCitation, setPendingSeedCitation] = useState(null)
  const [makingSeed, setMakingSeed] = useState({})  // Track which citations are being made into seeds
  const queryClient = useQueryClient()
  const { showToast } = useToast()

  // Fetch thinker details
  const { data: thinker, isLoading, error } = useQuery({
    queryKey: ['thinker', thinkerId],
    queryFn: () => api.getThinker(thinkerId),
  })

  // Fetch works
  const { data: worksData } = useQuery({
    queryKey: ['thinker-works', thinkerId, workFilter],
    queryFn: () => api.getThinkerWorks(thinkerId, { decision: workFilter !== 'all' ? workFilter : undefined }),
    enabled: !!thinkerId,
  })

  // Fetch analytics (only when tab is active)
  const { data: analytics, isLoading: analyticsLoading } = useQuery({
    queryKey: ['thinker-analytics', thinkerId],
    queryFn: () => api.getThinkerAnalytics(thinkerId),
    enabled: !!thinkerId && activeTab === 'analytics',
  })

  // Confirm disambiguation
  const confirmMutation = useMutation({
    mutationFn: (data) => api.confirmThinker(thinkerId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['thinker', thinkerId] })
      showToast('Thinker confirmed', 'success')
    },
    onError: (err) => showToast(`Confirmation failed: ${err.message}`, 'error'),
  })

  // Generate variants
  const variantsMutation = useMutation({
    mutationFn: () => api.generateThinkerVariants(thinkerId),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['thinker', thinkerId] })
      showToast(`Generated ${data.variants?.length || 0} name variants`, 'success')
    },
    onError: (err) => showToast(`Failed to generate variants: ${err.message}`, 'error'),
  })

  // Start discovery
  const discoveryMutation = useMutation({
    mutationFn: (options) => api.startThinkerDiscovery(thinkerId, options),
    onSuccess: (data) => {
      showToast(`Discovery job started (ID: ${data.job_id})`, 'success')
    },
    onError: (err) => showToast(`Failed to start discovery: ${err.message}`, 'error'),
  })

  // Detect translations
  const translationsMutation = useMutation({
    mutationFn: () => api.detectThinkerTranslations(thinkerId),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['thinker-works', thinkerId] })
      showToast(`Found ${data.work_groups?.length || 0} translation groups`, 'success')
    },
    onError: (err) => showToast(`Translation detection failed: ${err.message}`, 'error'),
  })

  // Start harvest
  const harvestMutation = useMutation({
    mutationFn: (options) => api.startThinkerHarvest(thinkerId, options),
    onSuccess: (data) => {
      showToast(`Harvest job started (ID: ${data.job_id})`, 'success')
    },
    onError: (err) => showToast(`Failed to start harvest: ${err.message}`, 'error'),
  })

  // Fetch papers for a specific citing author (from Top Citing Authors)
  const fetchAuthorPapers = async (author) => {
    if (!author.citation_ids?.length) {
      showToast('No paper data available for this author', 'warning')
      return
    }
    setSelectedAuthor(author)
    setLoadingAuthorPapers(true)
    try {
      const papers = await api.getAuthorPapers(thinkerId, author.citation_ids)
      setAuthorPapers(papers)
    } catch (err) {
      showToast(`Failed to load papers: ${err.message}`, 'error')
      setAuthorPapers([])
    } finally {
      setLoadingAuthorPapers(false)
    }
  }

  // Search ALL papers by author (from Top Citing Papers clickable author names)
  const searchAuthorPapers = async (authorName) => {
    setAuthorSearchQuery(authorName)
    setLoadingAuthorSearch(true)
    setAuthorSearchResults(null)
    try {
      const results = await api.searchPapersByAuthor(authorName, thinkerId, 50)
      setAuthorSearchResults(results)
    } catch (err) {
      showToast(`Failed to search author: ${err.message}`, 'error')
      setAuthorSearchResults({ papers: [], citations: [], total_results: 0 })
    } finally {
      setLoadingAuthorSearch(false)
    }
  }

  // Make a citation into a seed paper with dossier selection
  const handleMakeSeed = (citation) => {
    setPendingSeedCitation(citation)
    setPendingSeedWork(null)  // Clear any pending work
    setShowDossierModal(true)
  }

  // Make a thinker work into a seed paper with dossier selection
  const [pendingSeedWork, setPendingSeedWork] = useState(null)
  const handleMakeWorkSeed = (work) => {
    setPendingSeedWork(work)
    setPendingSeedCitation(null)  // Clear any pending citation
    setShowDossierModal(true)
  }

  // Called when dossier is selected from modal
  const handleDossierSelected = async (selections) => {
    const selection = selections[0] || {}  // Take first selection

    // Handle thinker work seed
    if (pendingSeedWork) {
      const workId = pendingSeedWork.work_id
      setMakingSeed(prev => ({ ...prev, [`work_${workId}`]: true }))

      try {
        const result = await api.makeThinkerWorkSeed(workId, {
          dossierId: selection.dossierId || null,
          createNewDossier: selection.createNewDossier || false,
          newDossierName: selection.newDossierName || null,
          collectionId: selection.collectionId || null,
        })

        showToast(`✅ Seed created (Paper #${result.paper_id})${result.dossier_name ? ` in "${result.dossier_name}"` : ''}`, 'success')
        showToast(`💡 Paper ready for harvesting. Go to Seeds tab to start.`, 'info')

        // Refresh analytics to update status
        queryClient.invalidateQueries({ queryKey: ['thinker-analytics', thinkerId] })

      } catch (err) {
        showToast(`Failed to create seed: ${err.message}`, 'error')
      } finally {
        setMakingSeed(prev => ({ ...prev, [`work_${workId}`]: false }))
        setShowDossierModal(false)
        setPendingSeedWork(null)
      }
      return
    }

    // Handle citation seed
    if (!pendingSeedCitation) return

    const citationId = pendingSeedCitation.citation_id || pendingSeedCitation.id
    setMakingSeed(prev => ({ ...prev, [citationId]: true }))

    try {
      const result = await api.makeCitationSeed(citationId, {
        dossierId: selection.dossierId || null,
        createNewDossier: selection.createNewDossier || false,
        newDossierName: selection.newDossierName || null,
        collectionId: selection.collectionId || null,
      })

      // Show success with paper ID
      showToast(`✅ Seed created (Paper #${result.paper_id})${result.dossier_name ? ` in "${result.dossier_name}"` : ''}`, 'success')

      // Offer to start harvest
      showToast(`💡 Paper ready for harvesting. Go to Seeds tab to start.`, 'info')

    } catch (err) {
      showToast(`Failed to create seed: ${err.message}`, 'error')
    } finally {
      setMakingSeed(prev => ({ ...prev, [citationId]: false }))
      setShowDossierModal(false)
      setPendingSeedCitation(null)
    }
  }

  // Quick seed without dossier selection (from search modal)
  const quickMakeSeed = async (citationId) => {
    setMakingSeed(prev => ({ ...prev, [citationId]: true }))
    try {
      const result = await api.makeCitationSeed(citationId, {})
      showToast(`✅ Seed created (Paper #${result.paper_id})`, 'success')
    } catch (err) {
      showToast(`Failed to create seed: ${err.message}`, 'error')
    } finally {
      setMakingSeed(prev => ({ ...prev, [citationId]: false }))
    }
  }

  if (isLoading) return <div className="loading">Loading thinker...</div>
  if (error) return <div className="error">Error: {error.message}</div>
  if (!thinker) return <div className="error">Thinker not found</div>

  // Handle both array (from API) and JSON string (from DB) formats
  const parseJsonField = (field) => {
    if (!field) return []
    if (Array.isArray(field)) return field
    try { return JSON.parse(field) } catch { return [] }
  }

  // Parse Scholar metadata string: "Author1, Author2 - 2000 - Publisher Name"
  // Returns { authors: ["Author1", "Author2"], venue: "Publisher Name" }
  const parseScholarMetadata = (rawString) => {
    if (!rawString) return { authors: [], venue: null }

    // Split by " - " to separate parts
    const parts = rawString.split(' - ')

    if (parts.length === 1) {
      // No dashes, assume it's just authors
      return {
        authors: parts[0].split(',').map(a => a.trim()).filter(Boolean),
        venue: null
      }
    }

    // First part is always authors
    const authorsPart = parts[0]
    const authors = authorsPart.split(',').map(a => a.trim()).filter(Boolean)

    // Find venue - skip year parts (4-digit numbers)
    let venue = null
    for (let i = 1; i < parts.length; i++) {
      const part = parts[i].trim()
      // Skip if it looks like a year (4-digit number at start)
      if (/^\d{4}$/.test(part)) continue
      // This is likely the venue/publisher
      venue = part
      break
    }

    return { authors, venue }
  }

  const domains = parseJsonField(thinker.domains)
  const notableWorks = parseJsonField(thinker.notable_works)
  const nameVariants = parseJsonField(thinker.name_variants)
  const works = worksData?.works || []

  return (
    <div className="thinker-detail">
      {/* Header */}
      <div className="detail-header">
        <button className="back-btn" onClick={onBack}>&larr; Back to Thinkers</button>
        <div className="thinker-info">
          <h2>{thinker.canonical_name}</h2>
          {thinker.birth_death && <span className="dates">({thinker.birth_death})</span>}
          <span className={`status-badge status-${thinker.status}`}>{thinker.status}</span>
        </div>
        {thinker.bio && <p className="bio">{thinker.bio}</p>}
        {domains.length > 0 && (
          <div className="domains">
            {domains.map((d, i) => <span key={i} className="domain-tag">{d}</span>)}
          </div>
        )}
      </div>

      {/* Action Bar */}
      <div className="action-bar">
        {thinker.status === 'pending' && (
          <button
            className="btn btn-primary"
            onClick={() => confirmMutation.mutate({ confirmed: true })}
            disabled={confirmMutation.isPending}
          >
            {confirmMutation.isPending ? 'Confirming...' : 'Confirm Identity'}
          </button>
        )}
        {thinker.status === 'disambiguated' && nameVariants.length === 0 && (
          <button
            className="btn btn-primary"
            onClick={() => variantsMutation.mutate()}
            disabled={variantsMutation.isPending}
          >
            {variantsMutation.isPending ? 'Generating...' : 'Generate Name Variants'}
          </button>
        )}
        {nameVariants.length > 0 && thinker.works_discovered === 0 && (
          <button
            className="btn btn-primary"
            onClick={() => discoveryMutation.mutate({})}
            disabled={discoveryMutation.isPending}
          >
            {discoveryMutation.isPending ? 'Starting...' : 'Start Work Discovery'}
          </button>
        )}
        {thinker.works_discovered > 0 && (
          <>
            <button
              className="btn btn-secondary"
              onClick={() => translationsMutation.mutate()}
              disabled={translationsMutation.isPending}
              title="Use AI to detect translations and group related works"
            >
              {translationsMutation.isPending ? 'Detecting...' : 'Detect Translations'}
            </button>
            <button
              className="btn btn-primary"
              onClick={() => harvestMutation.mutate({})}
              disabled={harvestMutation.isPending}
            >
              {harvestMutation.isPending ? 'Starting...' : 'Harvest Citations'}
            </button>
          </>
        )}
      </div>

      {/* Stats */}
      <div className="stats-bar">
        <div className="stat">
          <span className="stat-value">{thinker.works_discovered}</span>
          <span className="stat-label">Works Found</span>
        </div>
        <div className="stat">
          <span className="stat-value">{thinker.works_harvested}</span>
          <span className="stat-label">Harvested</span>
        </div>
        <div className="stat">
          <span className="stat-value">{thinker.total_citations.toLocaleString()}</span>
          <span className="stat-label">Total Citations</span>
        </div>
        <div className="stat">
          <span className="stat-value">{nameVariants.length}</span>
          <span className="stat-label">Name Variants</span>
        </div>
      </div>

      {/* Tabs */}
      <div className="tabs-nav">
        <button
          className={`tab-btn ${activeTab === 'works' ? 'active' : ''}`}
          onClick={() => setActiveTab('works')}
        >
          Works ({thinker.works_discovered})
        </button>
        <button
          className={`tab-btn ${activeTab === 'variants' ? 'active' : ''}`}
          onClick={() => setActiveTab('variants')}
        >
          Name Variants ({nameVariants.length})
        </button>
        <button
          className={`tab-btn ${activeTab === 'notable' ? 'active' : ''}`}
          onClick={() => setActiveTab('notable')}
        >
          Notable Works ({notableWorks.length})
        </button>
        <button
          className={`tab-btn ${activeTab === 'analytics' ? 'active' : ''}`}
          onClick={() => setActiveTab('analytics')}
        >
          Analytics
        </button>
      </div>

      {/* Tab Content */}
      <div className="tab-content">
        {activeTab === 'works' && (
          <div className="works-tab">
            <div className="filter-bar">
              <select value={workFilter} onChange={(e) => setWorkFilter(e.target.value)}>
                <option value="all">All Works</option>
                <option value="accepted">Accepted</option>
                <option value="rejected">Rejected</option>
                <option value="uncertain">Uncertain</option>
              </select>
            </div>
            {works.length === 0 ? (
              <p className="empty">No works found. Start discovery to find works by this thinker.</p>
            ) : (
              <table className="works-table">
                <thead>
                  <tr>
                    <th>Title</th>
                    <th>Year</th>
                    <th>Citations</th>
                    <th>Decision</th>
                    <th>Translation</th>
                  </tr>
                </thead>
                <tbody>
                  {works.map((work) => (
                    <tr key={work.id} className={`work-row decision-${work.decision}`}>
                      <td className="title-cell">
                        <span className="work-title">{work.title}</span>
                        {work.authors_raw && (
                          <span className="work-authors">{work.authors_raw}</span>
                        )}
                      </td>
                      <td>{work.year || '-'}</td>
                      <td className="number-cell">{work.citation_count.toLocaleString()}</td>
                      <td>
                        <span className={`decision-badge decision-${work.decision}`}>
                          {work.decision}
                        </span>
                      </td>
                      <td>
                        {work.is_translation ? (
                          <span className="translation-badge">
                            Translation ({work.original_language || 'unknown'})
                          </span>
                        ) : (
                          <span className="muted">-</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}

        {activeTab === 'variants' && (
          <div className="variants-tab">
            {nameVariants.length === 0 ? (
              <p className="empty">No name variants generated yet. Click "Generate Name Variants" to create search queries.</p>
            ) : (
              <div className="variants-list">
                {nameVariants.map((variant, i) => (
                  <div key={i} className="variant-item">
                    <span className="variant-query">{variant.query || variant}</span>
                    {variant.type && <span className="variant-type">{variant.type}</span>}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {activeTab === 'notable' && (
          <div className="notable-tab">
            {notableWorks.length === 0 ? (
              <p className="empty">No notable works recorded from disambiguation.</p>
            ) : (
              <ul className="notable-list">
                {notableWorks.map((work, i) => (
                  <li key={i}>{work}</li>
                ))}
              </ul>
            )}
          </div>
        )}

        {activeTab === 'analytics' && (
          <div className="analytics-tab">
            {analyticsLoading ? (
              <div className="loading">Loading analytics...</div>
            ) : !analytics || analytics.total_citations === 0 ? (
              <p className="empty">No citations harvested yet. Start harvesting to see analytics.</p>
            ) : (
              <div className="analytics-content">
                {/* Summary Stats */}
                <div className="analytics-summary">
                  <div className="summary-stat" title="Total number of times this thinker's works have been cited">
                    <span className="summary-value">{analytics.total_citations?.toLocaleString()}</span>
                    <span className="summary-label">Total Citations</span>
                    <span className="summary-hint">All citations to this thinker's works</span>
                  </div>
                  <div className="summary-stat" title="Number of unique papers that cite this thinker">
                    <span className="summary-value">{analytics.unique_citing_papers?.toLocaleString()}</span>
                    <span className="summary-label">Citing Papers</span>
                    <span className="summary-hint">Distinct papers referencing this thinker</span>
                  </div>
                  <div className="summary-stat" title="Number of unique scholars who have cited this thinker">
                    <span className="summary-value">{analytics.unique_citing_authors?.toLocaleString()}</span>
                    <span className="summary-label">Citing Authors</span>
                    <span className="summary-hint">Scholars who cite this thinker</span>
                  </div>
                  <div className="summary-stat" title="Number of journals, conferences, and publications where citations appear">
                    <span className="summary-value">{analytics.unique_venues?.toLocaleString()}</span>
                    <span className="summary-label">Venues</span>
                    <span className="summary-hint">Where citations appear</span>
                  </div>
                </div>

                {/* Citations Over Time */}
                {analytics.citations_by_year?.length > 0 && (
                  <div className="analytics-card">
                    <h3>Citations Over Time</h3>
                    <div className="chart-container">
                      <ResponsiveContainer width="100%" height={250}>
                        <LineChart data={analytics.citations_by_year}>
                          <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" />
                          <XAxis dataKey="year" stroke="var(--text-secondary)" fontSize={12} />
                          <YAxis stroke="var(--text-secondary)" fontSize={12} />
                          <Tooltip
                            contentStyle={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
                            labelStyle={{ color: 'var(--text-primary)' }}
                          />
                          <Line type="monotone" dataKey="count" stroke="var(--primary-color)" strokeWidth={2} dot={{ fill: 'var(--primary-color)', r: 3 }} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                )}

                {/* Most Cited Works */}
                {analytics.most_cited_works?.length > 0 && (
                  <div className="analytics-card">
                    <h3>Most Cited Works</h3>
                    <p className="card-subtitle">
                      This thinker's works ranked by how many papers cite them. Click title to view on Scholar, author to explore.
                    </p>
                    <div className="top-citing-papers-list">
                      {analytics.most_cited_works.slice(0, 10).map((work, i) => {
                        const { authors, venue } = parseScholarMetadata(work.authors)
                        return (
                        <div key={work.work_id} className="top-citing-paper">
                          <div className="paper-rank-badge">#{i + 1}</div>
                          <div className="paper-content">
                            <div className="paper-title-row">
                              {work.link ? (
                                <a href={work.link} target="_blank" rel="noopener noreferrer" className="paper-title clickable-title">
                                  {work.title}
                                  <span className="external-link-icon">↗</span>
                                </a>
                              ) : (
                                <span className="paper-title">{work.title}</span>
                              )}
                            </div>
                            {authors.length > 0 && (
                              <div className="paper-byline">
                                <span className="paper-authors">
                                  {authors.map((author, authorIdx) => (
                                    <span key={authorIdx}>
                                      <span
                                        className="clickable-author"
                                        onClick={() => searchAuthorPapers(author)}
                                        title={`Search papers by ${author}`}
                                      >
                                        {author}
                                      </span>
                                      {authorIdx < authors.length - 1 && ', '}
                                    </span>
                                  ))}
                                  {authors.length > 1 && (
                                    <button
                                      className="all-authors-btn"
                                      onClick={() => searchAuthorPapers(authors.join(', '))}
                                      title="Search using all authors"
                                    >
                                      [All]
                                    </button>
                                  )}
                                </span>
                              </div>
                            )}
                            <div className="paper-details">
                              {work.year && <span className="paper-year">{work.year}</span>}
                              {venue && <span className="paper-venue">{venue}</span>}
                              <span className={`work-status-badge ${work.paper_id ? 'status-harvested' : 'status-pending'}`}>
                                {work.paper_id ? `✓ Harvested (#${work.paper_id})` : 'Pending'}
                              </span>
                            </div>
                            <div className="paper-actions">
                              <button
                                className="action-btn seed-btn"
                                onClick={() => handleMakeWorkSeed(work)}
                                disabled={makingSeed[`work_${work.work_id}`]}
                                title={work.paper_id ? 'Already harvested - click to view/update dossier' : 'Create seed paper for harvesting'}
                              >
                                {makingSeed[`work_${work.work_id}`] ? 'Creating...' : work.paper_id ? '📄 View Seed' : '🌱 Make Seed'}
                              </button>
                            </div>
                          </div>
                          <div className="paper-influence-badge" title="How many papers cite this work">
                            <span className="influence-number">{work.citations_received?.toLocaleString()}</span>
                            <span className="influence-label">citations</span>
                          </div>
                        </div>
                        )
                      })}
                    </div>
                  </div>
                )}

                {/* Top Citing Papers */}
                {analytics.top_citing_papers?.length > 0 && (
                  <div className="analytics-card">
                    <h3>Top Citing Papers</h3>
                    <p className="card-subtitle">
                      Papers by other scholars that cite this thinker's work. Click title to view, author to explore their work.
                    </p>
                    <div className="top-citing-papers-list">
                      {analytics.top_citing_papers.slice(0, 10).map((paper, i) => (
                        <div key={paper.citation_id || i} className="top-citing-paper">
                          <div className="paper-rank-badge">#{i + 1}</div>
                          <div className="paper-content">
                            <div className="paper-title-row">
                              {paper.link ? (
                                <a href={paper.link} target="_blank" rel="noopener noreferrer" className="paper-title clickable-title">
                                  {paper.title || 'Untitled'}
                                  <span className="external-link-icon">↗</span>
                                </a>
                              ) : (
                                <span className="paper-title">{paper.title || 'Untitled'}</span>
                              )}
                            </div>
                            <div className="paper-byline">
                              <span className="paper-authors">
                                {paper.authors ? (
                                  <>
                                    {paper.authors.split(',').map((author, authorIdx, arr) => (
                                      <span key={authorIdx}>
                                        <span
                                          className="clickable-author"
                                          onClick={() => searchAuthorPapers(author.trim())}
                                          title={`Search papers by ${author.trim()}`}
                                        >
                                          {author.trim()}
                                        </span>
                                        {authorIdx < arr.length - 1 && ', '}
                                      </span>
                                    ))}
                                    {paper.authors.includes(',') && (
                                      <button
                                        className="all-authors-btn"
                                        onClick={() => searchAuthorPapers(paper.authors)}
                                        title="Search using full author string"
                                      >
                                        [All]
                                      </button>
                                    )}
                                  </>
                                ) : (
                                  'Unknown authors'
                                )}
                              </span>
                            </div>
                            <div className="paper-details">
                              {paper.venue && <span className="paper-venue">{paper.venue}</span>}
                              {paper.year && <span className="paper-year">{paper.year}</span>}
                            </div>
                            <div className="paper-actions">
                              <button
                                className="action-btn seed-btn"
                                onClick={() => handleMakeSeed(paper)}
                                disabled={makingSeed[paper.citation_id]}
                                title="Convert to seed paper for harvesting (choose dossier)"
                              >
                                {makingSeed[paper.citation_id] ? 'Creating...' : '🌱 Make Seed'}
                              </button>
                            </div>
                          </div>
                          <div className="paper-influence-badge" title="How many papers cite this citing paper">
                            <span className="influence-number">{paper.citation_count?.toLocaleString()}</span>
                            <span className="influence-label">citations</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Top Venues Chart */}
                {analytics.top_venues?.length > 0 && (
                  <div className="analytics-card">
                    <h3>Top Venues</h3>
                    <p className="card-subtitle">Where this thinker's work is cited</p>
                    <div className="chart-container">
                      <ResponsiveContainer width="100%" height={300}>
                        <BarChart data={analytics.top_venues.slice(0, 10)} layout="vertical">
                          <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" />
                          <XAxis type="number" stroke="var(--text-secondary)" fontSize={12} />
                          <YAxis
                            type="category"
                            dataKey="venue"
                            stroke="var(--text-secondary)"
                            fontSize={11}
                            width={200}
                            tickFormatter={(val) => val.length > 30 ? val.substring(0, 30) + '...' : val}
                          />
                          <Tooltip
                            contentStyle={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
                            labelStyle={{ color: 'var(--text-primary)' }}
                          />
                          <Bar dataKey="citation_count" fill="var(--primary-color)" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                )}

                {/* Top Citing Authors */}
                {analytics.top_citing_authors?.length > 0 && (
                  <div className="analytics-card">
                    <h3>Top Citing Authors</h3>
                    <p className="card-subtitle">
                      Scholars whose papers cite this thinker's work. Click any author to see their citing papers.
                    </p>
                    <div className="citing-authors-header">
                      <span className="header-rank">#</span>
                      <span className="header-author">Author</span>
                      <span className="header-papers" title="Number of papers by this author that cite the thinker">Papers</span>
                      <span className="header-influence" title="Total citations received by their citing papers (higher = more influential)">Influence</span>
                    </div>
                    <div className="citing-authors-list">
                      {analytics.top_citing_authors.slice(0, 15).map((author, i) => (
                        <div
                          key={i}
                          className={`citing-author-row clickable ${author.is_self_citation ? 'self-citation' : ''}`}
                          onClick={() => fetchAuthorPapers(author)}
                          title={author.is_self_citation ? 'Self-citation (thinker citing own work)' : 'Click to see papers'}
                        >
                          <span className="author-rank">#{i + 1}</span>
                          <div className="author-name-cell">
                            <span className="author-name">
                              {author.author}
                              {author.is_self_citation && <span className="self-citation-badge">self</span>}
                            </span>
                          </div>
                          <span className="author-papers">{author.papers_count}</span>
                          <span className="author-influence" title={`${author.citation_count} total citations on their ${author.papers_count} citing paper${author.papers_count !== 1 ? 's' : ''}`}>
                            {author.citation_count?.toLocaleString()}
                          </span>
                        </div>
                      ))}
                    </div>
                    <p className="citing-authors-footnote">
                      <strong>Influence</strong> = total citations received by the author's citing papers.
                      Higher numbers indicate the citing papers are themselves more widely read and influential.
                    </p>
                  </div>
                )}

                {/* Author Papers Modal (from Top Citing Authors click) */}
                {selectedAuthor && (
                  <div className="modal-overlay" onClick={() => setSelectedAuthor(null)}>
                    <div className="modal-content author-papers-modal" onClick={e => e.stopPropagation()}>
                      <div className="modal-header">
                        <h3>
                          Papers by {selectedAuthor.author}
                          {selectedAuthor.is_self_citation && <span className="self-citation-badge">self-citation</span>}
                        </h3>
                        <button className="close-btn" onClick={() => setSelectedAuthor(null)}>×</button>
                      </div>
                      <div className="modal-body">
                        {loadingAuthorPapers ? (
                          <div className="loading">Loading papers...</div>
                        ) : authorPapers.length === 0 ? (
                          <p className="no-data">No papers found</p>
                        ) : (
                          <div className="author-papers-list">
                            {authorPapers.map((paper, i) => (
                              <div key={paper.citation_id} className="author-paper-item">
                                <div className="paper-rank">#{i + 1}</div>
                                <div className="paper-info">
                                  <div className="paper-title">
                                    {paper.url ? (
                                      <a href={paper.url} target="_blank" rel="noopener noreferrer">
                                        {paper.title || 'Untitled'}
                                      </a>
                                    ) : (
                                      paper.title || 'Untitled'
                                    )}
                                  </div>
                                  <div className="paper-meta">
                                    <span className="paper-authors">{paper.authors || 'Unknown authors'}</span>
                                    {paper.venue && <span className="paper-venue">{paper.venue}</span>}
                                    {paper.year && <span className="paper-year">{paper.year}</span>}
                                  </div>
                                </div>
                                <div className="paper-citations">
                                  {paper.citation_count?.toLocaleString() || 0}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}

                {/* Author Search Modal (from clicking author name in Top Citing Papers) */}
                {authorSearchQuery && (
                  <div className="modal-overlay" onClick={() => { setAuthorSearchQuery(null); setAuthorSearchResults(null); }}>
                    <div className="modal-content author-search-modal" onClick={e => e.stopPropagation()}>
                      <div className="modal-header">
                        <h3>Papers by "{authorSearchQuery}"</h3>
                        <button className="close-btn" onClick={() => { setAuthorSearchQuery(null); setAuthorSearchResults(null); }}>×</button>
                      </div>
                      <div className="modal-body">
                        {loadingAuthorSearch ? (
                          <div className="loading">Searching database...</div>
                        ) : !authorSearchResults ? (
                          <p className="no-data">No results</p>
                        ) : (
                          <div className="author-search-results">
                            <p className="search-summary">
                              Found {authorSearchResults.total_results} results
                              ({authorSearchResults.papers?.length || 0} seed papers,
                              {authorSearchResults.citations?.length || 0} citations)
                            </p>

                            {authorSearchResults.papers?.length > 0 && (
                              <div className="search-section">
                                <h4>Seed Papers</h4>
                                <div className="search-papers-list">
                                  {authorSearchResults.papers.map((paper) => (
                                    <div key={`paper-${paper.id}`} className="search-paper-item">
                                      <div className="search-paper-info">
                                        {paper.link ? (
                                          <a href={paper.link} target="_blank" rel="noopener noreferrer" className="search-paper-title">
                                            {paper.title}
                                          </a>
                                        ) : (
                                          <span className="search-paper-title">{paper.title}</span>
                                        )}
                                        <div className="search-paper-meta">
                                          <span>{paper.authors}</span>
                                          {paper.year && <span>{paper.year}</span>}
                                          {paper.venue && <span>{paper.venue}</span>}
                                        </div>
                                      </div>
                                      <span className="search-paper-citations">{paper.citation_count?.toLocaleString()}</span>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}

                            {authorSearchResults.citations?.length > 0 && (
                              <div className="search-section">
                                <h4>Citations</h4>
                                <div className="search-papers-list">
                                  {authorSearchResults.citations.map((citation) => (
                                    <div
                                      key={`cit-${citation.id}`}
                                      className={`search-paper-item ${citation.is_from_current_thinker ? 'from-current-thinker' : ''}`}
                                    >
                                      <div className="search-paper-info">
                                        {citation.link ? (
                                          <a href={citation.link} target="_blank" rel="noopener noreferrer" className="search-paper-title">
                                            {citation.title}
                                          </a>
                                        ) : (
                                          <span className="search-paper-title">{citation.title}</span>
                                        )}
                                        <div className="search-paper-meta">
                                          <span>{citation.authors}</span>
                                          {citation.year && <span>{citation.year}</span>}
                                          {citation.citing_thinker_name && (
                                            <span className={`thinker-tag ${citation.is_from_current_thinker ? 'current' : ''}`}>
                                              Cites: {citation.citing_thinker_name}
                                            </span>
                                          )}
                                        </div>
                                      </div>
                                      <div className="search-paper-actions">
                                        <span className="search-paper-citations">{citation.citation_count?.toLocaleString()}</span>
                                        <button
                                          className="mini-action-btn"
                                          onClick={() => quickMakeSeed(citation.id)}
                                          disabled={makingSeed[citation.id]}
                                          title="Quick seed (no dossier)"
                                        >
                                          🌱
                                        </button>
                                        <button
                                          className="mini-action-btn dossier"
                                          onClick={() => handleMakeSeed(citation)}
                                          disabled={makingSeed[citation.id]}
                                          title="Seed with dossier selection"
                                        >
                                          📁
                                        </button>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}

                {/* Dossier Selection Modal (using shared component) */}
                <DossierSelectModal
                  isOpen={showDossierModal}
                  onClose={() => { setShowDossierModal(false); setPendingSeedCitation(null); }}
                  onSelect={handleDossierSelected}
                  title="Create Seed Paper"
                  subtitle={pendingSeedCitation ? `Adding: "${pendingSeedCitation.title?.substring(0, 50)}..."` : 'Select destination'}
                  allowMultiple={false}
                />
              </div>
            )}
          </div>
        )}
      </div>

      <style>{`
        .thinker-detail {
          padding: 20px;
        }

        .detail-header {
          margin-bottom: 24px;
        }

        .back-btn {
          background: none;
          border: none;
          color: var(--text-secondary);
          cursor: pointer;
          padding: 0;
          margin-bottom: 16px;
          font-size: 14px;
        }

        .back-btn:hover {
          color: var(--primary-color);
        }

        .thinker-info {
          display: flex;
          align-items: center;
          gap: 12px;
          margin-bottom: 8px;
        }

        .thinker-info h2 {
          margin: 0;
        }

        .dates {
          color: var(--text-secondary);
        }

        .bio {
          color: var(--text-secondary);
          margin: 8px 0;
        }

        .domains {
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
        }

        .domain-tag {
          background: var(--bg-secondary);
          padding: 4px 8px;
          border-radius: 4px;
          font-size: 0.85em;
        }

        .action-bar {
          display: flex;
          gap: 12px;
          margin-bottom: 24px;
          padding: 16px;
          background: var(--bg-secondary);
          border-radius: 8px;
        }

        .stats-bar {
          display: flex;
          gap: 32px;
          margin-bottom: 24px;
        }

        .stat {
          display: flex;
          flex-direction: column;
        }

        .stat-value {
          font-size: 24px;
          font-weight: 600;
          color: var(--text-primary);
        }

        .stat-label {
          font-size: 12px;
          color: var(--text-secondary);
        }

        .tabs-nav {
          display: flex;
          gap: 4px;
          border-bottom: 1px solid var(--border-color);
          margin-bottom: 16px;
        }

        .tab-btn {
          padding: 12px 20px;
          background: none;
          border: none;
          border-bottom: 2px solid transparent;
          cursor: pointer;
          color: var(--text-secondary);
          transition: all 0.15s;
        }

        .tab-btn:hover {
          color: var(--text-primary);
        }

        .tab-btn.active {
          color: var(--primary-color);
          border-bottom-color: var(--primary-color);
        }

        .filter-bar {
          margin-bottom: 16px;
        }

        .filter-bar select {
          padding: 8px 12px;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          background: var(--bg-secondary);
          color: var(--text-primary);
        }

        .works-table {
          width: 100%;
          border-collapse: collapse;
        }

        .works-table th,
        .works-table td {
          padding: 12px 16px;
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        .works-table th {
          background: var(--bg-secondary);
          font-weight: 600;
        }

        .title-cell {
          max-width: 400px;
        }

        .work-title {
          display: block;
          color: var(--text-primary);
        }

        .work-authors {
          display: block;
          font-size: 0.85em;
          color: var(--text-secondary);
        }

        .number-cell {
          font-family: monospace;
        }

        .decision-badge {
          display: inline-block;
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.8em;
        }

        .decision-accepted {
          background: var(--success-bg);
          color: var(--success-color);
        }

        .decision-rejected {
          background: var(--danger-bg);
          color: var(--danger-color);
        }

        .decision-uncertain {
          background: var(--warning-bg);
          color: var(--warning-color);
        }

        .work-row.decision-rejected {
          opacity: 0.6;
        }

        .translation-badge {
          background: var(--info-bg);
          color: var(--info-color);
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.8em;
        }

        .variants-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .variant-item {
          display: flex;
          justify-content: space-between;
          padding: 12px 16px;
          background: var(--bg-secondary);
          border-radius: 4px;
        }

        .variant-query {
          font-family: monospace;
        }

        .variant-type {
          color: var(--text-secondary);
          font-size: 0.85em;
        }

        .notable-list {
          padding-left: 24px;
        }

        .notable-list li {
          padding: 8px 0;
          border-bottom: 1px solid var(--border-color);
        }

        .empty {
          text-align: center;
          padding: 40px;
          color: var(--text-secondary);
        }

        .muted {
          color: var(--text-muted);
        }

        .status-badge {
          padding: 4px 8px;
          border-radius: 4px;
          font-size: 0.8em;
        }

        .status-pending {
          background: var(--warning-bg);
          color: var(--warning-color);
        }

        .status-disambiguated {
          background: var(--info-bg);
          color: var(--info-color);
        }

        .status-harvesting {
          background: var(--primary-bg);
          color: var(--primary-color);
        }

        .status-complete {
          background: var(--success-bg);
          color: var(--success-color);
        }

        .btn {
          padding: 8px 16px;
          border: none;
          border-radius: 4px;
          cursor: pointer;
          font-size: 14px;
        }

        .btn-primary {
          background: var(--primary-color);
          color: white;
        }

        .btn-secondary {
          background: var(--bg-primary);
          color: var(--text-primary);
          border: 1px solid var(--border-color);
        }

        .btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        /* Analytics Tab Styles */
        .analytics-content {
          display: flex;
          flex-direction: column;
          gap: 24px;
        }

        .analytics-summary {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 16px;
          padding: 20px;
          background: var(--bg-secondary);
          border-radius: 8px;
        }

        .summary-stat {
          text-align: center;
          cursor: help;
          padding: 12px 8px;
          border-radius: 8px;
          transition: background-color 0.15s;
        }

        .summary-stat:hover {
          background: color-mix(in srgb, var(--primary-color) 8%, transparent);
        }

        .summary-value {
          display: block;
          font-size: 2.2em;
          font-weight: 700;
          color: var(--primary-color);
          font-family: var(--font-mono);
        }

        .summary-label {
          display: block;
          font-size: 0.9em;
          font-weight: 600;
          color: var(--text-primary);
          margin-top: 4px;
        }

        .summary-hint {
          display: block;
          font-size: 0.75em;
          color: var(--text-muted);
          margin-top: 4px;
        }

        .analytics-card {
          background: var(--bg-secondary);
          border-radius: 8px;
          padding: 20px;
        }

        .analytics-card h3 {
          margin: 0 0 8px 0;
          font-size: 1.1em;
        }

        .card-subtitle {
          margin: 0 0 16px 0;
          font-size: 0.85em;
          color: var(--text-secondary);
        }

        .chart-container {
          margin-top: 16px;
        }

        .ranked-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .ranked-item {
          display: flex;
          align-items: center;
          gap: 12px;
          padding: 10px;
          background: var(--bg-primary);
          border-radius: 6px;
        }

        .rank {
          font-weight: bold;
          color: var(--text-secondary);
          min-width: 30px;
        }

        .item-content {
          flex: 1;
          min-width: 0;
        }

        .item-title {
          display: block;
          font-weight: 500;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }

        .item-meta {
          font-size: 0.8em;
          color: var(--text-secondary);
        }

        .item-count {
          font-weight: bold;
          font-family: var(--font-mono);
          color: var(--primary-color);
        }

        .citing-papers-list {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .citing-paper {
          padding: 12px;
          background: var(--bg-primary);
          border-radius: 6px;
        }

        .paper-main {
          margin-bottom: 6px;
        }

        .paper-title {
          display: block;
          font-weight: 500;
          margin-bottom: 4px;
        }

        .paper-authors {
          font-size: 0.85em;
          color: var(--text-secondary);
        }

        .paper-meta {
          display: flex;
          gap: 12px;
          font-size: 0.8em;
          color: var(--text-secondary);
        }

        .paper-citations {
          font-weight: bold;
          color: var(--primary-color);
        }

        /* Top Citing Papers & Most Cited Works - Card Style */
        .top-citing-papers-list {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .top-citing-paper {
          display: grid;
          grid-template-columns: 45px 1fr 80px;
          gap: 16px;
          padding: 16px;
          background: var(--bg-primary);
          border-radius: 8px;
          border: 1px solid var(--border-color);
          align-items: start;
        }

        .paper-rank-badge {
          font-weight: 700;
          color: var(--text-secondary);
          font-size: 0.95em;
          padding-top: 2px;
        }

        .paper-content {
          min-width: 0;
        }

        .paper-title-row .paper-title {
          font-weight: 600;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
          line-height: 1.4;
        }

        .paper-byline {
          margin-top: 6px;
        }

        .paper-byline .paper-authors {
          font-size: 0.9em;
          color: var(--text-secondary);
        }

        .paper-details {
          display: flex;
          gap: 12px;
          margin-top: 6px;
          font-size: 0.8em;
          color: var(--text-muted);
        }

        .paper-details .paper-venue {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          max-width: 250px;
        }

        .paper-influence-badge {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          padding: 8px;
          background: color-mix(in srgb, var(--primary-color) 12%, transparent);
          border-radius: 8px;
          cursor: help;
        }

        .influence-number {
          font-weight: 700;
          font-size: 1.15em;
          color: var(--primary-color);
          font-family: var(--font-mono);
        }

        .influence-label {
          font-size: 0.65em;
          color: var(--text-secondary);
          text-transform: uppercase;
          letter-spacing: 0.5px;
        }

        /* Clickable author items */
        .ranked-item.clickable {
          cursor: pointer;
          transition: background-color 0.15s ease;
        }

        .ranked-item.clickable:hover {
          background: var(--bg-secondary);
        }

        /* Self-citation styling */
        .ranked-item.self-citation {
          border-left: 3px solid var(--warning-color);
          background: color-mix(in srgb, var(--warning-bg) 30%, var(--bg-primary));
        }

        .self-citation-badge {
          display: inline-block;
          margin-left: 8px;
          padding: 2px 6px;
          background: var(--warning-bg);
          color: var(--warning-color);
          font-size: 0.7em;
          border-radius: 4px;
          font-weight: normal;
          text-transform: uppercase;
          letter-spacing: 0.5px;
        }

        /* Top Citing Authors - Table Style */
        .citing-authors-header {
          display: grid;
          grid-template-columns: 50px 1fr 80px 100px;
          gap: 12px;
          padding: 10px 12px;
          border-bottom: 2px solid var(--border-color);
          font-size: 0.75em;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.5px;
          color: var(--text-secondary);
        }

        .header-papers,
        .header-influence {
          text-align: right;
          cursor: help;
        }

        .citing-authors-list {
          display: flex;
          flex-direction: column;
        }

        .citing-author-row {
          display: grid;
          grid-template-columns: 50px 1fr 80px 100px;
          gap: 12px;
          padding: 12px;
          border-bottom: 1px solid var(--border-color);
          align-items: center;
          cursor: pointer;
          transition: background-color 0.15s ease;
        }

        .citing-author-row:hover {
          background: var(--bg-primary);
        }

        .citing-author-row:last-child {
          border-bottom: none;
        }

        .citing-author-row.self-citation {
          background: color-mix(in srgb, var(--warning-bg) 20%, transparent);
          border-left: 3px solid var(--warning-color);
          margin-left: -3px;
          padding-left: 15px;
        }

        .citing-author-row.self-citation:hover {
          background: color-mix(in srgb, var(--warning-bg) 35%, transparent);
        }

        .author-rank {
          font-weight: 600;
          color: var(--text-secondary);
          font-size: 0.9em;
        }

        .author-name-cell {
          min-width: 0;
        }

        .author-name {
          display: flex;
          align-items: center;
          gap: 8px;
          font-weight: 500;
        }

        .author-papers {
          text-align: right;
          font-family: var(--font-mono);
          font-size: 0.95em;
          color: var(--text-secondary);
        }

        .author-influence {
          text-align: right;
          font-weight: 600;
          font-family: var(--font-mono);
          font-size: 1.05em;
          color: var(--primary-color);
          cursor: help;
        }

        .citing-authors-footnote {
          margin-top: 16px;
          padding: 12px 16px;
          background: color-mix(in srgb, var(--primary-color) 8%, transparent);
          border-radius: 6px;
          font-size: 0.8em;
          color: var(--text-secondary);
          border-left: 3px solid var(--primary-color);
        }

        .citing-authors-footnote strong {
          color: var(--text-primary);
        }

        /* Modal styles - SOLID backgrounds */
        .modal-overlay {
          position: fixed;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background: rgba(0, 0, 0, 0.9);
          display: flex;
          align-items: center;
          justify-content: center;
          z-index: 1000;
          backdrop-filter: blur(4px);
        }

        .modal-content {
          background: #1a1a1a;
          border-radius: 12px;
          max-width: 700px;
          width: 90%;
          max-height: 80vh;
          overflow: hidden;
          display: flex;
          flex-direction: column;
          box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
          border: 1px solid var(--border-color);
        }

        .modal-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 16px 20px;
          border-bottom: 1px solid var(--border-color);
        }

        .modal-header h3 {
          margin: 0;
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .close-btn {
          background: none;
          border: none;
          font-size: 24px;
          cursor: pointer;
          color: var(--text-secondary);
          padding: 4px 8px;
          line-height: 1;
        }

        .close-btn:hover {
          color: var(--text-primary);
        }

        .modal-body {
          padding: 20px;
          overflow-y: auto;
          background: #1a1a1a;
        }

        .author-papers-list {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .author-paper-item {
          display: flex;
          gap: 12px;
          padding: 12px;
          background: #252525;
          border-radius: 8px;
        }

        .author-paper-item .paper-rank {
          font-weight: bold;
          color: var(--text-secondary);
          min-width: 30px;
        }

        .author-paper-item .paper-info {
          flex: 1;
          min-width: 0;
        }

        .author-paper-item .paper-title {
          font-weight: 500;
          margin-bottom: 4px;
        }

        .author-paper-item .paper-title a {
          color: var(--primary-color);
          text-decoration: none;
        }

        .author-paper-item .paper-title a:hover {
          text-decoration: underline;
        }

        .author-paper-item .paper-meta {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          font-size: 0.85em;
          color: var(--text-secondary);
        }

        .author-paper-item .paper-meta span::after {
          content: '·';
          margin-left: 8px;
        }

        .author-paper-item .paper-meta span:last-child::after {
          content: none;
        }

        .author-paper-item .paper-citations {
          font-weight: bold;
          color: var(--primary-color);
          min-width: 50px;
          text-align: right;
        }

        .no-data {
          text-align: center;
          color: var(--text-secondary);
          padding: 24px;
        }

        /* Clickable title in Top Citing Papers */
        .clickable-title {
          color: var(--primary-color);
          text-decoration: none;
          display: inline-flex;
          align-items: baseline;
          gap: 4px;
        }

        .clickable-title:hover {
          text-decoration: underline;
        }

        .external-link-icon {
          font-size: 0.75em;
          opacity: 0.6;
        }

        /* Clickable author name */
        .clickable-author {
          cursor: pointer;
          color: var(--text-secondary);
          transition: color 0.15s;
        }

        .clickable-author:hover {
          color: var(--primary-color);
          text-decoration: underline;
        }

        /* All authors button */
        .all-authors-btn {
          background: none;
          border: none;
          color: var(--text-muted);
          font-size: 0.8em;
          cursor: pointer;
          padding: 0 4px;
          margin-left: 6px;
          transition: color 0.15s;
        }

        .all-authors-btn:hover {
          color: var(--primary-color);
          text-decoration: underline;
        }

        /* Work status badge in paper details */
        .work-status-badge {
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.75em;
          font-weight: 500;
        }

        .work-status-badge.status-harvested {
          background: var(--success-bg);
          color: var(--success-color);
        }

        .work-status-badge.status-pending {
          background: var(--bg-tertiary);
          color: var(--text-muted);
        }

        /* Paper actions row */
        .paper-actions {
          display: flex;
          gap: 8px;
          margin-top: 10px;
        }

        .action-btn {
          padding: 4px 10px;
          font-size: 0.75em;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          background: var(--bg-secondary);
          color: var(--text-primary);
          cursor: pointer;
          transition: all 0.15s;
        }

        .action-btn:hover:not(:disabled) {
          background: var(--bg-primary);
          border-color: var(--primary-color);
        }

        .action-btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        .seed-btn:hover:not(:disabled) {
          background: color-mix(in srgb, green 10%, var(--bg-secondary));
          border-color: green;
        }

        .dossier-btn:hover:not(:disabled) {
          background: color-mix(in srgb, var(--primary-color) 10%, var(--bg-secondary));
        }

        /* Author Search Modal */
        .author-search-modal {
          max-width: 800px;
          width: 95%;
          max-height: 85vh;
          background: #1a1a1a;
        }

        .author-search-modal .modal-body {
          background: #1a1a1a;
        }

        .search-summary {
          font-size: 0.9em;
          color: var(--text-secondary);
          margin-bottom: 16px;
          padding: 8px 12px;
          background: #252525;
          border-radius: 4px;
        }

        .search-section {
          margin-bottom: 20px;
        }

        .search-section h4 {
          margin: 0 0 10px 0;
          font-size: 0.9em;
          color: var(--text-secondary);
          text-transform: uppercase;
          letter-spacing: 0.5px;
        }

        .search-papers-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .search-paper-item {
          display: flex;
          justify-content: space-between;
          align-items: start;
          gap: 12px;
          padding: 10px 12px;
          background: #252525;
          border-radius: 6px;
          border-left: 3px solid transparent;
        }

        .search-paper-item.from-current-thinker {
          border-left-color: var(--primary-color);
          background: #2a2a35;
        }

        .search-paper-info {
          flex: 1;
          min-width: 0;
        }

        .search-paper-title {
          font-weight: 500;
          display: block;
          margin-bottom: 4px;
          color: var(--text-primary);
        }

        a.search-paper-title {
          color: var(--primary-color);
          text-decoration: none;
        }

        a.search-paper-title:hover {
          text-decoration: underline;
        }

        .search-paper-meta {
          font-size: 0.8em;
          color: var(--text-secondary);
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }

        .search-paper-meta span::after {
          content: '·';
          margin-left: 8px;
        }

        .search-paper-meta span:last-child::after {
          content: none;
        }

        .thinker-tag {
          padding: 2px 6px;
          background: var(--bg-primary);
          border-radius: 4px;
          font-weight: 500;
        }

        .thinker-tag.current {
          background: color-mix(in srgb, var(--primary-color) 20%, transparent);
          color: var(--primary-color);
        }

        .search-paper-actions {
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .search-paper-citations {
          font-weight: 600;
          font-family: var(--font-mono);
          color: var(--primary-color);
        }

        .mini-action-btn {
          padding: 4px 8px;
          font-size: 0.9em;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          background: var(--bg-primary);
          cursor: pointer;
          transition: all 0.15s;
        }

        .mini-action-btn:hover:not(:disabled) {
          background: color-mix(in srgb, green 15%, var(--bg-primary));
          border-color: green;
        }

        .mini-action-btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        /* Dossier Selection Modal */
        .dossier-select-modal {
          max-width: 450px;
          width: 90%;
        }

        .dossier-modal-subtitle {
          font-size: 0.85em;
          color: var(--text-secondary);
          margin-bottom: 16px;
          padding-bottom: 12px;
          border-bottom: 1px solid var(--border-color);
        }

        .dossier-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
          max-height: 300px;
          overflow-y: auto;
          margin-bottom: 16px;
        }

        .dossier-option {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 12px 16px;
          border: 1px solid var(--border-color);
          border-radius: 6px;
          background: var(--bg-secondary);
          cursor: pointer;
          transition: all 0.15s;
          text-align: left;
        }

        .dossier-option:hover:not(:disabled) {
          border-color: var(--primary-color);
          background: color-mix(in srgb, var(--primary-color) 8%, var(--bg-secondary));
        }

        .dossier-option:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        .dossier-name {
          font-weight: 500;
        }

        .dossier-count {
          font-size: 0.8em;
          color: var(--text-secondary);
        }

        .create-seed-no-dossier {
          width: 100%;
          margin-top: 8px;
        }
      `}</style>
    </div>
  )
}

export default ThinkerDetail
