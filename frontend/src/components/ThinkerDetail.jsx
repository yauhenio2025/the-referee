/**
 * Thinker Detail Component - Shows thinker info, works, and actions
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import { useToast } from './Toast'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

function ThinkerDetail({ thinkerId, onBack }) {
  const [activeTab, setActiveTab] = useState('works')
  const [workFilter, setWorkFilter] = useState('accepted')
  const [selectedAuthor, setSelectedAuthor] = useState(null)  // For author papers modal
  const [authorPapers, setAuthorPapers] = useState([])
  const [loadingAuthorPapers, setLoadingAuthorPapers] = useState(false)
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

  // Fetch papers for a specific citing author
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

  if (isLoading) return <div className="loading">Loading thinker...</div>
  if (error) return <div className="error">Error: {error.message}</div>
  if (!thinker) return <div className="error">Thinker not found</div>

  // Handle both array (from API) and JSON string (from DB) formats
  const parseJsonField = (field) => {
    if (!field) return []
    if (Array.isArray(field)) return field
    try { return JSON.parse(field) } catch { return [] }
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
                      This thinker's works ranked by how many papers cite them
                    </p>
                    <div className="works-list-header">
                      <span className="header-rank">#</span>
                      <span className="header-work">Work</span>
                      <span className="header-citations" title="Number of papers that cite this work">Citations</span>
                    </div>
                    <div className="works-list">
                      {analytics.most_cited_works.slice(0, 10).map((work, i) => (
                        <div key={work.work_id} className="work-list-item">
                          <span className="work-rank">#{i + 1}</span>
                          <div className="work-info">
                            <span className="work-title-text">{work.title}</span>
                            <span className="work-year">{work.year || 'n.d.'}</span>
                          </div>
                          <span className="work-citations" title={`${work.citations_received} papers cite this work`}>
                            {work.citations_received?.toLocaleString()}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Top Citing Papers */}
                {analytics.top_citing_papers?.length > 0 && (
                  <div className="analytics-card">
                    <h3>Top Citing Papers</h3>
                    <p className="card-subtitle">
                      Papers by other scholars that cite this thinker's work, ranked by their own influence (citation count)
                    </p>
                    <div className="top-citing-papers-list">
                      {analytics.top_citing_papers.slice(0, 10).map((paper, i) => (
                        <div key={i} className="top-citing-paper">
                          <div className="paper-rank-badge">#{i + 1}</div>
                          <div className="paper-content">
                            <div className="paper-title-row">
                              <span className="paper-title">{paper.title || 'Untitled'}</span>
                            </div>
                            <div className="paper-byline">
                              <span className="paper-authors">{paper.authors || 'Unknown authors'}</span>
                            </div>
                            <div className="paper-details">
                              {paper.venue && <span className="paper-venue">{paper.venue}</span>}
                              {paper.year && <span className="paper-year">{paper.year}</span>}
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

                {/* Author Papers Modal */}
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

        /* Most Cited Works - Table Style */
        .works-list-header {
          display: grid;
          grid-template-columns: 50px 1fr 100px;
          gap: 12px;
          padding: 10px 12px;
          border-bottom: 2px solid var(--border-color);
          font-size: 0.75em;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.5px;
          color: var(--text-secondary);
        }

        .header-citations {
          text-align: right;
          cursor: help;
        }

        .works-list {
          display: flex;
          flex-direction: column;
        }

        .work-list-item {
          display: grid;
          grid-template-columns: 50px 1fr 100px;
          gap: 12px;
          padding: 14px 12px;
          border-bottom: 1px solid var(--border-color);
          align-items: center;
        }

        .work-list-item:last-child {
          border-bottom: none;
        }

        .work-rank {
          font-weight: 600;
          color: var(--text-secondary);
          font-size: 0.9em;
        }

        .work-info {
          min-width: 0;
          display: flex;
          flex-direction: column;
          gap: 2px;
        }

        .work-title-text {
          font-weight: 500;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
        }

        .work-year {
          font-size: 0.85em;
          color: var(--text-secondary);
        }

        .work-citations {
          text-align: right;
          font-weight: 600;
          font-family: var(--font-mono);
          font-size: 1.1em;
          color: var(--primary-color);
          cursor: help;
        }

        /* Top Citing Papers - Card Style */
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

        /* Modal styles */
        .modal-overlay {
          position: fixed;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background: rgba(0, 0, 0, 0.6);
          display: flex;
          align-items: center;
          justify-content: center;
          z-index: 1000;
        }

        .modal-content {
          background: var(--bg-primary);
          border-radius: 12px;
          max-width: 700px;
          width: 90%;
          max-height: 80vh;
          overflow: hidden;
          display: flex;
          flex-direction: column;
          box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
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
          background: var(--bg-secondary);
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
      `}</style>
    </div>
  )
}

export default ThinkerDetail
