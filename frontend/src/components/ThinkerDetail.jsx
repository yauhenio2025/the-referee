/**
 * Thinker Detail Component - Shows thinker info, works, and actions
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import { useToast } from './Toast'

function ThinkerDetail({ thinkerId, onBack }) {
  const [activeTab, setActiveTab] = useState('works')
  const [workFilter, setWorkFilter] = useState('accepted')
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
      `}</style>
    </div>
  )
}

export default ThinkerDetail
