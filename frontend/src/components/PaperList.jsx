import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperList({ onSelectPaper }) {
  const queryClient = useQueryClient()
  const [resolvingId, setResolvingId] = useState(null)
  const [expandedAbstracts, setExpandedAbstracts] = useState({})
  const [reconciliationPaper, setReconciliationPaper] = useState(null)
  const [editionCounts, setEditionCounts] = useState({})
  const [refreshingPapers, setRefreshingPapers] = useState({})
  const [quickHarvestingPapers, setQuickHarvestingPapers] = useState({})

  const { data: papers, isLoading, error } = useQuery({
    queryKey: ['papers'],
    queryFn: async () => {
      const papersData = await api.listPapers()
      // Fetch edition counts for resolved papers
      const counts = {}
      for (const paper of papersData.filter(p => p.status === 'resolved')) {
        try {
          const editions = await api.getPaperEditions(paper.id)
          counts[paper.id] = editions?.length || 0
        } catch (e) {
          counts[paper.id] = 0
        }
      }
      setEditionCounts(counts)
      return papersData
    },
  })

  const deletePaper = useMutation({
    mutationFn: (paperId) => api.deletePaper(paperId),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
    },
  })

  const resolvePaper = useMutation({
    mutationFn: (paperId) => api.resolvePaper(paperId),
    onSuccess: (data, paperId) => {
      if (data.needs_reconciliation && data.candidates) {
        // Show reconciliation modal
        const paper = papers.find(p => p.id === paperId)
        setReconciliationPaper({
          ...paper,
          candidates: data.candidates,
        })
      }
      queryClient.invalidateQueries(['papers'])
      setResolvingId(null)
    },
    onError: () => {
      setResolvingId(null)
    },
  })

  const confirmCandidate = useMutation({
    mutationFn: ({ paperId, candidateIndex }) => api.confirmCandidate(paperId, candidateIndex),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
      setReconciliationPaper(null)
    },
  })

  const addAsSeed = useMutation({
    mutationFn: (candidate) => api.createPaper({
      title: candidate.title,
      authors: candidate.authorsRaw || candidate.authors,
      year: candidate.year,
      venue: candidate.venue,
    }),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
    },
  })

  const refreshPaper = useMutation({
    mutationFn: (paperId) => api.refreshPaper(paperId),
    onMutate: (paperId) => {
      setRefreshingPapers(prev => ({ ...prev, [paperId]: true }))
    },
    onSuccess: (data, paperId) => {
      if (data.jobs_created > 0) {
        // Start polling for refresh status
        setRefreshingPapers(prev => ({ ...prev, [paperId]: data.batch_id }))
      } else {
        setRefreshingPapers(prev => {
          const next = { ...prev }
          delete next[paperId]
          return next
        })
      }
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['jobs'])
    },
    onError: (error, paperId) => {
      console.error('Refresh failed:', error)
      setRefreshingPapers(prev => {
        const next = { ...prev }
        delete next[paperId]
        return next
      })
    },
  })

  const quickHarvest = useMutation({
    mutationFn: (paperId) => api.quickHarvest(paperId),
    onMutate: (paperId) => {
      setQuickHarvestingPapers(prev => ({ ...prev, [paperId]: true }))
    },
    onSuccess: (data, paperId) => {
      setQuickHarvestingPapers(prev => {
        const next = { ...prev }
        delete next[paperId]
        return next
      })
      // Update edition count since we just created one
      setEditionCounts(prev => ({ ...prev, [paperId]: (prev[paperId] || 0) + (data.edition_created ? 1 : 0) }))
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['jobs'])
    },
    onError: (error, paperId) => {
      console.error('Quick harvest failed:', error)
      setQuickHarvestingPapers(prev => {
        const next = { ...prev }
        delete next[paperId]
        return next
      })
    },
  })

  const handleResolve = (paperId) => {
    setResolvingId(paperId)
    resolvePaper.mutate(paperId)
  }

  const handleSelectCandidate = (candidateIndex) => {
    if (reconciliationPaper) {
      confirmCandidate.mutate({
        paperId: reconciliationPaper.id,
        candidateIndex,
      })
    }
  }

  const handleAddAsSeed = (candidate, e) => {
    e.stopPropagation() // Don't trigger the card click
    addAsSeed.mutate(candidate)
  }

  const toggleAbstract = (paperId) => {
    setExpandedAbstracts(prev => ({
      ...prev,
      [paperId]: !prev[paperId]
    }))
  }

  // Check for papers needing reconciliation on load
  const papersNeedingReconciliation = papers?.filter(p => p.status === 'needs_reconciliation') || []

  if (isLoading) return <div className="loading">Loading papers...</div>
  if (error) return <div className="error">Error loading papers: {error.message}</div>
  if (!papers?.length) return <div className="empty">No papers yet. Add one above!</div>

  const getStatusBadge = (status) => {
    const badges = {
      pending: { label: 'Pending', class: 'badge-pending' },
      needs_reconciliation: { label: '⚠️ Choose Match', class: 'badge-warning' },
      resolved: { label: 'Resolved', class: 'badge-success' },
      error: { label: 'Error', class: 'badge-error' },
    }
    return badges[status] || badges.pending
  }

  // Format authors nicely
  const formatAuthors = (authors) => {
    if (!authors) return null
    if (Array.isArray(authors)) {
      if (authors.length > 3) {
        return authors.slice(0, 3).join(', ') + ' et al.'
      }
      return authors.join(', ')
    }
    // String - might be comma-separated or raw
    return authors.replace(/([a-z])([A-Z])/g, '$1 $2').trim()
  }

  return (
    <div className="paper-list">
      <h2>Papers ({papers.length})</h2>

      {/* Alert for papers needing reconciliation */}
      {papersNeedingReconciliation.length > 0 && (
        <div className="reconciliation-alert">
          <span className="alert-icon">⚠️</span>
          <span>{papersNeedingReconciliation.length} paper(s) need your attention - multiple Scholar matches found</span>
        </div>
      )}

      <div className="papers">
        {papers.map((paper) => {
          const badge = getStatusBadge(paper.status)
          const isResolving = resolvingId === paper.id
          const isExpanded = expandedAbstracts[paper.id]
          const needsReconciliation = paper.status === 'needs_reconciliation'

          return (
            <div key={paper.id} className={`paper-card ${paper.status === 'resolved' ? 'paper-card-resolved' : ''} ${needsReconciliation ? 'paper-card-warning' : ''}`}>
              {/* Header with title and status */}
              <div className="paper-header">
                {paper.link ? (
                  <a
                    href={paper.link}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="paper-title-link"
                  >
                    <h3 className="paper-title">{paper.title}</h3>
                  </a>
                ) : (
                  <h3 className="paper-title">{paper.title}</h3>
                )}
                <span className={`badge ${badge.class}`}>
                  {isResolving ? '🔄 Resolving...' : badge.label}
                </span>
              </div>

              {/* Metadata row: authors, year, venue */}
              <div className="paper-meta">
                {formatAuthors(paper.authors) && (
                  <span className="paper-authors">{formatAuthors(paper.authors)}</span>
                )}
                {paper.year && (
                  <span className="paper-year">
                    {formatAuthors(paper.authors) ? ' · ' : ''}{paper.year}
                  </span>
                )}
                {paper.venue && (
                  <span className="paper-venue">
                    {' · '}{paper.venue.length > 50 ? paper.venue.substring(0, 50) + '...' : paper.venue}
                  </span>
                )}
              </div>

              {/* Citation count and Scholar ID for resolved papers */}
              {paper.status === 'resolved' && (
                <div className="paper-stats">
                  {paper.citation_count > 0 && (
                    <span className="paper-citations">
                      📚 {paper.citation_count.toLocaleString()} citations
                    </span>
                  )}
                  {/* Harvest stats and staleness indicator */}
                  {paper.total_harvested_citations > 0 && (
                    <span className="paper-harvested">
                      📥 {paper.total_harvested_citations.toLocaleString()} harvested
                    </span>
                  )}
                  {paper.is_stale && (
                    <span className="staleness-badge stale" title={`Last harvested ${paper.days_since_harvest} days ago`}>
                      ⏰ Stale ({paper.days_since_harvest}d)
                    </span>
                  )}
                  {paper.any_edition_harvested_at && !paper.is_stale && (
                    <span className="staleness-badge fresh" title={`Last harvested ${paper.days_since_harvest} days ago`}>
                      ✓ Fresh
                    </span>
                  )}
                  {paper.total_harvested_citations > 0 && !paper.any_edition_harvested_at && (
                    <span className="staleness-badge never" title="Never harvested - run citation extraction first">
                      ⚠ Never harvested
                    </span>
                  )}
                  {paper.scholar_id && (
                    <span className="paper-scholar-id">
                      ID: {paper.scholar_id}
                    </span>
                  )}
                  {paper.abstract && (
                    <span className="paper-has-abstract">
                      📄 Has abstract
                    </span>
                  )}
                </div>
              )}

              {/* Abstract - expandable */}
              {paper.abstract && (
                <div
                  className={`paper-abstract ${isExpanded ? 'paper-abstract-expanded' : ''}`}
                  onClick={() => toggleAbstract(paper.id)}
                  title={isExpanded ? "Click to collapse" : "Click to expand abstract"}
                >
                  <span className="abstract-toggle">
                    {isExpanded ? '▼' : '▶'}
                  </span>
                  <span className="abstract-text">
                    {isExpanded
                      ? paper.abstract
                      : (paper.abstract.length > 150
                          ? paper.abstract.substring(0, 150) + '...'
                          : paper.abstract)}
                  </span>
                </div>
              )}

              {/* Actions */}
              <div className="paper-actions">
                {paper.status === 'pending' && (
                  <button
                    onClick={() => handleResolve(paper.id)}
                    disabled={isResolving}
                    className="btn-resolve"
                  >
                    {isResolving ? '🔄 Resolving...' : '🔍 Resolve on Scholar'}
                  </button>
                )}
                {paper.status === 'needs_reconciliation' && (
                  <button
                    onClick={() => {
                      // Handle candidates - could be array (already parsed) or string (legacy)
                      let candidates = []
                      if (paper.candidates) {
                        candidates = typeof paper.candidates === 'string'
                          ? JSON.parse(paper.candidates)
                          : paper.candidates
                      }
                      setReconciliationPaper({ ...paper, candidates })
                    }}
                    className="btn-reconcile"
                  >
                    ⚠️ Choose Correct Match
                  </button>
                )}
                {paper.status === 'error' && (
                  <button
                    onClick={() => handleResolve(paper.id)}
                    disabled={isResolving}
                    className="btn-resolve"
                  >
                    {isResolving ? '🔄 Retrying...' : '🔄 Retry Resolution'}
                  </button>
                )}
                <button
                  onClick={() => onSelectPaper(paper)}
                  disabled={paper.status !== 'resolved'}
                  className={editionCounts[paper.id] > 0 ? 'btn-success' : 'btn-primary'}
                  title={paper.status !== 'resolved' ? 'Resolve paper first' : editionCounts[paper.id] > 0 ? 'View discovered editions' : 'Discover all editions'}
                >
                  {editionCounts[paper.id] > 0
                    ? `📖 View ${editionCounts[paper.id]} Editions`
                    : '📖 Discover Editions'}
                </button>
                {/* Quick Harvest - skip edition discovery and harvest directly */}
                {paper.status === 'resolved' && !editionCounts[paper.id] && paper.total_harvested_citations === 0 && (
                  <button
                    onClick={() => quickHarvest.mutate(paper.id)}
                    disabled={!!quickHarvestingPapers[paper.id]}
                    className="btn-quick-harvest"
                    title="Skip edition discovery and harvest citations directly from this paper's Scholar entry"
                  >
                    {quickHarvestingPapers[paper.id] ? '⚡ Harvesting...' : '⚡ Quick Harvest'}
                  </button>
                )}
                {/* Refresh button - show for papers with harvested citations */}
                {paper.status === 'resolved' && paper.total_harvested_citations > 0 && (
                  <button
                    onClick={() => refreshPaper.mutate(paper.id)}
                    disabled={!!refreshingPapers[paper.id] || refreshPaper.isPending}
                    className={`btn-refresh ${paper.is_stale ? 'stale' : ''}`}
                    title={paper.is_stale
                      ? `Refresh citations (${paper.days_since_harvest} days since last harvest)`
                      : 'Refresh citations to find new ones'}
                  >
                    {refreshingPapers[paper.id] ? '🔄 Refreshing...' : '🔄 Refresh'}
                  </button>
                )}
                <button
                  onClick={() => deletePaper.mutate(paper.id)}
                  className="btn-danger"
                  disabled={deletePaper.isPending}
                >
                  🗑️ Delete
                </button>
              </div>
            </div>
          )
        })}
      </div>

      {/* Reconciliation Modal */}
      {reconciliationPaper && (
        <div className="modal-overlay">
          <div className="modal reconciliation-modal">
            <h3>🔍 Select the Correct Paper</h3>
            <p className="reconciliation-hint">
              Multiple matches were found for "<strong>{reconciliationPaper.title}</strong>".
              Please select the correct one:
            </p>

            <div className="candidates-list">
              {reconciliationPaper.candidates?.map((candidate, index) => (
                <div
                  key={index}
                  className="candidate-card"
                  onClick={() => handleSelectCandidate(index)}
                >
                  <div className="candidate-index">{index + 1}</div>
                  <div className="candidate-info">
                    <h4 className="candidate-title">
                      {candidate.link ? (
                        <a href={candidate.link} target="_blank" rel="noopener noreferrer" onClick={(e) => e.stopPropagation()}>
                          {candidate.title}
                        </a>
                      ) : (
                        candidate.title
                      )}
                    </h4>
                    <div className="candidate-meta">
                      {(candidate.authorsRaw || candidate.authors) && (
                        <span className="candidate-authors">
                          {candidate.authorsRaw || (Array.isArray(candidate.authors) ? candidate.authors.join(', ') : candidate.authors)}
                        </span>
                      )}
                      {candidate.year && <span className="candidate-year">({candidate.year})</span>}
                      {candidate.venue && <span className="candidate-venue">{candidate.venue}</span>}
                    </div>
                    <div className="candidate-stats">
                      <span className="candidate-citations">
                        📚 {(candidate.citationCount || candidate.citation_count || 0).toLocaleString()} citations
                      </span>
                      {candidate.scholarId && (
                        <span className="candidate-scholar-id">ID: {candidate.scholarId}</span>
                      )}
                    </div>
                    {candidate.abstract && (
                      <p className="candidate-abstract">
                        {candidate.abstract.substring(0, 200)}...
                      </p>
                    )}
                  </div>
                  <div className="candidate-actions">
                    <button className="btn-select-candidate">
                      Select
                    </button>
                    <button
                      className="btn-add-seed"
                      onClick={(e) => handleAddAsSeed(candidate, e)}
                      disabled={addAsSeed.isPending}
                      title="Add this paper as a new seed to harvest"
                    >
                      {addAsSeed.isPending ? '➕ Adding...' : '➕ Add as Seed'}
                    </button>
                  </div>
                </div>
              ))}
            </div>

            <div className="modal-actions">
              <button onClick={() => setReconciliationPaper(null)} className="btn-secondary">
                Cancel
              </button>
              <button
                onClick={() => deletePaper.mutate(reconciliationPaper.id).then(() => setReconciliationPaper(null))}
                className="btn-danger"
              >
                Delete Paper
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
