import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { api } from '../lib/api'

/**
 * Collection Detail - Shows papers in a collection
 */
export default function CollectionDetail({ collectionId, onBack }) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [refreshBatchId, setRefreshBatchId] = useState(null)
  const [refreshProgress, setRefreshProgress] = useState(null)

  const { data: collection, isLoading } = useQuery({
    queryKey: ['collection', collectionId],
    queryFn: () => api.getCollection(collectionId),
  })

  const removePaper = useMutation({
    mutationFn: (paperId) => api.assignPapersToCollection([paperId], null),
    onSuccess: () => queryClient.invalidateQueries(['collection', collectionId]),
  })

  // Refresh all papers in collection
  const refreshCollection = useMutation({
    mutationFn: () => api.refreshCollection(collectionId),
    onSuccess: (result) => {
      if (result.jobs_created > 0) {
        setRefreshBatchId(result.batch_id)
        setRefreshProgress({
          total: result.jobs_created,
          completed: 0,
          message: `Queued ${result.jobs_created} refresh jobs for ${result.papers_included} papers`,
        })
        queryClient.invalidateQueries(['jobs'])
      } else {
        setRefreshProgress({
          message: 'No papers need refreshing',
          done: true,
        })
        setTimeout(() => setRefreshProgress(null), 3000)
      }
    },
    onError: (error) => {
      setRefreshProgress({
        message: `Refresh failed: ${error.message}`,
        error: true,
      })
      setTimeout(() => setRefreshProgress(null), 5000)
    },
  })

  // Poll for refresh batch status
  useEffect(() => {
    if (!refreshBatchId) return

    const pollInterval = setInterval(async () => {
      try {
        const status = await api.getRefreshStatus(refreshBatchId)
        setRefreshProgress({
          total: status.total_jobs,
          completed: status.completed_jobs,
          newCitations: status.new_citations_added,
          message: `Refreshing: ${status.completed_jobs}/${status.total_jobs} done`,
          done: status.is_complete,
        })

        if (status.is_complete) {
          setRefreshBatchId(null)
          queryClient.invalidateQueries(['collection', collectionId])
          queryClient.invalidateQueries(['papers'])
          setRefreshProgress({
            total: status.total_jobs,
            completed: status.completed_jobs,
            newCitations: status.new_citations_added,
            message: `Done! Found ${status.new_citations_added} new citations`,
            done: true,
          })
          setTimeout(() => setRefreshProgress(null), 5000)
        }
      } catch (err) {
        console.error('Refresh status poll error:', err)
      }
    }, 3000)

    return () => clearInterval(pollInterval)
  }, [refreshBatchId, collectionId, queryClient])

  // Check if any papers have stale citations
  const hasStaleItems = collection?.papers?.some(p => p.is_stale)
  const hasHarvestedItems = collection?.papers?.some(p => p.total_harvested_citations > 0)

  if (isLoading) {
    return <div className="loading">Loading collection...</div>
  }

  if (!collection) {
    return <div className="error">Collection not found</div>
  }

  return (
    <div className="collection-detail">
      <header className="collection-detail-header">
        <button onClick={onBack} className="btn-text">← Collections</button>
        <div className="collection-info">
          <div className="collection-title-row">
            <span className="color-indicator" style={{ backgroundColor: collection.color }} />
            <h2>{collection.name}</h2>
          </div>
          <p className="description">{collection.description}</p>
          <span className="paper-count">{collection.paper_count} papers</span>
        </div>
        {/* Refresh All button */}
        {hasHarvestedItems && (
          <button
            onClick={() => refreshCollection.mutate()}
            disabled={refreshCollection.isPending || !!refreshBatchId}
            className={`btn-refresh ${hasStaleItems ? 'stale' : ''}`}
            title={hasStaleItems
              ? 'Some papers have stale citations - click to refresh all'
              : 'Check for new citations across all papers'}
          >
            {refreshBatchId ? '🔄 Refreshing...' : '🔄 Refresh All'}
          </button>
        )}
      </header>

      {/* Refresh Progress */}
      {refreshProgress && (
        <div className={`collection-refresh-progress ${refreshProgress.done ? 'done' : ''} ${refreshProgress.error ? 'error' : ''}`}>
          {refreshProgress.total > 0 && !refreshProgress.done && (
            <div className="progress-bar">
              <div className="progress-fill" style={{ width: `${(refreshProgress.completed / refreshProgress.total) * 100}%` }} />
            </div>
          )}
          <span className="progress-message">
            {refreshProgress.message}
            {refreshProgress.newCitations > 0 && ` (${refreshProgress.newCitations} new)`}
          </span>
        </div>
      )}

      {collection.papers?.length === 0 ? (
        <div className="empty">
          <p>No papers in this collection yet.</p>
          <p>Go to Papers tab and add papers, or assign existing papers to this collection.</p>
        </div>
      ) : (
        <div className="papers-list">
          <table className="papers-table">
            <thead>
              <tr>
                <th>Title</th>
                <th>Authors</th>
                <th>Year</th>
                <th>Status</th>
                <th>Editions</th>
                <th>Total Citations</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {collection.papers?.map(paper => (
                <tr key={paper.id}>
                  <td className="col-title">
                    <a
                      href="#"
                      onClick={(e) => { e.preventDefault(); navigate(`/paper/${paper.id}`); }}
                    >
                      {paper.title}
                    </a>
                    {paper.canonical_edition && paper.canonical_edition.language && (
                      <span className="canonical-lang" title={`Canonical edition: ${paper.canonical_edition.title}`}>
                        ({paper.canonical_edition.language})
                      </span>
                    )}
                  </td>
                  <td className="col-authors">{paper.authors || '–'}</td>
                  <td className="col-year">{paper.year || '–'}</td>
                  <td className="col-status">
                    <span className={`status-badge status-${paper.status}`}>
                      {paper.status}
                    </span>
                  </td>
                  <td className="col-editions">{paper.edition_count || 0}</td>
                  <td className="col-citations">
                    <span className="citation-count" title="Sum of citations across all editions">
                      {paper.total_edition_citations?.toLocaleString() || 0}
                    </span>
                    {paper.canonical_edition && paper.edition_count > 1 && (
                      <span className="canonical-citation" title={`Top edition: ${paper.canonical_edition.citation_count.toLocaleString()}`}>
                        (top: {paper.canonical_edition.citation_count.toLocaleString()})
                      </span>
                    )}
                  </td>
                  <td className="col-actions">
                    <button
                      className="btn-icon btn-danger"
                      onClick={() => removePaper.mutate(paper.id)}
                      title="Remove from collection"
                    >
                      ×
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
