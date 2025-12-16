import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperList({ onSelectPaper }) {
  const queryClient = useQueryClient()
  const [resolvingId, setResolvingId] = useState(null)

  const { data: papers, isLoading, error } = useQuery({
    queryKey: ['papers'],
    queryFn: () => api.listPapers(),
  })

  const deletePaper = useMutation({
    mutationFn: (paperId) => api.deletePaper(paperId),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
    },
  })

  const resolvePaper = useMutation({
    mutationFn: (paperId) => api.resolvePaper(paperId),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
      setResolvingId(null)
    },
    onError: () => {
      setResolvingId(null)
    },
  })

  const handleResolve = (paperId) => {
    setResolvingId(paperId)
    resolvePaper.mutate(paperId)
  }

  if (isLoading) return <div className="loading">Loading papers...</div>
  if (error) return <div className="error">Error loading papers: {error.message}</div>
  if (!papers?.length) return <div className="empty">No papers yet. Add one above!</div>

  const getStatusBadge = (status) => {
    const badges = {
      pending: { label: 'Pending', class: 'badge-pending' },
      resolved: { label: 'Resolved', class: 'badge-success' },
      error: { label: 'Error', class: 'badge-error' },
    }
    return badges[status] || badges.pending
  }

  return (
    <div className="paper-list">
      <h2>Papers ({papers.length})</h2>
      <div className="papers">
        {papers.map((paper) => {
          const badge = getStatusBadge(paper.status)
          const isResolving = resolvingId === paper.id
          return (
            <div key={paper.id} className="paper-card">
              <div className="paper-header">
                <h3 className="paper-title">{paper.title}</h3>
                <span className={`badge ${badge.class}`}>
                  {isResolving ? '🔄 Resolving...' : badge.label}
                </span>
              </div>
              <div className="paper-meta">
                {paper.authors && <span className="paper-authors">{paper.authors}</span>}
                {paper.year && <span className="paper-year">({paper.year})</span>}
                {paper.venue && <span className="paper-venue">• {paper.venue}</span>}
                {paper.citation_count > 0 && (
                  <span className="paper-citations">📚 {paper.citation_count} citations</span>
                )}
              </div>
              {paper.scholar_id && (
                <div className="paper-scholar-id">
                  Scholar ID: {paper.scholar_id}
                </div>
              )}
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
                  className="btn-primary"
                  title={paper.status !== 'resolved' ? 'Resolve paper first' : 'Discover all editions'}
                >
                  📖 Discover Editions
                </button>
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
    </div>
  )
}
