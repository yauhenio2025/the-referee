import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperList({ onSelectPaper }) {
  const queryClient = useQueryClient()

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
          return (
            <div key={paper.id} className="paper-card">
              <div className="paper-header">
                <h3 className="paper-title">{paper.title}</h3>
                <span className={`badge ${badge.class}`}>{badge.label}</span>
              </div>
              <div className="paper-meta">
                {paper.authors && <span className="paper-authors">{paper.authors}</span>}
                {paper.year && <span className="paper-year">({paper.year})</span>}
                {paper.citation_count > 0 && (
                  <span className="paper-citations">{paper.citation_count} citations</span>
                )}
              </div>
              <div className="paper-actions">
                <button
                  onClick={() => onSelectPaper(paper)}
                  disabled={paper.status !== 'resolved'}
                  className="btn-primary"
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
