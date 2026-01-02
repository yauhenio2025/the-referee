import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function ForeignEditionsNeeded({ onSelectPaper }) {
  const queryClient = useQueryClient()
  const [page, setPage] = useState(1)
  const perPage = 25

  const { data, isLoading, error } = useQuery({
    queryKey: ['papers-foreign-edition', page, perPage],
    queryFn: () => api.listPapersNeedingForeignEdition(page, perPage),
    keepPreviousData: true,
  })

  const toggleMutation = useMutation({
    mutationFn: ({ paperId, needed }) => api.toggleForeignEditionNeeded(paperId, needed),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers-foreign-edition'])
      queryClient.invalidateQueries(['papers'])
    },
  })

  const handleRemoveFromList = (paper) => {
    toggleMutation.mutate({ paperId: paper.id, needed: false })
  }

  if (isLoading) {
    return <div className="loading">Loading papers needing foreign editions...</div>
  }

  if (error) {
    return <div className="error">Error loading papers: {error.message}</div>
  }

  const papers = data?.papers || []
  const { total = 0, total_pages = 1, has_next = false, has_prev = false } = data || {}

  return (
    <div className="foreign-editions-needed">
      <div className="section-header">
        <h2>📕 Foreign Editions Needed</h2>
        <span className="count-badge">{total} paper{total !== 1 ? 's' : ''}</span>
      </div>

      <p className="section-description">
        Papers marked as needing foreign edition lookup. Click the ✓ button to mark as resolved.
      </p>

      {papers.length === 0 ? (
        <div className="empty-state">
          <span className="empty-icon">✅</span>
          <p>No papers currently need foreign editions!</p>
        </div>
      ) : (
        <>
          <div className="paper-list">
            {papers.map((paper) => (
              <div key={paper.id} className="paper-card foreign-edition-card">
                <div className="paper-main" onClick={() => onSelectPaper && onSelectPaper(paper)}>
                  <div className="paper-title">{paper.title}</div>
                  <div className="paper-meta">
                    <span className="paper-author">{paper.author}</span>
                    {paper.year && <span className="paper-year">({paper.year})</span>}
                  </div>
                  {paper.harvest_expected > 0 && (
                    <div className="harvest-info">
                      <div className="harvest-bar">
                        <div
                          className="harvest-progress"
                          style={{
                            width: `${paper.harvest_percent}%`,
                            backgroundColor: paper.harvest_percent >= 80 ? '#22c55e' :
                                           paper.harvest_percent >= 50 ? '#eab308' : '#ef4444'
                          }}
                        />
                      </div>
                      <span className="harvest-text">
                        {paper.harvest_actual}/{paper.harvest_expected} citations ({paper.harvest_percent.toFixed(0)}%)
                      </span>
                    </div>
                  )}
                </div>
                <div className="paper-actions">
                  <button
                    className="btn btn-sm btn-success"
                    onClick={(e) => {
                      e.stopPropagation()
                      handleRemoveFromList(paper)
                    }}
                    disabled={toggleMutation.isLoading}
                    title="Mark as resolved (remove from list)"
                  >
                    ✓ Resolved
                  </button>
                </div>
              </div>
            ))}
          </div>

          {/* Pagination */}
          {total_pages > 1 && (
            <div className="pagination">
              <button
                className="btn btn-sm"
                onClick={() => setPage(p => p - 1)}
                disabled={!has_prev}
              >
                ← Previous
              </button>
              <span className="page-info">
                Page {page} of {total_pages}
              </span>
              <button
                className="btn btn-sm"
                onClick={() => setPage(p => p + 1)}
                disabled={!has_next}
              >
                Next →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  )
}
