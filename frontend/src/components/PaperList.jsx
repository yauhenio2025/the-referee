import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperList({ onSelectPaper }) {
  const queryClient = useQueryClient()
  const [resolvingId, setResolvingId] = useState(null)
  const [expandedAbstracts, setExpandedAbstracts] = useState({})

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

  const toggleAbstract = (paperId) => {
    setExpandedAbstracts(prev => ({
      ...prev,
      [paperId]: !prev[paperId]
    }))
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
      <div className="papers">
        {papers.map((paper) => {
          const badge = getStatusBadge(paper.status)
          const isResolving = resolvingId === paper.id
          const isExpanded = expandedAbstracts[paper.id]

          return (
            <div key={paper.id} className={`paper-card ${paper.status === 'resolved' ? 'paper-card-resolved' : ''}`}>
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
