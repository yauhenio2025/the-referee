import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { api } from '../lib/api'

/**
 * Collection Detail - Shows papers in a collection
 */
export default function CollectionDetail({ collectionId, onBack }) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const { data: collection, isLoading } = useQuery({
    queryKey: ['collection', collectionId],
    queryFn: () => api.getCollection(collectionId),
  })

  const removePaper = useMutation({
    mutationFn: (paperId) => api.assignPapersToCollection([paperId], null),
    onSuccess: () => queryClient.invalidateQueries(['collection', collectionId]),
  })

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
      </header>

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
