import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

/**
 * Collections Management - View and manage paper collections
 */
export default function Collections({ onSelectCollection }) {
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [newCollection, setNewCollection] = useState({ name: '', description: '', color: '#3182CE' })
  const queryClient = useQueryClient()

  const { data: collections, isLoading } = useQuery({
    queryKey: ['collections'],
    queryFn: () => api.getCollections(),
  })

  const createCollection = useMutation({
    mutationFn: (data) => api.createCollection(data),
    onSuccess: () => {
      queryClient.invalidateQueries(['collections'])
      setShowCreateModal(false)
      setNewCollection({ name: '', description: '', color: '#3182CE' })
    },
  })

  const deleteCollection = useMutation({
    mutationFn: (id) => api.deleteCollection(id),
    onSuccess: () => queryClient.invalidateQueries(['collections']),
  })

  const colors = [
    '#E53E3E', '#DD6B20', '#D69E2E', '#38A169', '#319795',
    '#3182CE', '#5A67D8', '#805AD5', '#D53F8C', '#718096'
  ]

  return (
    <div className="collections-view">
      <header className="collections-header">
        <h2>Collections</h2>
        <button onClick={() => setShowCreateModal(true)} className="btn-primary">
          + New Collection
        </button>
      </header>

      {isLoading ? (
        <div className="loading">Loading collections...</div>
      ) : !collections?.length ? (
        <div className="empty">
          <p>No collections yet. Create one to organize your papers.</p>
        </div>
      ) : (
        <div className="collections-grid">
          {collections.map(collection => (
            <div
              key={collection.id}
              className="collection-card"
              style={{ borderLeftColor: collection.color || '#3182CE' }}
            >
              <div className="collection-header">
                <h3 onClick={() => onSelectCollection?.(collection)} style={{ cursor: 'pointer' }}>
                  {collection.name}
                </h3>
                <button
                  className="btn-icon btn-danger"
                  onClick={() => {
                    if (confirm(`Delete collection "${collection.name}"? Papers will be unassigned but not deleted.`)) {
                      deleteCollection.mutate(collection.id)
                    }
                  }}
                  title="Delete collection"
                >
                  ×
                </button>
              </div>
              <p className="collection-desc">{collection.description || 'No description'}</p>
              <div className="collection-stats">
                <span className="paper-count">{collection.paper_count} papers</span>
                <span
                  className="color-dot"
                  style={{ backgroundColor: collection.color || '#3182CE' }}
                />
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Create Modal */}
      {showCreateModal && (
        <div className="modal-overlay" onClick={() => setShowCreateModal(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <h3>Create Collection</h3>

            <div className="form-group">
              <label>Name</label>
              <input
                type="text"
                value={newCollection.name}
                onChange={e => setNewCollection({ ...newCollection, name: e.target.value })}
                placeholder="e.g., Platform Capitalism Research"
                autoFocus
              />
            </div>

            <div className="form-group">
              <label>Description</label>
              <textarea
                value={newCollection.description}
                onChange={e => setNewCollection({ ...newCollection, description: e.target.value })}
                placeholder="What papers belong in this collection?"
                rows={3}
              />
            </div>

            <div className="form-group">
              <label>Color</label>
              <div className="color-picker">
                {colors.map(color => (
                  <button
                    key={color}
                    className={`color-option ${newCollection.color === color ? 'selected' : ''}`}
                    style={{ backgroundColor: color }}
                    onClick={() => setNewCollection({ ...newCollection, color })}
                  />
                ))}
              </div>
            </div>

            <div className="modal-footer">
              <button onClick={() => setShowCreateModal(false)}>Cancel</button>
              <button
                onClick={() => createCollection.mutate(newCollection)}
                disabled={!newCollection.name.trim() || createCollection.isPending}
                className="btn-primary"
              >
                {createCollection.isPending ? 'Creating...' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
