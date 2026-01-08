/**
 * Thinkers List Component - Thinker Bibliographies Feature
 * Lists all thinkers and provides quick-add functionality
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import { useToast } from './Toast'

function Thinkers({ onSelectThinker }) {
  const [showAddDialog, setShowAddDialog] = useState(false)
  const [addInput, setAddInput] = useState('')
  const [scholarProfileUrl, setScholarProfileUrl] = useState('')
  const [isQuickAdd, setIsQuickAdd] = useState(true)
  const queryClient = useQueryClient()
  const { showToast } = useToast()

  // Fetch thinkers list
  const { data: thinkers, isLoading, error } = useQuery({
    queryKey: ['thinkers'],
    queryFn: () => api.getThinkers(),
  })

  // Quick-add mutation
  const quickAddMutation = useMutation({
    mutationFn: (query) => api.quickAddThinker(query),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['thinkers'] })
      setShowAddDialog(false)
      setAddInput('')
      if (data.disambiguation_required) {
        showToast('Thinker created - disambiguation needed', 'info')
        onSelectThinker?.(data.thinker)
      } else {
        showToast(`Thinker "${data.thinker.canonical_name}" added`, 'success')
      }
    },
    onError: (err) => {
      showToast(`Failed to add thinker: ${err.message}`, 'error')
    },
  })

  // Create thinker mutation
  const createMutation = useMutation({
    mutationFn: ({ name, scholarProfileUrl }) => api.createThinker(name, scholarProfileUrl || null),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['thinkers'] })
      setShowAddDialog(false)
      setAddInput('')
      setScholarProfileUrl('')
      const seededMsg = data.works_discovered > 0 ? ` (${data.works_discovered} works from profile)` : ''
      showToast(`Thinker "${data.canonical_name}" created${seededMsg}`, 'success')
      if (data.status === 'pending') {
        onSelectThinker?.(data)
      }
    },
    onError: (err) => {
      showToast(`Failed to create thinker: ${err.message}`, 'error')
    },
  })

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: (id) => api.deleteThinker(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['thinkers'] })
      showToast('Thinker deleted', 'success')
    },
    onError: (err) => {
      showToast(`Failed to delete: ${err.message}`, 'error')
    },
  })

  const handleAdd = () => {
    if (!addInput.trim()) return
    if (isQuickAdd) {
      quickAddMutation.mutate(addInput.trim())
    } else {
      createMutation.mutate({
        name: addInput.trim(),
        scholarProfileUrl: scholarProfileUrl.trim() || null
      })
    }
  }

  const handleCloseDialog = () => {
    setShowAddDialog(false)
    setAddInput('')
    setScholarProfileUrl('')
  }

  const handleDelete = (e, thinker) => {
    e.stopPropagation()
    if (confirm(`Delete thinker "${thinker.canonical_name}"? This will remove all associated works.`)) {
      deleteMutation.mutate(thinker.id)
    }
  }

  const getStatusBadge = (status) => {
    const badges = {
      pending: { class: 'status-pending', label: 'Pending Disambiguation' },
      disambiguated: { class: 'status-ready', label: 'Ready' },
      harvesting: { class: 'status-running', label: 'Harvesting' },
      complete: { class: 'status-complete', label: 'Complete' },
    }
    const badge = badges[status] || { class: 'status-unknown', label: status }
    return <span className={`status-badge ${badge.class}`}>{badge.label}</span>
  }

  if (isLoading) {
    return <div className="loading">Loading thinkers...</div>
  }

  if (error) {
    return <div className="error">Error loading thinkers: {error.message}</div>
  }

  return (
    <div className="thinkers-container">
      <div className="thinkers-header">
        <h2>Thinker Bibliographies</h2>
        <p className="description">
          Harvest complete citation data for specific authors/thinkers.
          The system will find all their works and collect citations to each.
        </p>
        <button className="btn btn-primary" onClick={() => setShowAddDialog(true)}>
          + Add Thinker
        </button>
      </div>

      {/* Add Dialog */}
      {showAddDialog && (
        <div className="modal-overlay" onClick={handleCloseDialog}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Add Thinker</h3>
              <button className="close-btn" onClick={handleCloseDialog}>&times;</button>
            </div>
            <div className="modal-body">
              <div className="form-group">
                <label>
                  <input
                    type="radio"
                    checked={isQuickAdd}
                    onChange={() => setIsQuickAdd(true)}
                  />
                  Quick Add (natural language)
                </label>
                <label>
                  <input
                    type="radio"
                    checked={!isQuickAdd}
                    onChange={() => setIsQuickAdd(false)}
                  />
                  Direct Name
                </label>
              </div>
              <div className="form-group">
                <input
                  type="text"
                  value={addInput}
                  onChange={(e) => setAddInput(e.target.value)}
                  placeholder={isQuickAdd ? 'e.g., "harvest works by Herbert Marcuse"' : 'e.g., "Herbert Marcuse"'}
                  className="input-full"
                  autoFocus
                  onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
                />
              </div>
              {isQuickAdd && (
                <p className="hint">
                  Use natural language like "find papers by Jurgen Habermas" or
                  "collect citations to all works by Andrew Feenberg"
                </p>
              )}
              {!isQuickAdd && (
                <div className="form-group">
                  <label style={{ display: 'block', marginBottom: '4px', fontWeight: 500 }}>
                    Google Scholar Profile URL (optional)
                  </label>
                  <input
                    type="text"
                    value={scholarProfileUrl}
                    onChange={(e) => setScholarProfileUrl(e.target.value)}
                    placeholder="https://scholar.google.com/citations?user=..."
                    className="input-full"
                  />
                  <p className="hint" style={{ marginTop: '4px' }}>
                    If provided, all publications from the profile will be imported first.
                    This ensures a complete bibliography even if the author search misses some works.
                  </p>
                </div>
              )}
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={handleCloseDialog}>
                Cancel
              </button>
              <button
                className="btn btn-primary"
                onClick={handleAdd}
                disabled={!addInput.trim() || quickAddMutation.isPending || createMutation.isPending}
              >
                {(quickAddMutation.isPending || createMutation.isPending) ? 'Adding...' : 'Add Thinker'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Thinkers List */}
      {(!thinkers || thinkers.length === 0) ? (
        <div className="empty-state">
          <p>No thinkers yet. Add one to start harvesting their bibliography.</p>
        </div>
      ) : (
        <div className="thinkers-table-container">
          <table className="thinkers-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Domains</th>
                <th>Status</th>
                <th>Works</th>
                <th>Citations</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {thinkers.map((thinker) => (
                <tr
                  key={thinker.id}
                  className="thinker-row"
                  onClick={() => onSelectThinker?.(thinker)}
                >
                  <td className="name-cell">
                    <strong>{thinker.canonical_name}</strong>
                    {thinker.birth_death && (
                      <span className="dates"> ({thinker.birth_death})</span>
                    )}
                  </td>
                  <td className="domains-cell">
                    {thinker.domains && thinker.domains.length > 0 ? (
                      <span className="domains-list">
                        {(Array.isArray(thinker.domains) ? thinker.domains : JSON.parse(thinker.domains)).slice(0, 3).join(', ')}
                      </span>
                    ) : (
                      <span className="muted">-</span>
                    )}
                  </td>
                  <td>{getStatusBadge(thinker.status)}</td>
                  <td className="number-cell">
                    {thinker.works_discovered > 0 ? (
                      <>
                        <span className="harvested">{thinker.works_harvested}</span>
                        <span className="muted"> / {thinker.works_discovered}</span>
                      </>
                    ) : (
                      <span className="muted">-</span>
                    )}
                  </td>
                  <td className="number-cell">
                    {thinker.total_citations > 0 ? (
                      thinker.total_citations.toLocaleString()
                    ) : (
                      <span className="muted">-</span>
                    )}
                  </td>
                  <td className="actions-cell">
                    <button
                      className="btn btn-small btn-danger"
                      onClick={(e) => handleDelete(e, thinker)}
                      title="Delete thinker"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <style>{`
        .thinkers-container {
          padding: 20px;
        }

        .thinkers-header {
          margin-bottom: 24px;
        }

        .thinkers-header h2 {
          margin: 0 0 8px 0;
        }

        .thinkers-header .description {
          color: var(--text-secondary);
          margin: 0 0 16px 0;
        }

        .thinkers-table-container {
          overflow-x: auto;
        }

        .thinkers-table {
          width: 100%;
          border-collapse: collapse;
        }

        .thinkers-table th,
        .thinkers-table td {
          padding: 12px 16px;
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        .thinkers-table th {
          background: var(--bg-secondary);
          font-weight: 600;
        }

        .thinker-row {
          cursor: pointer;
          transition: background 0.15s;
        }

        .thinker-row:hover {
          background: var(--bg-hover);
        }

        .name-cell strong {
          color: var(--text-primary);
        }

        .name-cell .dates {
          color: var(--text-secondary);
          font-size: 0.9em;
        }

        .domains-cell {
          color: var(--text-secondary);
          font-size: 0.9em;
        }

        .number-cell {
          font-family: monospace;
        }

        .number-cell .harvested {
          color: var(--success-color);
        }

        .status-badge {
          display: inline-block;
          padding: 4px 8px;
          border-radius: 4px;
          font-size: 0.8em;
          font-weight: 500;
        }

        .status-pending {
          background: var(--warning-bg);
          color: var(--warning-color);
        }

        .status-ready {
          background: var(--info-bg);
          color: var(--info-color);
        }

        .status-running {
          background: var(--primary-bg);
          color: var(--primary-color);
        }

        .status-complete {
          background: var(--success-bg);
          color: var(--success-color);
        }

        .actions-cell {
          white-space: nowrap;
        }

        .empty-state {
          text-align: center;
          padding: 40px;
          color: var(--text-secondary);
        }

        .muted {
          color: var(--text-muted);
        }

        /* Modal styles */
        .modal-overlay {
          position: fixed;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background: rgba(0, 0, 0, 0.5);
          display: flex;
          align-items: center;
          justify-content: center;
          z-index: 1000;
        }

        .modal {
          background: var(--bg-primary);
          border-radius: 8px;
          width: 500px;
          max-width: 90%;
          box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
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
        }

        .close-btn {
          background: none;
          border: none;
          font-size: 24px;
          cursor: pointer;
          color: var(--text-secondary);
        }

        .modal-body {
          padding: 20px;
        }

        .modal-footer {
          padding: 16px 20px;
          border-top: 1px solid var(--border-color);
          display: flex;
          justify-content: flex-end;
          gap: 12px;
        }

        .form-group {
          margin-bottom: 16px;
        }

        .form-group label {
          display: block;
          margin-bottom: 8px;
          cursor: pointer;
        }

        .form-group input[type="radio"] {
          margin-right: 8px;
        }

        .input-full {
          width: 100%;
          padding: 10px 12px;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          background: var(--bg-secondary);
          color: var(--text-primary);
          font-size: 14px;
        }

        .input-full:focus {
          outline: none;
          border-color: var(--primary-color);
        }

        .hint {
          font-size: 0.85em;
          color: var(--text-secondary);
          margin: 0;
        }

        .btn {
          padding: 8px 16px;
          border: none;
          border-radius: 4px;
          cursor: pointer;
          font-size: 14px;
          transition: background 0.15s;
        }

        .btn-primary {
          background: var(--primary-color);
          color: white;
        }

        .btn-primary:hover:not(:disabled) {
          background: var(--primary-hover);
        }

        .btn-secondary {
          background: var(--bg-secondary);
          color: var(--text-primary);
          border: 1px solid var(--border-color);
        }

        .btn-danger {
          background: var(--danger-bg);
          color: var(--danger-color);
        }

        .btn-small {
          padding: 4px 8px;
          font-size: 12px;
        }

        .btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
      `}</style>
    </div>
  )
}

export default Thinkers
