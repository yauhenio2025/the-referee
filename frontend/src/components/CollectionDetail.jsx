import { useState, useEffect, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { api } from '../lib/api'
import { useToast } from './Toast'

/**
 * Collection Detail - Shows papers organized by dossiers
 * Power-user friendly with keyboard shortcuts and drag-drop
 */
export default function CollectionDetail({ collectionId, onBack }) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const toast = useToast()
  const [refreshBatchId, setRefreshBatchId] = useState(null)
  const [refreshProgress, setRefreshProgress] = useState(null)
  const [selectedDossier, setSelectedDossier] = useState(null) // null = all papers, 'unassigned' = no dossier
  const [showCreateDossier, setShowCreateDossier] = useState(false)
  const [newDossierName, setNewDossierName] = useState('')
  const [editingDossier, setEditingDossier] = useState(null)
  const [selectedPapers, setSelectedPapers] = useState(new Set())
  const [searchFilter, setSearchFilter] = useState('')
  const [expandedPapers, setExpandedPapers] = useState(new Set()) // Track which papers have editions expanded
  const [paperEditions, setPaperEditions] = useState({}) // Cache of editions by paper id

  const { data: collection, isLoading } = useQuery({
    queryKey: ['collection', collectionId],
    queryFn: () => api.getCollection(collectionId),
  })

  const { data: dossiers = [] } = useQuery({
    queryKey: ['dossiers', collectionId],
    queryFn: () => api.getDossiers(collectionId),
  })

  // Create dossier
  const createDossier = useMutation({
    mutationFn: (data) => api.createDossier({ ...data, collection_id: collectionId }),
    onSuccess: (newDossier) => {
      queryClient.invalidateQueries(['dossiers', collectionId])
      setShowCreateDossier(false)
      setNewDossierName('')
      toast.success(`Created dossier: ${newDossier.name}`)
      setSelectedDossier(newDossier.id)
    },
    onError: (err) => toast.error(`Failed: ${err.message}`),
  })

  // Update dossier
  const updateDossier = useMutation({
    mutationFn: ({ id, ...data }) => api.updateDossier(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries(['dossiers', collectionId])
      setEditingDossier(null)
      toast.success('Dossier updated')
    },
    onError: (err) => toast.error(`Failed: ${err.message}`),
  })

  // Delete dossier
  const deleteDossier = useMutation({
    mutationFn: (id) => api.deleteDossier(id),
    onSuccess: (result, id) => {
      queryClient.invalidateQueries(['dossiers', collectionId])
      queryClient.invalidateQueries(['collection', collectionId])
      if (selectedDossier === id) setSelectedDossier(null)
      toast.success(`Deleted dossier (${result.papers_unassigned} papers unassigned)`)
    },
    onError: (err) => toast.error(`Failed: ${err.message}`),
  })

  // Assign papers to dossier
  const assignToDossier = useMutation({
    mutationFn: ({ paperIds, dossierId }) => api.assignPapersToDossier(paperIds, dossierId),
    onSuccess: () => {
      queryClient.invalidateQueries(['dossiers', collectionId])
      queryClient.invalidateQueries(['collection', collectionId])
      setSelectedPapers(new Set())
      toast.success('Papers moved')
    },
    onError: (err) => toast.error(`Failed: ${err.message}`),
  })

  // Remove paper from collection
  const removePaper = useMutation({
    mutationFn: async (paper) => {
      await api.assignPapersToCollection([paper.id], null)
      return paper
    },
    onSuccess: (paper) => {
      queryClient.invalidateQueries(['collection', collectionId])
      queryClient.invalidateQueries(['dossiers', collectionId])
      toast.undo(
        `Removed: ${paper.title?.substring(0, 40)}...`,
        async () => {
          try {
            await api.assignPapersToCollection([paper.id], collectionId)
            queryClient.invalidateQueries(['collection', collectionId])
            queryClient.invalidateQueries(['dossiers', collectionId])
            toast.success('Paper restored')
          } catch (err) {
            toast.error(`Failed: ${err.message}`)
          }
        }
      )
    },
  })

  // Refresh collection
  const refreshCollection = useMutation({
    mutationFn: () => api.refreshCollection(collectionId),
    onSuccess: (result) => {
      if (result.jobs_created > 0) {
        setRefreshBatchId(result.batch_id)
        setRefreshProgress({
          total: result.jobs_created,
          completed: 0,
          message: `Queued ${result.jobs_created} refresh jobs`,
        })
        queryClient.invalidateQueries(['jobs'])
      } else {
        setRefreshProgress({ message: 'No papers need refreshing', done: true })
        setTimeout(() => setRefreshProgress(null), 3000)
      }
    },
  })

  // Poll refresh status
  useEffect(() => {
    if (!refreshBatchId) return
    const pollInterval = setInterval(async () => {
      try {
        const status = await api.getRefreshStatus(refreshBatchId)
        setRefreshProgress(prev => ({
          ...prev,
          completed: status.jobs_completed + status.jobs_failed,
          message: status.is_complete
            ? `Done! Found ${status.new_citations_added} new citations`
            : `${status.jobs_completed}/${status.jobs_total} jobs complete`,
        }))
        if (status.is_complete) {
          setRefreshBatchId(null)
          queryClient.invalidateQueries(['collection', collectionId])
          setTimeout(() => setRefreshProgress(null), 5000)
        }
      } catch (err) {
        console.error('Refresh poll error:', err)
      }
    }, 3000)
    return () => clearInterval(pollInterval)
  }, [refreshBatchId, collectionId, queryClient])

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e) => {
      // Escape to clear selection
      if (e.key === 'Escape') {
        setSelectedPapers(new Set())
        setShowCreateDossier(false)
        setEditingDossier(null)
      }
      // Cmd/Ctrl+A to select all visible papers
      if ((e.metaKey || e.ctrlKey) && e.key === 'a' && filteredPapers.length) {
        e.preventDefault()
        setSelectedPapers(new Set(filteredPapers.map(p => p.id)))
      }
      // Cmd/Ctrl+N to create new dossier
      if ((e.metaKey || e.ctrlKey) && e.key === 'n' && !showCreateDossier) {
        e.preventDefault()
        setShowCreateDossier(true)
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [showCreateDossier])

  // Filter papers by dossier and search
  const filteredPapers = (collection?.papers || []).filter(paper => {
    // Filter by dossier
    if (selectedDossier === 'unassigned') {
      if (paper.dossier_id) return false
    } else if (selectedDossier !== null) {
      if (paper.dossier_id !== selectedDossier) return false
    }
    // Filter by search
    if (searchFilter) {
      const search = searchFilter.toLowerCase()
      return (
        paper.title?.toLowerCase().includes(search) ||
        paper.authors?.toLowerCase().includes(search)
      )
    }
    return true
  })

  // Count unassigned papers
  const unassignedCount = (collection?.papers || []).filter(p => !p.dossier_id).length

  const handleSelectPaper = (paperId, e) => {
    setSelectedPapers(prev => {
      const next = new Set(prev)
      if (e?.shiftKey && prev.size > 0) {
        // Shift-click: select range
        const lastSelected = Array.from(prev).pop()
        const paperIds = filteredPapers.map(p => p.id)
        const start = paperIds.indexOf(lastSelected)
        const end = paperIds.indexOf(paperId)
        const range = paperIds.slice(Math.min(start, end), Math.max(start, end) + 1)
        range.forEach(id => next.add(id))
      } else if (e?.metaKey || e?.ctrlKey) {
        // Cmd/Ctrl-click: toggle
        if (next.has(paperId)) next.delete(paperId)
        else next.add(paperId)
      } else {
        // Regular click: select only this one
        next.clear()
        next.add(paperId)
      }
      return next
    })
  }

  const handleMoveSelected = (dossierId) => {
    if (selectedPapers.size === 0) return
    assignToDossier.mutate({
      paperIds: Array.from(selectedPapers),
      dossierId: dossierId === 'unassigned' ? null : dossierId,
    })
  }

  // Toggle paper expansion and fetch editions if not cached
  const togglePaperExpansion = async (paperId, e) => {
    e.stopPropagation()
    setExpandedPapers(prev => {
      const next = new Set(prev)
      if (next.has(paperId)) {
        next.delete(paperId)
      } else {
        next.add(paperId)
        // Fetch editions if not cached
        if (!paperEditions[paperId]) {
          api.getPaperEditions(paperId).then(editions => {
            setPaperEditions(prev => ({ ...prev, [paperId]: editions }))
          }).catch(err => {
            console.error('Failed to fetch editions:', err)
            setPaperEditions(prev => ({ ...prev, [paperId]: [] }))
          })
        }
      }
      return next
    })
  }

  if (isLoading) {
    return <div className="loading">Loading collection...</div>
  }

  if (!collection) {
    return <div className="error">Collection not found</div>
  }

  return (
    <div className="collection-detail with-dossiers">
      {/* Header */}
      <header className="collection-detail-header">
        <button onClick={onBack} className="btn-text">← Collections</button>
        <div className="collection-info">
          <div className="collection-title-row">
            <span className="color-indicator" style={{ backgroundColor: collection.color }} />
            <h2>{collection.name}</h2>
          </div>
          {collection.description && <p className="description">{collection.description}</p>}
        </div>
        <div className="header-actions">
          {collection.papers?.some(p => p.total_harvested_citations > 0) && (
            <button
              onClick={() => refreshCollection.mutate()}
              disabled={refreshCollection.isPending || !!refreshBatchId}
              className="btn-secondary"
              title="Check for new citations"
            >
              🔄 Refresh All
            </button>
          )}
        </div>
      </header>

      {/* Refresh Progress */}
      {refreshProgress && (
        <div className={`collection-refresh-progress ${refreshProgress.done ? 'done' : ''}`}>
          {refreshProgress.total > 0 && !refreshProgress.done && (
            <div className="progress-bar">
              <div className="progress-fill" style={{ width: `${(refreshProgress.completed / refreshProgress.total) * 100}%` }} />
            </div>
          )}
          <span>{refreshProgress.message}</span>
        </div>
      )}

      <div className="collection-body">
        {/* Dossier Sidebar */}
        <aside className="dossier-sidebar">
          <div className="dossier-header">
            <h3>Dossiers</h3>
            <button
              className="btn-icon"
              onClick={() => setShowCreateDossier(true)}
              title="New Dossier (⌘N)"
            >
              +
            </button>
          </div>

          {/* Create Dossier Form */}
          {showCreateDossier && (
            <div className="dossier-create-form">
              <input
                type="text"
                placeholder="Dossier name..."
                value={newDossierName}
                onChange={e => setNewDossierName(e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Enter' && newDossierName.trim()) {
                    createDossier.mutate({ name: newDossierName.trim() })
                  }
                  if (e.key === 'Escape') setShowCreateDossier(false)
                }}
                autoFocus
              />
              <div className="form-actions">
                <button
                  className="btn-primary btn-sm"
                  onClick={() => newDossierName.trim() && createDossier.mutate({ name: newDossierName.trim() })}
                  disabled={!newDossierName.trim()}
                >
                  Create
                </button>
                <button className="btn-text btn-sm" onClick={() => setShowCreateDossier(false)}>
                  Cancel
                </button>
              </div>
            </div>
          )}

          <div className="dossier-list">
            {/* All Papers */}
            <button
              className={`dossier-item ${selectedDossier === null ? 'active' : ''}`}
              onClick={() => setSelectedDossier(null)}
            >
              <span className="dossier-name">All Papers</span>
              <span className="dossier-count">{collection.paper_count}</span>
            </button>

            {/* Dossiers */}
            {dossiers.map(dossier => (
              <div
                key={dossier.id}
                className={`dossier-item ${selectedDossier === dossier.id ? 'active' : ''}`}
                onClick={() => setSelectedDossier(dossier.id)}
                onDrop={e => {
                  e.preventDefault()
                  const paperIds = JSON.parse(e.dataTransfer.getData('paperIds') || '[]')
                  if (paperIds.length) {
                    assignToDossier.mutate({ paperIds, dossierId: dossier.id })
                  }
                }}
                onDragOver={e => e.preventDefault()}
              >
                {editingDossier === dossier.id ? (
                  <input
                    type="text"
                    defaultValue={dossier.name}
                    onClick={e => e.stopPropagation()}
                    onKeyDown={e => {
                      if (e.key === 'Enter') {
                        updateDossier.mutate({ id: dossier.id, name: e.target.value })
                      }
                      if (e.key === 'Escape') setEditingDossier(null)
                    }}
                    onBlur={e => {
                      if (e.target.value !== dossier.name) {
                        updateDossier.mutate({ id: dossier.id, name: e.target.value })
                      } else {
                        setEditingDossier(null)
                      }
                    }}
                    autoFocus
                  />
                ) : (
                  <>
                    <span className="dossier-name" onDoubleClick={() => setEditingDossier(dossier.id)}>
                      📁 {dossier.name}
                    </span>
                    <span className="dossier-count">{dossier.paper_count}</span>
                    <button
                      className="btn-icon btn-xs btn-danger"
                      onClick={e => {
                        e.stopPropagation()
                        if (confirm(`Delete "${dossier.name}"? Papers will be unassigned.`)) {
                          deleteDossier.mutate(dossier.id)
                        }
                      }}
                      title="Delete dossier"
                    >
                      ×
                    </button>
                  </>
                )}
              </div>
            ))}

            {/* Unassigned */}
            {unassignedCount > 0 && (
              <button
                className={`dossier-item unassigned ${selectedDossier === 'unassigned' ? 'active' : ''}`}
                onClick={() => setSelectedDossier('unassigned')}
                onDrop={e => {
                  e.preventDefault()
                  const paperIds = JSON.parse(e.dataTransfer.getData('paperIds') || '[]')
                  if (paperIds.length) {
                    assignToDossier.mutate({ paperIds, dossierId: null })
                  }
                }}
                onDragOver={e => e.preventDefault()}
              >
                <span className="dossier-name">📄 Unassigned</span>
                <span className="dossier-count">{unassignedCount}</span>
              </button>
            )}
          </div>

          {/* Keyboard hints */}
          <div className="keyboard-hints">
            <span>⌘N New dossier</span>
            <span>⌘A Select all</span>
            <span>Esc Clear</span>
          </div>
        </aside>

        {/* Papers Panel */}
        <main className="papers-panel">
          {/* Toolbar */}
          <div className="papers-toolbar">
            <input
              type="text"
              placeholder="🔍 Filter papers..."
              value={searchFilter}
              onChange={e => setSearchFilter(e.target.value)}
              className="search-input"
            />
            {selectedPapers.size > 0 && (
              <div className="selection-actions">
                <span className="selection-count">{selectedPapers.size} selected</span>
                <select
                  onChange={e => {
                    if (e.target.value) handleMoveSelected(e.target.value === 'unassigned' ? null : parseInt(e.target.value))
                    e.target.value = ''
                  }}
                  value=""
                >
                  <option value="">Move to...</option>
                  {dossiers.map(d => (
                    <option key={d.id} value={d.id}>📁 {d.name}</option>
                  ))}
                  <option value="unassigned">📄 Unassigned</option>
                </select>
                <button className="btn-text" onClick={() => setSelectedPapers(new Set())}>
                  Clear
                </button>
              </div>
            )}
          </div>

          {/* Papers Table */}
          {filteredPapers.length === 0 ? (
            <div className="empty">
              <p>
                {searchFilter
                  ? 'No papers match your search.'
                  : selectedDossier === 'unassigned'
                    ? 'No unassigned papers.'
                    : selectedDossier
                      ? 'No papers in this dossier. Drag papers here or select and move.'
                      : 'No papers in this collection yet.'}
              </p>
            </div>
          ) : (
            <table className="papers-table compact">
              <thead>
                <tr>
                  <th className="col-expand"></th>
                  <th className="col-select">
                    <input
                      type="checkbox"
                      checked={selectedPapers.size === filteredPapers.length && filteredPapers.length > 0}
                      onChange={e => {
                        if (e.target.checked) {
                          setSelectedPapers(new Set(filteredPapers.map(p => p.id)))
                        } else {
                          setSelectedPapers(new Set())
                        }
                      }}
                    />
                  </th>
                  <th className="col-title">Title</th>
                  <th className="col-year">Year</th>
                  <th className="col-status">Status</th>
                  <th className="col-editions">Editions</th>
                  <th className="col-dossier">Dossier</th>
                  <th className="col-citations">Citations</th>
                  <th className="col-actions"></th>
                </tr>
              </thead>
              <tbody>
                {filteredPapers.map(paper => {
                  const isExpanded = expandedPapers.has(paper.id)
                  const editions = paperEditions[paper.id] || []
                  const editionCount = paper.edition_count || editions.length || 0
                  return (
                    <>
                      <tr
                        key={paper.id}
                        className={`paper-row ${selectedPapers.has(paper.id) ? 'selected' : ''} ${isExpanded ? 'expanded' : ''}`}
                        onClick={e => handleSelectPaper(paper.id, e)}
                        draggable
                        onDragStart={e => {
                          const ids = selectedPapers.has(paper.id)
                            ? Array.from(selectedPapers)
                            : [paper.id]
                          e.dataTransfer.setData('paperIds', JSON.stringify(ids))
                        }}
                      >
                        <td className="col-expand" onClick={e => e.stopPropagation()}>
                          {paper.status === 'resolved' && (
                            <button
                              className={`btn-expand ${isExpanded ? 'expanded' : ''}`}
                              onClick={e => togglePaperExpansion(paper.id, e)}
                              title={isExpanded ? 'Collapse editions' : 'Show editions'}
                            >
                              {isExpanded ? '▼' : '▶'}
                            </button>
                          )}
                        </td>
                        <td className="col-select" onClick={e => e.stopPropagation()}>
                          <input
                            type="checkbox"
                            checked={selectedPapers.has(paper.id)}
                            onChange={() => handleSelectPaper(paper.id)}
                          />
                        </td>
                        <td className="col-title">
                          <a
                            href="#"
                            onClick={e => { e.preventDefault(); e.stopPropagation(); navigate(`/paper/${paper.id}`); }}
                          >
                            {paper.title}
                          </a>
                          <span className="authors-sub">{paper.authors}</span>
                        </td>
                        <td className="col-year">{paper.year || '–'}</td>
                        <td className="col-status">
                          <span className={`status-badge status-${paper.status}`}>
                            {paper.status}
                          </span>
                        </td>
                        <td className="col-editions" onClick={e => e.stopPropagation()}>
                          {paper.status === 'resolved' && (
                            <button
                              className="btn-editions-count"
                              onClick={e => togglePaperExpansion(paper.id, e)}
                              title="Click to expand/collapse editions"
                            >
                              📖 {editionCount > 0 ? editionCount : '?'}
                            </button>
                          )}
                        </td>
                        <td className="col-dossier">
                          {paper.dossier_id ? (
                            <span className="dossier-tag">
                              {dossiers.find(d => d.id === paper.dossier_id)?.name || '...'}
                            </span>
                          ) : (
                            <span className="unassigned-tag">—</span>
                          )}
                        </td>
                        <td className="col-citations">
                          {paper.total_harvested_citations?.toLocaleString() || 0}
                        </td>
                        <td className="col-actions" onClick={e => e.stopPropagation()}>
                          <button
                            className="btn-icon btn-danger btn-xs"
                            onClick={() => removePaper.mutate(paper)}
                            title="Remove from collection"
                          >
                            ×
                          </button>
                        </td>
                      </tr>
                      {/* Expanded editions row */}
                      {isExpanded && (
                        <tr key={`${paper.id}-editions`} className="editions-row">
                          <td colSpan="9">
                            <div className="editions-panel">
                              {!paperEditions[paper.id] ? (
                                <div className="editions-loading">Loading editions...</div>
                              ) : editions.length === 0 ? (
                                <div className="editions-empty">
                                  No editions discovered yet.
                                  <button
                                    className="btn-link"
                                    onClick={() => navigate(`/paper/${paper.id}`)}
                                  >
                                    Discover Editions →
                                  </button>
                                </div>
                              ) : (
                                <table className="editions-table">
                                  <thead>
                                    <tr>
                                      <th>Language</th>
                                      <th>Title</th>
                                      <th>Citations</th>
                                      <th>Harvested</th>
                                      <th>Confidence</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {editions.filter(e => e.selected && !e.excluded).map(edition => (
                                      <tr key={edition.id} className={`edition-item confidence-${edition.confidence}`}>
                                        <td className="edition-lang">
                                          <span className="lang-badge">{edition.language?.toUpperCase() || '?'}</span>
                                        </td>
                                        <td className="edition-title">
                                          {edition.link ? (
                                            <a href={edition.link} target="_blank" rel="noopener noreferrer">
                                              {edition.title}
                                            </a>
                                          ) : edition.title}
                                        </td>
                                        <td className="edition-citations">
                                          {edition.citation_count?.toLocaleString() || 0}
                                        </td>
                                        <td className="edition-harvested">
                                          {edition.harvested_citation_count?.toLocaleString() || 0}
                                        </td>
                                        <td className="edition-confidence">
                                          <span className={`confidence-badge ${edition.confidence}`}>
                                            {edition.confidence}
                                          </span>
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </>
                  )
                })}
              </tbody>
            </table>
          )}
        </main>
      </div>
    </div>
  )
}
