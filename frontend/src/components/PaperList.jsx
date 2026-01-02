import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import { useToast } from './Toast'
import DossierSelectModal from './DossierSelectModal'

export default function PaperList({ onSelectPaper }) {
  const queryClient = useQueryClient()
  const toast = useToast()
  const [resolvingId, setResolvingId] = useState(null)
  const [expandedAbstracts, setExpandedAbstracts] = useState({})
  const [reconciliationPaper, setReconciliationPaper] = useState(null)
  const [editionCounts, setEditionCounts] = useState({})
  const [refreshingPapers, setRefreshingPapers] = useState({})
  const [quickHarvestingPapers, setQuickHarvestingPapers] = useState({})
  const [batchResolving, setBatchResolving] = useState(false)
  const [selectedPapers, setSelectedPapers] = useState(new Set())
  const [addToCollectionPaper, setAddToCollectionPaper] = useState(null)
  const [showProcessed, setShowProcessed] = useState(false)
  const [batchAddToCollection, setBatchAddToCollection] = useState(false)

  // Drag-drop state for linking editions
  const [draggingPaperId, setDraggingPaperId] = useState(null)
  const [dragOverPaperId, setDragOverPaperId] = useState(null)

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1)
  const perPage = 25

  // Fetch collections for badge display
  const { data: collections = [] } = useQuery({
    queryKey: ['collections'],
    queryFn: () => api.getCollections(),
  })

  // Fetch dossiers for badge display
  const { data: allDossiers = [] } = useQuery({
    queryKey: ['all-dossiers'],
    queryFn: () => api.getDossiers(),
  })

  const { data: papersData, isLoading, error } = useQuery({
    queryKey: ['papers', currentPage, perPage],
    queryFn: async () => {
      const response = await api.listPapersPaginated(currentPage, perPage)
      // Fetch edition counts for resolved papers
      const counts = {}
      for (const paper of response.papers.filter(p => p.status === 'resolved')) {
        try {
          const editions = await api.getPaperEditions(paper.id)
          counts[paper.id] = editions?.length || 0
        } catch (e) {
          counts[paper.id] = 0
        }
      }
      setEditionCounts(counts)
      return response
    },
  })

  const papers = papersData?.papers || []
  const pagination = papersData ? {
    total: papersData.total,
    page: papersData.page,
    perPage: papersData.per_page,
    totalPages: papersData.total_pages,
    hasNext: papersData.has_next,
    hasPrev: papersData.has_prev,
  } : null

  const deletePaper = useMutation({
    mutationFn: (paperId) => api.deletePaper(paperId),
    onSuccess: (result) => {
      queryClient.invalidateQueries(['papers'])
      if (result.can_restore) {
        toast.undo(
          `Deleted: ${result.title?.substring(0, 40)}...`,
          async () => {
            try {
              await api.restorePaper(result.paper_id)
              queryClient.invalidateQueries(['papers'])
              toast.success('Paper restored')
            } catch (err) {
              toast.error(`Failed to restore: ${err.message}`)
            }
          }
        )
      }
    },
  })

  const resolvePaper = useMutation({
    mutationFn: (paperId) => api.resolvePaper(paperId),
    onSuccess: (data, paperId) => {
      if (data.needs_reconciliation && data.candidates) {
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

  const batchResolve = useMutation({
    mutationFn: (paperIds) => api.batchResolvePapers(paperIds),
    onMutate: () => {
      setBatchResolving(true)
    },
    onSuccess: (data) => {
      setBatchResolving(false)
      setSelectedPapers(new Set())
      if (data.jobs_created > 0) {
        toast.success(`Queued ${data.jobs_created} papers for resolution`)
      } else {
        toast.info('No pending papers to resolve')
      }
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['jobs'])
    },
    onError: (error) => {
      setBatchResolving(false)
      toast.error(`Failed to queue resolution: ${error.message}`)
    },
  })

  // Toggle foreign edition needed
  const toggleForeignEdition = useMutation({
    mutationFn: ({ paperId, needed }) => api.toggleForeignEditionNeeded(paperId, needed),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
    },
    onError: (error) => {
      toast.error(`Failed to update: ${error.message}`)
    },
  })

  // Batch foreign edition
  const batchForeignEdition = useMutation({
    mutationFn: ({ paperIds, needed }) => api.batchForeignEditionNeeded(paperIds, needed),
    onSuccess: (data) => {
      queryClient.invalidateQueries(['papers'])
      toast.success(`Marked ${data.updated} papers`)
      setSelectedPapers(new Set())
    },
    onError: (error) => {
      toast.error(`Failed to update: ${error.message}`)
    },
  })

  // Link paper as edition (drag-drop)
  const linkAsEdition = useMutation({
    mutationFn: ({ sourcePaperId, targetPaperId }) =>
      api.linkPaperAsEdition(sourcePaperId, targetPaperId, true),
    onSuccess: (data) => {
      queryClient.invalidateQueries(['papers'])
      toast.success(data.message || 'Linked as edition')
    },
    onError: (error) => {
      toast.error(`Failed to link: ${error.message}`)
    },
  })

  // Add paper to collection/dossier
  const handleAddToCollection = async (selection) => {
    if (!addToCollectionPaper) return
    try {
      const paperId = addToCollectionPaper.id
      if (selection.collectionId) {
        await api.assignPapersToCollection([paperId], selection.collectionId)
      }
      if (selection.dossierId) {
        await api.assignPapersToDossier([paperId], selection.dossierId)
      } else if (selection.createNewDossier && selection.newDossierName) {
        const newDossier = await api.createDossier({
          name: selection.newDossierName,
          collection_id: selection.collectionId,
        })
        await api.assignPapersToDossier([paperId], newDossier.id)
      }
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['collections'])
      queryClient.invalidateQueries(['dossiers'])
      toast.success(`Added to collection`)
      setAddToCollectionPaper(null)
    } catch (err) {
      toast.error(`Failed to add: ${err.message}`)
    }
  }

  // Batch add to collection
  const handleBatchAddToCollection = async (selection) => {
    const paperIds = Array.from(selectedPapers)
    if (paperIds.length === 0) return

    try {
      await api.batchAssignToCollection(
        paperIds,
        selection.collectionId,
        selection.dossierId,
        {
          createNewDossier: selection.createNewDossier,
          newDossierName: selection.newDossierName,
        }
      )
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['collections'])
      queryClient.invalidateQueries(['dossiers'])
      toast.success(`Added ${paperIds.length} papers to collection`)
      setSelectedPapers(new Set())
      setBatchAddToCollection(false)
    } catch (err) {
      toast.error(`Failed to add: ${err.message}`)
    }
  }

  // Selection helpers
  const togglePaperSelection = (paperId) => {
    setSelectedPapers(prev => {
      const next = new Set(prev)
      if (next.has(paperId)) {
        next.delete(paperId)
      } else {
        next.add(paperId)
      }
      return next
    })
  }

  const selectAllVisible = () => {
    const visibleIds = papers.map(p => p.id)
    setSelectedPapers(new Set(visibleIds))
  }

  const clearSelection = () => {
    setSelectedPapers(new Set())
  }

  const handleBatchResolve = () => {
    const paperIds = Array.from(selectedPapers)
    if (paperIds.length > 0) {
      batchResolve.mutate(paperIds)
    }
  }

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
    e.stopPropagation()
    addAsSeed.mutate(candidate)
  }

  const toggleAbstract = (paperId) => {
    setExpandedAbstracts(prev => ({
      ...prev,
      [paperId]: !prev[paperId]
    }))
  }

  // Drag-drop handlers for linking editions
  const handleDragStart = (e, paper) => {
    // Only allow dragging resolved papers
    if (paper.status !== 'resolved') {
      e.preventDefault()
      return
    }
    setDraggingPaperId(paper.id)
    e.dataTransfer.effectAllowed = 'link'
    e.dataTransfer.setData('text/plain', paper.id.toString())
    // Add paper info for visual feedback
    e.dataTransfer.setData('application/json', JSON.stringify({
      id: paper.id,
      title: paper.title,
      year: paper.year
    }))
  }

  const handleDragEnd = () => {
    setDraggingPaperId(null)
    setDragOverPaperId(null)
  }

  const handleDragOver = (e, paper) => {
    // Only allow drop on resolved papers that are different from the source
    if (paper.status !== 'resolved' || paper.id === draggingPaperId) {
      return
    }
    e.preventDefault()
    e.dataTransfer.dropEffect = 'link'
    setDragOverPaperId(paper.id)
  }

  const handleDragLeave = (e) => {
    // Only clear if we're leaving the card entirely
    if (!e.currentTarget.contains(e.relatedTarget)) {
      setDragOverPaperId(null)
    }
  }

  const handleDrop = (e, targetPaper) => {
    e.preventDefault()
    const sourcePaperId = parseInt(e.dataTransfer.getData('text/plain'))

    if (!sourcePaperId || sourcePaperId === targetPaper.id) {
      setDragOverPaperId(null)
      return
    }

    // Find the source paper for the confirmation message
    const sourcePaper = papers.find(p => p.id === sourcePaperId)

    // Link source paper as an edition of target
    linkAsEdition.mutate({
      sourcePaperId,
      targetPaperId: targetPaper.id
    })

    setDragOverPaperId(null)
    setDraggingPaperId(null)
  }

  // Pagination handlers
  const goToPage = (page) => {
    setCurrentPage(page)
    setSelectedPapers(new Set())
  }

  // Generate page numbers for pagination
  const getPageNumbers = () => {
    if (!pagination) return []
    const { page, totalPages } = pagination
    const pages = []

    if (totalPages <= 7) {
      for (let i = 1; i <= totalPages; i++) pages.push(i)
    } else {
      pages.push(1)
      if (page > 3) pages.push('...')
      for (let i = Math.max(2, page - 1); i <= Math.min(totalPages - 1, page + 1); i++) {
        pages.push(i)
      }
      if (page < totalPages - 2) pages.push('...')
      pages.push(totalPages)
    }
    return pages
  }

  const papersNeedingReconciliation = papers?.filter(p => p.status === 'needs_reconciliation') || []

  if (isLoading) return <div className="loading">Loading papers...</div>
  if (error) return <div className="error">Error loading papers: {error.message}</div>
  if (!papers?.length) return <div className="empty">No papers yet. Add one above!</div>

  const getStatusBadge = (status) => {
    const badges = {
      pending: { label: 'Pending', class: 'badge-pending' },
      needs_reconciliation: { label: 'Choose Match', class: 'badge-warning' },
      resolved: { label: 'Resolved', class: 'badge-success' },
      error: { label: 'Error', class: 'badge-error' },
    }
    return badges[status] || badges.pending
  }

  const formatAuthors = (authors) => {
    if (!authors) return null
    if (Array.isArray(authors)) {
      if (authors.length > 3) {
        return authors.slice(0, 3).join(', ') + ' et al.'
      }
      return authors.join(', ')
    }
    return authors.replace(/([a-z])([A-Z])/g, '$1 $2').trim()
  }

  const isProcessed = (paper) => {
    return paper.collection_id && paper.total_harvested_citations > 0
  }

  const visiblePapers = papers?.filter(p => showProcessed || !isProcessed(p)) || []
  const processedCount = papers?.filter(isProcessed).length || 0
  const selectedCount = selectedPapers.size

  const getCollectionInfo = (paper) => {
    if (!paper.collection_id) return null
    const collection = collections.find(c => c.id === paper.collection_id)
    const dossier = paper.dossier_id ? allDossiers.find(d => d.id === paper.dossier_id) : null
    return { collection, dossier }
  }

  return (
    <div className="paper-list">
      {/* Minimal header with count and controls */}
      <div className="paper-list-header-minimal">
        <div className="header-left">
          <span className="paper-count">{pagination?.total || papers.length} papers</span>
          {processedCount > 0 && (
            <label className="toggle-processed-minimal">
              <input
                type="checkbox"
                checked={showProcessed}
                onChange={e => setShowProcessed(e.target.checked)}
              />
              <span>+{processedCount} processed</span>
            </label>
          )}
        </div>

        {/* Batch actions - only show when selected */}
        {selectedCount > 0 && (
          <div className="batch-actions-minimal">
            <span className="selected-indicator">{selectedCount} selected</span>
            <button onClick={clearSelection} className="btn-text">clear</button>
            <span className="action-divider">|</span>
            <button onClick={() => setBatchAddToCollection(true)} className="btn-text">+ collection</button>
            <button
              onClick={() => batchForeignEdition.mutate({ paperIds: Array.from(selectedPapers), needed: true })}
              className="btn-text"
            >
              + foreign ed.
            </button>
            <button onClick={handleBatchResolve} disabled={batchResolving} className="btn-text">
              {batchResolving ? 'resolving...' : 'resolve'}
            </button>
          </div>
        )}

        {selectedCount === 0 && (
          <button onClick={selectAllVisible} className="btn-text-subtle">
            select all
          </button>
        )}
      </div>

      {/* Alert for papers needing reconciliation */}
      {papersNeedingReconciliation.length > 0 && (
        <div className="reconciliation-alert-minimal">
          {papersNeedingReconciliation.length} need attention
        </div>
      )}

      {/* Minimal Pagination - top */}
      {pagination && pagination.totalPages > 1 && (
        <div className="pagination-minimal">
          <button
            onClick={() => goToPage(currentPage - 1)}
            disabled={!pagination.hasPrev}
            className="page-nav"
          >
            &laquo;
          </button>
          {getPageNumbers().map((p, i) => (
            p === '...' ? (
              <span key={`ellipsis-${i}`} className="page-ellipsis">&hellip;</span>
            ) : (
              <button
                key={p}
                onClick={() => goToPage(p)}
                className={`page-num ${p === pagination.page ? 'active' : ''}`}
              >
                {p}
              </button>
            )
          ))}
          <button
            onClick={() => goToPage(currentPage + 1)}
            disabled={!pagination.hasNext}
            className="page-nav"
          >
            &raquo;
          </button>
        </div>
      )}

      <div className="papers">
        {visiblePapers.map((paper) => {
          const badge = getStatusBadge(paper.status)
          const isResolving = resolvingId === paper.id
          const isExpanded = expandedAbstracts[paper.id]
          const needsReconciliation = paper.status === 'needs_reconciliation'
          const collectionInfo = getCollectionInfo(paper)
          const isSelected = selectedPapers.has(paper.id)
          const hasForeignEd = paper.foreign_edition_needed

          const isDragging = draggingPaperId === paper.id
          const isDragOver = dragOverPaperId === paper.id
          const canDrag = paper.status === 'resolved'

          return (
            <div
              key={paper.id}
              className={`paper-card-minimal ${paper.status} ${isSelected ? 'selected' : ''} ${hasForeignEd ? 'foreign-needed' : ''} ${isProcessed(paper) ? 'processed' : ''} ${isDragging ? 'dragging' : ''} ${isDragOver ? 'drag-over' : ''}`}
              draggable={canDrag}
              onDragStart={canDrag ? (e) => handleDragStart(e, paper) : undefined}
              onDragEnd={handleDragEnd}
              onDragOver={(e) => handleDragOver(e, paper)}
              onDragLeave={handleDragLeave}
              onDrop={(e) => handleDrop(e, paper)}
            >
              {/* Selection checkbox - minimal */}
              <div
                className="paper-select-area"
                onClick={() => togglePaperSelection(paper.id)}
              >
                <div className={`select-indicator ${isSelected ? 'checked' : ''}`} />
              </div>

              {/* Main content */}
              <div className="paper-content">
                {/* Title row */}
                <div className="paper-title-row">
                  {paper.link ? (
                    <a href={paper.link} target="_blank" rel="noopener noreferrer" className="paper-title-minimal">
                      {paper.title}
                    </a>
                  ) : (
                    <span className="paper-title-minimal">{paper.title}</span>
                  )}

                  {/* Status indicator - very subtle */}
                  {paper.status !== 'resolved' && (
                    <span className={`status-dot ${paper.status}`} title={badge.label} />
                  )}
                </div>

                {/* Meta row - author, year, venue */}
                <div className="paper-meta-minimal">
                  {formatAuthors(paper.authors) && (
                    <span className="meta-authors">{formatAuthors(paper.authors)}</span>
                  )}
                  {paper.year && <span className="meta-year">{paper.year}</span>}
                  {paper.venue && (
                    <span className="meta-venue">{paper.venue.length > 40 ? paper.venue.substring(0, 40) + '...' : paper.venue}</span>
                  )}
                </div>

                {/* Stats row for resolved papers - only show if there's data */}
                {paper.status === 'resolved' && (paper.citation_count > 0 || (paper.harvest_expected > 0 && paper.harvest_actual > 0) || editionCounts[paper.id] > 0 || paper.is_stale) && (
                  <div className="paper-stats-minimal">
                    {paper.citation_count > 0 && (
                      <span className="stat-item">{paper.citation_count.toLocaleString()} cited</span>
                    )}
                    {/* Only show harvest progress if we have actual harvested data */}
                    {paper.harvest_expected > 0 && paper.harvest_actual > 0 && (
                      <span className="stat-item harvest-stat">
                        <span className="harvest-bar-mini">
                          <span
                            className="harvest-bar-fill"
                            style={{
                              width: `${Math.min(paper.harvest_percent, 100)}%`,
                              backgroundColor: paper.harvest_percent >= 90 ? '#22c55e' :
                                paper.harvest_percent >= 50 ? '#eab308' : '#ef4444'
                            }}
                          />
                        </span>
                        <span className="harvest-text-mini">
                          {paper.harvest_actual}/{paper.harvest_expected}
                        </span>
                      </span>
                    )}
                    {editionCounts[paper.id] > 0 && (
                      <span className="stat-item editions">{editionCounts[paper.id]} ed.</span>
                    )}
                    {paper.is_stale && (
                      <span className="stat-item stale">stale</span>
                    )}
                  </div>
                )}

                {/* Badges row */}
                <div className="paper-badges-minimal">
                  {collectionInfo && (
                    <span className="badge-mini collection" title={collectionInfo.collection?.name}>
                      {collectionInfo.collection?.name?.substring(0, 12)}
                    </span>
                  )}
                  {hasForeignEd && (
                    <span className="badge-mini foreign">foreign ed. needed</span>
                  )}
                </div>

                {/* Abstract - expandable */}
                {paper.abstract && (
                  <div
                    className={`paper-abstract-minimal ${isExpanded ? 'expanded' : ''}`}
                    onClick={() => toggleAbstract(paper.id)}
                  >
                    {isExpanded
                      ? paper.abstract
                      : (paper.abstract.length > 120
                          ? paper.abstract.substring(0, 120) + '...'
                          : paper.abstract)}
                  </div>
                )}
              </div>

              {/* Actions column - minimal */}
              <div className="paper-actions-minimal">
                {/* Primary action based on state */}
                {paper.status === 'pending' && (
                  <button onClick={() => handleResolve(paper.id)} disabled={isResolving} className="action-btn primary">
                    {isResolving ? '...' : 'Resolve'}
                  </button>
                )}
                {paper.status === 'needs_reconciliation' && (
                  <button
                    onClick={() => {
                      let candidates = []
                      if (paper.candidates) {
                        candidates = typeof paper.candidates === 'string'
                          ? JSON.parse(paper.candidates)
                          : paper.candidates
                      }
                      setReconciliationPaper({ ...paper, candidates })
                    }}
                    className="action-btn warning"
                  >
                    Choose
                  </button>
                )}
                {paper.status === 'resolved' && (
                  <button onClick={() => onSelectPaper(paper)} className="action-btn primary">
                    {editionCounts[paper.id] > 0 ? 'View' : 'Editions'}
                  </button>
                )}
                {paper.status === 'error' && (
                  <button onClick={() => handleResolve(paper.id)} disabled={isResolving} className="action-btn">
                    Retry
                  </button>
                )}

                {/* Refresh for stale papers */}
                {paper.status === 'resolved' && paper.total_harvested_citations > 0 && (
                  <button
                    onClick={() => refreshPaper.mutate(paper.id)}
                    disabled={!!refreshingPapers[paper.id]}
                    className={`action-btn icon ${paper.is_stale ? 'highlight' : ''}`}
                    title="Refresh citations"
                  >
                    {refreshingPapers[paper.id] ? '...' : '↻'}
                  </button>
                )}

                {/* Quick harvest for unharvested */}
                {paper.status === 'resolved' && !editionCounts[paper.id] && paper.total_harvested_citations === 0 && (
                  <button
                    onClick={() => quickHarvest.mutate(paper.id)}
                    disabled={!!quickHarvestingPapers[paper.id]}
                    className="action-btn icon"
                    title="Quick harvest"
                  >
                    {quickHarvestingPapers[paper.id] ? '...' : '⚡'}
                  </button>
                )}

                {/* Collection */}
                <button
                  onClick={() => setAddToCollectionPaper(paper)}
                  className="action-btn icon"
                  title="Add to collection"
                >
                  +
                </button>

                {/* Foreign edition toggle - icon button */}
                <button
                  onClick={() => toggleForeignEdition.mutate({ paperId: paper.id, needed: !hasForeignEd })}
                  className={`action-btn icon foreign-toggle ${hasForeignEd ? 'active' : ''}`}
                  title={hasForeignEd ? 'Unmark foreign edition needed' : 'Mark foreign edition needed'}
                >
                  {hasForeignEd ? '✓' : '🌐'}
                </button>

                {/* Delete - very subtle */}
                <button
                  onClick={() => deletePaper.mutate(paper.id)}
                  className="action-btn icon danger"
                  title="Delete"
                >
                  ×
                </button>
              </div>
            </div>
          )
        })}
      </div>

      {/* Minimal Pagination - bottom */}
      {pagination && pagination.totalPages > 1 && (
        <div className="pagination-minimal bottom">
          <button
            onClick={() => goToPage(currentPage - 1)}
            disabled={!pagination.hasPrev}
            className="page-nav"
          >
            &laquo;
          </button>
          {getPageNumbers().map((p, i) => (
            p === '...' ? (
              <span key={`ellipsis-bottom-${i}`} className="page-ellipsis">&hellip;</span>
            ) : (
              <button
                key={`bottom-${p}`}
                onClick={() => goToPage(p)}
                className={`page-num ${p === pagination.page ? 'active' : ''}`}
              >
                {p}
              </button>
            )
          ))}
          <button
            onClick={() => goToPage(currentPage + 1)}
            disabled={!pagination.hasNext}
            className="page-nav"
          >
            &raquo;
          </button>
        </div>
      )}

      {/* Modals */}
      <DossierSelectModal
        isOpen={!!addToCollectionPaper}
        onClose={() => setAddToCollectionPaper(null)}
        onSelect={handleAddToCollection}
        title="Add to Collection"
        subtitle={addToCollectionPaper ? `"${addToCollectionPaper.title?.substring(0, 50)}..."` : ''}
      />

      <DossierSelectModal
        isOpen={batchAddToCollection}
        onClose={() => setBatchAddToCollection(false)}
        onSelect={handleBatchAddToCollection}
        title="Add Selected to Collection"
        subtitle={`${selectedCount} papers`}
      />

      {/* Reconciliation Modal */}
      {reconciliationPaper && (
        <div className="modal-overlay">
          <div className="modal reconciliation-modal">
            <h3>Select the Correct Paper</h3>
            <p className="reconciliation-hint">
              Multiple matches found for "<strong>{reconciliationPaper.title}</strong>"
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
                        {(candidate.citationCount || candidate.citation_count || 0).toLocaleString()} citations
                      </span>
                    </div>
                  </div>
                  <div className="candidate-actions">
                    <button className="btn-select-candidate">Select</button>
                    <button
                      className="btn-add-seed"
                      onClick={(e) => handleAddAsSeed(candidate, e)}
                      disabled={addAsSeed.isPending}
                    >
                      + Seed
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
                Delete
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
