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
      toast.success(`Marked ${data.updated} papers as needing foreign edition`)
      setSelectedPapers(new Set())
    },
    onError: (error) => {
      toast.error(`Failed to update: ${error.message}`)
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
      toast.success(`Added "${addToCollectionPaper.title?.substring(0, 30)}..." to collection`)
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

  const selectAllPending = () => {
    const pendingIds = papers?.filter(p => p.status === 'pending' || p.status === 'error').map(p => p.id) || []
    setSelectedPapers(new Set(pendingIds))
  }

  const clearSelection = () => {
    setSelectedPapers(new Set())
  }

  const handleBatchResolve = () => {
    const paperIds = Array.from(selectedPapers)
    if (paperIds.length > 0) {
      batchResolve.mutate(paperIds)
    } else {
      batchResolve.mutate([])
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

  // Pagination handlers
  const goToPage = (page) => {
    setCurrentPage(page)
    setSelectedPapers(new Set())
  }

  // Check for papers needing reconciliation on load
  const papersNeedingReconciliation = papers?.filter(p => p.status === 'needs_reconciliation') || []
  const pendingPapers = papers?.filter(p => p.status === 'pending') || []

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

  const selectablePapers = visiblePapers?.filter(p => p.status === 'pending' || p.status === 'error') || []
  const selectedCount = selectedPapers.size
  const selectedResolvable = Array.from(selectedPapers).filter(id =>
    selectablePapers.some(p => p.id === id)
  ).length

  const getCollectionInfo = (paper) => {
    if (!paper.collection_id) return null
    const collection = collections.find(c => c.id === paper.collection_id)
    const dossier = paper.dossier_id ? allDossiers.find(d => d.id === paper.dossier_id) : null
    return { collection, dossier }
  }

  // Render harvest progress bar
  const renderHarvestProgress = (paper) => {
    if (!paper.harvest_expected || paper.harvest_expected === 0) return null
    const percent = paper.harvest_percent || 0
    const color = percent >= 90 ? '#38A169' : percent >= 50 ? '#ECC94B' : '#E53E3E'

    return (
      <div className="harvest-progress" title={`${paper.harvest_actual?.toLocaleString() || 0} / ${paper.harvest_expected?.toLocaleString() || 0} citations harvested`}>
        <div className="harvest-progress-bar" style={{ width: `${Math.min(percent, 100)}%`, backgroundColor: color }} />
        <span className="harvest-progress-text">{percent.toFixed(1)}%</span>
      </div>
    )
  }

  return (
    <div className="paper-list">
      <div className="paper-list-header">
        <h2>Papers ({pagination?.total || papers.length})</h2>
        <div className="paper-list-actions">
          {/* Toggle for processed papers */}
          {processedCount > 0 && (
            <label className="toggle-processed">
              <input
                type="checkbox"
                checked={showProcessed}
                onChange={e => setShowProcessed(e.target.checked)}
              />
              <span>Show processed ({processedCount})</span>
            </label>
          )}
          {/* Selection controls */}
          {selectedCount > 0 ? (
            <>
              <span className="selection-count">{selectedCount} selected</span>
              <button
                onClick={clearSelection}
                className="btn-clear-selection"
                title="Clear selection"
              >
                Clear
              </button>
              <button
                onClick={() => setBatchAddToCollection(true)}
                className="btn-batch-collection"
                title="Add selected papers to a collection"
              >
                Add to Collection
              </button>
              <button
                onClick={() => batchForeignEdition.mutate({ paperIds: Array.from(selectedPapers), needed: true })}
                className="btn-foreign-edition"
                title="Mark selected papers as needing foreign editions"
              >
                Mark Foreign Ed. Needed
              </button>
              {selectedResolvable > 0 && (
                <button
                  onClick={handleBatchResolve}
                  disabled={batchResolving}
                  className="btn-batch-resolve"
                  title={`Resolve ${selectedResolvable} selected papers`}
                >
                  {batchResolving ? 'Queuing...' : `Resolve Selected (${selectedResolvable})`}
                </button>
              )}
            </>
          ) : (
            <>
              <button
                onClick={selectAllVisible}
                className="btn-select-all"
                title="Select all visible papers"
              >
                Select All ({visiblePapers.length})
              </button>
              {selectablePapers.length > 0 && (
                <button
                  onClick={selectAllPending}
                  className="btn-select-all"
                  title="Select all pending papers"
                >
                  Select Pending ({selectablePapers.length})
                </button>
              )}
            </>
          )}
        </div>
      </div>

      {/* Alert for papers needing reconciliation */}
      {papersNeedingReconciliation.length > 0 && (
        <div className="reconciliation-alert">
          <span className="alert-icon">!</span>
          <span>{papersNeedingReconciliation.length} paper(s) need your attention - multiple Scholar matches found</span>
        </div>
      )}

      {/* Pagination controls - Top */}
      {pagination && pagination.totalPages > 1 && (
        <div className="pagination-controls">
          <button
            onClick={() => goToPage(1)}
            disabled={!pagination.hasPrev}
            className="btn-pagination"
          >
            First
          </button>
          <button
            onClick={() => goToPage(currentPage - 1)}
            disabled={!pagination.hasPrev}
            className="btn-pagination"
          >
            Prev
          </button>
          <span className="pagination-info">
            Page {pagination.page} of {pagination.totalPages} ({pagination.total} papers)
          </span>
          <button
            onClick={() => goToPage(currentPage + 1)}
            disabled={!pagination.hasNext}
            className="btn-pagination"
          >
            Next
          </button>
          <button
            onClick={() => goToPage(pagination.totalPages)}
            disabled={!pagination.hasNext}
            className="btn-pagination"
          >
            Last
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

          return (
            <div key={paper.id} className={`paper-card ${paper.status === 'resolved' ? 'paper-card-resolved' : ''} ${needsReconciliation ? 'paper-card-warning' : ''} ${isSelected ? 'paper-card-selected' : ''} ${isProcessed(paper) ? 'paper-card-processed' : ''}`}>
              {/* Checkbox for selection */}
              <div className="paper-select-checkbox">
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => togglePaperSelection(paper.id)}
                  onClick={(e) => e.stopPropagation()}
                  title={isSelected ? "Deselect paper" : "Select paper"}
                />
              </div>
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
                <div className="paper-badges">
                  <span className={`badge ${badge.class}`}>
                    {isResolving ? 'Resolving...' : badge.label}
                  </span>
                  {/* Foreign edition needed badge */}
                  {paper.foreign_edition_needed && (
                    <span className="badge badge-foreign-edition" title="Needs foreign edition lookup">
                      Foreign Ed.
                    </span>
                  )}
                  {/* Collection/Dossier badge */}
                  {collectionInfo && (
                    <span
                      className="badge badge-collection"
                      style={{ borderColor: collectionInfo.collection?.color || '#3182CE' }}
                      title={collectionInfo.dossier
                        ? `${collectionInfo.collection?.name} > ${collectionInfo.dossier.name}`
                        : collectionInfo.collection?.name}
                    >
                      {collectionInfo.collection?.name?.substring(0, 15) || '...'}
                      {collectionInfo.dossier && ` > ${collectionInfo.dossier.name?.substring(0, 10)}`}
                    </span>
                  )}
                </div>
              </div>

              {/* Metadata row: authors, year, venue */}
              <div className="paper-meta">
                {formatAuthors(paper.authors) && (
                  <span className="paper-authors">{formatAuthors(paper.authors)}</span>
                )}
                {paper.year && (
                  <span className="paper-year">
                    {formatAuthors(paper.authors) ? ' - ' : ''}{paper.year}
                  </span>
                )}
                {paper.venue && (
                  <span className="paper-venue">
                    {' - '}{paper.venue.length > 50 ? paper.venue.substring(0, 50) + '...' : paper.venue}
                  </span>
                )}
              </div>

              {/* Citation count and harvest progress for resolved papers */}
              {paper.status === 'resolved' && (
                <div className="paper-stats">
                  {paper.citation_count > 0 && (
                    <span className="paper-citations">
                      {paper.citation_count.toLocaleString()} citations
                    </span>
                  )}
                  {/* Harvest progress bar */}
                  {renderHarvestProgress(paper)}
                  {/* Harvest stats */}
                  {paper.total_harvested_citations > 0 && (
                    <span className="paper-harvested">
                      {paper.total_harvested_citations.toLocaleString()} harvested
                    </span>
                  )}
                  {paper.is_stale && (
                    <span className="staleness-badge stale" title={`Last harvested ${paper.days_since_harvest} days ago`}>
                      Stale ({paper.days_since_harvest}d)
                    </span>
                  )}
                  {paper.any_edition_harvested_at && !paper.is_stale && (
                    <span className="staleness-badge fresh" title={`Last harvested ${paper.days_since_harvest} days ago`}>
                      Fresh
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
                    {isExpanded ? 'v' : '>'}
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
                    {isResolving ? 'Resolving...' : 'Resolve on Scholar'}
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
                    className="btn-reconcile"
                  >
                    Choose Correct Match
                  </button>
                )}
                {paper.status === 'error' && (
                  <button
                    onClick={() => handleResolve(paper.id)}
                    disabled={isResolving}
                    className="btn-resolve"
                  >
                    {isResolving ? 'Retrying...' : 'Retry Resolution'}
                  </button>
                )}
                <button
                  onClick={() => onSelectPaper(paper)}
                  disabled={paper.status !== 'resolved'}
                  className={editionCounts[paper.id] > 0 ? 'btn-success' : 'btn-primary'}
                  title={paper.status !== 'resolved' ? 'Resolve paper first' : editionCounts[paper.id] > 0 ? 'View discovered editions' : 'Discover all editions'}
                >
                  {editionCounts[paper.id] > 0
                    ? `View ${editionCounts[paper.id]} Editions`
                    : 'Discover Editions'}
                </button>
                {/* Quick Harvest */}
                {paper.status === 'resolved' && !editionCounts[paper.id] && paper.total_harvested_citations === 0 && (
                  <button
                    onClick={() => quickHarvest.mutate(paper.id)}
                    disabled={!!quickHarvestingPapers[paper.id]}
                    className="btn-quick-harvest"
                    title="Skip edition discovery and harvest citations directly"
                  >
                    {quickHarvestingPapers[paper.id] ? 'Harvesting...' : 'Quick Harvest'}
                  </button>
                )}
                {/* Refresh button */}
                {paper.status === 'resolved' && paper.total_harvested_citations > 0 && (
                  <button
                    onClick={() => refreshPaper.mutate(paper.id)}
                    disabled={!!refreshingPapers[paper.id] || refreshPaper.isPending}
                    className={`btn-refresh ${paper.is_stale ? 'stale' : ''}`}
                    title={paper.is_stale
                      ? `Refresh citations (${paper.days_since_harvest} days since last harvest)`
                      : 'Refresh citations'}
                  >
                    {refreshingPapers[paper.id] ? 'Refreshing...' : 'Refresh'}
                  </button>
                )}
                {/* Add to Collection */}
                <button
                  onClick={() => setAddToCollectionPaper(paper)}
                  className="btn-collection"
                  title="Add this paper to a collection"
                >
                  Add to Collection
                </button>
                {/* Foreign edition toggle */}
                <button
                  onClick={() => toggleForeignEdition.mutate({ paperId: paper.id, needed: !paper.foreign_edition_needed })}
                  className={`btn-foreign-edition ${paper.foreign_edition_needed ? 'active' : ''}`}
                  title={paper.foreign_edition_needed ? 'Unmark as needing foreign edition' : 'Mark as needing foreign edition'}
                >
                  {paper.foreign_edition_needed ? 'Unmark Foreign' : 'Foreign Ed.'}
                </button>
                <button
                  onClick={() => deletePaper.mutate(paper.id)}
                  className="btn-danger"
                  disabled={deletePaper.isPending}
                >
                  Delete
                </button>
              </div>
            </div>
          )
        })}
      </div>

      {/* Pagination controls - Bottom */}
      {pagination && pagination.totalPages > 1 && (
        <div className="pagination-controls">
          <button
            onClick={() => goToPage(1)}
            disabled={!pagination.hasPrev}
            className="btn-pagination"
          >
            First
          </button>
          <button
            onClick={() => goToPage(currentPage - 1)}
            disabled={!pagination.hasPrev}
            className="btn-pagination"
          >
            Prev
          </button>
          <span className="pagination-info">
            Page {pagination.page} of {pagination.totalPages} ({pagination.total} papers)
          </span>
          <button
            onClick={() => goToPage(currentPage + 1)}
            disabled={!pagination.hasNext}
            className="btn-pagination"
          >
            Next
          </button>
          <button
            onClick={() => goToPage(pagination.totalPages)}
            disabled={!pagination.hasNext}
            className="btn-pagination"
          >
            Last
          </button>
        </div>
      )}

      {/* Add to Collection Modal - Single Paper */}
      <DossierSelectModal
        isOpen={!!addToCollectionPaper}
        onClose={() => setAddToCollectionPaper(null)}
        onSelect={handleAddToCollection}
        title="Add to Collection"
        subtitle={addToCollectionPaper ? `Add "${addToCollectionPaper.title?.substring(0, 50)}..." to a collection` : ''}
      />

      {/* Add to Collection Modal - Batch */}
      <DossierSelectModal
        isOpen={batchAddToCollection}
        onClose={() => setBatchAddToCollection(false)}
        onSelect={handleBatchAddToCollection}
        title="Add Selected to Collection"
        subtitle={`Add ${selectedCount} selected papers to a collection`}
      />

      {/* Reconciliation Modal */}
      {reconciliationPaper && (
        <div className="modal-overlay">
          <div className="modal reconciliation-modal">
            <h3>Select the Correct Paper</h3>
            <p className="reconciliation-hint">
              Multiple matches were found for "<strong>{reconciliationPaper.title}</strong>".
              Please select the correct one:
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
                      {candidate.scholarId && (
                        <span className="candidate-scholar-id">ID: {candidate.scholarId}</span>
                      )}
                    </div>
                    {candidate.abstract && (
                      <p className="candidate-abstract">
                        {candidate.abstract.substring(0, 200)}...
                      </p>
                    )}
                  </div>
                  <div className="candidate-actions">
                    <button className="btn-select-candidate">
                      Select
                    </button>
                    <button
                      className="btn-add-seed"
                      onClick={(e) => handleAddAsSeed(candidate, e)}
                      disabled={addAsSeed.isPending}
                      title="Add this paper as a new seed to harvest"
                    >
                      {addAsSeed.isPending ? 'Adding...' : 'Add as Seed'}
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
                Delete Paper
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
