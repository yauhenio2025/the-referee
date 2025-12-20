import { useState, useEffect, useMemo, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { api } from '../lib/api'
import { useToast } from './Toast'

/**
 * Edition Discovery - Tufte-inspired compact data view
 *
 * Design principles:
 * - High data-ink ratio: maximize information per pixel
 * - Small multiples: compact table rows, not cards
 * - Quick batch actions: one-click select by confidence/language
 * - Minimal chrome: no decorative elements
 */
export default function EditionDiscovery({ paper, onBack }) {
  const navigate = useNavigate()
  const toast = useToast()
  const [languageStrategy, setLanguageStrategy] = useState('recommended')
  const [customLanguages, setCustomLanguages] = useState([])
  const [showLanguageModal, setShowLanguageModal] = useState(false)
  const [showManualEditionModal, setShowManualEditionModal] = useState(false)
  const [manualEditionInput, setManualEditionInput] = useState('')
  const [manualEditionLanguage, setManualEditionLanguage] = useState('')
  const [manualEditionResult, setManualEditionResult] = useState(null)
  const [isLoadingRecs, setIsLoadingRecs] = useState(false)
  const [recommendations, setRecommendations] = useState(null)
  const [discoveryProgress, setDiscoveryProgress] = useState(null)
  const [expandedGroups, setExpandedGroups] = useState({ high: true, uncertain: true, rejected: false, excluded: false })
  const [languageFilter, setLanguageFilter] = useState(null)
  const [showExcluded, setShowExcluded] = useState(false) // Toggle to show excluded editions
  const queryClient = useQueryClient()

  const { data: editions, isLoading } = useQuery({
    queryKey: ['editions', paper.id],
    queryFn: () => api.getPaperEditions(paper.id),
  })

  const { data: citations } = useQuery({
    queryKey: ['citations', paper.id],
    queryFn: () => api.getPaperCitations(paper.id),
  })

  const { data: languages } = useQuery({
    queryKey: ['languages'],
    queryFn: () => api.getAvailableLanguages(),
  })

  // Fetch LLM recommendations when modal opens
  useEffect(() => {
    if (showLanguageModal && !recommendations && !isLoadingRecs) {
      setIsLoadingRecs(true)
      api.recommendLanguages({
        title: paper.title,
        author: paper.authors,
        year: paper.year,
      }).then(recs => {
        setRecommendations(recs)
        if (recs?.recommended) {
          setCustomLanguages(recs.recommended)
        }
        setIsLoadingRecs(false)
      }).catch(err => {
        console.error('Failed to get language recommendations:', err)
        setIsLoadingRecs(false)
      })
    }
  }, [showLanguageModal, recommendations, isLoadingRecs, paper])

  const discoverEditions = useMutation({
    mutationFn: async () => {
      let langsToUse = customLanguages
      if (languageStrategy === 'english_only') {
        langsToUse = ['english']
      } else if (languageStrategy === 'major_languages') {
        langsToUse = ['english', 'german', 'french', 'spanish', 'portuguese', 'italian', 'russian', 'chinese', 'japanese']
      } else if (languageStrategy === 'recommended' && recommendations?.recommended) {
        langsToUse = recommendations.recommended
      }

      setShowLanguageModal(false)
      setDiscoveryProgress({ stage: 'searching', message: 'Generating queries...', progress: 10 })

      const progressInterval = setInterval(() => {
        setDiscoveryProgress(prev => {
          if (!prev || prev.progress >= 90) return prev
          const newProgress = Math.min(prev.progress + Math.random() * 15, 90)
          const messages = ['Searching Scholar...', 'Analyzing results...', 'Identifying editions...', 'Classifying...']
          return { ...prev, progress: newProgress, message: messages[Math.floor(newProgress / 25)] }
        })
      }, 1500)

      try {
        const result = await api.discoverEditions(paper.id, { languageStrategy, customLanguages: langsToUse })
        clearInterval(progressInterval)
        setDiscoveryProgress({ stage: 'complete', message: `Found ${result.total_found} editions`, progress: 100 })
        setTimeout(() => setDiscoveryProgress(null), 2000)
        return result
      } catch (error) {
        clearInterval(progressInterval)
        setDiscoveryProgress(null)
        throw error
      }
    },
    onSuccess: () => queryClient.invalidateQueries(['editions', paper.id]),
  })

  // Optimistic update helper for selections
  const updateEditionsOptimistically = useCallback((ids, updates) => {
    queryClient.setQueryData(['editions', paper.id], (old) => {
      if (!old) return old
      return old.map(ed => ids.includes(ed.id) ? { ...ed, ...updates } : ed)
    })
  }, [queryClient, paper.id])

  const selectEditions = useMutation({
    mutationFn: ({ ids, selected }) => api.selectEditions(ids, selected),
    onMutate: async ({ ids, selected }) => {
      // Cancel outgoing refetches
      await queryClient.cancelQueries(['editions', paper.id])
      // Snapshot previous value
      const previous = queryClient.getQueryData(['editions', paper.id])
      // Optimistically update
      updateEditionsOptimistically(ids, { selected })
      return { previous }
    },
    onError: (err, variables, context) => {
      // Rollback on error
      if (context?.previous) {
        queryClient.setQueryData(['editions', paper.id], context.previous)
      }
    },
    onSettled: () => {
      // Refetch after mutation settles
      queryClient.invalidateQueries(['editions', paper.id])
    },
  })

  const updateConfidence = useMutation({
    mutationFn: ({ ids, confidence }) => api.updateEditionConfidence(ids, confidence),
    onMutate: async ({ ids, confidence }) => {
      await queryClient.cancelQueries(['editions', paper.id])
      const previous = queryClient.getQueryData(['editions', paper.id])
      // Optimistically update - if rejecting, also deselect
      const updates = confidence === 'rejected'
        ? { confidence, selected: false }
        : { confidence }
      updateEditionsOptimistically(ids, updates)
      return { previous }
    },
    onError: (err, variables, context) => {
      if (context?.previous) {
        queryClient.setQueryData(['editions', paper.id], context.previous)
      }
    },
    onSettled: () => {
      queryClient.invalidateQueries(['editions', paper.id])
    },
  })

  // Exclude editions
  const excludeEditions = useMutation({
    mutationFn: ({ ids, excluded }) => api.excludeEditions(ids, excluded),
    onMutate: async ({ ids, excluded }) => {
      await queryClient.cancelQueries(['editions', paper.id])
      const previous = queryClient.getQueryData(['editions', paper.id])
      updateEditionsOptimistically(ids, { excluded })
      return { previous, ids, excluded }
    },
    onSuccess: (data, { ids, excluded }) => {
      if (excluded) {
        toast.info(`⊘ Excluded ${ids.length} edition${ids.length > 1 ? 's' : ''}`)
      } else {
        toast.success(`↩ Restored ${ids.length} edition${ids.length > 1 ? 's' : ''}`)
      }
    },
    onError: (err, variables, context) => {
      if (context?.previous) {
        queryClient.setQueryData(['editions', paper.id], context.previous)
      }
      toast.error(`Failed to update editions: ${err.message}`)
    },
    onSettled: () => {
      queryClient.invalidateQueries(['editions', paper.id])
    },
  })

  // Add edition as new seed paper
  const addAsSeed = useMutation({
    mutationFn: (editionId) => api.addEditionAsSeed(editionId),
    onSuccess: (result) => {
      // Invalidate editions (source will be excluded)
      queryClient.invalidateQueries(['editions', paper.id])
      // Invalidate papers list (new paper added)
      queryClient.invalidateQueries(['papers'])
      // Show success toast
      toast.success(`🌱 Created new seed: ${result.title.substring(0, 50)}...`)
    },
    onError: (error) => {
      toast.error(`Failed to create seed: ${error.message}`)
    },
  })

  // Finalize editions
  const finalizeEditions = useMutation({
    mutationFn: () => api.finalizeEditions(paper.id),
    onSuccess: (result) => {
      queryClient.invalidateQueries(['editions', paper.id])
      queryClient.invalidateQueries(['papers', paper.id])
      toast.success(`✓ Editions finalized (${result.editions_excluded} candidates hidden)`)
    },
    onError: (error) => {
      toast.error(`Failed to finalize: ${error.message}`)
    },
  })

  // Reopen editions
  const reopenEditions = useMutation({
    mutationFn: () => api.reopenEditions(paper.id),
    onSuccess: () => {
      queryClient.invalidateQueries(['editions', paper.id])
      queryClient.invalidateQueries(['papers', paper.id])
      setShowExcluded(true) // Show excluded editions when reopening
      toast.info('🔓 Editions reopened for editing')
    },
    onError: (error) => {
      toast.error(`Failed to reopen: ${error.message}`)
    },
  })

  const [citationJobId, setCitationJobId] = useState(null)
  const [citationProgress, setCitationProgress] = useState(null)

  // Poll for citation extraction job
  useEffect(() => {
    if (!citationJobId) return

    const pollInterval = setInterval(async () => {
      try {
        const job = await api.getJob(citationJobId)

        if (job.status === 'running') {
          setCitationProgress({
            message: job.progress_message || 'Extracting citations...',
            progress: job.progress,
          })
        } else if (job.status === 'completed') {
          const result = job.result || {}
          setCitationProgress({
            message: `Done! Found ${result.total_citations_found?.toLocaleString() || 0} unique citations`,
            progress: 100,
            done: true,
            result,
          })
          setCitationJobId(null)
          queryClient.invalidateQueries(['stats'])
          setTimeout(() => setCitationProgress(null), 10000)
        } else if (job.status === 'failed') {
          setCitationProgress({
            message: `Failed: ${job.error || 'Unknown error'}`,
            error: true,
          })
          setCitationJobId(null)
          setTimeout(() => setCitationProgress(null), 10000)
        }
      } catch (err) {
        console.error('Citation job poll error:', err)
      }
    }, 3000)

    return () => clearInterval(pollInterval)
  }, [citationJobId, queryClient])

  const extractCitations = useMutation({
    mutationFn: () => api.extractCitations(paper.id),
    onSuccess: (data) => {
      setCitationJobId(data.job_id)
      setCitationProgress({
        message: `Queued: extracting from ${data.editions_to_process} editions (~${data.estimated_time_minutes} min)`,
        progress: 0,
      })
      queryClient.invalidateQueries(['jobs'])
    },
  })

  const clearAndRediscover = useMutation({
    mutationFn: async () => {
      // Clear existing editions first
      await api.clearPaperEditions(paper.id)
      // Then trigger a new discovery via the modal
      setShowLanguageModal(true)
    },
    onSuccess: () => queryClient.invalidateQueries(['editions', paper.id]),
  })

  const [fetchMoreProgress, setFetchMoreProgress] = useState(null)
  const [activeJobs, setActiveJobs] = useState({}) // { language: jobId }
  const [harvestingEditions, setHarvestingEditions] = useState({}) // { editionId: jobId }
  const [refreshBatchId, setRefreshBatchId] = useState(null)
  const [refreshProgress, setRefreshProgress] = useState(null)

  // Poll for job status updates
  useEffect(() => {
    const activeJobIds = Object.values(activeJobs).filter(Boolean)
    if (activeJobIds.length === 0) return

    const pollInterval = setInterval(async () => {
      for (const [lang, jobId] of Object.entries(activeJobs)) {
        if (!jobId) continue
        try {
          const job = await api.getJob(jobId)

          if (job.status === 'running') {
            setFetchMoreProgress({
              language: lang,
              message: job.progress_message || `Fetching ${lang} editions...`,
              progress: job.progress,
            })
          } else if (job.status === 'completed') {
            const result = job.result || {}
            setFetchMoreProgress({
              language: lang,
              message: `Found ${result.new_editions_found || 0} new ${lang} editions!`,
              progress: 100,
              done: true,
            })
            setActiveJobs(prev => ({ ...prev, [lang]: null }))
            queryClient.invalidateQueries(['editions', paper.id])
            setTimeout(() => setFetchMoreProgress(null), 3000)
          } else if (job.status === 'failed') {
            setFetchMoreProgress({
              language: lang,
              message: `Failed: ${job.error || 'Unknown error'}`,
              error: true,
            })
            setActiveJobs(prev => ({ ...prev, [lang]: null }))
            setTimeout(() => setFetchMoreProgress(null), 5000)
          }
        } catch (err) {
          console.error('Job poll error:', err)
        }
      }
    }, 2000) // Poll every 2 seconds

    return () => clearInterval(pollInterval)
  }, [activeJobs, queryClient, paper.id])

  const fetchMoreInLanguage = useMutation({
    mutationFn: async (language) => {
      setFetchMoreProgress({ language, message: `Queueing ${language} fetch...`, progress: 0 })
      // Use async version that queues a job
      return await api.fetchMoreInLanguageAsync(paper.id, language)
    },
    onSuccess: (result) => {
      // Job is now queued, start polling
      setActiveJobs(prev => ({ ...prev, [result.language]: result.job_id }))
      setFetchMoreProgress({
        language: result.language,
        message: result.message || `Queued: ${result.language}`,
        progress: 5,
      })
    },
    onError: (error) => {
      setFetchMoreProgress({ message: `Error: ${error.message}`, error: true })
      setTimeout(() => setFetchMoreProgress(null), 5000)
    },
  })

  const addManualEdition = useMutation({
    mutationFn: async () => {
      return await api.addManualEdition(
        paper.id,
        manualEditionInput,
        manualEditionLanguage || null
      )
    },
    onSuccess: (result) => {
      setManualEditionResult(result)
      if (result.success) {
        queryClient.invalidateQueries(['editions', paper.id])
        // Clear input after successful add
        setTimeout(() => {
          setManualEditionInput('')
          setManualEditionLanguage('')
        }, 2000)
      }
    },
    onError: (error) => {
      setManualEditionResult({ success: false, message: error.message })
    },
  })

  // Refresh paper citations (auto-updater feature)
  const refreshPaperMutation = useMutation({
    mutationFn: () => api.refreshPaper(paper.id),
    onSuccess: (result) => {
      if (result.jobs_created > 0) {
        setRefreshBatchId(result.batch_id)
        setRefreshProgress({
          total: result.editions_included,
          completed: 0,
          message: `Queued ${result.jobs_created} refresh jobs for ${result.editions_included} editions`,
        })
        queryClient.invalidateQueries(['jobs'])
      } else {
        setRefreshProgress({
          total: 0,
          completed: 0,
          message: 'No editions need refreshing (all fresh or never harvested)',
          done: true,
        })
        setTimeout(() => setRefreshProgress(null), 3000)
      }
    },
    onError: (error) => {
      setRefreshProgress({
        message: `Refresh failed: ${error.message}`,
        error: true,
      })
      setTimeout(() => setRefreshProgress(null), 5000)
    },
  })

  // Poll for refresh batch status
  useEffect(() => {
    if (!refreshBatchId) return

    const pollInterval = setInterval(async () => {
      try {
        const status = await api.getRefreshStatus(refreshBatchId)
        setRefreshProgress({
          total: status.total_jobs,
          completed: status.completed_jobs,
          failed: status.failed_jobs,
          newCitations: status.new_citations_added,
          message: `Refreshing: ${status.completed_jobs}/${status.total_jobs} done`,
          done: status.is_complete,
        })

        if (status.is_complete) {
          setRefreshBatchId(null)
          queryClient.invalidateQueries(['editions', paper.id])
          queryClient.invalidateQueries(['citations', paper.id])
          queryClient.invalidateQueries(['papers'])
          setRefreshProgress({
            total: status.total_jobs,
            completed: status.completed_jobs,
            newCitations: status.new_citations_added,
            message: `Done! ${status.new_citations_added} new citations found`,
            done: true,
          })
          setTimeout(() => setRefreshProgress(null), 5000)
        }
      } catch (err) {
        console.error('Refresh status poll error:', err)
      }
    }, 3000)

    return () => clearInterval(pollInterval)
  }, [refreshBatchId, paper.id, queryClient])

  // Poll for edition harvest jobs
  useEffect(() => {
    const activeHarvestIds = Object.entries(harvestingEditions).filter(([_, jobId]) => jobId)
    if (activeHarvestIds.length === 0) return

    const pollInterval = setInterval(async () => {
      for (const [editionId, jobId] of activeHarvestIds) {
        if (!jobId) continue
        try {
          const job = await api.getJob(jobId)

          if (job.status === 'completed') {
            // Job done - remove from tracking and refresh
            setHarvestingEditions(prev => {
              const next = { ...prev }
              delete next[editionId]
              return next
            })
            queryClient.invalidateQueries(['citations', paper.id])
            queryClient.invalidateQueries(['stats'])
          } else if (job.status === 'failed') {
            // Job failed - remove from tracking
            setHarvestingEditions(prev => {
              const next = { ...prev }
              delete next[editionId]
              return next
            })
          }
        } catch (err) {
          console.error('Harvest job poll error:', err)
        }
      }
    }, 3000)

    return () => clearInterval(pollInterval)
  }, [harvestingEditions, queryClient, paper.id])

  // Harvest citations from a single edition
  const harvestEdition = useCallback(async (editionId) => {
    try {
      const result = await api.extractCitations(paper.id, { editionIds: [editionId] })
      setHarvestingEditions(prev => ({ ...prev, [editionId]: result.job_id }))
      queryClient.invalidateQueries(['jobs'])
    } catch (error) {
      console.error('Failed to start harvest:', error)
    }
  }, [paper.id, queryClient])

  // Select editions AND start harvesting them immediately
  const selectAndHarvest = useCallback(async (editionIds) => {
    try {
      // First mark them as selected
      await api.selectEditions(editionIds, true)
      queryClient.invalidateQueries(['editions', paper.id])

      // Then start harvesting
      const result = await api.extractCitations(paper.id, { editionIds })

      // Track all of them as harvesting
      const newHarvesting = {}
      editionIds.forEach(id => { newHarvesting[id] = result.job_id })
      setHarvestingEditions(prev => ({ ...prev, ...newHarvesting }))
      queryClient.invalidateQueries(['jobs'])
      toast.success(`📥 Started harvesting ${editionIds.length} edition${editionIds.length > 1 ? 's' : ''} (Job #${result.job_id})`)
    } catch (error) {
      console.error('Failed to select and harvest:', error)
      toast.error(`Failed to start harvest: ${error.message}`)
    }
  }, [paper.id, queryClient, toast])

  // Navigate to citations filtered by a specific edition
  const viewCitationsForEdition = useCallback((editionId) => {
    navigate(`/paper/${paper.id}/citations?edition=${editionId}`)
  }, [paper.id, navigate])

  // Computed data
  const { highConfidence, uncertain, rejected, excluded, languageGroups, selectedCount, totalCitations, excludedCount, isFinalized } = useMemo(() => {
    if (!editions) return { highConfidence: [], uncertain: [], rejected: [], excluded: [], languageGroups: {}, selectedCount: 0, totalCitations: 0, excludedCount: 0, isFinalized: false }

    // Filter by language first
    const filtered = languageFilter ? editions.filter(e => e.language === languageFilter) : editions

    // Separate excluded from non-excluded
    const nonExcluded = filtered.filter(e => !e.excluded)
    const excludedEditions = filtered.filter(e => e.excluded)

    return {
      highConfidence: nonExcluded.filter(e => e.confidence === 'high'),
      uncertain: nonExcluded.filter(e => e.confidence === 'uncertain'),
      rejected: nonExcluded.filter(e => e.confidence === 'rejected'),
      excluded: excludedEditions,
      languageGroups: editions.reduce((acc, e) => {
        const lang = e.language || 'Unknown'
        acc[lang] = (acc[lang] || 0) + 1
        return acc
      }, {}),
      selectedCount: editions.filter(e => e.selected && !e.excluded).length,
      totalCitations: editions.filter(e => e.selected && !e.excluded).reduce((sum, e) => sum + (e.citation_count || 0), 0),
      excludedCount: editions.filter(e => e.excluded).length,
      isFinalized: paper.editions_finalized || false,
    }
  }, [editions, languageFilter, paper.editions_finalized])

  // Batch actions
  const selectByConfidence = (confidence) => {
    const ids = editions.filter(e => e.confidence === confidence).map(e => e.id)
    if (ids.length) selectEditions.mutate({ ids, selected: true })
  }

  const deselectByConfidence = (confidence) => {
    const ids = editions.filter(e => e.confidence === confidence).map(e => e.id)
    if (ids.length) selectEditions.mutate({ ids, selected: false })
  }

  const selectByLanguage = (lang) => {
    const ids = editions.filter(e => e.language === lang).map(e => e.id)
    if (ids.length) selectEditions.mutate({ ids, selected: true })
  }

  const selectAll = () => {
    const ids = editions.filter(e => e.confidence !== 'rejected').map(e => e.id)
    selectEditions.mutate({ ids, selected: true })
  }

  const deselectAll = () => {
    const ids = editions.map(e => e.id)
    selectEditions.mutate({ ids, selected: false })
  }

  const markAsIrrelevant = (ids) => {
    if (ids.length) updateConfidence.mutate({ ids, confidence: 'rejected' })
  }

  const markAsUncertain = (ids) => {
    if (ids.length) updateConfidence.mutate({ ids, confidence: 'uncertain' })
  }

  const markAsHigh = (ids) => {
    if (ids.length) updateConfidence.mutate({ ids, confidence: 'high' })
  }

  const toggleGroup = (group) => {
    setExpandedGroups(prev => ({ ...prev, [group]: !prev[group] }))
  }

  const toggleLanguage = (code) => {
    if (customLanguages.includes(code)) {
      setCustomLanguages(customLanguages.filter(c => c !== code))
    } else {
      setCustomLanguages([...customLanguages, code])
    }
  }

  return (
    <div className="edition-discovery tufte">
      {/* Compact Header */}
      <header className="ed-header">
        <button onClick={onBack} className="btn-text">← Papers</button>
        <div className="ed-title">
          <h2>{paper.title}</h2>
          <span className="meta">{paper.authors} {paper.year && `(${paper.year})`}</span>
        </div>
      </header>

      {/* Action Bar */}
      <div className="ed-actions">
        <button onClick={() => setShowLanguageModal(true)} disabled={discoverEditions.isPending} className="btn-primary">
          Discover Editions
        </button>
        {editions?.length > 0 && (
          <button
            onClick={() => clearAndRediscover.mutate()}
            disabled={clearAndRediscover.isPending || discoverEditions.isPending}
            className="btn-warning"
            title="Clear all editions and run fresh discovery"
          >
            🔄 Clear & Rediscover
          </button>
        )}
        <button
          onClick={() => extractCitations.mutate()}
          disabled={selectedCount === 0 || extractCitations.isPending || !!citationJobId}
          className="btn-success"
        >
          {citationJobId ? '⏳ Extracting...' : `Extract Citations (${selectedCount} selected, ~${totalCitations.toLocaleString()} citing papers)`}
        </button>
        {citations?.length > 0 && (
          <button
            onClick={() => navigate(`/paper/${paper.id}/citations`)}
            className="btn-info"
          >
            🔗 View Citations ({citations.length})
          </button>
        )}
        <button
          onClick={() => {
            setShowManualEditionModal(true)
            setManualEditionResult(null)
          }}
          className="btn-secondary"
          title="Manually add an edition by title, URL, or pasted text"
        >
          ➕ Add Edition
        </button>
        {/* Refresh button - check for new citations since last harvest */}
        {editions?.some(e => e.last_harvested_at) && (
          <button
            onClick={() => refreshPaperMutation.mutate()}
            disabled={refreshPaperMutation.isPending || !!refreshBatchId}
            className={`btn-refresh ${editions?.some(e => e.is_stale) ? 'stale' : ''}`}
            title={editions?.some(e => e.is_stale)
              ? 'Some editions are stale - click to refresh'
              : 'Check for new citations since last harvest'}
          >
            {refreshBatchId ? '🔄 Refreshing...' : '🔄 Refresh Citations'}
          </button>
        )}
      </div>

      {/* Refresh Progress */}
      {refreshProgress && (
        <div className={`ed-progress refresh-progress ${refreshProgress.done ? 'done' : ''} ${refreshProgress.error ? 'error' : ''}`}>
          {refreshProgress.total > 0 && !refreshProgress.done && (
            <div className="progress-bar" style={{ width: `${(refreshProgress.completed / refreshProgress.total) * 100}%` }} />
          )}
          <span>
            {refreshProgress.message}
            {refreshProgress.newCitations > 0 && ` (${refreshProgress.newCitations} new citations)`}
          </span>
        </div>
      )}

      {/* Progress */}
      {discoveryProgress && (
        <div className="ed-progress">
          <div className="progress-bar" style={{ width: `${discoveryProgress.progress}%` }} />
          <span>{discoveryProgress.message}</span>
        </div>
      )}

      {/* Citation Extraction Progress */}
      {citationProgress && (
        <div className={`ed-progress citation-progress ${citationProgress.done ? 'done' : ''} ${citationProgress.error ? 'error' : ''}`}>
          <div className="progress-bar" style={{ width: `${citationProgress.progress || 0}%` }} />
          <span>{citationProgress.message}</span>
          {citationProgress.done && citationProgress.result && (
            <span className="citation-stats">
              {' '}| {citationProgress.result.editions_processed} editions processed
              {citationProgress.result.intersection_distribution && Object.entries(citationProgress.result.intersection_distribution)
                .filter(([k]) => parseInt(k) > 1)
                .map(([k, v]) => ` | ${v} cite ${k}+ editions`).join('')}
            </span>
          )}
        </div>
      )}

      {/* Finalized Banner */}
      {isFinalized && (
        <div className="finalized-banner">
          <span>✓ Editions finalized - showing only selected editions</span>
          <button
            onClick={() => reopenEditions.mutate()}
            disabled={reopenEditions.isPending}
            className="btn-reopen-inline"
          >
            🔓 Reopen for Editing
          </button>
        </div>
      )}

      {/* Stats + Batch Actions */}
      {editions?.length > 0 && (
        <div className="ed-toolbar">
          <div className="stats-row">
            <span className="stat" onClick={() => selectByConfidence('high')} title="Click to select all">
              <strong>{highConfidence.length}</strong> high
            </span>
            <span className="stat uncertain" onClick={() => selectByConfidence('uncertain')} title="Click to select all">
              <strong>{uncertain.length}</strong> uncertain
            </span>
            <span className="stat rejected">
              <strong>{rejected.length}</strong> rejected
            </span>
            <span className="stat-sep">|</span>
            <span className="stat selected">
              <strong>{selectedCount}</strong>/{editions.length} selected
            </span>
          </div>

          <div className="batch-actions">
            <button onClick={selectAll} className="btn-sm">Select All</button>
            <button onClick={deselectAll} className="btn-sm">Clear</button>
            <button onClick={() => selectByConfidence('high')} className="btn-sm btn-high">+ High</button>
            <button onClick={() => deselectByConfidence('uncertain')} className="btn-sm">− Uncertain</button>
            <span className="action-sep">|</span>
            {isFinalized ? (
              <button
                onClick={() => reopenEditions.mutate()}
                disabled={reopenEditions.isPending}
                className="btn-sm btn-reopen"
                title="Show all candidates again for editing"
              >
                🔓 Reopen Editions
              </button>
            ) : (
              <button
                onClick={() => {
                  if (confirm('Finalize editions? This will hide all unselected candidates.')) {
                    finalizeEditions.mutate()
                  }
                }}
                disabled={finalizeEditions.isPending || selectedCount === 0}
                className="btn-sm btn-finalize"
                title="Hide unselected editions and show only final selection"
              >
                ✓ Finalize Editions
              </button>
            )}
            {excludedCount > 0 && (
              <button
                onClick={() => setShowExcluded(!showExcluded)}
                className={`btn-sm btn-excluded-toggle ${showExcluded ? 'active' : ''}`}
              >
                {showExcluded ? '👁️ Hide' : '👁️ Show'} Excluded ({excludedCount})
              </button>
            )}
          </div>

          {/* Language chips - click to filter, double-click to select */}
          <div className="lang-chips">
            <span className="chip-label">Languages:</span>
            {Object.entries(languageGroups).map(([lang, count]) => (
              <button
                key={lang}
                className={`lang-chip ${languageFilter === lang ? 'active' : ''}`}
                onClick={() => setLanguageFilter(languageFilter === lang ? null : lang)}
                onDoubleClick={() => selectByLanguage(lang)}
                title="Click to filter, double-click to select all"
              >
                {lang} <span className="count">{count}</span>
              </button>
            ))}
            {languageFilter && (
              <>
                <button className="lang-chip clear" onClick={() => setLanguageFilter(null)}>
                  × Clear filter
                </button>
                <button
                  className="lang-chip fetch-more"
                  onClick={() => fetchMoreInLanguage.mutate(languageFilter)}
                  disabled={fetchMoreInLanguage.isPending}
                  title={`Search for more ${languageFilter} editions`}
                >
                  {fetchMoreInLanguage.isPending && fetchMoreProgress?.language === languageFilter
                    ? '⏳ Searching...'
                    : `+ Fetch more ${languageFilter}`}
                </button>
              </>
            )}
          </div>
          {/* Fetch more progress */}
          {fetchMoreProgress && (
            <div className={`fetch-progress ${fetchMoreProgress.done ? 'done' : ''} ${fetchMoreProgress.error ? 'error' : ''}`}>
              {fetchMoreProgress.progress !== undefined && !fetchMoreProgress.done && !fetchMoreProgress.error && (
                <div className="fetch-progress-bar" style={{ width: `${fetchMoreProgress.progress}%` }} />
              )}
              <span className="fetch-progress-message">{fetchMoreProgress.message}</span>
            </div>
          )}
        </div>
      )}

      {/* Editions Table */}
      {isLoading ? (
        <div className="loading">Loading editions...</div>
      ) : editions?.length === 0 ? (
        <div className="empty">No editions yet. Click "Discover Editions" to search.</div>
      ) : (
        <div className="ed-table">
          {/* High Confidence */}
          {highConfidence.length > 0 && (
            <EditionGroup
              title="High Confidence"
              editions={highConfidence}
              expanded={expandedGroups.high}
              onToggle={() => toggleGroup('high')}
              onSelect={(id, selected) => selectEditions.mutate({ ids: [id], selected })}
              onSelectAll={() => selectByConfidence('high')}
              onDeselectAll={() => deselectByConfidence('high')}
              onMarkIrrelevant={(ids) => markAsIrrelevant(ids)}
              onMarkUncertain={(ids) => markAsUncertain(ids)}
              onHarvest={harvestEdition}
              harvestingEditions={harvestingEditions}
              className="group-high"
              showMarkAs="uncertain"
              onViewCitations={viewCitationsForEdition}
              onExclude={(ids) => excludeEditions.mutate({ ids, excluded: true })}
              onAddAsSeed={(id) => addAsSeed.mutate(id)}
              onSelectAndHarvest={selectAndHarvest}
            />
          )}

          {/* Uncertain */}
          {uncertain.length > 0 && (
            <EditionGroup
              title="Uncertain"
              editions={uncertain}
              expanded={expandedGroups.uncertain}
              onToggle={() => toggleGroup('uncertain')}
              onSelect={(id, selected) => selectEditions.mutate({ ids: [id], selected })}
              onSelectAll={() => selectByConfidence('uncertain')}
              onDeselectAll={() => deselectByConfidence('uncertain')}
              onMarkIrrelevant={(ids) => markAsIrrelevant(ids)}
              onMarkHigh={(ids) => markAsHigh(ids)}
              onHarvest={harvestEdition}
              harvestingEditions={harvestingEditions}
              className="group-uncertain"
              showMarkAs="both"
              onViewCitations={viewCitationsForEdition}
              onExclude={(ids) => excludeEditions.mutate({ ids, excluded: true })}
              onAddAsSeed={(id) => addAsSeed.mutate(id)}
              onSelectAndHarvest={selectAndHarvest}
            />
          )}

          {/* Rejected */}
          {rejected.length > 0 && (
            <EditionGroup
              title="Rejected"
              editions={rejected}
              expanded={expandedGroups.rejected}
              onToggle={() => toggleGroup('rejected')}
              onSelect={(id, selected) => selectEditions.mutate({ ids: [id], selected })}
              onSelectAll={() => {}}
              onDeselectAll={() => {}}
              onMarkUncertain={(ids) => markAsUncertain(ids)}
              onMarkHigh={(ids) => markAsHigh(ids)}
              onHarvest={harvestEdition}
              harvestingEditions={harvestingEditions}
              className="group-rejected"
              showMarkAs="restore"
              onViewCitations={viewCitationsForEdition}
              onExclude={(ids) => excludeEditions.mutate({ ids, excluded: true })}
              onAddAsSeed={(id) => addAsSeed.mutate(id)}
              onSelectAndHarvest={selectAndHarvest}
            />
          )}

          {/* Excluded - only show when toggled */}
          {showExcluded && excluded.length > 0 && (
            <EditionGroup
              title="Excluded"
              editions={excluded}
              expanded={expandedGroups.excluded}
              onToggle={() => toggleGroup('excluded')}
              onSelect={(id, selected) => selectEditions.mutate({ ids: [id], selected })}
              onSelectAll={() => {}}
              onDeselectAll={() => {}}
              onMarkUncertain={(ids) => markAsUncertain(ids)}
              onMarkHigh={(ids) => markAsHigh(ids)}
              onHarvest={harvestEdition}
              harvestingEditions={harvestingEditions}
              className="group-excluded"
              showMarkAs="restore-from-excluded"
              onViewCitations={viewCitationsForEdition}
              onInclude={(ids) => excludeEditions.mutate({ ids, excluded: false })}
              isExcludedGroup={true}
            />
          )}
        </div>
      )}

      {/* Language Modal */}
      {showLanguageModal && (
        <div className="modal-overlay" onClick={() => setShowLanguageModal(false)}>
          <div className="modal compact" onClick={e => e.stopPropagation()}>
            <h3>Search Languages</h3>

            {isLoadingRecs ? (
              <div className="loading-rec">Getting AI recommendations...</div>
            ) : recommendations && (
              <div className="ai-rec">
                <strong>AI suggests:</strong> {recommendations.recommended?.join(', ')}
                <p className="rec-reason">{recommendations.reasoning}</p>
              </div>
            )}

            <div className="strategy-options">
              {[
                { value: 'recommended', label: 'AI Recommended', desc: 'Based on author/title' },
                { value: 'major_languages', label: 'Major Languages', desc: 'EN, DE, FR, ES, PT, IT, RU, ZH, JA' },
                { value: 'english_only', label: 'English Only', desc: 'Fast, limited coverage' },
                { value: 'custom', label: 'Custom', desc: 'Choose below' },
              ].map(opt => (
                <label key={opt.value} className={languageStrategy === opt.value ? 'selected' : ''}>
                  <input
                    type="radio"
                    value={opt.value}
                    checked={languageStrategy === opt.value}
                    onChange={e => setLanguageStrategy(e.target.value)}
                  />
                  <span className="opt-label">{opt.label}</span>
                  <span className="opt-desc">{opt.desc}</span>
                </label>
              ))}
            </div>

            {languageStrategy === 'custom' && (
              <div className="custom-langs">
                {languages?.languages?.map(lang => (
                  <label key={lang.code} className={customLanguages.includes(lang.code) ? 'selected' : ''}>
                    <input
                      type="checkbox"
                      checked={customLanguages.includes(lang.code)}
                      onChange={() => toggleLanguage(lang.code)}
                    />
                    {lang.icon} {lang.name}
                  </label>
                ))}
              </div>
            )}

            <div className="modal-footer">
              <button onClick={() => setShowLanguageModal(false)}>Cancel</button>
              <button
                onClick={() => discoverEditions.mutate()}
                disabled={languageStrategy === 'custom' && customLanguages.length === 0}
                className="btn-primary"
              >
                Start Discovery
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Manual Edition Modal */}
      {showManualEditionModal && (
        <div className="modal-overlay" onClick={() => setShowManualEditionModal(false)}>
          <div className="modal compact" onClick={e => e.stopPropagation()}>
            <h3>➕ Add Edition Manually</h3>

            <p className="modal-hint">
              Enter a translated title, Google Scholar URL, or paste text from Scholar.
              AI will find the matching edition.
            </p>

            <div className="form-group">
              <label>Title / URL / Pasted Text</label>
              <textarea
                value={manualEditionInput}
                onChange={e => setManualEditionInput(e.target.value)}
                placeholder={`Examples:\n• Smarte Neue Welt (German title)\n• https://scholar.google.com/scholar?cluster=...\n• Paste citation text from Scholar`}
                rows={4}
                autoFocus
              />
            </div>

            <div className="form-group">
              <label>Language (optional hint)</label>
              <select
                value={manualEditionLanguage}
                onChange={e => setManualEditionLanguage(e.target.value)}
              >
                <option value="">Auto-detect</option>
                {languages?.languages?.map(lang => (
                  <option key={lang.code} value={lang.code}>
                    {lang.icon} {lang.name}
                  </option>
                ))}
              </select>
            </div>

            {manualEditionResult && (
              <div className={`manual-result ${manualEditionResult.success ? 'success' : 'error'}`}>
                {manualEditionResult.success ? '✓' : '✗'} {manualEditionResult.message}
                {manualEditionResult.edition && (
                  <div className="result-edition">
                    <strong>{manualEditionResult.edition.title}</strong>
                    <br />
                    <small>
                      {manualEditionResult.edition.citation_count?.toLocaleString() || 0} citations
                      {manualEditionResult.edition.language && ` • ${manualEditionResult.edition.language}`}
                    </small>
                  </div>
                )}
              </div>
            )}

            <div className="modal-footer">
              <button onClick={() => setShowManualEditionModal(false)}>Cancel</button>
              <button
                onClick={() => addManualEdition.mutate()}
                disabled={!manualEditionInput.trim() || addManualEdition.isPending}
                className="btn-primary"
              >
                {addManualEdition.isPending ? 'Searching...' : 'Find & Add Edition'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

/**
 * Edition Group - collapsible section with table rows
 */
function EditionGroup({
  title,
  editions,
  expanded,
  onToggle,
  onSelect,
  onSelectAll,
  onDeselectAll,
  onMarkIrrelevant,
  onMarkUncertain,
  onMarkHigh,
  onHarvest,
  harvestingEditions,
  className,
  showMarkAs,
  onViewCitations,
  onExclude,
  onAddAsSeed,
  onInclude,
  onSelectAndHarvest,
  isExcludedGroup = false
}) {
  const selectedCount = editions.filter(e => e.selected).length
  const totalCitations = editions.reduce((sum, e) => sum + (e.citation_count || 0), 0)

  // Get IDs of selected editions for batch actions
  const selectedIds = editions.filter(e => e.selected).map(e => e.id)

  return (
    <div className={`ed-group ${className}`}>
      <div className="group-header" onClick={onToggle}>
        <span className="toggle">{expanded ? '▼' : '▶'}</span>
        <span className="group-title">{title}</span>
        <span className="group-stats">
          {selectedCount}/{editions.length} selected · {totalCitations.toLocaleString()} citations
        </span>
        <div className="group-actions" onClick={e => e.stopPropagation()}>
          {title !== 'Rejected' && (
            <>
              <button className="btn-xs" onClick={onSelectAll} title="Select all for citation extraction">
                Select all
              </button>
              <button className="btn-xs" onClick={onDeselectAll} title="Deselect all">
                Deselect
              </button>
            </>
          )}
        </div>
      </div>

      {expanded && (
        <>
          {/* Batch action bar when items are selected */}
          {selectedCount > 0 && (
            <div className="batch-bar">
              <span>{selectedCount} selected</span>
              {showMarkAs === 'uncertain' && (
                <button className="btn-xs btn-danger" onClick={() => onMarkIrrelevant(selectedIds)}>
                  Mark Irrelevant
                </button>
              )}
              {showMarkAs === 'both' && (
                <>
                  <button className="btn-xs btn-success" onClick={() => onMarkHigh(selectedIds)}>
                    → High
                  </button>
                  <button className="btn-xs btn-danger" onClick={() => onMarkIrrelevant(selectedIds)}>
                    Mark Irrelevant
                  </button>
                </>
              )}
              {showMarkAs === 'restore' && (
                <>
                  <button className="btn-xs btn-success" onClick={() => onMarkHigh(selectedIds)}>
                    → High
                  </button>
                  <button className="btn-xs" onClick={() => onMarkUncertain(selectedIds)}>
                    → Uncertain
                  </button>
                </>
              )}
              {showMarkAs === 'restore-from-excluded' && onInclude && (
                <button className="btn-xs btn-success" onClick={() => onInclude(selectedIds)}>
                  ↩ Restore Selected
                </button>
              )}
              {/* Batch exclude - available for non-excluded groups */}
              {!isExcludedGroup && onExclude && (
                <button className="btn-xs btn-exclude-batch" onClick={() => onExclude(selectedIds)}>
                  ⊘ Exclude Selected
                </button>
              )}
              {/* Batch add as seed - available for non-excluded groups */}
              {!isExcludedGroup && onAddAsSeed && (
                <button
                  className="btn-xs btn-seed-batch"
                  onClick={() => selectedIds.forEach(id => onAddAsSeed(id))}
                >
                  🌱 Add {selectedCount} as Seeds
                </button>
              )}
              {/* Select & Harvest - mark as editions and start harvesting */}
              {!isExcludedGroup && onSelectAndHarvest && (
                <button
                  className="btn-xs btn-harvest-batch"
                  onClick={() => onSelectAndHarvest(selectedIds)}
                >
                  📥 Select & Harvest ({selectedCount})
                </button>
              )}
            </div>
          )}

          <table className="edition-table">
            <thead>
              <tr>
                <th className="col-check">
                  <input
                    type="checkbox"
                    checked={selectedCount === editions.length && editions.length > 0}
                    onChange={(e) => e.target.checked ? onSelectAll() : onDeselectAll()}
                    title="Select all"
                  />
                </th>
                <th className="col-title">Title / Authors</th>
                <th className="col-year">Year</th>
                <th className="col-lang">Lang</th>
                <th className="col-cites">Citations</th>
                <th className="col-harvested">Harvested</th>
                <th className="col-staleness">Status</th>
                <th className="col-actions"></th>
              </tr>
            </thead>
            <tbody>
              {editions.map(ed => (
                <EditionRow
                  key={ed.id}
                  edition={ed}
                  onSelect={onSelect}
                  onMarkIrrelevant={onMarkIrrelevant}
                  onMarkHigh={onMarkHigh}
                  onMarkUncertain={onMarkUncertain}
                  onHarvest={onHarvest}
                  isHarvesting={!!harvestingEditions[ed.id]}
                  showMarkAs={showMarkAs}
                  onViewCitations={onViewCitations}
                  onExclude={onExclude}
                  onAddAsSeed={onAddAsSeed}
                  onInclude={onInclude}
                  isExcludedGroup={isExcludedGroup}
                />
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  )
}

/**
 * Edition Row - single compact row
 */
function EditionRow({
  edition,
  onSelect,
  onMarkIrrelevant,
  onMarkHigh,
  onMarkUncertain,
  onHarvest,
  isHarvesting,
  showMarkAs,
  onViewCitations,
  onExclude,
  onAddAsSeed,
  onInclude,
  isExcludedGroup = false
}) {
  const maxCites = 5000 // for bar scaling
  const barWidth = Math.min(100, (edition.citation_count / maxCites) * 100)
  const hasCitations = edition.citation_count > 0
  const hasHarvested = edition.harvested_citations > 0

  return (
    <tr className={`${edition.selected ? 'selected' : ''} ${isHarvesting ? 'harvesting' : ''} ${hasHarvested ? 'has-harvested' : ''}`}>
      <td className="col-check">
        <input
          type="checkbox"
          checked={edition.selected}
          onChange={(e) => onSelect(edition.id, e.target.checked)}
        />
      </td>
      <td className="col-title">
        <div className="title-cell">
          {edition.added_by_job_id && <span className="badge-new">NEW</span>}
          {edition.link ? (
            <a href={edition.link} target="_blank" rel="noopener noreferrer" title={edition.title}>
              {edition.title.length > 80 ? edition.title.substring(0, 77) + '...' : edition.title}
            </a>
          ) : (
            <span title={edition.title}>
              {edition.title.length > 80 ? edition.title.substring(0, 77) + '...' : edition.title}
            </span>
          )}
          <span className="authors-line">{edition.authors || 'Unknown'}</span>
        </div>
      </td>
      <td className="col-year">{edition.year || '–'}</td>
      <td className="col-lang">
        <span className="lang-tag">{edition.language?.substring(0, 3) || '?'}</span>
      </td>
      <td className="col-cites">
        <div className="cite-cell">
          <span className="cite-num">{edition.citation_count?.toLocaleString() || 0}</span>
          <div className="cite-bar" style={{ width: `${barWidth}%` }} />
        </div>
      </td>
      <td className="col-harvested">
        {hasHarvested ? (
          <button
            className="btn-harvested"
            onClick={() => onViewCitations(edition.id)}
            title={`View ${edition.harvested_citations} harvested citations`}
          >
            {edition.harvested_citations}
          </button>
        ) : (
          <span className="not-harvested">–</span>
        )}
      </td>
      <td className="col-staleness">
        {/* Incomplete harvest indicator (takes priority) */}
        {edition.is_incomplete ? (
          <span
            className="staleness-badge incomplete"
            title={`Incomplete harvest: ${edition.missing_citations?.toLocaleString() || '?'} citations remaining. Will auto-resume.`}
          >
            ⚠️ {edition.missing_citations?.toLocaleString()}
          </span>
        ) : edition.is_stale ? (
          <span className="staleness-badge stale" title={`Last harvested ${edition.days_since_harvest} days ago`}>
            ⏰ {edition.days_since_harvest}d
          </span>
        ) : edition.last_harvested_at ? (
          <span className="staleness-badge fresh" title={`Last harvested ${edition.days_since_harvest || 0} days ago`}>
            ✓
          </span>
        ) : hasHarvested ? (
          <span className="staleness-badge never" title="Never tracked - legacy harvest">
            ?
          </span>
        ) : null}
      </td>
      <td className="col-actions">
        {/* Harvest button - only show if there are citations to harvest */}
        {hasCitations && !isExcludedGroup && (
          <button
            className={`btn-harvest ${isHarvesting ? 'harvesting' : ''}`}
            onClick={() => onHarvest(edition.id)}
            disabled={isHarvesting}
            title={isHarvesting ? 'Harvesting...' : `Harvest ${edition.citation_count.toLocaleString()} citations`}
          >
            {isHarvesting ? '⏳' : '📥'}
          </button>
        )}

        {/* Add as Seed button - for all groups except excluded */}
        {onAddAsSeed && !isExcludedGroup && (
          <button
            className="btn-icon btn-seed"
            onClick={() => onAddAsSeed(edition.id)}
            title="Add as new seed paper"
          >
            🌱
          </button>
        )}

        {/* Exclude button - for all groups except excluded */}
        {onExclude && !isExcludedGroup && (
          <button
            className="btn-icon btn-exclude"
            onClick={() => onExclude([edition.id])}
            title="Exclude from view"
          >
            ⊘
          </button>
        )}

        {/* Include button - only for excluded group */}
        {isExcludedGroup && onInclude && (
          <button
            className="btn-icon btn-include"
            onClick={() => onInclude([edition.id])}
            title="Restore to candidates"
          >
            ↩
          </button>
        )}

        {showMarkAs === 'uncertain' && (
          <button
            className="btn-icon"
            onClick={() => onMarkIrrelevant([edition.id])}
            title="Mark as irrelevant"
          >
            ✕
          </button>
        )}
        {showMarkAs === 'both' && (
          <>
            <button
              className="btn-icon btn-up"
              onClick={() => onMarkHigh([edition.id])}
              title="Move to High Confidence"
            >
              ↑
            </button>
            <button
              className="btn-icon btn-down"
              onClick={() => onMarkIrrelevant([edition.id])}
              title="Mark as irrelevant"
            >
              ✕
            </button>
          </>
        )}
        {showMarkAs === 'restore' && (
          <button
            className="btn-icon btn-restore"
            onClick={() => onMarkUncertain([edition.id])}
            title="Restore to Uncertain"
          >
            ↩
          </button>
        )}
      </td>
    </tr>
  )
}
