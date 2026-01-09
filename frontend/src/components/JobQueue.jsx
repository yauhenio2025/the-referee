import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import HarvestDashboard from './HarvestDashboard'

/**
 * Advanced Job Queue Monitor
 * Shows detailed progress for each job including edition info, harvest stats, current year, etc.
 */
export default function JobQueue() {
  const queryClient = useQueryClient()
  const [expandedJobs, setExpandedJobs] = useState(new Set())

  const { data: jobs, isLoading } = useQuery({
    queryKey: ['jobs'],
    queryFn: () => api.listJobs(),
    // Poll faster when there are running jobs
    refetchInterval: (query) => {
      const data = query.state.data
      const hasRunning = data?.some(j => j.status === 'running')
      return hasRunning ? 2000 : 5000
    },
  })

  // Fetch papers to get titles for jobs
  // Use distinct queryKey to avoid conflict with PaperList's complex queryFn
  const { data: papersData, isLoading: papersLoading } = useQuery({
    queryKey: ['papers-for-jobs'],
    queryFn: () => api.listPapers(),
    staleTime: 60000, // Cache for 60s - paper titles rarely change
  })

  // Handle both paginated response (object with papers array) and legacy array response
  const papers = Array.isArray(papersData) ? papersData : (papersData?.papers || [])

  // Create paper lookup map
  const paperLookup = papers.reduce((acc, p) => {
    acc[p.id] = p
    return acc
  }, {})

  const getPaperTitle = (paperId) => {
    if (!paperId) return null
    // Show loading state while papers are being fetched
    if (papersLoading) return 'Loading...'
    const paper = paperLookup[paperId]
    if (!paper) return `Paper #${paperId}`
    // Truncate long titles
    const title = paper.title
    return title.length > 60 ? title.substring(0, 57) + '...' : title
  }

  const cancelJob = useMutation({
    mutationFn: (jobId) => api.cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries(['jobs'])
    },
  })

  const pauseHarvest = useMutation({
    mutationFn: (paperId) => api.pauseHarvest(paperId),
    onSuccess: (data) => {
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['papers-for-jobs'])
      alert(`Paused harvest for: ${data.title}\n\nAuto-resume will skip this paper. Use Papers tab to unpause.`)
    },
  })

  const cancelAndPause = useMutation({
    mutationFn: async ({ jobId, paperId }) => {
      // Cancel the job first
      await api.cancelJob(jobId)
      // Then pause the paper to prevent auto-resume
      return api.pauseHarvest(paperId)
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries(['jobs'])
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['papers-for-jobs'])
      alert(`Stopped and paused: ${data.title}\n\nAuto-resume disabled. Use Papers tab to unpause later.`)
    },
  })

  const getStatusIcon = (status) => {
    const icons = {
      pending: '⏳',
      running: '🔄',
      completed: '✅',
      failed: '❌',
      cancelled: '🚫',
    }
    return icons[status] || '❓'
  }

  const getJobTypeLabel = (type) => {
    const labels = {
      resolve: 'Paper Resolution',
      discover_editions: 'Edition Discovery',
      extract_citations: 'Citation Extraction',
      fetch_more_editions: 'Fetch More Editions',
    }
    return labels[type] || type
  }

  const getJobTypeIcon = (type) => {
    const icons = {
      resolve: '🔍',
      discover_editions: '📖',
      extract_citations: '📥',
      fetch_more_editions: '🌐',
    }
    return icons[type] || '⚙️'
  }

  const parseParams = (job) => {
    if (!job.params) return {}
    try {
      return typeof job.params === 'string' ? JSON.parse(job.params) : job.params
    } catch {
      return {}
    }
  }

  const formatTime = (dateString) => {
    if (!dateString) return '-'
    // Server returns UTC timestamps without 'Z' suffix - add it for correct parsing
    const utcString = dateString.endsWith('Z') ? dateString : dateString + 'Z'
    const date = new Date(utcString)
    const now = new Date()
    const diffMs = now - date
    const diffMins = Math.floor(diffMs / 60000)
    const diffHours = Math.floor(diffMs / 3600000)

    if (diffMins < 1) return 'just now'
    if (diffMins < 60) return `${diffMins}m ago`
    if (diffHours < 24) return `${diffHours}h ago`
    return date.toLocaleDateString()
  }

  const formatDuration = (startDate, endDate) => {
    if (!startDate) return '-'
    // Server returns UTC timestamps without 'Z' suffix - add it for correct parsing
    const startUtc = startDate.endsWith('Z') ? startDate : startDate + 'Z'
    const start = new Date(startUtc)
    const end = endDate ? new Date(endDate.endsWith('Z') ? endDate : endDate + 'Z') : new Date()
    const diffMs = end - start
    const diffSecs = Math.floor(diffMs / 1000)
    const diffMins = Math.floor(diffMs / 60000)

    if (diffSecs < 60) return `${diffSecs}s`
    if (diffMins < 60) return `${diffMins}m ${diffSecs % 60}s`
    return `${Math.floor(diffMins / 60)}h ${diffMins % 60}m`
  }

  const toggleJobDetails = (jobId) => {
    setExpandedJobs(prev => {
      const next = new Set(prev)
      if (next.has(jobId)) {
        next.delete(jobId)
      } else {
        next.add(jobId)
      }
      return next
    })
  }

  if (isLoading) return <div className="loading">Loading jobs...</div>

  // Separate running/pending from completed
  const activeJobs = jobs?.filter(j => j.status === 'running' || j.status === 'pending') || []
  const recentJobs = jobs?.filter(j => j.status !== 'running' && j.status !== 'pending').slice(0, 5) || []

  return (
    <div className="job-queue-advanced">
      <HarvestDashboard />

      <h2>Job Queue</h2>

      {activeJobs.length === 0 && recentJobs.length === 0 ? (
        <div className="empty">No jobs in queue</div>
      ) : (
        <>
          {/* Active Jobs - Detailed View */}
          {activeJobs.length > 0 && (
            <div className="active-jobs">
              <h3>Active Jobs ({activeJobs.length})</h3>
              {activeJobs.map(job => {
                const params = parseParams(job)
                const details = params.progress_details || {}
                const hasDetails = details.stage && ['initializing', 'year_by_year_init', 'harvesting'].includes(details.stage)

                return (
                  <div key={job.id} className={`job-card status-${job.status}`}>
                    <div className="job-card-header">
                      <div className="job-type">
                        <span className="job-type-icon">{getJobTypeIcon(job.job_type)}</span>
                        <span className="job-type-label">{getJobTypeLabel(job.job_type)}</span>
                        {params.language && <span className="job-language">({params.language})</span>}
                      </div>
                      <div className="job-status">
                        <span className="status-icon">{getStatusIcon(job.status)}</span>
                        <span className="status-label">{job.status}</span>
                      </div>
                    </div>

                    {/* Paper Title */}
                    {job.paper_id && (
                      <div className="job-paper-title">
                        📄 <Link to={`/papers/${job.paper_id}/citations`} className="paper-link">
                          {getPaperTitle(job.paper_id)}
                        </Link>
                      </div>
                    )}

                    {/* Progress Bar */}
                    <div className="job-progress-section">
                      <div className="progress-bar-large">
                        <div
                          className="progress-fill"
                          style={{ width: `${Math.min(job.progress, 100)}%` }}
                        />
                        <span className="progress-text">{Math.round(job.progress)}%</span>
                      </div>
                    </div>

                    {/* Detailed Progress Info for Citation Extraction Jobs */}
                    {hasDetails && (
                      <div className="harvest-details">
                        {/* Stage Badge */}
                        <div className="stage-badge-row">
                          {details.stage === 'initializing' && (
                            <span className="stage-badge stage-init">⚙️ Initializing</span>
                          )}
                          {details.stage === 'year_by_year_init' && (
                            <span className="stage-badge stage-yby-init">🗓️ Year-by-Year Setup</span>
                          )}
                          {details.stage === 'harvesting' && details.harvest_mode === 'year_by_year' && (
                            <span className="stage-badge stage-yby">🗓️ Year-by-Year Harvest</span>
                          )}
                          {details.stage === 'harvesting' && details.harvest_mode === 'standard' && (
                            <span className="stage-badge stage-std">📥 Standard Harvest</span>
                          )}
                          {details.year_harvest_strategy === 'partition' && (
                            <span className="strategy-badge">⚡ Partition Mode</span>
                          )}
                        </div>

                        {/* Edition Info */}
                        {details.edition_index && (
                          <div className="detail-row edition-info">
                            <span className="detail-label">Edition:</span>
                            <span className="detail-value">
                              {details.edition_index}/{details.editions_total} - {details.edition_title}
                              {details.edition_language && <span className="lang-badge">{details.edition_language}</span>}
                            </span>
                          </div>
                        )}

                        {/* Year Progress (for year-by-year mode) */}
                        {details.harvest_mode === 'year_by_year' && details.current_year && (
                          <div className="year-progress-row">
                            <div className="year-current">
                              <span className="year-label">Processing:</span>
                              <span className="year-value">{details.current_year}</span>
                              <span className="year-range">
                                ({details.year_range_start} → {details.year_range_end})
                              </span>
                            </div>
                            <div className="year-stats">
                              <span className="year-completed">{details.years_completed || 0} done</span>
                              <span className="year-sep">/</span>
                              <span className="year-total">{details.years_total || '?'} years</span>
                              {details.years_remaining > 0 && (
                                <span className="year-remaining">({details.years_remaining} left)</span>
                              )}
                            </div>
                          </div>
                        )}

                        {/* Year Expected Citations (for current year) */}
                        {details.year_expected_citations > 0 && (
                          <div className="year-target-row">
                            <span className="target-label">Year {details.current_year} target:</span>
                            <span className="target-value">{details.year_expected_citations?.toLocaleString()}</span>
                            {details.year_harvest_strategy === 'partition' && (
                              <span className="partition-note">(using partition strategy)</span>
                            )}
                          </div>
                        )}

                        {/* Main Stats Grid */}
                        <div className="detail-stats">
                          <div className="stat-box saved">
                            <span className="stat-value">{details.citations_saved?.toLocaleString() || 0}</span>
                            <span className="stat-label">New Saved</span>
                          </div>
                          <div className="stat-box target">
                            <span className="stat-value">{details.target_citations_total?.toLocaleString() || details.edition_citation_count?.toLocaleString() || '?'}</span>
                            <span className="stat-label">Target Total</span>
                          </div>
                          <div className="stat-box previous">
                            <span className="stat-value">{details.previously_harvested?.toLocaleString() || 0}</span>
                            <span className="stat-label">Already Had</span>
                          </div>
                          {details.current_page && (
                            <div className="stat-box page">
                              <span className="stat-value">Page {details.current_page}</span>
                              <span className="stat-label">Current</span>
                            </div>
                          )}
                        </div>

                        {/* Editions Info (for initializing stage) */}
                        {details.editions_info && details.editions_info.length > 0 && (
                          <div className="editions-summary">
                            <div className="editions-header">
                              Editions to process: {details.editions_total}
                              {details.skipped_editions > 0 && ` (${details.skipped_editions} skipped)`}
                            </div>
                            <div className="editions-list">
                              {details.editions_info.slice(0, 3).map((ed, idx) => (
                                <div key={ed.id} className="edition-preview">
                                  <span className="ed-num">{idx + 1}.</span>
                                  <span className="ed-lang">[{ed.language}]</span>
                                  <span className="ed-count">{ed.citation_count?.toLocaleString() || '?'} cit.</span>
                                  {ed.harvested > 0 && <span className="ed-had">(had {ed.harvested})</span>}
                                </div>
                              ))}
                              {details.editions_info.length > 3 && (
                                <div className="editions-more">+{details.editions_info.length - 3} more</div>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Simple Progress Message for jobs without details */}
                    {!hasDetails && job.progress_message && (
                      <div className="job-message">{job.progress_message}</div>
                    )}

                    {/* Actions and Timing */}
                    <div className="job-card-footer">
                      <div className="job-timing">
                        <span className="timing-item">Started: {formatTime(job.started_at)}</span>
                        <span className="timing-item">Duration: {formatDuration(job.started_at)}</span>
                      </div>
                      <div className="job-actions">
                        {(job.status === 'pending' || job.status === 'running') && job.paper_id && (
                          <button
                            onClick={() => cancelAndPause.mutate({ jobId: job.id, paperId: job.paper_id })}
                            className="btn-pause"
                            disabled={cancelAndPause.isPending}
                            title="Cancel this job AND prevent auto-resume from restarting it"
                          >
                            ⏸️ Stop & Pause
                          </button>
                        )}
                        {(job.status === 'pending' || job.status === 'running') && (
                          <button
                            onClick={() => cancelJob.mutate(job.id)}
                            className="btn-cancel"
                            disabled={cancelJob.isPending}
                            title="Cancel this job (auto-resume may restart it)"
                          >
                            Cancel
                          </button>
                        )}
                        <button
                          onClick={() => toggleJobDetails(job.id)}
                          className="btn-toggle-details"
                          title="Show/hide raw job details"
                        >
                          {expandedJobs.has(job.id) ? '▼ Hide' : '▶ Raw'}
                        </button>
                      </div>
                    </div>

                    {/* Expandable Raw Details */}
                    {expandedJobs.has(job.id) && (
                      <div className="raw-details">
                        <div className="raw-details-header">Raw Job Data (for debugging)</div>
                        <pre className="raw-details-json">
                          {JSON.stringify({
                            job_id: job.id,
                            status: job.status,
                            progress: job.progress,
                            progress_message: job.progress_message,
                            params: params,
                            started_at: job.started_at,
                            completed_at: job.completed_at,
                            error: job.error,
                          }, null, 2)}
                        </pre>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )}

          {/* Recent Jobs - Compact Table */}
          {recentJobs.length > 0 && (
            <div className="recent-jobs">
              <h3>Recent Jobs</h3>
              <table className="jobs-table">
                <thead>
                  <tr>
                    <th>Status</th>
                    <th>Type</th>
                    <th>Paper</th>
                    <th>Result</th>
                    <th>Duration</th>
                  </tr>
                </thead>
                <tbody>
                  {recentJobs.map(job => {
                    const params = parseParams(job)
                    const details = params.progress_details || {}

                    return (
                      <tr key={job.id} className={`job-row status-${job.status}`}>
                        <td>
                          <span className="status-icon">{getStatusIcon(job.status)}</span>
                        </td>
                        <td>
                          <span className="job-type-compact">
                            {getJobTypeIcon(job.job_type)} {getJobTypeLabel(job.job_type)}
                            {params.language && ` (${params.language})`}
                          </span>
                        </td>
                        <td className="job-paper-cell">
                          {job.paper_id ? (
                            <Link to={`/papers/${job.paper_id}/citations`} className="paper-link">
                              {getPaperTitle(job.paper_id)}
                            </Link>
                          ) : '-'}
                        </td>
                        <td>
                          {job.status === 'completed' && details.citations_saved ? (
                            <span className="result-success">
                              {details.citations_saved.toLocaleString()} citations
                            </span>
                          ) : job.status === 'failed' ? (
                            <span className="result-error" title={job.error}>
                              {job.error?.substring(0, 30)}...
                            </span>
                          ) : (
                            <span className="result-neutral">
                              {job.progress_message || '-'}
                            </span>
                          )}
                        </td>
                        <td>{formatDuration(job.started_at, job.completed_at)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  )
}
