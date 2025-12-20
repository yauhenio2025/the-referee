import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

/**
 * Advanced Job Queue Monitor
 * Shows detailed progress for each job including edition info, harvest stats, current year, etc.
 */
export default function JobQueue() {
  const queryClient = useQueryClient()

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

  const cancelJob = useMutation({
    mutationFn: (jobId) => api.cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries(['jobs'])
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

  if (isLoading) return <div className="loading">Loading jobs...</div>

  // Separate running/pending from completed
  const activeJobs = jobs?.filter(j => j.status === 'running' || j.status === 'pending') || []
  const recentJobs = jobs?.filter(j => j.status !== 'running' && j.status !== 'pending').slice(0, 5) || []

  return (
    <div className="job-queue-advanced">
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
                const isHarvesting = details.stage === 'harvesting'

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

                    {/* Detailed Progress Info for Harvest Jobs */}
                    {isHarvesting && (
                      <div className="harvest-details">
                        <div className="detail-row edition-info">
                          <span className="detail-label">Edition:</span>
                          <span className="detail-value">
                            {details.edition_index}/{details.editions_total} - {details.edition_title}
                            {details.edition_language && <span className="lang-badge">{details.edition_language}</span>}
                          </span>
                        </div>

                        <div className="detail-stats">
                          <div className="stat-box">
                            <span className="stat-value">{details.citations_saved?.toLocaleString() || 0}</span>
                            <span className="stat-label">Saved</span>
                          </div>
                          <div className="stat-box">
                            <span className="stat-value">{details.edition_citation_count?.toLocaleString() || '?'}</span>
                            <span className="stat-label">Total (Est.)</span>
                          </div>
                          <div className="stat-box">
                            <span className="stat-value">Page {details.current_page || 1}</span>
                            <span className="stat-label">Current</span>
                          </div>
                          {details.current_year && (
                            <div className="stat-box year-box">
                              <span className="stat-value">{details.current_year}</span>
                              <span className="stat-label">Year</span>
                            </div>
                          )}
                        </div>

                        {details.harvest_mode === 'year_by_year' && (
                          <div className="harvest-mode-badge">
                            🗓️ Year-by-year mode (large edition)
                          </div>
                        )}
                      </div>
                    )}

                    {/* Simple Progress Message for non-harvest jobs */}
                    {!isHarvesting && job.progress_message && (
                      <div className="job-message">{job.progress_message}</div>
                    )}

                    {/* Actions and Timing */}
                    <div className="job-card-footer">
                      <div className="job-timing">
                        <span className="timing-item">Started: {formatTime(job.started_at)}</span>
                        <span className="timing-item">Duration: {formatDuration(job.started_at)}</span>
                      </div>
                      <div className="job-actions">
                        {(job.status === 'pending' || job.status === 'running') && (
                          <button
                            onClick={() => cancelJob.mutate(job.id)}
                            className="btn-cancel"
                            disabled={cancelJob.isPending}
                          >
                            Cancel
                          </button>
                        )}
                      </div>
                    </div>
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
