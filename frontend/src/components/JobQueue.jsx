import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

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

  const getJobParams = (job) => {
    if (!job.params) return null
    try {
      const params = typeof job.params === 'string' ? JSON.parse(job.params) : job.params
      if (job.job_type === 'fetch_more_editions' && params.language) {
        return `(${params.language})`
      }
      return null
    } catch {
      return null
    }
  }

  const formatTime = (dateString) => {
    if (!dateString) return '-'
    return new Date(dateString).toLocaleString()
  }

  if (isLoading) return <div className="loading">Loading jobs...</div>

  return (
    <div className="job-queue">
      <h2>Job Queue</h2>

      {!jobs?.length ? (
        <div className="empty">No jobs in queue</div>
      ) : (
        <div className="jobs-table">
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>Type</th>
                <th>Progress</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map(job => (
                <tr key={job.id} className={`job-row status-${job.status}`}>
                  <td>
                    <span className="status-icon">{getStatusIcon(job.status)}</span>
                    {job.status}
                  </td>
                  <td>
                    {getJobTypeLabel(job.job_type)}
                    {getJobParams(job) && <span className="job-params"> {getJobParams(job)}</span>}
                  </td>
                  <td>
                    <div className="progress-bar">
                      <div
                        className="progress-fill"
                        style={{ width: `${Math.min(job.progress, 100)}%` }}
                      />
                    </div>
                    <span className="progress-text">{Math.round(job.progress)}%</span>
                    {job.progress_message && (
                      <span className="progress-message">{job.progress_message}</span>
                    )}
                  </td>
                  <td>{formatTime(job.created_at)}</td>
                  <td>
                    {(job.status === 'pending' || job.status === 'running') && (
                      <button
                        onClick={() => cancelJob.mutate(job.id)}
                        className="btn-cancel"
                        disabled={cancelJob.isPending}
                      >
                        Cancel
                      </button>
                    )}
                    {job.error && (
                      <span className="job-error" title={job.error}>⚠️ Error</span>
                    )}
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
