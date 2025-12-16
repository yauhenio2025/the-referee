import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function JobQueue() {
  const queryClient = useQueryClient()

  const { data: jobs, isLoading } = useQuery({
    queryKey: ['jobs'],
    queryFn: () => api.listJobs(),
    refetchInterval: 5000,
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
    }
    return labels[type] || type
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
                  <td>{getJobTypeLabel(job.job_type)}</td>
                  <td>
                    <div className="progress-bar">
                      <div
                        className="progress-fill"
                        style={{ width: `${job.progress * 100}%` }}
                      />
                    </div>
                    <span className="progress-text">{Math.round(job.progress * 100)}%</span>
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
