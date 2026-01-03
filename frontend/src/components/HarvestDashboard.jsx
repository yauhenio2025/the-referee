import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import api from '../lib/api'

// Helper to format time ago
const formatTimeAgo = (dateString) => {
  if (!dateString) return '-'
  const date = new Date(dateString)
  const now = new Date()
  const diffMs = now - date
  const diffMins = Math.floor(diffMs / 60000)
  const diffHours = Math.floor(diffMins / 60)
  const diffDays = Math.floor(diffHours / 24)

  if (diffDays > 0) return `${diffDays}d ago`
  if (diffHours > 0) return `${diffHours}h ago`
  if (diffMins > 0) return `${diffMins}m ago`
  return 'just now'
}

// Helper to format duration
const formatDuration = (seconds) => {
  if (!seconds) return '-'
  const mins = Math.floor(seconds / 60)
  const hrs = Math.floor(mins / 60)
  if (hrs > 0) return `${hrs}h ${mins % 60}m`
  if (mins > 0) return `${mins}m ${seconds % 60}s`
  return `${seconds}s`
}

// Health Card Component
const HealthCard = ({ title, value, subtext, status = 'ok', icon }) => {
  const statusClass = status === 'ok' ? 'health-ok' : status === 'warning' ? 'health-warning' : 'health-danger'
  return (
    <div className={`health-card ${statusClass}`}>
      <div className="health-card-icon">{icon}</div>
      <div className="health-card-content">
        <div className="health-card-value">{value}</div>
        <div className="health-card-title">{title}</div>
        {subtext && <div className="health-card-subtext">{subtext}</div>}
      </div>
    </div>
  )
}

// Alerts Section Component
const AlertsSection = ({ alerts }) => {
  if (!alerts || alerts.length === 0) return null

  const getAlertIcon = (type) => {
    switch (type) {
      case 'high_duplicate_rate': return '🔄'
      case 'stalled_paper': return '🛑'
      case 'long_running_job': return '⏱️'
      case 'repeated_failures': return '❌'
      default: return '⚠️'
    }
  }

  const getAlertClass = (type) => {
    switch (type) {
      case 'high_duplicate_rate': return 'alert-warning'
      case 'stalled_paper': return 'alert-danger'
      case 'long_running_job': return 'alert-warning'
      case 'repeated_failures': return 'alert-danger'
      default: return 'alert-warning'
    }
  }

  return (
    <div className="dashboard-alerts">
      <h3 className="dashboard-section-title">Alerts</h3>
      <div className="alerts-list">
        {alerts.map((alert, idx) => (
          <div key={idx} className={`alert-item ${getAlertClass(alert.type)}`}>
            <span className="alert-icon">{getAlertIcon(alert.type)}</span>
            <span className="alert-paper">
              {alert.paper_title ? `${alert.paper_title.substring(0, 40)}...` : `Paper #${alert.paper_id}`}
            </span>
            <span className="alert-message">{alert.message}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// Active Harvests Table Component
const ActiveHarvestsTable = ({ harvests }) => {
  if (!harvests || harvests.length === 0) {
    return (
      <div className="dashboard-section">
        <h3 className="dashboard-section-title">Active Harvests</h3>
        <div className="empty-state">No active harvests</div>
      </div>
    )
  }

  return (
    <div className="dashboard-section">
      <h3 className="dashboard-section-title">Active Harvests ({harvests.length})</h3>
      <table className="dashboard-table">
        <thead>
          <tr>
            <th>Paper</th>
            <th>Progress</th>
            <th>Current</th>
            <th>Saved</th>
            <th>Dups</th>
            <th>Gap</th>
            <th>Time</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {harvests.map((h) => {
            const dupPercent = Math.round(h.duplicate_rate * 100)
            const isDupHigh = dupPercent > 50
            const isLongRunning = h.running_minutes > 45

            return (
              <tr key={h.job_id}>
                <td className="cell-paper">
                  <span className="paper-title" title={h.paper_title}>
                    {h.paper_title.substring(0, 35)}...
                  </span>
                  <span className="paper-id">#{h.paper_id}</span>
                </td>
                <td className="cell-progress">
                  <div className="mini-progress-bar">
                    <div
                      className="mini-progress-fill"
                      style={{ width: `${Math.min(h.job_progress, 100)}%` }}
                    />
                    <span className="mini-progress-text">{h.job_progress.toFixed(1)}%</span>
                  </div>
                </td>
                <td className="cell-current">
                  {h.current_year && (
                    <span className="current-info">
                      {h.current_year} / p{h.current_page || 0}
                    </span>
                  )}
                </td>
                <td className="cell-saved">
                  <span className="saved-job">{h.citations_saved_job.toLocaleString()}</span>
                  <span className="saved-hour">+{h.citations_saved_hour}/hr</span>
                </td>
                <td className={`cell-dups ${isDupHigh ? 'dups-high' : ''}`}>
                  {h.duplicates_job.toLocaleString()}
                  <span className="dup-percent">({dupPercent}%)</span>
                </td>
                <td className="cell-gap">
                  <span className="gap-remaining">{h.gap_remaining.toLocaleString()}</span>
                  <span className="gap-total">/ {h.expected_total.toLocaleString()}</span>
                </td>
                <td className={`cell-time ${isLongRunning ? 'time-long' : ''}`}>
                  {h.running_minutes}m
                </td>
                <td className="cell-status">
                  {h.stall_count > 0 && (
                    <span className="stall-badge" title={`${h.stall_count} stalls`}>
                      {h.stall_count}
                    </span>
                  )}
                  {h.edition_count > 1 && (
                    <span className="edition-badge" title={`${h.edition_count} editions`}>
                      {h.edition_count}ed
                    </span>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// Recently Completed Section
const RecentlyCompletedSection = ({ papers, isExpanded, onToggle }) => {
  if (!papers || papers.length === 0) {
    return null
  }

  return (
    <div className="dashboard-section collapsible">
      <h3 className="dashboard-section-title clickable" onClick={onToggle}>
        <span>{isExpanded ? '▼' : '▶'}</span> Recently Completed ({papers.length})
      </h3>
      {isExpanded && (
        <table className="dashboard-table compact">
          <thead>
            <tr>
              <th>Paper</th>
              <th>Harvested</th>
              <th>Complete</th>
              <th>When</th>
            </tr>
          </thead>
          <tbody>
            {papers.map((p) => (
              <tr key={p.paper_id}>
                <td className="cell-paper">
                  <span className="paper-title">{p.paper_title.substring(0, 45)}...</span>
                </td>
                <td>{p.total_harvested.toLocaleString()} / {p.expected_total.toLocaleString()}</td>
                <td className="cell-percent">{(p.gap_percent * 100).toFixed(1)}%</td>
                <td>{formatTimeAgo(p.completed_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

// Job History Section
const JobHistorySection = ({ isExpanded, onToggle }) => {
  const [hours, setHours] = useState(6)
  const [statusFilter, setStatusFilter] = useState('')

  const { data: historyData, isLoading } = useQuery({
    queryKey: ['job-history', hours, statusFilter],
    queryFn: () => api.getJobHistory({ hours, status: statusFilter || undefined, limit: 50 }),
    enabled: isExpanded,
    refetchInterval: 30000,
  })

  const getStatusIcon = (status) => {
    switch (status) {
      case 'completed': return '✅'
      case 'failed': return '❌'
      case 'cancelled': return '🚫'
      case 'running': return '🔄'
      case 'pending': return '⏳'
      default: return '❓'
    }
  }

  return (
    <div className="dashboard-section collapsible">
      <h3 className="dashboard-section-title clickable" onClick={onToggle}>
        <span>{isExpanded ? '▼' : '▶'}</span> Job History
        {historyData && <span className="history-count">({historyData.total} jobs)</span>}
      </h3>
      {isExpanded && (
        <>
          <div className="history-filters">
            <select value={hours} onChange={(e) => setHours(Number(e.target.value))}>
              <option value={1}>Last hour</option>
              <option value={6}>Last 6 hours</option>
              <option value={24}>Last 24 hours</option>
            </select>
            <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
              <option value="">All statuses</option>
              <option value="completed">Completed</option>
              <option value="failed">Failed</option>
              <option value="cancelled">Cancelled</option>
            </select>
          </div>
          {isLoading ? (
            <div className="loading">Loading...</div>
          ) : historyData?.jobs?.length > 0 ? (
            <table className="dashboard-table compact">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Paper</th>
                  <th>Type</th>
                  <th>Saved</th>
                  <th>Duration</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {historyData.jobs.map((job) => (
                  <tr key={job.id} className={`status-${job.status}`}>
                    <td className="cell-status-icon">{getStatusIcon(job.status)}</td>
                    <td className="cell-paper">
                      {job.paper_title ? (
                        <span title={job.paper_title}>{job.paper_title.substring(0, 30)}...</span>
                      ) : (
                        <span className="no-paper">-</span>
                      )}
                    </td>
                    <td className="cell-type">{job.job_type.replace('extract_', '').replace('_', ' ')}</td>
                    <td className="cell-saved-small">
                      {job.citations_saved > 0 ? job.citations_saved.toLocaleString() : '-'}
                    </td>
                    <td className="cell-duration">{formatDuration(job.duration_seconds)}</td>
                    <td className="cell-error">
                      {job.error && (
                        <span className="error-text" title={job.error}>
                          {job.error.substring(0, 30)}...
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state">No jobs in this time range</div>
          )}
        </>
      )}
    </div>
  )
}

// Main Dashboard Component
export default function HarvestDashboard() {
  const [showCompleted, setShowCompleted] = useState(false)
  const [showHistory, setShowHistory] = useState(false)

  const { data, isLoading, error } = useQuery({
    queryKey: ['harvest-dashboard'],
    queryFn: () => api.getHarvestDashboard(),
    refetchInterval: 5000, // Poll every 5 seconds
  })

  if (isLoading) {
    return (
      <div className="harvest-dashboard loading-state">
        <div className="loading-spinner">Loading dashboard...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="harvest-dashboard error-state">
        <div className="error-message">Failed to load dashboard: {error.message}</div>
      </div>
    )
  }

  const { system_health, active_harvests, recently_completed, alerts, job_history_summary } = data || {}

  // Determine health status for each metric
  const getJobsStatus = () => {
    if (system_health?.active_jobs === 0 && active_harvests?.length === 0) return 'warning'
    return 'ok'
  }

  const getCitationsStatus = () => {
    if (system_health?.citations_last_hour === 0 && system_health?.active_jobs > 0) return 'danger'
    return 'ok'
  }

  const getSuccessRate = () => {
    const h = job_history_summary?.last_24h || {}
    const total = (h.completed || 0) + (h.failed || 0) + (h.cancelled || 0)
    if (total === 0) return 100
    return Math.round(((h.completed || 0) / total) * 100)
  }

  const getSuccessStatus = () => {
    const rate = getSuccessRate()
    if (rate < 80) return 'danger'
    if (rate < 95) return 'warning'
    return 'ok'
  }

  const getDuplicateStatus = () => {
    const rate = (system_health?.avg_duplicate_rate_1h || 0) * 100
    if (rate > 50) return 'danger'
    if (rate > 30) return 'warning'
    return 'ok'
  }

  return (
    <div className="harvest-dashboard">
      {/* Health Cards Row */}
      <div className="dashboard-health-row">
        <HealthCard
          icon="🔄"
          title="Active Jobs"
          value={`${system_health?.active_jobs || 0} / ${system_health?.max_concurrent_jobs || 20}`}
          subtext={`${system_health?.papers_with_active_jobs || 0} papers`}
          status={getJobsStatus()}
        />
        <HealthCard
          icon="📊"
          title="Citations/Hour"
          value={(system_health?.citations_last_hour || 0).toLocaleString()}
          status={getCitationsStatus()}
        />
        <HealthCard
          icon="✅"
          title="Success Rate"
          value={`${getSuccessRate()}%`}
          subtext={`${job_history_summary?.last_24h?.completed || 0} / ${(job_history_summary?.last_24h?.completed || 0) + (job_history_summary?.last_24h?.failed || 0)} (24h)`}
          status={getSuccessStatus()}
        />
        <HealthCard
          icon="🔁"
          title="Duplicate Rate"
          value={`${Math.round((system_health?.avg_duplicate_rate_1h || 0) * 100)}%`}
          subtext="avg last hour"
          status={getDuplicateStatus()}
        />
      </div>

      {/* Alerts */}
      <AlertsSection alerts={alerts} />

      {/* Active Harvests */}
      <ActiveHarvestsTable harvests={active_harvests} />

      {/* Recently Completed (collapsible) */}
      <RecentlyCompletedSection
        papers={recently_completed}
        isExpanded={showCompleted}
        onToggle={() => setShowCompleted(!showCompleted)}
      />

      {/* Job History (collapsible) */}
      <JobHistorySection
        isExpanded={showHistory}
        onToggle={() => setShowHistory(!showHistory)}
      />
    </div>
  )
}
