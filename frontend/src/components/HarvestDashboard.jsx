import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
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

// AI Diagnosis Modal Component
const AIDiagnosisModal = ({ isOpen, onClose, diagnosisData, isLoading, error, editionId, onActionExecuted }) => {
  const [isExecuting, setIsExecuting] = useState(false)
  const [actionResult, setActionResult] = useState(null)
  const [actionError, setActionError] = useState(null)

  // Reset action state when new diagnosis data arrives
  useEffect(() => {
    setActionResult(null)
    setActionError(null)
    setIsExecuting(false)
  }, [diagnosisData])

  if (!isOpen) return null

  const handleExecuteAction = async () => {
    if (!diagnosisData?.analysis?.recommended_action || !editionId) return

    const { action_type, specific_params } = diagnosisData.analysis.recommended_action
    setIsExecuting(true)
    setActionResult(null)
    setActionError(null)

    try {
      const result = await api.executeAIAction(editionId, action_type, specific_params || {})
      setActionResult(result)
      // Notify parent to refresh data
      onActionExecuted?.()
    } catch (e) {
      console.error('Action execution failed:', e)
      setActionError(e.message || 'Action failed')
    } finally {
      setIsExecuting(false)
    }
  }

  const getRootCauseColor = (rootCause) => {
    switch (rootCause) {
      case 'RESUME_BUG': return '#f59e0b' // amber
      case 'RATE_LIMITING': return '#ef4444' // red
      case 'OVERFLOW_YEAR': return '#8b5cf6' // purple
      case 'GS_INCONSISTENCY': return '#22c55e' // green
      case 'INCOMPLETE_YEARS': return '#3b82f6' // blue
      case 'NETWORK_ISSUES': return '#f97316' // orange
      default: return '#6b7280' // gray
    }
  }

  const getConfidenceColor = (confidence) => {
    switch (confidence) {
      case 'HIGH': return '#22c55e'
      case 'MEDIUM': return '#f59e0b'
      case 'LOW': return '#ef4444'
      default: return '#6b7280'
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="ai-diagnosis-modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h2>🤖 AI Harvest Diagnosis</h2>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>

        <div className="modal-body">
          {isLoading && (
            <div className="diagnosis-loading">
              <div className="loading-spinner"></div>
              <p>Analyzing with Claude Opus 4.5...</p>
              <p className="loading-subtext">This may take 30-60 seconds (extended thinking mode)</p>
            </div>
          )}

          {error && (
            <div className="diagnosis-error">
              <p>❌ Analysis failed: {error}</p>
            </div>
          )}

          {diagnosisData && !isLoading && (
            <>
              {/* Context Summary */}
              <div className="diagnosis-section context-summary">
                <h3>📊 Context</h3>
                <div className="context-grid">
                  <div><strong>Paper:</strong> {diagnosisData.paper_title}</div>
                  <div><strong>Edition:</strong> {diagnosisData.edition_title}</div>
                  {diagnosisData.context_summary && (
                    <>
                      <div><strong>Expected:</strong> {diagnosisData.context_summary.expected?.toLocaleString()}</div>
                      <div><strong>Harvested:</strong> {diagnosisData.context_summary.harvested?.toLocaleString()}</div>
                      <div><strong>Gap:</strong> {diagnosisData.context_summary.gap?.toLocaleString()} ({diagnosisData.context_summary.gap_percent}%)</div>
                      <div><strong>Years:</strong> {diagnosisData.context_summary.years_complete}/{diagnosisData.context_summary.years_total} complete</div>
                    </>
                  )}
                </div>
              </div>

              {/* Analysis Results */}
              {diagnosisData.analysis && (
                <>
                  {/* Root Cause */}
                  <div className="diagnosis-section root-cause">
                    <h3>🔍 Root Cause</h3>
                    <div className="root-cause-badge" style={{ borderColor: getRootCauseColor(diagnosisData.analysis.root_cause) }}>
                      <span className="root-cause-label" style={{ backgroundColor: getRootCauseColor(diagnosisData.analysis.root_cause) }}>
                        {diagnosisData.analysis.root_cause || 'UNKNOWN'}
                      </span>
                      <span className="confidence-badge" style={{ color: getConfidenceColor(diagnosisData.analysis.confidence) }}>
                        {diagnosisData.analysis.confidence} confidence
                      </span>
                    </div>
                    <p className="explanation">{diagnosisData.analysis.root_cause_explanation}</p>
                  </div>

                  {/* Gap Recoverable */}
                  <div className="diagnosis-section recoverable">
                    <h3>{diagnosisData.analysis.gap_recoverable ? '✅ Gap is Recoverable' : '❌ Gap Not Recoverable'}</h3>
                    <p>{diagnosisData.analysis.gap_recoverable_explanation}</p>
                  </div>

                  {/* Recommended Action */}
                  {diagnosisData.analysis.recommended_action && (
                    <div className="diagnosis-section recommended-action">
                      <h3>💡 Recommended Action</h3>
                      <div className="action-header">
                        <div className="action-type-badge">
                          {diagnosisData.analysis.recommended_action.action_type}
                        </div>
                        <button
                          className="btn-execute-action"
                          onClick={handleExecuteAction}
                          disabled={isExecuting || actionResult?.success}
                        >
                          {isExecuting ? '⏳ Executing...' :
                           actionResult?.success ? '✅ Done' :
                           `▶️ Execute ${diagnosisData.analysis.recommended_action.action_type}`}
                        </button>
                      </div>
                      <p className="action-description">{diagnosisData.analysis.recommended_action.action_description}</p>
                      {diagnosisData.analysis.recommended_action.specific_params && (
                        <div className="action-params">
                          <strong>Parameters:</strong>
                          <pre>{JSON.stringify(diagnosisData.analysis.recommended_action.specific_params, null, 2)}</pre>
                        </div>
                      )}

                      {/* Action Result */}
                      {actionResult && (
                        <div className="action-result success">
                          <strong>✅ Action executed successfully!</strong>
                          <p>{actionResult.message}</p>
                          {actionResult.job_id && <p>Job #{actionResult.job_id} has been queued.</p>}
                        </div>
                      )}
                      {actionError && (
                        <div className="action-result error">
                          <strong>❌ Action failed:</strong>
                          <p>{actionError}</p>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Additional Notes */}
                  {diagnosisData.analysis.additional_notes && (
                    <div className="diagnosis-section notes">
                      <h3>📝 Additional Notes</h3>
                      <p>{diagnosisData.analysis.additional_notes}</p>
                    </div>
                  )}

                  {/* Parse Error - show raw response */}
                  {diagnosisData.analysis.parse_error && (
                    <div className="diagnosis-section parse-error">
                      <h3>⚠️ Response Parse Error</h3>
                      <p>Could not parse AI response as JSON. Raw response:</p>
                      <pre className="raw-response">{diagnosisData.analysis.raw_response}</pre>
                    </div>
                  )}

                  {/* Thinking Summary (collapsible) */}
                  {diagnosisData.analysis.thinking_summary && (
                    <details className="diagnosis-section thinking">
                      <summary>🧠 AI Thinking Process (click to expand)</summary>
                      <pre className="thinking-text">{diagnosisData.analysis.thinking_summary}</pre>
                    </details>
                  )}
                </>
              )}
            </>
          )}
        </div>

        <div className="modal-footer">
          <button className="btn-close-modal" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  )
}

// Alerts Section Component with restart functionality
const AlertsSection = ({ alerts, onPaperClick, onRefresh }) => {
  const [selectedEditions, setSelectedEditions] = useState(new Set())
  const [isRestarting, setIsRestarting] = useState(false)
  const [isMarking, setIsMarking] = useState(false)
  const [restartingIds, setRestartingIds] = useState(new Set())
  const [markingIds, setMarkingIds] = useState(new Set())
  const [diagnosingIds, setDiagnosingIds] = useState(new Set())
  const [diagnosisModal, setDiagnosisModal] = useState({ isOpen: false, data: null, isLoading: false, error: null, editionId: null })

  if (!alerts || alerts.length === 0) return null

  // Filter to only stalled papers (the ones we can restart)
  const stalledAlerts = alerts.filter(a => a.type === 'stalled_paper' && a.edition_id)
  const otherAlerts = alerts.filter(a => a.type !== 'stalled_paper' || !a.edition_id)

  // Count GS fault vs needs scraping
  const gsFaultCount = stalledAlerts.filter(a => a.diagnosis === 'gs_fault').length
  const needsScrapingCount = stalledAlerts.filter(a => a.diagnosis === 'needs_scraping').length

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

  const getDiagnosisLabel = (diagnosis) => {
    switch (diagnosis) {
      case 'gs_fault': return { text: 'GS Fault', class: 'diagnosis-gs-fault', tip: 'All years scraped - gap is GS data inaccuracy' }
      case 'needs_scraping': return { text: 'Incomplete', class: 'diagnosis-needs-scraping', tip: 'Some years not fully scraped yet' }
      case 'no_data': return { text: 'No Data', class: 'diagnosis-no-data', tip: 'No harvest targets yet - run harvest first' }
      default: return { text: 'Unknown', class: 'diagnosis-unknown', tip: 'Unknown status' }
    }
  }

  const toggleSelection = (editionId) => {
    const newSet = new Set(selectedEditions)
    if (newSet.has(editionId)) {
      newSet.delete(editionId)
    } else {
      newSet.add(editionId)
    }
    setSelectedEditions(newSet)
  }

  const toggleSelectAll = () => {
    if (selectedEditions.size === stalledAlerts.length) {
      setSelectedEditions(new Set())
    } else {
      setSelectedEditions(new Set(stalledAlerts.map(a => a.edition_id)))
    }
  }

  const handleRestartSingle = async (editionId) => {
    setRestartingIds(prev => new Set([...prev, editionId]))
    try {
      await api.restartStalledPapers([editionId])
      onRefresh?.()
    } catch (e) {
      console.error('Restart failed:', e)
    } finally {
      setRestartingIds(prev => {
        const newSet = new Set(prev)
        newSet.delete(editionId)
        return newSet
      })
    }
  }

  const handleRestartSelected = async () => {
    if (selectedEditions.size === 0) return
    setIsRestarting(true)
    try {
      await api.restartStalledPapers([...selectedEditions])
      setSelectedEditions(new Set())
      onRefresh?.()
    } catch (e) {
      console.error('Batch restart failed:', e)
    } finally {
      setIsRestarting(false)
    }
  }

  const handleRestartAll = async () => {
    setIsRestarting(true)
    try {
      await api.restartAllStalledPapers()
      setSelectedEditions(new Set())
      onRefresh?.()
    } catch (e) {
      console.error('Restart all failed:', e)
    } finally {
      setIsRestarting(false)
    }
  }

  const handleMarkCompleteSingle = async (editionId) => {
    setMarkingIds(prev => new Set([...prev, editionId]))
    try {
      await api.markEditionComplete(editionId)
      onRefresh?.()
    } catch (e) {
      console.error('Mark complete failed:', e)
    } finally {
      setMarkingIds(prev => {
        const newSet = new Set(prev)
        newSet.delete(editionId)
        return newSet
      })
    }
  }

  const handleMarkCompleteSelected = async () => {
    if (selectedEditions.size === 0) return
    setIsMarking(true)
    try {
      await api.markEditionsCompleteBatch([...selectedEditions])
      setSelectedEditions(new Set())
      onRefresh?.()
    } catch (e) {
      console.error('Batch mark complete failed:', e)
    } finally {
      setIsMarking(false)
    }
  }

  const handleDiagnose = async (editionId) => {
    setDiagnosingIds(prev => new Set([...prev, editionId]))
    setDiagnosisModal({ isOpen: true, data: null, isLoading: true, error: null, editionId })

    try {
      const result = await api.aiDiagnoseEdition(editionId)
      setDiagnosisModal({ isOpen: true, data: result, isLoading: false, error: null, editionId })
    } catch (e) {
      console.error('AI diagnosis failed:', e)
      setDiagnosisModal({ isOpen: true, data: null, isLoading: false, error: e.message, editionId })
    } finally {
      setDiagnosingIds(prev => {
        const newSet = new Set(prev)
        newSet.delete(editionId)
        return newSet
      })
    }
  }

  const closeDiagnosisModal = () => {
    setDiagnosisModal({ isOpen: false, data: null, isLoading: false, error: null, editionId: null })
  }

  const handleActionExecuted = () => {
    // Refresh dashboard data after executing an action
    onRefresh?.()
  }

  return (
    <>
      <AIDiagnosisModal
        isOpen={diagnosisModal.isOpen}
        onClose={closeDiagnosisModal}
        diagnosisData={diagnosisModal.data}
        isLoading={diagnosisModal.isLoading}
        error={diagnosisModal.error}
        editionId={diagnosisModal.editionId}
        onActionExecuted={handleActionExecuted}
      />
    <div className="dashboard-alerts">
      <div className="alerts-header">
        <h3 className="dashboard-section-title">
          Alerts ({stalledAlerts.length} stalled{otherAlerts.length > 0 ? `, ${otherAlerts.length} other` : ''})
          {gsFaultCount > 0 && <span className="diagnosis-summary diagnosis-gs-fault"> • {gsFaultCount} GS Fault</span>}
          {needsScrapingCount > 0 && <span className="diagnosis-summary diagnosis-needs-scraping"> • {needsScrapingCount} Incomplete</span>}
        </h3>
        {stalledAlerts.length > 0 && (
          <div className="alerts-actions">
            <button
              className="btn-mark-complete-selected"
              onClick={handleMarkCompleteSelected}
              disabled={selectedEditions.size === 0 || isMarking}
              title="Mark selected as complete (stop auto-resume)"
            >
              {isMarking ? '...' : `Mark Complete (${selectedEditions.size})`}
            </button>
            <button
              className="btn-restart-selected"
              onClick={handleRestartSelected}
              disabled={selectedEditions.size === 0 || isRestarting}
            >
              {isRestarting ? '...' : `Restart (${selectedEditions.size})`}
            </button>
            <button
              className="btn-restart-all"
              onClick={handleRestartAll}
              disabled={isRestarting}
            >
              {isRestarting ? '...' : 'Restart All'}
            </button>
          </div>
        )}
      </div>

      {/* Stalled papers table */}
      {stalledAlerts.length > 0 && (
        <table className="alerts-table">
          <thead>
            <tr>
              <th className="col-checkbox">
                <input
                  type="checkbox"
                  checked={selectedEditions.size === stalledAlerts.length && stalledAlerts.length > 0}
                  onChange={toggleSelectAll}
                />
              </th>
              <th className="col-paper">Paper</th>
              <th className="col-diagnosis">Diagnosis</th>
              <th className="col-years">Years</th>
              <th className="col-harvested">Harvested</th>
              <th className="col-expected">Expected</th>
              <th className="col-gap">Gap</th>
              <th className="col-action">Action</th>
            </tr>
          </thead>
          <tbody>
            {stalledAlerts.map((alert) => {
              const gapPercent = alert.expected_count > 0
                ? Math.round((alert.gap_remaining / alert.expected_count) * 100)
                : 0
              const isSelected = selectedEditions.has(alert.edition_id)
              const isRestartingThis = restartingIds.has(alert.edition_id)
              const isMarkingThis = markingIds.has(alert.edition_id)
              const diag = getDiagnosisLabel(alert.diagnosis)

              return (
                <tr key={alert.edition_id} className={`${isSelected ? 'selected' : ''} ${alert.diagnosis === 'gs_fault' ? 'row-gs-fault' : ''}`}>
                  <td className="col-checkbox">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => toggleSelection(alert.edition_id)}
                    />
                  </td>
                  <td className="col-paper">
                    <span
                      className="clickable-paper"
                      onClick={() => alert.paper_id && onPaperClick(alert.paper_id)}
                    >
                      {alert.paper_title ? `${alert.paper_title.substring(0, 30)}...` : `Paper #${alert.paper_id}`}
                    </span>
                  </td>
                  <td className="col-diagnosis">
                    <span className={`diagnosis-badge ${diag.class}`} title={diag.tip}>
                      {diag.text}
                      {alert.has_overflow_years && <span className="overflow-indicator" title="Has overflow years (>1000)">⚡</span>}
                    </span>
                  </td>
                  <td className="col-years">
                    {alert.years_total > 0 ? (
                      <span className={alert.years_complete === alert.years_total ? 'years-complete' : 'years-partial'}>
                        {alert.years_complete}/{alert.years_total}
                      </span>
                    ) : (
                      <span className="years-none">—</span>
                    )}
                  </td>
                  <td className="col-harvested">{(alert.harvested_count || 0).toLocaleString()}</td>
                  <td className="col-expected">{(alert.expected_count || 0).toLocaleString()}</td>
                  <td className="col-gap">
                    <span className={gapPercent > 20 ? 'gap-high' : 'gap-low'}>
                      {(alert.gap_remaining || 0).toLocaleString()} ({gapPercent}%)
                    </span>
                  </td>
                  <td className="col-action">
                    <div className="action-buttons">
                      <button
                        className="btn-diagnose"
                        onClick={() => handleDiagnose(alert.edition_id)}
                        disabled={diagnosingIds.has(alert.edition_id)}
                        title="Run AI diagnosis to understand why harvest stalled"
                      >
                        {diagnosingIds.has(alert.edition_id) ? '🔄' : '🤖'}
                      </button>
                      {alert.diagnosis === 'gs_fault' ? (
                        <button
                          className="btn-mark-complete"
                          onClick={() => handleMarkCompleteSingle(alert.edition_id)}
                          disabled={isMarkingThis || isMarking}
                          title="Mark as complete (gap is GS's fault)"
                        >
                          {isMarkingThis ? '...' : '✓ Done'}
                        </button>
                      ) : (
                        <button
                          className="btn-restart-single"
                          onClick={() => handleRestartSingle(alert.edition_id)}
                          disabled={isRestartingThis || isRestarting}
                        >
                          {isRestartingThis ? '...' : 'Restart'}
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}

      {/* Other alerts (non-stalled) */}
      {otherAlerts.length > 0 && (
        <div className="alerts-list">
          {otherAlerts.map((alert, idx) => (
            <div key={idx} className={`alert-item ${getAlertClass(alert.type)}`}>
              <span className="alert-icon">{getAlertIcon(alert.type)}</span>
              <span
                className="alert-paper clickable-paper"
                onClick={() => alert.paper_id && onPaperClick(alert.paper_id)}
              >
                {alert.paper_title ? `${alert.paper_title.substring(0, 40)}...` : `Paper #${alert.paper_id}`}
              </span>
              <span className="alert-message">{alert.message}</span>
            </div>
          ))}
        </div>
      )}
    </div>
    </>
  )
}

// Active Harvests Table Component
const ActiveHarvestsTable = ({ harvests, onPaperClick }) => {
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
                  <span
                    className="paper-title clickable-paper"
                    title={h.paper_title}
                    onClick={() => onPaperClick(h.paper_id)}
                  >
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
const RecentlyCompletedSection = ({ papers, isExpanded, onToggle, onPaperClick }) => {
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
                  <span
                    className="paper-title clickable-paper"
                    onClick={() => onPaperClick(p.paper_id)}
                  >
                    {p.paper_title.substring(0, 45)}...
                  </span>
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
const JobHistorySection = ({ isExpanded, onToggle, onPaperClick }) => {
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
                        <span
                          className="clickable-paper"
                          title={job.paper_title}
                          onClick={() => job.paper_id && onPaperClick(job.paper_id)}
                        >
                          {job.paper_title.substring(0, 30)}...
                        </span>
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
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [showCompleted, setShowCompleted] = useState(false)
  const [showHistory, setShowHistory] = useState(false)

  const handlePaperClick = (paperId) => {
    navigate(`/paper/${paperId}`)
  }

  const handleRefresh = () => {
    // Invalidate dashboard query to trigger immediate refresh
    queryClient.invalidateQueries({ queryKey: ['harvest-dashboard'] })
  }

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
      <AlertsSection alerts={alerts} onPaperClick={handlePaperClick} onRefresh={handleRefresh} />

      {/* Active Harvests */}
      <ActiveHarvestsTable harvests={active_harvests} onPaperClick={handlePaperClick} />

      {/* Recently Completed (collapsible) */}
      <RecentlyCompletedSection
        papers={recently_completed}
        isExpanded={showCompleted}
        onToggle={() => setShowCompleted(!showCompleted)}
        onPaperClick={handlePaperClick}
      />

      {/* Job History (collapsible) */}
      <JobHistorySection
        isExpanded={showHistory}
        onToggle={() => setShowHistory(!showHistory)}
        onPaperClick={handlePaperClick}
      />
    </div>
  )
}
