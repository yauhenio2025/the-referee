/**
 * Edition Analysis Component - Exhaustive bibliography analysis for thinker dossiers
 *
 * This component allows users to:
 * 1. Start edition analysis for a dossier
 * 2. View analysis results (linked works vs expected bibliography)
 * 3. See gaps (missing translations, missing major works)
 * 4. Create scraper jobs from gaps
 * 5. Dismiss gaps with reasons
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import { useToast } from './Toast'

function EditionAnalysis({ dossierId, thinkerName, onClose }) {
  const queryClient = useQueryClient()
  const { showToast } = useToast()
  const [activeTab, setActiveTab] = useState('overview')
  const [selectedRun, setSelectedRun] = useState(null)
  const [dismissReason, setDismissReason] = useState('')
  const [dismissingGap, setDismissingGap] = useState(null)

  // Fetch edition analysis runs for this dossier
  const { data: analysisData, isLoading, error } = useQuery({
    queryKey: ['edition-analysis', dossierId],
    queryFn: () => api.getDossierEditionAnalysis(dossierId),
    enabled: !!dossierId,
  })

  // Fetch specific run details when selected
  const { data: runDetails, isLoading: loadingRun } = useQuery({
    queryKey: ['edition-analysis-run', selectedRun],
    queryFn: () => api.getEditionAnalysisRun(selectedRun),
    enabled: !!selectedRun,
  })

  // Fetch LLM calls for selected run (for transparency)
  const { data: llmCalls } = useQuery({
    queryKey: ['edition-analysis-llm-calls', selectedRun],
    queryFn: () => api.getEditionAnalysisLLMCalls(selectedRun),
    enabled: !!selectedRun && activeTab === 'llm-calls',
  })

  // Fetch bibliography for thinker
  const { data: bibliography, isLoading: loadingBibliography } = useQuery({
    queryKey: ['thinker-bibliography', thinkerName],
    queryFn: () => api.getThinkerBibliography(thinkerName),
    enabled: !!thinkerName && activeTab === 'bibliography',
  })

  // Start new analysis mutation
  const startAnalysisMutation = useMutation({
    mutationFn: (options = {}) => api.startEditionAnalysis(dossierId, options),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['edition-analysis', dossierId] })
      showToast(`Analysis started (Run #${data.run_id})`, 'success')
      setSelectedRun(data.run_id)
    },
    onError: (err) => showToast(`Failed to start analysis: ${err.message}`, 'error'),
  })

  // Create job from gap mutation
  const createJobMutation = useMutation({
    mutationFn: ({ missingId, options }) => api.createJobFromGap(missingId, options),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['edition-analysis-run', selectedRun] })
      showToast(`Job created (ID: ${data.job_id})`, 'success')
    },
    onError: (err) => showToast(`Failed to create job: ${err.message}`, 'error'),
  })

  // Dismiss gap mutation
  const dismissGapMutation = useMutation({
    mutationFn: ({ missingId, reason }) => api.dismissGap(missingId, reason),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['edition-analysis-run', selectedRun] })
      showToast('Gap dismissed', 'success')
      setDismissingGap(null)
      setDismissReason('')
    },
    onError: (err) => showToast(`Failed to dismiss: ${err.message}`, 'error'),
  })

  const handleStartAnalysis = (forceRefresh = false) => {
    startAnalysisMutation.mutate({ forceRefresh })
  }

  const handleCreateJob = (missingId, priority = 'normal') => {
    createJobMutation.mutate({ missingId, options: { priority } })
  }

  const handleDismissGap = (missingId) => {
    dismissGapMutation.mutate({ missingId, reason: dismissReason })
  }

  if (isLoading) {
    return <div className="loading">Loading edition analysis...</div>
  }

  if (error) {
    return (
      <div className="error-state">
        <p>Error loading edition analysis: {error.message}</p>
        <button onClick={() => queryClient.invalidateQueries({ queryKey: ['edition-analysis', dossierId] })}>
          Retry
        </button>
      </div>
    )
  }

  // API returns single 'run' object, convert to array for history display
  const runs = analysisData?.run ? [analysisData.run] : []
  const currentRun = runDetails || analysisData?.run || null
  // Works and missing editions come from the top-level response, not the run
  const linkedWorks = analysisData?.works || []
  // Flatten missing editions from all works
  const missingEditions = linkedWorks.flatMap(w => w.missing_editions || [])

  return (
    <div className="edition-analysis">
      {/* Header */}
      <div className="analysis-header">
        <div className="header-left">
          <h2>Edition Analysis</h2>
          {thinkerName && <span className="thinker-name">for {thinkerName}</span>}
        </div>
        <div className="header-actions">
          <button
            className="btn btn-primary"
            onClick={() => handleStartAnalysis(false)}
            disabled={startAnalysisMutation.isPending}
          >
            {startAnalysisMutation.isPending ? 'Starting...' : 'Start Analysis'}
          </button>
          <button
            className="btn btn-secondary"
            onClick={() => handleStartAnalysis(true)}
            disabled={startAnalysisMutation.isPending}
            title="Force refresh even if recent analysis exists"
          >
            Force Refresh
          </button>
          {onClose && (
            <button className="btn btn-ghost" onClick={onClose}>
              Close
            </button>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="tabs-nav">
        <button
          className={`tab-btn ${activeTab === 'overview' ? 'active' : ''}`}
          onClick={() => setActiveTab('overview')}
        >
          Overview
        </button>
        <button
          className={`tab-btn ${activeTab === 'gaps' ? 'active' : ''}`}
          onClick={() => setActiveTab('gaps')}
        >
          Gaps ({missingEditions.filter(e => e.status === 'pending').length})
        </button>
        <button
          className={`tab-btn ${activeTab === 'linked' ? 'active' : ''}`}
          onClick={() => setActiveTab('linked')}
        >
          Linked Works ({linkedWorks.length})
        </button>
        <button
          className={`tab-btn ${activeTab === 'bibliography' ? 'active' : ''}`}
          onClick={() => setActiveTab('bibliography')}
        >
          Bibliography
        </button>
        <button
          className={`tab-btn ${activeTab === 'history' ? 'active' : ''}`}
          onClick={() => setActiveTab('history')}
        >
          History ({runs.length})
        </button>
        <button
          className={`tab-btn ${activeTab === 'llm-calls' ? 'active' : ''}`}
          onClick={() => setActiveTab('llm-calls')}
        >
          LLM Calls
        </button>
      </div>

      {/* Tab Content */}
      <div className="tab-content">
        {/* Overview Tab */}
        {activeTab === 'overview' && (
          <div className="overview-tab">
            {!currentRun ? (
              <div className="empty-state">
                <p>No edition analysis has been run yet for this dossier.</p>
                <p>Click "Start Analysis" to compare linked works against expected bibliography.</p>
              </div>
            ) : (
              <>
                <div className="stats-grid">
                  <div className="stat-card">
                    <span className="stat-value">{currentRun.works_identified || 0}</span>
                    <span className="stat-label">Works Identified</span>
                  </div>
                  <div className="stat-card">
                    <span className="stat-value">{currentRun.links_created || 0}</span>
                    <span className="stat-label">Editions Linked</span>
                  </div>
                  <div className="stat-card warning">
                    <span className="stat-value">{currentRun.gaps_found || 0}</span>
                    <span className="stat-label">Gaps Found</span>
                  </div>
                  <div className="stat-card">
                    <span className="stat-value">{currentRun.jobs_created || 0}</span>
                    <span className="stat-label">Jobs Created</span>
                  </div>
                </div>

                <div className="run-info">
                  <h3>Latest Analysis Run</h3>
                  <div className="info-grid">
                    <div className="info-item">
                      <span className="label">Status:</span>
                      <span className={`status-badge status-${currentRun.status}`}>
                        {currentRun.status}
                      </span>
                    </div>
                    <div className="info-item">
                      <span className="label">Started:</span>
                      <span>{new Date(currentRun.created_at).toLocaleString()}</span>
                    </div>
                    {currentRun.completed_at && (
                      <div className="info-item">
                        <span className="label">Completed:</span>
                        <span>{new Date(currentRun.completed_at).toLocaleString()}</span>
                      </div>
                    )}
                  </div>
                </div>

                {/* Quick summary of gaps */}
                {missingEditions.length > 0 && (
                  <div className="gaps-summary">
                    <h3>Gap Summary</h3>
                    <div className="gap-types">
                      <div className="gap-type">
                        <span className="type-label">Missing Editions:</span>
                        <span className="type-count">{missingEditions.length}</span>
                      </div>
                      <div className="gap-type">
                        <span className="type-label">Pending:</span>
                        <span className="type-count">
                          {missingEditions.filter(e => e.status === 'pending').length}
                        </span>
                      </div>
                      <div className="gap-type">
                        <span className="type-label">Jobs Created:</span>
                        <span className="type-count">
                          {missingEditions.filter(e => e.status === 'job_created').length}
                        </span>
                      </div>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {/* Gaps Tab */}
        {activeTab === 'gaps' && (
          <div className="gaps-tab">
            {missingEditions.length === 0 ? (
              <div className="empty-state">
                <p>No gaps identified. All expected editions are linked.</p>
              </div>
            ) : (
              <div className="gaps-list">
                {missingEditions.map((gap) => (
                  <div key={gap.id} className={`gap-card gap-${gap.status}`}>
                    <div className="gap-header">
                      <span className={`gap-type-badge type-${gap.source || 'unknown'}`}>
                        {gap.source === 'llm_knowledge' ? 'LLM Found' :
                         gap.source === 'web_search' ? 'Web Search' :
                         gap.source || 'Missing Edition'}
                      </span>
                      <span className={`gap-status status-${gap.status}`}>
                        {gap.status}
                      </span>
                    </div>
                    <div className="gap-content">
                      <h4 className="gap-title">{gap.expected_title || gap.work_canonical_title}</h4>
                      {gap.language && (
                        <span className="gap-language">Language: {gap.language}</span>
                      )}
                      {gap.expected_year && (
                        <span className="gap-year">Year: {gap.expected_year}</span>
                      )}
                      {gap.notes && (
                        <p className="gap-reason">{gap.notes}</p>
                      )}
                    </div>
                    <div className="gap-actions">
                      {gap.status === 'pending' && (
                        <>
                          <button
                            className="btn btn-sm btn-primary"
                            onClick={() => handleCreateJob(gap.id)}
                            disabled={createJobMutation.isPending}
                          >
                            Create Job
                          </button>
                          <button
                            className="btn btn-sm btn-secondary"
                            onClick={() => handleCreateJob(gap.id, 'high')}
                            disabled={createJobMutation.isPending}
                          >
                            High Priority
                          </button>
                          <button
                            className="btn btn-sm btn-ghost"
                            onClick={() => setDismissingGap(gap.id)}
                          >
                            Dismiss
                          </button>
                        </>
                      )}
                      {gap.status === 'job_created' && (
                        <span className="job-badge">Job #{gap.job_id}</span>
                      )}
                      {gap.status === 'dismissed' && (
                        <span className="dismissed-reason">
                          Dismissed: {gap.dismiss_reason || 'No reason provided'}
                        </span>
                      )}
                    </div>

                    {/* Dismiss dialog */}
                    {dismissingGap === gap.id && (
                      <div className="dismiss-dialog">
                        <input
                          type="text"
                          placeholder="Reason for dismissing (optional)"
                          value={dismissReason}
                          onChange={(e) => setDismissReason(e.target.value)}
                        />
                        <div className="dismiss-actions">
                          <button
                            className="btn btn-sm btn-danger"
                            onClick={() => handleDismissGap(gap.id)}
                            disabled={dismissGapMutation.isPending}
                          >
                            Confirm Dismiss
                          </button>
                          <button
                            className="btn btn-sm btn-ghost"
                            onClick={() => {
                              setDismissingGap(null)
                              setDismissReason('')
                            }}
                          >
                            Cancel
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Linked Works Tab */}
        {activeTab === 'linked' && (
          <div className="linked-tab">
            {linkedWorks.length === 0 ? (
              <div className="empty-state">
                <p>No works linked yet. Run analysis to identify linked works.</p>
              </div>
            ) : (
              <table className="works-table">
                <thead>
                  <tr>
                    <th>Title</th>
                    <th>Original Language</th>
                    <th>Year</th>
                    <th>Editions</th>
                    <th>Importance</th>
                  </tr>
                </thead>
                <tbody>
                  {linkedWorks.map((work) => (
                    <tr key={work.id}>
                      <td className="title-cell">
                        <span className="work-title">{work.canonical_title}</span>
                        {work.original_title && work.original_title !== work.canonical_title && (
                          <span className="original-title">{work.original_title}</span>
                        )}
                      </td>
                      <td>{work.original_language || '-'}</td>
                      <td>{work.original_year || '-'}</td>
                      <td className="number-cell">{work.editions?.length || 0}</td>
                      <td>
                        <span className={`importance importance-${work.importance || 'minor'}`}>
                          {work.importance || 'minor'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}

        {/* Bibliography Tab */}
        {activeTab === 'bibliography' && (
          <div className="bibliography-tab">
            {loadingBibliography ? (
              <div className="loading">Loading bibliography...</div>
            ) : !bibliography ? (
              <div className="empty-state">
                <p>No bibliography data available.</p>
              </div>
            ) : (
              <div className="bibliography-content">
                <div className="bibliography-header">
                  <h3>LLM-Generated Bibliography for {thinkerName}</h3>
                  <p className="bibliography-note">
                    This bibliography was generated by an LLM based on known information about the thinker.
                    It serves as a reference for identifying gaps in coverage.
                  </p>
                </div>
                <div className="bibliography-works">
                  {(bibliography.works || []).map((work, i) => (
                    <div key={i} className="bibliography-work">
                      <span className="work-number">#{i + 1}</span>
                      <div className="work-info">
                        <span className="work-title">{work.title}</span>
                        {work.original_title && (
                          <span className="original-title">{work.original_title}</span>
                        )}
                        <div className="work-meta">
                          {work.year && <span>Year: {work.year}</span>}
                          {work.language && <span>Language: {work.language}</span>}
                          {work.importance && (
                            <span className={`importance importance-${work.importance}`}>
                              {work.importance}
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* History Tab */}
        {activeTab === 'history' && (
          <div className="history-tab">
            {runs.length === 0 ? (
              <div className="empty-state">
                <p>No analysis runs yet.</p>
              </div>
            ) : (
              <div className="runs-list">
                {runs.map((run) => (
                  <div
                    key={run.id}
                    className={`run-card ${selectedRun === run.id ? 'selected' : ''}`}
                    onClick={() => setSelectedRun(run.id)}
                  >
                    <div className="run-header">
                      <span className="run-id">Run #{run.id}</span>
                      <span className={`status-badge status-${run.status}`}>
                        {run.status}
                      </span>
                    </div>
                    <div className="run-stats">
                      <span>{run.works_identified || 0} works</span>
                      <span>{run.gaps_found || 0} gaps</span>
                      <span>{run.jobs_created || 0} jobs</span>
                    </div>
                    <div className="run-date">
                      {new Date(run.created_at).toLocaleString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* LLM Calls Tab */}
        {activeTab === 'llm-calls' && (
          <div className="llm-calls-tab">
            {!selectedRun ? (
              <div className="empty-state">
                <p>Select an analysis run to view LLM calls.</p>
              </div>
            ) : !llmCalls?.calls?.length ? (
              <div className="empty-state">
                <p>No LLM calls recorded for this run.</p>
              </div>
            ) : (
              <div className="llm-calls-list">
                {llmCalls.calls.map((call, i) => (
                  <div key={i} className="llm-call-card">
                    <div className="call-header">
                      <span className="call-type">{call.call_type}</span>
                      <span className="call-model">{call.model}</span>
                      <span className="call-tokens">
                        {call.input_tokens} in / {call.output_tokens} out
                      </span>
                    </div>
                    {call.prompt_summary && (
                      <div className="call-prompt">
                        <strong>Prompt:</strong> {call.prompt_summary}
                      </div>
                    )}
                    {call.response_summary && (
                      <div className="call-response">
                        <strong>Response:</strong> {call.response_summary}
                      </div>
                    )}
                    <div className="call-timestamp">
                      {new Date(call.created_at).toLocaleString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      <style>{`
        .edition-analysis {
          padding: 20px;
        }

        .analysis-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 24px;
        }

        .header-left {
          display: flex;
          align-items: baseline;
          gap: 12px;
        }

        .header-left h2 {
          margin: 0;
        }

        .thinker-name {
          color: var(--text-secondary);
          font-size: 1.1em;
        }

        .header-actions {
          display: flex;
          gap: 8px;
        }

        .tabs-nav {
          display: flex;
          gap: 4px;
          border-bottom: 1px solid var(--border-color);
          margin-bottom: 16px;
          flex-wrap: wrap;
        }

        .tab-btn {
          padding: 12px 16px;
          background: none;
          border: none;
          border-bottom: 2px solid transparent;
          cursor: pointer;
          color: var(--text-secondary);
          transition: all 0.15s;
          font-size: 14px;
        }

        .tab-btn:hover {
          color: var(--text-primary);
        }

        .tab-btn.active {
          color: var(--primary-color);
          border-bottom-color: var(--primary-color);
        }

        .empty-state {
          text-align: center;
          padding: 40px;
          color: var(--text-secondary);
        }

        .stats-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 16px;
          margin-bottom: 24px;
        }

        .stat-card {
          background: var(--bg-secondary);
          padding: 20px;
          border-radius: 8px;
          text-align: center;
        }

        .stat-card.warning {
          border-left: 3px solid var(--warning-color);
        }

        .stat-value {
          display: block;
          font-size: 2em;
          font-weight: 700;
          color: var(--primary-color);
        }

        .stat-card.warning .stat-value {
          color: var(--warning-color);
        }

        .stat-label {
          display: block;
          font-size: 0.85em;
          color: var(--text-secondary);
          margin-top: 4px;
        }

        .run-info {
          background: var(--bg-secondary);
          padding: 20px;
          border-radius: 8px;
          margin-bottom: 24px;
        }

        .run-info h3 {
          margin: 0 0 16px 0;
        }

        .info-grid {
          display: grid;
          gap: 12px;
        }

        .info-item {
          display: flex;
          gap: 12px;
        }

        .info-item .label {
          color: var(--text-secondary);
          min-width: 100px;
        }

        .gaps-summary {
          background: var(--bg-secondary);
          padding: 20px;
          border-radius: 8px;
        }

        .gaps-summary h3 {
          margin: 0 0 16px 0;
        }

        .gap-types {
          display: flex;
          gap: 24px;
          flex-wrap: wrap;
        }

        .gap-type {
          display: flex;
          gap: 8px;
          align-items: center;
        }

        .type-count {
          font-weight: 600;
          color: var(--warning-color);
        }

        .gaps-list {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .gap-card {
          background: var(--bg-secondary);
          border-radius: 8px;
          padding: 16px;
          border-left: 3px solid var(--border-color);
        }

        .gap-card.gap-pending {
          border-left-color: var(--warning-color);
        }

        .gap-card.gap-job_created {
          border-left-color: var(--primary-color);
        }

        .gap-card.gap-dismissed {
          border-left-color: var(--text-muted);
          opacity: 0.7;
        }

        .gap-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 12px;
        }

        .gap-type-badge {
          padding: 4px 8px;
          border-radius: 4px;
          font-size: 0.8em;
          font-weight: 500;
        }

        .gap-type-badge.type-translation {
          background: var(--info-bg);
          color: var(--info-color);
        }

        .gap-type-badge.type-major_work {
          background: var(--warning-bg);
          color: var(--warning-color);
        }

        .gap-status {
          font-size: 0.8em;
        }

        .gap-content h4 {
          margin: 0 0 8px 0;
        }

        .gap-language, .gap-year {
          display: inline-block;
          margin-right: 12px;
          font-size: 0.9em;
          color: var(--text-secondary);
        }

        .gap-reason {
          margin: 8px 0 0 0;
          font-size: 0.9em;
          color: var(--text-secondary);
          font-style: italic;
        }

        .gap-actions {
          display: flex;
          gap: 8px;
          margin-top: 12px;
          flex-wrap: wrap;
        }

        .job-badge {
          padding: 4px 8px;
          background: var(--primary-bg);
          color: var(--primary-color);
          border-radius: 4px;
          font-size: 0.85em;
        }

        .dismissed-reason {
          font-size: 0.85em;
          color: var(--text-muted);
        }

        .dismiss-dialog {
          margin-top: 12px;
          padding-top: 12px;
          border-top: 1px solid var(--border-color);
        }

        .dismiss-dialog input {
          width: 100%;
          padding: 8px 12px;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          background: var(--bg-primary);
          color: var(--text-primary);
          margin-bottom: 8px;
        }

        .dismiss-actions {
          display: flex;
          gap: 8px;
        }

        .works-table {
          width: 100%;
          border-collapse: collapse;
        }

        .works-table th,
        .works-table td {
          padding: 12px;
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        .works-table th {
          background: var(--bg-secondary);
          font-weight: 600;
        }

        .title-cell {
          max-width: 400px;
        }

        .work-title {
          display: block;
        }

        .original-title {
          display: block;
          font-size: 0.85em;
          color: var(--text-secondary);
          font-style: italic;
        }

        .number-cell {
          font-family: var(--font-mono);
        }

        .status-badge {
          display: inline-block;
          padding: 4px 8px;
          border-radius: 4px;
          font-size: 0.8em;
        }

        .status-badge.status-pending {
          background: var(--warning-bg);
          color: var(--warning-color);
        }

        .status-badge.status-running {
          background: var(--info-bg);
          color: var(--info-color);
        }

        .status-badge.status-completed {
          background: var(--success-bg);
          color: var(--success-color);
        }

        .status-badge.status-failed {
          background: var(--danger-bg);
          color: var(--danger-color);
        }

        .bibliography-content {
          max-width: 800px;
        }

        .bibliography-header {
          margin-bottom: 24px;
        }

        .bibliography-note {
          color: var(--text-secondary);
          font-size: 0.9em;
        }

        .bibliography-works {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .bibliography-work {
          display: flex;
          gap: 16px;
          padding: 16px;
          background: var(--bg-secondary);
          border-radius: 8px;
        }

        .work-number {
          font-weight: 600;
          color: var(--text-secondary);
          min-width: 40px;
        }

        .work-info {
          flex: 1;
        }

        .work-meta {
          display: flex;
          gap: 16px;
          margin-top: 8px;
          font-size: 0.85em;
          color: var(--text-secondary);
        }

        .importance {
          padding: 2px 6px;
          border-radius: 4px;
          font-size: 0.9em;
        }

        .importance-major {
          background: var(--warning-bg);
          color: var(--warning-color);
        }

        .importance-significant {
          background: var(--info-bg);
          color: var(--info-color);
        }

        .runs-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .run-card {
          display: flex;
          flex-direction: column;
          gap: 8px;
          padding: 16px;
          background: var(--bg-secondary);
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.15s;
          border: 2px solid transparent;
        }

        .run-card:hover {
          border-color: var(--border-color);
        }

        .run-card.selected {
          border-color: var(--primary-color);
        }

        .run-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .run-id {
          font-weight: 600;
        }

        .run-stats {
          display: flex;
          gap: 16px;
          font-size: 0.9em;
          color: var(--text-secondary);
        }

        .run-date {
          font-size: 0.85em;
          color: var(--text-muted);
        }

        .llm-calls-list {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .llm-call-card {
          background: var(--bg-secondary);
          border-radius: 8px;
          padding: 16px;
        }

        .call-header {
          display: flex;
          gap: 12px;
          margin-bottom: 12px;
          flex-wrap: wrap;
        }

        .call-type {
          font-weight: 600;
        }

        .call-model {
          padding: 2px 8px;
          background: var(--bg-primary);
          border-radius: 4px;
          font-size: 0.85em;
        }

        .call-tokens {
          font-family: var(--font-mono);
          font-size: 0.85em;
          color: var(--text-secondary);
        }

        .call-prompt, .call-response {
          margin-bottom: 8px;
          font-size: 0.9em;
        }

        .call-timestamp {
          font-size: 0.8em;
          color: var(--text-muted);
        }

        .btn {
          padding: 8px 16px;
          border: none;
          border-radius: 4px;
          cursor: pointer;
          font-size: 14px;
          transition: all 0.15s;
        }

        .btn-sm {
          padding: 6px 12px;
          font-size: 12px;
        }

        .btn-primary {
          background: var(--primary-color);
          color: white;
        }

        .btn-primary:hover:not(:disabled) {
          opacity: 0.9;
        }

        .btn-secondary {
          background: var(--bg-secondary);
          color: var(--text-primary);
          border: 1px solid var(--border-color);
        }

        .btn-secondary:hover:not(:disabled) {
          background: var(--bg-primary);
        }

        .btn-ghost {
          background: transparent;
          color: var(--text-secondary);
        }

        .btn-ghost:hover:not(:disabled) {
          color: var(--text-primary);
        }

        .btn-danger {
          background: var(--danger-color);
          color: white;
        }

        .btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        .loading {
          text-align: center;
          padding: 40px;
          color: var(--text-secondary);
        }

        .error-state {
          text-align: center;
          padding: 40px;
          color: var(--danger-color);
        }
      `}</style>
    </div>
  )
}

export default EditionAnalysis
