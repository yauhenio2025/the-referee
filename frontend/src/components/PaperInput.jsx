import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperInput({ onPaperAdded }) {
  const queryClient = useQueryClient()

  // Fetch all dossiers for the selector
  const { data: allDossiers = [] } = useQuery({
    queryKey: ['all-dossiers'],
    queryFn: () => api.getDossiers(),
  })

  // Fetch all collections for grouping dossiers
  const { data: collections = [] } = useQuery({
    queryKey: ['collections'],
    queryFn: () => api.getCollections(),
  })

  // Input mode toggle
  const [inputMode, setInputMode] = useState('manual') // 'manual' | 'smart' | 'quick'

  // Manual mode state
  const [title, setTitle] = useState('')
  const [author, setAuthor] = useState('')
  const [year, setYear] = useState('')
  const [publisher, setPublisher] = useState('')

  // Smart paste state
  const [bibliographyText, setBibliographyText] = useState('')
  const [parsing, setParsing] = useState(false)
  const [parsedWorks, setParsedWorks] = useState(null)
  const [parseError, setParseError] = useState(null)

  // Batch add state
  const [addingBatch, setAddingBatch] = useState(false)

  // Quick add state - batch mode with multiple rows
  const INITIAL_ROWS = 10
  const [scholarInputs, setScholarInputs] = useState(Array(INITIAL_ROWS).fill(''))
  const [quickAddLoading, setQuickAddLoading] = useState(false)
  const [quickAddResults, setQuickAddResults] = useState([]) // Array of {input, success, result, error}
  const [startHarvestAfterAdd, setStartHarvestAfterAdd] = useState(true)
  const [bulkPasteText, setBulkPasteText] = useState('')
  const [selectedDossierId, setSelectedDossierId] = useState(null)

  // Create new dossier state
  const [isCreatingDossier, setIsCreatingDossier] = useState(false)
  const [newDossierName, setNewDossierName] = useState('')
  const [newDossierCollectionId, setNewDossierCollectionId] = useState(null)

  // Group dossiers by collection for the dropdown
  const dossiersByCollection = collections.map(col => ({
    collection: col,
    dossiers: allDossiers.filter(d => d.collection_id === col.id)
  }))

  // Create dossier mutation
  const createDossierMutation = useMutation({
    mutationFn: (dossierData) => api.createDossier(dossierData),
    onSuccess: (newDossier) => {
      queryClient.invalidateQueries(['all-dossiers'])
      queryClient.invalidateQueries(['dossiers'])
      setSelectedDossierId(newDossier.id)
      setIsCreatingDossier(false)
      setNewDossierName('')
      setNewDossierCollectionId(null)
    },
  })

  // Handle dossier dropdown change
  const handleDossierChange = (value) => {
    if (value === '__create_new__') {
      setIsCreatingDossier(true)
      setSelectedDossierId(null)
      // Default to first collection if available
      if (collections.length > 0 && !newDossierCollectionId) {
        setNewDossierCollectionId(collections[0].id)
      }
    } else {
      setSelectedDossierId(value ? parseInt(value) : null)
      setIsCreatingDossier(false)
    }
  }

  // Handle creating the new dossier
  const handleCreateDossier = () => {
    if (!newDossierName.trim() || !newDossierCollectionId) return
    createDossierMutation.mutate({
      name: newDossierName.trim(),
      collection_id: newDossierCollectionId,
    })
  }

  // Cancel creating dossier
  const handleCancelCreateDossier = () => {
    setIsCreatingDossier(false)
    setNewDossierName('')
    setNewDossierCollectionId(null)
  }

  const createPaper = useMutation({
    mutationFn: (paper) => api.createPaper(paper),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
      setTitle('')
      setAuthor('')
      setYear('')
      setPublisher('')
      onPaperAdded?.()
    },
  })

  const handleSubmit = (e) => {
    e.preventDefault()
    if (!title.trim()) return

    createPaper.mutate({
      title: title.trim(),
      authors: author.trim() || null,
      year: year ? parseInt(year) : null,
      venue: publisher.trim() || null,
    })
  }

  // Parse bibliography using LLM
  const handleParseBibliography = async () => {
    if (!bibliographyText.trim()) {
      setParseError('Please paste a bibliography to parse')
      return
    }

    setParsing(true)
    setParseError(null)

    try {
      const result = await api.parseBibliography(bibliographyText)
      if (result.success) {
        setParsedWorks(result.parsed)
      } else {
        setParseError(result.error || 'Failed to parse bibliography')
      }
    } catch (err) {
      console.error('Parse error:', err)
      setParseError('Failed to parse bibliography: ' + err.message)
    } finally {
      setParsing(false)
    }
  }

  // Add all parsed works to the database
  const handleAddParsedWorks = async () => {
    if (!parsedWorks?.authors) return

    setAddingBatch(true)
    try {
      // Flatten all works from all authors
      const papers = []
      parsedWorks.authors.forEach(author => {
        (author.works || []).forEach(work => {
          papers.push({
            title: work.title,
            authors: author.name,
            year: work.year ? parseInt(work.year) : null,
            venue: work.publisher || null,
          })
        })
      })

      // Add papers via batch API
      await api.createPapersBatch(papers)

      // Clear state and refresh
      queryClient.invalidateQueries(['papers'])
      setParsedWorks(null)
      setBibliographyText('')
      setInputMode('manual')
      onPaperAdded?.()
    } catch (err) {
      setParseError('Failed to add papers: ' + err.message)
    } finally {
      setAddingBatch(false)
    }
  }

  // Count total works in parsed result
  const totalWorks = parsedWorks?.authors?.reduce((sum, a) => sum + (a.works?.length || 0), 0) || 0

  // Update a single input row
  const updateScholarInput = (index, value) => {
    const newInputs = [...scholarInputs]
    newInputs[index] = value
    setScholarInputs(newInputs)
  }

  // Add 5 more empty rows
  const addMoreRows = () => {
    setScholarInputs([...scholarInputs, ...Array(5).fill('')])
  }

  // Clear all inputs and results
  const clearAllInputs = () => {
    setScholarInputs(Array(INITIAL_ROWS).fill(''))
    setQuickAddResults([])
  }

  // Get non-empty inputs
  const getNonEmptyInputs = () => scholarInputs.filter(s => s.trim())

  // Parse bulk paste text and distribute to input rows
  const handleParseBulkUrls = () => {
    if (!bulkPasteText.trim()) return

    // Smart parse: handle wrapped URLs (lines not starting with http get joined to previous)
    const lines = bulkPasteText.split('\n').map(line => line.trim())
    const urls = []
    let currentUrl = ''

    for (const line of lines) {
      if (!line) continue // skip empty lines

      // Check if this line starts a new URL
      if (line.startsWith('http://') || line.startsWith('https://')) {
        // Save previous URL if any
        if (currentUrl) urls.push(currentUrl)
        currentUrl = line
      } else if (currentUrl) {
        // This is a continuation of the previous URL (wrapped line)
        currentUrl += line
      } else {
        // Standalone line (maybe just an ID) - treat as its own entry
        urls.push(line)
      }
    }
    // Don't forget the last URL
    if (currentUrl) urls.push(currentUrl)

    if (urls.length === 0) return

    // Expand rows if needed
    const neededRows = Math.max(urls.length, INITIAL_ROWS)
    const newInputs = [...urls, ...Array(Math.max(0, neededRows - urls.length)).fill('')]
    setScholarInputs(newInputs)
    setBulkPasteText('')
  }

  // Count URLs in bulk paste (handles wrapped lines)
  const countBulkUrls = () => {
    if (!bulkPasteText.trim()) return 0
    const lines = bulkPasteText.split('\n').map(line => line.trim())
    let count = 0
    let inUrl = false
    for (const line of lines) {
      if (!line) continue
      if (line.startsWith('http://') || line.startsWith('https://')) {
        count++
        inUrl = true
      } else if (!inUrl) {
        // Standalone ID
        count++
      }
      // else: continuation of URL, don't count
    }
    return count
  }

  // Quick add batch - process all non-empty inputs
  const handleQuickAddBatch = async () => {
    const inputs = getNonEmptyInputs()
    if (inputs.length === 0) {
      return
    }

    setQuickAddLoading(true)
    setQuickAddResults([])

    // Process all inputs in parallel
    const results = await Promise.all(
      inputs.map(async (input) => {
        try {
          const result = await api.quickAdd(input.trim(), {
            startHarvest: startHarvestAfterAdd,
            dossierId: selectedDossierId,
          })
          return { input, success: true, result, error: null }
        } catch (err) {
          console.error('Quick add error for', input, ':', err)
          return { input, success: false, result: null, error: err.message || 'Failed to add' }
        }
      })
    )

    setQuickAddResults(results)

    // Clear successful inputs, keep failed ones
    const failedInputs = results.filter(r => !r.success).map(r => r.input)
    const newInputs = [...failedInputs, ...Array(Math.max(INITIAL_ROWS - failedInputs.length, 0)).fill('')]
    setScholarInputs(newInputs)

    // Refresh if any succeeded
    if (results.some(r => r.success)) {
      queryClient.invalidateQueries(['papers'])
      queryClient.invalidateQueries(['dossiers'])
      queryClient.invalidateQueries(['all-dossiers'])
      onPaperAdded?.()
    }

    setQuickAddLoading(false)
  }

  return (
    <div className="paper-input">
      {/* Mode toggle */}
      <div className="input-mode-toggle">
        <button
          type="button"
          className={`mode-btn ${inputMode === 'manual' ? 'active' : ''}`}
          onClick={() => setInputMode('manual')}
        >
          📝 Manual Entry
        </button>
        <button
          type="button"
          className={`mode-btn ${inputMode === 'smart' ? 'active' : ''}`}
          onClick={() => setInputMode('smart')}
        >
          🤖 Smart Paste
        </button>
        <button
          type="button"
          className={`mode-btn ${inputMode === 'quick' ? 'active' : ''}`}
          onClick={() => setInputMode('quick')}
        >
          ⚡ Quick Add
        </button>
      </div>

      {inputMode === 'manual' && (
        /* Manual entry mode */
        <form onSubmit={handleSubmit}>
          <div className="form-row">
            <input
              type="text"
              placeholder="Paper title (e.g., The Eighteenth Brumaire of Louis Bonaparte)"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              className="input-title"
            />
          </div>
          <div className="form-row form-row-split">
            <input
              type="text"
              placeholder="Author (e.g., Karl Marx)"
              value={author}
              onChange={(e) => setAuthor(e.target.value)}
              className="input-author"
            />
            <input
              type="text"
              placeholder="Publisher/Venue"
              value={publisher}
              onChange={(e) => setPublisher(e.target.value)}
              className="input-publisher"
            />
            <input
              type="number"
              placeholder="Year"
              value={year}
              onChange={(e) => setYear(e.target.value)}
              className="input-year"
            />
            <button type="submit" disabled={createPaper.isPending || !title.trim()}>
              {createPaper.isPending ? 'Adding...' : '+ Add Paper'}
            </button>
          </div>
          {createPaper.isError && (
            <div className="error">Error: {createPaper.error.message}</div>
          )}
        </form>
      )}

      {inputMode === 'smart' && (
        /* Smart paste mode */
        <div className="smart-paste">
          {!parsedWorks ? (
            <>
              <p className="smart-paste-hint">
                Paste a bibliography, reading list, or works cited. The AI will extract titles, authors, years, and publishers.
              </p>
              <textarea
                placeholder={`Example:

Group 1: Historical Core
Marx, Karl. The Eighteenth Brumaire of Louis Bonaparte (1852)
Engels, Friedrich. The Condition of the Working Class in England. Penguin, 1845.

Group 2: Contemporary Analysis
Harvey, David. A Brief History of Neoliberalism. Oxford University Press, 2005.`}
                value={bibliographyText}
                onChange={(e) => setBibliographyText(e.target.value)}
                className="bibliography-textarea"
                rows={10}
              />
              <div className="smart-paste-actions">
                <button
                  type="button"
                  onClick={handleParseBibliography}
                  disabled={parsing || !bibliographyText.trim()}
                  className="btn-parse"
                >
                  {parsing ? '🔄 Parsing...' : '🤖 Parse Bibliography'}
                </button>
                {bibliographyText.trim() && (
                  <button
                    type="button"
                    onClick={() => setBibliographyText('')}
                    className="btn-clear"
                  >
                    Clear
                  </button>
                )}
              </div>
              {parseError && <div className="error">{parseError}</div>}
            </>
          ) : (
            /* Show parsed results */
            <div className="parsed-results">
              <h3>📚 Parsed {totalWorks} works from {parsedWorks.authors?.length || 0} authors</h3>
              <div className="parsed-authors">
                {parsedWorks.authors?.map((author, idx) => (
                  <div key={idx} className="parsed-author">
                    <div className="author-header">
                      <strong>{author.name}</strong>
                      {author.group && <span className="author-group">{author.group}</span>}
                      <span className="work-count">{author.works?.length || 0} works</span>
                    </div>
                    <ul className="works-list">
                      {author.works?.map((work, widx) => (
                        <li key={widx}>
                          {work.title}
                          {work.year && <span className="work-year"> ({work.year})</span>}
                          {work.publisher && <span className="work-publisher"> - {work.publisher}</span>}
                        </li>
                      ))}
                    </ul>
                  </div>
                ))}
              </div>
              <div className="parsed-actions">
                <button
                  type="button"
                  onClick={handleAddParsedWorks}
                  disabled={addingBatch}
                  className="btn-primary"
                >
                  {addingBatch ? '⏳ Adding...' : `✅ Add All ${totalWorks} Papers`}
                </button>
                <button
                  type="button"
                  onClick={() => setParsedWorks(null)}
                  className="btn-secondary"
                >
                  ← Back to Edit
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {inputMode === 'quick' && (
        /* Quick add mode - batch paste Google Scholar IDs/URLs */
        <div className="quick-add">
          <p className="quick-add-hint">
            Paste Google Scholar URLs or IDs to quickly add multiple papers with their citation counts.
          </p>

          {/* Bulk paste area */}
          <div className="bulk-paste-area">
            <textarea
              placeholder="Paste multiple URLs here (one per line):

https://scholar.google.com/scholar?cites=13113053921268685298...
https://scholar.google.com/scholar?cites=1456224074819751909...
https://scholar.google.com/scholar?cites=4592822924704713920..."
              value={bulkPasteText}
              onChange={(e) => setBulkPasteText(e.target.value)}
              className="bulk-paste-textarea"
              rows={5}
              disabled={quickAddLoading}
            />
            <div className="bulk-paste-actions">
              <button
                type="button"
                onClick={handleParseBulkUrls}
                disabled={quickAddLoading || !bulkPasteText.trim()}
                className="btn-parse-urls"
              >
                Parse {countBulkUrls() || ''} URLs
              </button>
              {bulkPasteText.trim() && (
                <button
                  type="button"
                  onClick={() => setBulkPasteText('')}
                  className="btn-clear-paste"
                  disabled={quickAddLoading}
                >
                  Clear
                </button>
              )}
            </div>
          </div>

          {/* Multiple input rows */}
          <div className="quick-add-rows">
            {scholarInputs.map((value, idx) => (
              <div key={idx} className="quick-add-row">
                <span className="row-number">{idx + 1}</span>
                <input
                  type="text"
                  placeholder="https://scholar.google.com/scholar?cites=... or just the ID"
                  value={value}
                  onChange={(e) => updateScholarInput(idx, e.target.value)}
                  className="input-scholar"
                  disabled={quickAddLoading}
                />
              </div>
            ))}
          </div>

          {/* Actions row */}
          <div className="quick-add-actions">
            <button
              type="button"
              onClick={addMoreRows}
              disabled={quickAddLoading}
              className="btn-add-rows"
            >
              + Add 5 More Rows
            </button>
            <button
              type="button"
              onClick={clearAllInputs}
              disabled={quickAddLoading}
              className="btn-clear-rows"
            >
              Clear All
            </button>
            <div className="quick-add-spacer" />
            <button
              type="button"
              onClick={handleQuickAddBatch}
              disabled={quickAddLoading || getNonEmptyInputs().length === 0}
              className="btn-quick-add-batch"
            >
              {quickAddLoading
                ? `⏳ Adding ${getNonEmptyInputs().length} papers...`
                : `⚡ Add ${getNonEmptyInputs().length || ''} Paper${getNonEmptyInputs().length !== 1 ? 's' : ''}`}
            </button>
          </div>

          {/* Options row */}
          <div className="quick-add-options-row">
            {/* Dossier selector */}
            <div className="dossier-selector-inline">
              <label htmlFor="quick-add-dossier">Add to dossier:</label>
              {!isCreatingDossier ? (
                <select
                  id="quick-add-dossier"
                  value={selectedDossierId || ''}
                  onChange={(e) => handleDossierChange(e.target.value)}
                  disabled={quickAddLoading}
                  className="dossier-select"
                >
                  <option value="">— No dossier —</option>
                  <option value="__create_new__">+ Create new dossier...</option>
                  {dossiersByCollection.map(group => (
                    <optgroup key={group.collection.id} label={group.collection.name}>
                      {group.dossiers.map(dossier => (
                        <option key={dossier.id} value={dossier.id}>
                          {dossier.name}
                        </option>
                      ))}
                    </optgroup>
                  ))}
                </select>
              ) : (
                <div className="create-dossier-inline">
                  <select
                    value={newDossierCollectionId || ''}
                    onChange={(e) => setNewDossierCollectionId(parseInt(e.target.value))}
                    className="collection-select-small"
                    disabled={createDossierMutation.isPending}
                  >
                    <option value="" disabled>Collection...</option>
                    {collections.map(col => (
                      <option key={col.id} value={col.id}>{col.name}</option>
                    ))}
                  </select>
                  <input
                    type="text"
                    placeholder="Dossier name..."
                    value={newDossierName}
                    onChange={(e) => setNewDossierName(e.target.value)}
                    className="dossier-name-input"
                    disabled={createDossierMutation.isPending}
                    onKeyDown={(e) => e.key === 'Enter' && handleCreateDossier()}
                  />
                  <button
                    type="button"
                    onClick={handleCreateDossier}
                    disabled={!newDossierName.trim() || !newDossierCollectionId || createDossierMutation.isPending}
                    className="btn-create-dossier"
                  >
                    {createDossierMutation.isPending ? '...' : '✓'}
                  </button>
                  <button
                    type="button"
                    onClick={handleCancelCreateDossier}
                    disabled={createDossierMutation.isPending}
                    className="btn-cancel-dossier"
                  >
                    ✕
                  </button>
                </div>
              )}
            </div>

            <label className="quick-add-option">
              <input
                type="checkbox"
                checked={startHarvestAfterAdd}
                onChange={(e) => setStartHarvestAfterAdd(e.target.checked)}
              />
              Start harvesting citations immediately
            </label>
          </div>

          {/* Results display */}
          {quickAddResults.length > 0 && (
            <div className="quick-add-results">
              <h4>
                Results ({quickAddResults.filter(r => r.success).length}/{quickAddResults.length} succeeded)
                {selectedDossierId && allDossiers.find(d => d.id === selectedDossierId) && (
                  <span className="results-dossier-badge">
                    → {allDossiers.find(d => d.id === selectedDossierId)?.name}
                  </span>
                )}
              </h4>
              {quickAddResults.map((r, idx) => (
                <div key={idx} className={`quick-add-result ${r.success ? 'success' : 'error'}`}>
                  {r.success ? (
                    <>
                      <span className="result-icon">✓</span>
                      <span className="result-title">{r.result.title}</span>
                      {r.result.authors && <span className="result-authors"> by {r.result.authors}</span>}
                      {r.result.year && <span className="result-year"> ({r.result.year})</span>}
                      <span className="result-citations">📊 {r.result.citation_count?.toLocaleString() || 0}</span>
                      {r.result.harvest_job_id && <span className="result-harvest">🚀</span>}
                    </>
                  ) : (
                    <>
                      <span className="result-icon">✗</span>
                      <span className="result-input">{r.input.substring(0, 50)}...</span>
                      <span className="result-error">{r.error}</span>
                    </>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
