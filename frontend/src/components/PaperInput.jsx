import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperInput({ onPaperAdded }) {
  const queryClient = useQueryClient()

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

          <label className="quick-add-option">
            <input
              type="checkbox"
              checked={startHarvestAfterAdd}
              onChange={(e) => setStartHarvestAfterAdd(e.target.checked)}
            />
            Start harvesting citations immediately
          </label>

          {/* Results display */}
          {quickAddResults.length > 0 && (
            <div className="quick-add-results">
              <h4>Results ({quickAddResults.filter(r => r.success).length}/{quickAddResults.length} succeeded)</h4>
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
