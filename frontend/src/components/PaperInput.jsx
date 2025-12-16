import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperInput({ onPaperAdded }) {
  const queryClient = useQueryClient()

  // Input mode toggle
  const [inputMode, setInputMode] = useState('manual') // 'manual' | 'smart'

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
      </div>

      {inputMode === 'manual' ? (
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
      ) : (
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
    </div>
  )
}
