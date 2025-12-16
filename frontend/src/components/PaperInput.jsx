import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'

export default function PaperInput({ onPaperAdded }) {
  const [title, setTitle] = useState('')
  const [author, setAuthor] = useState('')
  const [year, setYear] = useState('')
  const queryClient = useQueryClient()

  const createPaper = useMutation({
    mutationFn: (paper) => api.createPaper(paper),
    onSuccess: () => {
      queryClient.invalidateQueries(['papers'])
      setTitle('')
      setAuthor('')
      setYear('')
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
    })
  }

  return (
    <div className="paper-input">
      <h2>Add Paper for Analysis</h2>
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
    </div>
  )
}
