import { useState, useEffect } from 'react'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { BrowserRouter, Routes, Route, useParams, useNavigate, useLocation } from 'react-router-dom'
import { api } from './lib/api'
import './App.css'

// Components
import PaperInput from './components/PaperInput'
import PaperList from './components/PaperList'
import EditionDiscovery from './components/EditionDiscovery'
import Citations from './components/Citations'
import Collections from './components/Collections'
import CollectionDetail from './components/CollectionDetail'
import JobQueue from './components/JobQueue'
import Stats from './components/Stats'
import ForeignEditionsNeeded from './components/ForeignEditionsNeeded'
import { ToastProvider } from './components/Toast'

const queryClient = new QueryClient()

function AppContent() {
  const [selectedPaper, setSelectedPaper] = useState(null)
  const [theme, setTheme] = useState(() => {
    return localStorage.getItem('referee-theme') || 'dark'
  })
  const navigate = useNavigate()
  const location = useLocation()

  // Apply theme to document
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('referee-theme', theme)
  }, [theme])

  const toggleTheme = () => {
    setTheme(prev => prev === 'dark' ? 'light' : 'dark')
  }

  // Determine active tab from URL
  const getActiveTab = () => {
    if (location.pathname.startsWith('/paper/') && location.pathname.includes('/citations')) return 'citations'
    if (location.pathname.startsWith('/paper/')) return 'editions'
    if (location.pathname === '/collections') return 'collections'
    if (location.pathname.startsWith('/collections/')) return 'collections'
    if (location.pathname === '/foreign-editions') return 'foreign-editions'
    if (location.pathname === '/jobs') return 'jobs'
    return 'papers'
  }
  const activeTab = getActiveTab()

  const { data: stats, refetch: refetchStats } = useQuery({
    queryKey: ['stats'],
    queryFn: () => api.getStats(),
    refetchInterval: 10000,
  })

  const handleSelectPaper = (paper) => {
    setSelectedPaper(paper)
    navigate(`/paper/${paper.id}`)
  }

  const handleBack = () => {
    setSelectedPaper(null)
    navigate('/')
  }

  return (
    <div className="app">
      <header className="header">
        <div className="header-content">
          <h1 className="logo" onClick={() => navigate('/')} style={{ cursor: 'pointer' }}>
            <span className="logo-icon">⚖️</span>
            The Referee
          </h1>
          <p className="tagline">Citation Analysis Engine</p>
        </div>
        <div className="header-right">
          <Stats stats={stats} />
          <button className="theme-toggle" onClick={toggleTheme} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}>
            <span className="theme-icon">{theme === 'dark' ? '☀️' : '🌙'}</span>
          </button>
        </div>
      </header>

      <nav className="tabs">
        <button
          className={`tab ${activeTab === 'papers' ? 'active' : ''}`}
          onClick={() => navigate('/')}
        >
          📚 Papers
        </button>
        <button
          className={`tab ${activeTab === 'collections' ? 'active' : ''}`}
          onClick={() => navigate('/collections')}
        >
          📁 Collections {stats?.collections > 0 && <span className="badge">{stats.collections}</span>}
        </button>
        <button
          className={`tab ${activeTab === 'foreign-editions' ? 'active' : ''}`}
          onClick={() => navigate('/foreign-editions')}
        >
          📕 Foreign Editions
        </button>
        <button
          className={`tab ${activeTab === 'editions' ? 'active' : ''}`}
          onClick={() => selectedPaper && navigate(`/paper/${selectedPaper.id}`)}
          disabled={!selectedPaper && activeTab !== 'editions'}
        >
          📖 Editions {selectedPaper && `(${selectedPaper.title.slice(0, 20)}...)`}
        </button>
        <button
          className={`tab ${activeTab === 'citations' ? 'active' : ''}`}
          onClick={() => selectedPaper && navigate(`/paper/${selectedPaper.id}/citations`)}
          disabled={!selectedPaper && activeTab !== 'citations'}
        >
          🔗 Citations
        </button>
        <button
          className={`tab ${activeTab === 'jobs' ? 'active' : ''}`}
          onClick={() => navigate('/jobs')}
        >
          ⚙️ Jobs {stats?.jobs?.running > 0 && <span className="badge">{stats.jobs.running}</span>}
        </button>
      </nav>

      <main className="main">
        <Routes>
          <Route path="/" element={
            <div className="papers-view">
              <PaperInput onPaperAdded={() => refetchStats()} />
              <PaperList onSelectPaper={handleSelectPaper} />
            </div>
          } />
          <Route path="/collections" element={
            <Collections onSelectCollection={(c) => navigate(`/collections/${c.id}`)} />
          } />
          <Route path="/foreign-editions" element={
            <ForeignEditionsNeeded onSelectPaper={handleSelectPaper} />
          } />
          <Route path="/collections/:collectionId" element={
            <CollectionDetailRoute />
          } />
          <Route path="/paper/:paperId" element={
            <PaperEditionsRoute
              selectedPaper={selectedPaper}
              setSelectedPaper={setSelectedPaper}
              onBack={handleBack}
            />
          } />
          <Route path="/paper/:paperId/citations" element={
            <PaperCitationsRoute
              selectedPaper={selectedPaper}
              setSelectedPaper={setSelectedPaper}
              onBack={() => navigate(`/paper/${selectedPaper?.id || ''}`)}
            />
          } />
          <Route path="/jobs" element={<JobQueue />} />
        </Routes>
      </main>

      <footer className="footer">
        <p>The Referee v1.0 | Citation Analysis API</p>
      </footer>
    </div>
  )
}

// Route component that loads paper by ID from URL
function PaperEditionsRoute({ selectedPaper, setSelectedPaper, onBack }) {
  const { paperId } = useParams()
  const [loading, setLoading] = useState(!selectedPaper || selectedPaper.id !== parseInt(paperId))

  useEffect(() => {
    // If we have the paper already selected and it matches, use it
    if (selectedPaper && selectedPaper.id === parseInt(paperId)) {
      setLoading(false)
      return
    }

    // Otherwise load it from API
    setLoading(true)
    api.getPaper(parseInt(paperId))
      .then(paper => {
        setSelectedPaper(paper)
        setLoading(false)
      })
      .catch(err => {
        console.error('Failed to load paper:', err)
        setLoading(false)
      })
  }, [paperId, selectedPaper, setSelectedPaper])

  if (loading) {
    return <div className="loading">Loading paper...</div>
  }

  if (!selectedPaper) {
    return <div className="error">Paper not found</div>
  }

  return <EditionDiscovery paper={selectedPaper} onBack={onBack} />
}

// Route component that loads paper citations by ID from URL
function PaperCitationsRoute({ selectedPaper, setSelectedPaper, onBack }) {
  const { paperId } = useParams()
  const [loading, setLoading] = useState(!selectedPaper || selectedPaper.id !== parseInt(paperId))

  useEffect(() => {
    if (selectedPaper && selectedPaper.id === parseInt(paperId)) {
      setLoading(false)
      return
    }

    setLoading(true)
    api.getPaper(parseInt(paperId))
      .then(paper => {
        setSelectedPaper(paper)
        setLoading(false)
      })
      .catch(err => {
        console.error('Failed to load paper:', err)
        setLoading(false)
      })
  }, [paperId, selectedPaper, setSelectedPaper])

  if (loading) {
    return <div className="loading">Loading paper...</div>
  }

  if (!selectedPaper) {
    return <div className="error">Paper not found</div>
  }

  return <Citations paper={selectedPaper} onBack={onBack} />
}

// Route component for collection detail
function CollectionDetailRoute() {
  const { collectionId } = useParams()
  const navigate = useNavigate()

  return (
    <CollectionDetail
      collectionId={parseInt(collectionId)}
      onBack={() => navigate('/collections')}
    />
  )
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <ToastProvider>
          <AppContent />
        </ToastProvider>
      </BrowserRouter>
    </QueryClientProvider>
  )
}

export default App
