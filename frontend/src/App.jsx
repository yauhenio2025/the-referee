import { useState } from 'react'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { api } from './lib/api'
import './App.css'

// Components
import PaperInput from './components/PaperInput'
import PaperList from './components/PaperList'
import EditionDiscovery from './components/EditionDiscovery'
import JobQueue from './components/JobQueue'
import Stats from './components/Stats'

const queryClient = new QueryClient()

function AppContent() {
  const [activeTab, setActiveTab] = useState('papers')
  const [selectedPaper, setSelectedPaper] = useState(null)

  const { data: stats, refetch: refetchStats } = useQuery({
    queryKey: ['stats'],
    queryFn: () => api.getStats(),
    refetchInterval: 10000,
  })

  return (
    <div className="app">
      <header className="header">
        <div className="header-content">
          <h1 className="logo">
            <span className="logo-icon">⚖️</span>
            The Referee
          </h1>
          <p className="tagline">Citation Analysis Engine</p>
        </div>
        <Stats stats={stats} />
      </header>

      <nav className="tabs">
        <button
          className={`tab ${activeTab === 'papers' ? 'active' : ''}`}
          onClick={() => setActiveTab('papers')}
        >
          📚 Papers
        </button>
        <button
          className={`tab ${activeTab === 'editions' ? 'active' : ''}`}
          onClick={() => setActiveTab('editions')}
          disabled={!selectedPaper}
        >
          📖 Editions {selectedPaper && `(${selectedPaper.title.slice(0, 30)}...)`}
        </button>
        <button
          className={`tab ${activeTab === 'jobs' ? 'active' : ''}`}
          onClick={() => setActiveTab('jobs')}
        >
          ⚙️ Jobs {stats?.jobs?.running > 0 && <span className="badge">{stats.jobs.running}</span>}
        </button>
      </nav>

      <main className="main">
        {activeTab === 'papers' && (
          <div className="papers-view">
            <PaperInput onPaperAdded={() => refetchStats()} />
            <PaperList
              onSelectPaper={(paper) => {
                setSelectedPaper(paper)
                setActiveTab('editions')
              }}
            />
          </div>
        )}

        {activeTab === 'editions' && selectedPaper && (
          <EditionDiscovery
            paper={selectedPaper}
            onBack={() => setActiveTab('papers')}
          />
        )}

        {activeTab === 'jobs' && (
          <JobQueue />
        )}
      </main>

      <footer className="footer">
        <p>The Referee v1.0 | Citation Analysis API</p>
      </footer>
    </div>
  )
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AppContent />
    </QueryClientProvider>
  )
}

export default App
