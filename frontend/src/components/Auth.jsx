import { useState, createContext, useContext, useEffect } from 'react'

const AuthContext = createContext(null)

// Simple hash function for basic obfuscation (not cryptographically secure, but enough for basic protection)
const simpleHash = (str) => {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash
  }
  return hash.toString(36)
}

// Pre-computed hashes
const VALID_USER_HASH = simpleHash('admin')
const VALID_PASS_HASH = simpleHash('@llendE1973')

export function AuthProvider({ children }) {
  const [isAuthenticated, setIsAuthenticated] = useState(() => {
    return localStorage.getItem('referee-auth') === 'true'
  })

  const login = (username, password) => {
    if (simpleHash(username) === VALID_USER_HASH && simpleHash(password) === VALID_PASS_HASH) {
      setIsAuthenticated(true)
      localStorage.setItem('referee-auth', 'true')
      return true
    }
    return false
  }

  const logout = () => {
    setIsAuthenticated(false)
    localStorage.removeItem('referee-auth')
  }

  return (
    <AuthContext.Provider value={{ isAuthenticated, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  return useContext(AuthContext)
}

export function LoginScreen() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const { login } = useAuth()

  const handleSubmit = (e) => {
    e.preventDefault()
    setError('')

    if (!login(username, password)) {
      setError('Invalid credentials')
      setPassword('')
    }
  }

  return (
    <div className="login-screen">
      <div className="login-container">
        <div className="login-header">
          <span className="login-icon">⚖️</span>
          <h1>The Referee</h1>
          <p>Citation Analysis Engine</p>
        </div>

        <form onSubmit={handleSubmit} className="login-form">
          <div className="form-group">
            <label htmlFor="username">Username</label>
            <input
              type="text"
              id="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Enter username"
              autoComplete="username"
              autoFocus
            />
          </div>

          <div className="form-group">
            <label htmlFor="password">Password</label>
            <input
              type="password"
              id="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Enter password"
              autoComplete="current-password"
            />
          </div>

          {error && <div className="login-error">{error}</div>}

          <button type="submit" className="login-button">
            Sign In
          </button>
        </form>
      </div>
    </div>
  )
}

export function RequireAuth({ children }) {
  const { isAuthenticated } = useAuth()

  if (!isAuthenticated) {
    return <LoginScreen />
  }

  return children
}
