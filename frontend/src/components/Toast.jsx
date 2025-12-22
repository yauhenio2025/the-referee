import { createContext, useContext, useState, useCallback } from 'react'

const ToastContext = createContext(null)

export function ToastProvider({ children }) {
  const [toasts, setToasts] = useState([])

  const addToast = useCallback((message, type = 'info', options = {}) => {
    const { duration = 4000, action, actionLabel = 'Undo' } = options
    const id = Date.now() + Math.random()
    setToasts(prev => [...prev, { id, message, type, action, actionLabel }])

    if (duration > 0) {
      setTimeout(() => {
        setToasts(prev => prev.filter(t => t.id !== id))
      }, duration)
    }

    return id
  }, [])

  const removeToast = useCallback((id) => {
    setToasts(prev => prev.filter(t => t.id !== id))
  }, [])

  const handleAction = useCallback((toast) => {
    if (toast.action) {
      toast.action()
    }
    removeToast(toast.id)
  }, [removeToast])

  const toast = {
    show: (message, type, options) => addToast(message, type, options),
    success: (message, options) => addToast(message, 'success', options),
    error: (message, options) => addToast(message, 'error', { duration: 6000, ...options }),
    info: (message, options) => addToast(message, 'info', options),
    warning: (message, options) => addToast(message, 'warning', options),
    // Special undo toast with longer duration
    undo: (message, undoFn) => addToast(message, 'info', {
      duration: 8000,
      action: undoFn,
      actionLabel: 'Undo',
    }),
  }

  return (
    <ToastContext.Provider value={toast}>
      {children}
      <div className="toast-container">
        {toasts.map(t => (
          <div key={t.id} className={`toast toast-${t.type} ${t.action ? 'toast-with-action' : ''}`}>
            <span className="toast-icon">
              {t.type === 'success' && '✓'}
              {t.type === 'error' && '✕'}
              {t.type === 'warning' && '⚠'}
              {t.type === 'info' && 'ℹ'}
            </span>
            <span className="toast-message">{t.message}</span>
            {t.action && (
              <button
                className="toast-action"
                onClick={(e) => { e.stopPropagation(); handleAction(t) }}
              >
                {t.actionLabel}
              </button>
            )}
            <button
              className="toast-close"
              onClick={() => removeToast(t.id)}
              aria-label="Close"
            >
              ×
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  )
}

export function useToast() {
  const context = useContext(ToastContext)
  if (!context) {
    throw new Error('useToast must be used within ToastProvider')
  }
  return context
}
