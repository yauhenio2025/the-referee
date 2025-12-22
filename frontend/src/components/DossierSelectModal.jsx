/**
 * DossierSelectModal - Modal for selecting a dossier when adding seeds
 *
 * This component allows users to:
 * - Select an existing dossier from a dropdown
 * - Create a new dossier on the fly
 * - See the collection hierarchy
 * - Remember last selection via localStorage
 */
import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import api from '../lib/api'

// localStorage key for remembering last selection
const LAST_SELECTION_KEY = 'referee_last_dossier_selection'

// Get last selection from localStorage
function getLastSelection() {
  try {
    const saved = localStorage.getItem(LAST_SELECTION_KEY)
    if (saved) {
      return JSON.parse(saved)
    }
  } catch (e) {
    console.warn('Failed to load last dossier selection:', e)
  }
  return { collectionId: null, dossierId: null }
}

// Save selection to localStorage
function saveLastSelection(collectionId, dossierId) {
  try {
    localStorage.setItem(LAST_SELECTION_KEY, JSON.stringify({ collectionId, dossierId }))
  } catch (e) {
    console.warn('Failed to save dossier selection:', e)
  }
}

export default function DossierSelectModal({
  isOpen,
  onClose,
  onSelect,
  defaultCollectionId = null,
  defaultDossierId = null,
  title = 'Select Dossier',
  subtitle = 'Choose where to add this seed paper',
}) {
  const queryClient = useQueryClient()
  const [selectedCollectionId, setSelectedCollectionId] = useState(defaultCollectionId)
  const [selectedDossierId, setSelectedDossierId] = useState(defaultDossierId)
  const [isCreatingDossier, setIsCreatingDossier] = useState(false)
  const [newDossierName, setNewDossierName] = useState('')

  // Fetch collections
  const { data: collections = [], isLoading: collectionsLoading } = useQuery({
    queryKey: ['collections'],
    queryFn: () => api.getCollections(),
    enabled: isOpen,
  })

  // Fetch dossiers for selected collection
  const { data: dossiers = [], isLoading: dossiersLoading } = useQuery({
    queryKey: ['dossiers', selectedCollectionId],
    queryFn: () => api.getDossiers(selectedCollectionId),
    enabled: isOpen && !!selectedCollectionId,
  })

  // Create dossier mutation
  const createDossier = useMutation({
    mutationFn: (dossierData) => api.createDossier(dossierData),
    onSuccess: (newDossier) => {
      queryClient.invalidateQueries(['dossiers', selectedCollectionId])
      setSelectedDossierId(newDossier.id)
      setIsCreatingDossier(false)
      setNewDossierName('')
      // Save new dossier as last selection
      saveLastSelection(selectedCollectionId, newDossier.id)
    },
  })

  // Reset when collection changes
  useEffect(() => {
    if (selectedCollectionId !== defaultCollectionId) {
      setSelectedDossierId(null)
    }
  }, [selectedCollectionId, defaultCollectionId])

  // Initialize with defaults or last selection when modal opens
  useEffect(() => {
    if (isOpen) {
      // Use provided defaults first, otherwise fall back to last selection from localStorage
      if (defaultCollectionId) {
        setSelectedCollectionId(defaultCollectionId)
        setSelectedDossierId(defaultDossierId)
      } else {
        const lastSelection = getLastSelection()
        setSelectedCollectionId(lastSelection.collectionId)
        setSelectedDossierId(lastSelection.dossierId)
      }
    }
  }, [isOpen, defaultCollectionId, defaultDossierId])

  if (!isOpen) return null

  const handleConfirm = () => {
    // Save selection to localStorage for next time
    if (selectedCollectionId) {
      saveLastSelection(selectedCollectionId, selectedDossierId)
    }

    if (isCreatingDossier && newDossierName.trim() && selectedCollectionId) {
      // Create new dossier, then select
      onSelect({
        createNewDossier: true,
        newDossierName: newDossierName.trim(),
        collectionId: selectedCollectionId,
      })
    } else if (selectedDossierId) {
      onSelect({
        dossierId: selectedDossierId,
        collectionId: selectedCollectionId,
      })
    } else if (selectedCollectionId) {
      // Just collection, no dossier
      onSelect({
        collectionId: selectedCollectionId,
      })
    } else {
      // No selection
      onSelect({})
    }
    onClose()
  }

  const handleCreateDossier = async () => {
    if (!newDossierName.trim() || !selectedCollectionId) return
    createDossier.mutate({
      name: newDossierName.trim(),
      collection_id: selectedCollectionId,
    })
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal compact" onClick={e => e.stopPropagation()}>
        <h2>{title}</h2>
        <p className="modal-subtitle">{subtitle}</p>

        <div className="form-group">
          <label>Collection</label>
          {collectionsLoading ? (
            <div className="loading-text">Loading collections...</div>
          ) : collections.length === 0 ? (
            <div className="empty-text">No collections found. Create one first.</div>
          ) : (
            <select
              value={selectedCollectionId || ''}
              onChange={(e) => setSelectedCollectionId(e.target.value ? parseInt(e.target.value) : null)}
            >
              <option value="">-- Select Collection --</option>
              {collections.map(c => (
                <option key={c.id} value={c.id}>
                  {c.name} ({c.paper_count} papers)
                </option>
              ))}
            </select>
          )}
        </div>

        {selectedCollectionId && (
          <div className="form-group">
            <label>
              Dossier
              <button
                type="button"
                className="btn-link"
                onClick={() => setIsCreatingDossier(!isCreatingDossier)}
              >
                {isCreatingDossier ? 'Select Existing' : '+ New Dossier'}
              </button>
            </label>

            {isCreatingDossier ? (
              <div className="inline-create">
                <input
                  type="text"
                  placeholder="New dossier name..."
                  value={newDossierName}
                  onChange={(e) => setNewDossierName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleCreateDossier()
                  }}
                />
              </div>
            ) : dossiersLoading ? (
              <div className="loading-text">Loading dossiers...</div>
            ) : dossiers.length === 0 ? (
              <div className="empty-text">
                No dossiers in this collection.
                <button
                  type="button"
                  className="btn-link"
                  onClick={() => setIsCreatingDossier(true)}
                >
                  Create one
                </button>
              </div>
            ) : (
              <select
                value={selectedDossierId || ''}
                onChange={(e) => setSelectedDossierId(e.target.value ? parseInt(e.target.value) : null)}
              >
                <option value="">-- Select Dossier --</option>
                {dossiers.map(d => (
                  <option key={d.id} value={d.id}>
                    {d.name} ({d.paper_count} papers)
                  </option>
                ))}
              </select>
            )}
          </div>
        )}

        <div className="modal-footer">
          <button onClick={onClose}>Cancel</button>
          <button
            className="btn-primary"
            onClick={handleConfirm}
            disabled={!selectedCollectionId && !selectedDossierId}
          >
            {isCreatingDossier ? 'Create & Add' : 'Confirm'}
          </button>
        </div>
      </div>
    </div>
  )
}
