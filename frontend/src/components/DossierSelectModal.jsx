/**
 * DossierSelectModal - Modal for selecting dossier(s) when adding seeds
 *
 * This component allows users to:
 * - Select one or more dossiers from dropdowns
 * - Create new dossiers on the fly
 * - See the collection hierarchy
 * - Remember last selection via localStorage
 * - Add to multiple dossiers at once (additional dossiers default to same collection)
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

// Create empty selection object
function createEmptySelection(defaultCollectionId = null) {
  return {
    collectionId: defaultCollectionId,
    dossierId: null,
    isCreatingDossier: false,
    newDossierName: '',
  }
}

// Single dossier selector row component
function DossierSelector({
  index,
  selection,
  collections,
  collectionsLoading,
  onUpdate,
  onRemove,
  canRemove,
  queryClient,
}) {
  const { collectionId, dossierId, isCreatingDossier, newDossierName } = selection

  // Fetch dossiers for this selector's collection
  const { data: dossiers = [], isLoading: dossiersLoading } = useQuery({
    queryKey: ['dossiers', collectionId],
    queryFn: () => api.getDossiers(collectionId),
    enabled: !!collectionId,
  })

  // Create dossier mutation
  const createDossier = useMutation({
    mutationFn: (dossierData) => api.createDossier(dossierData),
    onSuccess: (newDossier) => {
      queryClient.invalidateQueries(['dossiers', collectionId])
      onUpdate({
        ...selection,
        dossierId: newDossier.id,
        isCreatingDossier: false,
        newDossierName: '',
      })
    },
  })

  const handleCollectionChange = (newCollectionId) => {
    onUpdate({
      ...selection,
      collectionId: newCollectionId,
      dossierId: null, // Reset dossier when collection changes
    })
  }

  const handleDossierChange = (newDossierId) => {
    onUpdate({
      ...selection,
      dossierId: newDossierId,
    })
  }

  const handleToggleCreate = () => {
    onUpdate({
      ...selection,
      isCreatingDossier: !isCreatingDossier,
      newDossierName: '',
    })
  }

  const handleNewDossierNameChange = (name) => {
    onUpdate({
      ...selection,
      newDossierName: name,
    })
  }

  const handleCreateDossier = () => {
    if (!newDossierName.trim() || !collectionId) return
    createDossier.mutate({
      name: newDossierName.trim(),
      collection_id: collectionId,
    })
  }

  return (
    <div className="dossier-selector-row">
      <div className="selector-header">
        <span className="selector-label">
          {index === 0 ? 'Primary Dossier' : `Additional Dossier ${index}`}
        </span>
        {canRemove && (
          <button
            type="button"
            className="btn-remove-selector"
            onClick={onRemove}
            title="Remove this dossier"
          >
            ✕
          </button>
        )}
      </div>

      <div className="form-group">
        <label>Collection</label>
        {collectionsLoading ? (
          <div className="loading-text">Loading collections...</div>
        ) : collections.length === 0 ? (
          <div className="empty-text">No collections found. Create one first.</div>
        ) : (
          <select
            value={collectionId || ''}
            onChange={(e) => handleCollectionChange(e.target.value ? parseInt(e.target.value) : null)}
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

      {collectionId && (
        <div className="form-group">
          <label>
            Dossier
            <button
              type="button"
              className="btn-link"
              onClick={handleToggleCreate}
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
                onChange={(e) => handleNewDossierNameChange(e.target.value)}
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
                onClick={handleToggleCreate}
              >
                Create one
              </button>
            </div>
          ) : (
            <select
              value={dossierId || ''}
              onChange={(e) => handleDossierChange(e.target.value ? parseInt(e.target.value) : null)}
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
    </div>
  )
}

export default function DossierSelectModal({
  isOpen,
  onClose,
  onSelect,
  defaultCollectionId = null,
  defaultDossierId = null,
  title = 'Select Dossier',
  subtitle = 'Choose where to add this seed paper',
  allowMultiple = true, // New prop to enable/disable multi-dossier selection
}) {
  const queryClient = useQueryClient()
  const [selections, setSelections] = useState([createEmptySelection(defaultCollectionId)])

  // Fetch collections (shared across all selectors)
  const { data: collections = [], isLoading: collectionsLoading } = useQuery({
    queryKey: ['collections'],
    queryFn: () => api.getCollections(),
    enabled: isOpen,
  })

  // Initialize with defaults or last selection when modal opens
  useEffect(() => {
    if (isOpen) {
      let initialCollectionId = defaultCollectionId
      let initialDossierId = defaultDossierId

      // Use provided defaults first, otherwise fall back to last selection from localStorage
      if (!initialCollectionId) {
        const lastSelection = getLastSelection()
        initialCollectionId = lastSelection.collectionId
        initialDossierId = lastSelection.dossierId
      }

      setSelections([{
        collectionId: initialCollectionId,
        dossierId: initialDossierId,
        isCreatingDossier: false,
        newDossierName: '',
      }])
    }
  }, [isOpen, defaultCollectionId, defaultDossierId])

  if (!isOpen) return null

  const updateSelection = (index, updatedSelection) => {
    setSelections(prev => {
      const newSelections = [...prev]
      newSelections[index] = updatedSelection
      return newSelections
    })
  }

  const addSelection = () => {
    // Default new selection to the same collection as the first selection
    const firstCollectionId = selections[0]?.collectionId || null
    setSelections(prev => [...prev, createEmptySelection(firstCollectionId)])
  }

  const removeSelection = (index) => {
    if (selections.length <= 1) return
    setSelections(prev => prev.filter((_, i) => i !== index))
  }

  const handleConfirm = () => {
    // Build array of valid selections
    const validSelections = selections
      .map(sel => {
        if (sel.isCreatingDossier && sel.newDossierName.trim() && sel.collectionId) {
          return {
            createNewDossier: true,
            newDossierName: sel.newDossierName.trim(),
            collectionId: sel.collectionId,
          }
        } else if (sel.dossierId) {
          return {
            dossierId: sel.dossierId,
            collectionId: sel.collectionId,
          }
        } else if (sel.collectionId) {
          return {
            collectionId: sel.collectionId,
          }
        }
        return null
      })
      .filter(Boolean)

    // Save first selection to localStorage for next time
    if (validSelections.length > 0 && validSelections[0].collectionId) {
      saveLastSelection(validSelections[0].collectionId, validSelections[0].dossierId || null)
    }

    // Return array of selections (even if single, for consistent API)
    onSelect(validSelections)
    onClose()
  }

  // Check if at least one selection is valid
  const hasValidSelection = selections.some(sel =>
    sel.collectionId || sel.dossierId || (sel.isCreatingDossier && sel.newDossierName.trim())
  )

  // Count how many dossiers will be created
  const createCount = selections.filter(sel => sel.isCreatingDossier && sel.newDossierName.trim()).length
  const selectCount = selections.filter(sel => sel.dossierId).length
  const totalCount = createCount + selectCount

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal compact multi-dossier-modal" onClick={e => e.stopPropagation()}>
        <h2>{title}</h2>
        <p className="modal-subtitle">{subtitle}</p>

        <div className="dossier-selectors">
          {selections.map((selection, index) => (
            <DossierSelector
              key={index}
              index={index}
              selection={selection}
              collections={collections}
              collectionsLoading={collectionsLoading}
              onUpdate={(updated) => updateSelection(index, updated)}
              onRemove={() => removeSelection(index)}
              canRemove={index > 0}
              queryClient={queryClient}
            />
          ))}
        </div>

        {allowMultiple && (
          <button
            type="button"
            className="btn-add-dossier"
            onClick={addSelection}
          >
            + Add to another dossier
          </button>
        )}

        <div className="modal-footer">
          <button onClick={onClose}>Cancel</button>
          <button
            className="btn-primary"
            onClick={handleConfirm}
            disabled={!hasValidSelection}
          >
            {createCount > 0
              ? `Create & Add${totalCount > 1 ? ` (${totalCount})` : ''}`
              : `Confirm${totalCount > 1 ? ` (${totalCount})` : ''}`
            }
          </button>
        </div>
      </div>
    </div>
  )
}
