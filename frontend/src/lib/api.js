/**
 * The Referee API Client
 */

// Auto-detect API URL based on environment
const getApiBase = () => {
  // Explicit override via env var
  if (import.meta.env.VITE_API_URL) {
    return import.meta.env.VITE_API_URL;
  }
  // Production: use the referee-api service
  if (window.location.hostname.includes('onrender.com') ||
      window.location.hostname.includes('referee')) {
    return 'https://referee-api.onrender.com';
  }
  // Local development
  return 'http://localhost:8000';
};

const API_BASE = getApiBase();

class RefereeAPI {
  constructor() {
    this.baseUrl = API_BASE;
  }

  async request(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const config = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    };

    if (options.body && typeof options.body === 'object') {
      config.body = JSON.stringify(options.body);
    }

    const response = await fetch(url, config);

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}`);
    }

    return response.json();
  }

  // Health
  async health() {
    return this.request('/health');
  }

  // Stats
  async getStats() {
    return this.request('/api/stats');
  }

  // Papers
  async createPaper(paper) {
    return this.request('/api/papers', {
      method: 'POST',
      body: paper,
    });
  }

  async createPapersBatch(papers, options = {}) {
    return this.request('/api/papers/batch', {
      method: 'POST',
      body: {
        papers,
        auto_discover_editions: options.autoDiscoverEditions ?? true,
        language_strategy: options.languageStrategy || 'major_languages',
        custom_languages: options.customLanguages || [],
      },
    });
  }

  async listPapers(params = {}) {
    const query = new URLSearchParams(params).toString();
    return this.request(`/api/papers${query ? `?${query}` : ''}`);
  }

  // Paginated papers list with harvest stats
  async listPapersPaginated(page = 1, perPage = 25, filters = {}) {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
      ...filters
    });
    return this.request(`/api/papers?${params.toString()}`);
  }

  // Batch assign papers to collection
  async batchAssignToCollection(paperIds, collectionId, dossierId = null, options = {}) {
    return this.request('/api/papers/batch-assign-collection', {
      method: 'POST',
      body: {
        paper_ids: paperIds,
        collection_id: collectionId,
        dossier_id: dossierId,
        create_new_dossier: options.createNewDossier ?? false,
        new_dossier_name: options.newDossierName || null,
      },
    });
  }

  // Mark paper as needing foreign edition
  async toggleForeignEditionNeeded(paperId, needed = true) {
    return this.request(`/api/papers/${paperId}/foreign-edition-needed?needed=${needed}`, {
      method: 'POST',
    });
  }

  // Batch mark papers as needing foreign edition
  async batchForeignEditionNeeded(paperIds, needed = true) {
    return this.request('/api/papers/batch-foreign-edition', {
      method: 'POST',
      body: {
        paper_ids: paperIds,
        foreign_edition_needed: needed,
      },
    });
  }

  // List papers needing foreign editions
  async listPapersNeedingForeignEdition(page = 1, perPage = 25) {
    return this.request(`/api/papers/foreign-edition-needed?page=${page}&per_page=${perPage}`);
  }

  async getPaper(paperId) {
    return this.request(`/api/papers/${paperId}`);
  }

  async deletePaper(paperId, permanent = false) {
    return this.request(`/api/papers/${paperId}?permanent=${permanent}`, { method: 'DELETE' });
  }

  async restorePaper(paperId) {
    return this.request(`/api/papers/${paperId}/restore`, { method: 'POST' });
  }

  async resolvePaper(paperId) {
    return this.request(`/api/papers/${paperId}/resolve`, { method: 'POST' });
  }

  async batchResolvePapers(paperIds = []) {
    return this.request('/api/papers/batch-resolve', {
      method: 'POST',
      body: { paper_ids: paperIds },
    });
  }

  async confirmCandidate(paperId, candidateIndex) {
    return this.request(`/api/papers/${paperId}/confirm-candidate`, {
      method: 'POST',
      body: { candidate_index: candidateIndex },
    });
  }

  // Editions
  async discoverEditions(paperId, options = {}) {
    return this.request('/api/editions/discover', {
      method: 'POST',
      body: {
        paper_id: paperId,
        language_strategy: options.languageStrategy || 'major_languages',
        custom_languages: options.customLanguages || [],
      },
    });
  }

  async getPaperEditions(paperId) {
    return this.request(`/api/papers/${paperId}/editions`);
  }

  async clearPaperEditions(paperId) {
    return this.request(`/api/papers/${paperId}/editions`, {
      method: 'DELETE',
    });
  }

  async selectEditions(editionIds, selected = true) {
    return this.request('/api/editions/select', {
      method: 'POST',
      body: {
        edition_ids: editionIds,
        selected,
      },
    });
  }

  async updateEditionConfidence(editionIds, confidence) {
    return this.request('/api/editions/confidence', {
      method: 'POST',
      body: {
        edition_ids: editionIds,
        confidence, // "high", "uncertain", "rejected"
      },
    });
  }

  async excludeEditions(editionIds, excluded = true) {
    return this.request('/api/editions/exclude', {
      method: 'POST',
      body: {
        edition_ids: editionIds,
        excluded,
      },
    });
  }

  async addEditionAsSeed(editionId, options = {}) {
    return this.request(`/api/editions/${editionId}/add-as-seed`, {
      method: 'POST',
      body: {
        exclude_from_current: options.excludeFromCurrent ?? true,
        dossier_id: options.dossierId || null,
        collection_id: options.collectionId || null,
        create_new_dossier: options.createNewDossier ?? false,
        new_dossier_name: options.newDossierName || null,
      },
    });
  }

  async finalizeEditions(paperId) {
    return this.request(`/api/papers/${paperId}/finalize-editions`, {
      method: 'POST',
    });
  }

  async reopenEditions(paperId) {
    return this.request(`/api/papers/${paperId}/reopen-editions`, {
      method: 'POST',
    });
  }

  async fetchMoreInLanguage(paperId, language, maxResults = 50) {
    return this.request('/api/editions/fetch-more', {
      method: 'POST',
      body: {
        paper_id: paperId,
        language: language.toLowerCase(),
        max_results: maxResults,
      },
    });
  }

  // Async version - queues job and returns immediately
  async fetchMoreInLanguageAsync(paperId, language, maxResults = 50) {
    return this.request('/api/editions/fetch-more-async', {
      method: 'POST',
      body: {
        paper_id: paperId,
        language: language.toLowerCase(),
        max_results: maxResults,
      },
    });
  }

  // Manual edition addition with LLM resolution
  async addManualEdition(paperId, inputText, languageHint = null) {
    return this.request('/api/editions/add-manual', {
      method: 'POST',
      body: {
        paper_id: paperId,
        input_text: inputText,
        language_hint: languageHint,
      },
    });
  }

  // Citations
  async extractCitations(paperId, options = {}) {
    return this.request('/api/citations/extract', {
      method: 'POST',
      body: {
        paper_id: paperId,
        edition_ids: options.editionIds || [],
        max_citations_threshold: options.maxCitationsThreshold || 50000,
      },
    });
  }

  async getPaperCitations(paperId, params = {}) {
    const query = new URLSearchParams(params).toString();
    return this.request(`/api/papers/${paperId}/citations${query ? `?${query}` : ''}`);
  }

  async getCrossCitations(paperId, minIntersection = 2) {
    return this.request(`/api/papers/${paperId}/cross-citations?min_intersection=${minIntersection}`);
  }

  async markCitationsReviewed(citationIds, reviewed = true) {
    return this.request('/api/citations/mark-reviewed', {
      method: 'POST',
      body: {
        citation_ids: citationIds,
        reviewed,
      },
    });
  }

  // Jobs
  async listJobs(params = {}) {
    const query = new URLSearchParams(params).toString();
    return this.request(`/api/jobs${query ? `?${query}` : ''}`);
  }

  async getJob(jobId) {
    return this.request(`/api/jobs/${jobId}`);
  }

  async cancelJob(jobId) {
    return this.request(`/api/jobs/${jobId}/cancel`, { method: 'POST' });
  }

  // Languages
  async getAvailableLanguages() {
    return this.request('/api/languages');
  }

  async recommendLanguages(paper) {
    return this.request('/api/languages/recommend', {
      method: 'POST',
      body: {
        title: paper.title,
        author: paper.author || null,
        year: paper.year || null,
      },
    });
  }

  // Smart Parse - Bibliography parsing with LLM
  async parseBibliography(text) {
    return this.request('/api/bibliography/parse', {
      method: 'POST',
      body: { text },
    });
  }

  // Collections
  async getCollections() {
    return this.request('/api/collections');
  }

  async getCollection(collectionId) {
    return this.request(`/api/collections/${collectionId}`);
  }

  async createCollection(collection) {
    return this.request('/api/collections', {
      method: 'POST',
      body: collection,
    });
  }

  async updateCollection(collectionId, updates) {
    return this.request(`/api/collections/${collectionId}`, {
      method: 'PUT',
      body: updates,
    });
  }

  async deleteCollection(collectionId) {
    return this.request(`/api/collections/${collectionId}`, {
      method: 'DELETE',
    });
  }

  async assignPapersToCollection(paperIds, collectionId) {
    return this.request('/api/collections/assign', {
      method: 'POST',
      body: {
        paper_ids: paperIds,
        collection_id: collectionId,
      },
    });
  }

  // ============== Dossiers ==============

  async getDossiers(collectionId = null) {
    const query = collectionId ? `?collection_id=${collectionId}` : '';
    return this.request(`/api/dossiers${query}`);
  }

  async getDossier(dossierId) {
    return this.request(`/api/dossiers/${dossierId}`);
  }

  async createDossier(dossier) {
    return this.request('/api/dossiers', {
      method: 'POST',
      body: dossier,
    });
  }

  async updateDossier(dossierId, updates) {
    return this.request(`/api/dossiers/${dossierId}`, {
      method: 'PUT',
      body: updates,
    });
  }

  async deleteDossier(dossierId) {
    return this.request(`/api/dossiers/${dossierId}`, {
      method: 'DELETE',
    });
  }

  async assignPapersToDossier(paperIds, dossierId) {
    return this.request('/api/dossiers/assign', {
      method: 'POST',
      body: {
        paper_ids: paperIds,
        dossier_id: dossierId,
      },
    });
  }

  // ============== Refresh/Auto-Updater ==============

  async refreshPaper(paperId, options = {}) {
    return this.request(`/api/refresh/paper/${paperId}`, {
      method: 'POST',
      body: {
        force_full_refresh: options.forceFullRefresh ?? false,
        max_citations_per_edition: options.maxCitationsPerEdition ?? 1000,
        skip_threshold: options.skipThreshold ?? 50000,
      },
    });
  }

  async refreshCollection(collectionId, options = {}) {
    return this.request(`/api/refresh/collection/${collectionId}`, {
      method: 'POST',
      body: {
        force_full_refresh: options.forceFullRefresh ?? false,
        max_citations_per_edition: options.maxCitationsPerEdition ?? 1000,
        skip_threshold: options.skipThreshold ?? 50000,
      },
    });
  }

  async refreshGlobal(options = {}) {
    return this.request('/api/refresh/global', {
      method: 'POST',
      body: {
        force_full_refresh: options.forceFullRefresh ?? false,
        max_citations_per_edition: options.maxCitationsPerEdition ?? 1000,
        skip_threshold: options.skipThreshold ?? 50000,
      },
    });
  }

  async getRefreshStatus(batchId) {
    return this.request(`/api/refresh/status?batch_id=${batchId}`);
  }

  async getStalenessReport(collectionId = null) {
    const query = collectionId ? `?collection_id=${collectionId}` : '';
    return this.request(`/api/staleness${query}`);
  }

  // ============== Quick Add (Scholar ID/URL) ==============

  /**
   * Quick-add a paper using Google Scholar ID or URL.
   * Creates both Paper and Edition, ready for harvesting.
   * @param {string} input - Scholar ID or URL containing cites=/cluster=
   * @param {Object} options - { collectionId, dossierId, startHarvest }
   */
  async quickAdd(input, options = {}) {
    return this.request('/api/papers/quick-add', {
      method: 'POST',
      body: {
        input: input,
        collection_id: options.collectionId || null,
        dossier_id: options.dossierId || null,
        start_harvest: options.startHarvest ?? false,
      },
    });
  }

  // ============== Quick Harvest ==============

  async quickHarvest(paperId) {
    return this.request(`/api/papers/${paperId}/quick-harvest`, {
      method: 'POST',
    });
  }

  // ============== Pause/Unpause Harvest ==============

  async pauseHarvest(paperId) {
    return this.request(`/api/papers/${paperId}/pause-harvest`, {
      method: 'POST',
    });
  }

  async unpauseHarvest(paperId) {
    return this.request(`/api/papers/${paperId}/unpause-harvest`, {
      method: 'POST',
    });
  }

  // ============== Edition Harvest ==============

  async harvestEdition(editionId) {
    return this.request(`/api/editions/${editionId}/harvest`, {
      method: 'POST',
    });
  }

  // Test partition harvest for a single year (temporary test endpoint)
  async testPartitionHarvest(editionId, year) {
    return this.request('/api/test/partition-harvest', {
      method: 'POST',
      body: {
        edition_id: editionId,
        year: year,
      },
    });
  }

  // Re-harvest overflow years (>1000 citations) for a paper
  async reharvestOverflowYears(paperId, yearStart, yearEnd) {
    return this.request(`/api/papers/${paperId}/reharvest-overflow`, {
      method: 'POST',
      body: {
        paper_id: paperId,
        year_start: yearStart,
        year_end: yearEnd,
      },
    });
  }

  // ============== Multi-Dossier Support ==============

  /**
   * Add a paper to multiple dossiers.
   * First dossier becomes primary, rest are additional.
   * @param {number} paperId - Paper ID
   * @param {number[]} dossierIds - Array of dossier IDs
   */
  async addPaperToDossiers(paperId, dossierIds) {
    return this.request(`/api/papers/${paperId}/add-to-dossiers`, {
      method: 'POST',
      body: { dossier_ids: dossierIds },
    });
  }

  // ============== Harvest Completeness ==============

  /**
   * Get harvest completeness report for an edition.
   * Shows expected vs actual counts per year and any failed fetches.
   */
  async getEditionHarvestCompleteness(editionId) {
    return this.request(`/api/harvest-completeness/edition/${editionId}`);
  }

  /**
   * Get harvest completeness report for a paper (all selected editions).
   */
  async getPaperHarvestCompleteness(paperId) {
    return this.request(`/api/harvest-completeness/paper/${paperId}`);
  }

  /**
   * Get failed page fetches with optional filtering.
   * @param {Object} params - { status, edition_id, limit }
   */
  async getFailedFetches(params = {}) {
    const query = new URLSearchParams(params).toString();
    return this.request(`/api/failed-fetches${query ? `?${query}` : ''}`);
  }

  /**
   * Manually trigger a retry job for pending failed fetches.
   * @param {number} maxRetries - Max failed fetches to retry (default 50)
   */
  async retryFailedFetches(maxRetries = 50) {
    return this.request(`/api/failed-fetches/retry?max_retries=${maxRetries}`, {
      method: 'POST',
    });
  }

  /**
   * Verify and repair harvest gaps for a paper.
   * Checks each year for missing citations and fetches them.
   * @param {number} paperId - Paper ID
   * @param {Object} options - { yearStart, yearEnd, fixGaps }
   */
  async verifyRepairHarvest(paperId, options = {}) {
    return this.request(`/api/papers/${paperId}/verify-repair`, {
      method: 'POST',
      body: {
        year_start: options.yearStart ?? 2025,
        year_end: options.yearEnd ?? 1932,
        fix_gaps: options.fixGaps ?? true,
      },
    });
  }

  // ============== AI Gap Analysis ==============

  /**
   * Analyze harvest gaps for a paper using AI.
   * Returns gaps, recommended fixes, and AI-generated summary.
   * @param {number} paperId - Paper ID
   * @param {number} editionId - Optional edition ID to analyze gaps for specific edition only
   */
  async analyzeHarvestGaps(paperId, editionId = null) {
    const params = editionId ? `?edition_id=${editionId}` : '';
    return this.request(`/api/papers/${paperId}/analyze-gaps${params}`);
  }
}

export const api = new RefereeAPI();
export default api;
