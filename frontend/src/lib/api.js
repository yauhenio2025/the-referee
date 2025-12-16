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

  async getPaper(paperId) {
    return this.request(`/api/papers/${paperId}`);
  }

  async deletePaper(paperId) {
    return this.request(`/api/papers/${paperId}`, { method: 'DELETE' });
  }

  async resolvePaper(paperId) {
    return this.request(`/api/papers/${paperId}/resolve`, { method: 'POST' });
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

  async selectEditions(editionIds, selected = true) {
    return this.request('/api/editions/select', {
      method: 'POST',
      body: {
        edition_ids: editionIds,
        selected,
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
        max_citations_threshold: options.maxCitationsThreshold || 10000,
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
}

export const api = new RefereeAPI();
export default api;
