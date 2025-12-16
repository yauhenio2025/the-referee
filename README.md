# The Referee

**Citation Analysis Engine** - A tool for discovering all editions and translations of academic papers and analyzing their citation patterns across languages.

## Features

- **Paper Resolution**: Find canonical versions of academic papers via Google Scholar
- **Edition Discovery**: Discover all editions and translations across multiple languages
- **Citation Extraction**: Extract and analyze citation counts for each edition
- **Multi-language Support**: Search in Latin, Cyrillic, Arabic, Hebrew, Greek, CJK scripts
- **Background Processing**: Queue-based job processing for large-scale analysis
- **API Access**: RESTful API for integration with other tools

## Architecture

- **Backend**: FastAPI + SQLAlchemy + PostgreSQL + Celery/Redis
- **Frontend**: React + Vite + TanStack Query
- **Deployment**: Render (Web Service + PostgreSQL)

## Development Setup

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your database URL and API keys

# Run development server
python run.py
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

### Environment Variables

Backend requires:
- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string (for Celery)
- `ANTHROPIC_API_KEY`: For LLM-based edition discovery
- `SECRET_KEY`: Application secret key

## API Endpoints

- `POST /api/papers` - Submit a paper for analysis
- `GET /api/papers` - List all papers
- `GET /api/papers/{id}` - Get paper details
- `POST /api/papers/{id}/discover` - Start edition discovery
- `GET /api/papers/{id}/editions` - Get discovered editions
- `POST /api/papers/{id}/extract` - Start citation extraction
- `GET /api/jobs` - List background jobs
- `GET /api/languages` - Get available languages

## License

MIT
