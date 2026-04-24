# Text-to-SQL App

A full-stack analytics app where users ask questions in natural language and get SQL, table results, and interactive charts.

## Tech Stack

- Backend: FastAPI + SQLAlchemy + PostgreSQL
- AI: Anthropic Claude Messages API for SQL generation, chart intent, and chat replies
- Frontend: Vanilla JavaScript + HTML/CSS
- Charts: Custom Chart.js-based web component in `plotly-chart-wc`
- Containers: Docker Compose

## Project Structure

```text
code/
├─ backend-python/         # FastAPI backend
├─ frontend-javascript/    # Chat + dashboard UI (served by nginx in Docker)
├─ plotly-chart-wc/        # Chart.js web component (Angular build output used by frontend)
├─ docker-compose.yml
└─ .gitignore
```

## Prerequisites

- Docker + Docker Compose (recommended)
- Or for local dev:
  - Python 3.12+
  - Node.js 20+ (for `plotly-chart-wc` build)

## Environment Variables

Create `backend-python/.env` with at least:

```env
DATABASE_URL=postgresql+psycopg2://<db_user>:<db_password>@<db_host>:5432/<db_name>
ANTHROPIC_API_KEY=your_anthropic_api_key_here
ANTHROPIC_MODEL=claude-haiku-x-x-xxxxxx
```

Notes:
- This project expects an external/live PostgreSQL database via `DATABASE_URL`.

## Run with Docker (Recommended)

From the project root:

```bash
docker compose up --build
```

Services:
- Frontend: `http://localhost:3000`
- Backend API: `http://localhost:8000`

## Run Locally (Without Docker)

### 1) Run backend

```bash
cd backend-python
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 2) Build chart web component

```bash
cd ../plotly-chart-wc
npm install
npm run build
```

### 3) Serve frontend files

From project root (or `frontend-javascript`), serve static files on port `3000`.
One simple option:

```bash
python -m http.server 3000
```

Then open:
- `http://localhost:3000/frontend-javascript/index.html`
- `http://localhost:3000/frontend-javascript/dashboard.html`

## Main API Endpoints

- `POST /analyze/` - End-to-end analysis (prompt -> SQL -> results -> optional chart)
- `POST /upload-csv/` - Upload CSV and create table
- `GET /tables/` - List current tables with row counts
- `GET /history/conversations` - List conversation history
- `GET /history/{conversation_id}` - Get one conversation + turns
- `POST /api/charts/pin` - Pin chart
- `GET /api/charts` - List pinned charts
- `POST /api/charts/{chart_id}/refresh` - Re-run pinned chart SQL and rebuild chart
- `PATCH /api/charts/{chart_id}/layout` - Update chart layout metadata
- `DELETE /api/charts/{chart_id}` - Remove pinned chart

## Notes

- Backend currently allows only `SELECT` / `WITH` SQL execution for safety.
- CORS is configured for local frontend ports `3000`.
- Keep `backend-python/.env` private and never commit real API keys.

## MCP Server (Optional)

This repo includes an MCP (Model Context Protocol) **stdio server** that exposes read-only database tools.

PowerShell (from `backend-python/`):

```powershell
$env:PYTHONPATH="src"
python -m app.mcp
```
