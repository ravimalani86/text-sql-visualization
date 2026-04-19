# Backend Python Skills Guide

This file is a practical playbook for working inside `backend-python` of the Text-to-SQL app.

## 1) Project Snapshot

- Framework: FastAPI (`app.main:create_app`)
- DB: PostgreSQL via SQLAlchemy engine
- AI: OpenAI Responses API through `app/services/openai_client.py`
- Prompt templates: `app/templates/*.j2`
- Main behavior:
  - Natural language to SQL (`analyze`)
  - SQL execution with safety validation (`SELECT` / `WITH` only)
  - Optional chart intent + Chart.js config generation (stored in `plotly` for compatibility)
  - Conversation history persistence
  - Dashboard pinning for charts/tables

Key code layout:

- `src/app/main.py`: app boot, CORS, router wiring, startup table init
- `src/app/api/routes/`: HTTP routes
- `src/app/services/`: AI + SQL + response orchestration
- `src/app/repositories/`: DB persistence layer
- `src/app/core/config.py`: environment-driven runtime settings
- `src/app/templates/`: system prompts for selector/planner/sql/chart/conversation

## 2) Runtime Requirements

Python packages from `requirements.txt`:

- `fastapi`, `uvicorn`
- `SQLAlchemy`, `psycopg[binary]`
- `openai`, `Jinja2`
- `pandas`, `openpyxl`, `reportlab`
- `python-multipart`, `pydantic`

Required environment variables:

- `DATABASE_URL` (required)
- `OPENAI_API_KEY` (required)

Optional but important:

- `OPENAI_MODEL` (default: `gpt-5`)
- `MAX_RESULT_ROWS` (default: `500`)
- `DEFAULT_PAGE_SIZE` (default: `10`)
- `MAX_TURNS_IN_CONVERSATION` (default: `2`)
- `SCHEMA_CACHE_TTL_SECONDS` (default: `300`)
- `SCHEMA_SEARCH_MAX_TABLES` (default: `8`)
- `ENABLE_SQL_PLANNING` (default: `true`)
- `TEXT_TO_SQL_MAX_CORRECTION_RETRIES` (default: `2`)
- `REUSE_SQL_FROM_HISTORY_BY_PROMPT` (default: `true`)

Startup behavior:

- Initializes conversation tables (`init_history_tables`)
- Initializes pinned dashboard table (`init_pinned_dashboard_table`)

## 3) Core Workflow (Analyze Path)

Primary route: `POST /analyze/` (or streaming `POST /analyze/stream`)

High-level pipeline in `app/api/routes/analyze.py`:

1. Validate prompt, create/use conversation ID
2. Load recent successful turns for context
3. Build effective prompt (`prompt_context`)
4. Intent classification:
   - `CONVERSATION` -> direct AI assistant reply
   - data query path -> schema/SQL flow
5. Optional prompt-history SQL reuse (if enabled)
6. If chart-only request and last result exists, reuse previous SQL result
7. Else generate fresh query:
   - get cached schema
   - select relevant tables (`schema_selector`)
   - optional SQL plan (`sql_planner`)
   - generate SQL (`sql_generator.text_to_sql`)
   - validate SQL safety (`normalize_and_validate_sql`)
   - execute query (`sql_runtime`)
   - correction loop (`correct_sql`) on execution failure
8. Generate chart intent (`chart_intent_ai`) and figure (`plotly_mapper`) when needed
9. Build assistant text + response blocks
10. Persist turn success/failure into history

Streaming path (`/analyze/stream`) emits NDJSON stage events like:

- `intent_classified`, `searching`, `planning`, `generating`, `correcting`
- `query_executed`, `chart_intent_ready`, `chart_ready`, `assistant_ready`
- final payload event or error event

## 4) Async Ask Workflow

Routes in `app/api/routes/asks.py`:

- `POST /v1/asks`: create background analysis job
- `GET /v1/asks/{query_id}/result`: poll progressive status/result
- `PATCH /v1/asks/{query_id}` with `{"status":"stopped"}`: mark job stopped

In-memory job store features:

- stage to status mapping (`understanding/searching/planning/generating/...`)
- stores preview rows, sql, chart intent, plotly (Chart.js config), retrieved tables
- TTL cleanup (30 minutes)

## 5) API Surface Reference

Analysis + conversation:

- `POST /analyze/`
- `POST /analyze/stream`
- `GET /history/conversations`
- `GET /history/{conversation_id}`

Upload + table browser:

- `POST /upload-csv/`
- `GET /api/table-browser/tables`
- `GET /api/table-browser/rows?table_name=...`
- `DELETE /api/table-browser/record`
- `DELETE /api/table-browser/table`
- `GET /clear-all-tables/`

Pinned charts:

- `GET /api/charts`
- `POST /api/charts/pin`
- `POST /api/charts/{chart_id}/refresh`
- `PATCH /api/charts/{chart_id}/layout`
- `DELETE /api/charts/{chart_id}`

Pinned tables:

- `GET /api/tables`
- `POST /api/tables/pin`
- `POST /api/tables/{table_id}/refresh`
- `POST /api/tables/{table_id}/data`
- `PATCH /api/tables/{table_id}/layout`
- `DELETE /api/tables/{table_id}`

Turn table pagination:

- `POST /api/table-data` (paginated/filter/sort/search by `turn_id`)

Follow-up seeding:

- `POST /api/followup/seed` (`type=chart|table`, `id=...`)

Exports:

- `GET /api/export?turn_id=...&format=csv|xlsx|pdf&max_rows=...`

## 6) Data Model Notes

Conversation storage (`history_repo`):

- `conversations`
- `conversation_turns`
  - stores prompt, context_prompt, SQL, columns, data, chart_intent, plotly, response blocks, status, total_count
  - includes normalized prompt index for prompt-cache reuse

Pinned dashboard storage (`pinned_dashboard_repo`):

- single `pinned_dashboard` table
- `item_type` in `chart | table`
- layout metadata: `sort_order`, `width_units`, `height_px`
- chart metadata: `chart_type`, `x_field`, `y_field`, `series_field`

## 7) Safety + Validation Rules

- Only `SELECT` or `WITH` SQL allowed by `normalize_and_validate_sql`
- Query execution capped by `MAX_RESULT_ROWS`
- Table pagination/filtering uses wrapped SQL (`SELECT * FROM (<base_sql>) _t ...`)
- Sort/filter/search constrained to known columns where available

## 8) Development Workflow

Run locally:

```bash
cd backend-python
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

When adding/changing API behavior:

1. Update/verify route request/response schema
2. Keep repository methods thin and deterministic
3. Keep SQL safety checks in place (do not bypass `normalize_and_validate_sql`)
4. Ensure turn persistence on both success and error paths
5. Validate pagination/sorting/filtering with realistic large-result SQL

When changing AI prompting:

1. Update relevant `templates/*.j2`
2. Confirm parser assumptions in service layer still hold
3. Test both normal and correction-loop scenarios

## 9) Debugging Checklist

- Startup fails immediately:
  - check `DATABASE_URL`, `OPENAI_API_KEY`
- Empty/invalid SQL:
  - inspect `sql_generated` and `correcting` stages
- Unexpected chart not rendered:
  - inspect `chart_intent.make_chart`, axis fields, and figure payload
- Slow responses:
  - check schema size, `SCHEMA_SEARCH_MAX_TABLES`, and prompt-cache hit ratio
- Wrong pagination totals:
  - verify base SQL stability and active filters/search criteria
