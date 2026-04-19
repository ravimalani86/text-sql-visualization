from __future__ import annotations

import json
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import inspect, text

from app.db.engine import engine
from app.services.sql_runtime import execute_sql, normalize_and_validate_sql


_SUPPORTED_PROTOCOL_VERSIONS = ("2025-03-26", "2024-11-05")


def _jsonrpc_error(*, request_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    resp: Dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "error": err}
    return resp


def _jsonrpc_result(*, request_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _tool_result(*, text_out: str, structured: Any = None, is_error: bool = False) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "content": [{"type": "text", "text": text_out}],
        "isError": bool(is_error),
    }
    if structured is not None:
        result["structuredContent"] = structured
    return result


def _get_editable_tables() -> List[str]:
    # Mirrors the logic in `app/api/routes/upload.py` (table browser).
    from app.api.routes.upload import MANAGED_APP_TABLES  # local import to avoid import cycles at module load

    inspector = inspect(engine)
    all_tables = inspector.get_table_names()
    return [table for table in all_tables if table not in MANAGED_APP_TABLES]


def _list_tables_with_counts() -> List[Dict[str, Any]]:
    tables = _get_editable_tables()
    items: List[Dict[str, Any]] = []
    with engine.connect() as conn:
        for table in tables:
            safe_table = table.replace('"', '""')
            row_count = conn.execute(text(f'SELECT COUNT(*) FROM "{safe_table}"')).scalar() or 0
            items.append({"table": table, "row_count": int(row_count)})
    return items


def _sample_rows(*, table_name: str, limit: int = 50) -> Dict[str, Any]:
    tables = _get_editable_tables()
    if table_name not in tables:
        raise ValueError("Table not found")
    safe_table = table_name.replace('"', '""')
    lim = max(1, min(200, int(limit or 50)))
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT * FROM "{safe_table}" LIMIT :lim'), {"lim": lim})
        columns = list(result.keys())
        rows = [dict(r) for r in result.mappings().all()]
    return {"table": table_name, "columns": columns, "rows": rows, "count": len(rows)}


@dataclass
class _McpServerState:
    protocol_version: str = _SUPPORTED_PROTOCOL_VERSIONS[0]
    initialized: bool = False


def _tools() -> List[Dict[str, Any]]:
    return [
        {
            "name": "tables.list",
            "title": "List database tables",
            "description": "List user tables available in the Tables Browser (includes row counts).",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "tables.sample_rows",
            "title": "Sample table rows",
            "description": "Fetch a small sample of rows from a table for inspection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name"},
                    "limit": {"type": "integer", "description": "Max rows to return (1-200)", "minimum": 1, "maximum": 200},
                },
                "required": ["table_name"],
                "additionalProperties": False,
            },
        },
        {
            "name": "sql.execute",
            "title": "Execute read-only SQL",
            "description": "Execute a SELECT/WITH query and return columns + rows (limited).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query (SELECT/WITH only)"},
                    "max_rows": {"type": "integer", "description": "Max rows to return (1-500)", "minimum": 1, "maximum": 500},
                },
                "required": ["sql"],
                "additionalProperties": False,
            },
        },
    ]


def _handle_initialize(state: _McpServerState, params: Dict[str, Any]) -> Dict[str, Any]:
    requested = str((params or {}).get("protocolVersion") or "").strip()
    if requested in _SUPPORTED_PROTOCOL_VERSIONS:
        state.protocol_version = requested
    else:
        state.protocol_version = _SUPPORTED_PROTOCOL_VERSIONS[0]

    state.initialized = True

    return {
        "protocolVersion": state.protocol_version,
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "ai-analytics-backend", "version": "0.1.0"},
        "instructions": "Tools provide database context (tables + read-only SQL).",
    }


def _handle_tools_list(_: _McpServerState, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    # This server does not paginate currently.
    _ = params
    return {"tools": _tools()}


def _handle_tools_call(_: _McpServerState, params: Dict[str, Any]) -> Dict[str, Any]:
    name = str((params or {}).get("name") or "").strip()
    args = (params or {}).get("arguments")
    if args is None:
        args = {}
    if not isinstance(args, dict):
        return _tool_result(text_out="Invalid tool arguments: must be an object.", is_error=True)

    try:
        if name == "tables.list":
            items = _list_tables_with_counts()
            return _tool_result(text_out=json.dumps(items, indent=2), structured={"tables": items})

        if name == "tables.sample_rows":
            table_name = str(args.get("table_name") or "").strip()
            if not table_name:
                return _tool_result(text_out="Missing required argument: table_name", is_error=True)
            limit = args.get("limit", 50)
            payload = _sample_rows(table_name=table_name, limit=int(limit))
            return _tool_result(text_out=json.dumps(payload, indent=2), structured=payload)

        if name == "sql.execute":
            sql = str(args.get("sql") or "").strip()
            if not sql:
                return _tool_result(text_out="Missing required argument: sql", is_error=True)
            max_rows = int(args.get("max_rows", 100) or 100)
            max_rows = max(1, min(500, max_rows))
            validated = normalize_and_validate_sql(sql)
            cols, rows = execute_sql(engine=engine, sql=validated, max_rows=max_rows)
            payload = {"sql": validated, "columns": cols, "rows": rows, "count": len(rows)}
            return _tool_result(text_out=json.dumps(payload, indent=2), structured=payload)

        return _tool_result(text_out=f"Unknown tool: {name}", is_error=True)
    except Exception as exc:
        return _tool_result(text_out=str(exc) or "Tool error", structured={"error": str(exc)}, is_error=True)


def _dispatch(state: _McpServerState, req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(req, dict):
        return None
    if req.get("jsonrpc") != "2.0":
        # JSON-RPC parse error
        return _jsonrpc_error(request_id=req.get("id"), code=-32600, message="Invalid Request")

    method = req.get("method")
    params = req.get("params") if isinstance(req.get("params"), dict) else None
    request_id = req.get("id")

    # Notifications (no id) -> no response
    if request_id is None:
        return None

    try:
        if method == "initialize":
            return _jsonrpc_result(request_id=request_id, result=_handle_initialize(state, params or {}))

        if method == "ping":
            return _jsonrpc_result(request_id=request_id, result={})

        if method == "tools/list":
            return _jsonrpc_result(request_id=request_id, result=_handle_tools_list(state, params))

        if method == "tools/call":
            if not params:
                return _jsonrpc_error(request_id=request_id, code=-32602, message="Invalid params")
            return _jsonrpc_result(request_id=request_id, result=_handle_tools_call(state, params))

        # Accept initialized notification as a request if sent incorrectly.
        if method == "notifications/initialized":
            return _jsonrpc_result(request_id=request_id, result={})

        return _jsonrpc_error(request_id=request_id, code=-32601, message=f"Method not found: {method}")
    except Exception as exc:
        tb = traceback.format_exc(limit=12)
        return _jsonrpc_error(request_id=request_id, code=-32603, message=str(exc) or "Internal error", data={"trace": tb})


def run_stdio_server() -> None:
    state = _McpServerState()

    for line in sys.stdin:
        s = line.strip()
        if not s:
            continue
        try:
            msg: Any = json.loads(s)
        except Exception as exc:
            sys.stdout.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32700, "message": "Parse error", "data": {"error": str(exc)}},
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
            continue

        responses: List[Dict[str, Any]] = []

        if isinstance(msg, list):
            for req in msg:
                resp = _dispatch(state, req if isinstance(req, dict) else {})
                if resp is not None:
                    responses.append(resp)
            if responses:
                sys.stdout.write(json.dumps(responses) + "\n")
                sys.stdout.flush()
            continue

        if isinstance(msg, dict):
            resp = _dispatch(state, msg)
            if resp is not None:
                sys.stdout.write(json.dumps(resp) + "\n")
                sys.stdout.flush()
            continue

        # Unknown message type -> ignore
