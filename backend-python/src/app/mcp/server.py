from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.mcp.response.build_response import run as run_build_response
from app.mcp.schema.get_schema import run as run_get_schema
from app.mcp.sql.correct_sql import run as run_correct_sql
from app.mcp.sql.execute_sql import run as run_execute_sql
from app.mcp.sql.generate_sql import run as run_generate_sql
from app.mcp.sql.validate_sql import run as run_validate_sql
from app.mcp.visualization.generate_chart import run as run_generate_chart

router = APIRouter(prefix="/mcp", tags=["mcp"])
logger = logging.getLogger(__name__)


class MCPRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)


@router.post("/get_schema")
async def get_schema(req: MCPRequest) -> dict[str, Any]:
    logger.info("[MCP] Tool called: get_schema")
    logger.info("[MCP] Input: %s", req.payload)
    try:
        result = run_get_schema(req.payload)
        logger.info("[MCP] Output: %s", result)
        return result
    except Exception as exc:
        logger.error("[ERROR] mcp_tool=get_schema error=%s", str(exc), exc_info=True)
        raise


@router.post("/generate_sql")
async def generate_sql(req: MCPRequest) -> dict[str, Any]:
    logger.info("[MCP] Tool called: generate_sql")
    logger.info("[MCP] Input: %s", req.payload)
    try:
        result = run_generate_sql(req.payload)
        logger.info("[MCP] Output: %s", result)
        return result
    except Exception as exc:
        logger.error("[ERROR] mcp_tool=generate_sql error=%s", str(exc), exc_info=True)
        raise


@router.post("/validate_sql")
async def validate_sql(req: MCPRequest) -> dict[str, Any]:
    logger.info("[MCP] Tool called: validate_sql")
    logger.info("[MCP] Input: %s", req.payload)
    try:
        result = run_validate_sql(req.payload)
        logger.info("[MCP] Output: %s", result)
        return result
    except Exception as exc:
        logger.error("[ERROR] mcp_tool=validate_sql error=%s", str(exc), exc_info=True)
        raise


@router.post("/execute_sql")
async def execute_sql(req: MCPRequest) -> dict[str, Any]:
    logger.info("[MCP] Tool called: execute_sql")
    logger.info("[MCP] Input: %s", req.payload)
    try:
        result = run_execute_sql(req.payload)
        logger.info("[MCP] Output: %s", result)
        return result
    except Exception as exc:
        logger.error("[ERROR] mcp_tool=execute_sql error=%s", str(exc), exc_info=True)
        raise


@router.post("/correct_sql")
async def correct_sql(req: MCPRequest) -> dict[str, Any]:
    logger.info("[MCP] Tool called: correct_sql")
    logger.info("[MCP] Input: %s", req.payload)
    try:
        result = run_correct_sql(req.payload)
        logger.info("[MCP] Output: %s", result)
        return result
    except Exception as exc:
        logger.error("[ERROR] mcp_tool=correct_sql error=%s", str(exc), exc_info=True)
        raise


@router.post("/generate_chart")
async def generate_chart(req: MCPRequest) -> dict[str, Any]:
    logger.info("[MCP] Tool called: generate_chart")
    logger.info("[MCP] Input: %s", req.payload)
    try:
        result = run_generate_chart(req.payload)
        logger.info("[MCP] Output: %s", result)
        return result
    except Exception as exc:
        logger.error("[ERROR] mcp_tool=generate_chart error=%s", str(exc), exc_info=True)
        raise


@router.post("/build_response")
async def build_response(req: MCPRequest) -> dict[str, Any]:
    logger.info("[MCP] Tool called: build_response")
    logger.info("[MCP] Input: %s", req.payload)
    try:
        result = run_build_response(req.payload)
        logger.info("[MCP] Output: %s", result)
        return result
    except Exception as exc:
        logger.error("[ERROR] mcp_tool=build_response error=%s", str(exc), exc_info=True)
        raise

