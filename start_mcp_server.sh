#!/bin/bash
# MCP Server startup script for worktree-voting

cd "$(dirname "$0")"
exec uv run mcp_worktree_voting.py