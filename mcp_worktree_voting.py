#!/usr/bin/env python3
"""
MCP Server for Git Worktree Voting Pattern

Implements the voting pattern from Anthropic's agent workflows using git worktrees.
Allows creating multiple worktree variants, implementing solutions in parallel,
evaluating them, and selecting the best one.
"""

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent


class WorktreeSession:
    """Represents a voting session with multiple worktrees"""
    
    def __init__(self, session_id: str, task: str, num_variants: int, base_path: Path):
        self.session_id = session_id
        self.task = task
        self.num_variants = num_variants
        self.base_path = base_path
        self.worktrees: Dict[str, Path] = {}
        self.implementations: Dict[str, bool] = {}  # Track completion
        self.created_at = datetime.now()
        self.base_branch = self._get_current_branch()
    
    def _get_current_branch(self) -> str:
        """Get the current git branch"""
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=self.base_path,
            capture_output=True,
            text=True
        )
        return result.stdout.strip() if result.returncode == 0 else "main"


class WorktreeVotingServer:
    """MCP server for managing worktree-based voting workflows"""
    
    def __init__(self):
        self.server = Server("worktree-voting")
        self.sessions: Dict[str, WorktreeSession] = {}
        self._setup_tools()
    
    def _setup_tools(self):
        """Register all MCP tools"""
        
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name="create_voting_session",
                    description="Create a new voting session with multiple worktrees",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "task": {
                                "type": "string",
                                "description": "The task to implement in multiple variants"
                            },
                            "num_variants": {
                                "type": "integer",
                                "description": "Number of worktree variants to create (default: 5)",
                                "default": 5
                            }
                        },
                        "required": ["task"]
                    }
                ),
                Tool(
                    name="list_sessions",
                    description="List all active voting sessions",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                ),
                Tool(
                    name="get_worktree_info",
                    description="Get information about a specific worktree in a session",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "session_id": {
                                "type": "string",
                                "description": "The voting session ID"
                            },
                            "worktree_id": {
                                "type": "string",
                                "description": "The worktree ID (optional, returns all if not specified)"
                            }
                        },
                        "required": ["session_id"]
                    }
                ),
                Tool(
                    name="mark_implementation_complete",
                    description="Mark a worktree implementation as complete",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "session_id": {
                                "type": "string",
                                "description": "The voting session ID"
                            },
                            "worktree_id": {
                                "type": "string",
                                "description": "The worktree ID"
                            }
                        },
                        "required": ["session_id", "worktree_id"]
                    }
                ),
                Tool(
                    name="evaluate_implementations",
                    description="Get all implementations for evaluation",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "session_id": {
                                "type": "string",
                                "description": "The voting session ID"
                            }
                        },
                        "required": ["session_id"]
                    }
                ),
                Tool(
                    name="finalize_best",
                    description="Merge the best implementation and clean up others",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "session_id": {
                                "type": "string",
                                "description": "The voting session ID"
                            },
                            "worktree_id": {
                                "type": "string",
                                "description": "The winning worktree ID"
                            },
                            "merge_to_main": {
                                "type": "boolean",
                                "description": "Whether to merge to main branch (default: false)",
                                "default": False
                            }
                        },
                        "required": ["session_id", "worktree_id"]
                    }
                ),
                Tool(
                    name="cleanup_session",
                    description="Remove all worktrees and clean up a session",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "session_id": {
                                "type": "string",
                                "description": "The voting session ID"
                            },
                            "force": {
                                "type": "boolean",
                                "description": "Force cleanup even if implementations aren't finalized",
                                "default": False
                            }
                        },
                        "required": ["session_id"]
                    }
                )
            ]
        
        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            if name == "create_voting_session":
                return await self._create_voting_session(
                    arguments["task"],
                    arguments.get("num_variants", 5)
                )
            elif name == "list_sessions":
                return await self._list_sessions()
            elif name == "get_worktree_info":
                return await self._get_worktree_info(
                    arguments["session_id"],
                    arguments.get("worktree_id")
                )
            elif name == "mark_implementation_complete":
                return await self._mark_implementation_complete(
                    arguments["session_id"],
                    arguments["worktree_id"]
                )
            elif name == "evaluate_implementations":
                return await self._evaluate_implementations(arguments["session_id"])
            elif name == "finalize_best":
                return await self._finalize_best(
                    arguments["session_id"],
                    arguments["worktree_id"],
                    arguments.get("merge_to_main", False)
                )
            elif name == "cleanup_session":
                return await self._cleanup_session(
                    arguments["session_id"],
                    arguments.get("force", False)
                )
            else:
                return [TextContent(text=f"Unknown tool: {name}")]
    
    async def _create_voting_session(self, task: str, num_variants: int) -> List[TextContent]:
        """Create a new voting session with worktrees"""
        session_id = str(uuid.uuid4())[:8]
        base_path = Path.cwd()
        
        # Check if we're in a git repository
        if not (base_path / ".git").exists():
            return [TextContent(text="Error: Not in a git repository")]
        
        session = WorktreeSession(session_id, task, num_variants, base_path)
        
        # Create worktrees
        worktrees_dir = base_path / f".worktrees-{session_id}"
        worktrees_dir.mkdir(exist_ok=True)
        
        for i in range(num_variants):
            variant_id = f"variant-{i+1}"
            branch_name = f"voting-{session_id}-{variant_id}"
            worktree_path = worktrees_dir / variant_id
            
            # Create worktree
            result = subprocess.run(
                ["git", "worktree", "add", "-b", branch_name, str(worktree_path)],
                cwd=base_path,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                session.worktrees[variant_id] = worktree_path
                session.implementations[variant_id] = False
            else:
                return [TextContent(text=f"Error creating worktree: {result.stderr}")]
        
        self.sessions[session_id] = session
        
        response = {
            "session_id": session_id,
            "task": task,
            "num_variants": num_variants,
            "worktrees": {
                wid: str(path) for wid, path in session.worktrees.items()
            },
            "instructions": (
                f"Voting session created! For each worktree, navigate to the directory "
                f"and run: claude 'Implement the following task: {task}'"
            )
        }
        
        return [TextContent(text=json.dumps(response, indent=2))]
    
    async def _list_sessions(self) -> List[TextContent]:
        """List all active sessions"""
        sessions_list = []
        for sid, session in self.sessions.items():
            completed = sum(1 for done in session.implementations.values() if done)
            sessions_list.append({
                "session_id": sid,
                "task": session.task,
                "created_at": session.created_at.isoformat(),
                "variants": session.num_variants,
                "completed": completed,
                "status": f"{completed}/{session.num_variants} implementations complete"
            })
        
        return [TextContent(text=json.dumps(sessions_list, indent=2))]
    
    async def _get_worktree_info(self, session_id: str, worktree_id: Optional[str]) -> List[TextContent]:
        """Get worktree information"""
        if session_id not in self.sessions:
            return [TextContent(text=f"Session {session_id} not found")]
        
        session = self.sessions[session_id]
        
        if worktree_id:
            if worktree_id not in session.worktrees:
                return [TextContent(text=f"Worktree {worktree_id} not found")]
            
            info = {
                "worktree_id": worktree_id,
                "path": str(session.worktrees[worktree_id]),
                "completed": session.implementations[worktree_id],
                "branch": f"voting-{session_id}-{worktree_id}"
            }
        else:
            info = {
                "session_id": session_id,
                "task": session.task,
                "worktrees": [
                    {
                        "id": wid,
                        "path": str(path),
                        "completed": session.implementations[wid],
                        "branch": f"voting-{session_id}-{wid}"
                    }
                    for wid, path in session.worktrees.items()
                ]
            }
        
        return [TextContent(text=json.dumps(info, indent=2))]
    
    async def _mark_implementation_complete(self, session_id: str, worktree_id: str) -> List[TextContent]:
        """Mark an implementation as complete"""
        if session_id not in self.sessions:
            return [TextContent(text=f"Session {session_id} not found")]
        
        session = self.sessions[session_id]
        if worktree_id not in session.worktrees:
            return [TextContent(text=f"Worktree {worktree_id} not found")]
        
        session.implementations[worktree_id] = True
        completed = sum(1 for done in session.implementations.values() if done)
        
        return [TextContent(text=json.dumps({
            "status": "marked complete",
            "worktree_id": worktree_id,
            "progress": f"{completed}/{session.num_variants} complete"
        }, indent=2))]
    
    async def _evaluate_implementations(self, session_id: str) -> List[TextContent]:
        """Get all implementations for evaluation"""
        if session_id not in self.sessions:
            return [TextContent(text=f"Session {session_id} not found")]
        
        session = self.sessions[session_id]
        evaluations = []
        
        for wid, path in session.worktrees.items():
            # Get the diff for this worktree
            result = subprocess.run(
                ["git", "diff", session.base_branch],
                cwd=path,
                capture_output=True,
                text=True
            )
            
            evaluations.append({
                "worktree_id": wid,
                "path": str(path),
                "completed": session.implementations[wid],
                "branch": f"voting-{session_id}-{wid}",
                "has_changes": len(result.stdout) > 0
            })
        
        response = {
            "session_id": session_id,
            "task": session.task,
            "base_branch": session.base_branch,
            "implementations": evaluations,
            "evaluation_instructions": (
                "To evaluate, visit each worktree directory and examine the implementation. "
                "Use 'git diff' to see changes, run tests, and assess code quality."
            )
        }
        
        return [TextContent(text=json.dumps(response, indent=2))]
    
    async def _finalize_best(self, session_id: str, worktree_id: str, merge_to_main: bool) -> List[TextContent]:
        """Finalize the best implementation"""
        if session_id not in self.sessions:
            return [TextContent(text=f"Session {session_id} not found")]
        
        session = self.sessions[session_id]
        if worktree_id not in session.worktrees:
            return [TextContent(text=f"Worktree {worktree_id} not found")]
        
        winner_path = session.worktrees[worktree_id]
        winner_branch = f"voting-{session_id}-{worktree_id}"
        
        if merge_to_main:
            # Switch to base branch and merge
            subprocess.run(["git", "checkout", session.base_branch], cwd=session.base_path)
            result = subprocess.run(
                ["git", "merge", "--no-ff", winner_branch, "-m", f"Merge winner from voting session {session_id}: {session.task}"],
                cwd=session.base_path,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                return [TextContent(text=f"Error merging: {result.stderr}")]
        
        # Clean up other worktrees
        for wid, path in session.worktrees.items():
            if wid != worktree_id:
                subprocess.run(["git", "worktree", "remove", str(path), "--force"], cwd=session.base_path)
                subprocess.run(["git", "branch", "-D", f"voting-{session_id}-{wid}"], cwd=session.base_path)
        
        return [TextContent(text=json.dumps({
            "status": "finalized",
            "winner": worktree_id,
            "merged_to_main": merge_to_main,
            "message": f"Implementation {worktree_id} selected as best. Other worktrees cleaned up."
        }, indent=2))]
    
    async def _cleanup_session(self, session_id: str, force: bool) -> List[TextContent]:
        """Clean up all worktrees in a session"""
        if session_id not in self.sessions:
            return [TextContent(text=f"Session {session_id} not found")]
        
        session = self.sessions[session_id]
        
        # Check if any implementation is not finalized
        if not force:
            incomplete = [wid for wid, done in session.implementations.items() if not done]
            if incomplete:
                return [TextContent(text=json.dumps({
                    "error": "Session has incomplete implementations",
                    "incomplete": incomplete,
                    "message": "Use force=true to cleanup anyway"
                }, indent=2))]
        
        # Remove all worktrees
        for wid, path in session.worktrees.items():
            subprocess.run(["git", "worktree", "remove", str(path), "--force"], cwd=session.base_path)
            subprocess.run(["git", "branch", "-D", f"voting-{session_id}-{wid}"], cwd=session.base_path)
        
        # Remove worktrees directory
        worktrees_dir = session.base_path / f".worktrees-{session_id}"
        if worktrees_dir.exists():
            shutil.rmtree(worktrees_dir)
        
        del self.sessions[session_id]
        
        return [TextContent(text=json.dumps({
            "status": "cleaned up",
            "session_id": session_id,
            "message": "All worktrees removed and session deleted"
        }, indent=2))]
    
    async def run(self):
        """Run the MCP server"""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(read_stream, write_stream)


def main():
    """Main entry point for the MCP server"""
    server = WorktreeVotingServer()
    asyncio.run(server.run())


if __name__ == "__main__":
    main()