#!/usr/bin/env python3
"""
MCP Server for Git Worktree Voting Pattern

Implements the voting pattern from Anthropic's agent workflows using git worktrees.
Allows creating multiple worktree variants, implementing solutions in parallel,
evaluating them, and selecting the best one.
"""

import json
import shutil
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from fastmcp import FastMCP


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


# Initialize FastMCP
mcp = FastMCP("worktree-voting")

# Global sessions store
sessions: Dict[str, WorktreeSession] = {}


@mcp.tool()
def create_voting_session(task: str, num_variants: int = 5) -> str:
    """Create a new voting session with multiple worktrees
    
    Args:
        task: The task to implement in multiple variants
        num_variants: Number of worktree variants to create (default: 5)
    """
    session_id = str(uuid.uuid4())[:8]
    base_path = Path.cwd()
    
    # Check if we're in a git repository
    if not (base_path / ".git").exists():
        return "Error: Not in a git repository"
    
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
            return f"Error creating worktree: {result.stderr}"
    
    sessions[session_id] = session
    
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
    
    return json.dumps(response, indent=2)


@mcp.tool()
def list_sessions() -> str:
    """List all active voting sessions"""
    sessions_list = []
    for sid, session in sessions.items():
        completed = sum(1 for done in session.implementations.values() if done)
        sessions_list.append({
            "session_id": sid,
            "task": session.task,
            "created_at": session.created_at.isoformat(),
            "variants": session.num_variants,
            "completed": completed,
            "status": f"{completed}/{session.num_variants} implementations complete"
        })
    
    return json.dumps(sessions_list, indent=2)


@mcp.tool()
def get_worktree_info(session_id: str, worktree_id: Optional[str] = None) -> str:
    """Get information about a specific worktree in a session
    
    Args:
        session_id: The voting session ID
        worktree_id: The worktree ID (optional, returns all if not specified)
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    session = sessions[session_id]
    
    if worktree_id:
        if worktree_id not in session.worktrees:
            return f"Worktree {worktree_id} not found"
        
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
    
    return json.dumps(info, indent=2)


@mcp.tool()
def mark_implementation_complete(session_id: str, worktree_id: str) -> str:
    """Mark a worktree implementation as complete
    
    Args:
        session_id: The voting session ID
        worktree_id: The worktree ID
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    session = sessions[session_id]
    if worktree_id not in session.worktrees:
        return f"Worktree {worktree_id} not found"
    
    session.implementations[worktree_id] = True
    completed = sum(1 for done in session.implementations.values() if done)
    
    return json.dumps({
        "status": "marked complete",
        "worktree_id": worktree_id,
        "progress": f"{completed}/{session.num_variants} complete"
    }, indent=2)


@mcp.tool()
def evaluate_implementations(session_id: str) -> str:
    """Get all implementations for evaluation
    
    Args:
        session_id: The voting session ID
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    session = sessions[session_id]
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
    
    return json.dumps(response, indent=2)


@mcp.tool()
def finalize_best(session_id: str, worktree_id: str, merge_to_main: bool = False) -> str:
    """Merge the best implementation and clean up others
    
    Args:
        session_id: The voting session ID
        worktree_id: The winning worktree ID
        merge_to_main: Whether to merge to main branch (default: false)
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    session = sessions[session_id]
    if worktree_id not in session.worktrees:
        return f"Worktree {worktree_id} not found"
    
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
            return f"Error merging: {result.stderr}"
    
    # Clean up other worktrees
    for wid, path in session.worktrees.items():
        if wid != worktree_id:
            subprocess.run(["git", "worktree", "remove", str(path), "--force"], cwd=session.base_path)
            subprocess.run(["git", "branch", "-D", f"voting-{session_id}-{wid}"], cwd=session.base_path)
    
    return json.dumps({
        "status": "finalized",
        "winner": worktree_id,
        "merged_to_main": merge_to_main,
        "message": f"Implementation {worktree_id} selected as best. Other worktrees cleaned up."
    }, indent=2)


@mcp.tool()
def cleanup_session(session_id: str, force: bool = False) -> str:
    """Remove all worktrees and clean up a session
    
    Args:
        session_id: The voting session ID
        force: Force cleanup even if implementations aren't finalized
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    session = sessions[session_id]
    
    # Check if any implementation is not finalized
    if not force:
        incomplete = [wid for wid, done in session.implementations.items() if not done]
        if incomplete:
            return json.dumps({
                "error": "Session has incomplete implementations",
                "incomplete": incomplete,
                "message": "Use force=true to cleanup anyway"
            }, indent=2)
    
    # Remove all worktrees
    for wid, path in session.worktrees.items():
        subprocess.run(["git", "worktree", "remove", str(path), "--force"], cwd=session.base_path)
        subprocess.run(["git", "branch", "-D", f"voting-{session_id}-{wid}"], cwd=session.base_path)
    
    # Remove worktrees directory
    worktrees_dir = session.base_path / f".worktrees-{session_id}"
    if worktrees_dir.exists():
        shutil.rmtree(worktrees_dir)
    
    del sessions[session_id]
    
    return json.dumps({
        "status": "cleaned up",
        "session_id": session_id,
        "message": "All worktrees removed and session deleted"
    }, indent=2)


if __name__ == "__main__":
    mcp.run()