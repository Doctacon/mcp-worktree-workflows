#!/usr/bin/env python3
"""
MCP Server for Git Worktree Voting Pattern

Implements the voting pattern from Anthropic's agent workflows using git worktrees.
Allows creating multiple worktree variants, implementing solutions in parallel,
evaluating them, and selecting the best one.
"""

import asyncio
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
        self.execution_results: Dict[str, dict] = {}  # Track Claude execution results
        self.evaluations: Dict[str, dict] = {}  # Track implementation evaluations
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


async def execute_claude_in_worktree(worktree_path: Path, task: str) -> dict:
    """Execute Claude in a specific worktree"""
    try:
        # Construct the Claude command
        cmd = [
            "claude", 
            "--print", 
            "--add-dir", str(worktree_path),
            f"Implement the following task: {task}"
        ]
        
        # Execute Claude
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=worktree_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        return {
            "success": process.returncode == 0,
            "stdout": stdout.decode() if stdout else "",
            "stderr": stderr.decode() if stderr else "",
            "returncode": process.returncode
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "returncode": -1
        }


def evaluate_implementation(worktree_path: Path, base_branch: str) -> dict:
    """Evaluate an implementation in a worktree"""
    evaluation = {
        "has_changes": False,
        "files_changed": 0,
        "lines_added": 0,
        "lines_removed": 0,
        "test_results": None,
        "quality_score": 0
    }
    
    try:
        # Get diff statistics
        diff_result = subprocess.run(
            ["git", "diff", "--stat", base_branch],
            cwd=worktree_path,
            capture_output=True,
            text=True
        )
        
        if diff_result.returncode == 0 and diff_result.stdout:
            evaluation["has_changes"] = True
            lines = diff_result.stdout.strip().split('\n')
            if lines:
                # Parse diff stats from last line (e.g., "2 files changed, 45 insertions(+), 12 deletions(-)")
                last_line = lines[-1]
                if "file" in last_line:
                    parts = last_line.split(',')
                    for part in parts:
                        part = part.strip()
                        if "file" in part:
                            evaluation["files_changed"] = int(part.split()[0])
                        elif "insertion" in part:
                            evaluation["lines_added"] = int(part.split()[0])
                        elif "deletion" in part:
                            evaluation["lines_removed"] = int(part.split()[0])
        
        # Try to run tests if available
        for test_cmd in [["npm", "test"], ["pytest"], ["python", "-m", "pytest"], ["uv", "run", "pytest"]]:
            try:
                test_result = subprocess.run(
                    test_cmd,
                    cwd=worktree_path,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                evaluation["test_results"] = {
                    "command": " ".join(test_cmd),
                    "success": test_result.returncode == 0,
                    "output": test_result.stdout[:1000] if test_result.stdout else "",
                    "error": test_result.stderr[:500] if test_result.stderr else ""
                }
                break
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue
        
        # Calculate quality score (simple heuristic)
        score = 0
        if evaluation["has_changes"]:
            score += 30
        if evaluation["test_results"] and evaluation["test_results"]["success"]:
            score += 50
        if evaluation["files_changed"] > 0:
            score += min(evaluation["files_changed"] * 5, 20)
        
        evaluation["quality_score"] = score
        
    except Exception as e:
        evaluation["error"] = str(e)
    
    return evaluation


async def execute_all_worktrees(session_id: str):
    """Execute Claude in all worktrees for a session"""
    if session_id not in sessions:
        return
    
    session = sessions[session_id]
    
    # Create tasks for all worktrees
    tasks = []
    for worktree_id, worktree_path in session.worktrees.items():
        task = execute_claude_in_worktree(worktree_path, session.task)
        tasks.append((worktree_id, task))
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
    
    # Process results
    for (worktree_id, _), result in zip(tasks, results):
        try:
            if isinstance(result, Exception):
                session.execution_results[worktree_id] = {
                    "success": False,
                    "error": f"Task execution failed: {str(result)}",
                    "returncode": -1
                }
            else:
                session.execution_results[worktree_id] = result
                
                # Mark as complete if successful
                if result.get("success", False):
                    session.implementations[worktree_id] = True
                    # Evaluate the implementation
                    evaluation = evaluate_implementation(session.worktrees[worktree_id], session.base_branch)
                    session.evaluations[worktree_id] = evaluation
            
        except Exception as e:
            session.execution_results[worktree_id] = {
                "success": False,
                "error": f"Task processing failed: {str(e)}",
                "returncode": -1
            }


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
    
    # Start executing Claude in each worktree asynchronously
    asyncio.create_task(execute_all_worktrees(session_id))
    
    response = {
        "session_id": session_id,
        "task": task,
        "num_variants": num_variants,
        "worktrees": {
            wid: str(path) for wid, path in session.worktrees.items()
        },
        "status": "Worktrees created. Claude is now executing in each worktree automatically.",
        "instructions": (
            f"Claude is automatically implementing the task in each worktree. "
            f"Use list_sessions() to monitor progress, then evaluate_implementations() "
            f"when all are complete to select the best implementation."
        )
    }
    
    return json.dumps(response, indent=2)


@mcp.tool()
def list_sessions() -> str:
    """List all active voting sessions"""
    sessions_list = []
    for sid, session in sessions.items():
        completed = sum(1 for done in session.implementations.values() if done)
        executing = len(session.execution_results)
        sessions_list.append({
            "session_id": sid,
            "task": session.task,
            "created_at": session.created_at.isoformat(),
            "variants": session.num_variants,
            "completed": completed,
            "executing": executing,
            "status": f"{completed}/{session.num_variants} complete, {executing}/{session.num_variants} executed"
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
    """Get all implementations for evaluation and ranking
    
    Args:
        session_id: The voting session ID
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    session = sessions[session_id]
    evaluations = []
    
    for wid, path in session.worktrees.items():
        evaluation_data = {
            "worktree_id": wid,
            "path": str(path),
            "completed": session.implementations[wid],
            "branch": f"voting-{session_id}-{wid}",
            "execution_result": session.execution_results.get(wid, {}),
            "evaluation": session.evaluations.get(wid, {})
        }
        
        # If no evaluation exists yet, create one
        if not evaluation_data["evaluation"] and evaluation_data["completed"]:
            evaluation = evaluate_implementation(path, session.base_branch)
            session.evaluations[wid] = evaluation
            evaluation_data["evaluation"] = evaluation
        
        evaluations.append(evaluation_data)
    
    # Sort by quality score (highest first)
    evaluations.sort(key=lambda x: x["evaluation"].get("quality_score", 0), reverse=True)
    
    # Add ranking
    for i, eval_data in enumerate(evaluations):
        eval_data["rank"] = i + 1
    
    # Find the best implementation
    best_implementation = evaluations[0] if evaluations else None
    
    response = {
        "session_id": session_id,
        "task": session.task,
        "base_branch": session.base_branch,
        "implementations": evaluations,
        "best_implementation": {
            "worktree_id": best_implementation["worktree_id"],
            "quality_score": best_implementation["evaluation"].get("quality_score", 0),
            "rank": 1
        } if best_implementation else None,
        "summary": {
            "total_variants": session.num_variants,
            "completed": sum(1 for e in evaluations if e["completed"]),
            "with_changes": sum(1 for e in evaluations if e["evaluation"].get("has_changes", False)),
            "tests_passed": sum(1 for e in evaluations if e["evaluation"].get("test_results", {}).get("success", False))
        },
        "recommendation": (
            f"Best implementation: {best_implementation['worktree_id']} "
            f"(score: {best_implementation['evaluation'].get('quality_score', 0)})"
        ) if best_implementation else "No implementations completed successfully"
    }
    
    return json.dumps(response, indent=2)


@mcp.tool()
def auto_select_best(session_id: str, merge_to_main: bool = False) -> str:
    """Automatically select and finalize the best implementation
    
    Args:
        session_id: The voting session ID
        merge_to_main: Whether to merge to main branch (default: false)
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    session = sessions[session_id]
    
    # Get evaluations
    evaluations = []
    for wid, path in session.worktrees.items():
        if session.implementations[wid]:  # Only consider completed implementations
            evaluation = session.evaluations.get(wid)
            if not evaluation:
                evaluation = evaluate_implementation(path, session.base_branch)
                session.evaluations[wid] = evaluation
            
            evaluations.append((wid, evaluation))
    
    if not evaluations:
        return json.dumps({
           "error": "No completed implementations found",
           "message": "Wait for implementations to complete before auto-selecting"
        }, indent=2)
    
    # Sort by quality score
    evaluations.sort(key=lambda x: x[1].get("quality_score", 0), reverse=True)
    best_worktree_id, _ = evaluations[0]
    
    # Use the existing finalize_best function
    return finalize_best(session_id, best_worktree_id, merge_to_main)


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
    
    # Get evaluation details for the winner
    winner_evaluation = session.evaluations.get(worktree_id, {})
    
    # Clean up other worktrees
    cleaned_up = []
    for wid, path in session.worktrees.items():
        if wid != worktree_id:
            subprocess.run(["git", "worktree", "remove", str(path), "--force"], cwd=session.base_path)
            subprocess.run(["git", "branch", "-D", f"voting-{session_id}-{wid}"], cwd=session.base_path)
            cleaned_up.append(wid)
    
    return json.dumps({
        "status": "finalized",
        "winner": {
            "worktree_id": worktree_id,
            "quality_score": winner_evaluation.get("quality_score", 0),
            "files_changed": winner_evaluation.get("files_changed", 0),
            "lines_added": winner_evaluation.get("lines_added", 0),
            "test_success": winner_evaluation.get("test_results", {}).get("success", False)
        },
        "merged_to_main": merge_to_main,
        "cleaned_up": cleaned_up,
        "message": f"Implementation {worktree_id} selected as best (score: {winner_evaluation.get('quality_score', 0)}). {len(cleaned_up)} other worktrees cleaned up."
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