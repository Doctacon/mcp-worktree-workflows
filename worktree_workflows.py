#!/usr/bin/env python3
"""
MCP Server for Git Worktree Workflows

Implements multiple development workflows using git worktrees:
- Voting pattern: Create multiple implementations and select the best
- Ad hoc tasks: Single worktree for quick development
- Orchestrated subtasks: Break complex tasks into parallel worktrees
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
import re


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
        # Store task slug for consistent branch naming
        self.task_slug = ''.join(c for c in task[:20] if c.isalnum() or c == ' ').replace(' ', '-').lower()
    
    def _get_current_branch(self) -> str:
        """Get the current git branch"""
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=self.base_path,
            capture_output=True,
            text=True
        )
        return result.stdout.strip() if result.returncode == 0 else "main"
    
    def _get_branch_name(self, session_id: str, worktree_id: str) -> str:
        """Get the branch name for a worktree"""
        return f"voting-{session_id}-{self.task_slug}-{worktree_id}"


def _get_branch_name(session_id: str, worktree_id: str) -> str:
    """Get the branch name for a worktree when session is not available"""
    if session_id in sessions:
        return sessions[session_id]._get_branch_name(session_id, worktree_id)
    # Fallback for cleanup operations
    return f"voting-{session_id}-{worktree_id}"


# Initialize FastMCP
mcp = FastMCP("worktree-workflows")

# Global sessions store
sessions: Dict[str, WorktreeSession] = {}

# Global monitoring tasks
monitoring_tasks: Dict[str, asyncio.Task] = {}


def spawn_terminal_for_worktree(worktree_path: Path, task: str) -> None:
    """Spawn a new Terminal window and run Claude in the specified worktree"""
    try:
        # Use osascript to open a new Terminal window and run Claude with automatic completion logging
        applescript = f'''
        tell application "Terminal"
            do script "cd '{worktree_path}' && echo 'TASK STARTED - '$(date) >> execution.log && claude '{task}' --dangerously-skip-permissions 2>&1 | tee -a execution.log; echo 'TASK COMPLETED SUCCESSFULLY - '$(date) >> execution.log"
            activate
        end tell
        '''
        
        subprocess.run(
            ["osascript", "-e", applescript],
            check=False  # Don't raise exception if osascript fails
        )
    except Exception as e:
        print(f"Warning: Could not spawn terminal for {worktree_path}: {e}")


def create_task_instructions(worktree_path: Path, task: str, session_id: str, variant_id: str) -> str:
    """Create a task instruction file for Claude to execute"""
    log_file = worktree_path / "execution.log"
    
    instructions = f"""# Task Instructions for {variant_id}

## Task
{task}

## Instructions
1. Work in this directory: {worktree_path}
2. Complete the task thoroughly and with high quality
3. Completion logging is automatic - no manual action required
4. Alternative: Use MCP command if available: mark_implementation_complete("{session_id}", "{variant_id}")

## Automatic Completion Detection
The system will automatically write "TASK COMPLETED SUCCESSFULLY" to execution.log when the Claude session ends.
No manual completion signal is required.

## Execution Log
All output will be logged to: {log_file}

## How to start
Open a terminal and run:
```bash
cd "{worktree_path}"
echo "TASK STARTED - $(date)" >> execution.log
claude "{task}" --dangerously-skip-permissions 2>&1 | tee -a execution.log
echo "TASK COMPLETED SUCCESSFULLY - $(date)" >> execution.log
```

The Claude session will run and completion will be logged automatically.

## Completion Detection
The system monitors execution.log for "TASK COMPLETED SUCCESSFULLY" to detect when tasks finish.
"""
    
    # Write instructions to the worktree
    instructions_file = worktree_path / "TASK_INSTRUCTIONS.md"
    with open(instructions_file, 'w') as f:
        f.write(instructions)
    
    # Initialize log file
    with open(log_file, 'w') as f:
        f.write(f"WORKTREE INITIALIZED - {datetime.now().isoformat()}\n")
        f.write(f"Session ID: {session_id}\n")
        f.write(f"Variant ID: {variant_id}\n")
        f.write(f"Task: {task}\n")
        f.write("=" * 80 + "\n")
    
    return str(instructions_file)


def check_log_completion(worktree_path: Path) -> bool:
    """Check if worktree has completed based on log file"""
    log_file = worktree_path / "execution.log"
    
    if not log_file.exists():
        return False
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            return "TASK COMPLETED SUCCESSFULLY" in content
    except Exception:
        return False


def analyze_implementation_basic(worktree_path: Path, base_branch: str) -> dict:
    """Get basic statistics about an implementation"""
    analysis = {
        "has_changes": False,
        "files_changed": 0,
        "lines_added": 0,
        "lines_removed": 0,
        "test_results": None,
        "file_list": []
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
            analysis["has_changes"] = True
            lines = diff_result.stdout.strip().split('\n')
            if lines:
                # Parse diff stats from last line
                last_line = lines[-1]
                if "file" in last_line:
                    parts = last_line.split(',')
                    for part in parts:
                        part = part.strip()
                        if "file" in part:
                            analysis["files_changed"] = int(part.split()[0])
                        elif "insertion" in part:
                            analysis["lines_added"] = int(part.split()[0])
                        elif "deletion" in part:
                            analysis["lines_removed"] = int(part.split()[0])
        
        # Get list of changed files
        files_result = subprocess.run(
            ["git", "diff", "--name-only", base_branch],
            cwd=worktree_path,
            capture_output=True,
            text=True
        )
        if files_result.returncode == 0:
            analysis["file_list"] = [f.strip() for f in files_result.stdout.split('\n') if f.strip()]
        
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
                analysis["test_results"] = {
                    "command": " ".join(test_cmd),
                    "success": test_result.returncode == 0,
                    "output": test_result.stdout[:1000] if test_result.stdout else "",
                    "error": test_result.stderr[:500] if test_result.stderr else ""
                }
                break
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue
        
    except Exception as e:
        analysis["error"] = str(e)
    
    return analysis


def evaluate_implementations_with_claude(session_id: str) -> dict:
    """Use Claude to evaluate and rank all implementations"""
    if session_id not in sessions:
        return {"error": f"Session {session_id} not found"}
    
    session = sessions[session_id]
    implementations = []
    
    # Gather all implementation data
    for wid, path in session.worktrees.items():
        if not session.implementations[wid]:
            continue  # Skip incomplete implementations
            
        analysis = analyze_implementation_basic(path, session.base_branch)
        
        # Get file contents of changed files
        file_contents = {}
        for filename in analysis.get("file_list", []):
            try:
                file_path = path / filename
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        file_contents[filename] = f.read()[:2000]  # Limit to 2000 chars
            except Exception:
                file_contents[filename] = "[Could not read file]"
        
        # Get execution log
        log_content = ""
        log_file = path / "execution.log"
        if log_file.exists():
            try:
                with open(log_file, 'r') as f:
                    log_content = f.read()
            except Exception:
                log_content = "[Could not read log]"
        
        implementations.append({
            "worktree_id": wid,
            "path": str(path),
            "analysis": analysis,
            "file_contents": file_contents,
            "execution_log": log_content,
            "branch": session._get_branch_name(session_id, wid)
        })
    
    return {
        "session_id": session_id,
        "task": session.task,
        "base_branch": session.base_branch,
        "implementations": implementations,
        "evaluation_method": "claude_based",
        "ready_for_claude_evaluation": True
    }


async def monitor_voting_session(session_id: str):
    """Monitor a voting session and auto-select best when all are complete"""
    print(f"Starting monitoring for voting session {session_id}")
    
    while session_id in sessions:
        session = sessions[session_id]
        
        # Check both MCP-based and log-based completion
        for worktree_id, worktree_path in session.worktrees.items():
            if not session.implementations[worktree_id]:  # Not marked complete via MCP
                if check_log_completion(worktree_path):
                    print(f"Log-based completion detected for {worktree_id}")
                    session.implementations[worktree_id] = True
        
        completed = sum(1 for done in session.implementations.values() if done)
        
        print(f"Session {session_id}: {completed}/{session.num_variants} complete")
        
        # Check if all implementations are complete
        if completed == session.num_variants:
            print(f"All implementations complete for session {session_id}. Preparing for Claude evaluation...")
            
            try:
                # Prepare implementations for Claude evaluation
                result = present_top_candidates(session_id)
                print(f"Implementations ready for evaluation. Use evaluate_implementations({session_id}) to proceed.")
                
                # Clean up monitoring task
                if session_id in monitoring_tasks:
                    del monitoring_tasks[session_id]
                
                break
                
            except Exception as e:
                print(f"Error during evaluation preparation for session {session_id}: {e}")
                break
        
        # Wait before checking again
        await asyncio.sleep(10)  # Check every 10 seconds
    
    print(f"Monitoring ended for session {session_id}")


async def monitor_orchestrated_session(session_id: str, worktrees: dict, main_task: str):
    """Monitor orchestrated worktrees and combine results when all are complete"""
    print(f"Starting monitoring for orchestrated session {session_id}")
    
    # For orchestrated sessions, we need a different completion tracking mechanism
    # Since they don't use the WorktreeSession class, we'll check for status files
    
    while True:
        completed_count = 0
        
        for worktree_id, worktree_info in worktrees.items():
            worktree_path = Path(worktree_info["path"])
            status_file = worktree_path / ".task_complete"
            
            if status_file.exists():
                completed_count += 1
        
        print(f"Orchestrated session {session_id}: {completed_count}/{len(worktrees)} complete")
        
        # Check if all subtasks are complete
        if completed_count == len(worktrees):
            print(f"All subtasks complete for session {session_id}. Combining results...")
            
            try:
                # TODO: Implement orchestrated combination logic
                result = combine_orchestrated_results(session_id, worktrees, main_task)
                print(f"Orchestration result: {result}")
                
                # Clean up monitoring task
                if session_id in monitoring_tasks:
                    del monitoring_tasks[session_id]
                
                break
                
            except Exception as e:
                print(f"Error during orchestration combination for session {session_id}: {e}")
                break
        
        # Wait before checking again
        await asyncio.sleep(10)  # Check every 10 seconds
    
    print(f"Orchestration monitoring ended for session {session_id}")


def combine_orchestrated_results(session_id: str, worktrees: dict, main_task: str) -> str:
    """Combine results from all orchestrated worktrees into main branch"""
    # This is a placeholder - we'll implement the full logic later
    results = []
    
    for worktree_id, worktree_info in worktrees.items():
        worktree_path = Path(worktree_info["path"])
        branch_name = worktree_info["branch"]
        
        # Get changes from each worktree
        # TODO: Implement git diff analysis and intelligent merging
        results.append({
            "worktree_id": worktree_id,
            "branch": branch_name,
            "path": str(worktree_path),
            "subtask": worktree_info["subtask"]
        })
    
    return json.dumps({
        "status": "combined",
        "session_id": session_id,
        "main_task": main_task,
        "combined_results": results,
        "message": f"Combined {len(results)} subtask implementations"
    }, indent=2)


def setup_worktree_instructions(session_id: str):
    """Create instruction files for all worktrees in a session"""
    if session_id not in sessions:
        return
    
    session = sessions[session_id]
    instruction_files = []
    
    for worktree_id, worktree_path in session.worktrees.items():
        instruction_file = create_task_instructions(worktree_path, session.task, session_id, worktree_id)
        instruction_files.append(instruction_file)
    
    return instruction_files


@mcp.tool()
def create_voting_worktrees(task: str, num_variants: int = 5, target_repo: str = None) -> str:
    """Create a new voting session with multiple worktrees
    
    Args:
        task: The task to implement in multiple variants
        num_variants: Number of worktree variants to create (default: 5)
        target_repo: Name of the target repository directory (if not provided, looks for single repo)
    """
    session_id = str(uuid.uuid4())[:8]
    parent_path = Path.cwd()
    
    # Find target repository
    if target_repo:
        repo_path = parent_path / target_repo
    else:
        # Look for a single git repository in current directory
        git_repos = [d for d in parent_path.iterdir() if d.is_dir() and (d / ".git").exists()]
        if len(git_repos) == 0:
            return "Error: No git repository found in current directory"
        elif len(git_repos) > 1:
            return f"Error: Multiple git repositories found. Please specify target_repo parameter. Found: {[r.name for r in git_repos]}"
        repo_path = git_repos[0]
    
    if not (repo_path / ".git").exists():
        return f"Error: {repo_path} is not a git repository"
    
    session = WorktreeSession(session_id, task, num_variants, repo_path)
    
    # Create worktrees directory alongside the repo
    worktrees_dir = parent_path / f"{repo_path.name}.worktrees"
    worktrees_dir.mkdir(exist_ok=True)
    
    # Create a task slug for naming (first 20 chars, alphanumeric only)
    task_slug = ''.join(c for c in task[:20] if c.isalnum() or c == ' ').replace(' ', '-').lower()
    
    for i in range(num_variants):
        variant_id = f"variant-{i+1}"
        branch_name = f"voting-{session_id}-{task_slug}-{variant_id}"
        worktree_path = worktrees_dir / f"{session_id}-{task_slug}-var{i+1}"
        
        # Create worktree
        result = subprocess.run(
            ["git", "worktree", "add", "-b", branch_name, str(worktree_path)],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            session.worktrees[variant_id] = worktree_path
            session.implementations[variant_id] = False
        else:
            return f"Error creating worktree: {result.stderr}"
    
    sessions[session_id] = session
    
    # Create instruction files for each worktree
    instruction_files = setup_worktree_instructions(session_id)
    
    # Generate terminal commands and spawn Terminal windows
    terminal_commands = []
    for worktree_id, worktree_path in session.worktrees.items():
        # Use a command that always writes completion signal when finished
        command = f'''echo "TASK STARTED - $(date)" >> execution.log && claude "{task}" --dangerously-skip-permissions 2>&1 | tee -a execution.log; echo "TASK COMPLETED SUCCESSFULLY - $(date)" >> execution.log'''
        terminal_commands.append({
            "worktree_id": worktree_id,
            "path": str(worktree_path),
            "command": f'cd "{worktree_path}" && {command}'
        })
        
        # Spawn Terminal window for this worktree
        print(f"DEBUG: Spawning terminal for {worktree_path}")  # Debug line
        spawn_terminal_for_worktree(worktree_path, task)
    
    response = {
        "session_id": session_id,
        "task": task,
        "num_variants": num_variants,
        "target_repo": repo_path.name,
        "worktrees": {
            wid: str(path) for wid, path in session.worktrees.items()
        },
        "instruction_files": instruction_files,
        "terminal_commands": terminal_commands,
        "status": "Worktrees created with task instructions.",
        "instructions": (
            f"Open {num_variants} separate terminals and run the commands above to start Claude sessions. "
            f"Each worktree has a TASK_INSTRUCTIONS.md file with the task details. "
            f"Child instances will automatically signal completion using mark_implementation_complete(). "
            f"The system will automatically evaluate and select the best implementation when all are complete."
        )
    }
    
    # Start monitoring task for automatic completion
    try:
        loop = asyncio.get_event_loop()
        monitoring_task = loop.create_task(monitor_voting_session(session_id))
        monitoring_tasks[session_id] = monitoring_task
        print(f"Started monitoring task for voting session {session_id}")
    except Exception as e:
        print(f"Warning: Could not start monitoring for session {session_id}: {e}")
    
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
    
    # Update completion status based on logs before reporting
    for wid, path in session.worktrees.items():
        if not session.implementations[wid] and check_log_completion(path):
            session.implementations[wid] = True
    
    if worktree_id:
        if worktree_id not in session.worktrees:
            return f"Worktree {worktree_id} not found"
        
        info = {
            "worktree_id": worktree_id,
            "path": str(session.worktrees[worktree_id]),
            "completed": session.implementations[worktree_id],
            "log_completion": check_log_completion(session.worktrees[worktree_id]),
            "branch": session._get_branch_name(session_id, worktree_id)
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
                    "log_completion": check_log_completion(path),
                    "branch": session._get_branch_name(session_id, wid)
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
    """Get all implementations ready for Claude evaluation
    
    Args:
        session_id: The voting session ID
    """
    return present_top_candidates(session_id)


@mcp.tool()
def present_top_candidates(session_id: str) -> str:
    """Present top 2 candidates for user selection based on Claude evaluation
    
    Args:
        session_id: The voting session ID
    """
    if session_id not in sessions:
        return f"Session {session_id} not found"
    
    # Get Claude evaluation data
    evaluation_data = evaluate_implementations_with_claude(session_id)
    
    if "error" in evaluation_data:
        return json.dumps(evaluation_data, indent=2)
    
    implementations = evaluation_data["implementations"]
    
    if not implementations:
        return json.dumps({
           "error": "No completed implementations found",
           "message": "Wait for implementations to complete before evaluating"
        }, indent=2)
    
    # Prepare data for Claude evaluation
    claude_prompt = f"""Task: {evaluation_data['task']}

I need you to evaluate these {len(implementations)} implementation(s) and rank them. For each implementation, consider:
- How well it fulfills the original task
- Code quality and approach
- Completeness and correctness
- Any issues or problems

Please provide a detailed analysis and rank them from best to worst, explaining your reasoning.

Implementations:
"""
    
    for i, impl in enumerate(implementations, 1):
        claude_prompt += f"\n=== Implementation {i}: {impl['worktree_id']} ===\n"
        claude_prompt += f"Files changed: {impl['analysis'].get('files_changed', 0)}\n"
        claude_prompt += f"Lines added: {impl['analysis'].get('lines_added', 0)}\n"
        claude_prompt += f"Changed files: {', '.join(impl['analysis'].get('file_list', []))}\n"
        
        if impl['file_contents']:
            claude_prompt += "\nFile contents:\n"
            for filename, content in impl['file_contents'].items():
                claude_prompt += f"\n--- {filename} ---\n{content}\n"
        
        if impl['execution_log']:
            claude_prompt += f"\nExecution log:\n{impl['execution_log']}\n"
    
    return json.dumps({
        "session_id": session_id,
        "task": evaluation_data['task'],
        "status": "ready_for_claude_evaluation",
        "implementations": implementations,
        "claude_evaluation_prompt": claude_prompt,
        "next_steps": [
            "1. Main Claude will evaluate all implementations using the provided prompt",
            "2. Present top 2 candidates with detailed analysis", 
            "3. User selects preferred implementation",
            "4. Use finalize_best(session_id, chosen_worktree_id) to complete"
        ],
        "usage": f"Call finalize_best('{session_id}', 'chosen_worktree_id') when ready to select winner"
    }, indent=2)


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
    
    winner_branch = _get_branch_name(session_id, worktree_id)
    
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
            subprocess.run(["git", "branch", "-D", _get_branch_name(session_id, wid)], cwd=session.base_path)
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
        subprocess.run(["git", "branch", "-D", _get_branch_name(session_id, wid)], cwd=session.base_path)
    
    # Note: The worktrees directory is now shared across sessions,
    # so we don't remove it entirely. The git worktree remove command
    # already removes the individual worktree directories.
    
    del sessions[session_id]
    
    return json.dumps({
        "status": "cleaned up",
        "session_id": session_id,
        "message": "All worktrees removed and session deleted"
    }, indent=2)


@mcp.tool()
def create_adhoc_worktree(task: str, target_repo: str = None) -> str:
    """Create a single worktree from origin/main and execute task with Claude
    
    Args:
        task: The task to execute in the worktree
        target_repo: Name of the target repository directory (if not provided, looks for single repo)
    """
    worktree_id = str(uuid.uuid4())[:8]
    parent_path = Path.cwd()
    
    # Find target repository
    if target_repo:
        repo_path = parent_path / target_repo
    else:
        # Look for a single git repository in current directory
        git_repos = [d for d in parent_path.iterdir() if d.is_dir() and (d / ".git").exists()]
        if len(git_repos) == 0:
            return "Error: No git repository found in current directory"
        elif len(git_repos) > 1:
            return f"Error: Multiple git repositories found. Please specify target_repo parameter. Found: {[r.name for r in git_repos]}"
        repo_path = git_repos[0]
    
    if not (repo_path / ".git").exists():
        return f"Error: {repo_path} is not a git repository"
    
    # Create a task slug for naming
    task_slug = ''.join(c for c in task[:30] if c.isalnum() or c == ' ').replace(' ', '-').lower()
    
    # Create worktrees directory alongside the repo
    worktrees_dir = parent_path / f"{repo_path.name}.worktrees"
    worktrees_dir.mkdir(exist_ok=True)
    
    # Create worktree from origin/main
    branch_name = f"adhoc-{worktree_id}-{task_slug}"
    worktree_path = worktrees_dir / f"adhoc-{worktree_id}-{task_slug}"
    
    # Fetch latest from origin and create worktree from origin/main
    subprocess.run(["git", "fetch", "origin"], cwd=repo_path, capture_output=True)
    result = subprocess.run(
        ["git", "worktree", "add", "-b", branch_name, str(worktree_path), "origin/main"],
        cwd=repo_path,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        return f"Error creating worktree: {result.stderr}"
    
    # Create task instructions
    instructions_file = create_task_instructions(worktree_path, task, worktree_id, worktree_id)
    
    # Spawn terminal with Claude
    spawn_terminal_for_worktree(worktree_path, task)
    
    response = {
        "worktree_id": worktree_id,
        "task": task,
        "branch": branch_name,
        "path": str(worktree_path),
        "instruction_file": instructions_file,
        "status": "Worktree created from origin/main and Claude session started",
        "terminal_command": f'cd "{worktree_path}" && claude "{task}" --dangerously-skip-permissions'
    }
    
    return json.dumps(response, indent=2)


@mcp.tool()
def create_orchestrated_worktrees(task: str, subtasks: list[str], target_repo: str = None) -> str:
    """Create multiple worktrees for subtasks with orchestrated execution
    
    Args:
        task: The main task description
        subtasks: List of subtasks to execute in separate worktrees
        target_repo: Name of the target repository directory (if not provided, looks for single repo)
    """
    session_id = str(uuid.uuid4())[:8]
    parent_path = Path.cwd()
    
    # Find target repository
    if target_repo:
        repo_path = parent_path / target_repo
    else:
        # Look for a single git repository in current directory
        git_repos = [d for d in parent_path.iterdir() if d.is_dir() and (d / ".git").exists()]
        if len(git_repos) == 0:
            return "Error: No git repository found in current directory"
        elif len(git_repos) > 1:
            return f"Error: Multiple git repositories found. Please specify target_repo parameter. Found: {[r.name for r in git_repos]}"
        repo_path = git_repos[0]
    
    if not (repo_path / ".git").exists():
        return f"Error: {repo_path} is not a git repository"
    
    # Create worktrees directory alongside the repo
    worktrees_dir = parent_path / f"{repo_path.name}.worktrees"
    worktrees_dir.mkdir(exist_ok=True)
    
    # Get current branch
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=repo_path,
        capture_output=True,
        text=True
    )
    base_branch = result.stdout.strip() if result.returncode == 0 else "main"
    
    worktrees = {}
    terminal_commands = []
    instruction_files = []
    
    for i, subtask in enumerate(subtasks):
        # Create a subtask slug for naming
        subtask_slug = ''.join(c for c in subtask[:30] if c.isalnum() or c == ' ').replace(' ', '-').lower()
        
        worktree_id = f"subtask-{i+1}"
        branch_name = f"orchestrated-{session_id}-{subtask_slug}"
        worktree_path = worktrees_dir / f"{session_id}-{subtask_slug}"
        
        # Create worktree
        result = subprocess.run(
            ["git", "worktree", "add", "-b", branch_name, str(worktree_path)],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            return f"Error creating worktree for subtask {i+1}: {result.stderr}"
        
        worktrees[worktree_id] = {
            "path": str(worktree_path),
            "branch": branch_name,
            "subtask": subtask
        }
        
        # Create task instructions for subtask
        instructions = f"""# Orchestrated Task: Subtask {i+1}

## Main Task
{task}

## Your Subtask
{subtask}

## Instructions
1. Work in this directory: {worktree_path}
2. Focus only on your specific subtask
3. Complete the subtask thoroughly and with high quality
4. When finished, create a completion file: `touch .task_complete`
5. Your work will be combined with other subtasks to complete the main task

## How to start
Open a terminal and run:
```bash
cd "{worktree_path}"
claude
```

Then describe what you want to accomplish based on the subtask above.

## When done
Run this command to signal completion:
```bash
touch .task_complete
```
"""
        
        # Write instructions to the worktree
        instructions_file = worktree_path / "SUBTASK_INSTRUCTIONS.md"
        with open(instructions_file, 'w') as f:
            f.write(instructions)
        
        instruction_files.append(str(instructions_file))
        
        command = f'claude "{subtask}" --dangerously-skip-permissions'
        terminal_commands.append({
            "worktree_id": worktree_id,
            "path": str(worktree_path),
            "command": f'cd "{worktree_path}" && {command}'
        })
        
        # Spawn Terminal window for this subtask
        spawn_terminal_for_worktree(worktree_path, subtask)
    
    response = {
        "session_id": session_id,
        "main_task": task,
        "subtasks": subtasks,
        "num_worktrees": len(subtasks),
        "target_repo": repo_path.name,
        "base_branch": base_branch,
        "worktrees": worktrees,
        "instruction_files": instruction_files,
        "terminal_commands": terminal_commands,
        "status": f"Created {len(subtasks)} worktrees for orchestrated execution",
        "instructions": (
            f"{len(subtasks)} Terminal windows have been opened, each with a Claude session for a specific subtask. "
            f"Each worktree has a SUBTASK_INSTRUCTIONS.md file with the subtask details. "
            f"Monitor progress across all worktrees to ensure coordinated completion of the main task."
        )
    }
    
    # Start monitoring task for automatic combination
    try:
        loop = asyncio.get_event_loop()
        monitoring_task = loop.create_task(monitor_orchestrated_session(session_id, worktrees, task))
        monitoring_tasks[session_id] = monitoring_task
        print(f"Started monitoring task for orchestrated session {session_id}")
    except Exception as e:
        print(f"Warning: Could not start monitoring for session {session_id}: {e}")
    
    return json.dumps(response, indent=2)


def main():
    """Entry point for the MCP server"""
    mcp.run()


if __name__ == "__main__":
    main()