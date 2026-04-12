#!/usr/bin/env python3
"""
MCP Server for Git Worktrees

Supports parallel development workflows using git worktrees. The primary
workflow is 'adhoc': branch from origin/main, do the work, commit, done.
The user reviews changes in the worktree branch independently.
"""

import json
import re
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastmcp import FastMCP

mcp = FastMCP("worktrees")


# ============================================================================
# Helpers
# ============================================================================

def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def _task_slug(task: str, max_len: int = 30) -> str:
    slug = re.sub(r"[^a-zA-Z0-9 ]", "", task[:max_len])
    return slug.strip().replace(" ", "-").lower()


def _resolve_repo(repo_path: Optional[str]) -> tuple[Optional[Path], str]:
    """Resolve repo_path to an absolute Path. Returns (path, error_message).
    If repo_path is omitted, looks for a single git repo under cwd.
    On error, path is None and error_message is non-empty.
    """
    cwd = Path.cwd()
    if repo_path:
        p = Path(repo_path).expanduser().resolve()
    else:
        candidates = [d for d in cwd.iterdir() if d.is_dir() and (d / ".git").exists()]
        if len(candidates) == 0:
            return None, "No git repository found. Provide repo_path."
        if len(candidates) > 1:
            names = [c.name for c in candidates]
            return None, f"Multiple repos found: {names}. Provide repo_path."
        p = candidates[0]

    if not p.exists():
        return None, f"Path does not exist: {p}"
    if not (p / ".git").exists():
        return None, f"Not a git repository: {p}"
    return p, ""


# ============================================================================
# Tools
# ============================================================================

@mcp.tool()
def create_adhoc_worktree(
    task: str,
    repo_path: Optional[str] = None,
) -> str:
    """Create a git worktree branched from origin/main for an ad-hoc task.

    Fetches the latest origin/main, creates a new branch and worktree,
    then returns the worktree path so work can begin immediately.
    The caller (Claude) does the work in that directory, then calls
    commit_worktree. The user reviews changes in the branch independently.

    Args:
        task: Short description of the task (used for branch/directory naming).
        repo_path: Absolute or relative path to the git repository. If omitted,
                   looks for a single repo in the current working directory.

    Returns:
        JSON with worktree_path, branch, and instructions.
    """
    repo, err = _resolve_repo(repo_path)
    if err:
        return json.dumps({"error": err})
    if repo is None:
        return json.dumps({"error": "Could not resolve repository"})

    # Fetch latest
    fetch = _run(["git", "fetch", "origin"], repo)
    if fetch.returncode != 0:
        return json.dumps({"error": f"git fetch failed: {fetch.stderr.strip()}"})

    worktree_id = str(uuid.uuid4())[:8]
    slug = _task_slug(task)
    branch = f"adhoc-{worktree_id}-{slug}"

    worktrees_dir = repo.parent / f"{repo.name}.worktrees"
    worktrees_dir.mkdir(exist_ok=True)
    worktree_path = worktrees_dir / branch

    result = _run(
        ["git", "worktree", "add", "-b", branch, str(worktree_path), "origin/main"],
        repo,
    )
    if result.returncode != 0:
        return json.dumps({"error": f"git worktree add failed: {result.stderr.strip()}"})

    return json.dumps(
        {
            "worktree_path": str(worktree_path),
            "branch": branch,
            "repo": str(repo),
            "task": task,
            "created_at": datetime.now().isoformat(),
            "next_step": (
                f"Do the work inside {worktree_path}, "
                "then call commit_worktree with that path and a commit message."
            ),
        },
        indent=2,
    )


@mcp.tool()
def commit_worktree(
    worktree_path: str,
    message: str,
) -> str:
    """Stage all changes and create a commit inside a worktree.

    Args:
        worktree_path: Absolute path to the worktree directory.
        message: Commit message.

    Returns:
        JSON with commit SHA and summary.
    """
    wt = Path(worktree_path).expanduser().resolve()
    if not wt.exists():
        return json.dumps({"error": f"Worktree path does not exist: {wt}"})

    # Check for changes
    status = _run(["git", "status", "--porcelain"], wt)
    if status.returncode != 0:
        return json.dumps({"error": f"git status failed: {status.stderr.strip()}"})
    if not status.stdout.strip():
        return json.dumps({"error": "No changes to commit in worktree."})

    add = _run(["git", "add", "-A"], wt)
    if add.returncode != 0:
        return json.dumps({"error": f"git add failed: {add.stderr.strip()}"})

    commit = _run(["git", "commit", "-m", message], wt)
    if commit.returncode != 0:
        return json.dumps({"error": f"git commit failed: {commit.stderr.strip()}"})

    # Get the new SHA
    sha_result = _run(["git", "rev-parse", "--short", "HEAD"], wt)
    sha = sha_result.stdout.strip() if sha_result.returncode == 0 else "unknown"

    return json.dumps(
        {
            "committed": True,
            "sha": sha,
            "worktree_path": str(wt),
            "message": message,
            "note": "Changes are committed in the worktree branch. The user will review them.",
        },
        indent=2,
    )


@mcp.tool()
def list_worktrees(repo_path: Optional[str] = None) -> str:
    """List all git worktrees for a repository.

    Args:
        repo_path: Absolute or relative path to the git repository. If omitted,
                   looks for a single repo in the current working directory.

    Returns:
        JSON list of worktrees with path, branch, and HEAD sha.
    """
    repo, err = _resolve_repo(repo_path)
    if err:
        return json.dumps({"error": err})
    if repo is None:
        return json.dumps({"error": "Could not resolve repository"})

    result = _run(["git", "worktree", "list", "--porcelain"], repo)
    if result.returncode != 0:
        return json.dumps({"error": f"git worktree list failed: {result.stderr.strip()}"})

    worktrees = []
    current: dict = {}
    for line in result.stdout.splitlines():
        if line.startswith("worktree "):
            if current:
                worktrees.append(current)
            current = {"path": line[len("worktree "):].strip()}
        elif line.startswith("HEAD "):
            current["sha"] = line[len("HEAD "):].strip()
        elif line.startswith("branch "):
            current["branch"] = line[len("branch refs/heads/"):].strip()
        elif line == "bare":
            current["bare"] = True
    if current:
        worktrees.append(current)

    return json.dumps({"repo": str(repo), "worktrees": worktrees}, indent=2)


@mcp.tool()
def remove_worktree(
    branch: str,
    repo_path: Optional[str] = None,
    force: bool = False,
) -> str:
    """Remove a git worktree and delete its branch.

    Args:
        branch: The branch name (e.g. adhoc-abc12345-fix-login-bug).
        repo_path: Absolute or relative path to the git repository. If omitted,
                   looks for a single repo in the current working directory.
        force: Pass --force to git worktree remove (needed for dirty worktrees).

    Returns:
        JSON confirmation or error.
    """
    repo, err = _resolve_repo(repo_path)
    if err:
        return json.dumps({"error": err})
    if repo is None:
        return json.dumps({"error": "Could not resolve repository"})

    # Derive the worktree path from the branch name
    worktrees_dir = repo.parent / f"{repo.name}.worktrees"
    worktree_path = worktrees_dir / branch

    remove_cmd = ["git", "worktree", "remove", str(worktree_path)]
    if force:
        remove_cmd.append("--force")

    remove = _run(remove_cmd, repo)
    if remove.returncode != 0:
        return json.dumps({"error": f"git worktree remove failed: {remove.stderr.strip()}"})

    # Delete the branch
    delete_branch = _run(["git", "branch", "-D", branch], repo)
    branch_deleted = delete_branch.returncode == 0

    return json.dumps(
        {
            "removed": True,
            "worktree_path": str(worktree_path),
            "branch": branch,
            "branch_deleted": branch_deleted,
        },
        indent=2,
    )


@mcp.tool()
def worktree_status(worktree_path: str) -> str:
    """Show git status and recent commits inside a worktree.

    Args:
        worktree_path: Absolute path to the worktree directory.

    Returns:
        JSON with status output and last few commits.
    """
    wt = Path(worktree_path).expanduser().resolve()
    if not wt.exists():
        return json.dumps({"error": f"Worktree path does not exist: {wt}"})

    status = _run(["git", "status", "--short"], wt)
    log = _run(["git", "log", "--oneline", "-5"], wt)
    branch_result = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], wt)

    return json.dumps(
        {
            "worktree_path": str(wt),
            "branch": branch_result.stdout.strip() if branch_result.returncode == 0 else "unknown",
            "status": status.stdout.strip() if status.returncode == 0 else status.stderr.strip(),
            "recent_commits": log.stdout.strip() if log.returncode == 0 else log.stderr.strip(),
        },
        indent=2,
    )


if __name__ == "__main__":
    mcp.run()
