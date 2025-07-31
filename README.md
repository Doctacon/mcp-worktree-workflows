# MCP Worktree Voting Server

An MCP (Model Context Protocol) server that implements the voting pattern for parallel development using git worktrees. This allows you to create multiple implementations of the same task, evaluate them, and select the best one.

## Installation

1. Clone this repository and install:
```bash
git clone <this-repo>
cd mcp-servers
pip install -e .  # or just: pip install mcp>=1.1.0
```

2. Add the server to Claude Code using ONE of these methods:

### Option A: Using Claude Code CLI (Recommended)
```bash
# For project-specific use (stored in current project)
claude mcp add worktree-voting python /path/to/mcp-servers/mcp_worktree_voting.py

# For use across all projects
claude mcp add --scope user worktree-voting python /path/to/mcp-servers/mcp_worktree_voting.py
```

### Option B: Manual Configuration
Add to `.mcp.json` in your project root (for project scope) or `~/.config/claude/mcp.json` (for user scope):
```json
{
  "mcpServers": {
    "worktree-voting": {
      "command": "python",
      "args": ["/path/to/mcp-servers/mcp_worktree_voting.py"]
    }
  }
}
```

3. Restart Claude Code or use `/mcp` command to reconnect

## Workflow

### 1. Create a Voting Session
```
Use tool: create_voting_session
Arguments: {
  "task": "Implement a caching layer for the API",
  "num_variants": 5
}
```

This creates 5 worktrees, each in a separate branch.

### 2. Implement in Each Worktree
For each worktree directory returned:
```bash
cd /path/to/worktree
claude "Implement the following task: Implement a caching layer for the API"
```

### 3. Mark Implementations Complete
After implementing in each worktree:
```
Use tool: mark_implementation_complete
Arguments: {
  "session_id": "abc123",
  "worktree_id": "variant-1"
}
```

### 4. Evaluate All Implementations
```
Use tool: evaluate_implementations
Arguments: {
  "session_id": "abc123"
}
```

Then review each implementation by visiting the worktree directories.

### 5. Select the Best Implementation
```
Use tool: finalize_best
Arguments: {
  "session_id": "abc123",
  "worktree_id": "variant-3",
  "merge_to_main": true
}
```

This merges the best implementation and cleans up the other worktrees.

## Available Tools

- **create_voting_session**: Start a new voting workflow
- **list_sessions**: See all active voting sessions
- **get_worktree_info**: Get details about worktrees in a session
- **mark_implementation_complete**: Track implementation progress
- **evaluate_implementations**: Prepare for evaluation phase
- **finalize_best**: Select winner and clean up
- **cleanup_session**: Force cleanup of a session

## Example Use Case

Perfect for tasks like:
- Trying different architectural approaches
- Implementing features with various libraries
- Optimizing algorithms with different strategies
- Exploring multiple UI/UX implementations