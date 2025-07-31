# MCP Worktree Voting Server

An MCP (Model Context Protocol) server that implements the voting pattern for parallel development using git worktrees. This allows you to create multiple implementations of the same task, evaluate them, and select the best one.

## Installation

1. Install dependencies:
```bash
pip install mcp
```

2. Add to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "worktree-voting": {
      "command": "python",
      "args": ["/path/to/mcp_worktree_voting.py"]
    }
  }
}
```

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