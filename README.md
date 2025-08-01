# MCP Git Worktree Workflows

An MCP (Model Context Protocol) server that implements automated parallel development using git worktrees.

## 🚀 Key Features
- **Voting Workflow**: Intelligently ranks and selects the highest-quality implementation
- **Ad Hoc Workflow**: Quick single-worktree creation for simple tasks
- **Orchestration Workflow**: Break complex tasks into subtasks with coordinated execution
- **Clean Workflow**: Automatic cleanup of unsuccessful variants

## Installation

1. Install dependencies:
```bash
uv add fastmcp  # or pip install fastmcp
```

2. Add the server to Claude Code:

### Option A: Using Claude Code CLI (Recommended)
```bash
# For project-specific use
claude mcp add worktree-workflows python /path/to/mcp-servers/worktree_workflows.py

# For global use across all projects
claude mcp add --scope user worktree-workflows python /path/to/mcp-servers/worktree_workflows.py
```

### Option B: Manual Configuration
Add to your MCP configuration file:
```json
{
  "mcpServers": {
    "worktree-workflows": {
      "command": "python",
      "args": ["/path/to/mcp-servers/worktree_workflows.py"]
    }
  }
}
```

3. Restart Claude Code or use `/mcp` command to reconnect

## 🔄 Workflow Options

### 1. Voting Pattern (Multiple Implementations)
```
create_voting_worktrees(
  task="Rewrite the README.md with better examples",
  num_variants=5,
  target_repo="lombardi"  # Optional: specify repository
)
```

### 2. Ad Hoc Single Worktree
```
create_adhoc_worktree(
  task="Fix the login bug in auth.js",
  target_repo="my-app"  # Optional
)
```

### 3. Orchestrated Subtasks
```
create_orchestrated_worktrees(
  task="Implement user authentication system",
  subtasks=[
    "Create database models for users and sessions",
    "Build authentication API endpoints",
    "Implement JWT token generation and validation",
    "Create login/logout UI components",
    "Add authentication middleware and route protection"
  ],
  target_repo="my-app"  # Optional
)
```

## 📊 Evaluation Metrics

The system automatically evaluates implementations using:

- **Code Changes** (30 points): Has meaningful modifications
- **Test Success** (50 points): Tests pass successfully  
- **File Impact** (up to 20 points): Number of files modified
- **Quality Heuristics**: Additional scoring based on implementation patterns

## 🛠️ Available Tools

### Core Voting Workflow
- **`create_voting_worktrees`**: Creates worktrees and starts automated execution
- **`list_sessions`**: Monitor all active sessions and their progress
- **`evaluate_implementations`**: Get detailed ranking and evaluation of all variants
- **`auto_select_best`**: Automatically choose and finalize the best implementation
- **`finalize_best`**: Manually select a specific implementation
- **`cleanup_session`**: Force cleanup of sessions

### Additional Workflows
- **`create_adhoc_worktree`**: Single worktree for quick tasks
- **`create_orchestrated_worktrees`**: Multiple worktrees for coordinated subtasks

### Utility Functions
- **`get_worktree_info`**: Inspect specific worktree details
- **`mark_implementation_complete`**: Manually mark implementations as done

## 💡 Example Use Cases

### Voting Pattern
Perfect for:
- **Architecture Exploration**: Try different design patterns simultaneously
- **Library Comparison**: Implement with various frameworks/libraries
- **Algorithm Optimization**: Test multiple approaches to performance problems
- **UI/UX Variants**: Create different interface implementations

### Ad Hoc Tasks
Great for:
- **Bug Fixes**: Quick isolation and resolution
- **Small Features**: Rapid implementation without overhead
- **Experiments**: Try ideas without affecting main branch

### Orchestrated Development
Ideal for:
- **Large Features**: Break down and parallelize development
- **System Refactoring**: Coordinate multiple related changes
- **API Development**: Build endpoints, models, and tests in parallel

## 🎯 Quick Start Examples

### Example 1: Voting Pattern
```python
# 1. Start automated voting session
create_voting_worktrees(
    task="Add Redis caching to the user service with error handling",
    num_variants=3
)

# 2. Check progress (Claude is working automatically)
list_sessions()
# Returns: "2/3 complete, 3/3 executed"

# 3. View results and rankings  
evaluate_implementations(session_id="xyz789")
# Shows ranked implementations with scores

# 4. Auto-select winner and merge
auto_select_best(session_id="xyz789", merge_to_main=true)
# Merges best implementation, cleans up others
```

### Example 2: Quick Fix
```python
# Create single worktree for a bug fix
create_adhoc_worktree(
    task="Fix null pointer exception in user profile component"
)
# Claude starts immediately in new Terminal window
```

### Example 3: Feature Development
```python
# Break down complex feature into subtasks
create_orchestrated_worktrees(
    task="Add real-time notifications",
    subtasks=[
        "Set up WebSocket server infrastructure",
        "Create notification database schema and models",
        "Build notification API endpoints",
        "Implement frontend notification component",
        "Add notification preferences to user settings"
    ]
)
# 5 Terminal windows open, each with Claude working on a subtask
```

## ⚡ Performance Notes

- **Concurrent Execution**: All Claude instances run in parallel
- **Automatic Cleanup**: Failed/low-quality implementations are removed
- **Resource Efficient**: Only keeps the winning implementation
- **Fast Evaluation**: Uses git diff stats and automated test detection
- **Smart Naming**: Worktrees include task description for easy identification

## 🔧 Advanced Features

### Worktree Naming
- Branches and directories now include task descriptions
- Format: `{session-id}-{task-slug}-{variant}`
- Example: `abc123-fix-login-bug-var1`

### Origin/Main Support
- Ad hoc worktrees always branch from `origin/main`
- Ensures clean starting point for isolated tasks
- Automatically fetches latest changes

### Terminal Integration
- Automatically spawns Terminal windows (macOS)
- Each worktree gets its own Claude session
- No manual terminal management required