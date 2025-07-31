# MCP Worktree Voting Server

An MCP (Model Context Protocol) server that implements automated parallel development using git worktrees. Creates multiple implementations of the same task simultaneously, automatically evaluates them, and selects the best one.

## 🚀 Key Features

- **Fully Automated**: Claude automatically executes in each worktree
- **Parallel Execution**: All implementations run concurrently for speed
- **Smart Evaluation**: Automatic code analysis, test execution, and quality scoring
- **Best Selection**: Intelligently ranks and selects the highest-quality implementation
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
claude mcp add worktree-voting python /path/to/mcp-servers/mcp_worktree_voting.py

# For global use across all projects
claude mcp add --scope user worktree-voting python /path/to/mcp-servers/mcp_worktree_voting.py
```

### Option B: Manual Configuration
Add to your MCP configuration file:
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

## 🔄 Automated Workflow

### 1. Create Session (Everything Starts Here)
```
create_voting_session(
  repository=lombardi
  task="Rewrite the README.md ",
  num_variants=5
)
```

**What happens automatically:**
- Creates 5 git worktrees with separate branches
- Launches Claude in each worktree with `--add-dir` flag
- Claude implements the task in parallel across all worktrees
- Evaluates each implementation (code changes, tests, quality metrics)
- Ranks implementations by quality score

### 2. Monitor Progress
```
list_sessions()
```
Shows execution status: `3/5 complete, 5/5 executed`

### 3. Review Evaluations
```
evaluate_implementations(session_id="abc12345")
```

Returns detailed analysis:
- **Ranked implementations** by quality score
- **Code metrics**: files changed, lines added/removed
- **Test results**: pass/fail status for each variant
- **Recommendation**: Best implementation identified

### 4. Auto-Select Best Implementation
```
auto_select_best(
  session_id="abc12345",
  merge_to_main=true
)
```
Automatically selects highest-scoring implementation and merges it.

## 📊 Evaluation Metrics

The system automatically evaluates implementations using:

- **Code Changes** (30 points): Has meaningful modifications
- **Test Success** (50 points): Tests pass successfully  
- **File Impact** (up to 20 points): Number of files modified
- **Quality Heuristics**: Additional scoring based on implementation patterns

## 🛠️ Available Tools

### Core Workflow
- **`create_voting_session`**: Creates worktrees and starts automated execution
- **`list_sessions`**: Monitor all active sessions and their progress
- **`evaluate_implementations`**: Get detailed ranking and evaluation of all variants
- **`auto_select_best`**: Automatically choose and finalize the best implementation

### Manual Control (Optional)
- **`get_worktree_info`**: Inspect specific worktree details
- **`mark_implementation_complete`**: Manually mark implementations as done
- **`finalize_best`**: Manually select a specific implementation
- **`cleanup_session`**: Force cleanup of sessions

## 💡 Example Use Cases

Perfect for:
- **Architecture Exploration**: Try different design patterns simultaneously
- **Library Comparison**: Implement with various frameworks/libraries
- **Algorithm Optimization**: Test multiple approaches to performance problems
- **UI/UX Variants**: Create different interface implementations
- **API Design**: Explore different endpoint structures
- **Database Integration**: Try various ORM approaches or query strategies

## 🎯 Quick Start Example

```python
# 1. Start automated voting session
create_voting_session(
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

## ⚡ Performance Notes

- **Concurrent Execution**: All Claude instances run in parallel
- **Automatic Cleanup**: Failed/low-quality implementations are removed
- **Resource Efficient**: Only keeps the winning implementation
- **Fast Evaluation**: Uses git diff stats and automated test detection