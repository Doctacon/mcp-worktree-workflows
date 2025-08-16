# repository num_voters task_desc

Create multiple worktree implementations using the voting pattern to find the best solution.

## Usage

```
/voting $ARGUMENTS
```

## Description

This command creates multiple git worktrees (default: 5) where Claude will implement the same task in different ways via parallel and concurrent data-engineer subagents. After all implementations are complete, you can evaluate them and select the best one.

## Examples

```
/voting 3 Refactor the authentication system to use JWT tokens

/voting 5 Add comprehensive error handling to the API endpoints

/voting 2 Optimize the database queries for better performance
```

## Tree Diagram of Folder Structure
  parent-directory/
  ├── mcp-servers/                           # Main repository
  │   ├── .git/
  │   ├── src/
  │   ├── package.json
  │   └── ... (other project files)
  │
  └── mcp-servers.worktrees/                 # Worktrees folder
      ├── voting-20250116-143022-variant-1/  # First implementation
      │   ├── .git                           # (file pointing to main repo)
      │   ├── src/
      │   ├── package.json
      │   └── ... (modified files)
      │
      ├── voting-20250116-143022-variant-2/  # Second implementation
      │   ├── .git
      │   ├── src/
      │   ├── package.json
      │   └── ... (different approach)
      │
      ├── voting-20250116-143022-variant-3/  # Third implementation
      │   ├── .git
      │   ├── src/
      │   ├── package.json
      │   └── ... (another approach)
      │
      ├── voting-20250116-143022-variant-4/  # Fourth implementation
      │   └── ...
      │
      └── voting-20250116-143022-variant-5/  # Fifth implementation
          └── ...

## What happens

1. Creates specified number of parallel, concurrent, Claude subagents
2. Each subagent:
   1. Creates git worktree from the current commit
   2. Names each worktree with a unique variant identifier
   3. Implements the tasks independently
   4. Changes are left uncommitted for review and selection
3. Once subagents are all done, you can evaluate all implementations and compare them 
4. User choice and implementation
7. Cleanup of worktrees after selection and implementation

## Implementation

Instead of using MCP tools, follow these steps:
1. Get current commit: `git rev-parse HEAD`
2. Generate session ID: `voting-$(date +%Y%m%d-%H%M%S)`
3. Get repository name: `basename "$(pwd)"`
4. For each variant (1 to num_voters):
   - Create branch name: `<session-id>-variant-<number>`
   - Create worktree: `git worktree add ../<repo-name>.worktrees/<branch-name> -b <branch-name>`
   - Change to worktree and execute task
5. Leave all changes uncommitted across all worktrees
6. Manually review and compare implementations
7. Select best implementation and merge if desired
8. Clean up worktrees: `git worktree remove <path>` for each

## Next steps

After running this command:
- Check worktree status with `git worktree list`
- Navigate to each worktree to review implementations
- Compare implementations across worktrees
- Manually select and merge the best implementation
- Clean up worktrees with `git worktree remove <path>`
