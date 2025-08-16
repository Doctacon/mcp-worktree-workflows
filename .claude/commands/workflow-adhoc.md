# repository task_desc

Create a single worktree for quick tasks without the voting overhead.

## Usage

```
/adhoc $ARGUMENTS
```

## Description

This command creates a single git worktree branched from the current commit and launches Claude to complete the task. Perfect for bug fixes, small features, or quick experiments.

## Examples

```
/adhoc Fix the null pointer exception in UserProfile component

/adhoc Add input validation to the contact form

/adhoc Update the README with installation instructions
```

## Tree Diagram of Folder Structure
  parent-directory/
  ├── mcp-servers/                                           # Main repository
  │   ├── .git/
  │   ├── src/
  │   ├── package.json
  │   └── ... (other project files)
  │
  └── mcp-servers.worktrees/                                # Worktrees folder
      └── adhoc-fix-null-pointer-userprofile-20250116-152245/  # Single worktree
          ├── .git                                           # (file pointing to main repo)
          ├── src/
          │   └── components/
          │       └── UserProfile.jsx                       # Fixed file
          ├── package.json
          └── ... (other project files with fix applied)

## What happens

1. Creates a new worktree branched from the current commit (NOT origin/main)
2. Names the worktree branch descriptively based on the task
3. Changes to the new worktree directory
4. Executes the task immediately
5. Work is isolated in its own branch
6. Does NOT commit changes - leaves them uncommitted for review

## Benefits

- Quick start from current working state
- No voting overhead for simple tasks
- Isolated development environment
- Easy to test changes without affecting main branch

## Implementation

Instead of using MCP tools, follow these steps:
1. Get current commit hash: `git rev-parse HEAD`
2. Generate timestamp: `date +%Y%m%d-%H%M%S`
3. Create branch name: `adhoc-[sanitized-task-description]-[timestamp]`
   - Sanitize task description: lowercase, replace spaces with hyphens, remove special chars
4. Create worktree: `git worktree add ../<repo-name>.worktrees/<branch-name> -b <branch-name>`
5. Change to worktree: `cd ../<repo-name>.worktrees/<branch-name>`
6. Execute the specified task
7. Leave changes uncommitted for review (do NOT run git commit)