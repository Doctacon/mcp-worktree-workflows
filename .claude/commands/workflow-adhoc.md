# task_desc

Create a single worktree for quick tasks without the voting overhead.

## Usage

```
/adhoc $ARGUMENTS
```

## Description

This command creates a single git worktree branched from `origin/main` and launches Claude to complete the task. Perfect for bug fixes, small features, or quick experiments.

## Examples

```
/adhoc Fix the null pointer exception in UserProfile component

/adhoc Add input validation to the contact form

/adhoc Update the README with installation instructions
```

## What happens

1. Fetches latest from `origin/main`
2. Creates a new worktree with a descriptive branch name
3. Launches Claude in a new Terminal window
4. Claude executes the task immediately
5. Work is isolated in its own branch

## Benefits

- Quick start from clean `origin/main`
- No voting overhead for simple tasks
- Isolated development environment
- Easy to test changes without affecting main branch

## MCP Tool

This command uses: `create_adhoc_worktree(task)`