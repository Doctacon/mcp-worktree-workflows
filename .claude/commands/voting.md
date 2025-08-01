# /voting

Create multiple worktree implementations using the voting pattern to find the best solution.

## Usage

```
/voting $ARGUMENTS
```

## Description

This command creates multiple git worktrees (default: 5) where Claude will implement the same task in different ways. After all implementations are complete, you can evaluate them and select the best one.

## Examples

```
/voting Refactor the authentication system to use JWT tokens

/voting Add comprehensive error handling to the API endpoints

/voting Optimize the database queries for better performance
```

## What happens

1. Creates 5 parallel git worktrees with descriptive names
2. Launches Claude in each worktree with the task
3. Each Claude instance implements the task independently
4. You can evaluate all implementations and select the best one
5. The selected implementation can be merged to main
6. Other implementations are automatically cleaned up

## Next steps

After running this command:
- Monitor progress with `list_sessions()`
- Evaluate implementations with `evaluate_implementations(session_id)`
- Auto-select the best with `auto_select_best(session_id, merge_to_main=true)`

## MCP Tool

This command uses: `create_voting_worktrees(task, num_variants=5)`