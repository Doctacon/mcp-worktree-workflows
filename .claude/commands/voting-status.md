# /voting-status

Check the status of all active voting sessions and evaluate implementations.

## Usage

```
/voting-status $ARGUMENTS
```

## Description

This command helps you monitor and manage active voting sessions. It shows all running sessions, their progress, and allows you to evaluate and select the best implementations.

## What it shows

- All active voting sessions with IDs
- Task descriptions for each session
- Progress (e.g., "3/5 complete, 5/5 executed")
- Creation timestamps
- Quick actions for each session

## Example output

```
Active Voting Sessions:

1. Session: abc12345
   Task: "Refactor authentication system"
   Status: 4/5 complete, 5/5 executed
   Created: 2024-01-15 10:30:00

2. Session: xyz78910
   Task: "Add caching layer"
   Status: 2/3 complete, 3/3 executed
   Created: 2024-01-15 11:45:00
```

## Next steps

For each session, you can:
- `evaluate_implementations("session_id")` - See detailed rankings
- `auto_select_best("session_id")` - Automatically select the best
- `cleanup_session("session_id")` - Remove all worktrees

## MCP Tool

This command uses: `list_sessions()`