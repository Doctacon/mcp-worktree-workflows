# /orchestrate

Break down complex tasks into coordinated subtasks with parallel development.

## Usage

```
/orchestrate $ARGUMENTS
```

Then follow the prompts to enter:
1. Main task description
2. Individual subtasks (one per line)

## Description

This command creates multiple worktrees, each dedicated to a specific subtask of a larger feature. Claude runs in parallel across all subtasks, allowing coordinated development of complex features.

## Example

```
/orchestrate

Main task: Implement user authentication system

Subtasks:
1. Create database models for users and sessions
2. Build authentication API endpoints
3. Implement JWT token generation and validation
4. Create login/logout UI components
5. Add authentication middleware and route protection
```

## What happens

1. Creates separate worktrees for each subtask
2. Each worktree gets a descriptive branch name
3. Launches Claude in each worktree with its specific subtask
4. All subtasks run in parallel
5. Each worktree has a `SUBTASK_INSTRUCTIONS.md` file
6. Development is coordinated across all subtasks

## Best for

- Large features requiring multiple components
- System-wide refactoring
- API development with models, endpoints, and tests
- Any task that can be parallelized into independent parts

## Tips

- Break tasks into truly independent subtasks
- Each subtask should be completable on its own
- Consider dependencies between subtasks
- Monitor all Terminal windows for progress

## MCP Tool

This command uses: `create_orchestrated_worktrees(task, subtasks)`