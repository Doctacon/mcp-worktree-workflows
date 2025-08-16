# repository task_desc

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

## Tree Diagram of Folder Structure
  parent-directory/
  ├── mcp-servers/                                      # Main repository
  │   ├── .git/
  │   ├── src/
  │   ├── package.json
  │   └── ... (other project files)
  │
  └── mcp-servers.worktrees/                           # Worktrees folder
      ├── orchestrate-20250116-151530-database-models/ # Subtask 1
      │   ├── .git                                     # (file pointing to main repo)
      │   ├── SUBTASK_INSTRUCTIONS.md                  # Task: Create user/session models
      │   ├── src/
      │   │   └── models/
      │   │       ├── user.js                         # New user model
      │   │       └── session.js                      # New session model
      │   └── ...
      │
      ├── orchestrate-20250116-151530-auth-endpoints/  # Subtask 2
      │   ├── .git
      │   ├── SUBTASK_INSTRUCTIONS.md                  # Task: Build auth API endpoints
      │   ├── src/
      │   │   └── api/
      │   │       ├── login.js                        # Login endpoint
      │   │       ├── logout.js                       # Logout endpoint
      │   │       └── register.js                     # Registration endpoint
      │   └── ...
      │
      ├── orchestrate-20250116-151530-jwt-tokens/      # Subtask 3
      │   ├── .git
      │   ├── SUBTASK_INSTRUCTIONS.md                  # Task: Implement JWT generation
      │   ├── src/
      │   │   └── auth/
      │   │       └── jwt.js                          # JWT utilities
      │   └── ...
      │
      ├── orchestrate-20250116-151530-ui-components/   # Subtask 4
      │   ├── .git
      │   ├── SUBTASK_INSTRUCTIONS.md                  # Task: Create login/logout UI
      │   ├── src/
      │   │   └── components/
      │   │       ├── LoginForm.jsx                   # Login component
      │   │       └── LogoutButton.jsx                # Logout component
      │   └── ...
      │
      └── orchestrate-20250116-151530-middleware/      # Subtask 5
          ├── .git
          ├── SUBTASK_INSTRUCTIONS.md                  # Task: Add auth middleware
          ├── src/
          │   └── middleware/
          │       └── authGuard.js                     # Route protection
          └── ...

## What happens

1. Creates separate worktrees for each subtask from the current commit
2. Each worktree gets a descriptive branch name based on its subtask
3. Changes to each worktree and executes its specific subtask
4. Subtasks are worked on sequentially (manual parallel execution possible)
5. Each worktree maintains a `SUBTASK_INSTRUCTIONS.md` file
6. Development is coordinated across all subtasks
7. Changes are left uncommitted for review and integration

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

## Implementation

Instead of using MCP tools, follow these steps:
1. Get current commit: `git rev-parse HEAD`
2. Generate session ID: `orchestrate-$(date +%Y%m%d-%H%M%S)`
3. Get repository name: `basename "$(pwd)"`
4. Parse main task and subtasks from user input
5. For each subtask:
   - Sanitize subtask name (remove spaces/special chars, lowercase)
   - Create branch name: `<session-id>-<sanitized-subtask-name>`
   - Create worktree: `git worktree add ../<repo-name>.worktrees/<branch-name> -b <branch-name>`
   - If worktree creation fails, log error and continue with next subtask
   - Create `SUBTASK_INSTRUCTIONS.md` in worktree with:
     - Main task description
     - Specific subtask details
     - Related subtasks for context
   - Change to worktree directory
   - Execute the subtask implementation
6. Return to main repository after all subtasks
7. Leave all changes uncommitted across all worktrees for review
8. To clean up worktrees after integration:
   - List worktrees: `git worktree list`
   - Remove each: `git worktree remove <path>`