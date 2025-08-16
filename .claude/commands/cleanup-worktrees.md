# repository
---
name: cleanup-worktrees
description: Clean up all worktrees on a specific repository
confirmation: This will remove all worktrees for the selected repository. Are you sure?
---

```bash
#!/bin/bash

# Find all directories in the current directory that are git repositories (not ending in .worktrees)
echo "Searching for repositories..."
repos=()
for dir in */; do
    # Remove trailing slash
    dir="${dir%/}"
    # Skip if it ends with .worktrees
    if [[ "$dir" == *.worktrees ]]; then
        continue
    fi
    # Check if it's a git repository
    if [ -d "$dir/.git" ] || [ -f "$dir/.git" ]; then
        repos+=("$dir")
    fi
done

if [ ${#repos[@]} -eq 0 ]; then
    echo "No git repositories found in the current directory."
    exit 1
fi

# Present repository selection
echo "Select a repository to clean up worktrees:"
select repo in "${repos[@]}" "Cancel"; do
    if [ "$repo" = "Cancel" ]; then
        echo "Operation cancelled."
        exit 0
    elif [ -n "$repo" ]; then
        break
    else
        echo "Invalid selection. Please try again."
    fi
done

echo "Selected repository: $repo"

# Check if worktrees directory exists
worktrees_dir="${repo}.worktrees"
if [ ! -d "$worktrees_dir" ]; then
    echo "No worktrees directory found for $repo"
    exit 0
fi

# List worktrees in the selected repository
cd "$repo" || exit 1
worktree_list=$(git worktree list 2>/dev/null | tail -n +2)  # Skip the main worktree
cd - > /dev/null || exit 1

if [ -z "$worktree_list" ]; then
    echo "No worktrees found for $repo"
    # Check if the worktrees directory exists but is empty
    if [ -d "$worktrees_dir" ]; then
        echo "Removing empty worktrees directory: $worktrees_dir"
        rm -rf "$worktrees_dir"
    fi
    exit 0
fi

echo ""
echo "Found the following worktrees:"
echo "$worktree_list"
echo ""

# Clean up worktrees
echo "Cleaning up worktrees for $repo..."
cd "$repo" || exit 1

# Remove each worktree
while IFS= read -r line; do
    if [ -n "$line" ]; then
        worktree_path=$(echo "$line" | awk '{print $1}')
        echo "Removing worktree: $worktree_path"
        git worktree remove --force "$worktree_path" 2>/dev/null || git worktree prune
    fi
done <<< "$worktree_list"

# Prune any remaining worktree references
git worktree prune

cd - > /dev/null || exit 1

# Remove the worktrees directory if it exists
if [ -d "$worktrees_dir" ]; then
    echo "Removing worktrees directory: $worktrees_dir"
    rm -rf "$worktrees_dir"
fi

echo ""
echo "✅ Successfully cleaned up all worktrees for $repo"
```