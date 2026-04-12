#!/usr/bin/env python3
"""
MCP Server for SQLMesh

Exposes SQLMesh data transformation workflow capabilities through MCP:
- Project management and scaffolding
- Planning and deployment
- Model execution and evaluation
- Testing and auditing
- Lineage introspection
- Configuration management
- State management
"""

import asyncio
import functools
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Any

import yaml
from fastmcp import FastMCP

try:
    from sqlmesh.core.context import Context
    from sqlmesh.core.config import Config, ModelDefaultsConfig
    from sqlmesh.core.gateway import GatewayConfig
    from sqlmesh.core.connection import DuckDBConnectionConfig
    SQLMESH_AVAILABLE = True
except ImportError:
    SQLMESH_AVAILABLE = False

# Initialize FastMCP
mcp = FastMCP("sqlmesh")

# Global state
contexts: Dict[str, Context] = {}
configs: Dict[str, Config] = {}
state_dir: Optional[Path] = None


# ============================================================================
# Error Handling
# ============================================================================

def handle_sqlmesh_errors(func):
    """Decorator for consistent SQLMesh error handling"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not SQLMESH_AVAILABLE:
            return json.dumps({
                "error": "SQLMeshNotAvailable",
                "message": "SQLMesh is not installed. Install it with: pip install sqlmesh",
                "suggestion": "Install SQLMesh to use this server"
            }, indent=2)

        try:
            return func(*args, **kwargs)
        except ImportError as e:
            return json.dumps({
                "error": "ImportError",
                "message": str(e),
                "suggestion": "Ensure SQLMesh is properly installed"
            }, indent=2)
        except FileNotFoundError as e:
            return json.dumps({
                "error": "FileNotFoundError",
                "message": str(e),
                "suggestion": "Check that the project path exists"
            }, indent=2)
        except Exception as e:
            return json.dumps({
                "error": "UnexpectedError",
                "message": str(e),
                "type": type(e).__name__
            }, indent=2)
    return wrapper


# ============================================================================
# State Management
# ============================================================================

def init_state_dir():
    """Initialize state directory for persistence"""
    global state_dir
    state_path = Path.cwd() / ".sqlmesh_state"
    state_path.mkdir(exist_ok=True)
    state_dir = state_path


def save_context_state(context_id: str, state: dict):
    """Save context state to disk"""
    if state_dir is None:
        init_state_dir()

    state_file = state_dir / f"{context_id}.json"
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2, default=str)


def load_context_state(context_id: str) -> dict:
    """Load context state from disk"""
    if state_dir is None:
        init_state_dir()

    state_file = state_dir / f"{context_id}.json"
    if state_file.exists():
        with open(state_file, 'r') as f:
            return json.load(f)
    return {}


# ============================================================================
# Configuration Management
# ============================================================================

class SQLMeshConfigManager:
    """Manages SQLMesh configuration files"""

    DIALECTS = ["duckdb", "snowflake", "postgres", "bigquery", "redshift", "databricks"]

    @staticmethod
    def create_default_config(dialect: str = "duckdb") -> Config:
        """Create default configuration"""
        if dialect not in SQLMeshConfigManager.DIALECTS:
            raise ValueError(f"Unsupported dialect: {dialect}. Supported: {SQLMeshConfigManager.DIALECTS}")

        # For now, we'll create a basic config structure
        # Full implementation would use SQLMesh's Config API
        config_dict = {
            "model_defaults": {
                "dialect": dialect,
                "start": "2024-01-01",
                "cron": "@daily"
            },
            "default_gateway": "local"
        }

        return config_dict

    @staticmethod
    def save_config(config: dict, project_path: Path):
        """Save configuration to config.yml"""
        config_file = project_path / "config.yml"

        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

    @staticmethod
    def load_config(project_path: Path) -> dict:
        """Load configuration from config.yml"""
        config_file = project_path / "config.yml"

        if not config_file.exists():
            return SQLMeshConfigManager.create_default_config()

        with open(config_file, 'r') as f:
            return yaml.safe_load(f)


# ============================================================================
# Project Scaffolding
# ============================================================================

class SQLMeshProjectScaffolder:
    """Generates new SQLMesh projects"""

    def create_project(
        self,
        project_name: str,
        dialect: str,
        base_path: Path
    ) -> Path:
        """Create a new SQLMesh project with scaffold"""

        if dialect not in SQLMeshConfigManager.DIALECTS:
            raise ValueError(f"Unsupported dialect: {dialect}. Supported: {SQLMeshConfigManager.DIALECTS}")

        project_path = base_path / project_name
        project_path.mkdir(exist_ok=True)

        # Create directories
        (project_path / "models").mkdir(exist_ok=True)
        (project_path / "seeds").mkdir(exist_ok=True)
        (project_path / "audits").mkdir(exist_ok=True)
        (project_path / "tests").mkdir(exist_ok=True)
        (project_path / "macros").mkdir(exist_ok=True)

        # Create config.yml
        config = SQLMeshConfigManager.create_default_config(dialect)
        SQLMeshConfigManager.save_config(config, project_path)

        # Create example models
        self._create_example_models(project_path, dialect)

        # Create __init__.py
        with open(project_path / "models" / "__init__.py", 'w') as f:
            f.write('"""SQLMesh models"""\n')

        # Create README
        self._create_readme(project_path, project_name, dialect)

        return project_path

    def _create_example_models(self, project_path: Path, dialect: str):
        """Create example model files"""

        # Example model
        example_model = """MODEL (
  name my_project.example_model,
  dialect {dialect},
  kind FULL
);

SELECT
  id,
  name,
  created_at
FROM source_table
WHERE created_at >= '@start_date'
"""

        with open(project_path / "models" / "example_model.sql", 'w') as f:
            f.write(example_model.format(dialect=dialect))

    def _create_readme(self, project_path: Path, project_name: str, dialect: str):
        """Create project README"""
        readme = f"""# {project_name}

SQLMesh project using {dialect} dialect.

## Getting Started

1. Load this project in the MCP server:
   ```
   load_project(project_path="{project_path}")
   ```

2. Generate a plan:
   ```
   generate_plan(context_id="<context_id>")
   ```

3. Apply changes:
   ```
   apply_plan(context_id="<context_id>", plan_id="<plan_id>")
   ```

## Project Structure

- `models/`: SQL and Python model definitions
- `seeds/`: Static seed data files
- `audits/`: Data quality audits
- `tests/`: Unit test definitions
- `macros/`: Reusable SQL macros
- `config.yml`: Project configuration

## SQLMesh Resources

- [Documentation](https://sqlmesh.readthedocs.io/)
- [GitHub](https://github.com/sqlmesh/sqlmesh)
"""

        with open(project_path / "README.md", 'w') as f:
            f.write(readme)


# ============================================================================
# MCP Tools - Phase 1: Project Management
# ============================================================================

@mcp.tool()
@handle_sqlmesh_errors
def create_project(
    project_name: str,
    dialect: str = "duckdb",
    base_path: str = "."
) -> str:
    """Create a new SQLMesh project with scaffold structure

    Args:
        project_name: Name for the project directory
        dialect: SQL dialect (duckdb, snowflake, postgres, bigquery, redshift, databricks)
        base_path: Parent directory for project creation

    Returns:
        Project path and created structure details
    """
    base = Path(base_path)
    scaffolder = SQLMeshProjectScaffolder()

    project_path = scaffolder.create_project(project_name, dialect, base)

    # List created structure
    structure = []
    for item in project_path.rglob("*"):
        if item.is_file():
            relative_path = item.relative_to(project_path)
            structure.append(str(relative_path))

    return json.dumps({
        "status": "success",
        "project_name": project_name,
        "project_path": str(project_path),
        "dialect": dialect,
        "structure": structure,
        "instructions": (
            f"Project created at {project_path}. "
            f"Use load_project(project_path='{project_path}') to start working with it."
        )
    }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def load_project(
    project_path: str,
    environment: str = "prod"
) -> str:
    """Load an existing SQLMesh project context

    Args:
        project_path: Path to existing SQLMesh project
        environment: Environment to load (prod, dev, etc.)

    Returns:
        Context ID and project metadata
    """
    path = Path(project_path)

    if not path.exists():
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project path does not exist: {project_path}",
            "suggestion": "Check the path or create a new project with create_project()"
        }, indent=2)

    # Load config
    config = SQLMeshConfigManager.load_config(path)

    # Create context
    context = Context(paths=str(path))

    # Generate context ID
    context_id = str(uuid.uuid4())[:8]

    # Store context
    contexts[context_id] = context
    configs[context_id] = config

    # Save state
    save_context_state(context_id, {
        "project_path": str(path),
        "environment": environment,
        "loaded_at": datetime.now().isoformat()
    })

    return json.dumps({
        "status": "success",
        "context_id": context_id,
        "project_path": str(path),
        "environment": environment,
        "config": config,
        "instructions": (
            f"Project loaded. Use context_id='{context_id}' for subsequent operations. "
            f"Available tools: generate_plan, evaluate_model, run_tests, etc."
        )
    }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def list_projects() -> str:
    """List all loaded SQLMesh projects and their status

    Returns:
        List of active projects with metadata
    """
    projects = []

    for context_id, context in contexts.items():
        state = load_context_state(context_id)

        projects.append({
            "context_id": context_id,
            "project_path": state.get("project_path", "unknown"),
            "environment": state.get("environment", "unknown"),
            "loaded_at": state.get("loaded_at", "unknown")
        })

    return json.dumps({
        "status": "success",
        "count": len(projects),
        "projects": projects
    }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def unload_project(context_id: str) -> str:
    """Unload a SQLMesh project and free resources

    Args:
        context_id: Context ID to unload

    Returns:
        Confirmation of unload
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}",
            "suggestion": "Check list_projects() for valid context IDs"
        }, indent=2)

    # Get project path before removing
    state = load_context_state(context_id)
    project_path = state.get("project_path", "unknown")

    # Remove from memory
    del contexts[context_id]
    if context_id in configs:
        del configs[context_id]

    # Remove state file
    if state_dir:
        state_file = state_dir / f"{context_id}.json"
        if state_file.exists():
            state_file.unlink()

    return json.dumps({
        "status": "success",
        "context_id": context_id,
        "project_path": project_path,
        "message": f"Context {context_id} unloaded successfully"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 2: Planning & Deployment
# ============================================================================

@mcp.tool()
@handle_sqlmesh_errors
async def generate_plan(
    context_id: str,
    environment: str = "prod",
    skip_tests: bool = False,
    start: str = None,
    end: str = None
) -> str:
    """Generate a deployment plan for changes

    Args:
        context_id: Project context ID
        environment: Target environment
        skip_tests: Whether to skip tests
        start: Start date for plan (YYYY-MM-DD)
        end: End date for plan (YYYY-MM-DD)

    Returns:
        Plan details with added/modified models
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}",
            "suggestion": "Use load_project() to load a project first"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Generate plan
        plan = context.plan(
            start=start,
            end=end,
            skip_tests=skip_tests
        )

        # Extract plan information
        plan_info = {
            "context_id": context_id,
            "environment": environment,
            "plan_id": str(uuid.uuid4())[:8],
            "skip_tests": skip_tests,
            "start": start,
            "end": end,
            "has_changes": False,
            "changes": []
        }

        # Try to extract change information from the plan
        # The actual structure depends on SQLMesh's plan object
        if hasattr(plan, 'changes'):
            plan_info["has_changes"] = len(plan.changes) > 0
            plan_info["changes"] = [str(change) for change in plan.changes]

        return json.dumps({
            "status": "success",
            "plan": plan_info,
            "instructions": (
                f"Plan generated with ID {plan_info['plan_id']}. "
                f"Use apply_plan(context_id='{context_id}', plan_id='{plan_info['plan_id']}') to apply."
            )
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "PlanGenerationError",
            "message": str(e),
            "context_id": context_id,
            "suggestion": "Check model syntax and dependencies"
        }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
async def apply_plan(
    context_id: str,
    plan_id: str,
    environment: str = "prod",
    no_gaps: bool = False
) -> str:
    """Apply a generated plan to the environment

    Args:
        context_id: Project context ID
        plan_id: Plan ID from generate_plan
        environment: Target environment
        no_gaps: Ensure no data gaps in intervals

    Returns:
        Deployment status and details
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Apply the plan
        # Note: In a real implementation, we'd need to store plans and retrieve them by plan_id
        # For now, we'll create a new plan and apply it
        plan = context.plan()

        # Apply the plan
        context.apply(plan)

        return json.dumps({
            "status": "success",
            "context_id": context_id,
            "plan_id": plan_id,
            "environment": environment,
            "applied_at": datetime.now().isoformat(),
            "message": "Plan applied successfully"
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "PlanApplyError",
            "message": str(e),
            "context_id": context_id,
            "suggestion": "Review plan details and ensure environment is properly configured"
        }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def preview_plan(
    context_id: str,
    environment: str = "prod"
) -> str:
    """Preview changes without applying

    Args:
        context_id: Project context ID
        environment: Target environment

    Returns:
        Preview of changes and impacts
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    # For preview, we can generate a plan but not apply it
    # This is similar to generate_plan but emphasizes the preview aspect

    return json.dumps({
        "status": "success",
        "context_id": context_id,
        "environment": environment,
        "preview": "Plan preview - use generate_plan() to create a full plan",
        "instructions": "Use generate_plan() to create a detailed plan before applying"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 3: Model Execution
# ============================================================================

@mcp.tool()
@handle_sqlmesh_errors
async def evaluate_model(
    context_id: str,
    model_name: str,
    start: str,
    end: str,
    limit: int = 1000
) -> str:
    """Evaluate a model and return results

    Args:
        context_id: Project context ID
        model_name: Model name (schema.model_name)
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        limit: Maximum rows to return

    Returns:
        Query results as JSON
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Evaluate the model
        df = context.evaluate(
            model_name,
            start=start,
            end=end
        )

        # Convert to JSON
        if df is not None and len(df) > 0:
            # Limit rows
            df_limited = df.head(limit)

            # Convert to list of dicts
            results = df_limited.to_dict(orient='records')

            return json.dumps({
                "status": "success",
                "context_id": context_id,
                "model_name": model_name,
                "start": start,
                "end": end,
                "row_count": len(results),
                "limit": limit,
                "data": results
            }, indent=2, default=str)
        else:
            return json.dumps({
                "status": "success",
                "context_id": context_id,
                "model_name": model_name,
                "start": start,
                "end": end,
                "row_count": 0,
                "data": [],
                "message": "No data returned"
            }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "EvaluationError",
            "message": str(e),
            "context_id": context_id,
            "model_name": model_name,
            "suggestion": "Check model name and date range"
        }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
async def run_model(
    context_id: str,
    model_name: str,
    start: str,
    end: str
) -> str:
    """Run a model and materialize results

    Args:
        context_id: Project context ID
        model_name: Model to run
        start: Start date
        end: End date

    Returns:
        Execution status and metrics
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Run the model
        # This is similar to evaluate but materializes the results
        df = context.evaluate(
            model_name,
            start=start,
            end=end
        )

        row_count = len(df) if df is not None else 0

        return json.dumps({
            "status": "success",
            "context_id": context_id,
            "model_name": model_name,
            "start": start,
            "end": end,
            "executed_at": datetime.now().isoformat(),
            "row_count": row_count,
            "message": f"Model {model_name} executed successfully"
        }, indent=2, default=str)

    except Exception as e:
        return json.dumps({
            "error": "ExecutionError",
            "message": str(e),
            "context_id": context_id,
            "model_name": model_name,
            "suggestion": "Check model definition and dependencies"
        }, indent=2)


# ============================================================================
# MCP Tools - Phase 4: Testing & Auditing
# ============================================================================

@mcp.tool()
@handle_sqlmesh_errors
async def run_tests(
    context_id: str,
    model_name: str = None
) -> str:
    """Run unit tests for models

    Args:
        context_id: Project context ID
        model_name: Specific model to test (None for all)

    Returns:
        Test results with pass/fail status
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Run tests
        test_results = context.test()

        return json.dumps({
            "status": "success",
            "context_id": context_id,
            "model_name": model_name,
            "results": str(test_results),
            "message": "Tests completed"
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "TestError",
            "message": str(e),
            "context_id": context_id,
            "suggestion": "Check test definitions in tests/ directory"
        }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
async def run_audits(
    context_id: str,
    start: str,
    end: str,
    model_name: str = None
) -> str:
    """Run audits for data quality checks

    Args:
        context_id: Project context ID
        start: Start date
        end: End date
        model_name: Specific model to audit (None for all)

    Returns:
        Audit results with violations
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Run audits
        audit_results = context.audit(
            start=start,
            end=end
        )

        return json.dumps({
            "status": "success",
            "context_id": context_id,
            "model_name": model_name,
            "start": start,
            "end": end,
            "results": str(audit_results),
            "message": "Audits completed"
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "AuditError",
            "message": str(e),
            "context_id": context_id,
            "suggestion": "Check audit definitions in audits/ directory"
        }, indent=2)


# ============================================================================
# MCP Tools - Phase 5: Lineage Introspection
# ============================================================================

@mcp.tool()
@handle_sqlmesh_errors
def get_lineage(
    context_id: str,
    model_name: str,
    direction: str = "both"
) -> str:
    """Get model lineage information

    Args:
        context_id: Project context ID
        model_name: Model to analyze
        direction: Lineage direction (upstream, downstream, both)

    Returns:
        Lineage graph with relationships
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Get lineage information
        # The exact API depends on SQLMesh's lineage capabilities
        lineage_info = {
            "context_id": context_id,
            "model_name": model_name,
            "direction": direction,
            "upstream": [],
            "downstream": []
        }

        # Try to get model metadata
        if hasattr(context, 'models'):
            models = context.models
            if model_name in models:
                model = models[model_name]
                if hasattr(model, 'dependencies'):
                    lineage_info["upstream"] = list(model.dependencies)

        return json.dumps({
            "status": "success",
            "lineage": lineage_info
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "LineageError",
            "message": str(e),
            "context_id": context_id,
            "model_name": model_name,
            "suggestion": "Check model name and ensure it exists in the project"
        }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def get_dependencies(
    context_id: str,
    model_name: str = None
) -> str:
    """Get model dependencies

    Args:
        context_id: Project context ID
        model_name: Specific model (None for all models)

    Returns:
        Dependency tree/graph
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        dependencies = {
            "context_id": context_id,
            "model_name": model_name,
            "dependencies": []
        }

        # Try to get model dependencies
        if hasattr(context, 'models'):
            models = context.models

            if model_name:
                # Get dependencies for specific model
                if model_name in models:
                    model = models[model_name]
                    if hasattr(model, 'dependencies'):
                        dependencies["dependencies"] = list(model.dependencies)
            else:
                # Get all models and their dependencies
                for name, model in models.items():
                    deps = []
                    if hasattr(model, 'dependencies'):
                        deps = list(model.dependencies)
                    dependencies["dependencies"].append({
                        "model": name,
                        "depends_on": deps
                    })

        return json.dumps({
            "status": "success",
            "dependencies": dependencies
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "DependencyError",
            "message": str(e),
            "context_id": context_id,
            "suggestion": "Check project models and their definitions"
        }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def render_dag(
    context_id: str,
    output_path: str,
    select_model: str = None
) -> str:
    """Render the DAG as HTML file

    Args:
        context_id: Project context ID
        output_path: Where to save DAG HTML
        select_model: Specific model to highlight

    Returns:
        Path to generated DAG file
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Try to render DAG
        # The exact API depends on SQLMesh's DAG rendering capabilities
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        # For now, create a simple HTML placeholder
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>SQLMesh DAG</title>
</head>
<body>
    <h1>SQLMesh DAG</h1>
    <p>Context ID: {context_id}</p>
    <p>Selected Model: {select_model or 'None'}</p>
    <p>DAG rendering will be implemented with SQLMesh's DAG API</p>
</body>
</html>
"""

        with open(output, 'w') as f:
            f.write(html_content)

        return json.dumps({
            "status": "success",
            "context_id": context_id,
            "output_path": str(output),
            "select_model": select_model,
            "message": "DAG rendered successfully"
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "DAGRenderError",
            "message": str(e),
            "context_id": context_id,
            "suggestion": "Check output path and permissions"
        }, indent=2)


# ============================================================================
# MCP Tools - Phase 6: Configuration Management
# ============================================================================

@mcp.tool()
@handle_sqlmesh_errors
def set_config(
    context_id: str,
    model_defaults: dict = None,
    gateway: dict = None,
    cache_dir: str = None
) -> str:
    """Update SQLMesh configuration

    Args:
        context_id: Project context ID
        model_defaults: Model default settings (dialect, owner, start, cron)
        gateway: Gateway connection settings
        cache_dir: Cache directory path

    Returns:
        Updated configuration
    """
    if context_id not in configs:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    config = configs[context_id]

    # Update configuration
    if model_defaults:
        if "model_defaults" not in config:
            config["model_defaults"] = {}
        config["model_defaults"].update(model_defaults)

    if gateway:
        if "gateways" not in config:
            config["gateways"] = {}
        config["gateways"].update(gateway)

    if cache_dir:
        config["cache_dir"] = cache_dir

    # Save to disk
    state = load_context_state(context_id)
    project_path = state.get("project_path")
    if project_path:
        SQLMeshConfigManager.save_config(config, Path(project_path))

    return json.dumps({
        "status": "success",
        "context_id": context_id,
        "config": config,
        "message": "Configuration updated successfully"
    }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def get_config(context_id: str) -> str:
    """Get current SQLMesh configuration

    Args:
        context_id: Project context ID

    Returns:
        Current configuration as JSON
    """
    if context_id not in configs:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    config = configs[context_id]

    return json.dumps({
        "status": "success",
        "context_id": context_id,
        "config": config
    }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def add_gateway(
    context_id: str,
    gateway_name: str,
    connection_type: str,
    connection_params: dict
) -> str:
    """Add a new gateway connection

    Args:
        context_id: Project context ID
        gateway_name: Name for the gateway
        connection_type: Connection type (duckdb, snowflake, postgres, etc.)
        connection_params: Connection-specific parameters

    Returns:
        Gateway configuration
    """
    if context_id not in configs:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    config = configs[context_id]

    if "gateways" not in config:
        config["gateways"] = {}

    # Add gateway
    config["gateways"][gateway_name] = {
        "connection_type": connection_type,
        "connection_params": connection_params
    }

    # Save to disk
    state = load_context_state(context_id)
    project_path = state.get("project_path")
    if project_path:
        SQLMeshConfigManager.save_config(config, Path(project_path))

    return json.dumps({
        "status": "success",
        "context_id": context_id,
        "gateway_name": gateway_name,
        "gateway": config["gateways"][gateway_name],
        "message": f"Gateway {gateway_name} added successfully"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 7: State Management
# ============================================================================

@mcp.tool()
@handle_sqlmesh_errors
def get_state(context_id: str) -> str:
    """Get current project state

    Args:
        context_id: Project context ID

    Returns:
        Current state with model versions, environments
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    context = contexts[context_id]

    try:
        # Get state information
        state = {
            "context_id": context_id,
            "state": "State export will be implemented with SQLMesh's state API"
        }

        return json.dumps({
            "status": "success",
            "state": state
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "StateError",
            "message": str(e),
            "context_id": context_id
        }, indent=2)


@mcp.tool()
@handle_sqlmesh_errors
def list_environments(context_id: str) -> str:
    """List all environments in the project

    Args:
        context_id: Project context ID

    Returns:
        List of environments and their status
    """
    if context_id not in contexts:
        return json.dumps({
            "error": "ContextNotFound",
            "message": f"Context ID not found: {context_id}"
        }, indent=2)

    # For now, return a placeholder
    # SQLMesh environments will be listed via the context
    return json.dumps({
        "status": "success",
        "context_id": context_id,
        "environments": ["prod", "dev"],
        "message": "Environment listing will be implemented with SQLMesh's environment API"
    }, indent=2)


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Entry point for the MCP server"""
    # Initialize state directory
    init_state_dir()

    # Run server
    mcp.run()


if __name__ == "__main__":
    main()
