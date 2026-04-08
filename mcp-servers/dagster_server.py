#!/usr/bin/env python3
"""
MCP Server for Dagster

Exposes Dagster data orchestration capabilities through MCP:
- Asset management and materialization
- Job execution and monitoring
- Schedule and sensor management
- Lineage and metadata introspection
- Run lifecycle management
- Workspace and project management
"""

import asyncio
import json
import os
import uuid
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Any

from fastmcp import FastMCP

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

# Initialize FastMCP
mcp = FastMCP("dagster")

# Global state
projects: Dict[str, 'DagsterProject'] = {}
runs: Dict[str, 'DagsterRun'] = {}
state_dir: Optional[Path] = None


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class DagsterProject:
    """Represents a loaded Dagster project"""
    project_id: str
    project_path: Path
    workspace_file: Path
    loaded_at: str
    pythonpath: str
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class DagsterRun:
    """Track execution state of runs"""
    run_id: str
    project_id: str
    status: str  # STARTED, SUCCESS, FAILURE, CANCELED
    started_at: str
    completed_at: Optional[str] = None
    logs: List[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.logs is None:
            self.logs = []
        if self.metadata is None:
            self.metadata = {}


# ============================================================================
# State Management
# ============================================================================

def init_state_dir():
    """Initialize state directory for persistence"""
    global state_dir
    state_path = Path.cwd() / ".dagster_state"
    state_path.mkdir(exist_ok=True)
    state_dir = state_path


def save_project_state(project: DagsterProject):
    """Save project state to disk"""
    if state_dir is None:
        init_state_dir()

    state_file = state_dir / f"project_{project.project_id}.json"
    with open(state_file, 'w') as f:
        json.dump(asdict(project), f, indent=2, default=str)


def save_run_state(run: DagsterRun):
    """Save run state to disk"""
    if state_dir is None:
        init_state_dir()

    state_file = state_dir / f"run_{run.run_id}.json"
    with open(state_file, 'w') as f:
        json.dump(asdict(run), f, indent=2, default=str)


# ============================================================================
# CLI Execution
# ============================================================================

class DagsterCommandBuilder:
    """Build Dagster CLI commands with proper context"""

    def __init__(self, project: DagsterProject):
        self.project = project

    def asset_list(self, prefix: str = None) -> List[str]:
        """Build asset list command"""
        cmd = ["dagster", "-f", str(self.project.workspace_file), "asset", "list"]
        if prefix:
            cmd.extend(["--prefix", prefix])
        return cmd

    def asset_materialize(self, asset_key: str, partition: str = None) -> List[str]:
        """Build asset materialize command"""
        cmd = ["dagster", "-f", str(self.project.workspace_file), "asset", "materialize", asset_key]
        if partition:
            cmd.extend(["--partition", partition])
        return cmd

    def asset_wipe(self, asset_key: str) -> List[str]:
        """Build asset wipe command"""
        return ["dagster", "-f", str(self.project.workspace_file), "asset", "wipe", asset_key]

    def job_list(self) -> List[str]:
        """Build job list command"""
        return ["dagster", "-f", str(self.project.workspace_file), "job", "list"]

    def job_execute(self, job_name: str) -> List[str]:
        """Build job execute command"""
        return ["dagster", "-f", str(self.project.workspace_file), "job", "execute", job_name]

    def run_list(self, job_name: str = None, limit: int = 50) -> List[str]:
        """Build run list command"""
        cmd = ["dagster", "-f", str(self.project.workspace_file), "run", "list"]
        if job_name:
            cmd.extend(["--job", job_name])
        cmd.extend(["--limit", str(limit)])
        return cmd

    def run_logs(self, run_id: str) -> List[str]:
        """Build run logs command"""
        return ["dagster", "-f", str(self.project.workspace_file), "run", "logs", run_id]

    def run_report(self, run_id: str) -> List[str]:
        """Build run report command"""
        return ["dagster", "-f", str(self.project.workspace_file), "run", "report", run_id]

    def schedule_list(self) -> List[str]:
        """Build schedule list command"""
        return ["dagster", "-f", str(self.project.workspace_file), "schedule", "list"]

    def schedule_tick(self, schedule_name: str) -> List[str]:
        """Build schedule tick command"""
        return ["dagster", "-f", str(self.project.workspace_file), "schedule", "tick", schedule_name]

    def sensor_list(self) -> List[str]:
        """Build sensor list command"""
        return ["dagster", "-f", str(self.project.workspace_file), "sensor", "list"]


async def execute_dagster_command(
    command: List[str],
    cwd: Path = None,
    timeout: int = 300,
    env: Dict[str, str] = None
) -> Dict[str, Any]:
    """Execute Dagster command asynchronously"""

    try:
        # Setup environment
        process_env = os.environ.copy()
        if cwd:
            process_env["PYTHONPATH"] = str(cwd)
        if env:
            process_env.update(env)

        # Execute command
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=process_env
        )

        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout
        )

        return {
            "returncode": process.returncode,
            "stdout": stdout.decode(),
            "stderr": stderr.decode(),
            "success": process.returncode == 0
        }

    except asyncio.TimeoutError:
        if process:
            process.kill()
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": f"Command timed out after {timeout}s",
            "success": False,
            "error": "timeout"
        }
    except Exception as e:
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "success": False,
            "error": type(e).__name__
        }


# ============================================================================
# Output Parsing
# ============================================================================

class DagsterOutputParser:
    """Parse Dagster CLI output into structured data"""

    @staticmethod
    def parse_asset_list(output: str) -> List[Dict[str, Any]]:
        """Parse asset list output"""
        assets = []

        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('Asset') or line.startswith('---'):
                continue

            # Simple parsing - assumes format: "asset_key"
            if line:
                assets.append({
                    "asset_key": line
                })

        return assets

    @staticmethod
    def parse_job_list(output: str) -> List[Dict[str, Any]]:
        """Parse job list output"""
        jobs = []

        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('Job') or line.startswith('---'):
                continue

            if line:
                jobs.append({
                    "job_name": line
                })

        return jobs

    @staticmethod
    def parse_run_list(output: str) -> List[Dict[str, Any]]:
        """Parse run list output"""
        runs = []

        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('Run') or line.startswith('---'):
                continue

            # Parse run ID and status
            parts = line.split()
            if len(parts) >= 2:
                runs.append({
                    "run_id": parts[0],
                    "status": parts[1] if len(parts) > 1 else "UNKNOWN"
                })

        return runs

    @staticmethod
    def parse_schedule_list(output: str) -> List[Dict[str, Any]]:
        """Parse schedule list output"""
        schedules = []

        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('Schedule') or line.startswith('---'):
                continue

            if line:
                schedules.append({
                    "schedule_name": line
                })

        return schedules

    @staticmethod
    def parse_sensor_list(output: str) -> List[Dict[str, Any]]:
        """Parse sensor list output"""
        sensors = []

        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('Sensor') or line.startswith('---'):
                continue

            if line:
                sensors.append({
                    "sensor_name": line
                })

        return sensors


# ============================================================================
# Error Handling
# ============================================================================

def handle_dagster_errors(func):
    """Decorator for consistent Dagster error handling"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            return json.dumps({
                "error": "FileNotFoundError",
                "message": str(e),
                "suggestion": "Check that the Dagster project path exists"
            }, indent=2)
        except Exception as e:
            return json.dumps({
                "error": "UnexpectedError",
                "message": str(e),
                "type": type(e).__name__
            }, indent=2)
    return wrapper


# ============================================================================
# MCP Tools - Phase 1: Project Management
# ============================================================================

@mcp.tool()
@handle_dagster_errors
def load_project(
    project_path: str,
    workspace_file: str = None
) -> str:
    """Load a Dagster project/workspace

    Args:
        project_path: Path to Dagster project directory
        workspace_file: Optional workspace.yaml file path

    Returns:
        Project ID and metadata
    """
    path = Path(project_path)

    if not path.exists():
        return json.dumps({
            "error": "PathNotFound",
            "message": f"Project path does not exist: {project_path}"
        }, indent=2)

    # Generate project ID
    project_id = str(uuid.uuid4())[:8]

    # Determine workspace file
    if workspace_file:
        workspace = Path(workspace_file)
    else:
        workspace = path / "workspace.yaml"
        # Create workspace file if it doesn't exist
        if not workspace.exists():
            # Try to find repository definition
            if (path / "__init__.py").exists():
                workspace = path / "workspace.yaml"
            elif (path / "repository.py").exists():
                workspace = path / "workspace.yaml"
            else:
                return json.dumps({
                    "error": "InvalidProject",
                    "message": "No Dagster repository definitions found",
                    "suggestion": "Ensure project contains __init__.py or repository.py"
                }, indent=2)

    # Create project
    project = DagsterProject(
        project_id=project_id,
        project_path=path,
        workspace_file=workspace,
        loaded_at=datetime.now().isoformat(),
        pythonpath=str(path)
    )

    # Store project
    projects[project_id] = project
    save_project_state(project)

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "project_path": str(path),
        "workspace_file": str(workspace),
        "loaded_at": project.loaded_at,
        "instructions": f"Project loaded. Use project_id='{project_id}' for subsequent operations."
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
def list_projects() -> str:
    """List all loaded Dagster projects

    Returns:
        List of active projects with metadata
    """
    active_projects = []

    for project_id, project in projects.items():
        active_projects.append({
            "project_id": project_id,
            "project_path": str(project.project_path),
            "workspace_file": str(project.workspace_file),
            "loaded_at": project.loaded_at
        })

    return json.dumps({
        "status": "success",
        "count": len(active_projects),
        "projects": active_projects
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
def unload_project(project_id: str) -> str:
    """Unload a Dagster project

    Args:
        project_id: Project ID to unload

    Returns:
        Confirmation of unload
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]

    # Remove from state
    del projects[project_id]

    # Remove state file
    if state_dir:
        state_file = state_dir / f"project_{project_id}.json"
        if state_file.exists():
            state_file.unlink()

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "project_path": str(project.project_path),
        "message": f"Project '{project_id}' unloaded successfully"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 2: Asset Management
# ============================================================================

@mcp.tool()
@handle_dagster_errors
async def list_assets(
    project_id: str,
    prefix: str = None
) -> str:
    """List all assets in a project

    Args:
        project_id: Project context ID
        prefix: Optional filter prefix

    Returns:
        List of assets with metadata
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.asset_list(prefix=prefix)
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "error": "CommandFailed",
            "message": result["stderr"],
            "details": result
        }, indent=2)

    # Parse output
    assets = DagsterOutputParser.parse_asset_list(result["stdout"])

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "prefix": prefix,
        "count": len(assets),
        "assets": assets
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def materialize_assets(
    project_id: str,
    asset_key: str,
    partition: str = None,
    run_config: dict = None
) -> str:
    """Materialize a single asset

    Args:
        project_id: Project context ID
        asset_key: Asset key to materialize
        partition: Optional partition key
        run_config: Optional run configuration

    Returns:
        Materialization status and run ID
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.asset_materialize(asset_key, partition=partition)
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    # Create run record
    run_id = str(uuid.uuid4())[:8]
    run = DagsterRun(
        run_id=run_id,
        project_id=project_id,
        status="SUCCESS" if result["success"] else "FAILURE",
        started_at=datetime.now().isoformat(),
        completed_at=datetime.now().isoformat() if result["success"] else None,
        logs=result["stdout"].split('\n') if result["stdout"] else [],
        metadata={"asset_key": asset_key, "partition": partition}
    )

    runs[run_id] = run
    save_run_state(run)

    if not result["success"]:
        return json.dumps({
            "status": "failure",
            "run_id": run_id,
            "error": result["stderr"],
            "logs": result["stdout"]
        }, indent=2)

    return json.dumps({
        "status": "success",
        "run_id": run_id,
        "project_id": project_id,
        "asset_key": asset_key,
        "partition": partition,
        "completed_at": run.completed_at
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def materialize_assets(
    project_id: str,
    asset_keys: List[str] = None,
    selection: str = None,
    partition: str = None,
    run_config: dict = None
) -> str:
    """Materialize multiple assets

    Args:
        project_id: Project context ID
        asset_keys: List of asset keys (optional)
        selection: Asset selection string (optional)
        partition: Optional partition key
        run_config: Optional run configuration

    Returns:
        Materialization status and run ID
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    if not asset_keys and not selection:
        return json.dumps({
            "error": "InvalidInput",
            "message": "Must provide either asset_keys or selection"
        }, indent=2)

    # Use asset keys if provided
    if asset_keys:
        results = []
        for asset_key in asset_keys:
            result = await materialize_asset(
                project_id=project_id,
                asset_key=asset_key,
                partition=partition,
                run_config=run_config
            )
            results.append(json.loads(result))

        return json.dumps({
            "status": "success",
            "count": len(asset_keys),
            "results": results
        }, indent=2)

    # Otherwise use selection
    # (For simplicity, we'll return a message about selection)
    return json.dumps({
        "status": "not_implemented",
        "message": "Asset selection not yet implemented",
        "suggestion": "Use asset_keys parameter instead"
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def wipe_asset(
    project_id: str,
    asset_key: str
) -> str:
    """Wipe asset data

    Args:
        project_id: Project context ID
        asset_key: Asset key to wipe

    Returns:
        Wipe status confirmation
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.asset_wipe(asset_key)
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "status": "failure",
            "error": result["stderr"]
        }, indent=2)

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "asset_key": asset_key,
        "message": f"Asset '{asset_key}' wiped successfully"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 3: Job Execution
# ============================================================================

@mcp.tool()
@handle_dagster_errors
async def list_jobs(project_id: str) -> str:
    """List all jobs in a project

    Args:
        project_id: Project context ID

    Returns:
        List of jobs with metadata
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.job_list()
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "error": "CommandFailed",
            "message": result["stderr"]
        }, indent=2)

    # Parse output
    jobs = DagsterOutputParser.parse_job_list(result["stdout"])

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "count": len(jobs),
        "jobs": jobs
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def execute_job(
    project_id: str,
    job_name: str,
    run_config: dict = None,
    tags: dict = None
) -> str:
    """Execute a job

    Args:
        project_id: Project context ID
        job_name: Job name to execute
        run_config: Optional run configuration
        tags: Optional execution tags

    Returns:
        Execution status and run ID
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.job_execute(job_name)
    result = await execute_dagster_command(cmd, cwd=project.project_path, timeout=600)

    # Create run record
    run_id = str(uuid.uuid4())[:8]
    run = DagsterRun(
        run_id=run_id,
        project_id=project_id,
        status="SUCCESS" if result["success"] else "FAILURE",
        started_at=datetime.now().isoformat(),
        completed_at=datetime.now().isoformat() if result["success"] else None,
        logs=result["stdout"].split('\n') if result["stdout"] else [],
        metadata={"job_name": job_name, "run_config": run_config, "tags": tags}
    )

    runs[run_id] = run
    save_run_state(run)

    if not result["success"]:
        return json.dumps({
            "status": "failure",
            "run_id": run_id,
            "error": result["stderr"],
            "logs": result["stdout"]
        }, indent=2)

    return json.dumps({
        "status": "success",
        "run_id": run_id,
        "project_id": project_id,
        "job_name": job_name,
        "completed_at": run.completed_at
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def get_job_definition(
    project_id: str,
    job_name: str
) -> str:
    """Get job definition and metadata

    Args:
        project_id: Project context ID
        job_name: Job name

    Returns:
        Job definition with dependencies
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    # For now, return basic info
    # Full definition parsing would require more complex CLI output parsing
    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "job_name": job_name,
        "message": "Job definition retrieval not yet fully implemented",
        "suggestion": "Use Dagster UI or GraphQL API for detailed job definitions"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 4: Run Management
# ============================================================================

@mcp.tool()
@handle_dagster_errors
async def list_runs(
    project_id: str,
    job_name: str = None,
    status: str = None,
    limit: int = 50
) -> str:
    """List historical runs

    Args:
        project_id: Project context ID
        job_name: Optional job filter
        status: Optional status filter (SUCCESS, FAILURE, etc.)
        limit: Maximum runs to return

    Returns:
        List of runs with metadata
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.run_list(job_name=job_name, limit=limit)
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "error": "CommandFailed",
            "message": result["stderr"]
        }, indent=2)

    # Parse output
    runs_list = DagsterOutputParser.parse_run_list(result["stdout"])

    # Filter by status if provided
    if status:
        runs_list = [r for r in runs_list if r.get("status") == status]

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "job_name": job_name,
        "status_filter": status,
        "count": len(runs_list),
        "runs": runs_list
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def get_run_logs(
    project_id: str,
    run_id: str
) -> str:
    """Get logs for a run

    Args:
        project_id: Project context ID
        run_id: Run ID

    Returns:
        Run logs with structured output
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.run_logs(run_id)
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "error": "CommandFailed",
            "message": result["stderr"]
        }, indent=2)

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "run_id": run_id,
        "logs": result["stdout"]
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def get_run_report(
    project_id: str,
    run_id: str
) -> str:
    """Get detailed run report

    Args:
        project_id: Project context ID
        run_id: Run ID

    Returns:
        Detailed run report with metrics
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.run_report(run_id)
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "error": "CommandFailed",
            "message": result["stderr"]
        }, indent=2)

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "run_id": run_id,
        "report": result["stdout"]
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
def terminate_run(
    project_id: str,
    run_id: str
) -> str:
    """Terminate a running run

    Args:
        project_id: Project context ID
        run_id: Run ID to terminate

    Returns:
        Termination status
    """
    # For now, return not implemented
    # Actual termination would require GraphQL API or more complex CLI integration
    return json.dumps({
        "status": "not_implemented",
        "message": "Run termination not yet implemented",
        "suggestion": "Use Dagster UI or GraphQL API for run termination"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 5: Schedules & Sensors
# ============================================================================

@mcp.tool()
@handle_dagster_errors
async def list_schedules(project_id: str) -> str:
    """List all schedules

    Args:
        project_id: Project context ID

    Returns:
        List of schedules with next tick times
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.schedule_list()
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "error": "CommandFailed",
            "message": result["stderr"]
        }, indent=2)

    # Parse output
    schedules = DagsterOutputParser.parse_schedule_list(result["stdout"])

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "count": len(schedules),
        "schedules": schedules
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def trigger_schedule(
    project_id: str,
    schedule_name: str
) -> str:
    """Trigger a schedule manually

    Args:
        project_id: Project context ID
        schedule_name: Schedule name

    Returns:
        Trigger status and run ID
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.schedule_tick(schedule_name)
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    # Create run record
    run_id = str(uuid.uuid4())[:8]
    run = DagsterRun(
        run_id=run_id,
        project_id=project_id,
        status="SUCCESS" if result["success"] else "FAILURE",
        started_at=datetime.now().isoformat(),
        completed_at=datetime.now().isoformat() if result["success"] else None,
        logs=result["stdout"].split('\n') if result["stdout"] else [],
        metadata={"schedule_name": schedule_name}
    )

    runs[run_id] = run
    save_run_state(run)

    if not result["success"]:
        return json.dumps({
            "status": "failure",
            "run_id": run_id,
            "error": result["stderr"]
        }, indent=2)

    return json.dumps({
        "status": "success",
        "run_id": run_id,
        "project_id": project_id,
        "schedule_name": schedule_name,
        "completed_at": run.completed_at
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def list_sensors(project_id: str) -> str:
    """List all sensors

    Args:
        project_id: Project context ID

    Returns:
        List of sensors with status
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    project = projects[project_id]
    builder = DagsterCommandBuilder(project)

    # Execute command
    cmd = builder.sensor_list()
    result = await execute_dagster_command(cmd, cwd=project.project_path)

    if not result["success"]:
        return json.dumps({
            "error": "CommandFailed",
            "message": result["stderr"]
        }, indent=2)

    # Parse output
    sensors = DagsterOutputParser.parse_sensor_list(result["stdout"])

    return json.dumps({
        "status": "success",
        "project_id": project_id,
        "count": len(sensors),
        "sensors": sensors
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
async def trigger_sensor(
    project_id: str,
    sensor_name: str
) -> str:
    """Trigger a sensor manually

    Args:
        project_id: Project context ID
        sensor_name: Sensor name

    Returns:
        Trigger status
    """
    # For now, return not implemented
    return json.dumps({
        "status": "not_implemented",
        "message": "Sensor triggering not yet implemented",
        "suggestion": "Use Dagster UI or GraphQL API for sensor triggering"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 6: Lineage & Metadata
# ============================================================================

@mcp.tool()
@handle_dagster_errors
def get_asset_lineage(
    project_id: str,
    asset_key: str,
    direction: str = "both"
) -> str:
    """Get asset lineage information

    Args:
        project_id: Project context ID
        asset_key: Asset key
        direction: Lineage direction (upstream, downstream, both)

    Returns:
        Lineage graph with relationships
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    # For now, return placeholder
    # Full lineage would require GraphQL API or more complex CLI parsing
    return json.dumps({
        "status": "not_implemented",
        "message": "Asset lineage not yet implemented",
        "suggestion": "Use Dagster UI or GraphQL API for asset lineage"
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
def get_asset_metadata(
    project_id: str,
    asset_key: str
) -> str:
    """Get asset metadata and definitions

    Args:
        project_id: Project context ID
        asset_key: Asset key

    Returns:
        Asset metadata with partitions, dependencies
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    # For now, return placeholder
    return json.dumps({
        "status": "not_implemented",
        "message": "Asset metadata not yet implemented",
        "suggestion": "Use Dagster UI or GraphQL API for asset metadata"
    }, indent=2)


@mcp.tool()
@handle_dagster_errors
def get_job_dependencies(
    project_id: str,
    job_name: str
) -> str:
    """Get job dependency graph

    Args:
        project_id: Project context ID
        job_name: Job name

    Returns:
        Dependency graph with ops/jobs
    """
    if project_id not in projects:
        return json.dumps({
            "error": "ProjectNotFound",
            "message": f"Project ID not found: {project_id}"
        }, indent=2)

    # For now, return placeholder
    return json.dumps({
        "status": "not_implemented",
        "message": "Job dependencies not yet implemented",
        "suggestion": "Use Dagster UI or GraphQL API for job dependencies"
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
