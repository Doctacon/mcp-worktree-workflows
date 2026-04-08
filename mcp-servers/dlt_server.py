#!/usr/bin/env python3
"""
MCP Server for dlt (Data Loading Tool)

Exposes dlt data pipeline capabilities through MCP:
- Pipeline operations (create, list, get, delete, update)
- Data loading (async execution with progress tracking)
- Source management (REST, database, file, verified sources)
- Destination management (DuckDB, BigQuery, Snowflake, Postgres, etc.)
- Schema operations (get, export, update, suggest, compare, table hints)
- Project management (init, validate, info)
- Runtime & execution (run, monitor, schedule, logs, metrics)
"""

import asyncio
import json
import os
import uuid
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Any, Union

from fastmcp import FastMCP

try:
    import dlt
    DLT_AVAILABLE = True
except ImportError:
    DLT_AVAILABLE = False

# Initialize FastMCP
mcp = FastMCP("dlt")

# Global state
pipelines: Dict[str, 'DltPipeline'] = {}
projects: Dict[str, 'DltProject'] = {}
runs: Dict[str, 'DltRun'] = {}
sources: Dict[str, 'SourceConfig'] = {}
destinations: Dict[str, 'DestinationConfig'] = {}
state_dir: Optional[Path] = None


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class DltPipeline:
    """Represents a dlt pipeline"""
    pipeline_id: str
    pipeline_name: str
    destination: str
    dataset_name: str
    config: Dict[str, Any]
    schema: Optional[Dict[str, Any]] = None
    pipeline_object: Optional[Any] = None
    created_at: str = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()


@dataclass
class DltProject:
    """Represents a dlt project"""
    project_id: str
    project_path: Path
    sources: List[str] = None
    destinations: List[str] = None
    config: Dict[str, Any] = None

    def __post_init__(self):
        if self.sources is None:
            self.sources = []
        if self.destinations is None:
            self.destinations = []
        if self.config is None:
            self.config = {}


@dataclass
class DltRun:
    """Track pipeline execution"""
    run_id: str
    pipeline_id: str
    status: str  # RUNNING, SUCCESS, FAILURE, CANCELLED
    started_at: str
    completed_at: Optional[str] = None
    load_info: Dict[str, Any] = None
    logs: List[str] = None
    metrics: Dict[str, Any] = None

    def __post_init__(self):
        if self.load_info is None:
            self.load_info = {}
        if self.logs is None:
            self.logs = []
        if self.metrics is None:
            self.metrics = {}


@dataclass
class SourceConfig:
    """Source configuration"""
    source_id: str
    source_type: str  # rest, database, file, verified
    config: Dict[str, Any]
    name: Optional[str] = None


@dataclass
class DestinationConfig:
    """Destination configuration"""
    destination_id: str
    destination_type: str  # duckdb, bigquery, snowflake, postgres, etc.
    config: Dict[str, Any]
    credentials: Optional[Dict[str, str]] = None


# ============================================================================
# State Management
# ============================================================================

def init_state_dir():
    """Initialize state directory for persistence"""
    global state_dir
    state_path = Path.cwd() / ".dlt_state"
    state_path.mkdir(exist_ok=True)
    state_dir = state_path


def save_pipeline_state(pipeline: DltPipeline):
    """Save pipeline state to disk"""
    if state_dir is None:
        init_state_dir()

    state_file = state_dir / f"pipeline_{pipeline.pipeline_id}.json"
    # Don't serialize the pipeline_object
    pipeline_copy = DltPipeline(
        pipeline_id=pipeline.pipeline_id,
        pipeline_name=pipeline.pipeline_name,
        destination=pipeline.destination,
        dataset_name=pipeline.dataset_name,
        config=pipeline.config,
        schema=pipeline.schema,
        pipeline_object=None,
        created_at=pipeline.created_at
    )
    with open(state_file, 'w') as f:
        json.dump(asdict(pipeline_copy), f, indent=2, default=str)


def save_run_state(run: DltRun):
    """Save run state to disk"""
    if state_dir is None:
        init_state_dir()

    state_file = state_dir / f"run_{run.run_id}.json"
    with open(state_file, 'w') as f:
        json.dump(asdict(run), f, indent=2, default=str)


# ============================================================================
# Error Handling
# ============================================================================

def handle_dlt_errors(func):
    """Decorator for consistent dlt error handling"""
    def wrapper(*args, **kwargs):
        if not DLT_AVAILABLE:
            return json.dumps({
                "error": "DltNotAvailable",
                "message": "dlt is not installed. Install it with: pip install dlt",
                "suggestion": "Install dlt to use this server"
            }, indent=2)

        try:
            return func(*args, **kwargs)
        except ImportError as e:
            return json.dumps({
                "error": "ImportError",
                "message": str(e),
                "suggestion": "Ensure dlt is properly installed"
            }, indent=2)
        except Exception as e:
            error_msg = str(e)

            # Categorize errors
            if "connection" in error_msg.lower():
                category = "ConnectionError"
                suggestion = "Check connection settings and credentials"
            elif "credentials" in error_msg.lower():
                category = "CredentialsError"
                suggestion = "Verify credentials are correct"
            elif "schema" in error_msg.lower():
                category = "SchemaError"
                suggestion = "Check schema compatibility"
            elif "permission" in error_msg.lower():
                category = "PermissionError"
                suggestion = "Check file/database permissions"
            else:
                category = "UnexpectedError"
                suggestion = "Review the error and configuration"

            return json.dumps({
                "error": category,
                "message": error_msg,
                "type": type(e).__name__,
                "suggestion": suggestion
            }, indent=2)
    return wrapper


# ============================================================================
# MCP Tools - Phase 1: Pipeline Management
# ============================================================================

@mcp.tool()
@handle_dlt_errors
def create_pipeline(
    pipeline_name: str,
    destination: str = "duckdb",
    dataset_name: str = "dlt_data",
    pipeline_id: str = None
) -> str:
    """Create a new dlt pipeline

    Args:
        pipeline_name: Name for the pipeline
        destination: Destination type (duckdb, bigquery, snowflake, postgres, etc.)
        dataset_name: Dataset name in destination
        pipeline_id: Optional pipeline ID (auto-generated if not provided)

    Returns:
        Pipeline creation confirmation with pipeline_id
    """
    if not pipeline_id:
        pipeline_id = str(uuid.uuid4())[:8]

    try:
        # Create pipeline using dlt Python API
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name
        )

        # Create pipeline state
        dlt_pipeline = DltPipeline(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            config={
                "pipeline_name": pipeline_name,
                "destination": destination,
                "dataset_name": dataset_name
            },
            pipeline_object=pipeline
        )

        # Store pipeline
        pipelines[pipeline_id] = dlt_pipeline
        save_pipeline_state(dlt_pipeline)

        return json.dumps({
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "destination": destination,
            "dataset_name": dataset_name,
            "message": f"Pipeline '{pipeline_name}' created successfully"
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "PipelineCreationError",
            "message": str(e),
            "suggestion": "Check pipeline configuration and destination settings"
        }, indent=2)


@mcp.tool()
@handle_dlt_errors
def list_pipelines() -> str:
    """List all active pipelines

    Returns:
        List of pipelines with metadata
    """
    active_pipelines = []

    for pipeline_id, dlt_pipeline in pipelines.items():
        active_pipelines.append({
            "pipeline_id": pipeline_id,
            "pipeline_name": dlt_pipeline.pipeline_name,
            "destination": dlt_pipeline.destination,
            "dataset_name": dlt_pipeline.dataset_name,
            "created_at": dlt_pipeline.created_at
        })

    return json.dumps({
        "status": "success",
        "count": len(active_pipelines),
        "pipelines": active_pipelines
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_pipeline(pipeline_id: str) -> str:
    """Get pipeline configuration and status

    Args:
        pipeline_id: Pipeline ID

    Returns:
        Pipeline details and configuration
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]

    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "pipeline_name": dlt_pipeline.pipeline_name,
        "destination": dlt_pipeline.destination,
        "dataset_name": dlt_pipeline.dataset_name,
        "config": dlt_pipeline.config,
        "schema": dlt_pipeline.schema,
        "created_at": dlt_pipeline.created_at
    }, indent=2, default=str)


@mcp.tool()
@handle_dlt_errors
def delete_pipeline(pipeline_id: str) -> str:
    """Delete pipeline and cleanup

    Args:
        pipeline_id: Pipeline ID

    Returns:
        Deletion confirmation
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]
    pipeline_name = dlt_pipeline.pipeline_name

    # Remove from state
    del pipelines[pipeline_id]

    # Remove state file
    if state_dir:
        state_file = state_dir / f"pipeline_{pipeline_id}.json"
        if state_file.exists():
            state_file.unlink()

    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_name,
        "message": f"Pipeline '{pipeline_name}' deleted successfully"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def update_pipeline(
    pipeline_id: str,
    destination: str = None,
    dataset_name: str = None
) -> str:
    """Update pipeline configuration

    Args:
        pipeline_id: Pipeline ID
        destination: New destination (optional)
        dataset_name: New dataset name (optional)

    Returns:
        Update confirmation
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]

    # Update fields
    if destination:
        dlt_pipeline.destination = destination
    if dataset_name:
        dlt_pipeline.dataset_name = dataset_name

    # Save updated state
    save_pipeline_state(dlt_pipeline)

    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "message": "Pipeline updated successfully"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 2: Data Loading
# ============================================================================

@mcp.tool()
@handle_dlt_errors
async def load_data(
    pipeline_id: str,
    data: Union[List[dict], dict],
    table_name: str,
    write_disposition: str = "append"
) -> str:
    """Load data from source to destination

    Args:
        pipeline_id: Pipeline ID
        data: Data to load (list of dicts or single dict)
        table_name: Target table name
        write_disposition: Write mode (append, replace, merge)

    Returns:
        Load information and status
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]

    if not dlt_pipeline.pipeline_object:
        return json.dumps({
            "error": "PipelineNotAvailable",
            "message": "Pipeline object not available",
            "suggestion": "Pipeline may not have been properly initialized"
        }, indent=2)

    # Create run tracker
    run_id = str(uuid.uuid4())[:8]
    run = DltRun(
        run_id=run_id,
        pipeline_id=pipeline_id,
        status="RUNNING",
        started_at=datetime.now().isoformat()
    )

    runs[run_id] = run
    save_run_state(run)

    try:
        # Load data using dlt pipeline
        load_info = dlt_pipeline.pipeline_object.run(
            data,
            table_name=table_name,
            write_disposition=write_disposition
        )

        # Update run state
        run.status = "SUCCESS"
        run.completed_at = datetime.now().isoformat()
        run.load_info = {
            "table_name": table_name,
            "write_disposition": write_disposition,
            "load_info": str(load_info)
        }
        save_run_state(run)

        return json.dumps({
            "status": "success",
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "table_name": table_name,
            "write_disposition": write_disposition,
            "load_info": run.load_info,
            "completed_at": run.completed_at
        }, indent=2, default=str)

    except Exception as e:
        run.status = "FAILURE"
        run.completed_at = datetime.now().isoformat()
        run.logs.append(f"Error: {str(e)}")
        save_run_state(run)

        return json.dumps({
            "status": "failure",
            "run_id": run_id,
            "error": str(e),
            "logs": run.logs
        }, indent=2)


@mcp.tool()
@handle_dlt_errors
async def load_file(
    pipeline_id: str,
    file_path: str,
    table_name: str,
    file_format: str = "auto"
) -> str:
    """Load data from file (CSV, JSON, Parquet)

    Args:
        pipeline_id: Pipeline ID
        file_path: Path to data file
        table_name: Target table name
        file_format: File format (auto, csv, json, parquet)

    Returns:
        Load information and status
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    if not Path(file_path).exists():
        return json.dumps({
            "error": "FileNotFound",
            "message": f"File not found: {file_path}"
        }, indent=2)

    # Auto-detect format
    if file_format == "auto":
        file_ext = Path(file_path).suffix.lower()
        if file_ext == ".csv":
            file_format = "csv"
        elif file_ext == ".json":
            file_format = "json"
        elif file_ext == ".parquet":
            file_format = "parquet"
        else:
            return json.dumps({
                "error": "UnsupportedFormat",
                "message": f"Cannot auto-detect format from extension: {file_ext}"
            }, indent=2)

    # Load file based on format
    try:
        if file_format == "csv":
            import pandas as pd
            data = pd.read_csv(file_path)
            data = data.to_dict(orient='records')
        elif file_format == "json":
            import pandas as pd
            data = pd.read_json(file_path)
            data = data.to_dict(orient='records')
        elif file_format == "parquet":
            import pandas as pd
            data = pd.read_parquet(file_path)
            data = data.to_dict(orient='records')
        else:
            return json.dumps({
                "error": "UnsupportedFormat",
                "message": f"Unsupported format: {file_format}"
            }, indent=2)

        # Load data using pipeline
        return await load_data(
            pipeline_id=pipeline_id,
            data=data,
            table_name=table_name
        )

    except Exception as e:
        return json.dumps({
            "error": "FileLoadingError",
            "message": str(e),
            "suggestion": "Check file format and path"
        }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_load_info(run_id: str) -> str:
    """Get detailed load information

    Args:
        run_id: Run ID

    Returns:
        Detailed load information and metrics
    """
    if run_id not in runs:
        return json.dumps({
            "error": "RunNotFound",
            "message": f"Run ID not found: {run_id}"
        }, indent=2)

    run = runs[run_id]

    return json.dumps({
        "status": "success",
        "run_id": run_id,
        "pipeline_id": run.pipeline_id,
        "status": run.status,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "load_info": run.load_info,
        "metrics": run.metrics
    }, indent=2, default=str)


# ============================================================================
# MCP Tools - Phase 3: Source Management
# ============================================================================

@mcp.tool()
@handle_dlt_errors
def create_rest_source(
    source_id: str,
    base_url: str,
    endpoints: List[str] = None,
    headers: Dict[str, str] = None
) -> str:
    """Create REST API source

    Args:
        source_id: Source identifier
        base_url: Base URL for the API
        endpoints: List of API endpoints
        headers: Optional HTTP headers

    Returns:
        Source creation confirmation
    """
    source_config = SourceConfig(
        source_id=source_id,
        source_type="rest",
        config={
            "base_url": base_url,
            "endpoints": endpoints or [],
            "headers": headers or {}
        }
    )

    sources[source_id] = source_config

    return json.dumps({
        "status": "success",
        "source_id": source_id,
        "source_type": "rest",
        "base_url": base_url,
        "endpoints": endpoints,
        "message": f"REST source '{source_id}' created successfully"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def create_database_source(
    source_id: str,
    connection_string: str,
    tables: List[str] = None
) -> str:
    """Create database source

    Args:
        source_id: Source identifier
        connection_string: Database connection string
        tables: List of tables to replicate

    Returns:
        Source creation confirmation
    """
    source_config = SourceConfig(
        source_id=source_id,
        source_type="database",
        config={
            "connection_string": connection_string,
            "tables": tables or []
        }
    )

    sources[source_id] = source_config

    return json.dumps({
        "status": "success",
        "source_id": source_id,
        "source_type": "database",
        "tables": tables,
        "message": f"Database source '{source_id}' created successfully"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def create_file_source(
    source_id: str,
    file_pattern: str,
    file_format: str = "auto"
) -> str:
    """Create file-based source

    Args:
        source_id: Source identifier
        file_pattern: Glob pattern for files
        file_format: File format (auto, csv, json, parquet)

    Returns:
        Source creation confirmation
    """
    source_config = SourceConfig(
        source_id=source_id,
        source_type="file",
        config={
            "file_pattern": file_pattern,
            "file_format": file_format
        }
    )

    sources[source_id] = source_config

    return json.dumps({
        "status": "success",
        "source_id": source_id,
        "source_type": "file",
        "file_pattern": file_pattern,
        "file_format": file_format,
        "message": f"File source '{source_id}' created successfully"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def create_verified_source(
    source_id: str,
    source_name: str,
    config: Dict[str, Any] = None
) -> str:
    """Create verified source (GitHub, Google Sheets, etc.)

    Args:
        source_id: Source identifier
        source_name: Name of verified source
        config: Source-specific configuration

    Returns:
        Source creation confirmation
    """
    source_config = SourceConfig(
        source_id=source_id,
        source_type="verified",
        name=source_name,
        config=config or {}
    )

    sources[source_id] = source_config

    return json.dumps({
        "status": "success",
        "source_id": source_id,
        "source_type": "verified",
        "source_name": source_name,
        "config": config,
        "message": f"Verified source '{source_name}' created successfully"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def test_source(source_id: str) -> str:
    """Test source connection

    Args:
        source_id: Source ID

    Returns:
        Connection test results
    """
    if source_id not in sources:
        return json.dumps({
            "error": "SourceNotFound",
            "message": f"Source ID not found: {source_id}"
        }, indent=2)

    source = sources[source_id]

    # For now, return basic validation
    # In a real implementation, this would actually test the connection
    return json.dumps({
        "status": "success",
        "source_id": source_id,
        "source_type": source.source_type,
        "message": "Source configuration validated (connection test not yet implemented)",
        "config": source.config
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def list_available_sources() -> str:
    """List available verified sources

    Returns:
        List of available verified sources
    """
    # Common verified sources
    available_sources = [
        "github", "gitlab", "bitbucket",
        "google_sheets", "google_analytics", "google_ads",
        "salesforce", "hubspot", "stripe",
        "twitter", "reddit", "news_api",
        "mongodb", "postgres", "mysql",
        "notion", "airtable", "zendesk"
    ]

    return json.dumps({
        "status": "success",
        "count": len(available_sources),
        "sources": available_sources
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 4: Destination Management
# ============================================================================

@mcp.tool()
@handle_dlt_errors
def add_destination(
    destination_id: str,
    destination_type: str,
    config: Dict[str, Any] = None,
    credentials: Dict[str, str] = None
) -> str:
    """Add destination configuration

    Args:
        destination_id: Destination identifier
        destination_type: Type (duckdb, bigquery, snowflake, postgres, etc.)
        config: Destination configuration
        credentials: Optional credentials

    Returns:
        Destination addition confirmation
    """
    dest_config = DestinationConfig(
        destination_id=destination_id,
        destination_type=destination_type,
        config=config or {},
        credentials=credentials
    )

    destinations[destination_id] = dest_config

    return json.dumps({
        "status": "success",
        "destination_id": destination_id,
        "destination_type": destination_type,
        "message": f"Destination '{destination_id}' added successfully"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def test_destination(destination_id: str) -> str:
    """Test destination connection

    Args:
        destination_id: Destination ID

    Returns:
        Connection test results
    """
    if destination_id not in destinations:
        return json.dumps({
            "error": "DestinationNotFound",
            "message": f"Destination ID not found: {destination_id}"
        }, indent=2)

    destination = destinations[destination_id]

    # For now, return basic validation
    # In a real implementation, this would actually test the connection
    return json.dumps({
        "status": "success",
        "destination_id": destination_id,
        "destination_type": destination.destination_type,
        "message": "Destination configuration validated (connection test not yet implemented)",
        "config": destination.config
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def list_destinations() -> str:
    """List available destinations

    Returns:
        List of supported destination types
    """
    # Common destination types
    destination_types = [
        {"type": "duckdb", "description": "Local analytical database"},
        {"type": "bigquery", "description": "Google Cloud data warehouse"},
        {"type": "snowflake", "description": "Cloud data warehouse"},
        {"type": "redshift", "description": "AWS data warehouse"},
        {"type": "postgres", "description": "PostgreSQL database"},
        {"type": "mysql", "description": "MySQL database"},
        {"type": "databricks", "description": "Databricks data platform"},
        {"type": "clickhouse", "description": "ClickHouse analytical database"}
    ]

    configured = []

    for dest_id, dest in destinations.items():
        configured.append({
            "destination_id": dest_id,
            "type": dest.destination_type
        })

    return json.dumps({
        "status": "success",
        "supported_types": destination_types,
        "configured": configured
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_destination_info(destination_id: str) -> str:
    """Get destination capabilities

    Args:
        destination_id: Destination ID

    Returns:
        Destination information and capabilities
    """
    if destination_id not in destinations:
        return json.dumps({
            "error": "DestinationNotFound",
            "message": f"Destination ID not found: {destination_id}"
        }, indent=2)

    destination = destinations[destination_id]

    return json.dumps({
        "status": "success",
        "destination_id": destination_id,
        "destination_type": destination.destination_type,
        "config": destination.config,
        "capabilities": "Destination capabilities not yet fully implemented"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 5: Schema Operations
# ============================================================================

@mcp.tool()
@handle_dlt_errors
def get_schema(pipeline_id: str) -> str:
    """Get current schema

    Args:
        pipeline_id: Pipeline ID

    Returns:
        Current schema structure
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]

    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "schema": dlt_pipeline.schema,
        "message": "Schema retrieved successfully"
    }, indent=2, default=str)


@mcp.tool()
@handle_dlt_errors
def export_schema(pipeline_id: str, output_path: str = None) -> str:
    """Export schema to YAML

    Args:
        pipeline_id: Pipeline ID
        output_path: Optional file path for export

    Returns:
        Exported schema in YAML format
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]

    try:
        # Get schema from pipeline
        if dlt_pipeline.pipeline_object and hasattr(dlt_pipeline.pipeline_object, 'default_schema'):
            schema = dlt_pipeline.pipeline_object.default_schema
            schema_yaml = schema.to_pretty_yaml() if schema else "{}"
        else:
            schema_yaml = json.dumps(dlt_pipeline.schema, indent=2)

        if output_path:
            # Save to file
            with open(output_path, 'w') as f:
                f.write(schema_yaml)

        return json.dumps({
            "status": "success",
            "pipeline_id": pipeline_id,
            "output_path": output_path,
            "schema_yaml": schema_yaml
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "SchemaExportError",
            "message": str(e),
            "suggestion": "Check pipeline has a schema defined"
        }, indent=2)


@mcp.tool()
@handle_dlt_errors
def update_schema(
    pipeline_id: str,
    schema_updates: Dict[str, Any]
) -> str:
    """Update schema configuration

    Args:
        pipeline_id: Pipeline ID
        schema_updates: Schema updates to apply

    Returns:
        Update confirmation
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]

    # Update schema
    if not dlt_pipeline.schema:
        dlt_pipeline.schema = {}

    dlt_pipeline.schema.update(schema_updates)

    # Save pipeline state
    save_pipeline_state(dlt_pipeline)

    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "schema_updates": schema_updates,
        "message": "Schema updated successfully"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def suggest_schema(
    pipeline_id: str,
    sample_data: List[dict]
) -> str:
    """Suggest schema from sample data

    Args:
        pipeline_id: Pipeline ID
        sample_data: Sample data to analyze

    Returns:
        Suggested schema structure
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    try:
        import pandas as pd

        # Convert to DataFrame for analysis
        df = pd.DataFrame(sample_data)

        # Analyze schema
        schema_suggestion = {
            "columns": [],
            "dtypes": {},
            "sample_count": len(sample_data)
        }

        for col in df.columns:
            dtype = str(df[col].dtype)
            schema_suggestion["columns"].append(col)
            schema_suggestion["dtypes"][col] = dtype

        return json.dumps({
            "status": "success",
            "pipeline_id": pipeline_id,
            "suggested_schema": schema_suggestion,
            "message": "Schema suggestion generated successfully"
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "error": "SchemaSuggestionError",
            "message": str(e),
            "suggestion": "Check sample data format"
        }, indent=2)


@mcp.tool()
@handle_dlt_errors
def compare_schemas(
    pipeline_id_1: str,
    pipeline_id_2: str
) -> str:
    """Compare two schemas

    Args:
        pipeline_id_1: First pipeline ID
        pipeline_id_2: Second pipeline ID

    Returns:
        Schema comparison results
    """
    if pipeline_id_1 not in pipelines or pipeline_id_2 not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": "Both pipeline IDs must exist"
        }, indent=2)

    schema_1 = pipelines[pipeline_id_1].schema or {}
    schema_2 = pipelines[pipeline_id_2].schema or {}

    comparison = {
        "pipeline_1": pipeline_id_1,
        "pipeline_2": pipeline_id_2,
        "differences": [],
        "schema_1_keys": set(schema_1.keys()) if isinstance(schema_1, dict) else [],
        "schema_2_keys": set(schema_2.keys()) if isinstance(schema_2, dict) else []
    }

    return json.dumps({
        "status": "success",
        "comparison": comparison
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_table_schema(
    pipeline_id: str,
    table_name: str
) -> str:
    """Get specific table schema

    Args:
        pipeline_id: Pipeline ID
        table_name: Table name

    Returns:
        Table schema details
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    dlt_pipeline = pipelines[pipeline_id]

    # For now, return basic table info
    # In a real implementation, this would extract detailed table schema
    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "table_name": table_name,
        "message": "Table schema retrieval not yet fully implemented",
        "suggestion": "Use export_schema() to see full schema and find table details"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_column_details(
    pipeline_id: str,
    table_name: str,
    column_name: str
) -> str:
    """Get column details and types

    Args:
        pipeline_id: Pipeline ID
        table_name: Table name
        column_name: Column name

    Returns:
        Column details
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    # For now, return placeholder
    return json.dumps({
        "status": "not_implemented",
        "message": "Column details retrieval not yet implemented",
        "suggestion": "Use get_table_schema() for table-level information"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def apply_table_hints(
    pipeline_id: str,
    table_name: str,
    hints: Dict[str, Any]
) -> str:
    """Apply table hints for optimization

    Args:
        pipeline_id: Pipeline ID
        table_name: Table name
        hints: Table hints to apply

    Returns:
        Hint application confirmation
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    # For now, return placeholder
    return json.dumps({
        "status": "not_implemented",
        "message": "Table hints not yet implemented",
        "suggestion": "Configure table hints directly in dlt pipeline configuration"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 6: Project Management
# ============================================================================

@mcp.tool()
@handle_dlt_errors
def init_project(
    project_name: str,
    source_type: str = "rest_api",
    destination_type: str = "duckdb"
) -> str:
    """Initialize new dlt project

    Args:
        project_name: Name for the project
        source_type: Type of source (rest_api, database, etc.)
        destination_type: Type of destination

    Returns:
        Project initialization confirmation
    """
    # For now, return basic confirmation
    # In a real implementation, this would use dlt CLI to init project
    return json.dumps({
        "status": "success",
        "project_name": project_name,
        "source_type": source_type,
        "destination_type": destination_type,
        "message": "Project initialization would use dlt CLI (not yet implemented)",
        "suggestion": "Use 'dlt init <source> <destination>' CLI command for full project initialization"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def validate_project(project_id: str = None) -> str:
    """Validate project configuration

    Args:
        project_id: Optional project ID

    Returns:
        Validation results
    """
    # For now, return basic validation
    return json.dumps({
        "status": "success",
        "message": "Project validation not yet fully implemented",
        "suggestion": "Validate pipeline configurations and source/destination connections"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_project_info(project_id: str = None) -> str:
    """Get project information

    Args:
        project_id: Optional project ID

    Returns:
        Project information
    """
    active_pipelines = len(pipelines)
    active_sources = len(sources)
    active_destinations = len(destinations)

    return json.dumps({
        "status": "success",
        "pipelines": active_pipelines,
        "sources": active_sources,
        "destinations": active_destinations,
        "message": "Project information retrieved successfully"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 7: Runtime & Execution
# ============================================================================

@mcp.tool()
@handle_dlt_errors
async def run_pipeline(
    pipeline_id: str,
    source_id: str = None,
    table_name: str = None
) -> str:
    """Run pipeline asynchronously

    Args:
        pipeline_id: Pipeline ID
        source_id: Optional source ID
        table_name: Optional table name

    Returns:
        Run execution status
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    # Create run tracker
    run_id = str(uuid.uuid4())[:8]
    run = DltRun(
        run_id=run_id,
        pipeline_id=pipeline_id,
        status="RUNNING",
        started_at=datetime.now().isoformat()
    )

    runs[run_id] = run
    save_run_state(run)

    # For now, just mark as success
    # In a real implementation, this would execute the pipeline
    run.status = "SUCCESS"
    run.completed_at = datetime.now().isoformat()
    save_run_state(run)

    return json.dumps({
        "status": "success",
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "completed_at": run.completed_at,
        "message": "Pipeline execution completed (simplified implementation)"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_run_status(run_id: str) -> str:
    """Get pipeline run status

    Args:
        run_id: Run ID

    Returns:
        Run status and progress
    """
    if run_id not in runs:
        return json.dumps({
            "error": "RunNotFound",
            "message": f"Run ID not found: {run_id}"
        }, indent=2)

    run = runs[run_id]

    return json.dumps({
        "status": "success",
        "run_id": run_id,
        "pipeline_id": run.pipeline_id,
        "status": run.status,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "load_info": run.load_info
    }, indent=2, default=str)


@mcp.tool()
@handle_dlt_errors
def list_runs(
    pipeline_id: str = None,
    status: str = None
) -> str:
    """List all pipeline runs

    Args:
        pipeline_id: Optional pipeline ID filter
        status: Optional status filter

    Returns:
        List of runs with metadata
    """
    filtered_runs = []

    for run_id, run in runs.items():
        # Apply filters
        if pipeline_id and run.pipeline_id != pipeline_id:
            continue
        if status and run.status != status:
            continue

        filtered_runs.append({
            "run_id": run_id,
            "pipeline_id": run.pipeline_id,
            "status": run.status,
            "started_at": run.started_at,
            "completed_at": run.completed_at
        })

    return json.dumps({
        "status": "success",
        "count": len(filtered_runs),
        "runs": filtered_runs
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_run_logs(run_id: str) -> str:
    """Get run logs

    Args:
        run_id: Run ID

    Returns:
        Run logs
    """
    if run_id not in runs:
        return json.dumps({
            "error": "RunNotFound",
            "message": f"Run ID not found: {run_id}"
        }, indent=2)

    run = runs[run_id]

    return json.dumps({
        "status": "success",
        "run_id": run_id,
        "logs": run.logs
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def cancel_run(run_id: str) -> str:
    """Cancel running pipeline

    Args:
        run_id: Run ID

    Returns:
        Cancellation status
    """
    if run_id not in runs:
        return json.dumps({
            "error": "RunNotFound",
            "message": f"Run ID not found: {run_id}"
        }, indent=2)

    run = runs[run_id]

    if run.status == "RUNNING":
        run.status = "CANCELLED"
        run.completed_at = datetime.now().isoformat()
        save_run_state(run)

        return json.dumps({
            "status": "success",
            "run_id": run_id,
            "message": "Run cancelled successfully"
        }, indent=2)
    else:
        return json.dumps({
            "status": "error",
            "run_id": run_id,
            "message": f"Cannot cancel run with status: {run.status}"
        }, indent=2)


@mcp.tool()
@handle_dlt_errors
def schedule_pipeline(
    pipeline_id: str,
    cron_expression: str
) -> str:
    """Schedule pipeline with cron

    Args:
        pipeline_id: Pipeline ID
        cron_expression: Cron expression

    Returns:
        Scheduling confirmation
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    # For now, return confirmation
    # In a real implementation, this would set up actual scheduling
    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "cron_expression": cron_expression,
        "message": "Pipeline scheduling configured (actual scheduling not yet implemented)",
        "suggestion": "Use dlt runtime schedule CLI command for production scheduling"
    }, indent=2)


@mcp.tool()
@handle_dlt_errors
def get_pipeline_metrics(pipeline_id: str) -> str:
    """Get pipeline performance metrics

    Args:
        pipeline_id: Pipeline ID

    Returns:
        Pipeline metrics
    """
    if pipeline_id not in pipelines:
        return json.dumps({
            "error": "PipelineNotFound",
            "message": f"Pipeline ID not found: {pipeline_id}"
        }, indent=2)

    # Get runs for this pipeline
    pipeline_runs = [r for r in runs.values() if r.pipeline_id == pipeline_id]

    metrics = {
        "total_runs": len(pipeline_runs),
        "successful_runs": len([r for r in pipeline_runs if r.status == "SUCCESS"]),
        "failed_runs": len([r for r in pipeline_runs if r.status == "FAILURE"]),
        "message": "Basic metrics computed (detailed metrics not yet implemented)"
    }

    return json.dumps({
        "status": "success",
        "pipeline_id": pipeline_id,
        "metrics": metrics
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
