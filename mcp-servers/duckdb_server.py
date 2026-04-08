#!/usr/bin/env python3
"""
MCP Server for DuckDB

Exposes DuckDB analytical database capabilities through MCP:
- Multi-connection management (in-memory and file-based)
- Query execution with multiple result formats
- Direct file querying (CSV, Parquet, JSON)
- Schema inspection and management
- Data import/export
- Query explanation and optimization
- Extension management
- Progress monitoring
- Remote file access (HTTP, S3, GCS, Azure)
- Pandas integration
"""

import asyncio
import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Any, Union

from fastmcp import FastMCP

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Initialize FastMCP
mcp = FastMCP("duckdb")

# Global state
connections: Dict[str, 'duckdb.DuckDBPyConnection'] = {}
connection_metadata: Dict[str, 'ConnectionMetadata'] = {}
schema_cache: Dict[str, 'SchemaInfo'] = {}
state_dir: Optional[Path] = None


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class ConnectionMetadata:
    """Metadata for a connection"""
    name: str
    database_path: str
    created_at: str
    last_used: str
    is_read_only: bool
    query_count: int = 0
    current_query: Optional[str] = None
    query_start_time: Optional[str] = None


@dataclass
class SchemaInfo:
    """Cached schema information"""
    connection_name: str
    table_name: str
    columns: List[Dict[str, str]]
    row_count: int
    cached_at: str
    schema_hash: str


# ============================================================================
# Error Handling
# ============================================================================

def handle_duckdb_errors(func):
    """Decorator for consistent DuckDB error handling"""
    def wrapper(*args, **kwargs):
        if not DUCKDB_AVAILABLE:
            return json.dumps({
                "error": "DuckDBNotAvailable",
                "message": "DuckDB is not installed. Install it with: pip install duckdb",
                "suggestion": "Install DuckDB to use this server"
            }, indent=2)

        try:
            return func(*args, **kwargs)
        except duckdb.Error as e:
            error_type = type(e).__name__
            error_msg = str(e)

            # Categorize errors and provide suggestions
            if "IOException" in error_type or "File" in error_msg:
                category = "IOError"
                suggestion = "Check file paths and permissions"
            elif "Catalog" in error_type or "Table" in error_msg or "Column" in error_msg:
                category = "CatalogError"
                suggestion = "Verify table/column names exist in the database"
            elif "Binder" in error_type:
                category = "BinderError"
                suggestion = "Check SQL syntax and column references"
            elif "Parser" in error_type or "syntax" in error_msg.lower():
                category = "ParserError"
                suggestion = "Check SQL syntax for typos or missing keywords"
            else:
                category = "DatabaseError"
                suggestion = "Review the query and database state"

            return json.dumps({
                "error": category,
                "message": error_msg,
                "type": error_type,
                "suggestion": suggestion
            }, indent=2)
        except FileNotFoundError as e:
            return json.dumps({
                "error": "FileNotFoundError",
                "message": str(e),
                "suggestion": "Check that the file path is correct"
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
    state_path = Path.cwd() / ".duckdb_state"
    state_path.mkdir(exist_ok=True)
    state_dir = state_path


def save_connection_state(connection_name: str):
    """Save connection state to disk"""
    if state_dir is None:
        init_state_dir()

    if connection_name in connection_metadata:
        state_file = state_dir / f"connection_{connection_name}.json"
        with open(state_file, 'w') as f:
            json.dump(asdict(connection_metadata[connection_name]), f, indent=2, default=str)


def invalidate_schema_cache(connection_name: str, table_name: str = None):
    """Invalidate schema cache for a table or connection"""
    global schema_cache

    if table_name:
        # Invalidate specific table
        cache_key = f"{connection_name}.{table_name}"
        if cache_key in schema_cache:
            del schema_cache[cache_key]
    else:
        # Invalidate all tables for connection
        keys_to_delete = [k for k in schema_cache.keys() if k.startswith(f"{connection_name}.")]
        for key in keys_to_delete:
            del schema_cache[key]


# ============================================================================
# MCP Tools - Phase 1: Connection Management
# ============================================================================

@mcp.tool()
@handle_duckdb_errors
def connect(
    connection_name: str = "default",
    database_path: str = ":memory:",
    read_only: bool = False
) -> str:
    """Create a new DuckDB connection

    Args:
        connection_name: Name for this connection (default: 'default')
        database_path: Path to database file or ':memory:' (default: ':memory:')
        read_only: Open in read-only mode (default: False)

    Returns:
        Connection details and success status
    """
    if connection_name in connections:
        return json.dumps({
            "error": "ConnectionExists",
            "message": f"Connection '{connection_name}' already exists",
            "suggestion": f"Use disconnect('{connection_name}') first or choose a different name"
        }, indent=2)

    # Create connection
    con = duckdb.connect(database=database_path, read_only=read_only)

    # Store connection
    connections[connection_name] = con

    # Create metadata
    metadata = ConnectionMetadata(
        name=connection_name,
        database_path=database_path,
        created_at=datetime.now().isoformat(),
        last_used=datetime.now().isoformat(),
        is_read_only=read_only
    )
    connection_metadata[connection_name] = metadata

    # Save state
    save_connection_state(connection_name)

    return json.dumps({
        "status": "success",
        "connection_name": connection_name,
        "database_path": database_path,
        "read_only": read_only,
        "created_at": metadata.created_at,
        "instructions": f"Connection '{connection_name}' created. Use this name for subsequent operations."
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def disconnect(connection_name: str = "default") -> str:
    """Close a specific connection

    Args:
        connection_name: Name of connection to close

    Returns:
        Confirmation of disconnection
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found",
            "suggestion": f"Use list_connections() to see available connections"
        }, indent=2)

    # Close connection
    connections[connection_name].close()

    # Get metadata before removing
    metadata = connection_metadata[connection_name]

    # Remove from state
    del connections[connection_name]
    del connection_metadata[connection_name]

    # Invalidate schema cache
    invalidate_schema_cache(connection_name)

    # Remove state file
    if state_dir:
        state_file = state_dir / f"connection_{connection_name}.json"
        if state_file.exists():
            state_file.unlink()

    return json.dumps({
        "status": "success",
        "connection_name": connection_name,
        "message": f"Connection '{connection_name}' closed successfully",
        "was_active_since": metadata.created_at,
        "query_count": metadata.query_count
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def list_connections() -> str:
    """List all active connections

    Returns:
        List of active connections with metadata
    """
    active_connections = []

    for name, metadata in connection_metadata.items():
        if name in connections:
            active_connections.append({
                "name": name,
                "database_path": metadata.database_path,
                "created_at": metadata.created_at,
                "last_used": metadata.last_used,
                "is_read_only": metadata.is_read_only,
                "query_count": metadata.query_count,
                "current_query": metadata.current_query
            })

    return json.dumps({
        "status": "success",
        "count": len(active_connections),
        "connections": active_connections
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def get_connection_info(connection_name: str = "default") -> str:
    """Get detailed information about a connection

    Args:
        connection_name: Name of connection

    Returns:
        Connection metadata and database info
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]
    metadata = connection_metadata[connection_name]

    # Get database info
    try:
        db_info = con.execute("PRAGMA database_info").fetchdf()
        db_info_dict = db_info.to_dict(orient='records') if PANDAS_AVAILABLE else []
    except:
        db_info_dict = []

    return json.dumps({
        "status": "success",
        "connection_name": connection_name,
        "metadata": asdict(metadata),
        "database_info": db_info_dict
    }, indent=2, default=str)


# ============================================================================
# MCP Tools - Phase 2: Query Execution
# ============================================================================

@mcp.tool()
@handle_duckdb_errors
def execute_query(
    sql: str,
    connection_name: str = "default",
    format: str = "json",
    limit: int = 100
) -> str:
    """Execute SQL query and return results

    Args:
        sql: SQL query to execute
        connection_name: Connection to use (default: 'default')
        format: Output format - 'json', 'csv', 'markdown' (default: 'json')
        limit: Maximum rows to return (default: 100, use -1 for unlimited)

    Returns:
        Query results in specified format
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]
    metadata = connection_metadata[connection_name]

    # Update metadata
    metadata.current_query = sql[:100] + "..." if len(sql) > 100 else sql
    metadata.query_start_time = datetime.now().isoformat()
    metadata.last_used = datetime.now().isoformat()

    try:
        # Execute query
        result = con.execute(sql)

        # Fetch results
        if limit == -1:
            df = result.fetchdf()
        else:
            df = result.limit(limit).fetchdf()

        # Update metadata
        metadata.query_count += 1
        metadata.current_query = None
        metadata.query_start_time = None
        save_connection_state(connection_name)

        # Format output
        if format == "csv":
            output = df.to_csv(index=False)
            return json.dumps({
                "status": "success",
                "format": "csv",
                "row_count": len(df),
                "data": output
            }, indent=2)
        elif format == "markdown":
            output = df.to_markdown(index=False)
            return json.dumps({
                "status": "success",
                "format": "markdown",
                "row_count": len(df),
                "data": output
            }, indent=2)
        else:  # json
            data = df.to_dict(orient='records')
            return json.dumps({
                "status": "success",
                "format": "json",
                "row_count": len(df),
                "columns": list(df.columns),
                "data": data
            }, indent=2, default=str)

    except Exception as e:
        metadata.current_query = None
        metadata.query_start_time = None
        raise


@mcp.tool()
@handle_duckdb_errors
def query_file(
    file_path: str,
    sql: str = "",
    connection_name: str = "default",
    format: str = "json"
) -> str:
    """Query a file directly (CSV, Parquet, JSON)

    Args:
        file_path: Path to data file
        sql: Optional WHERE/filter clause (default: "")
        connection_name: Connection to use (default: 'default')
        format: Output format (default: 'json')

    Returns:
        Query results from file
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Auto-detect format from extension
    file_path_obj = Path(file_path)
    extension = file_path_obj.suffix.lower()

    if extension == '.csv':
        query = f"SELECT * FROM read_csv('{file_path}')"
    elif extension == '.parquet':
        query = f"SELECT * FROM read_parquet('{file_path}')"
    elif extension == '.json':
        query = f"SELECT * FROM read_json('{file_path}')"
    else:
        return json.dumps({
            "error": "UnsupportedFormat",
            "message": f"Unsupported file format: {extension}",
            "suggestion": "Supported formats: .csv, .parquet, .json"
        }, indent=2)

    # Add custom SQL if provided
    if sql:
        query = f"SELECT * FROM ({query}) AS subq WHERE {sql}"

    # Execute query
    return execute_query(query, connection_name, format)


@mcp.tool()
@handle_duckdb_errors
def query_relational(
    table_name: str,
    filter_expr: str = "",
    columns: str = "*",
    aggregate: str = "",
    connection_name: str = "default",
    format: str = "json"
) -> str:
    """Query using DuckDB's relational API

    Args:
        table_name: Table to query
        filter_expr: Filter expression (e.g., "age > 25")
        columns: Columns to select (default: "*")
        aggregate: Aggregation expression (e.g., "SUM(amount)")
        connection_name: Connection to use (default: 'default')
        format: Output format (default: 'json')

    Returns:
        Query results using relational API
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Build relational query
    rel = con.table(table_name)

    if filter_expr:
        rel = rel.filter(filter_expr)

    if aggregate:
        rel = rel.aggregate(aggregate)
    else:
        rel = rel.project(columns)

    # Fetch results
    df = rel.fetchdf()

    # Format output
    if format == "csv":
        output = df.to_csv(index=False)
        return json.dumps({
            "status": "success",
            "format": "csv",
            "row_count": len(df),
            "data": output
        }, indent=2)
    elif format == "markdown":
        output = df.to_markdown(index=False)
        return json.dumps({
            "status": "success",
            "format": "markdown",
            "row_count": len(df),
            "data": output
        }, indent=2)
    else:  # json
        data = df.to_dict(orient='records')
        return json.dumps({
            "status": "success",
            "format": "json",
            "row_count": len(df),
            "columns": list(df.columns),
            "data": data
        }, indent=2, default=str)


@mcp.tool()
@handle_duckdb_errors
def explain_query(
    sql: str,
    connection_name: str = "default",
    analyze: bool = False
) -> str:
    """Explain query execution plan

    Args:
        sql: SQL query to explain
        connection_name: Connection to use (default: 'default')
        analyze: Run actual query and get timing (default: False)

    Returns:
        Query execution plan
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Build explain query
    if analyze:
        explain_sql = f"EXPLAIN ANALYZE {sql}"
    else:
        explain_sql = f"EXPLAIN {sql}"

    # Execute explain
    result = con.execute(explain_sql).fetchdf()

    return json.dumps({
        "status": "success",
        "query": sql,
        "analyze": analyze,
        "plan": result.to_dict(orient='records')
    }, indent=2, default=str)


# ============================================================================
# MCP Tools - Phase 3: Schema Inspection
# ============================================================================

@mcp.tool()
@handle_duckdb_errors
def list_tables(
    connection_name: str = "default",
    schema: str = "main"
) -> str:
    """List all tables in database

    Args:
        connection_name: Connection to use (default: 'default')
        schema: Schema name (default: 'main')

    Returns:
        List of tables with metadata
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Get tables
    result = con.execute(f"SHOW TABLES")
    df = result.fetchdf()

    tables = df.to_dict(orient='records')

    return json.dumps({
        "status": "success",
        "schema": schema,
        "count": len(tables),
        "tables": tables
    }, indent=2, default=str)


@mcp.tool()
@handle_duckdb_errors
def describe_table(
    table_name: str,
    connection_name: str = "default"
) -> str:
    """Get table schema and column information

    Args:
        table_name: Name of table
        connection_name: Connection to use (default: 'default')

    Returns:
        Table schema with column types, constraints
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Check cache first
    cache_key = f"{connection_name}.{table_name}"
    if cache_key in schema_cache:
        return json.dumps({
            "status": "success",
            "cached": True,
            "table_name": table_name,
            "schema": schema_cache[cache_key]
        }, indent=2, default=str)

    # Describe table
    result = con.execute(f"DESCRIBE {table_name}")
    df = result.fetchdf()

    columns = df.to_dict(orient='records')

    # Get row count
    count_result = con.execute(f"SELECT COUNT(*) as count FROM {table_name}")
    row_count = count_result.fetchone()[0]

    # Create schema info
    schema_info = SchemaInfo(
        connection_name=connection_name,
        table_name=table_name,
        columns=columns,
        row_count=row_count,
        cached_at=datetime.now().isoformat(),
        schema_hash=str(hash(str(columns)))
    )

    # Cache it
    schema_cache[cache_key] = schema_info

    return json.dumps({
        "status": "success",
        "cached": False,
        "table_name": table_name,
        "row_count": row_count,
        "columns": columns
    }, indent=2, default=str)


@mcp.tool()
@handle_duckdb_errors
def show_columns(
    table_name: str,
    connection_name: str = "default"
) -> str:
    """Show detailed column information

    Args:
        table_name: Name of table
        connection_name: Connection to use (default: 'default')

    Returns:
        Detailed column metadata
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Get detailed column info
    result = con.execute(f"""
        SELECT
            column_name,
            column_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
    """)
    df = result.fetchdf()

    columns = df.to_dict(orient='records')

    return json.dumps({
        "status": "success",
        "table_name": table_name,
        "columns": columns
    }, indent=2, default=str)


@mcp.tool()
@handle_duckdb_errors
def get_table_stats(
    table_name: str,
    connection_name: str = "default"
) -> str:
    """Get table statistics (row count, size, etc)

    Args:
        table_name: Name of table
        connection_name: Connection to use (default: 'default')

    Returns:
        Table statistics
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Get basic stats
    result = con.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
    row_count = result.fetchone()[0]

    # Get column count
    result = con.execute(f"DESCRIBE {table_name}")
    columns = result.fetchdf()
    column_count = len(columns)

    return json.dumps({
        "status": "success",
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "columns": columns.to_dict(orient='records')
    }, indent=2, default=str)


# ============================================================================
# MCP Tools - Phase 4: Data Management
# ============================================================================

@mcp.tool()
@handle_duckdb_errors
def create_table(
    table_name: str,
    columns: dict,
    connection_name: str = "default",
    from_file: str = ""
) -> str:
    """Create a new table

    Args:
        table_name: Name of new table
        columns: Column definitions {"name": "type", ...}
        connection_name: Connection to use (default: 'default')
        from_file: Optional file path to create table from

    Returns:
        Table creation confirmation
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    if from_file:
        # Create table from file
        file_path = Path(from_file)
        extension = file_path.suffix.lower()

        if extension == '.csv':
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{from_file}')"
        elif extension == '.parquet':
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{from_file}')"
        elif extension == '.json':
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_json('{from_file}')"
        else:
            return json.dumps({
                "error": "UnsupportedFormat",
                "message": f"Unsupported file format: {extension}"
            }, indent=2)

        con.execute(query)
    else:
        # Create table from column definitions
        col_defs = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        query = f"CREATE TABLE {table_name} ({col_defs})"
        con.execute(query)

    # Invalidate schema cache
    invalidate_schema_cache(connection_name, table_name)

    return json.dumps({
        "status": "success",
        "table_name": table_name,
        "created_from": from_file if from_file else "schema",
        "message": f"Table '{table_name}' created successfully"
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def drop_table(
    table_name: str,
    connection_name: str = "default",
    if_exists: bool = True
) -> str:
    """Drop a table

    Args:
        table_name: Name of table to drop
        connection_name: Connection to use (default: 'default')
        if_exists: Use IF EXISTS clause (default: True)

    Returns:
        Table drop confirmation
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Build drop query
    if if_exists:
        query = f"DROP TABLE IF EXISTS {table_name}"
    else:
        query = f"DROP TABLE {table_name}"

    con.execute(query)

    # Invalidate schema cache
    invalidate_schema_cache(connection_name, table_name)

    return json.dumps({
        "status": "success",
        "table_name": table_name,
        "message": f"Table '{table_name}' dropped successfully"
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def import_data(
    table_name: str,
    file_path: str,
    format: str = "auto",
    connection_name: str = "default"
) -> str:
    """Import data from file into table

    Args:
        table_name: Target table name
        file_path: Source file path
        format: File format - 'auto', 'csv', 'parquet', 'json' (default: 'auto')
        connection_name: Connection to use (default: 'default')

    Returns:
        Import results with row count
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Auto-detect format
    if format == "auto":
        file_path_obj = Path(file_path)
        extension = file_path_obj.suffix.lower()

        if extension == '.csv':
            format = 'csv'
        elif extension == '.parquet':
            format = 'parquet'
        elif extension == '.json':
            format = 'json'
        else:
            return json.dumps({
                "error": "UnsupportedFormat",
                "message": f"Cannot auto-detect format from extension: {extension}"
            }, indent=2)

    # Build import query
    if format == 'csv':
        query = f"INSERT INTO {table_name} SELECT * FROM read_csv('{file_path}')"
    elif format == 'parquet':
        query = f"INSERT INTO {table_name} SELECT * FROM read_parquet('{file_path}')"
    elif format == 'json':
        query = f"INSERT INTO {table_name} SELECT * FROM read_json('{file_path}')"
    else:
        return json.dumps({
            "error": "UnsupportedFormat",
            "message": f"Unsupported format: {format}"
        }, indent=2)

    # Execute import
    con.execute(query)

    # Get row count
    result = con.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = result.fetchone()[0]

    # Invalidate schema cache
    invalidate_schema_cache(connection_name, table_name)

    return json.dumps({
        "status": "success",
        "table_name": table_name,
        "file_path": file_path,
        "format": format,
        "row_count": row_count,
        "message": f"Data imported successfully into '{table_name}'"
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def export_data(
    sql: str,
    file_path: str,
    format: str = "parquet",
    connection_name: str = "default"
) -> str:
    """Export query results to file

    Args:
        sql: SQL query to export
        file_path: Output file path
        format: Output format - 'parquet', 'csv', 'json' (default: 'parquet')
        connection_name: Connection to use (default: 'default')

    Returns:
        Export confirmation with row count
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Build export query
    if format == 'parquet':
        query = f"COPY ({sql}) TO '{file_path}' (FORMAT PARQUET)"
    elif format == 'csv':
        query = f"COPY ({sql}) TO '{file_path}' (FORMAT CSV, HEADER)"
    elif format == 'json':
        query = f"COPY ({sql}) TO '{file_path}' (FORMAT JSON)"
    else:
        return json.dumps({
            "error": "UnsupportedFormat",
            "message": f"Unsupported format: {format}"
        }, indent=2)

    # Execute export
    con.execute(query)

    return json.dumps({
        "status": "success",
        "file_path": file_path,
        "format": format,
        "message": f"Data exported successfully to '{file_path}'"
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def insert_data(
    table_name: str,
    data: Union[str, List[dict]],
    connection_name: str = "default"
) -> str:
    """Insert data into table

    Args:
        table_name: Target table name
        data: JSON string or list of dicts to insert
        connection_name: Connection to use (default: 'default')

    Returns:
        Insert confirmation with row count
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Parse data
    if isinstance(data, str):
        data = json.loads(data)

    if not isinstance(data, list):
        data = [data]

    # Get columns from first row
    if len(data) == 0:
        return json.dumps({
            "error": "NoData",
            "message": "No data to insert"
        }, indent=2)

    columns = list(data[0].keys())

    # Build insert query
    placeholders = ", ".join(["?" for _ in columns])
    col_names = ", ".join(columns)
    query = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    # Insert rows
    for row in data:
        values = [row[col] for col in columns]
        con.execute(query, values)

    # Invalidate schema cache
    invalidate_schema_cache(connection_name, table_name)

    return json.dumps({
        "status": "success",
        "table_name": table_name,
        "rows_inserted": len(data),
        "message": f"{len(data)} rows inserted into '{table_name}'"
    }, indent=2)


# ============================================================================
# MCP Tools - Phase 5: Advanced Features
# ============================================================================

@mcp.tool()
@handle_duckdb_errors
def create_function(
    function_name: str,
    function_code: str,
    return_type: str = "varchar",
    connection_name: str = "default"
) -> str:
    """Create a user-defined function

    Args:
        function_name: Name of function
        function_code: Python code for function
        return_type: Return type (default: 'varchar')
        connection_name: Connection to use (default: 'default')

    Returns:
        Function creation confirmation
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Create function
    query = f"""
    CREATE OR REPLACE FUNCTION {function_name}()
    RETURNS {return_type}
    LANGUAGE python
    AS $$
    {function_code}
    $$
    """

    con.execute(query)

    return json.dumps({
        "status": "success",
        "function_name": function_name,
        "return_type": return_type,
        "message": f"Function '{function_name}' created successfully"
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def list_extensions(connection_name: str = "default") -> str:
    """List available and installed extensions

    Args:
        connection_name: Connection to use (default: 'default')

    Returns:
        List of extensions with installation status
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Get installed extensions
    result = con.execute("FROM duckdb_extensions() SELECT *")
    df = result.fetchdf()

    extensions = df.to_dict(orient='records')

    return json.dumps({
        "status": "success",
        "count": len(extensions),
        "extensions": extensions
    }, indent=2, default=str)


@mcp.tool()
@handle_duckdb_errors
def install_extension(
    extension_name: str,
    connection_name: str = "default",
    load: bool = True
) -> str:
    """Install and optionally load an extension

    Args:
        extension_name: Name of extension
        connection_name: Connection to use (default: 'default')
        load: Load extension after install (default: True)

    Returns:
        Installation confirmation
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Install extension
    con.execute(f"INSTALL {extension_name}")

    if load:
        con.execute(f"LOAD {extension_name}")

    return json.dumps({
        "status": "success",
        "extension_name": extension_name,
        "loaded": load,
        "message": f"Extension '{extension_name}' installed successfully"
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def get_query_progress(connection_name: str = "default") -> str:
    """Get progress of currently running query

    Args:
        connection_name: Connection to use (default: 'default')

    Returns:
        Query progress information
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    metadata = connection_metadata[connection_name]

    if metadata.current_query:
        # Calculate elapsed time
        if metadata.query_start_time:
            start_time = datetime.fromisoformat(metadata.query_start_time)
            elapsed = (datetime.now() - start_time).total_seconds()
        else:
            elapsed = 0

        return json.dumps({
            "status": "running",
            "query": metadata.current_query,
            "started_at": metadata.query_start_time,
            "elapsed_seconds": elapsed
        }, indent=2)
    else:
        return json.dumps({
            "status": "idle",
            "message": "No query currently running"
        }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def cancel_query(connection_name: str = "default") -> str:
    """Cancel currently running query

    Args:
        connection_name: Connection to use (default: 'default')

    Returns:
        Cancellation confirmation
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    metadata = connection_metadata[connection_name]

    # Note: DuckDB doesn't have a built-in cancel mechanism
    # This is a placeholder for future implementation
    return json.dumps({
        "status": "not_implemented",
        "message": "Query cancellation not directly supported in DuckDB",
        "suggestion": "Use connection timeout or restart connection"
    }, indent=2)


@mcp.tool()
@handle_duckdb_errors
def database_info(connection_name: str = "default") -> str:
    """Get database information and configuration

    Args:
        connection_name: Connection to use (default: 'default')

    Returns:
        Database configuration and status
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]
    metadata = connection_metadata[connection_name]

    # Get database info
    try:
        db_info = con.execute("PRAGMA database_info").fetchdf()
        db_info_dict = db_info.to_dict(orient='records') if PANDAS_AVAILABLE else []
    except:
        db_info_dict = []

    # Get PRAGMA settings
    try:
        pragma_info = con.execute("PRAGMA pragma_settings").fetchdf()
        pragma_dict = pragma_info.to_dict(orient='records') if PANDAS_AVAILABLE else []
    except:
        pragma_dict = []

    return json.dumps({
        "status": "success",
        "connection_name": connection_name,
        "database_path": metadata.database_path,
        "database_info": db_info_dict,
        "pragma_settings": pragma_dict
    }, indent=2, default=str)


# ============================================================================
# MCP Tools - Phase 6: File Operations
# ============================================================================

@mcp.tool()
@handle_duckdb_errors
def query_remote_file(
    url: str,
    sql: str = "",
    connection_name: str = "default",
    format: str = "json"
) -> str:
    """Query a remote file (HTTP/S3/GCS/Azure)

    Args:
        url: URL to remote file
        sql: Optional filter/query (default: "")
        connection_name: Connection to use (default: 'default')
        format: Output format (default: 'json')

    Returns:
        Query results from remote file
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    # Auto-detect protocol
    if url.startswith('http://') or url.startswith('https://'):
        # HTTP files - need httpfs extension
        con = connections[connection_name]
        con.execute("LOAD httpfs")

        if url.endswith('.csv'):
            query = f"SELECT * FROM read_csv('{url}')"
        elif url.endswith('.parquet'):
            query = f"SELECT * FROM read_parquet('{url}')"
        else:
            return json.dumps({
                "error": "UnsupportedFormat",
                "message": "Cannot determine file format from URL"
            }, indent=2)

    elif url.startswith('s3://'):
        # S3 files - need httpfs extension
        con = connections[connection_name]
        con.execute("LOAD httpfs")

        if url.endswith('.csv'):
            query = f"SELECT * FROM read_csv('{url}')"
        elif url.endswith('.parquet'):
            query = f"SELECT * FROM read_parquet('{url}')"
        else:
            return json.dumps({
                "error": "UnsupportedFormat",
                "message": "Cannot determine file format from URL"
            }, indent=2)

    else:
        return json.dumps({
            "error": "UnsupportedProtocol",
            "message": f"Unsupported protocol in URL: {url}",
            "suggestion": "Supported protocols: http://, https://, s3://"
        }, indent=2)

    # Add custom SQL if provided
    if sql:
        query = f"SELECT * FROM ({query}) AS subq WHERE {sql}"

    # Execute query
    return execute_query(query, connection_name, format)


@mcp.tool()
@handle_duckdb_errors
def list_files(
    directory: str,
    pattern: str = "*",
    connection_name: str = "default"
) -> str:
    """List files in directory using DuckDB's glob functions

    Args:
        directory: Directory path or glob pattern
        pattern: File pattern (default: "*")
        connection_name: Connection to use (default: 'default')

    Returns:
        List of files with metadata
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    con = connections[connection_name]

    # Build glob pattern
    glob_pattern = f"{directory}/{pattern}"

    # List files using glob
    query = f"SELECT * FROM glob('{glob_pattern}')"
    result = con.execute(query)
    df = result.fetchdf()

    files = df.to_dict(orient='records')

    return json.dumps({
        "status": "success",
        "directory": directory,
        "pattern": pattern,
        "count": len(files),
        "files": files
    }, indent=2, default=str)


# ============================================================================
# MCP Tools - Phase 7: Pandas Integration
# ============================================================================

@mcp.tool()
@handle_duckdb_errors
def query_to_dataframe(
    sql: str,
    connection_name: str = "default"
) -> str:
    """Execute query and return as DataFrame info
    (Note: Returns DataFrame metadata, not actual data)

    Args:
        sql: SQL query to execute
        connection_name: Connection to use (default: 'default')

    Returns:
        DataFrame metadata (shape, columns, dtypes)
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    if not PANDAS_AVAILABLE:
        return json.dumps({
            "error": "PandasNotAvailable",
            "message": "Pandas is not installed",
            "suggestion": "Install pandas with: pip install pandas"
        }, indent=2)

    con = connections[connection_name]

    # Execute query
    df = con.execute(sql).fetchdf()

    # Get metadata
    metadata = {
        "shape": df.shape,
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "memory_usage": df.memory_usage(deep=True).to_dict()
    }

    return json.dumps({
        "status": "success",
        "query": sql,
        "dataframe_metadata": metadata
    }, indent=2, default=str)


@mcp.tool()
@handle_duckdb_errors
def register_dataframe(
    dataframe_name: str,
    data: List[dict],
    connection_name: str = "default"
) -> str:
    """Register a Python dict/list as a queryable table

    Args:
        dataframe_name: Name to register as
        data: Data as list of dicts
        connection_name: Connection to use (default: 'default')

    Returns:
        Registration confirmation
    """
    if connection_name not in connections:
        return json.dumps({
            "error": "ConnectionNotFound",
            "message": f"Connection '{connection_name}' not found"
        }, indent=2)

    if not PANDAS_AVAILABLE:
        return json.dumps({
            "error": "PandasNotAvailable",
            "message": "Pandas is not installed",
            "suggestion": "Install pandas with: pip install pandas"
        }, indent=2)

    con = connections[connection_name]

    # Create DataFrame
    df = pd.DataFrame(data)

    # Register as view
    con.register(dataframe_name, df)

    return json.dumps({
        "status": "success",
        "dataframe_name": dataframe_name,
        "shape": df.shape,
        "columns": list(df.columns),
        "message": f"DataFrame registered as '{dataframe_name}'"
    }, indent=2, default=str)


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
