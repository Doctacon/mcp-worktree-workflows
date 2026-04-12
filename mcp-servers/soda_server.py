#!/usr/bin/env python3
"""
MCP Server for Soda Core (local, no cloud)

Provides data quality contract management and verification through MCP:
- List contracts and datasource configs
- Verify contracts against data sources (DuckDB, PostgreSQL, etc.)
- Validate contract YAML syntax without connecting to a database
- View scan history from saved results
- Scaffold new contract YAML files

Expects a SODA_DIR directory (default: ./soda) with:
  datasources/   - datasource YAML configs
  contracts/     - contract YAML files
  results/       - saved JSON scan results (created automatically)

No Soda Cloud account required. All checks run locally.
"""

import functools
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from fastmcp import FastMCP

try:
    from soda_core.contracts.api.verify_api import verify_contract_locally
    from soda_core.contracts.contract_verification import (
        ContractVerificationSession,
        ContractVerificationSessionResult,
    )
    from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource

    SODA_AVAILABLE = True
except ImportError:
    SODA_AVAILABLE = False

mcp = FastMCP("soda")

# ---------------------------------------------------------------------------
# Directory resolution
# ---------------------------------------------------------------------------

def _soda_dir() -> Path:
    return Path(os.environ.get("SODA_DIR", "./soda"))


def _contracts_dir() -> Path:
    return _soda_dir() / "contracts"


def _datasources_dir() -> Path:
    return _soda_dir() / "datasources"


def _results_dir() -> Path:
    d = _soda_dir() / "results"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _resolve_contract(name_or_path: str) -> Path:
    p = Path(name_or_path)
    if p.is_absolute() or p.exists():
        return p
    # Try relative to contracts dir
    candidate = _contracts_dir() / name_or_path
    if candidate.exists():
        return candidate
    # Try with .yml suffix
    candidate_yml = _contracts_dir() / (name_or_path + ".yml")
    if candidate_yml.exists():
        return candidate_yml
    return candidate  # return anyway; error will surface at verify time


def _resolve_datasource(name_or_path: str) -> Path:
    p = Path(name_or_path)
    if p.is_absolute() or p.exists():
        return p
    candidate = _datasources_dir() / name_or_path
    if candidate.exists():
        return candidate
    candidate_yml = _datasources_dir() / (name_or_path + ".yml")
    if candidate_yml.exists():
        return candidate_yml
    return candidate


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

_REDACT = {"password", "secret", "token", "key", "credential", "api_key", "apikey"}


def handle_soda_errors(func):
    """Decorator for consistent Soda Core error handling."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not SODA_AVAILABLE:
            return json.dumps(
                {
                    "error": "SodaNotAvailable",
                    "message": "soda-core package not installed",
                    "suggestion": "Install with: pip install soda-core soda-core-duckdb soda-core-postgres",
                },
                indent=2,
            )
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            return json.dumps({"error": "FileNotFound", "message": str(e)}, indent=2)
        except Exception as e:
            return json.dumps(
                {"error": type(e).__name__, "message": str(e)}, indent=2
            )

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _redact_dict(d: dict) -> dict:
    """Recursively redact sensitive keys from a dict."""
    result = {}
    for k, v in d.items():
        if any(r in k.lower() for r in _REDACT):
            result[k] = "***REDACTED***"
        elif isinstance(v, dict):
            result[k] = _redact_dict(v)
        else:
            result[k] = v
    return result


def _parse_check_results(result: "ContractVerificationSessionResult") -> list:
    """Extract per-check summaries from a verification result."""
    checks = []
    for contract_result in (result.contract_verification_results or []):
        for check_result in (contract_result.check_results or []):
            check_info = {
                "name": getattr(check_result, "name", None) or str(check_result),
                "status": "passed" if check_result.is_passed else "failed",
                "metrics": {},
            }
            diag = getattr(check_result, "diagnostic_metric_values", None)
            if diag:
                check_info["metrics"] = {k: v for k, v in diag.items()}
            checks.append(check_info)
    return checks


def _save_result(contract_name: str, summary: dict) -> Path:
    """Save a scan result summary to the results directory."""
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    safe_name = contract_name.replace("/", "_").replace(".yml", "")
    result_file = _results_dir() / f"{safe_name}_{timestamp}.json"
    result_file.write_text(json.dumps(summary, indent=2))
    return result_file


# ---------------------------------------------------------------------------
# Discovery tools
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_soda_errors
def list_contracts(datasource_filter: Optional[str] = None) -> str:
    """List available Soda contract YAML files.

    Args:
        datasource_filter: Optional datasource name to filter by (matches the
            datasource name referenced in each contract's dataset field).

    Returns:
        JSON array of contracts with name, path, and dataset fields.
    """
    contracts_dir = _contracts_dir()
    if not contracts_dir.exists():
        return json.dumps(
            {"contracts": [], "hint": f"No contracts directory found at {contracts_dir}"},
            indent=2,
        )

    contracts = []
    for yml_file in sorted(contracts_dir.rglob("*.yml")):
        try:
            content = yaml.safe_load(yml_file.read_text()) or {}
            dataset = content.get("dataset", "")
            datasource = dataset.split("/")[0] if "/" in dataset else ""
            if datasource_filter and datasource_filter not in datasource:
                continue
            contracts.append(
                {
                    "name": yml_file.stem,
                    "path": str(yml_file),
                    "dataset": dataset,
                }
            )
        except Exception:
            contracts.append({"name": yml_file.stem, "path": str(yml_file), "dataset": None})

    return json.dumps({"contracts": contracts, "total": len(contracts)}, indent=2)


@mcp.tool()
@handle_soda_errors
def list_datasources() -> str:
    """List available Soda datasource config files.

    Returns:
        JSON array of datasources with name, path, and type fields.
        Connection credentials are redacted.
    """
    ds_dir = _datasources_dir()
    if not ds_dir.exists():
        return json.dumps(
            {"datasources": [], "hint": f"No datasources directory found at {ds_dir}"},
            indent=2,
        )

    datasources = []
    for yml_file in sorted(ds_dir.glob("*.yml")):
        try:
            content = yaml.safe_load(yml_file.read_text()) or {}
            datasources.append(
                {
                    "name": content.get("name", yml_file.stem),
                    "path": str(yml_file),
                    "type": content.get("type", "unknown"),
                }
            )
        except Exception:
            datasources.append({"name": yml_file.stem, "path": str(yml_file), "type": None})

    return json.dumps({"datasources": datasources, "total": len(datasources)}, indent=2)


# ---------------------------------------------------------------------------
# Verification tools
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_soda_errors
def verify_contract(
    contract_file: str,
    datasource_file: str,
    variables: Optional[str] = None,
) -> str:
    """Run a Soda contract verification against a data source.

    Args:
        contract_file: Path or name of the contract YAML file. Can be an
            absolute path, relative path, or just the filename (searched in
            the contracts directory).
        datasource_file: Path or name of the datasource config YAML file.
            Can be an absolute path, relative path, or just the filename
            (searched in the datasources directory).
        variables: Optional JSON string of variables to pass to the contract,
            e.g. '{"min_rows": 1000, "env": "prod"}'.

    Returns:
        JSON with pass/fail status, per-check results, and path to saved result file.
    """
    contract_path = _resolve_contract(contract_file)
    datasource_path = _resolve_datasource(datasource_file)

    parsed_vars = {}
    if variables:
        try:
            parsed_vars = json.loads(variables)
        except json.JSONDecodeError as e:
            return json.dumps(
                {"error": "InvalidVariables", "message": f"variables must be valid JSON: {e}"},
                indent=2,
            )

    result = verify_contract_locally(
        data_source_file_path=str(datasource_path),
        contract_file_path=str(contract_path),
        variables=parsed_vars,
    )

    checks = _parse_check_results(result)
    errors_str = result.get_errors_str() if result.is_failed else None

    summary = {
        "passed": result.is_passed,
        "checks_total": result.number_of_checks,
        "checks_passed": result.number_of_checks_passed,
        "checks_failed": result.number_of_checks_failed,
        "checks": checks,
        "errors": errors_str,
        "contract": str(contract_path),
        "datasource": str(datasource_path),
        "timestamp": datetime.now().isoformat(),
    }

    result_file = _save_result(contract_path.stem, summary)
    summary["result_file"] = str(result_file)

    return json.dumps(summary, indent=2)


@mcp.tool()
@handle_soda_errors
def validate_contract(
    contract_file: str,
    datasource_file: str,
) -> str:
    """Validate a Soda contract YAML without connecting to the database.

    Checks that the contract and datasource YAML files are well-formed and
    syntactically valid. Does not run any queries.

    Args:
        contract_file: Path or name of the contract YAML file.
        datasource_file: Path or name of the datasource config YAML file.

    Returns:
        JSON with valid flag and any validation errors found.
    """
    contract_path = _resolve_contract(contract_file)
    datasource_path = _resolve_datasource(datasource_file)

    contract_text = contract_path.read_text()
    datasource_text = datasource_path.read_text()

    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract_text)],
        data_source_yaml_sources=[DataSourceYamlSource.from_str(datasource_text)],
        only_validate_without_execute=True,
    )

    if result.has_errors:
        return json.dumps(
            {"valid": False, "errors": result.get_errors_str()}, indent=2
        )

    return json.dumps({"valid": True, "message": "Contract YAML is valid"}, indent=2)


# ---------------------------------------------------------------------------
# History tool
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_soda_errors
def scan_history(
    contract_name: Optional[str] = None,
    limit: int = 10,
    show_checks: bool = False,
) -> str:
    """List recent scan results from saved history.

    Args:
        contract_name: Optional contract name prefix to filter results.
        limit: Maximum number of results to return (default: 10).
        show_checks: If True, include per-check details in the response.

    Returns:
        JSON array of scan result summaries ordered newest first.
    """
    results_dir = _results_dir()
    result_files = sorted(results_dir.glob("*.json"), reverse=True)

    if contract_name:
        result_files = [f for f in result_files if f.name.startswith(contract_name)]

    result_files = result_files[:limit]

    history = []
    for f in result_files:
        try:
            data = json.loads(f.read_text())
            entry = {
                "file": f.name,
                "timestamp": data.get("timestamp"),
                "contract": Path(data.get("contract", "")).name,
                "passed": data.get("passed"),
                "checks_total": data.get("checks_total"),
                "checks_passed": data.get("checks_passed"),
                "checks_failed": data.get("checks_failed"),
            }
            if show_checks:
                entry["checks"] = data.get("checks", [])
            history.append(entry)
        except Exception as e:
            history.append({"file": f.name, "error": str(e)})

    return json.dumps({"history": history, "total": len(history)}, indent=2)


# ---------------------------------------------------------------------------
# Scaffolding tool
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_soda_errors
def scaffold_contract(
    dataset_name: str,
    datasource_name: str,
    columns: Optional[str] = None,
) -> str:
    """Generate a starter Soda contract YAML for a dataset.

    Args:
        dataset_name: Name of the table or dataset (e.g. "orders", "public.customers").
        datasource_name: Name of the datasource as defined in the datasource config.
        columns: Optional JSON array of column names to include in the contract,
            e.g. '["id", "email", "created_at", "amount"]'.

    Returns:
        JSON with the generated contract YAML string and a suggested file path.
    """
    col_list = []
    if columns:
        try:
            col_list = json.loads(columns)
        except json.JSONDecodeError:
            return json.dumps(
                {"error": "InvalidColumns", "message": "columns must be a JSON array of strings"},
                indent=2,
            )

    lines = [
        f"dataset: {datasource_name}/{dataset_name}",
        "",
        "# owner: your-email@company.com",
        "",
    ]

    if col_list:
        lines.append("columns:")
        for col in col_list:
            lines.append(f"  - name: {col}")
            lines.append(f"    checks:")
            lines.append(f"      - missing_count:")
            lines.append(f"            # must_be: 0")
            lines.append(f"")
    else:
        lines += [
            "columns:",
            "  - name: id",
            "    checks:",
            "      - duplicate_count:",
            "      - missing_count:",
            "",
            "  # Add more columns here",
        ]

    lines += [
        "",
        "checks:",
        "  - row_count:",
        "      threshold:",
        "        must_be_greater_than: 0",
    ]

    contract_yaml = "\n".join(lines)
    safe_name = dataset_name.replace(".", "_").replace("/", "_")
    suggested_path = str(_contracts_dir() / f"{safe_name}.yml")

    return json.dumps(
        {
            "yaml": contract_yaml,
            "suggested_path": suggested_path,
            "hint": f"Write this to {suggested_path} and run verify_contract to test it.",
        },
        indent=2,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    mcp.run()


if __name__ == "__main__":
    mcp.run()
