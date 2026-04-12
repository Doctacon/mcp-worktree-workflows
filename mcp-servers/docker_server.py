#!/usr/bin/env python3
"""
MCP Server for Docker (Colima-compatible)

Provides container and image management through MCP:
- Container lifecycle: list, inspect, start, stop, restart, remove
- Logs and exec: tail logs, run one-shot commands inside containers
- Image management: list, pull, remove
- Volumes and networks: list and inspect
- Compose-aware: filter by project, aggregate logs by service
- System info: daemon version, container counts, disk usage

Designed for use with Colima on macOS. Set DOCKER_HOST in .mcp.json env
to point at the Colima socket: unix:///Users/<you>/.colima/default/docker.sock
"""

import concurrent.futures
import functools
import json
from typing import Optional

from fastmcp import FastMCP

try:
    import docker
    from docker.errors import APIError, DockerException, NotFound

    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False

mcp = FastMCP("docker")

# ---------------------------------------------------------------------------
# Lazy client — initialized on first use so startup failures surface per-tool
# ---------------------------------------------------------------------------

_client = None


def get_client():
    global _client
    if _client is None:
        _client = docker.from_env()
    return _client


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def handle_docker_errors(func):
    """Decorator for consistent Docker error handling."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not DOCKER_AVAILABLE:
            return json.dumps(
                {
                    "error": "DockerNotAvailable",
                    "message": "docker package not installed",
                    "suggestion": "Install with: pip install docker",
                },
                indent=2,
            )
        try:
            return func(*args, **kwargs)
        except NotFound as e:
            return json.dumps({"error": "NotFound", "message": str(e)}, indent=2)
        except APIError as e:
            return json.dumps({"error": "APIError", "message": str(e)}, indent=2)
        except DockerException as e:
            return json.dumps(
                {
                    "error": "DockerException",
                    "message": str(e),
                    "hint": "Is Colima running? Try: colima start",
                },
                indent=2,
            )
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

_REDACT_KEYS = {"PASSWORD", "SECRET", "TOKEN", "KEY", "CREDENTIAL", "API"}


def _safe_env(env_list: list) -> dict:
    """Return env vars with sensitive values redacted."""
    result = {}
    for item in env_list or []:
        k, _, v = item.partition("=")
        if any(r in k.upper() for r in _REDACT_KEYS):
            result[k] = "***REDACTED***"
        else:
            result[k] = v
    return result


def _format_ports(ports: dict) -> list:
    """Format container port bindings into readable strings."""
    result = []
    for container_port, bindings in (ports or {}).items():
        if bindings:
            for b in bindings:
                result.append(f"{b['HostIp'] or '0.0.0.0'}:{b['HostPort']} -> {container_port}")
        else:
            result.append(container_port)
    return result


def _container_summary(c) -> dict:
    """Compact container dict for list operations."""
    return {
        "id": c.short_id,
        "name": c.name,
        "image": c.image.tags[0] if c.image.tags else c.image.short_id,
        "status": c.status,
        "ports": _format_ports(c.ports),
        "labels": c.labels,
        "created": c.attrs.get("Created", ""),
    }


def _exec_with_timeout(container, command: str, workdir: Optional[str], timeout: int) -> dict:
    """Run a one-shot exec in a container with a timeout."""

    def run():
        return container.exec_run(
            ["sh", "-c", command],
            workdir=workdir,
            demux=True,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            result = future.result(timeout=timeout)
            stdout = (result.output[0] or b"").decode(errors="replace")
            stderr = (result.output[1] or b"").decode(errors="replace")
            return {
                "exit_code": result.exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "timed_out": False,
            }
        except concurrent.futures.TimeoutError:
            return {
                "exit_code": -1,
                "stdout": "",
                "stderr": f"Command timed out after {timeout}s",
                "timed_out": True,
            }


# ---------------------------------------------------------------------------
# Container lifecycle tools
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_docker_errors
def container_list(
    all: bool = False,
    label_filter: Optional[str] = None,
    name_filter: Optional[str] = None,
) -> str:
    """List Docker containers.

    Args:
        all: Include stopped containers (default: running only).
        label_filter: Filter by label, e.g. "com.docker.compose.project=myapp".
        name_filter: Substring match on container name.

    Returns:
        JSON array of containers with id, name, image, status, ports, labels, created.
    """
    filters = {}
    if label_filter:
        filters["label"] = label_filter
    if name_filter:
        filters["name"] = name_filter

    containers = get_client().containers.list(all=all, filters=filters)
    return json.dumps([_container_summary(c) for c in containers], indent=2)


@mcp.tool()
@handle_docker_errors
def container_inspect(container: str) -> str:
    """Inspect a container's configuration and state.

    Args:
        container: Container name or ID.

    Returns:
        JSON with id, name, image, status, state, ports, env (sensitive values
        redacted), mounts, networks, and restart policy.
    """
    c = get_client().containers.get(container)
    attrs = c.attrs

    return json.dumps(
        {
            "id": c.id,
            "short_id": c.short_id,
            "name": c.name,
            "image": attrs["Config"]["Image"],
            "status": c.status,
            "state": attrs.get("State", {}),
            "ports": _format_ports(c.ports),
            "env": _safe_env(attrs["Config"].get("Env", [])),
            "mounts": [
                {"type": m["Type"], "source": m.get("Source", ""), "destination": m["Destination"]}
                for m in attrs.get("Mounts", [])
            ],
            "networks": list((attrs.get("NetworkSettings") or {}).get("Networks", {}).keys()),
            "restart_policy": attrs.get("HostConfig", {}).get("RestartPolicy", {}),
        },
        indent=2,
    )


@mcp.tool()
@handle_docker_errors
def container_start(container: str) -> str:
    """Start a stopped container.

    Args:
        container: Container name or ID.

    Returns:
        JSON with container name and status before/after.
    """
    c = get_client().containers.get(container)
    status_before = c.status
    c.start()
    c.reload()
    return json.dumps(
        {"ok": True, "container": c.name, "status_before": status_before, "status_after": c.status},
        indent=2,
    )


@mcp.tool()
@handle_docker_errors
def container_stop(container: str, timeout: int = 10) -> str:
    """Stop a running container.

    Args:
        container: Container name or ID.
        timeout: Seconds to wait before killing (default: 10).

    Returns:
        JSON with container name and status before/after.
    """
    c = get_client().containers.get(container)
    status_before = c.status
    c.stop(timeout=timeout)
    c.reload()
    return json.dumps(
        {"ok": True, "container": c.name, "status_before": status_before, "status_after": c.status},
        indent=2,
    )


@mcp.tool()
@handle_docker_errors
def container_restart(container: str, timeout: int = 10) -> str:
    """Restart a container.

    Args:
        container: Container name or ID.
        timeout: Seconds to wait before killing during stop (default: 10).

    Returns:
        JSON with container name and status before/after.
    """
    c = get_client().containers.get(container)
    status_before = c.status
    c.restart(timeout=timeout)
    c.reload()
    return json.dumps(
        {"ok": True, "container": c.name, "status_before": status_before, "status_after": c.status},
        indent=2,
    )


@mcp.tool()
@handle_docker_errors
def container_remove(
    container: str,
    force: bool = False,
    remove_volumes: bool = False,
) -> str:
    """Remove a container.

    Args:
        container: Container name or ID.
        force: Kill the container if it is running before removing.
        remove_volumes: Also remove anonymous volumes attached to the container.

    Returns:
        JSON confirming removal.
    """
    c = get_client().containers.get(container)
    name = c.name
    c.remove(force=force, v=remove_volumes)
    return json.dumps({"removed": True, "container": name}, indent=2)


# ---------------------------------------------------------------------------
# Logs and exec tools
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_docker_errors
def container_logs(
    container: str,
    tail: int = 100,
    since: Optional[str] = None,
    timestamps: bool = False,
) -> str:
    """Fetch recent logs from a container.

    Args:
        container: Container name or ID.
        tail: Number of lines to return from the end (default: 100).
        since: Return logs since this timestamp or duration, e.g. "5m", "2024-01-01T00:00:00".
        timestamps: Prefix each line with its timestamp.

    Returns:
        JSON with container name, line count, and logs as a plain string.
    """
    c = get_client().containers.get(container)
    kwargs = {"tail": tail, "timestamps": timestamps, "stream": False}
    if since:
        kwargs["since"] = since

    raw = c.logs(**kwargs)
    log_text = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
    lines = log_text.splitlines()

    return json.dumps(
        {"container": c.name, "lines": len(lines), "logs": log_text},
        indent=2,
    )


@mcp.tool()
@handle_docker_errors
def container_exec(
    container: str,
    command: str,
    workdir: Optional[str] = None,
    timeout: int = 30,
) -> str:
    """Execute a one-shot command inside a running container.

    Args:
        container: Container name or ID.
        command: Shell command to run, executed via sh -c.
        workdir: Working directory inside the container.
        timeout: Seconds before the command is killed (default: 30).

    Returns:
        JSON with exit_code, stdout, stderr, and timed_out flag.
    """
    c = get_client().containers.get(container)
    result = _exec_with_timeout(c, command, workdir, timeout)
    result["container"] = c.name
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Image tools
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_docker_errors
def image_list(name_filter: Optional[str] = None) -> str:
    """List Docker images.

    Args:
        name_filter: Filter by repository name, e.g. "postgres".

    Returns:
        JSON array of images with id, tags, size_mb, and created.
    """
    kwargs = {}
    if name_filter:
        kwargs["name"] = name_filter

    images = get_client().images.list(**kwargs)
    result = []
    for img in images:
        result.append(
            {
                "id": img.short_id,
                "tags": img.tags,
                "size_mb": round(img.attrs.get("Size", 0) / 1_048_576, 1),
                "created": img.attrs.get("Created", ""),
            }
        )
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_docker_errors
def image_pull(image: str) -> str:
    """Pull a Docker image.

    Args:
        image: Image name and tag, e.g. "postgres:16", "ghcr.io/org/name:tag".

    Returns:
        JSON with image name, id, and size_mb after pull.
    """
    img = get_client().images.pull(image)
    return json.dumps(
        {
            "pulled": True,
            "image": image,
            "id": img.short_id,
            "tags": img.tags,
            "size_mb": round(img.attrs.get("Size", 0) / 1_048_576, 1),
        },
        indent=2,
    )


@mcp.tool()
@handle_docker_errors
def image_remove(image: str, force: bool = False) -> str:
    """Remove a Docker image.

    Args:
        image: Image name, tag, or ID.
        force: Force removal even if image is used by stopped containers.

    Returns:
        JSON with deleted and untagged layer lists.
    """
    result = get_client().images.remove(image, force=force) or []
    deleted = [r["Deleted"] for r in result if "Deleted" in r]
    untagged = [r["Untagged"] for r in result if "Untagged" in r]
    return json.dumps({"removed": True, "deleted": deleted, "untagged": untagged}, indent=2)


# ---------------------------------------------------------------------------
# Volume and network tools
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_docker_errors
def volume_list(label_filter: Optional[str] = None) -> str:
    """List Docker volumes.

    Args:
        label_filter: Filter by label, e.g. "com.docker.compose.project=myapp".

    Returns:
        JSON array of volumes with name, driver, mountpoint, labels, and created.
    """
    filters = {}
    if label_filter:
        filters["label"] = label_filter

    volumes = get_client().volumes.list(filters=filters)
    result = []
    for v in volumes:
        result.append(
            {
                "name": v.name,
                "driver": v.attrs.get("Driver", ""),
                "mountpoint": v.attrs.get("Mountpoint", ""),
                "labels": v.attrs.get("Labels") or {},
                "created": v.attrs.get("CreatedAt", ""),
            }
        )
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_docker_errors
def network_list() -> str:
    """List Docker networks.

    Returns:
        JSON array of networks with id, name, driver, scope, and connected container names.
    """
    networks = get_client().networks.list()
    result = []
    for n in networks:
        containers = list((n.attrs.get("Containers") or {}).keys())
        # Containers dict keys are container IDs; resolve to names when possible
        container_names = []
        for cid in containers:
            container_info = (n.attrs.get("Containers") or {}).get(cid, {})
            container_names.append(container_info.get("Name", cid[:12]))

        result.append(
            {
                "id": n.short_id,
                "name": n.name,
                "driver": n.attrs.get("Driver", ""),
                "scope": n.attrs.get("Scope", ""),
                "containers": container_names,
            }
        )
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Compose-aware tools
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_docker_errors
def compose_ps(project: str) -> str:
    """List containers belonging to a Docker Compose project.

    Args:
        project: Compose project name (value of com.docker.compose.project label).

    Returns:
        JSON array of services with service name, container name, status, and ports.
    """
    containers = get_client().containers.list(
        all=True, filters={"label": f"com.docker.compose.project={project}"}
    )
    result = []
    for c in containers:
        result.append(
            {
                "service": c.labels.get("com.docker.compose.service", ""),
                "container": c.name,
                "status": c.status,
                "ports": _format_ports(c.ports),
            }
        )
    result.sort(key=lambda x: x["service"])
    return json.dumps(result, indent=2)


@mcp.tool()
@handle_docker_errors
def compose_logs(
    project: str,
    service: Optional[str] = None,
    tail: int = 50,
) -> str:
    """Fetch logs from all (or one) service in a Docker Compose project.

    Args:
        project: Compose project name.
        service: Specific service name to fetch logs for (default: all services).
        tail: Number of lines per service (default: 50).

    Returns:
        JSON with project name and a dict mapping service names to their log strings.
    """
    label_filters: list = [f"com.docker.compose.project={project}"]
    if service:
        label_filters.append(f"com.docker.compose.service={service}")

    containers = get_client().containers.list(all=True, filters={"label": label_filters})
    logs_by_service = {}
    for c in containers:
        svc = c.labels.get("com.docker.compose.service", c.name)
        raw = c.logs(tail=tail, stream=False)
        log_text = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
        logs_by_service[svc] = log_text

    return json.dumps({"project": project, "logs": logs_by_service}, indent=2)


# ---------------------------------------------------------------------------
# System info
# ---------------------------------------------------------------------------


@mcp.tool()
@handle_docker_errors
def docker_system_info() -> str:
    """Get Docker daemon info and disk usage summary.

    Returns:
        JSON with server version, OS, architecture, container counts,
        image count, and disk usage breakdown in human-readable units.
    """
    client = get_client()
    info = client.info()
    df = client.df()

    def _mb(b):
        return round(b / 1_048_576, 1)

    def _gb(b):
        return round(b / 1_073_741_824, 2)

    images_size = sum(img.get("Size", 0) for img in (df.get("Images") or []))
    containers_size = sum(c.get("SizeRootFs", 0) for c in (df.get("Containers") or []))
    volumes_size = sum(
        (v.get("UsageData") or {}).get("Size", 0) for v in (df.get("Volumes") or [])
    )
    build_cache_size = sum(b.get("Size", 0) for b in (df.get("BuildCache") or []))

    return json.dumps(
        {
            "server_version": info.get("ServerVersion"),
            "os": info.get("OperatingSystem"),
            "arch": info.get("Architecture"),
            "containers": {
                "running": info.get("ContainersRunning", 0),
                "paused": info.get("ContainersPaused", 0),
                "stopped": info.get("ContainersStopped", 0),
            },
            "images": info.get("Images", 0),
            "disk_usage": {
                "images_gb": _gb(images_size),
                "containers_mb": _mb(containers_size),
                "volumes_mb": _mb(volumes_size),
                "build_cache_mb": _mb(build_cache_size),
            },
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
