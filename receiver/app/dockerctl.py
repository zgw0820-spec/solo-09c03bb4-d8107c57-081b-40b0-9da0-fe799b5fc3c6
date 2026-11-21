"""Thin wrapper around the Docker SDK for pausing/resuming demo replicas."""
from __future__ import annotations

from . import config


class DockerUnavailable(RuntimeError):
    """Raised when the docker daemon/socket cannot be reached."""


def _client():
    try:
        import docker
    except ImportError as exc:  # pragma: no cover - image always ships docker SDK
        raise DockerUnavailable(f"docker SDK missing: {exc}") from exc
    try:
        return docker.DockerClient(base_url=config.DOCKER_BASE_URL, timeout=5)
    except Exception as exc:
        raise DockerUnavailable(f"cannot connect to docker: {exc}") from exc


def list_replica_containers() -> list[dict]:
    """Return demo replica containers with their docker status."""
    client = _client()
    result: list[dict] = []
    try:
        for replica, container_name in config.REPLICA_CONTAINERS.items():
            try:
                container = client.containers.get(container_name)
                state = container.attrs.get("State", {})
                result.append(
                    {
                        "replica": replica,
                        "container": container_name,
                        "status": state.get("Status", "unknown"),
                        "paused": bool(state.get("Paused", False)),
                        "running": bool(state.get("Running", False)),
                    }
                )
            except Exception as exc:
                result.append(
                    {
                        "replica": replica,
                        "container": container_name,
                        "status": f"unreachable: {exc}",
                        "paused": False,
                        "running": False,
                    }
                )
    finally:
        client.close()
    return result


def control(replica: str, action: str) -> dict:
    if replica not in config.REPLICA_CONTAINERS:
        raise KeyError(replica)
    if action not in ("pause", "resume"):
        raise ValueError(action)

    client = _client()
    try:
        container = client.containers.get(config.REPLICA_CONTAINERS[replica])
        state = container.attrs.get("State", {})
        if action == "pause":
            if not state.get("Paused"):
                container.pause()
        else:
            if state.get("Paused"):
                container.unpause()
        container.reload()
        state = container.attrs.get("State", {})
        return {
            "replica": replica,
            "container": config.REPLICA_CONTAINERS[replica],
            "action": action,
            "status": state.get("Status", "unknown"),
            "paused": bool(state.get("Paused", False)),
        }
    finally:
        client.close()
