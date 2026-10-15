"""Select an exact active account/deployment session without profile aliases."""
from __future__ import annotations

from typing import Any, Callable, Mapping


def session_document(session: Any) -> dict[str, Any]:
    if isinstance(session, Mapping):
        return dict(session)
    method = getattr(session, "to_dict", None)
    return dict(method()) if callable(method) else {}


def select_session_bindings(active: Mapping[str, Any], loader: Callable, *,
                            runtime_id: str = "", deployment_id: str = "",
                            broker_id: str = "", profile_id: str = "") -> list[tuple[str, Any]]:
    """Unknown explicit selectors never fall back to another account/session.

    A legacy profile key is accepted only for that profile. Scoped keys must
    agree with the immutable session metadata, including when no selector was
    supplied (Kill/Stop all). An unreadable session raises instead of vanishing.
    """
    result = []
    if runtime_id and runtime_id not in active:
        return result
    for key, session_id in active.items():
        if runtime_id and key != runtime_id:
            continue
        if not session_id:
            continue
        session = loader(str(session_id))
        if session is None:
            raise ValueError("active-runtime-session-unreadable")
        document = session_document(session)
        metadata = document.get("metadata") or {}
        if not isinstance(metadata, Mapping):
            raise ValueError("active-runtime-session-metadata-invalid")
        session_scope = str(metadata.get("runtimeScopeId") or metadata.get("runtime_scope_id") or "")
        if key not in {"stock", "crypto"} and session_scope != key:
            raise ValueError("active-runtime-session-scope-mismatch")
        session_profile = str(document.get("profile") or (key if key in {"stock", "crypto"} else ""))
        session_deployment = str(metadata.get("runtimeDeploymentId") or document.get("deploymentId") or "")
        session_broker = str(metadata.get("brokerId") or "").lower()
        if profile_id and session_profile != profile_id:
            continue
        if deployment_id and session_deployment != deployment_id:
            continue
        if broker_id and session_broker != broker_id:
            # Historical sessions did not always include brokerId. The exact
            # immutable manifest is still checked at the dispatch boundary.
            if key not in {"stock", "crypto"} or session_broker:
                continue
            if (broker_id == "kis") != (session_profile == "stock"):
                continue
        result.append((key, session))
    return result


def unique_session_binding(active: Mapping[str, Any], loader: Callable, **selectors):
    rows = select_session_bindings(active, loader, **selectors)
    if len(rows) != 1:
        return "", None
    return rows[0]


def runtime_rows(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Prefer real runtimes; compatibility profile aggregates are not sessions."""
    scoped = snapshot.get("runtimes")
    if isinstance(scoped, Mapping) and scoped:
        return {str(key): dict(value) for key, value in scoped.items() if isinstance(value, Mapping)}
    profiles = snapshot.get("profiles")
    if isinstance(profiles, Mapping):
        return {str(key): dict(value) for key, value in profiles.items() if isinstance(value, Mapping) and value.get("ambiguous") is not True}
    return {str(snapshot.get("profileId") or ""): dict(snapshot)}
