from __future__ import annotations

"""Separate supervised audit authority backed by a non-force Git ref.

Run this tool under a dedicated Windows service identity.  The trader must
not be able to read the Ed25519 private key or push to the configured ref.
The named-pipe response is emitted only after the remote fast-forward push is
verified.  This is deliberately classified as supervised/non-promotion and
not as formal WORM.
"""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import subprocess
import sys
import tempfile
from typing import Any, Mapping
import urllib.parse


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from live_trader.crypto_first_live_supervised_anchor import (  # noqa: E402
    COMMAND_SCHEMA,
    RESPONSE_SCHEMA,
    CryptoFirstLiveSupervisedAnchorError,
    FastForwardGitSupervisedAuthority,
    WindowsNamedPipeSupervisedAuthorityServer,
    decode_authenticated_pipe_request,
    encode_authenticated_pipe_response,
)


CONFIG_SCHEMA = "crypto-first-live-supervised-git-authority-config/v1"
CONFIG_FIELDS = {
    "schemaVersion",
    "authorityId",
    "namespaceId",
    "keyId",
    "authorityOsSid",
    "traderOsSid",
    "authorityRepoPath",
    "traderDataRoot",
    "privateKeyPath",
    "pipeAuthKeyPath",
    "pipeAddress",
    "remoteName",
    "remoteRef",
    "remoteUrlSha256",
    "statePath",
}
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
_REMOTE_RE = re.compile(r"^[A-Za-z0-9._-]{1,80}$")
_REF_RE = re.compile(r"^refs/heads/[A-Za-z0-9][A-Za-z0-9._/-]{2,180}$")
_SID_RE = re.compile(r"^S-1-(?:\d+-){1,14}\d+$", re.IGNORECASE)
_PIPE_RE = re.compile(r"^\\\\\.\\pipe\\[A-Za-z0-9._-]{8,120}$")
_GIT_OBJECT_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_MAX_WIRE_BYTES = 65_536


class SupervisedGitAuthorityToolError(RuntimeError):
    pass


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _load_secret(path: Path, *, label: str) -> bytes:
    raw = path.read_bytes().strip()
    if len(raw) == 64:
        try:
            decoded = bytes.fromhex(raw.decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            decoded = b""
        if len(decoded) == 32:
            return decoded
    if len(raw) < 32:
        raise SupervisedGitAuthorityToolError(label + "-invalid")
    return raw


def _current_windows_sid() -> str:
    if sys.platform != "win32":
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-windows-service-identity-required"
        )
    result = subprocess.run(
        ["whoami", "/user", "/fo", "csv", "/nh"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=5,
    )
    rows = list(csv.reader(result.stdout.splitlines()))
    if len(rows) != 1 or len(rows[0]) != 2:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-os-identity-unreadable"
        )
    sid = rows[0][1].strip().upper()
    if _SID_RE.fullmatch(sid) is None:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-os-identity-invalid"
        )
    return sid


def _assert_restricted_acl(path: Path, *, authority_sid: str) -> None:
    """Require authority/SYSTEM/Administrators-only readable ACL entries."""

    script = (
        "$ErrorActionPreference='Stop';"
        "$acl=Get-Acl -LiteralPath $args[0];"
        "$rows=@($acl.Access | ForEach-Object {"
        "$sid=$_.IdentityReference.Translate("
        "[System.Security.Principal.SecurityIdentifier]).Value;"
        "[pscustomobject]@{sid=$sid;type=$_.AccessControlType.ToString();"
        "rights=[int64]$_.FileSystemRights}});"
        "$rows | ConvertTo-Json -Compress"
    )
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            script,
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=10,
    )
    if result.returncode != 0:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-acl-unreadable"
        )
    try:
        decoded = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-acl-invalid"
        ) from exc
    rows = decoded if isinstance(decoded, list) else [decoded]
    allowed_sids = {
        authority_sid.upper(),
        "S-1-5-18",  # LocalSystem
        "S-1-5-32-544",  # Builtin Administrators
    }
    authority_full_control = False
    for row in rows:
        if not isinstance(row, Mapping):
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-acl-invalid"
            )
        sid = str(row.get("sid") or "").strip().upper()
        access_type = str(row.get("type") or "").strip().upper()
        try:
            rights = int(row.get("rights"))
        except (TypeError, ValueError) as exc:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-acl-invalid"
            ) from exc
        readable = bool(rights & 0x1 or rights & 0x20000)
        if access_type == "ALLOW" and readable and sid not in allowed_sids:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-acl-too-broad"
            )
        if (
            sid == authority_sid.upper()
            and access_type == "ALLOW"
            and (rights & 0x1F01FF) == 0x1F01FF
        ):
            authority_full_control = True
    if not authority_full_control:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-acl-full-control-missing"
        )


def _validate_state_path(value: object) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    path = PurePosixPath(raw)
    if (
        not raw
        or path.is_absolute()
        or ".." in path.parts
        or ".git" in path.parts
        or len(path.parts) > 8
        or len(raw) > 240
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-state-path-invalid"
        )
    return path.as_posix()


def _external_remote_url(value: str) -> bool:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme in {"https", "ssh"}:
        hostname = str(parsed.hostname or "").strip().lower()
        return bool(
            hostname
            and hostname not in {"localhost", "127.0.0.1", "::1"}
            and not (parsed.scheme == "https" and parsed.username)
            and not (parsed.scheme == "https" and parsed.password)
        )
    match = re.fullmatch(
        r"[A-Za-z0-9._-]+@([A-Za-z0-9.-]+):[^\s]+", value
    )
    return bool(
        match
        and match.group(1).lower()
        not in {"localhost", "127.0.0.1", "::1"}
    )


def load_config(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or set(raw) != CONFIG_FIELDS:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-config-fields-not-exact"
        )
    value = dict(raw)
    if value.get("schemaVersion") != CONFIG_SCHEMA:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-config-schema-invalid"
        )
    for field in ("authorityId", "namespaceId", "keyId"):
        if _ID_RE.fullmatch(str(value.get(field) or "")) is None:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-config-" + field + "-invalid"
            )
    sid = str(value.get("authorityOsSid") or "").strip().upper()
    trader_sid = str(value.get("traderOsSid") or "").strip().upper()
    if (
        _SID_RE.fullmatch(sid) is None
        or _SID_RE.fullmatch(trader_sid) is None
        or sid == trader_sid
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-config-os-sid-invalid"
        )
    remote = str(value.get("remoteName") or "").strip()
    ref = str(value.get("remoteRef") or "").strip()
    remote_hash = str(value.get("remoteUrlSha256") or "").strip().lower()
    pipe_address = str(value.get("pipeAddress") or "").strip()
    if (
        _REMOTE_RE.fullmatch(remote) is None
        or _REF_RE.fullmatch(ref) is None
        or ".." in ref.split("/")
        or _HASH_RE.fullmatch(remote_hash) is None
        or _PIPE_RE.fullmatch(pipe_address) is None
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-config-route-invalid"
        )
    value["statePath"] = _validate_state_path(value.get("statePath"))
    for field in (
        "authorityRepoPath",
        "traderDataRoot",
        "privateKeyPath",
        "pipeAuthKeyPath",
    ):
        candidate = Path(str(value.get(field) or "")).expanduser()
        if not candidate.is_absolute():
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-config-path-not-absolute:"
                + field
            )
        value[field] = str(candidate.resolve(strict=True))
    repo = Path(value["authorityRepoPath"])
    trader = Path(value["traderDataRoot"])
    private_key = Path(value["privateKeyPath"])
    pipe_key = Path(value["pipeAuthKeyPath"])
    if (
        _is_within(repo, trader)
        or _is_within(trader, repo)
        or _is_within(private_key, trader)
        or _is_within(pipe_key, trader)
        or not private_key.is_file()
        or not pipe_key.is_file()
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-path-separation-invalid"
        )
    value["authorityOsSid"] = sid
    value["traderOsSid"] = trader_sid
    value["remoteUrlSha256"] = remote_hash
    value["pipeAddress"] = pipe_address
    value["remoteName"] = remote
    value["remoteRef"] = ref
    return value


class GitFastForwardRemoteStore:
    """Dedicated clean clone that updates one remote ref without force."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.repo = Path(str(config["authorityRepoPath"]))
        self.remote_name = str(config["remoteName"])
        self.remote_ref = str(config["remoteRef"])
        self.remote_url_sha256 = str(config["remoteUrlSha256"])
        self.state_path = str(config["statePath"])
        self._observed_head = ""
        if not (self.repo / ".git").is_dir():
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-dedicated-git-clone-required"
            )
        top = self._git("rev-parse", "--show-toplevel").strip()
        if Path(top).resolve() != self.repo.resolve():
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-git-root-changed"
            )
        remote_url = self._git(
            "remote", "get-url", self.remote_name
        ).strip()
        if (
            _sha256_text(remote_url) != self.remote_url_sha256
            or not _external_remote_url(remote_url)
        ):
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-url-pin-changed"
            )
        if self._git("status", "--porcelain") != "":
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-repository-not-clean"
            )

    def _git(
        self,
        *args: str,
        input_text: str | None = None,
        check: bool = True,
    ) -> str:
        try:
            result = subprocess.run(
                ["git", *args],
                cwd=self.repo,
                input=input_text,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="strict",
                check=False,
                timeout=30,
                env={
                    **os.environ,
                    "GIT_TERMINAL_PROMPT": "0",
                    "GIT_ASKPASS": "",
                },
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-git-execution-failed"
            ) from exc
        if check and result.returncode != 0:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-git-command-failed:"
                + str(args[0] if args else "unknown")
            )
        return result.stdout

    def _fetch_head(self) -> str:
        self._git(
            "fetch",
            "--no-tags",
            self.remote_name,
            self.remote_ref,
        )
        head = self._git("rev-parse", "--verify", "FETCH_HEAD").strip().lower()
        if _GIT_OBJECT_RE.fullmatch(head) is None:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-head-invalid"
            )
        return head

    def _head_tree_kind(self, head: str) -> str:
        """Accept only the provisioned empty tree or the one audit blob.

        A write deploy key is intentionally able to fast-forward the anchor
        ref.  Treating an arbitrary tree at that ref as authority state would
        let a stolen deploy key smuggle workflows or unrelated content into
        the dedicated repository.  The signed state is therefore also bound
        to an exact Git tree shape and file mode.
        """

        listing = self._git(
            "ls-tree", "-r", "--full-tree", head
        ).rstrip("\n")
        if listing == "":
            return "empty"
        lines = listing.split("\n")
        if len(lines) != 1 or "\t" not in lines[0]:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-tree-invalid"
            )
        metadata, path = lines[0].split("\t", 1)
        fields = metadata.split()
        if (
            len(fields) != 3
            or fields[0] != "100644"
            or fields[1] != "blob"
            or _GIT_OBJECT_RE.fullmatch(fields[2]) is None
            or path != self.state_path
        ):
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-tree-invalid"
            )
        return "state"

    def provision_empty_ref(self) -> dict[str, Any]:
        existing = self._git(
            "ls-remote", "--heads", self.remote_name, self.remote_ref
        ).strip()
        if existing:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-ref-already-exists"
            )
        empty_tree = self._git("mktree", input_text="").strip().lower()
        commit = self._git(
            "-c",
            "user.name=Crypto First Live Supervised Authority",
            "-c",
            "user.email=crypto-first-live-authority@invalid",
            "commit-tree",
            empty_tree,
            "-m",
            "provision supervised non-promotion audit anchor",
        ).strip().lower()
        if _GIT_OBJECT_RE.fullmatch(commit) is None:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-provision-commit-invalid"
            )
        self._git(
            "push",
            "--porcelain",
            self.remote_name,
            commit + ":" + self.remote_ref,
        )
        remote = self._git(
            "ls-remote", self.remote_name, self.remote_ref
        ).split()
        if not remote or remote[0].strip().lower() != commit:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-provision-remote-verify-failed"
            )
        if self._head_tree_kind(commit) != "empty":
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-provision-tree-invalid"
            )
        return {
            "provisioned": True,
            "remoteRef": self.remote_ref,
            "remoteCommitId": commit,
            "forcePushUsed": False,
        }

    def read_head(self) -> Mapping[str, Any] | None:
        head = self._fetch_head()
        self._observed_head = head
        tree_kind = self._head_tree_kind(head)
        if tree_kind == "empty":
            return None
        result = subprocess.run(
            ["git", "show", head + ":" + self.state_path],
            cwd=self.repo,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            check=False,
            timeout=10,
        )
        if result.returncode != 0:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-state-unreadable"
            )
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-state-json-invalid"
            ) from exc
        if not isinstance(value, dict):
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-state-invalid"
            )
        return dict(value)

    def fast_forward_append(self, value: Mapping[str, Any]) -> str:
        if not self._observed_head:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-read-before-append-required"
            )
        current = self._fetch_head()
        if current != self._observed_head:
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-remote-head-cas-changed"
            )
        self._head_tree_kind(current)
        if self._git("status", "--porcelain") != "":
            raise SupervisedGitAuthorityToolError(
                "supervised-authority-repository-not-clean"
            )
        self._git("checkout", "--detach", current)
        target = self.repo.joinpath(*PurePosixPath(self.state_path).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        encoded = _canonical(value) + b"\n"
        temporary_path = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=target.name + ".",
                suffix=".tmp",
                dir=target.parent,
                delete=False,
            ) as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
                temporary_path = handle.name
            os.replace(temporary_path, target)
            temporary_path = ""
            self._git("add", "--", self.state_path)
            self._git(
                "-c",
                "user.name=Crypto First Live Supervised Authority",
                "-c",
                "user.email=crypto-first-live-authority@invalid",
                "commit",
                "--no-gpg-sign",
                "-m",
                "append supervised audit checkpoint",
                "--",
                self.state_path,
            )
            commit = self._git("rev-parse", "HEAD").strip().lower()
            if _GIT_OBJECT_RE.fullmatch(commit) is None:
                raise SupervisedGitAuthorityToolError(
                    "supervised-authority-commit-invalid"
                )
            if self._head_tree_kind(commit) != "state":
                raise SupervisedGitAuthorityToolError(
                    "supervised-authority-commit-tree-invalid"
                )
            self._git(
                "push",
                "--porcelain",
                self.remote_name,
                "HEAD:" + self.remote_ref,
            )
            remote = self._git(
                "ls-remote", self.remote_name, self.remote_ref
            ).split()
            if not remote or remote[0].strip().lower() != commit:
                raise SupervisedGitAuthorityToolError(
                    "supervised-authority-remote-commit-verify-failed"
                )
            self._observed_head = commit
            return commit
        finally:
            if temporary_path:
                try:
                    Path(temporary_path).unlink()
                except OSError:
                    pass


def _build_authority(config: Mapping[str, Any]) -> tuple[Any, bytes, Any]:
    current_sid = _current_windows_sid()
    if current_sid != str(config["authorityOsSid"]).upper():
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-service-identity-changed"
        )
    if current_sid == str(config["traderOsSid"]).upper():
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-trader-identity-not-independent"
        )
    _assert_restricted_acl(
        Path(str(config["privateKeyPath"])), authority_sid=current_sid
    )
    _assert_restricted_acl(
        Path(str(config["authorityRepoPath"])), authority_sid=current_sid
    )
    store = GitFastForwardRemoteStore(config)
    private_key = Path(str(config["privateKeyPath"])).read_bytes()
    pipe_authkey = _load_secret(
        Path(str(config["pipeAuthKeyPath"])), label="pipe-authkey"
    )
    authority = FastForwardGitSupervisedAuthority(
        authority_id=str(config["authorityId"]),
        namespace_id=str(config["namespaceId"]),
        key_id=str(config["keyId"]),
        private_key=private_key,
        read_head=store.read_head,
        fast_forward_append=store.fast_forward_append,
    )
    return authority, pipe_authkey, store


def _parse_command(raw: bytes) -> tuple[str, dict[str, Any]]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-command-json-invalid"
        ) from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"schemaVersion", "requestId", "request"}
        or value.get("schemaVersion") != COMMAND_SCHEMA
        or _ID_RE.fullmatch(str(value.get("requestId") or "")) is None
        or not isinstance(value.get("request"), dict)
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-command-invalid"
        )
    return str(value["requestId"]), dict(value["request"])


def _revalidate_runtime_config(
    config_path: Path,
    expected: Mapping[str, Any],
    *,
    private_key_sha256: str,
    pipe_authkey: bytes,
) -> None:
    fresh = load_config(config_path.resolve(strict=True))
    if not secrets.compare_digest(
        _canonical(fresh), _canonical(expected)
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-config-changed"
        )
    current_sid = _current_windows_sid()
    if (
        current_sid != str(fresh["authorityOsSid"]).upper()
        or current_sid == str(fresh["traderOsSid"]).upper()
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-service-identity-changed"
        )
    private_path = Path(str(fresh["privateKeyPath"]))
    _assert_restricted_acl(private_path, authority_sid=current_sid)
    _assert_restricted_acl(
        Path(str(fresh["authorityRepoPath"])), authority_sid=current_sid
    )
    if not secrets.compare_digest(
        hashlib.sha256(private_path.read_bytes()).hexdigest(),
        private_key_sha256,
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-private-key-changed"
        )
    current_pipe_key = _load_secret(
        Path(str(fresh["pipeAuthKeyPath"])), label="pipe-authkey"
    )
    if len(current_pipe_key) != 32 or not secrets.compare_digest(
        current_pipe_key, pipe_authkey
    ):
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-pipe-authkey-changed"
        )
    # Constructor rechecks the dedicated clone root, clean worktree, pinned
    # external URL, and exact remote ref configuration without network I/O.
    GitFastForwardRemoteStore(fresh)


def serve(
    config: Mapping[str, Any],
    *,
    config_path: Path,
    once: bool = False,
) -> None:
    if sys.platform != "win32":
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-windows-service-required"
        )
    authority, pipe_authkey, _store = _build_authority(config)
    if len(pipe_authkey) != 32:
        raise SupervisedGitAuthorityToolError(
            "supervised-authority-pipe-authkey-invalid"
        )
    private_key_sha256 = hashlib.sha256(
        Path(str(config["privateKeyPath"])).read_bytes()
    ).hexdigest()
    listener = WindowsNamedPipeSupervisedAuthorityServer(
        pipe_address=str(config["pipeAddress"]),
        trader_os_sid=str(config["traderOsSid"]),
        timeout_seconds=5.0,
    )
    print(
        json.dumps(
            {
                "ok": True,
                "serving": True,
                "pipeAddress": config["pipeAddress"],
                "transport": "WIN32_CTYPES_LENGTH_PREFIXED_HMAC",
                "authorityId": config["authorityId"],
                "namespaceId": config["namespaceId"],
                "authorityOsSid": config["authorityOsSid"],
                "traderOsSid": config["traderOsSid"],
                "remoteClientsRejected": True,
                "maximumWireBytes": 65_536,
                "formalWorm": False,
                "networkOrderPostAllowed": False,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    while True:
        connection = listener.accept(connect_timeout_seconds=1.0)
        if connection is None:
            continue
        request_id = "invalid-command-00000000"
        request_nonce = ""
        authenticated = False
        response: dict[str, Any] | None = None
        try:
            _revalidate_runtime_config(
                config_path,
                config,
                private_key_sha256=private_key_sha256,
                pipe_authkey=pipe_authkey,
            )
            raw = connection.recv_bytes(_MAX_WIRE_BYTES)
            command, request_nonce = decode_authenticated_pipe_request(
                raw, authkey=pipe_authkey
            )
            authenticated = True
            candidate_id = str(command.get("requestId") or "").strip()
            if _ID_RE.fullmatch(candidate_id) is not None:
                request_id = candidate_id
            request_id, request = _parse_command(_canonical(command))
            receipt = authority.checkpoint(request)
            response = {
                "schemaVersion": RESPONSE_SCHEMA,
                "requestId": request_id,
                "ok": True,
                "receipt": receipt,
                "error": "",
            }
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "event": "request-rejected",
                        "requestId": request_id,
                        "authenticated": authenticated,
                        "errorType": type(exc).__name__,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
            if authenticated:
                response = {
                    "schemaVersion": RESPONSE_SCHEMA,
                    "requestId": request_id,
                    "ok": False,
                    "receipt": None,
                    "error": "authority-request-rejected",
                }
        try:
            if response is not None:
                encoded = encode_authenticated_pipe_response(
                    response,
                    request_nonce=request_nonce,
                    request_id=request_id,
                    authkey=pipe_authkey,
                )
                connection.send_bytes(encoded)
        finally:
            connection.close(server_side=True)
        if once:
            break


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run/provision the separate signed remote-fast-forward audit "
            "authority for supervised non-promotion first-live."
        )
    )
    parser.add_argument("--config", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check-config-only", action="store_true")
    mode.add_argument("--provision-ref", action="store_true")
    mode.add_argument("--serve", action="store_true")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Serve one request, for supervised provisioning checks only.",
    )
    args = parser.parse_args()
    config_path = args.config.resolve(strict=True)
    config = load_config(config_path)
    authority, _pipe_authkey, store = _build_authority(config)
    if args.check_config_only:
        print(
            json.dumps(
                {
                    "ok": True,
                    "configured": True,
                    "authorityId": config["authorityId"],
                    "namespaceId": config["namespaceId"],
                    "authorityOsSid": config["authorityOsSid"],
                    "remoteRef": config["remoteRef"],
                    "remoteUrlPinned": True,
                    "privateKeyOutsideTraderDataRoot": True,
                    "networkRequestCount": 0,
                    "networkOrderPostAllowed": False,
                    "formalWorm": False,
                },
                sort_keys=True,
            )
        )
        del authority
        return 0
    if args.provision_ref:
        print(json.dumps(store.provision_empty_ref(), sort_keys=True))
        return 0
    if args.once and not args.serve:
        raise SupervisedGitAuthorityToolError("--once-requires---serve")
    serve(config, config_path=config_path, once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
