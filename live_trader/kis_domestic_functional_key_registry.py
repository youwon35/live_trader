from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
import sqlite3
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

try:
    from Crypto.PublicKey import ECC
    from Crypto.Signature import eddsa
except ImportError:  # pragma: no cover - explicit production blocker/status.
    ECC = None
    eddsa = None

from .kis_domestic_functional_contract import PDNO, ROUTE


KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_RELEASE_AVAILABLE = False

REGISTRY_SCHEMA = "kis-domestic-functional-verify-only-registry/v1"
REGISTRY_STATUS_SCHEMA = "kis-domestic-functional-key-registry-status/v1"
ACCEPTANCE_SCHEMA_VERSION = "kis-domestic-functional-key-registry-acceptance/v1"
FACTORY_BINDING_SCHEMA = "kis-domestic-functional-key-registry-factory-binding/v1"
GRAPH_BINDING_SCHEMA = "kis-domestic-functional-key-registry-graph-binding/v1"
COMPONENT_BINDING_SCHEMA = (
    "kis-domestic-functional-key-registry-component-binding/v1"
)
KEY_ALGORITHM = "ED25519"
MAX_TRUSTED_CLOCK_DIVERGENCE_SECONDS = 5.0
_ROOT_SIGNATURE_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY\0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)

KEY_PURPOSES = (
    "LANE_RECORD_VERIFY",
    "SOURCE_RECORD_VERIFY",
    "MARKET_SOURCE_RECORD_VERIFY",
    "MARKET_ARCHIVE_CAPTURE_VERIFY",
    "READERS_COMPONENT_VERIFY",
    "ROLLING_RECORD_VERIFY",
    "HEARTBEAT_RECORD_VERIFY",
    "MUTATION_RECORD_VERIFY",
    "CAPABILITY_REVOKE_VERIFY",
    "QUOTE_RECORD_VERIFY",
    "GRAPH_RECORD_VERIFY",
    "TRUTH_RECORD_VERIFY",
    "SIGNED_GET_CAPTURE_VERIFY",
    "OWNER_STATE_VERIFY",
    "ARCHIVE_EXTRACTION_VERIFY",
)

_COMPONENT_BINDING_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "component",
    "sourceFileHash",
    "protocolHash",
    "schemaFingerprint",
    "statusHash",
    "authorityKeyIdHash",
    "authorityPurpose",
    "signatureDomain",
}

VerifyCallback = Callable[[str, Mapping[str, Any], str], bool]
TrustedClock = Callable[[], datetime]
TrustedMonotonicClock = Callable[[], int]


@dataclass(frozen=True)
class ProductionKeyRegistryPins:
    registry_id: str
    manifest_file_hash: str
    root_public_key_pem: str
    root_key_id_hash: str
    account_fingerprint: str
    credential_configuration_hash: str
    code_manifest_hash: str
    graph_file_hash: str
    graph_protocol_hash: str
    graph_schema_fingerprint: str

    def canonical_body(self) -> dict[str, Any]:
        registry_id = _identifier(self.registry_id, "key-registry-factory-registry-id")
        root_key = _load_public_key(
            self.root_public_key_pem, "key-registry-factory-root-public-key"
        )
        exported = root_key.export_key(format="PEM")
        root_hash = _sha(self.root_key_id_hash, "key-registry-factory-root-key-id")
        if not hmac.compare_digest(
            root_hash, hashlib.sha256(exported.encode("utf-8")).hexdigest()
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-factory-root-key-id-mismatch"
            )
        return {
            "schemaVersion": FACTORY_BINDING_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": registry_id,
            "manifestFileHash": _sha(
                self.manifest_file_hash, "key-registry-factory-manifest-file-hash"
            ),
            "rootKeyIdHash": root_hash,
            "accountFingerprint": _sha(
                self.account_fingerprint, "key-registry-factory-account-fingerprint"
            ),
            "credentialConfigurationHash": _sha(
                self.credential_configuration_hash,
                "key-registry-factory-credential-configuration-hash",
            ),
            "codeManifestHash": _sha(
                self.code_manifest_hash, "key-registry-factory-code-manifest-hash"
            ),
            "graphFileHash": _sha(
                self.graph_file_hash, "key-registry-factory-graph-file-hash"
            ),
            "graphProtocolHash": _sha(
                self.graph_protocol_hash, "key-registry-factory-graph-protocol-hash"
            ),
            "graphSchemaFingerprint": _sha(
                self.graph_schema_fingerprint,
                "key-registry-factory-graph-schema-fingerprint",
            ),
        }


class KisDomesticFunctionalKeyRegistryBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalKeyRegistryBlocked(
            "key-registry-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    return value


def _time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid") from exc
    if result.tzinfo is None:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    return result.astimezone(timezone.utc)


def _now(clock: TrustedClock) -> datetime:
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalKeyRegistryBlocked(
            "key-registry-trusted-now-invalid"
        )
    return value.astimezone(timezone.utc)


def _decode_signature(value: Any, label: str) -> bytes:
    if type(value) is not str:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    try:
        result = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid") from exc
    if len(result) != 64:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    return result


def _load_public_key(value: str, label: str):
    if ECC is None or eddsa is None:
        raise KisDomesticFunctionalKeyRegistryBlocked(
            "key-registry-asymmetric-runtime-unavailable"
        )
    if type(value) is not str or "PRIVATE KEY" in value:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    try:
        key = ECC.import_key(value)
    except (ValueError, TypeError, IndexError) as exc:
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid") from exc
    if key.has_private() or getattr(key, "curve", None) != "Ed25519":
        raise KisDomesticFunctionalKeyRegistryBlocked(f"{label}-invalid")
    return key


def _verify_ed25519(public_key, message: bytes, signature: bytes) -> bool:
    try:
        eddsa.new(public_key, mode="rfc8032").verify(message, signature)
        return True
    except (ValueError, TypeError):
        return False


_ACCEPTANCE_SCHEMA_SQL = (
    "CREATE TABLE IF NOT EXISTS kis_key_registry_acceptance_meta("
    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_version TEXT NOT NULL,"
    "schema_fingerprint TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS kis_key_registry_acceptance("
    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),registry_id TEXT NOT NULL,"
    "accepted_epoch INTEGER NOT NULL CHECK(accepted_epoch>=1),"
    "accepted_manifest_hash TEXT NOT NULL,previous_manifest_hash TEXT,"
    "accepted_wall_at TEXT NOT NULL,accepted_monotonic_ns INTEGER NOT NULL CHECK(accepted_monotonic_ns>=0),"
    "clock_generation TEXT NOT NULL,root_key_id_hash TEXT NOT NULL,"
    "account_fingerprint TEXT NOT NULL,credential_configuration_hash TEXT NOT NULL,"
    "code_manifest_hash TEXT NOT NULL,factory_binding_hash TEXT NOT NULL,"
    "graph_binding_hash TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=1),"
    "transition_head_hash TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS kis_key_registry_acceptance_transition("
    "ordinal INTEGER PRIMARY KEY,registry_id TEXT NOT NULL,accepted_epoch INTEGER NOT NULL,"
    "manifest_hash TEXT NOT NULL,previous_manifest_hash TEXT,accepted_wall_at TEXT NOT NULL,"
    "accepted_monotonic_ns INTEGER NOT NULL,clock_generation TEXT NOT NULL,"
    "factory_binding_hash TEXT NOT NULL,graph_binding_hash TEXT NOT NULL,"
    "previous_head_hash TEXT NOT NULL,transition_hash TEXT NOT NULL UNIQUE)",
)


def _acceptance_schema_snapshot(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ).fetchall()
    return [
        {
            "type": row[0],
            "name": row[1],
            "table": row[2],
            "sql": " ".join((row[3] or "").split()),
            "columns": [
                list(item)
                for item in conn.execute(f'PRAGMA table_info("{row[1]}")').fetchall()
            ] if row[0] == "table" else [],
        }
        for row in rows
    ]


def _expected_acceptance_schema() -> tuple[list[dict[str, Any]], str]:
    conn = sqlite3.connect(":memory:")
    try:
        for statement in _ACCEPTANCE_SCHEMA_SQL:
            conn.execute(statement)
        snapshot = _acceptance_schema_snapshot(conn)
        return snapshot, _hash(snapshot)
    finally:
        conn.close()


_EXPECTED_ACCEPTANCE_SCHEMA, ACCEPTANCE_SCHEMA_FINGERPRINT = (
    _expected_acceptance_schema()
)


class VerifyOnlyKeyRegistry:
    """Atomic local manifest loader exposing only public verification methods."""

    def __init__(
        self,
        manifest_path: str | Path,
        *,
        pinned_root_public_key_pem: str | None,
        pinned_root_key_id_hash: str,
        expected_account_fingerprint: str,
        expected_credential_configuration_hash: str,
        expected_code_manifest_hash: str,
        trusted_clock: TrustedClock,
        trusted_monotonic_clock: TrustedMonotonicClock | None = None,
        acceptance_db_path: str | Path | None = None,
        clock_generation: str | None = None,
        allow_mock_root_verifier: bool = False,
        mock_root_verifier: VerifyCallback | None = None,
        _production_factory_pins: ProductionKeyRegistryPins | None = None,
    ) -> None:
        if not callable(trusted_clock):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-clock-invalid"
            )
        if type(allow_mock_root_verifier) is not bool:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-mock-flag-invalid"
            )
        self.path = Path(manifest_path).expanduser().resolve()
        if not self.path.is_file():
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-manifest-missing"
            )
        self.clock = trusted_clock
        durable_values = (
            trusted_monotonic_clock,
            acceptance_db_path,
            clock_generation,
        )
        if any(value is not None for value in durable_values) and not all(
            value is not None for value in durable_values
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-durable-acceptance-arguments-incomplete"
            )
        if trusted_monotonic_clock is not None and not callable(
            trusted_monotonic_clock
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-monotonic-clock-invalid"
            )
        self.monotonic_clock = trusted_monotonic_clock
        self.acceptance_path = (
            Path(acceptance_db_path).expanduser().resolve()
            if acceptance_db_path is not None
            else None
        )
        self.clock_generation = (
            _identifier(clock_generation, "key-registry-clock-generation")
            if clock_generation is not None
            else None
        )
        self._last_wall: datetime | None = None
        self._last_monotonic_ns: int | None = None
        if _production_factory_pins is not None and type(
            _production_factory_pins
        ) is not ProductionKeyRegistryPins:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-production-factory-pins-type-invalid"
            )
        self._factory_pin_body = (
            _production_factory_pins.canonical_body()
            if _production_factory_pins is not None
            else None
        )
        self.production_factory_pins_bound = self._factory_pin_body is not None
        self.factory_binding_hash = (
            _hash(self._factory_pin_body)
            if self._factory_pin_body is not None
            else "0" * 64
        )
        self.graph_binding_hash = "0" * 64
        self.durable_acceptance_verified = False
        self.acceptance_revision = 0
        self.acceptance_head_hash = "0" * 64
        self.root_key_id_hash = _sha(
            pinned_root_key_id_hash, "key-registry-root-key-id"
        )
        self.account_fingerprint = _sha(
            expected_account_fingerprint, "key-registry-account-fingerprint"
        )
        self.credential_configuration_hash = _sha(
            expected_credential_configuration_hash,
            "key-registry-credential-configuration-hash",
        )
        self.code_manifest_hash = _sha(
            expected_code_manifest_hash, "key-registry-code-manifest-hash"
        )
        if allow_mock_root_verifier:
            if not callable(mock_root_verifier):
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-mock-root-verifier-invalid"
                )
            self._root_key = None
            self._mock_root_verifier = mock_root_verifier
            self.asymmetric_root_verified = False
        else:
            if mock_root_verifier is not None:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-mock-root-verifier-forbidden"
                )
            self._root_key = _load_public_key(
                pinned_root_public_key_pem,
                "key-registry-root-public-key",
            )
            exported = self._root_key.export_key(format="PEM")
            expected_root_id = hashlib.sha256(exported.encode("utf-8")).hexdigest()
            if not hmac.compare_digest(expected_root_id, self.root_key_id_hash):
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-root-key-id-mismatch"
                )
            self._mock_root_verifier = None
            self.asymmetric_root_verified = True
        self._load_atomic()
        self.production_authority_pinned = bool(
            self.asymmetric_root_verified
            and self.durable_acceptance_verified
            and self.production_factory_pins_bound
            and self.monotonic_clock is not None
        )
        self._assert_acceptance_current()

    def _trusted_pair(self) -> tuple[datetime, int | None]:
        wall = _now(self.clock)
        monotonic_ns: int | None = None
        if self.monotonic_clock is not None:
            value = self.monotonic_clock()
            if type(value) is not int or value < 0:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-trusted-monotonic-invalid"
                )
            monotonic_ns = value
        if self._last_wall is not None:
            if wall < self._last_wall:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-trusted-wall-rollback"
                )
            if monotonic_ns is not None:
                if monotonic_ns < self._last_monotonic_ns:
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-trusted-monotonic-rollback"
                    )
                wall_elapsed = (wall - self._last_wall).total_seconds()
                mono_elapsed = (
                    monotonic_ns - self._last_monotonic_ns
                ) / 1_000_000_000
                if (
                    not math.isfinite(wall_elapsed)
                    or not math.isfinite(mono_elapsed)
                    or abs(wall_elapsed - mono_elapsed)
                    > MAX_TRUSTED_CLOCK_DIVERGENCE_SECONDS
                ):
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-trusted-clock-divergence"
                    )
        self._last_wall = wall
        self._last_monotonic_ns = monotonic_ns
        return wall, monotonic_ns

    def _verify_root(self, body: Mapping[str, Any], signature_text: str) -> bool:
        if self._root_key is not None:
            return _verify_ed25519(
                self._root_key,
                _ROOT_SIGNATURE_DOMAIN + _canonical(body),
                _decode_signature(signature_text, "key-registry-root-signature"),
            )
        try:
            return self._mock_root_verifier(
                "KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY",
                deepcopy(dict(body)),
                signature_text,
            ) is True
        except BaseException:
            return False

    def _load_atomic(self) -> None:
        before = self.path.read_bytes()
        before_hash = hashlib.sha256(before).hexdigest()
        try:
            document = json.loads(before)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-manifest-json-invalid"
            ) from exc
        after = self.path.read_bytes()
        after_hash = hashlib.sha256(after).hexdigest()
        if before != after or not hmac.compare_digest(before_hash, after_hash):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-manifest-changed-during-load"
            )
        if not isinstance(document, Mapping) or set(document) != {
            "manifest",
            "manifestHash",
            "rootKeyIdHash",
            "rootSignature",
        }:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-envelope-invalid"
            )
        manifest = document["manifest"]
        if not isinstance(manifest, Mapping) or set(manifest) != {
            "schemaVersion",
            "route",
            "pdno",
            "registryId",
            "registryEpoch",
            "notBefore",
            "notAfter",
            "accountFingerprint",
            "credentialConfigurationHash",
            "codeManifestHash",
            "previousManifestHash",
            "keys",
            "revocations",
            "componentBindings",
        }:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-manifest-contract-invalid"
            )
        manifest_hash = _sha(
            document["manifestHash"], "key-registry-manifest-hash"
        )
        signed = {**dict(manifest), "manifestHash": manifest_hash}
        if (
            manifest_hash != _hash(manifest)
            or document["rootKeyIdHash"] != self.root_key_id_hash
            or not self._verify_root(signed, document["rootSignature"])
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-root-signature-invalid"
            )
        now, monotonic_ns = self._trusted_pair()
        not_before = _time(manifest["notBefore"], "key-registry-not-before")
        not_after = _time(manifest["notAfter"], "key-registry-not-after")
        if (
            manifest["schemaVersion"] != REGISTRY_SCHEMA
            or manifest["route"] != ROUTE
            or manifest["pdno"] != PDNO
            or type(manifest["registryId"]) is not str
            or not _IDENTIFIER.fullmatch(manifest["registryId"])
            or type(manifest["registryEpoch"]) is not int
            or manifest["registryEpoch"] < 1
            or not_before >= not_after
            or now < not_before
            or now >= not_after
            or manifest["accountFingerprint"] != self.account_fingerprint
            or manifest["credentialConfigurationHash"]
            != self.credential_configuration_hash
            or manifest["codeManifestHash"] != self.code_manifest_hash
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-manifest-binding-or-time-invalid"
            )
        previous = manifest["previousManifestHash"]
        if manifest["registryEpoch"] == 1 and previous is not None:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-first-epoch-previous-hash-invalid"
            )
        if manifest["registryEpoch"] > 1 and previous is None:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-rotation-previous-hash-missing"
            )
        if previous is not None:
            _sha(previous, "key-registry-previous-manifest-hash")
        keys = manifest["keys"]
        revocations = manifest["revocations"]
        component_bindings = manifest["componentBindings"]
        if (
            not isinstance(keys, list)
            or not isinstance(revocations, list)
            or not isinstance(component_bindings, list)
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-keys-revocations-or-bindings-invalid"
            )
        revoked: dict[str, dict[str, Any]] = {}
        for item in revocations:
            if not isinstance(item, Mapping) or set(item) != {
                "keyIdHash",
                "revokedAt",
                "reason",
            }:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-revocation-invalid"
                )
            key_id_hash = _sha(item["keyIdHash"], "key-registry-revoked-key")
            revoked_at = _time(item["revokedAt"], "key-registry-revoked-at")
            if revoked_at > now:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-revocation-future-dated"
                )
            _identifier(item["reason"], "key-registry-revocation-reason")
            if key_id_hash in revoked:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-revocation-duplicate"
                )
            revoked[key_id_hash] = deepcopy(dict(item))

        by_purpose: dict[str, list[dict[str, Any]]] = {
            purpose: [] for purpose in KEY_PURPOSES
        }
        key_ids: set[str] = set()
        for item in keys:
            if not isinstance(item, Mapping) or set(item) != {
                "keyId",
                "keyIdHash",
                "purpose",
                "algorithm",
                "rotationEpoch",
                "notBefore",
                "notAfter",
                "accountFingerprint",
                "credentialConfigurationHash",
                "codeManifestHash",
                "publicKeyPem",
            }:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-key-contract-invalid"
                )
            key_id = _identifier(item["keyId"], "key-registry-key-id")
            key_id_hash = _sha(item["keyIdHash"], "key-registry-key-id-hash")
            purpose = item["purpose"]
            key_not_before = _time(
                item["notBefore"], "key-registry-key-not-before"
            )
            key_not_after = _time(
                item["notAfter"], "key-registry-key-not-after"
            )
            public_key = _load_public_key(
                item["publicKeyPem"], "key-registry-public-key"
            )
            exported = public_key.export_key(format="PEM")
            if (
                purpose not in by_purpose
                or item["algorithm"] != KEY_ALGORITHM
                or type(item["rotationEpoch"]) is not int
                or item["rotationEpoch"] < 1
                or item["rotationEpoch"] > manifest["registryEpoch"]
                or key_not_before < not_before
                or key_not_after > not_after
                or key_not_before >= key_not_after
                or item["accountFingerprint"] != self.account_fingerprint
                or item["credentialConfigurationHash"]
                != self.credential_configuration_hash
                or item["codeManifestHash"] != self.code_manifest_hash
                or key_id_hash != hashlib.sha256(exported.encode()).hexdigest()
                or key_id_hash in key_ids
            ):
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-key-binding-invalid"
                )
            key_ids.add(key_id_hash)
            by_purpose[purpose].append(
                {
                    **deepcopy(dict(item)),
                    "_publicKey": public_key,
                    "_notBefore": key_not_before,
                    "_notAfter": key_not_after,
                    "revoked": key_id_hash in revoked,
                }
            )
        if not set(revoked).issubset(key_ids):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-revocation-key-missing"
            )
        if any(not rows for rows in by_purpose.values()):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-purpose-coverage-incomplete"
            )
        for purpose, rows in by_purpose.items():
            epochs = [row["rotationEpoch"] for row in rows]
            if len(epochs) != len(set(epochs)):
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    f"key-registry-rotation-epoch-duplicate:{purpose}"
                )
            active_epochs = [
                row["rotationEpoch"]
                for row in rows
                if not row["revoked"]
                and row["_notBefore"] <= now < row["_notAfter"]
            ]
            if len(active_epochs) != 1:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    f"key-registry-active-rotation-invalid:{purpose}"
                )
        if len(component_bindings) != 1:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-component-binding-cardinality-invalid"
            )
        binding = component_bindings[0]
        if not isinstance(binding, Mapping) or set(binding) != _COMPONENT_BINDING_KEYS:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-component-binding-contract-invalid"
            )
        if (
            binding.get("schemaVersion") != COMPONENT_BINDING_SCHEMA
            or binding.get("route") != ROUTE
            or binding.get("pdno") != PDNO
            or binding.get("component") != "readers"
            or binding.get("authorityPurpose") != "READERS_COMPONENT_VERIFY"
            or binding.get("signatureDomain")
            != "KIS_DOMESTIC_FUNCTIONAL_READERS_COMPONENT"
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-component-binding-semantic-invalid"
            )
        for field in (
            "sourceFileHash",
            "protocolHash",
            "schemaFingerprint",
            "statusHash",
            "authorityKeyIdHash",
        ):
            _sha(binding.get(field), f"key-registry-component-binding-{field}")
        active_reader_keys = [
            row["keyIdHash"]
            for row in by_purpose["READERS_COMPONENT_VERIFY"]
            if not row["revoked"]
            and row["_notBefore"] <= now < row["_notAfter"]
        ]
        if active_reader_keys != [binding["authorityKeyIdHash"]]:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-component-binding-key-not-current"
            )
        self.manifest = deepcopy(dict(manifest))
        self.manifest_hash = manifest_hash
        self.file_hash = before_hash
        self.registry_epoch = int(manifest["registryEpoch"])
        self.not_after = not_after
        self._keys = by_purpose
        self._component_bindings = {
            "readers": deepcopy(dict(binding))
        }
        if self._factory_pin_body is not None:
            if (
                manifest["registryId"] != self._factory_pin_body["registryId"]
                or before_hash != self._factory_pin_body["manifestFileHash"]
                or self.root_key_id_hash
                != self._factory_pin_body["rootKeyIdHash"]
                or self.account_fingerprint
                != self._factory_pin_body["accountFingerprint"]
                or self.credential_configuration_hash
                != self._factory_pin_body["credentialConfigurationHash"]
                or self.code_manifest_hash
                != self._factory_pin_body["codeManifestHash"]
            ):
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-production-factory-binding-mismatch"
                )
            graph_body = {
                "schemaVersion": GRAPH_BINDING_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "registryId": manifest["registryId"],
                "registryEpoch": self.registry_epoch,
                "manifestHash": self.manifest_hash,
                "rootKeyIdHash": self.root_key_id_hash,
                "accountFingerprint": self.account_fingerprint,
                "credentialConfigurationHash": self.credential_configuration_hash,
                "codeManifestHash": self.code_manifest_hash,
                "graphFileHash": self._factory_pin_body["graphFileHash"],
                "graphProtocolHash": self._factory_pin_body["graphProtocolHash"],
                "graphSchemaFingerprint": self._factory_pin_body[
                    "graphSchemaFingerprint"
                ],
            }
            self.graph_binding_hash = _hash(graph_body)
        if self.acceptance_path is not None:
            self._accept_manifest_durably(
                now=now,
                monotonic_ns=monotonic_ns,
            )

    def _accept_manifest_durably(
        self, *, now: datetime, monotonic_ns: int | None
    ) -> None:
        if monotonic_ns is None or self.clock_generation is None:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-durable-clock-lineage-missing"
            )
        self.acceptance_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.acceptance_path, timeout=5.0)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            existing = _acceptance_schema_snapshot(conn)
            if existing and existing != _EXPECTED_ACCEPTANCE_SCHEMA:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-acceptance-schema-dirty"
                )
            for statement in _ACCEPTANCE_SCHEMA_SQL:
                conn.execute(statement)
            conn.execute(
                "INSERT OR IGNORE INTO kis_key_registry_acceptance_meta "
                "VALUES(1,?,?)",
                (ACCEPTANCE_SCHEMA_VERSION, ACCEPTANCE_SCHEMA_FINGERPRINT),
            )
            if _acceptance_schema_snapshot(conn) != _EXPECTED_ACCEPTANCE_SCHEMA:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-acceptance-schema-dirty"
                )
            meta = conn.execute(
                "SELECT singleton,schema_version,schema_fingerprint "
                "FROM kis_key_registry_acceptance_meta"
            ).fetchall()
            if meta != [
                (1, ACCEPTANCE_SCHEMA_VERSION, ACCEPTANCE_SCHEMA_FINGERPRINT)
            ]:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-acceptance-meta-dirty"
                )
            conn.commit()
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT singleton,registry_id,accepted_epoch,accepted_manifest_hash,"
                "previous_manifest_hash,accepted_wall_at,accepted_monotonic_ns,"
                "clock_generation,root_key_id_hash,account_fingerprint,"
                "credential_configuration_hash,code_manifest_hash,factory_binding_hash,"
                "graph_binding_hash,revision,transition_head_hash "
                "FROM kis_key_registry_acceptance"
            ).fetchall()
            if len(rows) > 1:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-acceptance-row-cardinality-dirty"
                )
            transitions = conn.execute(
                "SELECT ordinal,registry_id,accepted_epoch,manifest_hash,"
                "previous_manifest_hash,accepted_wall_at,accepted_monotonic_ns,"
                "clock_generation,factory_binding_hash,graph_binding_hash,"
                "previous_head_hash,transition_hash "
                "FROM kis_key_registry_acceptance_transition ORDER BY ordinal"
            ).fetchall()
            previous_head = "0" * 64
            for expected_ordinal, transition in enumerate(transitions, 1):
                body = {
                    "schemaVersion": ACCEPTANCE_SCHEMA_VERSION,
                    "ordinal": transition[0],
                    "registryId": transition[1],
                    "acceptedEpoch": transition[2],
                    "manifestHash": transition[3],
                    "previousManifestHash": transition[4],
                    "acceptedWallAt": transition[5],
                    "acceptedMonotonicNs": transition[6],
                    "clockGeneration": transition[7],
                    "factoryBindingHash": transition[8],
                    "graphBindingHash": transition[9],
                    "previousHeadHash": transition[10],
                }
                if (
                    transition[0] != expected_ordinal
                    or transition[10] != previous_head
                    or transition[11] != _hash(body)
                ):
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-acceptance-transition-chain-dirty"
                    )
                previous_head = transition[11]
            previous_manifest = self.manifest["previousManifestHash"]
            accepted_at = now.isoformat().replace("+00:00", "Z")
            if not rows:
                if self.registry_epoch != 1 or previous_manifest is not None:
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-acceptance-predecessor-unproven"
                    )
                revision = 1
            else:
                row = rows[0]
                if (
                    row[0] != 1
                    or type(row[1]) is not str
                    or type(row[2]) is not int
                    or type(row[6]) is not int
                    or type(row[7]) is not str
                    or type(row[14]) is not int
                    or any(
                        not _SHA256.fullmatch(value or "")
                        for value in (
                            row[3], row[8], row[9], row[10], row[11],
                            row[12], row[13], row[15],
                        )
                    )
                    or row[15] != previous_head
                    or row[14] != len(transitions)
                ):
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-acceptance-row-dirty"
                    )
                prior_wall = _time(
                    row[5], "key-registry-acceptance-prior-wall"
                )
                if now < prior_wall or (
                    row[7] == self.clock_generation
                    and monotonic_ns < row[6]
                ):
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-acceptance-clock-rollback"
                    )
                exact_binding = (
                    row[1] == self.manifest["registryId"]
                    and row[8] == self.root_key_id_hash
                    and row[9] == self.account_fingerprint
                    and row[10] == self.credential_configuration_hash
                    and row[11] == self.code_manifest_hash
                )
                if not exact_binding:
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-acceptance-binding-mismatch"
                    )
                if self.registry_epoch == row[2]:
                    if (
                        self.manifest_hash != row[3]
                        or previous_manifest != row[4]
                        or row[12] != self.factory_binding_hash
                        or row[13] != self.graph_binding_hash
                    ):
                        raise KisDomesticFunctionalKeyRegistryBlocked(
                            "key-registry-acceptance-epoch-conflict"
                        )
                    revision = row[14] + 1
                elif (
                    self.registry_epoch != row[2] + 1
                    or previous_manifest != row[3]
                ):
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-acceptance-rollback-or-lineage-gap"
                    )
                else:
                    revision = row[14] + 1
            transition_body = {
                "schemaVersion": ACCEPTANCE_SCHEMA_VERSION,
                "ordinal": revision,
                "registryId": self.manifest["registryId"],
                "acceptedEpoch": self.registry_epoch,
                "manifestHash": self.manifest_hash,
                "previousManifestHash": previous_manifest,
                "acceptedWallAt": accepted_at,
                "acceptedMonotonicNs": monotonic_ns,
                "clockGeneration": self.clock_generation,
                "factoryBindingHash": self.factory_binding_hash,
                "graphBindingHash": self.graph_binding_hash,
                "previousHeadHash": previous_head,
            }
            transition_hash = _hash(transition_body)
            conn.execute(
                "INSERT INTO kis_key_registry_acceptance_transition "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    revision, self.manifest["registryId"], self.registry_epoch,
                    self.manifest_hash, previous_manifest, accepted_at,
                    monotonic_ns, self.clock_generation, self.factory_binding_hash,
                    self.graph_binding_hash, previous_head, transition_hash,
                ),
            )
            if rows:
                cursor = conn.execute(
                    "UPDATE kis_key_registry_acceptance SET accepted_epoch=?,"
                    "accepted_manifest_hash=?,previous_manifest_hash=?,accepted_wall_at=?,"
                    "accepted_monotonic_ns=?,clock_generation=?,revision=?,transition_head_hash=? "
                    " ,factory_binding_hash=?,graph_binding_hash=? "
                    "WHERE singleton=1 AND revision=? AND accepted_epoch=? AND accepted_manifest_hash=?",
                    (
                        self.registry_epoch, self.manifest_hash, previous_manifest,
                        accepted_at, monotonic_ns, self.clock_generation, revision,
                        transition_hash, self.factory_binding_hash,
                        self.graph_binding_hash, rows[0][14], rows[0][2], rows[0][3],
                    ),
                )
                if cursor.rowcount != 1:
                    raise KisDomesticFunctionalKeyRegistryBlocked(
                        "key-registry-acceptance-cas-lost"
                    )
            else:
                conn.execute(
                    "INSERT INTO kis_key_registry_acceptance VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        self.manifest["registryId"], self.registry_epoch,
                        self.manifest_hash, previous_manifest, accepted_at,
                        monotonic_ns, self.clock_generation, self.root_key_id_hash,
                        self.account_fingerprint, self.credential_configuration_hash,
                        self.code_manifest_hash, self.factory_binding_hash,
                        self.graph_binding_hash, revision, transition_hash,
                    ),
                )
            conn.commit()
            self.acceptance_revision = revision
            self.acceptance_head_hash = transition_hash
            self.durable_acceptance_verified = True
        except sqlite3.Error as exc:
            conn.rollback()
            raise KisDomesticFunctionalKeyRegistryBlocked(
                f"key-registry-acceptance-db-failed:{type(exc).__name__}"
            ) from None
        finally:
            conn.close()

    def verify(
        self,
        *,
        purpose: str,
        domain: str,
        body: Mapping[str, Any],
        signature: str,
        key_id_hash: str,
        observed_at: datetime | None = None,
    ) -> bool:
        if (
            purpose not in KEY_PURPOSES
            or type(domain) is not str
            or not domain
            or not isinstance(body, Mapping)
        ):
            return False
        try:
            self._trusted_pair()
            self._assert_acceptance_current()
            key_id = _sha(key_id_hash, "key-registry-verify-key-id")
            if observed_at is not None and (
                not isinstance(observed_at, datetime)
                or observed_at.tzinfo is None
            ):
                return False
            when = (
                self._last_wall
                if observed_at is None
                else observed_at.astimezone(timezone.utc)
            )
            signature_bytes = _decode_signature(
                signature, "key-registry-component-signature"
            )
            canonical_body = _canonical(body)
        except BaseException:
            return False
        try:
            for key in self._keys[purpose]:
                if (
                    key["keyIdHash"] == key_id
                    and not key["revoked"]
                    and key["_notBefore"] <= when < key["_notAfter"]
                ):
                    message = domain.encode("utf-8") + b"\0" + canonical_body
                    return _verify_ed25519(
                        key["_publicKey"], message, signature_bytes
                    )
            return False
        except BaseException:
            return False

    def _assert_acceptance_current(self) -> None:
        if self.acceptance_path is None:
            return
        try:
            conn = sqlite3.connect(
                f"file:{self.acceptance_path.as_posix()}?mode=ro", uri=True
            )
            row = conn.execute(
                "SELECT registry_id,accepted_epoch,accepted_manifest_hash,"
                "root_key_id_hash,account_fingerprint,credential_configuration_hash,"
                "code_manifest_hash,factory_binding_hash,graph_binding_hash,revision,"
                "transition_head_hash FROM kis_key_registry_acceptance WHERE singleton=1"
            ).fetchone()
            transition_rows = conn.execute(
                "SELECT ordinal,registry_id,accepted_epoch,manifest_hash,"
                "previous_manifest_hash,accepted_wall_at,accepted_monotonic_ns,"
                "clock_generation,factory_binding_hash,graph_binding_hash,"
                "previous_head_hash,transition_hash "
                "FROM kis_key_registry_acceptance_transition ORDER BY ordinal"
            ).fetchall()
        except sqlite3.Error:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-acceptance-current-read-failed"
            ) from None
        finally:
            if "conn" in locals():
                conn.close()
        if (
            row is None
            or row[0] != self.manifest["registryId"]
            or row[1] != self.registry_epoch
            or row[2] != self.manifest_hash
            or row[3] != self.root_key_id_hash
            or row[4] != self.account_fingerprint
            or row[5] != self.credential_configuration_hash
            or row[6] != self.code_manifest_hash
            or row[7] != self.factory_binding_hash
            or row[8] != self.graph_binding_hash
            or row[9] != self.acceptance_revision
            or row[10] != self.acceptance_head_hash
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-acceptance-current-binding-invalid"
            )
        previous_head = "0" * 64
        previous_epoch = 0
        previous_manifest = None
        previous_wall: datetime | None = None
        for expected_ordinal, transition in enumerate(transition_rows, 1):
            body = {
                "schemaVersion": ACCEPTANCE_SCHEMA_VERSION,
                "ordinal": transition[0],
                "registryId": transition[1],
                "acceptedEpoch": transition[2],
                "manifestHash": transition[3],
                "previousManifestHash": transition[4],
                "acceptedWallAt": transition[5],
                "acceptedMonotonicNs": transition[6],
                "clockGeneration": transition[7],
                "factoryBindingHash": transition[8],
                "graphBindingHash": transition[9],
                "previousHeadHash": transition[10],
            }
            try:
                wall = _time(
                    transition[5],
                    "key-registry-acceptance-current-transition-wall",
                )
                exact = (
                    transition[0] == expected_ordinal
                    and transition[1] == self.manifest["registryId"]
                    and type(transition[2]) is int
                    and type(transition[6]) is int
                    and transition[6] >= 0
                    and type(transition[7]) is str
                    and _IDENTIFIER.fullmatch(transition[7]) is not None
                    and all(
                        _SHA256.fullmatch(value or "") is not None
                        for value in (
                            transition[3], transition[8], transition[9],
                            transition[10], transition[11],
                        )
                    )
                    and transition[10] == previous_head
                    and transition[11] == _hash(body)
                    and (previous_wall is None or wall >= previous_wall)
                )
                if transition[2] == previous_epoch:
                    exact = exact and transition[3] == previous_manifest
                elif transition[2] == previous_epoch + 1:
                    exact = exact and (
                        (transition[2] == 1 and transition[4] is None)
                        or (
                            transition[2] > 1
                            and transition[4] == previous_manifest
                        )
                    )
                else:
                    exact = False
            except (KisDomesticFunctionalKeyRegistryBlocked, TypeError):
                exact = False
            if not exact:
                raise KisDomesticFunctionalKeyRegistryBlocked(
                    "key-registry-acceptance-current-chain-invalid"
                )
            previous_head = transition[11]
            previous_epoch = transition[2]
            previous_manifest = transition[3]
            previous_wall = wall
        if (
            len(transition_rows) != self.acceptance_revision
            or previous_head != self.acceptance_head_hash
            or previous_epoch != self.registry_epoch
            or previous_manifest != self.manifest_hash
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-acceptance-current-chain-invalid"
            )

    def verifier_for(self, purpose: str) -> VerifyCallback:
        if purpose not in KEY_PURPOSES:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-purpose-invalid"
            )

        def verifier(domain, body, signature, *, purpose=purpose):
            if not isinstance(body, Mapping):
                return False
            key_id_hash = body.get("authorityKeyIdHash")
            return self.verify(
                purpose=purpose,
                domain=domain,
                body=body,
                signature=signature,
                key_id_hash=key_id_hash,
            )

        return verifier

    def active_key_id_for(self, purpose: str) -> str:
        if purpose not in KEY_PURPOSES:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-purpose-invalid"
            )
        now, _ = self._trusted_pair()
        self._assert_acceptance_current()
        rows = [
            row
            for row in self._keys[purpose]
            if not row["revoked"] and row["_notBefore"] <= now < row["_notAfter"]
        ]
        if len(rows) != 1:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-active-key-not-exact"
            )
        return str(rows[0]["keyIdHash"])

    def component_verifier_for(self, purpose: str):
        """Return the five-argument adapter used by frozen component readers."""
        if purpose not in KEY_PURPOSES:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-purpose-invalid"
            )

        def verifier(
            _component,
            domain,
            body,
            signature,
            key_id_hash,
            *,
            purpose=purpose,
        ):
            if not isinstance(body, Mapping):
                return False
            return self.verify(
                purpose=purpose,
                domain=domain,
                body=body,
                signature=signature,
                key_id_hash=key_id_hash,
            )

        return verifier

    def component_binding(self, component: str) -> dict[str, Any]:
        """Return one current root-signed, durably accepted component pin.

        The result is verify-only.  It is reconstructed from the manifest
        bytes already verified by the pinned root and is refreshed against
        the immutable manifest file, durable accepted head, dual clock, key
        revocation set, and exact production-factory binding on every call.
        """
        if type(component) is not str or component != "readers":
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-component-binding-name-invalid"
            )
        try:
            before = self.path.read_bytes()
            after = self.path.read_bytes()
        except OSError as exc:
            raise KisDomesticFunctionalKeyRegistryBlocked(
                f"key-registry-component-binding-file-unreadable:{type(exc).__name__}"
            ) from None
        file_hash = hashlib.sha256(before).hexdigest()
        now, _ = self._trusted_pair()
        self._assert_acceptance_current()
        binding = self._component_bindings.get(component)
        active_rows = [
            row
            for row in self._keys["READERS_COMPONENT_VERIFY"]
            if not row["revoked"]
            and row["_notBefore"] <= now < row["_notAfter"]
        ]
        if (
            before != after
            or not hmac.compare_digest(file_hash, self.file_hash)
            or now >= self.not_after
            or binding is None
            or len(active_rows) != 1
            or binding["authorityKeyIdHash"] != active_rows[0]["keyIdHash"]
        ):
            raise KisDomesticFunctionalKeyRegistryBlocked(
                "key-registry-component-binding-not-current"
            )
        body = {
            "schemaVersion": (
                "kis-domestic-functional-key-registry-component-binding-result/v1"
            ),
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": self.manifest["registryId"],
            "registryEpoch": self.registry_epoch,
            "manifestHash": self.manifest_hash,
            "manifestFileHash": self.file_hash,
            "rootKeyIdHash": self.root_key_id_hash,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "codeManifestHash": self.code_manifest_hash,
            "acceptedManifestHeadHash": self.acceptance_head_hash,
            "acceptanceRevision": self.acceptance_revision,
            "factoryBindingHash": self.factory_binding_hash,
            "graphBindingHash": self.graph_binding_hash,
            "clockGeneration": self.clock_generation,
            "componentBinding": deepcopy(dict(binding)),
            "componentBindingHash": _hash(binding),
            "asymmetricRootVerified": self.asymmetric_root_verified,
            "durableAcceptanceVerified": self.durable_acceptance_verified,
            "trustedWallMonotonicLineageVerified": self.monotonic_clock is not None,
            "productionFactoryAuthorityPinned": self.production_authority_pinned,
            "verifyOnly": True,
            "productionAvailable": False,
        }
        return {**body, "bindingResultHash": _hash(body)}

    def status(self) -> dict[str, Any]:
        now, _ = self._trusted_pair()
        self._assert_acceptance_current()
        manifest_fresh = now < self.not_after
        body = {
            "schemaVersion": REGISTRY_STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": self.manifest["registryId"],
            "registryEpoch": self.registry_epoch,
            "manifestHash": self.manifest_hash,
            "manifestFileHash": self.file_hash,
            "rootKeyIdHash": self.root_key_id_hash,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "codeManifestHash": self.code_manifest_hash,
            "purposeCount": len(KEY_PURPOSES),
            "componentBindingCount": len(self._component_bindings),
            "componentBindingsHash": _hash(self._component_bindings),
            "allPurposesCovered": all(self._keys[purpose] for purpose in KEY_PURPOSES),
            "manifestFresh": manifest_fresh,
            "durableAcceptanceVerified": self.durable_acceptance_verified,
            "acceptanceSchemaFingerprint": ACCEPTANCE_SCHEMA_FINGERPRINT,
            "acceptedManifestHeadHash": self.acceptance_head_hash,
            "acceptanceRevision": self.acceptance_revision,
            "trustedWallMonotonicLineageVerified": (
                self.monotonic_clock is not None
            ),
            "productionFactoryPinsBound": self.production_factory_pins_bound,
            "productionFactoryBindingHash": self.factory_binding_hash,
            "graphRegistryBindingHash": self.graph_binding_hash,
            "graphRegistryBindingWired": False,
            "verifyOnly": True,
            "privateKeyMaterialPresent": False,
            "signingSurfacePresent": False,
            "asymmetricRootVerified": self.asymmetric_root_verified,
            # Compatibility field consumed by frozen verify-only readers: it
            # means only that the asymmetric root is independently pinned.
            "productionAuthorityPinned": self.asymmetric_root_verified,
            "productionFactoryAuthorityPinned": (
                self.production_authority_pinned
            ),
            "readinessBlockers": sorted(
                ([] if self.asymmetric_root_verified else [
                    "ASYMMETRIC_PRODUCTION_ROOT_NOT_PINNED"
                ])
                + ([] if manifest_fresh else ["KEY_REGISTRY_MANIFEST_EXPIRED"])
                + ([] if self.durable_acceptance_verified else [
                    "KEY_REGISTRY_DURABLE_ANTI_ROLLBACK_NOT_WIRED"
                ])
                + ([] if self.monotonic_clock is not None else [
                    "KEY_REGISTRY_TRUSTED_WALL_MONOTONIC_LINEAGE_NOT_WIRED"
                ])
                + ([] if self.production_factory_pins_bound else [
                    "PRODUCTION_KEY_REGISTRY_FACTORY_PINS_NOT_WIRED"
                ])
                + ["GRAPH_KEY_REGISTRY_BINDING_NOT_WIRED"]
            ),
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**body, "statusHash": _hash(body)}


def build_production_key_registry(
    manifest_path: str | Path,
    acceptance_db_path: str | Path,
    *,
    pins: ProductionKeyRegistryPins,
    trusted_wall_clock: TrustedClock,
    trusted_monotonic_clock: TrustedMonotonicClock,
    clock_generation: str,
) -> VerifyOnlyKeyRegistry:
    """Build the only production-pin-aware registry instance.

    This factory has no signing or trading surface.  It binds the immutable
    manifest bytes, asymmetric root, account/credential/release code identity,
    graph contract fields, durable anti-rollback ledger, and dual-clock
    lineage.  The graph binding remains deliberately unwired and therefore
    does not enable live trading.
    """
    if type(pins) is not ProductionKeyRegistryPins:
        raise KisDomesticFunctionalKeyRegistryBlocked(
            "key-registry-production-pins-type-invalid"
        )
    pin_body = pins.canonical_body()
    path = Path(manifest_path).expanduser().resolve()
    try:
        before = path.read_bytes()
        after = path.read_bytes()
    except OSError as exc:
        raise KisDomesticFunctionalKeyRegistryBlocked(
            f"key-registry-production-manifest-unreadable:{type(exc).__name__}"
        ) from None
    file_hash = hashlib.sha256(before).hexdigest()
    if (
        before != after
        or not hmac.compare_digest(file_hash, pin_body["manifestFileHash"])
    ):
        raise KisDomesticFunctionalKeyRegistryBlocked(
            "key-registry-production-manifest-file-pin-mismatch"
        )
    registry = VerifyOnlyKeyRegistry(
        path,
        pinned_root_public_key_pem=pins.root_public_key_pem,
        pinned_root_key_id_hash=pin_body["rootKeyIdHash"],
        expected_account_fingerprint=pin_body["accountFingerprint"],
        expected_credential_configuration_hash=pin_body[
            "credentialConfigurationHash"
        ],
        expected_code_manifest_hash=pin_body["codeManifestHash"],
        trusted_clock=trusted_wall_clock,
        trusted_monotonic_clock=trusted_monotonic_clock,
        acceptance_db_path=acceptance_db_path,
        clock_generation=clock_generation,
        _production_factory_pins=pins,
    )
    if not registry.production_authority_pinned:
        raise KisDomesticFunctionalKeyRegistryBlocked(
            "key-registry-production-authority-not-pinned"
        )
    return registry


def key_registry_component_status() -> dict[str, Any]:
    body = {
        "schemaVersion": "kis-domestic-functional-key-registry-component/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "algorithm": KEY_ALGORITHM,
        "requiredPurposes": list(KEY_PURPOSES),
        "asymmetricRuntimeAvailable": ECC is not None and eddsa is not None,
        "verifyOnly": True,
        "privateKeyMaterialPresent": False,
        "signingSurfacePresent": False,
        "durableAntiRollbackRequired": True,
        "trustedWallMonotonicLineageRequired": True,
        "productionFactoryPinsRequired": True,
        "graphRegistryBindingWired": False,
        "productionAuthorityPinned": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
    }
    return {**body, "statusHash": _hash(body)}


__all__ = [
    "KEY_ALGORITHM",
    "KEY_PURPOSES",
    "ACCEPTANCE_SCHEMA_FINGERPRINT",
    "ACCEPTANCE_SCHEMA_VERSION",
    "COMPONENT_BINDING_SCHEMA",
    "FACTORY_BINDING_SCHEMA",
    "GRAPH_BINDING_SCHEMA",
    "KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY_RELEASE_AVAILABLE",
    "KisDomesticFunctionalKeyRegistryBlocked",
    "ProductionKeyRegistryPins",
    "REGISTRY_SCHEMA",
    "REGISTRY_STATUS_SCHEMA",
    "VerifyOnlyKeyRegistry",
    "build_production_key_registry",
    "key_registry_component_status",
]
