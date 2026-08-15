from __future__ import annotations

"""Provision/probe an external crypto first-live WORM namespace.

This command never talks to a broker.  The default ``--check-config-only``
mode is network-zero; remove that flag only after the independently managed
authority endpoint and immutable-retention namespace have been created.
"""

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from live_trader.crypto_first_live_external_worm import (  # noqa: E402
    PinnedExternalWormAuthorityClient,
    provisioning_request,
)
from live_trader.crypto_first_live_high_water import (  # noqa: E402
    DurableCryptoFirstLiveHighWaterAnchor,
    ExternallyAnchoredCryptoFirstLiveHighWaterAuthority,
)


CONFIG_FIELDS = {
    "schemaVersion",
    "endpointUrl",
    "namespaceId",
    "authorityId",
    "keyId",
    "publicKeyPath",
    "tlsCertificateSha256",
}


def _load_config(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != CONFIG_FIELDS:
        raise ValueError("external-worm-provision-config-fields-not-exact")
    if value.get("schemaVersion") != "crypto-first-live-worm-provision/v1":
        raise ValueError("external-worm-provision-config-schema-invalid")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate or provision the independently administered monotonic "
            "WORM checkpoint required by crypto first-live."
        )
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--high-water-db", type=Path, required=True)
    parser.add_argument(
        "--check-config-only",
        action="store_true",
        help="Validate pins and the revision-zero prefix without network.",
    )
    args = parser.parse_args()
    config_path = args.config.resolve(strict=True)
    high_water_path = args.high_water_db.resolve(strict=True)
    config = _load_config(config_path)
    public_key_path = Path(str(config["publicKeyPath"])).resolve(strict=True)
    client = PinnedExternalWormAuthorityClient(
        endpoint_url=str(config["endpointUrl"]),
        namespace_id=str(config["namespaceId"]),
        authority_id=str(config["authorityId"]),
        key_id=str(config["keyId"]),
        public_key=public_key_path.read_bytes(),
        tls_certificate_sha256=str(config["tlsCertificateSha256"]),
    )
    anchor = DurableCryptoFirstLiveHighWaterAnchor(high_water_path)
    request = provisioning_request(anchor.external_checkpoint_descriptor())
    if args.check_config_only:
        print(json.dumps({
            "ok": True,
            "networkRequestCount": 0,
            "readyToProvision": True,
            "namespaceId": client.namespace_id,
            "authorityId": client.authority_id,
            "databaseId": request["databaseId"],
            "revision": request["revision"],
        }, sort_keys=True))
        return 0
    receipt = client(request)
    verified = (
        ExternallyAnchoredCryptoFirstLiveHighWaterAuthority
        .validate_external_receipt(receipt, request=request)
    )
    if (
        int(verified["revision"]) != 0
        or verified["publicationHash"] != ""
    ):
        raise RuntimeError("external-worm-provision-prefix-mismatch")
    print(json.dumps({
        "ok": True,
        "networkRequestCount": 1,
        "provisioned": True,
        "namespaceId": client.namespace_id,
        "authorityId": client.authority_id,
        "databaseId": verified["databaseId"],
        "revision": verified["revision"],
        "checkpointId": verified["checkpointId"],
        "receiptHash": verified["receiptHash"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
