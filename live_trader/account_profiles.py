"""Public saved-account identities. Never reads balances or opens a broker socket."""
from __future__ import annotations
import os
import re
from typing import Any, Mapping
from trading_runtime.artifact_governance import stable_sha256

_SPECS = {
    "kis": ("KIS 실계좌", ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE"), "KIS_BASE_URL", "https://openapi.koreainvestment.com:9443", "KRW"),
    "binance": ("Binance Spot", ("BINANCE_API_KEY", "BINANCE_API_SECRET"), "BINANCE_BASE_URL", "https://api.binance.com", "USDT"),
    "upbit": ("Upbit 현물", ("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY"), "UPBIT_BASE_URL", "https://api.upbit.com", "KRW"),
}


def saved_account_profiles(settings: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    settings = os.environ if settings is None else settings
    profiles = []
    for broker, (label, keys, endpoint_key, official_origin, currency) in _SPECS.items():
        values = {key: str(settings.get(key) or "").strip() for key in keys}
        if broker == "kis":
            raw = values["KIS_ACCOUNT_NO"]
            parts = raw.split("-")
            cano = parts[0]
            product = values["KIS_ACCOUNT_PRODUCT_CODE"] or (parts[1] if len(parts) == 2 else "01")
            values["KIS_ACCOUNT_NO"] = cano
            values["KIS_ACCOUNT_PRODUCT_CODE"] = product
            if not re.fullmatch(r"[0-9]{8}", cano) or not re.fullmatch(r"[0-9]{2}", product):
                continue
            identity = {"cano": cano, "product": product}
            suffix = cano[-4:]
        else:
            identity = {"apiKey": values[keys[0]]}
            suffix = ""
        if not all(values.values()):
            continue
        origin = str(settings.get(endpoint_key) or official_origin).strip()
        if origin != official_origin:
            continue
        fingerprint = stable_sha256({"broker": broker, "identity": identity})
        configuration_hash = stable_sha256({"broker": broker, "values": values, "origin": origin})
        profile_id = stable_sha256({"broker": broker, "accountFingerprint": fingerprint, "credentialConfigurationHash": configuration_hash})
        profiles.append({
            "profileId": profile_id, "brokerId": broker,
            "alias": label, "maskedAccount": "••••" + (suffix or fingerprint[-4:].upper()),
            "accountFingerprint": fingerprint, "credentialConfigurationHash": configuration_hash,
            "accountBindingId": "live-account:" + broker + ":" + fingerprint,
            "currency": currency, "balanceQueried": False,
        })
    return profiles


def public_saved_accounts(settings: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"ok": True, "schemaVersion": "live-saved-account-profiles-v1",
            "accounts": saved_account_profiles(settings), "readOnly": True,
            "brokerCalls": 0, "ordersSubmitted": 0,
            "capitalBasis": "ACCOUNT_AVAILABLE_CASH", "capitalPercent": 100}
