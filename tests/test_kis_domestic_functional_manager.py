from __future__ import annotations

from contextlib import contextmanager
import hashlib
import hmac
import json
import threading
import time
import unittest

from live_trader.kis_domestic_functional_manager import (
    DisabledKisDomesticFunctionalManager,
    KIS_DOMESTIC_FUNCTIONAL_MANAGER_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MANAGER_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MANAGER_RELEASE_AVAILABLE,
    KisDomesticFunctionalManagerBlocked,
    OfflinePinnedKisManagerAdapter,
    OfflinePinnedKisMutationAdapter,
    OfflinePinnedKisStateAdapter,
    OfflinePinnedKisTransportAdapter,
    production_entrypoint_status,
)


ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
OWNER_EPOCH = "c" * 64
MANAGER_KEY = b"manager-offline-test-key-material-32-bytes-minimum"
TRANSPORT_KEY = b"transport-offline-test-key-material-32-bytes-minimum"
TRANSPORT_KEY_ID_HASH = hashlib.sha256(b"transport-test-key-v1").hexdigest()
PLAN_KEY = b"mutation-plan-offline-test-key-material-32-bytes-minimum"
PLAN_KEY_ID_HASH = hashlib.sha256(b"mutation-plan-test-key-v1").hexdigest()


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class KisDomesticFunctionalManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = "kis-manager-session-one"
        self.revision = 2
        self.owner_epoch_id = "kis-owner-epoch-one"
        self.boundary_active = False
        self.boundary_entries = 0
        self.sent = []
        self.sender_entered = threading.Event()
        self.sender_release = threading.Event()
        self.sender_release.set()
        self.sender_mode = "ACKNOWLEDGED"
        self.component_readers_hash = "0" * 64
        self.current_plan = {}
        self.statuses = {
            name: self._status(name) for name in (
                "state",
                "owner",
                "capability",
                "quote",
                "rolling",
                "heartbeat",
                "mutation",
                "transport",
            )
        }
        provisional = self._reservation("START")
        self.reservation = self._bind_plan(
            provisional,
            "START",
            [self._request("NATURAL_BUY", "START")],
        )
        self.manager = self._manager()

    @staticmethod
    def _code(name: str) -> str:
        return hashlib.sha256(("code:" + name).encode()).hexdigest()

    @staticmethod
    def _protocol(name: str) -> str:
        return hashlib.sha256(("protocol:" + name).encode()).hexdigest()

    def _status(self, name: str) -> dict:
        value = {
            "schemaVersion": "kis-domestic-functional-manager-component-status/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "component": name,
            "implementationType": f"offline-{name}-adapter-v1",
            "codeHash": self._code(name),
            "protocolHash": self._protocol(name),
            "statusRevision": 1,
            "statusHeadHash": hashlib.sha256(("head:" + name).encode()).hexdigest(),
            "stateRevision": self.revision,
            "sessionId": self.session,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "ownerEpochId": self.owner_epoch_id,
            "ownerEpochHash": OWNER_EPOCH,
            "hazards": [],
            "readable": True,
            "ready": True,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseEvidenceAvailable": False,
        }
        if name == "mutation":
            value.update(
                {
                    "authoritativeMutationPlanHash": "0" * 64,
                    "ownedProjectionHash": "0" * 64,
                    "ownedProjectionHeadHash": "0" * 64,
                    "ownedProjectionRevision": 1,
                    "ownedProjectionObservedAt": "2026-08-14T04:15:00+00:00",
                    "mutationPlanKeyIdHash": PLAN_KEY_ID_HASH,
                }
            )
        return value

    def _reservation(self, command: str) -> dict:
        return {
            "reservationId": f"kis-state-{command.lower()}-reservation-one",
            "reservationKind": command,
            "revision": self.revision,
            "sessionId": self.session,
            "reservedAt": "2026-08-14T04:15:00+00:00",
            "previousAccountFingerprint": ACCOUNT,
            "previousCredentialConfigurationHash": CREDENTIAL,
            "reservedAccountFingerprint": ACCOUNT,
            "reservedCredentialConfigurationHash": CREDENTIAL,
            "ownerEpochId": self.owner_epoch_id,
            "ownerEpochHash": OWNER_EPOCH,
            "componentReadersHash": self.component_readers_hash,
            "finalMutationBoundaryRequired": True,
            "finalMutationBoundaryHandleSchema": "kis-domestic-functional-final-reservation/v1",
            "finalMutationBoundaryHandle": "d" * 64,
            "productionAvailable": False,
        }

    @staticmethod
    def _verify_plan(value) -> bool:
        try:
            raw = dict(value)
            signature = raw.pop("signature")
            plan_hash = raw.pop("planHash")
            return bool(
                hmac.compare_digest(plan_hash, digest(raw))
                and hmac.compare_digest(
                    signature,
                    hmac.new(PLAN_KEY, plan_hash.encode(), hashlib.sha256).hexdigest(),
                )
            )
        except Exception:
            return False

    def _bind_plan(self, reservation: dict, command: str, requests: list[dict]) -> dict:
        order_keys = sorted(
            [
                dict(item["ownedOrderKey"])
                for item in requests
                if item["operation"] == "CLEANUP_CANCEL"
            ],
            key=canonical,
        )
        sells = [
            item for item in requests if item["operation"] == "CLEANUP_SELL"
        ]
        position = (
            dict(sells[0]["ownedPosition"])
            if sells
            else {
                "pdno": "",
                "baselineAccountQuantity": "",
                "currentAccountQuantity": "",
                "ownedDeltaQuantity": "",
                "sourceClaimId": "",
                "positionProofHash": "",
            }
        )
        delta = (
            position["ownedDeltaQuantity"]
            if sells else "0"
        )
        projection_body = {
            "schemaVersion": "kis-domestic-functional-manager-owned-projection/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": reservation["sessionId"],
            "stateRevision": reservation["revision"],
            "ownerEpochId": reservation["ownerEpochId"],
            "ownerEpochHash": reservation["ownerEpochHash"],
            "accountFingerprint": reservation["reservedAccountFingerprint"],
            "credentialConfigurationHash": reservation[
                "reservedCredentialConfigurationHash"
            ],
            "observedAt": reservation["reservedAt"],
            "revision": 1,
            "headHash": hashlib.sha256(
                ("owned-head:" + command + ":" + digest(requests)).encode()
            ).hexdigest(),
            "ownedWorkingOrders": order_keys,
            "ownedPosition": position,
            "ownedWorkingOrderCount": len(order_keys),
            "ownedPositionDelta": delta,
            "ownedWorking0": not order_keys,
            "ownedDelta0": delta == "0",
            "productionAvailable": False,
        }
        projection = {
            **projection_body,
            "projectionHash": digest(projection_body),
        }
        plan_body = {
            "schemaVersion": "kis-domestic-functional-manager-authority-plan/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "planId": f"kis-authoritative-{command.lower()}-plan-one",
            "planRevision": 1,
            "reservationId": reservation["reservationId"],
            "reservationKind": command,
            "reservationRevision": reservation["revision"],
            "sessionId": reservation["sessionId"],
            "stateRevision": reservation["revision"],
            "ownerEpochId": reservation["ownerEpochId"],
            "ownerEpochHash": reservation["ownerEpochHash"],
            "accountFingerprint": reservation["reservedAccountFingerprint"],
            "credentialConfigurationHash": reservation[
                "reservedCredentialConfigurationHash"
            ],
            "ownedProjection": projection,
            "ownedProjectionHash": projection["projectionHash"],
            "mutationRequests": [dict(item) for item in requests],
            "requestCount": len(requests),
            "keyIdHash": PLAN_KEY_ID_HASH,
            "productionAvailable": False,
        }
        plan_hash = digest(plan_body)
        self.current_plan = {
            **plan_body,
            "planHash": plan_hash,
            "signature": hmac.new(
                PLAN_KEY, plan_hash.encode(), hashlib.sha256
            ).hexdigest(),
        }
        self.statuses["mutation"].update(
            {
                "authoritativeMutationPlanHash": plan_hash,
                "ownedProjectionHash": projection["projectionHash"],
                "ownedProjectionHeadHash": projection["headHash"],
                "ownedProjectionRevision": projection["revision"],
                "ownedProjectionObservedAt": projection["observedAt"],
                "mutationPlanKeyIdHash": PLAN_KEY_ID_HASH,
            }
        )
        self.component_readers_hash = digest(self.statuses)
        reservation["componentReadersHash"] = self.component_readers_hash
        return reservation

    @contextmanager
    def _boundary(self, reservation):
        self.boundary_entries += 1
        self.boundary_active = True
        try:
            yield {
                "schemaVersion": "kis-domestic-functional-final-reservation/v1",
                "route": ROUTE,
                "reservationId": reservation["reservationId"],
                "reservationKind": reservation["reservationKind"],
                "reservationRevision": reservation["revision"],
                "sessionId": reservation["sessionId"],
                "accountFingerprint": reservation["reservedAccountFingerprint"],
                "credentialConfigurationHash": reservation[
                    "reservedCredentialConfigurationHash"
                ],
                "ownerEpochHash": reservation["ownerEpochHash"],
                "componentReadersHash": reservation["componentReadersHash"],
                "productionAvailable": False,
                "finalMutationBoundaryHandle": reservation[
                    "finalMutationBoundaryHandle"
                ],
                "routeLockHeld": True,
            }
        finally:
            self.boundary_active = False

    def _transport_receipt(self, request, attempt):
        self.assertTrue(self.boundary_active)
        self.assertFalse(self.manager._control_lock._is_owned())
        self.sent.append((dict(request), dict(attempt)))
        self.sender_entered.set()
        self.sender_release.wait(2)
        body = {
            "schemaVersion": "kis-domestic-functional-mock-transport-receipt/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "operation": request["operation"],
            "claimId": request["claimId"],
            "requestHash": request["requestHash"],
            "attemptProofHash": attempt["attemptProofHash"],
            "status": self.sender_mode,
            "mutationMayHaveOccurred": self.sender_mode != "NOT_SENT",
            "occurredAt": "2026-08-14T04:15:01+00:00",
            "signerKeyIdHash": TRANSPORT_KEY_ID_HASH,
            "productionAvailable": False,
            "networkAvailable": False,
        }
        receipt_hash = digest(body)
        return {
            **body,
            "receiptHash": receipt_hash,
            "signature": hmac.new(
                TRANSPORT_KEY, receipt_hash.encode(), hashlib.sha256
            ).hexdigest(),
        }

    @staticmethod
    def _verify_transport(value) -> bool:
        raw = dict(value)
        signature = raw.pop("signature", "")
        receipt_hash = raw.pop("receiptHash", "")
        return bool(
            hmac.compare_digest(receipt_hash, digest(raw))
            and hmac.compare_digest(
                signature,
                hmac.new(TRANSPORT_KEY, receipt_hash.encode(), hashlib.sha256).hexdigest(),
            )
        )

    def _manager(self, *, timeout=0.25, sender=None, monotonic_clock=time.monotonic):
        adapters = {}
        for name in self.statuses:
            reader = lambda name=name: dict(self.statuses[name])
            if name == "state":
                adapters[name] = OfflinePinnedKisStateAdapter(
                    implementation_type=f"offline-{name}-adapter-v1",
                    code_hash=self._code(name),
                    protocol_hash=self._protocol(name),
                    status_reader=reader,
                    final_boundary_factory=self._boundary,
                    allow_mock=True,
                )
            elif name == "transport":
                adapters[name] = OfflinePinnedKisTransportAdapter(
                    implementation_type=f"offline-{name}-adapter-v1",
                    code_hash=self._code(name),
                    protocol_hash=self._protocol(name),
                    status_reader=reader,
                    mutation_sender=sender or self._transport_receipt,
                    receipt_verifier=self._verify_transport,
                    allow_mock=True,
                )
            elif name == "mutation":
                adapters[name] = OfflinePinnedKisMutationAdapter(
                    implementation_type=f"offline-{name}-adapter-v1",
                    code_hash=self._code(name),
                    protocol_hash=self._protocol(name),
                    status_reader=reader,
                    plan_reader=lambda _reservation, _command: dict(
                        self.current_plan
                    ),
                    plan_verifier=self._verify_plan,
                    allow_mock=True,
                )
            else:
                adapters[name] = OfflinePinnedKisManagerAdapter(
                    component=name,
                    implementation_type=f"offline-{name}-adapter-v1",
                    code_hash=self._code(name),
                    protocol_hash=self._protocol(name),
                    status_reader=reader,
                    allow_mock=True,
                )
        return DisabledKisDomesticFunctionalManager(
            adapters=adapters,
            manager_id="offline-kis-manager-test-v1",
            signer_key=MANAGER_KEY,
            signer_key_id="offline-kis-manager-key-v1",
            timeout_seconds=timeout,
            wall_clock=lambda: 1_786_680_902.0,
            monotonic_clock=monotonic_clock,
            allow_offline_signer=True,
        )

    def _request(
        self,
        operation: str,
        command: str,
        *,
        claim=None,
        baseline_account_quantity: str = "0",
        current_account_quantity: str | None = None,
    ) -> dict:
        empty_order = {"orderDate": "", "organizationNo": "", "orderNo": ""}
        empty_position = {
            "pdno": "",
            "baselineAccountQuantity": "",
            "currentAccountQuantity": "",
            "ownedDeltaQuantity": "",
            "sourceClaimId": "",
            "positionProofHash": "",
        }
        if operation == "CLEANUP_CANCEL":
            endpoint = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
            owned_order = {
                "orderDate": "20260814",
                "organizationNo": "00123",
                "orderNo": "0000012345",
            }
            owned_position = empty_position
            payload = {
                "KRX_FWDG_ORD_ORGNO": owned_order["organizationNo"],
                "ORGN_ODNO": owned_order["orderNo"],
                "ORD_DVSN": "00",
                "RVSE_CNCL_DVSN_CD": "02",
                "ORD_QTY": "1",
                "ORD_UNPR": "0",
                "QTY_ALL_ORD_YN": "Y",
                "EXCG_ID_DVSN_CD": "KRX",
            }
        else:
            endpoint = "/uapi/domestic-stock/v1/trading/order-cash"
            owned_order = empty_order
            payload = {
                "PDNO": PDNO,
                "ORD_DVSN": "00",
                "ORD_QTY": "1",
                "ORD_UNPR": "80000",
            }
            owned_position = (
                {
                    "pdno": PDNO,
                    "baselineAccountQuantity": baseline_account_quantity,
                    "currentAccountQuantity": (
                        current_account_quantity
                        if current_account_quantity is not None
                        else str(int(baseline_account_quantity) + 1)
                    ),
                    "ownedDeltaQuantity": "1",
                    "sourceClaimId": "claim-natural-buy-one",
                    "positionProofHash": "e" * 64,
                }
                if operation == "CLEANUP_SELL"
                else empty_position
            )
        body = {
            "schemaVersion": "kis-domestic-functional-manager-mutation-request/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "command": command,
            "operation": operation,
            "claimId": claim or f"claim-{operation.lower().replace('_', '-')}-one",
            "sessionId": self.session,
            "stateRevision": self.revision,
            "ownerEpochId": self.owner_epoch_id,
            "ownerEpochHash": OWNER_EPOCH,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "endpoint": endpoint,
            "payload": payload,
            "payloadHash": digest(payload),
            "ownedOrderKey": owned_order,
            "ownedPosition": owned_position,
            "productionAvailable": False,
        }
        return {**body, "requestHash": digest(body)}

    def _set_command(self, command: str) -> dict:
        defaults = {
            "START": [self._request("NATURAL_BUY", "START")],
            "STOP": [self._request("CLEANUP_CANCEL", "STOP")],
            "KILL": [self._request("CLEANUP_SELL", "KILL")],
            "SETTINGS": [],
        }
        self.reservation = self._bind_plan(
            self._reservation(command), command, defaults[command]
        )
        return self.reservation

    def test_start_happy_path_enters_boundary_once_and_returns_full_proofs(self) -> None:
        result = self.manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        receipt = result["receipt"]
        self.assertTrue(receipt["ok"])
        self.assertEqual(1, receipt["attemptCount"])
        self.assertEqual(1, self.boundary_entries)
        self.assertEqual(1, len(self.sent))
        self.assertEqual(
            result["boundaryEntryProof"]["boundaryEntryProofHash"],
            result["attemptProofs"][0]["boundaryEntryProofHash"],
        )
        self.assertEqual(
            result["attemptProofs"][0]["attemptProofHash"],
            result["transportReceipts"][0]["attemptProofHash"],
        )
        state_receipt = result["stateManagerReceipt"]
        self.assertEqual(
            "kis-domestic-functional-manager-receipt/v2",
            state_receipt["schemaVersion"],
        )
        self.assertTrue(self.manager.verify_state_manager_receipt(state_receipt))
        self.assertEqual(
            self.reservation["componentReadersHash"],
            state_receipt["componentReadersHash"],
        )
        self.assertEqual(
            receipt["receiptHash"], state_receipt["managerReceiptHash"]
        )
        self.assertEqual(
            receipt["executionProofHash"], state_receipt["executionProofHash"]
        )
        self.assertEqual(
            receipt["transportReceiptSetHash"],
            state_receipt["transportReceiptSetHash"],
        )
        self.assertTrue(state_receipt["reservationFinishAllowed"])
        self.assertFalse(state_receipt["pendingReservation"])

    def test_start_requires_all_pinned_components_ready_and_exact_join(self) -> None:
        self.statuses["quote"]["ready"] = False
        self.reservation["componentReadersHash"] = digest(self.statuses)
        result = self.manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["reconciliationRequired"])
        self.assertEqual(0, len(self.sent))

        self.statuses["quote"]["ready"] = True
        self.statuses["heartbeat"]["ownerEpochHash"] = "f" * 64
        self.reservation["componentReadersHash"] = digest(self.statuses)
        result = self.manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertEqual(0, len(self.sent))

    def test_component_reader_hash_is_bound_by_state_reservation(self) -> None:
        self.statuses["rolling"]["statusRevision"] = 2
        result = self.manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertEqual("BOUNDARY_OR_COMPONENT_FAILURE", result["receipt"]["failureCode"])
        self.assertEqual(0, self.boundary_entries)

    def test_stop_cleanup_cancel_requires_exact_owned_tuple_and_payload(self) -> None:
        reservation = self._set_command("STOP")
        request = self._request("CLEANUP_CANCEL", "STOP")
        result = self.manager.execute(
            reservation=reservation,
            command="STOP",
            mutation_requests=[request],
        )
        self.assertTrue(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["cleanupExactOwned"])
        self.assertEqual("0000012345", self.sent[0][0]["ownedOrderKey"]["orderNo"])

        changed = self._request("CLEANUP_CANCEL", "STOP")
        changed["ownedOrderKey"] = {**changed["ownedOrderKey"], "orderNo": "999999"}
        changed_body = dict(changed); changed_body.pop("requestHash")
        changed["requestHash"] = digest(changed_body)
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "tuple/payload"):
            self.manager.execute(
                reservation=reservation,
                command="STOP",
                mutation_requests=[changed],
            )

    def test_kill_cleanup_sell_requires_exact_owned_qty_one_position(self) -> None:
        request = self._request(
            "CLEANUP_SELL",
            "KILL",
            baseline_account_quantity="5",
            current_account_quantity="6",
        )
        reservation = self._bind_plan(
            self._reservation("KILL"), "KILL", [request]
        )
        result = self.manager.execute(
            reservation=reservation,
            command="KILL",
            mutation_requests=[request],
        )
        self.assertTrue(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["cleanupExactOwned"])
        self.assertEqual("1", self.sent[0][0]["payload"]["ORD_QTY"])
        self.assertEqual(
            "5", self.sent[0][0]["ownedPosition"]["baselineAccountQuantity"]
        )
        self.assertEqual(
            "6", self.sent[0][0]["ownedPosition"]["currentAccountQuantity"]
        )

        changed = self._request("CLEANUP_SELL", "KILL")
        changed["ownedPosition"] = {
            **changed["ownedPosition"],
            "ownedDeltaQuantity": "2",
        }
        changed_body = dict(changed); changed_body.pop("requestHash")
        changed["requestHash"] = digest(changed_body)
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "exact-owned"):
            self.manager.execute(
                reservation=reservation,
                command="KILL",
                mutation_requests=[changed],
            )

        drift = self._request(
            "CLEANUP_SELL",
            "KILL",
            baseline_account_quantity="5",
            current_account_quantity="7",
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalManagerBlocked, "exact-owned"
        ):
            self.manager.execute(
                reservation=reservation,
                command="KILL",
                mutation_requests=[drift],
            )
        total_sell = self._request(
            "CLEANUP_SELL",
            "KILL",
            baseline_account_quantity="5",
            current_account_quantity="6",
        )
        total_sell["payload"] = {**total_sell["payload"], "ORD_QTY": "6"}
        total_sell["payloadHash"] = digest(total_sell["payload"])
        total_unsigned = dict(total_sell)
        total_unsigned.pop("requestHash")
        total_sell["requestHash"] = digest(total_unsigned)
        with self.assertRaisesRegex(
            KisDomesticFunctionalManagerBlocked, "order mutation contract changed"
        ):
            self.manager.execute(
                reservation=reservation,
                command="KILL",
                mutation_requests=[total_sell],
            )

    def test_stop_and_kill_reject_entry_or_arbitrary_cleanup(self) -> None:
        reservation = self._set_command("STOP")
        request = self._request("NATURAL_BUY", "STOP")
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "exact-owned"):
            self.manager.execute(
                reservation=reservation,
                command="STOP",
                mutation_requests=[request],
            )
        self.assertEqual(0, self.boundary_entries)
        self.assertEqual(0, len(self.sent))

    def test_settings_posts_zero_but_revalidates_state_boundary(self) -> None:
        reservation = self._set_command("SETTINGS")
        result = self.manager.execute(
            reservation=reservation,
            command="SETTINGS",
            mutation_requests=[],
        )
        self.assertTrue(result["receipt"]["ok"])
        self.assertEqual(1, self.boundary_entries)
        self.assertEqual(0, result["receipt"]["attemptCount"])
        self.assertEqual(0, len(self.sent))

    def test_transport_receipt_tamper_is_reconciliation_not_success(self) -> None:
        def tampered(request, attempt):
            value = self._transport_receipt(request, attempt)
            return {**value, "requestHash": "f" * 64}

        manager = self._manager(sender=tampered)
        result = manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["mutationMayHaveOccurred"])
        self.assertTrue(result["receipt"]["reconciliationRequired"])

        state_receipt = dict(result["stateManagerReceipt"])
        state_receipt["ok"] = True
        self.assertFalse(manager.verify_state_manager_receipt(state_receipt))

    def test_signed_transport_receipt_outside_boundary_is_reconciliation(self) -> None:
        def future_receipt(request, attempt):
            value = self._transport_receipt(request, attempt)
            body = dict(value)
            body.pop("receiptHash")
            body.pop("signature")
            body["occurredAt"] = "2026-08-14T04:15:03+00:00"
            receipt_hash = digest(body)
            return {
                **body,
                "receiptHash": receipt_hash,
                "signature": hmac.new(
                    TRANSPORT_KEY,
                    receipt_hash.encode(),
                    hashlib.sha256,
                ).hexdigest(),
            }

        manager = self._manager(sender=future_receipt)
        result = manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["mutationMayHaveOccurred"])
        self.assertTrue(result["receipt"]["reconciliationRequired"])

    def test_invalid_final_monotonic_sample_is_sticky_reconciliation(self) -> None:
        samples = iter((10.0, float("nan")))
        manager = self._manager(monotonic_clock=lambda: next(samples))
        result = manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["detachedMutationHazard"])
        self.assertTrue(result["receipt"]["reconciliationRequired"])
        self.assertEqual(0.0, result["receipt"]["elapsedMonotonicSeconds"])
        self.assertTrue(
            manager.verify_state_manager_receipt(result["stateManagerReceipt"])
        )

    def test_boundary_failure_posts_zero_and_returns_reconciliation(self) -> None:
        @contextmanager
        def failed_boundary(_reservation):
            raise RuntimeError("injected boundary failure")
            yield  # pragma: no cover

        manager = self._manager()
        object.__setattr__(manager.adapters["state"], "_final_boundary_factory", failed_boundary)
        result = manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertFalse(result["receipt"]["mutationMayHaveOccurred"])
        self.assertEqual(0, result["receipt"]["attemptCount"])
        self.assertEqual(0, len(self.sent))

    def test_timeout_is_sticky_detached_reconciliation_and_never_early_success(self) -> None:
        self.sender_release.clear()
        manager = self._manager(timeout=0.02)
        result = manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        self.assertTrue(self.sender_entered.wait(1))
        self.assertFalse(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["detachedMutationHazard"])
        self.assertTrue(result["receipt"]["detachedBoundaryHazard"])
        self.assertTrue(result["receipt"]["mutationMayHaveOccurred"])
        self.assertFalse(result["receipt"]["operationDeadlineComplete"])
        self.assertTrue(result["receipt"]["pendingReservation"])
        self.assertFalse(result["receipt"]["reservationFinishAllowed"])
        self.assertIsNone(result["stateManagerReceipt"])
        self.assertTrue(
            manager.verify_pending_reservation_proof(
                result["pendingReservationProof"]
            )
        )
        self.assertTrue(manager.status()["hazardousAuthorityOpen"])
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "running|detached"):
            manager.execute(
                reservation=self.reservation,
                command="START",
                mutation_requests=[self._request("NATURAL_BUY", "START")],
            )
        self.sender_release.set()
        deadline = time.monotonic() + 1
        while self.boundary_active and time.monotonic() < deadline:
            time.sleep(0.005)

    def test_concurrent_second_manager_operation_is_blocked_without_second_boundary(self) -> None:
        self.sender_release.clear()
        result_holder = {}

        def first():
            result_holder["value"] = self.manager.execute(
                reservation=self.reservation,
                command="START",
                mutation_requests=[self._request("NATURAL_BUY", "START")],
            )

        thread = threading.Thread(target=first)
        thread.start()
        self.assertTrue(self.sender_entered.wait(1))
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "already running"):
            self.manager.execute(
                reservation=self.reservation,
                command="START",
                mutation_requests=[self._request("NATURAL_BUY", "START")],
            )
        self.assertEqual(1, self.boundary_entries)
        self.sender_release.set()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertTrue(result_holder["value"]["receipt"]["ok"])

    def test_exact_adapter_types_flags_and_no_sender_surface(self) -> None:
        status = self.manager.status()
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MANAGER_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MANAGER_NETWORK_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MANAGER_RELEASE_AVAILABLE)
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["sharedStateWired"])
        self.assertTrue(status["authoritativeMutationPlanRequired"])
        self.assertTrue(status["ownedProjectionHeadRequired"])
        self.assertTrue(status["zeroCleanupRequiresSignedOwnedZero"])
        self.assertFalse(status["duplicateCleanupOperationAllowed"])
        self.assertFalse(status["detachedBoundaryCanFinishReservation"])
        self.assertFalse(status["stateReceiptV2IntegrationWired"])
        self.assertFalse(production_entrypoint_status()["available"])
        self.assertFalse(hasattr(self.manager, "send"))
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "explicit offline"):
            OfflinePinnedKisManagerAdapter(
                component="quote",
                implementation_type="offline-quote-adapter-v1",
                code_hash=self._code("quote"),
                protocol_hash=self._protocol("quote"),
                status_reader=lambda: {},
                allow_mock=False,
            )

    def test_command_request_hash_and_duplicate_claims_fail_before_boundary(self) -> None:
        request = self._request("NATURAL_BUY", "START")
        changed = {**request, "payloadHash": "f" * 64}
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "payload hash"):
            self.manager.execute(
                reservation=self.reservation,
                command="START",
                mutation_requests=[changed],
            )
        with self.assertRaisesRegex(KisDomesticFunctionalManagerBlocked, "cardinality"):
            reservation = self._set_command("STOP")
            duplicate = self._request("CLEANUP_CANCEL", "STOP", claim="same-claim")
            self.manager.execute(
                reservation=reservation,
                command="STOP",
                mutation_requests=[duplicate, duplicate],
            )
        self.assertEqual(0, self.boundary_entries)

    def test_authoritative_plan_rejects_caller_substitution_and_duplicate_operation(self) -> None:
        substituted = self._request("NATURAL_BUY", "START", claim="other-claim")
        result = self.manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[substituted],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertEqual("BOUNDARY_OR_COMPONENT_FAILURE", result["receipt"]["failureCode"])
        self.assertEqual(0, result["receipt"]["attemptCount"])
        reservation = self._reservation("STOP")
        first = self._request("CLEANUP_CANCEL", "STOP", claim="cancel-one")
        second = self._request("CLEANUP_CANCEL", "STOP", claim="cancel-two")
        self._bind_plan(reservation, "STOP", [first])
        with self.assertRaisesRegex(
            KisDomesticFunctionalManagerBlocked,
            "operation uniqueness",
        ):
            self.manager.execute(
                reservation=reservation,
                command="STOP",
                mutation_requests=[first, second],
            )
        self.assertEqual(0, self.boundary_entries)

    def test_zero_cleanup_requires_same_snapshot_signed_owned_zero(self) -> None:
        reservation = self._bind_plan(self._reservation("KILL"), "KILL", [])
        result = self.manager.execute(
            reservation=reservation,
            command="KILL",
            mutation_requests=[],
        )
        self.assertTrue(result["receipt"]["ok"])
        self.assertTrue(result["receipt"]["cleanupExactOwned"])
        self.assertEqual(0, result["receipt"]["attemptCount"])
        self.assertEqual(0, len(self.sent))

        sell = self._request("CLEANUP_SELL", "KILL")
        reservation = self._bind_plan(
            self._reservation("KILL"), "KILL", [sell]
        )
        plan = dict(self.current_plan)
        projection = dict(plan["ownedProjection"])
        projection["ownedDelta0"] = True
        unsigned_projection = dict(projection)
        unsigned_projection.pop("projectionHash")
        projection["projectionHash"] = digest(unsigned_projection)
        plan["ownedProjection"] = projection
        plan["ownedProjectionHash"] = projection["projectionHash"]
        unsigned_plan = dict(plan)
        unsigned_plan.pop("planHash")
        unsigned_plan.pop("signature")
        plan["planHash"] = digest(unsigned_plan)
        plan["signature"] = hmac.new(
            PLAN_KEY, plan["planHash"].encode(), hashlib.sha256
        ).hexdigest()
        self.current_plan = plan
        self.statuses["mutation"]["authoritativeMutationPlanHash"] = plan[
            "planHash"
        ]
        self.statuses["mutation"]["ownedProjectionHash"] = plan[
            "ownedProjectionHash"
        ]
        reservation["componentReadersHash"] = digest(self.statuses)
        result = self.manager.execute(
            reservation=reservation,
            command="KILL",
            mutation_requests=[sell],
        )
        self.assertFalse(result["receipt"]["ok"])
        self.assertEqual(0, result["receipt"]["attemptCount"])

    def test_state_receipt_binds_full_execution_proof_and_tamper_fails(self) -> None:
        result = self.manager.execute(
            reservation=self.reservation,
            command="START",
            mutation_requests=[self._request("NATURAL_BUY", "START")],
        )
        state_receipt = dict(result["stateManagerReceipt"])
        self.assertEqual(
            result["receipt"]["executionProofHash"],
            state_receipt["executionProofHash"],
        )
        for field in (
            "managerReceiptHash",
            "executionProofHash",
            "mutationPlanHash",
            "ownedProjectionHash",
            "ownedProjectionHeadHash",
            "boundaryEntryProofHash",
            "attemptChainHead",
            "transportReceiptSetHash",
        ):
            tampered = dict(state_receipt)
            tampered[field] = "f" * 64
            self.assertFalse(
                self.manager.verify_state_manager_receipt(tampered), field
            )


if __name__ == "__main__":
    unittest.main()
