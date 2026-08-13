from __future__ import annotations

import copy
import unittest
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from live_trader.kis_domestic_functional_contract import (
    ACTIVE_SECONDS,
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_ARTIFACT_ID,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    APPROVED_INSTANCE_ID,
    LIVE_ORIGIN,
    PDNO,
    ROUTE,
    KisDomesticFunctionalContractBlocked,
    _validate_approved_semantics,
    canonical_content_hash,
    production_entrypoint_status,
    seal_kis_domestic_activation_window,
    terminal_taxonomy_contract,
    verify_kis_domestic_functional_publication,
)


KST = ZoneInfo("Asia/Seoul")


def _evaluation(publication, when: datetime) -> dict[str, object]:
    value: dict[str, object] = {
        "schemaVersion": "kis-domestic-natural-breakout-evaluation/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "artifactContentHash": publication.artifact_content_hash,
        "artifactFileSha256": publication.artifact_file_sha256,
        "instanceContentHash": publication.instance_content_hash,
        "instanceFileSha256": publication.instance_file_sha256,
        "plugin": "breakout",
        "breakoutWindow": 10,
        "breakoutK": "0.3",
        "barIntervalMinutes": 5,
        "signal": "BUY",
        "barCloseAt": when.isoformat(),
        "evaluatedAt": when.isoformat(),
    }
    value["evaluationBodyHash"] = canonical_content_hash(value)
    return value


class KisDomesticFunctionalContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.publication = verify_kis_domestic_functional_publication()

    def test_approved_existing_strategy_core_publication_is_exactly_bound(self) -> None:
        publication = self.publication
        self.assertEqual(APPROVED_ARTIFACT_ID, publication.artifact["id"])
        self.assertEqual(APPROVED_INSTANCE_ID, publication.instance["instanceId"])
        self.assertEqual(APPROVED_ARTIFACT_CONTENT_HASH, publication.artifact_content_hash)
        self.assertEqual(APPROVED_ARTIFACT_FILE_SHA256, publication.artifact_file_sha256)
        self.assertEqual(APPROVED_INSTANCE_CONTENT_HASH, publication.instance_content_hash)
        self.assertEqual(APPROVED_INSTANCE_FILE_SHA256, publication.instance_file_sha256)
        self.assertEqual("breakout", publication.artifact["plugin"])
        self.assertFalse(publication.artifact["traderContract"]["canPlaceOrders"])
        self.assertFalse(publication.artifact["promotionEligible"])
        self.assertFalse(publication.instance["qualification"]["promotionEligible"])

        envelope = publication.contract_envelope
        self.assertEqual(ROUTE, envelope["route"])
        self.assertEqual(LIVE_ORIGIN, envelope["origin"])
        self.assertEqual(PDNO, envelope["pdno"])
        self.assertEqual("100000", envelope["maxOrderKrw"])
        self.assertEqual("5000", envelope["ownerLossMustRemainBelowKrw"])
        self.assertFalse(envelope["terminalVerifierAvailable"])
        self.assertEqual(64, len(publication.contract_envelope_hash))

    def test_semantic_substitution_is_rejected_even_as_an_opaque_copy(self) -> None:
        artifact = copy.deepcopy(self.publication.artifact)
        artifact["parameters"]["breakoutWindow"] = 9
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "breakoutWindow"):
            _validate_approved_semantics(artifact, self.publication.instance)

        artifact = copy.deepcopy(self.publication.artifact)
        artifact["settings"]["executionTiming"] = "same-close"
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "executionTiming"):
            _validate_approved_semantics(artifact, self.publication.instance)

        instance = copy.deepcopy(self.publication.instance)
        instance["qualification"]["promotionEligible"] = True
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "promotionEligible"):
            _validate_approved_semantics(self.publication.artifact, instance)

        instance = copy.deepcopy(self.publication.instance)
        instance["runtimeMarketDataContract"]["openBoundaryAttestationRequired"] = "REST"
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "openBoundary"):
            _validate_approved_semantics(self.publication.artifact, instance)

    def test_activation_is_exact_natural_closed_five_minute_breakout_boundary(self) -> None:
        trading_date = date(2026, 8, 13)
        activated = datetime(2026, 8, 13, 13, 15, tzinfo=KST)
        window = seal_kis_domestic_activation_window(
            publication=self.publication,
            natural_evaluation=_evaluation(self.publication, activated),
            trading_date=trading_date,
            armed_at=datetime(2026, 8, 13, 13, 10, tzinfo=KST),
            activated_at=activated,
        )
        self.assertEqual(ACTIVE_SECONDS, window.active_seconds)
        self.assertEqual("2026-08-13T15:15:00+09:00", window.active_ends_at.isoformat())
        self.assertEqual("2026-08-13T15:30:00+09:00", window.cleanup_ends_at.isoformat())
        self.assertFalse(window.as_dict()["freshSignedQuotePreflightSatisfied"])
        self.assertFalse(window.as_dict()["productionAvailable"])

        non_boundary = datetime(2026, 8, 13, 13, 14, 59, tzinfo=KST)
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "5-minute"):
            seal_kis_domestic_activation_window(
                publication=self.publication,
                natural_evaluation=_evaluation(self.publication, non_boundary),
                trading_date=trading_date,
                armed_at=datetime(2026, 8, 13, 13, 10, tzinfo=KST),
                activated_at=non_boundary,
            )
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "regular hours"):
            early = datetime(2026, 8, 13, 8, 55, tzinfo=KST)
            seal_kis_domestic_activation_window(
                publication=self.publication,
                natural_evaluation=_evaluation(self.publication, early),
                trading_date=trading_date,
                armed_at=early,
                activated_at=early,
            )

    def test_arbitrary_or_tampered_evaluation_cannot_activate(self) -> None:
        activated = datetime(2026, 8, 13, 13, 10, tzinfo=KST)
        evaluation = _evaluation(self.publication, activated)
        evaluation["breakoutK"] = "0.4"
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "breakoutK"):
            seal_kis_domestic_activation_window(
                publication=self.publication,
                natural_evaluation=evaluation,
                trading_date=date(2026, 8, 13),
                armed_at=datetime(2026, 8, 13, 13, 5, tzinfo=KST),
                activated_at=activated,
            )

        backdated = _evaluation(self.publication, activated)
        with self.assertRaisesRegex(KisDomesticFunctionalContractBlocked, "exact natural evaluation"):
            seal_kis_domestic_activation_window(
                publication=self.publication,
                natural_evaluation=backdated,
                trading_date=date(2026, 8, 13),
                armed_at=datetime(2026, 8, 13, 13, 5, tzinfo=KST),
                activated_at=datetime(2026, 8, 13, 13, 15, tzinfo=KST),
            )

    def test_activation_uses_official_xkrx_session_calendar(self) -> None:
        for day in (date(2026, 8, 14), date(2026, 8, 18)):
            with self.subTest(day=day, expected="session"):
                activated = datetime.combine(day, time(13, 15), KST)
                window = seal_kis_domestic_activation_window(
                    publication=self.publication,
                    natural_evaluation=_evaluation(self.publication, activated),
                    trading_date=day,
                    armed_at=activated - timedelta(minutes=5),
                    activated_at=activated,
                )
                self.assertEqual(day, window.trading_date)
        for day in (date(2026, 8, 15), date(2026, 8, 17)):
            with self.subTest(day=day, expected="closed"):
                activated = datetime.combine(day, time(13, 15), KST)
                with self.assertRaisesRegex(
                    KisDomesticFunctionalContractBlocked,
                    "official XKRX regular session",
                ):
                    seal_kis_domestic_activation_window(
                        publication=self.publication,
                        natural_evaluation=_evaluation(self.publication, activated),
                        trading_date=day,
                        armed_at=activated - timedelta(minutes=5),
                        activated_at=activated,
                    )

    def test_taxonomy_and_release_gate_are_nonpromoting_and_unavailable(self) -> None:
        taxonomy = terminal_taxonomy_contract()
        self.assertEqual(
            "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT",
            taxonomy["terminalOutcomeIfNaturalSellAbsent"],
        )
        self.assertFalse(taxonomy["naturalSellSupported"])
        self.assertFalse(taxonomy["productionPromotionEligible"])
        self.assertFalse(taxonomy["terminalVerifierAvailable"])

        status = production_entrypoint_status()
        self.assertFalse(status["available"])
        self.assertFalse(status["networkEnabled"])
        self.assertFalse(status["mutationEnabled"])
        self.assertFalse(status["freshSignedQuotePreflightAvailable"])
        self.assertFalse(status["freshSignedHolidayOpenPreflightAvailable"])
        self.assertFalse(status["nextOpenWebsocketAttestationAvailable"])


if __name__ == "__main__":
    unittest.main()
