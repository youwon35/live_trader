"""Final KIS ticket boundary: local reservations cannot unlock integrated orders."""
from copy import deepcopy
import os
import unittest
from unittest.mock import patch

import test_kis_equity_operational as fixture

MODE_KEY = 'KIS_OVERSEAS_FUNDING_MODE'
BLOCKER = '^KIS_INTEGRATED_MARGIN_EXECUTION_NOT_VERIFIED$'


def ticket(exchange='KRX', side='BUY'):
    spec, contract, context = fixture.fixture(exchange, side)
    if exchange == 'KRX' and side == 'SELL':
        # A sufficiently large synthetic owned sale preserves the existing
        # KRW1000 cost-holdback guard; tiny test sales correctly fail earlier.
        context['nativeMarketObservation'] = fixture.service.seal({
            **context['nativeMarketObservation'], 'holdingQuantity': '40', 'sellableQuantity': '40'})
        calculated = fixture.service.live_equity_quantity(spec, side=side, price=100,
            allocation_weight=1, identity={'deploymentId': 'deployment'}, context=context,
            position_quantity=40, now=fixture.NOW)
    else:
        calculated = fixture.quantity(spec, context, side)
    if calculated['status'] != 'READY':
        raise AssertionError('Legacy synthetic ticket fixture is not valid')
    return fixture.intent(spec, context, calculated, side)


class IntegratedFinalTicketGuardTests(unittest.TestCase):
    def test_explicit_foreign_cash_retains_all_legacy_venues_and_sides(self):
        for exchange in fixture.service.VENUES:
            for side in ('BUY', 'SELL'):
                with self.subTest(exchange=exchange, side=side):
                    checked = fixture.service.validate_ticket(ticket(exchange, side),
                        now=fixture.NOW, settings=dict(fixture.SETTINGS))
                    self.assertEqual(checked['exchange'], exchange)
                    self.assertIs(checked['orderAuthorityGranted'], False)

    def test_integrated_default_blank_and_explicit_block_domestic_and_us(self):
        for exchange in fixture.service.VENUES:
            for side in ('BUY', 'SELL'):
                for mode in (None, '', 'INTEGRATED_MARGIN'):
                    settings = dict(fixture.SETTINGS)
                    if mode is None:
                        settings.pop(MODE_KEY)
                    else:
                        settings[MODE_KEY] = mode
                    with self.subTest(exchange=exchange, side=side, mode=mode):
                        with self.assertRaisesRegex(ValueError, BLOCKER):
                            fixture.service.validate_ticket(ticket(exchange, side), now=fixture.NOW, settings=settings)

    def test_generic_ticket_path_uses_current_public_mode_and_defaults_fail_closed(self):
        for mode in (None, '', 'INTEGRATED_MARGIN'):
            with patch.dict(os.environ, {}, clear=True):
                if mode is not None:
                    os.environ[MODE_KEY] = mode
                with self.assertRaisesRegex(ValueError, BLOCKER):
                    fixture.ticket_module.validate_native_ticket(ticket('KRX'), now=fixture.NOW)
        with patch.dict(os.environ, {MODE_KEY: 'FOREIGN_CASH'}, clear=True):
            checked = fixture.ticket_module.validate_native_ticket(ticket('KRX'), now=fixture.NOW)
            self.assertEqual(checked['exchange'], 'KRX')
            self.assertIs(checked['orderAuthorityGranted'], False)

    def test_current_mode_change_rejects_an_already_sealed_cash_only_ticket(self):
        sealed = ticket('KRX')
        before = deepcopy(sealed.metadata)
        fixture.service.validate_ticket(sealed, now=fixture.NOW, settings=fixture.SETTINGS)
        with self.assertRaisesRegex(ValueError, BLOCKER):
            fixture.service.validate_ticket(sealed, now=fixture.NOW,
                settings={**fixture.SETTINGS, MODE_KEY: 'INTEGRATED_MARGIN'})
        self.assertEqual(sealed.metadata, before)

    def test_local_pool_request_and_self_hash_never_grant_integrated_wire_authority(self):
        sealed = ticket('KRX')
        # Even a caller-provided local reservation envelope is not the missing
        # authenticated financial/settlement producer. No DB is opened here.
        sealed.metadata['kis_shared_krw_reservation'] = {
            'status': 'RESERVED', 'policyHash': 'a' * 64, 'requestHash': 'b' * 64,
            'requestedKrw': '1000000', 'financialQualificationGranted': False,
            'orderAuthorityGranted': False}
        with self.assertRaisesRegex(ValueError, BLOCKER):
            fixture.service.validate_ticket(sealed, now=fixture.NOW,
                settings={**fixture.SETTINGS, MODE_KEY: 'INTEGRATED_MARGIN'})

    def test_invalid_mode_cannot_fall_back_to_cash_only(self):
        for mode in ('AUTO', 0, True, None):
            with self.assertRaises(ValueError):
                fixture.service.validate_ticket(ticket('KRX'), now=fixture.NOW,
                    settings={**fixture.SETTINGS, MODE_KEY: mode})

    def test_cash_only_mode_still_rejects_changed_quantity_and_cost_reserve(self):
        changed = ticket('KRX'); changed.quantity += 1
        with self.assertRaisesRegex(ValueError, 'KIS_V2_TICKET_WIRE_CHANGED'):
            fixture.service.validate_ticket(changed, now=fixture.NOW, settings=fixture.SETTINGS)
        changed = ticket('KRX')
        altered = fixture.service.seal({**changed.metadata['native_quantity_model'], 'costReserve': '0'})
        changed.metadata['native_quantity_model'] = altered
        changed.metadata['native_preflight']['kisEquityTicket'] = altered
        with self.assertRaisesRegex(ValueError, 'KIS_V2_TICKET_DECIMAL_ACCOUNTING_CHANGED'):
            fixture.service.validate_ticket(changed, now=fixture.NOW, settings=fixture.SETTINGS)


if __name__ == '__main__':
    unittest.main()
