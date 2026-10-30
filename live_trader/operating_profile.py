"""Remember configuration, never an approval, prepared receipt or running session."""
from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4


def save_profile(root, contract, selections):
    # Only catalog-pinned selections and limits that passed build_contract.
    value = {
        'schemaVersion': 'operating-profile-v1',
        'selections': [{k: row[k] for k in ('selectionKey', 'artifactFileSha256', 'instanceFileSha256',
                       'portfolioFileSha256', 'kind', 'instanceId', 'portfolioId', 'artifactId', 'route', 'timeframe') if k in row}
                       for row in selections],
        'durationHours': contract['durationHours'],
        'budgetKrwPerBroker': contract['capitalKrwPerBroker'],
        'lossKrwPerBroker': contract['dailyLossStopKrwPerBroker'],
        'futuresPolicy': contract.get('futuresPolicy'),
        'savedAt': contract['preparedAt'],
        'automaticStartAllowed': False,
    }
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    target = root / 'operating-profile.json'
    temporary = root / f'.operating-profile-{uuid4().hex}.tmp'
    try:
        with temporary.open('x', encoding='utf-8', newline='\n') as stream:
            json.dump(value, stream, ensure_ascii=False, allow_nan=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return value


def load_profile(root):
    path = Path(root) / 'operating-profile.json'
    try:
        if path.stat().st_size > 65536:
            return None
        value = json.loads(path.read_text(encoding='utf-8'))
        if value.get('schemaVersion') != 'operating-profile-v1':
            return None
        # A saved profile is an untrusted suggestion. Fresh catalog resolve,
        # build_contract and account checks remain mandatory on every prepare.
        return {key: value[key] for key in ('schemaVersion', 'selections', 'durationHours',
            'budgetKrwPerBroker', 'lossKrwPerBroker', 'futuresPolicy', 'savedAt') if key in value}
    except (OSError, ValueError, TypeError, AttributeError):
        return None
