"""JSON/JS integer representation only; exchange permission is always observed."""
from decimal import Decimal, InvalidOperation

MAX_SAFE_LEVERAGE_INTEGER = 9007199254740991

def leverage_integer(value):
    if isinstance(value, bool):
        raise ValueError("FUTURES_LEVERAGE_INTEGER_REQUIRED")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("FUTURES_LEVERAGE_INTEGER_REQUIRED") from None
    if (not number.is_finite() or number != number.to_integral_value()
            or not 1 <= number <= MAX_SAFE_LEVERAGE_INTEGER):
        raise ValueError("FUTURES_LEVERAGE_INTEGER_REQUIRED")
    return int(number)
