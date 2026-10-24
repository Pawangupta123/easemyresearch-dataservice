"""Short-lived service tokens Node signs for every call (plan step 0.5).

Format: `<issuedAtUnixSeconds>.<hex HMAC-SHA256(secret, issuedAt)>`. Pure functions — no I/O.
"""

import hashlib
import hmac

CLOCK_SKEW_SECONDS = 30


def sign(secret: str, issued_at: int) -> str:
    mac = hmac.new(secret.encode(), str(issued_at).encode(), hashlib.sha256).hexdigest()
    return f"{issued_at}.{mac}"


def verify(secret: str, token: str, now: int, max_age_seconds: int) -> bool:
    issued_part, _, mac = token.partition(".")
    if not issued_part.isdigit() or not mac:
        return False
    issued_at = int(issued_part)
    if issued_at > now + CLOCK_SKEW_SECONDS or now - issued_at > max_age_seconds:
        return False
    return hmac.compare_digest(sign(secret, issued_at), token)
