"""The scoped dataset token (api.md §2): only a short-lived HS256 token from Node, for one
dataset, one scope and a row scope of all rows or the caller's own, is accepted."""

import jwt
import pytest

from dataservice.common.utils import dataset_token
from dataservice.common.utils.dataset_token import DatasetClaims, InvalidDatasetToken
from tests.conftest import SECRET

NOW = 1_800_000_000
CLAIMS = DatasetClaims(user_id="u1", dataset_id="d1", scope="read", row_scope="all")


def _raw(**changes: object) -> str:
    payload: dict[str, object] = {"sub": "u1", "datasetId": "d1", "scope": "read", "rowScope": "all"}
    payload |= {"iat": NOW, "exp": NOW + 60} | changes
    return jwt.encode({k: v for k, v in payload.items() if v is not None}, SECRET, algorithm="HS256")


def test_a_fresh_token_from_node_gives_its_claims() -> None:
    assert dataset_token.verify(SECRET, dataset_token.sign(SECRET, CLAIMS, NOW), NOW + 5) == CLAIMS
    own_rows = DatasetClaims(user_id="u1", dataset_id="d1", scope="write", row_scope="u1")
    assert dataset_token.verify(SECRET, dataset_token.sign(SECRET, own_rows, NOW), NOW) == own_rows


def test_clock_skew_of_up_to_30_seconds_is_tolerated() -> None:
    token = dataset_token.sign(SECRET, CLAIMS, NOW)
    assert dataset_token.verify(SECRET, token, NOW + 60 + 30) == CLAIMS
    assert dataset_token.verify(SECRET, token, NOW - 30) == CLAIMS


@pytest.mark.parametrize(
    "token",
    [
        dataset_token.sign(SECRET, CLAIMS, NOW - 200),  # expired
        dataset_token.sign(SECRET, CLAIMS, NOW + 120),  # issued in the future
        dataset_token.sign(SECRET, CLAIMS, NOW, lifetime_seconds=3600),  # lives too long
        dataset_token.sign("another-secret-that-is-32-characters-long!", CLAIMS, NOW),  # forged
        jwt.encode(
            {"sub": "u1", "datasetId": "d1", "scope": "read", "rowScope": "all", "iat": NOW, "exp": NOW + 60},
            "",
            algorithm="none",
        ),
        jwt.encode(
            {"sub": "u1", "datasetId": "d1", "scope": "read", "rowScope": "all", "iat": NOW, "exp": NOW + 60},
            SECRET,
            algorithm="HS512",
        ),
        _raw(scope="admin"),
        _raw(rowScope="someone-else"),  # another user's rows
        _raw(datasetId=None),  # a claim missing
        _raw(sub=""),
        _raw(iat=True, exp=True),
        "not-a-token",
        "",
    ],
    ids=[
        "expired",
        "future",
        "too-long",
        "forged",
        "alg-none",
        "alg-hs512",
        "bad-scope",
        "other-rows",
        "missing-claim",
        "empty-user",
        "bool-times",
        "garbage",
        "empty",
    ],
)
def test_anything_else_is_refused(token: str) -> None:
    with pytest.raises(InvalidDatasetToken):
        dataset_token.verify(SECRET, token, NOW)
