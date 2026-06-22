import pandas as pd
from quality.expectations_silver import validate_silver


def test_validate_silver_catches_null_auth_id():
    bad = pd.DataFrame({"auth_id": ["a1", None], "amount": [10.0, 20.0],
                        "decision": ["approve", "decline"]})
    r = validate_silver(bad)
    assert r["ok"] is False
    assert "auth_id" in r["failures"]


def test_validate_silver_catches_negative_amount():
    bad = pd.DataFrame({"auth_id": ["a1"], "amount": [-5.0], "decision": ["approve"]})
    r = validate_silver(bad)
    assert r["ok"] is False
    assert "amount" in r["failures"]


def test_validate_silver_passes_clean():
    good = pd.DataFrame({"auth_id": ["a1", "a2"], "amount": [10.0, 20.0],
                         "decision": ["approve", "decline"]})
    assert validate_silver(good)["ok"] is True
