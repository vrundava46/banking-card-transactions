import pandas as pd

VALID_DECISIONS = {"approve", "decline"}


def validate_silver(df: pd.DataFrame) -> dict:
    failures = []
    if df["auth_id"].isnull().any():
        failures.append("auth_id")
    if (df["amount"] < 0).any():
        failures.append("amount")
    if not df["decision"].isin(VALID_DECISIONS).all():
        failures.append("decision")
    return {"ok": len(failures) == 0, "failures": failures}


def main():  # pragma: no cover
    raise SystemExit("wire to read silver from Trino in production")


if __name__ == "__main__":  # pragma: no cover
    main()
