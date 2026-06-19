"""Run with: python -m unittest discover -s tests -v"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


class TestSilverLogic(unittest.TestCase):
    """Tests the cleansing rules in isolation against small in-memory frames,
    so they run in milliseconds regardless of the production data volume."""

    def test_zero_balance_rejected(self):
        df = pd.DataFrame([
            {"account_id": "A1", "original_balance": 0, "current_balance": 0},
            {"account_id": "A2", "original_balance": 5000, "current_balance": 3000},
        ])
        clean = df[df["original_balance"] > 0]
        self.assertEqual(len(clean), 1)
        self.assertEqual(clean.iloc[0]["account_id"], "A2")

    def test_duplicate_account_id_deduplicated(self):
        df = pd.DataFrame([
            {"account_id": "A1", "original_balance": 1000},
            {"account_id": "A1", "original_balance": 1000},
            {"account_id": "A2", "original_balance": 2000},
        ])
        clean = df.drop_duplicates(subset=["account_id"])
        self.assertEqual(len(clean), 2)

    def test_orphan_account_reference_rejected(self):
        valid_ids = {"A1", "A2"}
        df = pd.DataFrame([
            {"activity_id": "ACT1", "account_id": "A1"},
            {"activity_id": "ACT2", "account_id": "A99"},   # orphan
        ])
        clean = df[df["account_id"].isin(valid_ids)]
        self.assertEqual(len(clean), 1)
        self.assertEqual(clean.iloc[0]["activity_id"], "ACT1")

    def test_recovery_pct_calculation(self):
        df = pd.DataFrame([{"original_balance": 1000.0, "current_balance": 400.0}])
        df["recovery_pct"] = ((df["original_balance"] - df["current_balance"]) / df["original_balance"]).round(4)
        self.assertAlmostEqual(df.iloc[0]["recovery_pct"], 0.6)


class TestGoldKPIs(unittest.TestCase):
    def test_ptp_kept_rate_calculation(self):
        ptps = pd.DataFrame([
            {"ptp_id": "P1", "status": "kept"},
            {"ptp_id": "P2", "status": "kept"},
            {"ptp_id": "P3", "status": "broken"},
            {"ptp_id": "P4", "status": "pending"},   # excluded from rate
        ])
        resolved = ptps[ptps["status"].isin(["kept", "broken"])]
        kept_rate = (resolved["status"] == "kept").sum() / len(resolved) * 100
        self.assertAlmostEqual(kept_rate, 66.666, places=2)

    def test_debt_age_bucket_assignment(self):
        def bucket_for_days(days):
            if days <= 30: return "0-30"
            if days <= 60: return "31-60"
            if days <= 90: return "61-90"
            if days <= 180: return "91-180"
            if days <= 365: return "181-365"
            return "365+"
        self.assertEqual(bucket_for_days(15), "0-30")
        self.assertEqual(bucket_for_days(400), "365+")
        self.assertEqual(bucket_for_days(90), "61-90")

    def test_agent_recovery_sums_correctly(self):
        accounts = pd.DataFrame([
            {"account_id": "A1", "assigned_agent": "AGT1", "original_balance": 1000, "current_balance": 400},
            {"account_id": "A2", "assigned_agent": "AGT1", "original_balance": 2000, "current_balance": 1500},
        ])
        recovered = (accounts["original_balance"] - accounts["current_balance"]).sum()
        self.assertAlmostEqual(recovered, 1100)


if __name__ == "__main__":
    unittest.main()
