"""
Debt Collection Lakehouse — Medallion Architecture at Scale

Bronze → Silver → Gold pipeline over 1M+ rows of debt collection data,
using chunked CSV processing throughout so memory stays flat regardless
of input size — the same discipline required when a Fabric notebook reads
billions of rows from OneLake rather than loading everything into a single
pandas DataFrame.

Comments throughout show the PySpark/Fabric equivalent for each step.
"""
import csv
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RAW    = Path(__file__).resolve().parent.parent / "data" / "raw"
BRONZE = Path(__file__).resolve().parent.parent / "data" / "processed" / "bronze"
SILVER = Path(__file__).resolve().parent.parent / "data" / "processed" / "silver"
GOLD   = Path(__file__).resolve().parent.parent / "data" / "processed" / "gold"
for p in [BRONZE, SILVER, GOLD]:
    p.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 50_000   # rows processed per chunk — keeps memory flat at any input scale

def now(): return datetime.now(timezone.utc).isoformat()


# ── BRONZE: chunked raw ingest ──────────────────────────────────────────────
# In Fabric: spark.read.csv(path).write.format("delta").mode("append").save("Tables/bronze_<table>")
def bronze_ingest(table_name: str):
    src = RAW / f"{table_name}.csv"
    dst = BRONZE / f"{table_name}.csv"
    total = 0
    first_chunk = True
    for chunk in pd.read_csv(src, chunksize=CHUNK_SIZE):
        chunk["_ingested_ts"] = now()
        chunk.to_csv(dst, mode="w" if first_chunk else "a", header=first_chunk, index=False)
        first_chunk = False
        total += len(chunk)
    return total


# ── SILVER: chunked cleanse + validation, accumulating in-memory only what's needed ──
# In Fabric: df.filter(...).dropDuplicates(["account_id"]).write.format("delta").save("Tables/silver_accounts")
def silver_cleanse_accounts():
    rejected_count = 0
    clean_chunks = []
    seen_ids = set()
    for chunk in pd.read_csv(BRONZE / "accounts.csv", chunksize=CHUNK_SIZE):
        before = len(chunk)
        chunk = chunk[chunk["original_balance"] > 0]
        chunk = chunk[~chunk["account_id"].isin(seen_ids)]
        chunk = chunk.drop_duplicates(subset=["account_id"])
        seen_ids.update(chunk["account_id"])
        rejected_count += before - len(chunk)
        clean_chunks.append(chunk)
    clean = pd.concat(clean_chunks, ignore_index=True)
    clean["recovery_pct"] = ((clean["original_balance"] - clean["current_balance"]) / clean["original_balance"]).round(4)
    clean.to_csv(SILVER / "accounts_clean.csv", index=False)
    return len(clean), rejected_count


def silver_cleanse_activities(valid_account_ids: set):
    rejected_count, total = 0, 0
    first_chunk = True
    dst = SILVER / "activities_clean.csv"
    for chunk in pd.read_csv(BRONZE / "collection_activities.csv", chunksize=CHUNK_SIZE):
        before = len(chunk)
        chunk = chunk[chunk["account_id"].isin(valid_account_ids)]
        rejected_count += before - len(chunk)
        chunk.to_csv(dst, mode="w" if first_chunk else "a", header=first_chunk, index=False)
        first_chunk = False
        total += len(chunk)
    return total, rejected_count


def silver_cleanse_simple(table_name: str, valid_account_ids: set, amount_col: str = None):
    """Generic chunked cleanse for ptps / payments / settlements:
    drop rows referencing unknown accounts, drop non-positive amounts."""
    rejected_count, total = 0, 0
    first_chunk = True
    dst = SILVER / f"{table_name}_clean.csv"
    for chunk in pd.read_csv(BRONZE / f"{table_name}.csv", chunksize=CHUNK_SIZE):
        before = len(chunk)
        chunk = chunk[chunk["account_id"].isin(valid_account_ids)]
        if amount_col:
            chunk = chunk[chunk[amount_col] > 0]
        rejected_count += before - len(chunk)
        chunk.to_csv(dst, mode="w" if first_chunk else "a", header=first_chunk, index=False)
        first_chunk = False
        total += len(chunk)
    return total, rejected_count


# ── GOLD: business-ready aggregates ──────────────────────────────────────────
# In Fabric: aggregated Delta tables consumed directly by Power BI via DirectLake
def gold_aggregate():
    accounts = pd.read_csv(SILVER / "accounts_clean.csv")
    ptps = pd.read_csv(SILVER / "ptps_clean.csv")
    payments = pd.read_csv(SILVER / "payments_clean.csv")
    settlements = pd.read_csv(SILVER / "settlements_clean.csv")

    # 1. Recovery performance by debt age bucket
    recovery_by_bucket = accounts.groupby("debt_age_bucket").agg(
        accounts=("account_id", "count"),
        total_original_balance=("original_balance", "sum"),
        total_current_balance=("current_balance", "sum"),
        avg_recovery_pct=("recovery_pct", "mean"),
    ).reset_index()
    recovery_by_bucket["avg_recovery_pct"] = (recovery_by_bucket["avg_recovery_pct"] * 100).round(2)
    recovery_by_bucket = recovery_by_bucket.sort_values("debt_age_bucket")
    recovery_by_bucket.to_csv(GOLD / "recovery_by_debt_age_bucket.csv", index=False)

    # 2. PTP kept-rate — a core collections KPI
    ptp_summary = ptps.groupby("status").size().reset_index(name="count")
    total_resolved = ptp_summary[ptp_summary["status"].isin(["kept", "broken"])]["count"].sum()
    kept = ptp_summary.loc[ptp_summary["status"] == "kept", "count"].sum() if "kept" in ptp_summary["status"].values else 0
    ptp_kept_rate = round(kept / total_resolved * 100, 2) if total_resolved else 0
    ptp_summary.to_csv(GOLD / "ptp_status_summary.csv", index=False)

    # 3. Agent performance leaderboard (by recovered amount and PTP kept rate)
    acct_with_agent = accounts[["account_id", "assigned_agent", "original_balance", "current_balance"]]
    agent_perf = acct_with_agent.groupby("assigned_agent").agg(
        accounts_managed=("account_id", "count"),
        total_recovered=("original_balance", lambda s: (
            acct_with_agent.loc[s.index, "original_balance"] - acct_with_agent.loc[s.index, "current_balance"]
        ).sum()),
    ).reset_index().sort_values("total_recovered", ascending=False)
    agent_perf.to_csv(GOLD / "agent_performance_leaderboard.csv", index=False)

    # 4. Settlement / discount analysis
    settlement_summary = settlements.groupby(pd.cut(settlements["discount_pct"],
        bins=[0, 0.3, 0.5, 0.7, 1.0], labels=["0-30%", "30-50%", "50-70%", "70%+"])).agg(
        settlements=("settlement_id", "count"),
        total_original=("original_balance", "sum"),
        total_settled=("settled_amount", "sum"),
    ).reset_index().rename(columns={"discount_pct": "discount_band"})
    settlement_summary.to_csv(GOLD / "settlement_discount_analysis.csv", index=False)

    # 5. Monthly cash collected (payments + settlements combined)
    payments["month"] = pd.to_datetime(payments["payment_date"]).dt.to_period("M").astype(str)
    monthly_payments = payments.groupby("month")["amount"].sum().reset_index(name="payments_collected")
    settlements["month"] = pd.to_datetime(settlements["settlement_date"]).dt.to_period("M").astype(str)
    monthly_settlements = settlements.groupby("month")["settled_amount"].sum().reset_index(name="settlements_collected")
    monthly_cash = monthly_payments.merge(monthly_settlements, on="month", how="outer").fillna(0).sort_values("month")
    monthly_cash["total_cash_collected"] = monthly_cash["payments_collected"] + monthly_cash["settlements_collected"]
    monthly_cash.to_csv(GOLD / "monthly_cash_collected.csv", index=False)

    return recovery_by_bucket, ptp_kept_rate, agent_perf, settlement_summary, monthly_cash


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("DEBT COLLECTION LAKEHOUSE — MEDALLION PIPELINE (BIG DATA)")
    print("=" * 60)

    tables = ["debtors", "accounts", "collection_activities", "ptps", "payments", "settlements"]
    print("\n[Bronze] Chunked ingest (50K rows/chunk)...")
    for t in tables:
        n = bronze_ingest(t)
        print(f"   {t}: {n:,} rows ingested")

    print("\n[Silver] Cleansing, validating, deduplicating...")
    acct_clean, acct_rejected = silver_cleanse_accounts()
    print(f"   accounts: {acct_clean:,} clean ({acct_rejected:,} rejected)")
    valid_ids = set(pd.read_csv(SILVER / "accounts_clean.csv", usecols=["account_id"])["account_id"])

    act_clean, act_rejected = silver_cleanse_activities(valid_ids)
    print(f"   collection_activities: {act_clean:,} clean ({act_rejected:,} rejected)")

    ptp_clean, ptp_rejected = silver_cleanse_simple("ptps", valid_ids, amount_col="promised_amount")
    print(f"   ptps: {ptp_clean:,} clean ({ptp_rejected:,} rejected)")

    pay_clean, pay_rejected = silver_cleanse_simple("payments", valid_ids, amount_col="amount")
    print(f"   payments: {pay_clean:,} clean ({pay_rejected:,} rejected)")

    settle_clean, settle_rejected = silver_cleanse_simple("settlements", valid_ids, amount_col="settled_amount")
    print(f"   settlements: {settle_clean:,} clean ({settle_rejected:,} rejected)")

    print("\n[Gold] Aggregating collections KPIs...")
    recovery, ptp_rate, agents, settle_summary, monthly = gold_aggregate()

    print("\n" + "=" * 60)
    print("RECOVERY PERFORMANCE BY DEBT AGE BUCKET")
    print("=" * 60)
    print(recovery.to_string(index=False))

    print(f"\nOverall PTP kept rate: {ptp_rate}%")

    print("\nTop 5 collection agents by amount recovered:")
    print(agents.head(5).to_string(index=False))

    total_collected = monthly["total_cash_collected"].sum()
    print(f"\nTotal cash collected across all months: R{total_collected:,.2f}")


if __name__ == "__main__":
    main()
