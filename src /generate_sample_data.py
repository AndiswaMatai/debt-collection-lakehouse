"""
Generates large-scale synthetic debt collection data — modelled on the kind
of book a debt collection / BPO operation (e.g. Ison Xperiences) manages:
debtor accounts, promise-to-pay (PTP) agreements, payments, settlement
discounts, and collection call activity.

Volumes are deliberately large (100K+ accounts, 500K+ call activities) to
exercise the chunked, vectorised processing patterns used for big data in
a Fabric Lakehouse, while still running comfortably on a laptop.

Generation is fully vectorised with numpy so it scales to millions of rows
without Python-level loops.
"""
import csv
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

rng = np.random.default_rng(42)
RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

# ── Scale knobs ──────────────────────────────────────────────────────────────
N_DEBTORS      = 60_000
N_ACCOUNTS     = 150_000     # a debtor can have multiple accounts (credit card, retail, telco...)
N_ACTIVITIES   = 600_000     # collection call/SMS/email attempts
N_PTPS         = 120_000     # promise-to-pay agreements
N_PAYMENTS     = 200_000
N_SETTLEMENTS  = 18_000      # discounted full-and-final settlements

ORIGINAL_CREDITORS = ["MTN Retail Credit", "Edgars Stores", "Vodacom Contracts", "Capitec Personal Loans",
                       "Telkom Mobile", "Foschini Group", "African Bank", "Truworths"]
REGIONS = ["Gauteng", "Western Cape", "KwaZulu-Natal", "Eastern Cape", "Free State", "Limpopo"]
AGENTS = [f"AGT{str(i).zfill(4)}" for i in range(1, 251)]   # 250 collection agents
DEBT_BUCKETS = ["0-30", "31-60", "61-90", "91-180", "181-365", "365+"]
ACCOUNT_STATUSES = ["active", "ptp_pending", "settled", "written_off", "paid_in_full", "legal"]
CALL_OUTCOMES = ["no_answer", "promise_to_pay", "dispute", "refused_to_pay", "wrong_number",
                  "payment_made", "call_back_requested", "voicemail"]
PAYMENT_METHODS = ["EFT", "Debit Order", "Cash Deposit", "Card Payment", "Payroll Deduction"]

start_date = datetime(2024, 1, 1)
today = datetime(2026, 6, 1)


def _random_dates(n, start, end):
    delta_days = (end - start).days
    offsets = rng.integers(0, delta_days, size=n)
    return [start + timedelta(days=int(o)) for o in offsets]


# ── Debtors ──────────────────────────────────────────────────────────────────
print("Generating debtors...")
debtor_ids = [f"DBT{str(i).zfill(7)}" for i in range(1, N_DEBTORS + 1)]
debtors = pd.DataFrame({
    "debtor_id": debtor_ids,
    "region": rng.choice(REGIONS, N_DEBTORS),
    "age_band": rng.choice(["18-25", "26-35", "36-45", "46-55", "56-65", "65+"], N_DEBTORS,
                            p=[0.10, 0.28, 0.27, 0.18, 0.12, 0.05]),
    "employment_status": rng.choice(["employed", "self_employed", "unemployed", "pensioner"], N_DEBTORS,
                                     p=[0.55, 0.15, 0.20, 0.10]),
})
debtors.to_csv(RAW / "debtors.csv", index=False)

# ── Accounts ─────────────────────────────────────────────────────────────────
print("Generating accounts...")
account_ids = [f"ACC{str(i).zfill(8)}" for i in range(1, N_ACCOUNTS + 1)]
acct_debtor = rng.choice(debtor_ids, N_ACCOUNTS)
original_balance = np.round(rng.gamma(shape=2.2, scale=3500, size=N_ACCOUNTS) + 500, 2)
placement_dates = _random_dates(N_ACCOUNTS, start_date, today - timedelta(days=30))
days_overdue = np.array([(today - d).days for d in placement_dates])

def bucket_for_days(days):
    if days <= 30: return "0-30"
    if days <= 60: return "31-60"
    if days <= 90: return "61-90"
    if days <= 180: return "91-180"
    if days <= 365: return "181-365"
    return "365+"

debt_bucket = [bucket_for_days(d) for d in days_overdue]
recovery_pct = rng.beta(1.5, 4, N_ACCOUNTS)            # most accounts recover a minority of balance
current_balance = np.round(original_balance * (1 - recovery_pct), 2)
status = rng.choice(ACCOUNT_STATUSES, N_ACCOUNTS, p=[0.42, 0.10, 0.10, 0.15, 0.18, 0.05])

accounts = pd.DataFrame({
    "account_id": account_ids,
    "debtor_id": acct_debtor,
    "original_creditor": rng.choice(ORIGINAL_CREDITORS, N_ACCOUNTS),
    "original_balance": original_balance,
    "current_balance": current_balance,
    "placement_date": [d.strftime("%Y-%m-%d") for d in placement_dates],
    "days_overdue": days_overdue,
    "debt_age_bucket": debt_bucket,
    "status": status,
    "assigned_agent": rng.choice(AGENTS, N_ACCOUNTS),
})

# Inject ~0.5% dirty records: zero/negative balances and duplicate account_ids,
# so the Silver layer's validation has real defects to catch.
n_dirty = int(N_ACCOUNTS * 0.005)
dirty_idx = rng.choice(N_ACCOUNTS, n_dirty, replace=False)
accounts.loc[dirty_idx[: n_dirty // 2], "original_balance"] = 0
dup_idx = rng.choice(N_ACCOUNTS, n_dirty // 2, replace=False)
accounts.loc[dup_idx, "account_id"] = accounts["account_id"].iloc[0]   # force duplicates of the first account_id

accounts.to_csv(RAW / "accounts.csv", index=False)

# ── Collection activities (calls/SMS/email) — the big table ────────────────
print(f"Generating {N_ACTIVITIES:,} collection activities...")
act_account_idx = rng.integers(0, N_ACCOUNTS, N_ACTIVITIES)
act_accounts = np.array(account_ids)[act_account_idx]
act_agents = np.array(accounts["assigned_agent"])[act_account_idx]
act_dates = _random_dates(N_ACTIVITIES, start_date, today)
act_outcomes = rng.choice(CALL_OUTCOMES, N_ACTIVITIES,
                           p=[0.30, 0.12, 0.08, 0.10, 0.08, 0.12, 0.12, 0.08])
act_channel = rng.choice(["call", "sms", "email", "whatsapp"], N_ACTIVITIES, p=[0.55, 0.20, 0.15, 0.10])

activities = pd.DataFrame({
    "activity_id": [f"ACT{str(i).zfill(9)}" for i in range(1, N_ACTIVITIES + 1)],
    "account_id": act_accounts,
    "agent_id": act_agents,
    "activity_date": [d.strftime("%Y-%m-%d") for d in act_dates],
    "channel": act_channel,
    "outcome": act_outcomes,
})
# Inject ~0.3% orphan account references (simulates late-arriving / out-of-sync data)
n_orphans = int(N_ACTIVITIES * 0.003)
orphan_idx = rng.choice(N_ACTIVITIES, n_orphans, replace=False)
activities.loc[orphan_idx, "account_id"] = [f"ACC{str(i).zfill(8)}" for i in range(99_000_000, 99_000_000 + n_orphans)]
activities.to_csv(RAW / "collection_activities.csv", index=False)

# ── Promise to Pay (PTP) ─────────────────────────────────────────────────────
print(f"Generating {N_PTPS:,} PTP agreements...")
ptp_account_idx = rng.integers(0, N_ACCOUNTS, N_PTPS)
ptp_accounts = np.array(account_ids)[ptp_account_idx]
ptp_balance = np.array(current_balance)[ptp_account_idx]
ptp_created = _random_dates(N_PTPS, start_date, today - timedelta(days=5))
ptp_promised_date = [d + timedelta(days=int(rng.integers(3, 30))) for d in ptp_created]
ptp_promised_amount = np.round(np.clip(ptp_balance, 100, None) * rng.uniform(0.1, 1.0, N_PTPS), 2)
# PTP status: kept / broken / pending (pending if promised_date is in the future)
ptp_status = []
for pd_, created in zip(ptp_promised_date, ptp_created):
    if pd_ > today:
        ptp_status.append("pending")
    else:
        ptp_status.append(rng.choice(["kept", "broken"], p=[0.42, 0.58]))

ptps = pd.DataFrame({
    "ptp_id": [f"PTP{str(i).zfill(8)}" for i in range(1, N_PTPS + 1)],
    "account_id": ptp_accounts,
    "created_date": [d.strftime("%Y-%m-%d") for d in ptp_created],
    "promised_date": [d.strftime("%Y-%m-%d") for d in ptp_promised_date],
    "promised_amount": ptp_promised_amount,
    "status": ptp_status,
})
ptps.to_csv(RAW / "ptps.csv", index=False)

# ── Payments ─────────────────────────────────────────────────────────────────
print(f"Generating {N_PAYMENTS:,} payments...")
pay_account_idx = rng.integers(0, N_ACCOUNTS, N_PAYMENTS)
pay_accounts = np.array(account_ids)[pay_account_idx]
pay_balance = np.array(current_balance)[pay_account_idx]
pay_dates = _random_dates(N_PAYMENTS, start_date, today)
pay_amount = np.round(np.clip(pay_balance, 50, None) * rng.uniform(0.05, 0.5, N_PAYMENTS), 2)

payments = pd.DataFrame({
    "payment_id": [f"PAY{str(i).zfill(8)}" for i in range(1, N_PAYMENTS + 1)],
    "account_id": pay_accounts,
    "payment_date": [d.strftime("%Y-%m-%d") for d in pay_dates],
    "amount": pay_amount,
    "payment_method": rng.choice(PAYMENT_METHODS, N_PAYMENTS),
})
payments.to_csv(RAW / "payments.csv", index=False)

# ── Settlements (discounted full-and-final agreements) ──────────────────────
print(f"Generating {N_SETTLEMENTS:,} settlements...")
settle_account_idx = rng.integers(0, N_ACCOUNTS, N_SETTLEMENTS)
settle_accounts = np.array(account_ids)[settle_account_idx]
settle_original = np.array(original_balance)[settle_account_idx]
discount_pct = np.round(rng.uniform(0.20, 0.70, N_SETTLEMENTS), 3)
settled_amount = np.round(settle_original * (1 - discount_pct), 2)
settle_dates = _random_dates(N_SETTLEMENTS, start_date, today)

settlements = pd.DataFrame({
    "settlement_id": [f"SET{str(i).zfill(6)}" for i in range(1, N_SETTLEMENTS + 1)],
    "account_id": settle_accounts,
    "original_balance": settle_original,
    "settled_amount": settled_amount,
    "discount_pct": discount_pct,
    "settlement_date": [d.strftime("%Y-%m-%d") for d in settle_dates],
    "approved_by": rng.choice([f"MGR{str(i).zfill(3)}" for i in range(1, 21)], N_SETTLEMENTS),
})
settlements.to_csv(RAW / "settlements.csv", index=False)

total_rows = len(debtors) + len(accounts) + len(activities) + len(ptps) + len(payments) + len(settlements)
print(f"\nDone. Total rows generated: {total_rows:,}")
print(f"  debtors: {len(debtors):,}")
print(f"  accounts: {len(accounts):,}")
print(f"  collection_activities: {len(activities):,}")
print(f"  ptps: {len(ptps):,}")
print(f"  payments: {len(payments):,}")
print(f"  settlements: {len(settlements):,}")
