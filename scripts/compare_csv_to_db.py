#!/usr/bin/env python3
"""Compare Close CSV export against synced leads in Supabase."""

import csv
import json
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mappers import CLOSE_OWNED_LEAD_COLUMNS, ENRICHMENT_COLUMNS, LEAD_CUSTOM_LABEL_MAPPINGS

CSV_PATH = Path(r"c:\Users\Marcel Brown\Downloads\FB(X) leads 2026-06-22 19-36.csv")

CSV_RENAMES = {
    "id": "close_lead_id",
    "date_created": "close_created_at",
    "date_updated": "close_updated_at",
    "primary_contact_name": "contact_name",
    "primary_contact_primary_phone": "contact_phone",
    "primary_contact_primary_email": "contact_email",
    "last_lead_status_change_date": None,
}

CUSTOM_LABEL_TO_DB = {label: db for label, (db, _) in LEAD_CUSTOM_LABEL_MAPPINGS.items()}

USER_REF_SUFFIX = {
    "Lead Owner": ("lead_owner_id", "lead_owner_name"),
    "Originator": ("originator_id", "originator_name"),
    "Partner Owner": ("partner_owner_id", "partner_owner_name"),
    "Analyst/Account Manager": ("analyst_or_account_manager_id", "analyst_or_account_manager_name"),
    "CFA": ("cfa_id", "cfa_name"),
    "FBX Principal": ("fbx_principal_id", "fbx_principal_name"),
}

SLUG_OVERRIDES = {
    "in_funnel": "in_funnel",
    "in_funnel_hot_warm": "in_funnel_hot_or_warm",
    "webpage_form": "webpage_or_form",
    "partner_split": "partner_split_pct",
    "partner_prospect_or_lender": "partner_prospect_or_lender",
    "quotezone_6m": "quotezone_gt_6m",
    "r_d_hb": "r_and_d_hb",
    "fb_or_fbx_web_inbound": "fb_or_fbx_web_inbound",
    "fb_fbx": "fb_or_fbx",
    "director_email": "director_email",
}


def csv_header_to_db(header: str):
    if header in CSV_RENAMES:
        return CSV_RENAMES[header]
    if header.startswith("custom."):
        label = header[len("custom.") :]
        if label.endswith(".id") or label.endswith(".name"):
            base = label[:-3] if label.endswith(".id") else label[:-5]
            pair = USER_REF_SUFFIX.get(base)
            if pair:
                return pair[0] if label.endswith(".id") else pair[1]
        if label in CUSTOM_LABEL_TO_DB:
            return CUSTOM_LABEL_TO_DB[label]
        slug = re.sub(r"[^a-z0-9]+", "_", label.lower().replace("/", "_or_").replace("?", "")).strip("_")
        slug = SLUG_OVERRIDES.get(slug, slug)
        if slug in CLOSE_OWNED_LEAD_COLUMNS or slug in ENRICHMENT_COLUMNS:
            return slug
        return None
    db = CSV_RENAMES.get(header, header)
    if db in CLOSE_OWNED_LEAD_COLUMNS or db in ENRICHMENT_COLUMNS:
        return db
    return None


def norm(val, db_col=None):
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s.replace("'", '"'))
                if isinstance(parsed, list):
                    if not parsed:
                        return None
                    if len(parsed) == 1:
                        return norm(parsed[0], db_col)
                    return ", ".join(str(x) for x in parsed if str(x).strip())
            except json.JSONDecodeError:
                pass
        if db_col == "paid_partner":
            low = s.lower()
            if low == "yes":
                return True
            if low == "no":
                return False
        if db_col and db_col.startswith("num_"):
            try:
                f = float(s)
                return str(int(f)) if f.is_integer() else str(f)
            except ValueError:
                return s
        if db_col and (db_col.endswith("_at") or "date" in db_col):
            return s.replace(" ", "T")[:19]
        if db_col == "partner_split_pct":
            try:
                f = float(s)
                return str(int(f)) if f.is_integer() else str(f)
            except ValueError:
                return s
        return s
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        if db_col and db_col.startswith("num_"):
            return str(int(val)) if float(val).is_integer() else str(val)
        if db_col == "partner_split_pct":
            return str(int(val)) if float(val).is_integer() else str(val)
        return val
    return val


def main():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])

    with CSV_PATH.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header_map = {h: csv_header_to_db(h) for h in reader.fieldnames}
        csv_by_id = {row["id"]: row for row in reader}

    rows = []
    offset = 0
    while True:
        batch = client.table("leads").select("*").range(offset, offset + 999).execute()
        if not batch.data:
            break
        rows.extend(batch.data)
        if len(batch.data) < 1000:
            break
        offset += 1000

    db_ids = {r["close_lead_id"] for r in rows}
    common = db_ids & set(csv_by_id)
    n = len(common)

    print("=" * 70)
    print("COVERAGE")
    print("=" * 70)
    print(f"DB leads: {len(db_ids)}")
    print(f"CSV leads: {len(csv_by_id)}")
    print(f"Compared (in both): {n}")

    unmapped = [h for h, db in header_map.items() if db is None]
    print(f"CSV headers with no DB column: {len(unmapped)}")
    for h in unmapped:
        print(f"  - {h}")

    comparable = [(h, db) for h, db in header_map.items() if db]
    stats = {db: {"match": 0, "mismatch": 0, "csv_only": 0, "both_empty": 0, "examples": []} for _, db in comparable}

    for lead_id in common:
        csv_row = csv_by_id[lead_id]
        db_row = next(r for r in rows if r["close_lead_id"] == lead_id)
        for csv_h, db_col in comparable:
            csv_val = norm(csv_row.get(csv_h), db_col)
            db_val = norm(db_row.get(db_col), db_col)
            if csv_val is None and db_val is None:
                stats[db_col]["both_empty"] += 1
            elif csv_val is not None and db_val is None:
                stats[db_col]["csv_only"] += 1
                if len(stats[db_col]["examples"]) < 2:
                    stats[db_col]["examples"].append((lead_id, csv_val))
            elif csv_val is None and db_val is not None:
                pass
            elif str(csv_val) == str(db_val) or csv_val == db_val:
                stats[db_col]["match"] += 1
            else:
                stats[db_col]["mismatch"] += 1
                if len(stats[db_col]["examples"]) < 2:
                    stats[db_col]["examples"].append((lead_id, csv_val, db_val))

    print("\n" + "=" * 70)
    print(f"FIELD COMPARISON ({n} leads)")
    print("=" * 70)

    perfect, good, sync_gaps, enrich_gaps, mismatches = [], [], [], [], []

    for _, db_col in comparable:
        s = stats[db_col]
        populated = s["match"] + s["mismatch"] + s["csv_only"]
        if populated == 0:
            continue
        rate = s["match"] / populated
        if s["mismatch"] == 0 and s["csv_only"] == 0:
            perfect.append(db_col)
        elif rate >= 0.9 and s["csv_only"] <= max(1, n * 0.05):
            good.append((db_col, rate))
        elif s["csv_only"] > max(2, n * 0.1):
            item = (db_col, s["csv_only"], populated, rate, s["examples"])
            (enrich_gaps if db_col in ENRICHMENT_COLUMNS else sync_gaps).append(item)
        if s["mismatch"] > 0:
            mismatches.append((db_col, s["mismatch"], s["examples"]))

    print(f"Perfect when CSV has data: {len(perfect)} columns")
    print(f"Good (>=90% match): {len(good)} columns")
    print(f"Sync gaps (CSV has data, DB empty): {len(sync_gaps)} columns")
    print(f"Enrichment gaps (not synced): {len(enrich_gaps)} columns")
    print(f"Mismatches: {len(mismatches)} columns")

    print("\n--- SYNC GAPS (need mapping) ---")
    sync_gaps.sort(key=lambda x: -x[1])
    for db_col, csv_only, populated, rate, examples in sync_gaps[:20]:
        print(f"  {db_col}: empty in DB for {csv_only}/{n} leads (match {rate:.0%} when both set)")
        if examples:
            print(f"    e.g. {examples[0][0]} csv={examples[0][1]!r}")

    print("\n--- ENRICHMENT GAPS (by design, unless we expand sync) ---")
    enrich_gaps.sort(key=lambda x: -x[1])
    for db_col, csv_only, populated, rate, examples in enrich_gaps[:15]:
        print(f"  {db_col}: empty in DB for {csv_only}/{n} leads")

    print("\n--- MISMATCHES ---")
    mismatches.sort(key=lambda x: -x[1])
    for db_col, cnt, examples in mismatches[:15]:
        print(f"  {db_col}: {cnt} leads differ")
        if examples:
            e = examples[0]
            print(f"    e.g. {e[0]} csv={e[1]!r} db={e[2]!r}")

    print("\n--- KEY COLUMNS DB FILL RATE ---")
    for col in [
        "lead_source", "company_registration_number", "paid_partner", "fb_or_fbx",
        "smart_view_tag", "lead_owner_id", "lead_owner_name", "partner_split_pct",
        "num_calls", "num_emails", "last_activity_date", "turnover", "sic_code",
    ]:
        filled = sum(1 for r in rows if r.get(col) not in (None, ""))
        print(f"  {col}: {filled}/{len(rows)}")

    print(f"\nraw_payload present: {sum(1 for r in rows if r.get('raw_payload'))}/{len(rows)}")


if __name__ == "__main__":
    main()
