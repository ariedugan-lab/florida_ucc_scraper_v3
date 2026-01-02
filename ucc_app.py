from UCC import search_ucc_fl_document, search_ucc_fl_all
from MCA import mca
from supabase import create_client, Client
import os
import time
from dotenv import load_dotenv
import json

# ---- configs
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BATCH_SIZE = int(os.getenv("BATCH_SIZE_UCC", "100"))
MIN_UCC_CONFIDENCE_SCORE = int(os.getenv("MIN_UCC_CONFIDENCE_SCORE", "85"))
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- supabase companies querys
# -- only companies with sunbiz results but no uccs yet
companies = (
    supabase
    .table("companies")
    .select("id, name, city, state_portal_fetch")
    .eq("state", "FL")
    .not_.is_("state_portal_fetch", "null")  # IS NOT NULL
    .is_("uccs", "null")                     # IS NULL
    .limit(BATCH_SIZE)
    .execute()
)
# ---- helper functions
# -- select candiates for ucc search from sunbiz results
def select_sunbiz_candidates(candidates):
    return [
        c for c in candidates
        if c["score"] >= MIN_UCC_CONFIDENCE_SCORE
        and c["filing_number"]
    ]
# -- fetch filing details and return an analytics-ready UCC object
def enrich_ucc(ucc_number: str) -> dict | None:
    doc = search_ucc_fl_document(ucc_number)
    payload = doc.get("payload")

    if not payload:
        return None

    secured_parties = [
        {
            "name": s.get("name"),
            "city": s.get("city"),
            "state": s.get("state"),
        }
        for s in payload.get("secureds", [])
        if s.get("name")
    ]

    debtor_parties = [
        {
            "name": d.get("name"),
            "city": d.get("city"),
            "state": d.get("state"),
        }
        for d in payload.get("debtors", [])
        if d.get("name")
    ]

    secured_names = [s["name"] for s in secured_parties]
    is_mca_filing = any(mca(name) for name in secured_names)

    return {
        "ucc_number": ucc_number,
        "status": payload.get("status"),
        "file_date": payload.get("fileDate"),
        "expiration_date": payload.get("expirationDate"),
        "is_mca": is_mca_filing,
        "secured_parties": secured_parties,
        "debtors": debtor_parties,
    }

# ---- process companies
rows = companies.data or []

# ---- main loop
for row in rows:
    company_id = row["id"]
    company_name = row["name"]
    company_names = row["state_portal_fetch"]
    if isinstance(company_names, str):  # ensure company_names is not a string
        company_names = json.loads(company_names)
    # -- get names to search ucc portal with
    names_to_search = select_sunbiz_candidates(company_names)
    names_to_search.append({"entity_name": company_name})  # append the actual company name to the list
    print(f"[+] Resolving UCC for: {company_name} ({len(names_to_search)} candidates)")
    # -- search ucc portal
    all_uccs_raw = []

    for entity in names_to_search:
        uccs_raw = search_ucc_fl_all(entity["entity_name"])
        all_uccs_raw.extend(uccs_raw)

    uccs_final = []
    seen = set()

    for ucc in all_uccs_raw:
        ucc_number = ucc.get("ucc_number")

        if not ucc_number or ucc_number in seen:
            continue

        seen.add(ucc_number)

        enriched = enrich_ucc(ucc_number)
        if enriched:
            uccs_final.append(enriched)

    # ---- derive MCA flags
    mca_filings = [
        u for u in uccs_final
        if u.get("is_mca") and u.get("file_date")
    ]

    mca_taken = len(mca_filings) > 0

    mca_latest_date = None
    if mca_taken:
        mca_latest_date = max(
            u["file_date"][:10]  # ISO → YYYY-MM-DD
            for u in mca_filings
        )

    supabase.table("companies").update({
        "uccs": uccs_final or [],
        "mca_taken": mca_taken,
        "mca_taken_latest_date": mca_latest_date
    }).eq("id", company_id).execute()
