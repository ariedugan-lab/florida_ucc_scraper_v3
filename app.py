from UCC import find_ucc_matches
from SUNBIZ import resolve_sunbiz_entities
from dotenv import load_dotenv
from supabase import create_client, Client
import time
import os

# -- configs
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
AUTO_THRESHOLD = 90
LIKELY_THRESHOLD = 75
REVIEW_THRESHOLD = 70

# -- supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

companies = (
    supabase
    .table("companies")
    .select("id, name, city")
    .eq("state", "FL")
    .is_("state_portal_fetch", None)
    .limit(BATCH_SIZE)
    .execute()
)

# -- process companies
rows = companies.data or []

# -- main loop
for row in rows:
    company_id = row["id"]
    company_name = row["name"]

    print(f"[+] Resolving Sunbiz for: {company_name}")
    time.sleep(5)  # to avoid rate limiting
    response = resolve_sunbiz_entities(company_name)
    results = response["results"]

    # build flat sunbiz payload (ALL results)
    try:
        if results:
            sunbiz_payload = [
                {
                    "entity_name": r["name"],
                    "filing_number": r["filing_number"],
                    "status": r["status"],
                    "score": r["final_score"],
                    "detail_url": r["detail_url"],
                }
                for r in results
            ]
        else:
            sunbiz_payload = []
    except Exception as e:
        print(f"[!] Error building sunbiz payload: {e}")
        sunbiz_payload = []

    # update company row
    supabase.table("companies").update(
        {
            "state_portal_fetch": sunbiz_payload
        }
    ).eq("id", company_id).execute()
