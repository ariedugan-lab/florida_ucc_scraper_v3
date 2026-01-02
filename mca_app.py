from UCC import search_ucc_fl_document
from MCA import mca
from dotenv import load_dotenv
import os
from supabase import create_client, Client
import time
import json

# ---- configs
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BATCH_SIZE = int(os.getenv("BATCH_SIZE_MCA", "100"))
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- supabase companies querys
# -- only companies with uccs
companies = (
    supabase
    .table("companies")
    .select("id, name, uccs")
    .eq("state", "FL")
    .not_.is_("uccs", "null")              # uccs IS NOT NULL
    .filter("uccs", "neq", "[]")           # uccs != []
    .limit(BATCH_SIZE)
    .execute()
)

# ---- helper functions
# -- extract secured party names from UCC filing details payload
def extract_secured_names(payload: dict) -> list[str]:
    secureds = payload.get("secureds", [])

    names = []
    for s in secureds:
        name = s.get("name")
        if name:
            names.append(name)

    return names


# ---- process companies
rows = companies.data or []

# ---- iterate companies
for row in rows:
    company_id = row["id"]
    company_name = row["name"]
    uccs = row["uccs"]
    for ucc in uccs:
        filing_number = ucc.get("ucc_number")
        filing_details = search_ucc_fl_document(filing_number)
        secured_names = extract_secured_names(filing_details.get("payload", {}))
        is_mca_filing = False
        for secured_name in secured_names:
            if mca(secured_name):
                is_mca_filing = True
                break

        if is_mca_filing:
            print(f"[+] Company {company_name} has MCA secured party in UCC {filing_number}")
        else:
            print(f"[-] {filing_number} was not an MCA filing")

