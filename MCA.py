import csv
import re
from rapidfuzz import fuzz

# ---- configs
CSV_PATH = "./MCAs.csv"

# ---- helper functions
# -- normalize a name string for matching
def normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()

# -- load all MCA funder names from CSV
def load_all_mca_names(csv_path=CSV_PATH):
    names = set()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # add company name
            company = row.get("Company Name")
            if company and company.strip():
                names.add(normalize(company))

            # add all alias columns
            for key, value in row.items():
                if "alias" in key.lower():
                    if value and value.strip():
                        names.add(normalize(value))

    return sorted(names)

# -- check if a secured party name matches any known MCA funder names
def is_mca_funder(
    secured_party_name: str,
    mca_names: list[str],
    threshold: int = 90
) -> bool:
    if not secured_party_name:
        return False

    name_norm = normalize(secured_party_name)

    for alias in mca_names:
        # fast exact / substring check first (cheap)
        if alias in name_norm:
            return True

        # fallback to fuzzy match
        score = fuzz.partial_ratio(name_norm, alias)
        if score >= threshold:
            return True

    return False

# -- wrapper function
ALL_MCA_NAMES = load_all_mca_names()
def mca(mca_name):
    test = is_mca_funder(mca_name, ALL_MCA_NAMES)
    return test