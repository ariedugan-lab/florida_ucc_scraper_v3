import requests
from urllib.parse import quote_plus
from rapidfuzz import fuzz
import re
import os
from dotenv import load_dotenv

load_dotenv()
HTTP_PROXY = os.getenv("OXY_HTTP_PROXY")
HTTPS_PROXY = os.getenv("OXY_HTTPS_PROXY")

PROXIES = {
    "http": HTTP_PROXY,
    "https": HTTPS_PROXY,
}

API_URL = "https://publicsearchapi.floridaucc.com/search"

LEGAL_SUFFIXES = {
    "llc", "l.l.c", "inc", "incorporated", "corp", "corporation",
    "co", "company", "ltd", "limited", "pllc", "lp", "llp"
}

def normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^\w\s]", " ", name)  # remove punctuation
    tokens = name.split()
    tokens = [t for t in tokens if t not in LEGAL_SUFFIXES]
    return " ".join(tokens)


def score_match(
    source_name: str,
    debtor_name: str,
    source_city: str | None = None,
    debtor_city: str | None = None,
):
    src = normalize_name(source_name)
    deb = normalize_name(debtor_name)

    scores = {
        "token_set": fuzz.token_set_ratio(src, deb),
        "token_sort": fuzz.token_sort_ratio(src, deb),
        "partial": fuzz.partial_ratio(src, deb),
    }

    # Weighted average (tuned for UCCs)
    name_score = (
        scores["partial"] * 0.5 +
        scores["token_set"] * 0.3 +
        scores["token_sort"] * 0.2
    )

    city_bonus = 0
    if source_city and debtor_city:
        if source_city.strip().lower() == debtor_city.strip().lower():
            city_bonus = 10

    penalty = positional_match_penalty(source_name, debtor_name)
    total = min(100, round(name_score + city_bonus + penalty, 1))


    return {
        "normalized_source": src,
        "normalized_debtor": deb,
        "name_score": round(name_score, 1),
        "city_bonus": city_bonus,
        "score": total,
        "components": scores,
    }

def normalize_letters(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())

def positional_letter_match_ratio(a: str, b: str) -> float:
    a = normalize_letters(a)
    b = normalize_letters(b)

    if not a:
        return 0.0

    matches = sum(1 for i, ch in enumerate(a) if i < len(b) and ch == b[i])
    return matches / len(a)

def positional_match_penalty(src: str, deb: str) -> int:
    src_head = src.split()[0]
    deb_head = deb.split()[0]
    ratio = positional_letter_match_ratio(src_head, deb_head)

    # Only penalize high accidental similarity
    if ratio >= 0.9:
        return -10
    if ratio >= 0.75:
        return -8
    if ratio >= 0.6:
        return -6
    if ratio >= 0.45:
        return -4
    if ratio >= 0.3:
        return -2

    return 0


def search_ucc_fl(query: str) -> list[dict]:
    if not query:
        raise ValueError("query is required")

    params = {
        "rowNumber": "",
        "text": quote_plus(query),
        "searchOptionType": "OrganizationDebtorName",
        "searchOptionSubOption": "FiledCompactDebtorNameList",
        "searchCategory": "Standard",
    }

    headers = {
        "User-Agent": "PostmanRuntime/7.51.0",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    resp = requests.get(API_URL, params=params, headers=headers, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    payload = data.get("payload", {})
    debtors = payload.get("debtors", [])

    results = []

    for d in debtors:
        results.append({
            "rowNumber": d.get("rowNumber"),
            "debtor_name": d.get("name"),
            "ucc_number": d.get("uccNumber"),
            "address": d.get("address"),
            "city": d.get("city"),
            "state": d.get("state"),
            "zip": d.get("zipCode"),
            "status": d.get("status"),
        })

    return results

def find_ucc_matches(query_name: str, query_city: str | None = None) -> list[dict]:
    results = search_ucc_fl(query_name)
    scored = []

    for r in results:
        s = score_match(
            query_name,
            r["debtor_name"],
            query_city,
            r["city"],
        )

        scored.append({
            "debtor_name": r["debtor_name"],
            "ucc_number": r["ucc_number"],
            "score": s["score"],
            "city": r["city"],
            "state": r["state"],
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored
