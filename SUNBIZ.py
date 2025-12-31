from playwright.sync_api import sync_playwright
from urllib.parse import quote
from rapidfuzz import fuzz
import re
import os
from dotenv import load_dotenv

load_dotenv()

PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USER = os.getenv("PROXY_USER")
PROXY_PASS = os.getenv("PROXY_PASS")


def fetch_sunbiz_results(entity_name: str, page: int = 1) -> list[dict]:
    encoded_name = quote(entity_name)

    url = (
        "https://search.sunbiz.org/Inquiry/CorporationSearch/"
        f"SearchResults/EntityName/{encoded_name}/Page{page}"
    )

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USER,
                "password": PROXY_PASS,
            },
        )

        page_obj = browser.new_page()
        page_obj.goto(url, wait_until="domcontentloaded", timeout=60000)

        # ✅ Wait for rows that match Sunbiz structure
        page_obj.wait_for_selector("tr td.large-width a", timeout=15000)

        rows = page_obj.query_selector_all("tr")

        for row in rows:
            name_td = row.query_selector("td.large-width a")
            filing_td = row.query_selector("td.medium-width")
            status_td = row.query_selector("td.small-width")

            if not name_td or not filing_td or not status_td:
                continue

            name = name_td.inner_text().strip()
            filing_number = filing_td.inner_text().strip()
            status = status_td.inner_text().strip()
            detail_url = "https://search.sunbiz.org" + name_td.get_attribute("href")

            results.append({
                "name": name,
                "filing_number": filing_number,
                "status": status,
                "detail_url": detail_url,
            })

        browser.close()

    return results

def normalize_name(name: str) -> str:
    """
    Normalize for scoring (remove punctuation, collapse spaces)
    """
    name = name.upper()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def token_count(name: str) -> int:
    return len(normalize_name(name).split())

def score_entity(search_term: str, entity_name: str) -> dict:
    search = normalize_name(search_term)
    target = normalize_name(entity_name)

    token_score = fuzz.token_set_ratio(search, target)
    partial_score = fuzz.partial_ratio(search, target)

    keyword_bonus = 10 if search in target else 0

    # ----------------------------
    # LENGTH / VERBOSITY PENALTY
    # ----------------------------
    search_len = len(search.split())
    target_len = len(target.split())

    extra_tokens = max(0, target_len - search_len)

    length_penalty = extra_tokens * 8  # tune: 4–8 works well

    final_score = int(
        (token_score * 0.6)
        + (partial_score * 0.4)
        + keyword_bonus
        - length_penalty
    )

    final_score = max(0, min(100, final_score))

    return {
        "token_score": token_score,
        "partial_score": partial_score,
        "bonus": keyword_bonus,
        "extra_tokens": extra_tokens,
        "length_penalty": length_penalty,
        "final_score": final_score,
    }


def rank_sunbiz_results(search_term: str, results: list[dict]) -> list[dict]:
    ranked = []

    for r in results:
        scores = score_entity(search_term, r["name"])
        ranked.append({
            **r,
            **scores,
        })

    return sorted(ranked, key=lambda x: x["final_score"], reverse=True)

def resolve_sunbiz_entities(query: str) -> dict:
    """
    Resolve a business name against Sunbiz.
    Always returns ALL entities with scores.

    Returns a structured response so downstream
    logic can decide what to do.
    """
    raw_results = fetch_sunbiz_results(query)
    ranked_results = rank_sunbiz_results(query, raw_results)

    return {
        "query": query,
        "total_results": len(ranked_results),
        "results": ranked_results
    }


  
if __name__ == "__main__":
    data = fetch_sunbiz_results("Costa Del Sol")
    ranked = rank_sunbiz_results("Costa Del Sol", data)

    for r in ranked:
        print(
            f"{r['name']:35} | score={r['final_score']} "
            f"(token={r['token_score']}, partial={r['partial_score']})"
        )
