import requests
import pandas as pd
import time
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# ================== CONFIG ==================
BASE_URL = "https://api.hh.ru/vacancies"
PER_PAGE = 100
TIMEOUT = 10
HEADERS = {"User-Agent": "hh-balanced-local"}

SEARCH_TEXTS = ["a", "e", "о", "1"]

# ⚠️ СТАРТУЕМ ТОЛЬКО С KZ
COUNTRIES = {
    "KZ": {"area": 40, "target": 1000},  # хватит для Data Mining
}

COUNTRY_NAMES = {
    "KZ": "Kazakhstan",
}

QUALITY_MIN_DESC = 150
MAX_ATTEMPTS = 100
CHECKPOINT_FILE = "hh_kz_checkpoint.csv"

# ================== DATE WINDOWS ==================
def generate_date_windows():
    today = datetime.today()
    windows = [
        (today - timedelta(days=7), today),
        (today - timedelta(days=14), today - timedelta(days=7)),
        (today - timedelta(days=30), today - timedelta(days=14)),
        (today - timedelta(days=60), today - timedelta(days=30)),
        (today - timedelta(days=120), today - timedelta(days=60)),
    ]
    return [(a.strftime("%Y-%m-%d"), b.strftime("%Y-%m-%d")) for a, b in windows]

# ================== HELPERS ==================
def clean_html(html):
    if not html:
        return None
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    return text if len(text) >= QUALITY_MIN_DESC else None

def fetch_page(area_id, text, page, date_from, date_to):
    params = {
        "area": area_id,
        "text": text,
        "date_from": date_from,
        "date_to": date_to,
        "page": page,
        "per_page": PER_PAGE,
        "order_by": "publication_time",
    }
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        return []
    return r.json().get("items", [])

def fetch_details_safe(vac_id):
    try:
        r = requests.get(f"{BASE_URL}/{vac_id}", headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None

        v = r.json()
        desc = clean_html(v.get("description"))
        if not desc:
            return None

        return {
            "description": desc,
            "key_skills": ", ".join(k["name"] for k in v.get("key_skills", [])),
            "address": (v.get("address") or {}).get("raw"),
        }
    except requests.RequestException:
        return None

# ================== CHECKPOINT ==================
def save_checkpoint(rows):
    if not rows:
        return
    pd.DataFrame(rows).to_csv(CHECKPOINT_FILE, index=False)
    print(f"💾 Checkpoint saved: {len(rows)} rows")

# ================== BATCH COLLECTOR ==================
def collect_batch_for_country(area_id, seen_ids):
    rows = []
    text_cycle = itertools.cycle(SEARCH_TEXTS)
    date_windows = generate_date_windows()

    for date_from, date_to in date_windows:
        for _ in range(len(SEARCH_TEXTS)):
            text = next(text_cycle)
            print(f"      🔎 text='{text}' | {date_from} → {date_to}")

            for page in range(3):
                items = fetch_page(area_id, text, page, date_from, date_to)
                if not items:
                    break

                print(f"         📄 page {page}, items={len(items)}")

                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {}
                    for item in items:
                        vid = item["id"]
                        if vid in seen_ids:
                            continue
                        seen_ids.add(vid)
                        futures[executor.submit(fetch_details_safe, vid)] = item

                    for fut in as_completed(futures):
                        details = fut.result()
                        if not details:
                            continue

                        salary = item.get("salary") or {}
                        employer = item.get("employer") or {}

                        rows.append({
                            "id": item["id"],
                            "name": item.get("name"),
                            "company": employer.get("name"),
                            "published_at": item.get("published_at"),
                            "url": item.get("alternate_url"),
                            "salary_from": salary.get("from"),
                            "salary_to": salary.get("to"),
                            "salary_currency": salary.get("currency"),
                            "experience": (item.get("experience") or {}).get("name"),
                            "employment": (item.get("employment") or {}).get("name"),
                            "schedule": (item.get("schedule") or {}).get("name"),
                            "city": item.get("area", {}).get("name"),
                            **details,
                        })

                time.sleep(0.3)

    print(f"      ✅ batch rows collected: {len(rows)}")
    return pd.DataFrame(rows)

# ================== QUALITY FILTER ==================
def is_quality(row):
    return isinstance(row["description"], str) and len(row["description"]) >= QUALITY_MIN_DESC

# ================== MAIN ==================
final_rows = []
total_seen = 0

try:
    for country, cfg in COUNTRIES.items():
        print(f"\n🌍 Collecting {country}")

        collected = 0
        attempts = 0
        seen_ids = set()

        while collected < cfg["target"] and attempts < MAX_ATTEMPTS:
            attempts += 1
            batch_df = collect_batch_for_country(cfg["area"], seen_ids)

            total_seen += len(batch_df)
            print(f"   🔍 seen raw: {total_seen}, quality: {collected}")
            print(f"   📦 Batch fetched: {len(batch_df)} rows")

            if batch_df.empty:
                continue

            added = 0
            for _, row in batch_df.iterrows():
                if is_quality(row):
                    row["country"] = country
                    row["country_name"] = COUNTRY_NAMES[country]
                    final_rows.append(row)
                    collected += 1
                    added += 1
                if collected >= cfg["target"]:
                    break

            print(f"   ➕ +{added}, всего: {collected}")
            save_checkpoint(final_rows)

        print(f"✅ {country}: {collected} (attempts={attempts})")

except KeyboardInterrupt:
    print("\n⛔ Остановка пользователем")
    save_checkpoint(final_rows)
    print("💾 Данные сохранены, можно идти в анализ")

# ================== FINAL SAVE ==================
final_df = pd.DataFrame(final_rows)
final_df.to_csv("hh_kz_FINAL.csv", index=False)
print("\n🎉 DONE: hh_kz_FINAL.csv")
