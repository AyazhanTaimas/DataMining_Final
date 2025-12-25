import requests
import csv
import os
from datetime import datetime, timedelta

# ---------------- НАСТРОЙКИ ----------------
BASE_URL = "https://api.hh.ru/vacancies"
OUTPUT_FILE = "hh_kz_daily.csv"
AREA_ID = 40             
PER_PAGE = 100
MAX_EMPTY_PAGES = 3       
# ------------------------------------------


def flatten(v):
    if isinstance(v, (dict, list)):
        return str(v)
    return v


def load_existing_ids():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}


def save_rows(rows):
    if not rows:
        return

    keys = set()
    for r in rows:
        keys.update(r.keys())

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sorted(keys))
        writer.writeheader()
        writer.writerows(rows)


def collect():
    seen_ids = load_existing_ids()
    all_rows = []

    today = datetime.utcnow().date()
    print(f"▶ Старт сбора с {today}")

    for days_back in range(0, 60):  
        date_from = today - timedelta(days=days_back)
        print(f"\n📅 Дата: {date_from}")

        page = 0
        empty_pages = 0

        while True:
            params = {
                "area": AREA_ID,
                "date_from": date_from.isoformat(),
                "page": page,
                "per_page": PER_PAGE
            }

            r = requests.get(BASE_URL, params=params)
            if r.status_code != 200:
                print("⚠️ HTTP", r.status_code)
                break

            items = r.json().get("items", [])
            if not items:
                break

            new_count = 0
            for it in items:
                if it["id"] not in seen_ids:
                    seen_ids.add(it["id"])
                    all_rows.append({k: flatten(v) for k, v in it.items()})
                    new_count += 1

            print(f"📦 +{new_count}")

            if new_count == 0:
                empty_pages += 1
            else:
                empty_pages = 0

            if empty_pages >= MAX_EMPTY_PAGES:
                print("⏹ Дальше нет новых вакансий — переход к следующему дню")
                break

            page += 1

        save_rows(all_rows)

    print(f"\n✅ Готово! Всего вакансий: {len(all_rows)}")


if __name__ == "__main__":
    collect()
