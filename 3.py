import requests
import csv
import os
from datetime import datetime, timedelta

BASE_URL = "https://api.hh.ru/vacancies"
OUTPUT = "hh_daily.csv"
AREA_ID = 40
PER_PAGE = 100


def flatten(v):
    return v if not isinstance(v, (dict, list)) else str(v)


def save_rows(rows):
    if not rows:
        return
    keys = set()
    for r in rows:
        keys.update(r.keys())
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sorted(keys))
        writer.writeheader()
        writer.writerows(rows)


def collect():
    today = datetime.utcnow().date()
    date_from = today - timedelta(days=2)

    rows = []

    page = 0
    while True:
        params = {
            "area": AREA_ID,
            "date_from": date_from.isoformat(),
            "page": page,
            "per_page": PER_PAGE,
        }

        r = requests.get(BASE_URL, params=params)
        if r.status_code != 200:
            print("❌ HTTP", r.status_code)
            break

        items = r.json().get("items", [])
        if not items:
            break

        for it in items:
            rows.append({k: flatten(v) for k, v in it.items()})

        print(f"📦 Получено: {len(rows)}")
        page += 1

    save_rows(rows)
    print(f"✅ Готово! Всего {len(rows)} вакансий")


if __name__ == "__main__":
    collect()
