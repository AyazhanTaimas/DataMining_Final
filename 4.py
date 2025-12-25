import requests
import csv
import os
import json
from datetime import datetime, timedelta

BASE_URL = "https://api.hh.ru/vacancies"
OUTPUT = "hh_daily_kz.csv"
AREA_ID = 40          
PER_PAGE = 100


def flatten(v):
    return json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v


def load_existing_ids():
    if not os.path.exists(OUTPUT):
        return set()
    with open(OUTPUT, encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}


def save_rows(rows):
    if not rows:
        return
    exists = os.path.exists(OUTPUT)
    with open(OUTPUT, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def collect_by_day(days_back=60):
    seen_ids = load_existing_ids()
    print(f"▶ Уже собрано: {len(seen_ids)}")

    today = datetime.utcnow().date()

    for delta in range(days_back):
        day = today - timedelta(days=delta)
        print(f"\n📅 Сбор за {day}")

        page = 0
        new_rows = []

        while True:
            params = {
                "area": AREA_ID,
                "date_from": day.isoformat(),
                "date_to": day.isoformat(),
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

            for it in items:
                if it["id"] not in seen_ids:
                    seen_ids.add(it["id"])
                    new_rows.append({k: flatten(v) for k, v in it.items()})

            page += 1

        if new_rows:
            save_rows(new_rows)
            print(f"✅ {len(new_rows)} вакансий сохранено")
        else:
            print("ℹ️ новых вакансий нет")

    print("\n🎉 Готово! Все данные сохранены.")


if __name__ == "__main__":
    collect_by_day()
