import requests
import csv
import json
import os
import time
from datetime import datetime, timedelta

BASE_URL = "https://api.hh.ru/vacancies"
HEADERS = {"User-Agent": "Mozilla/5.0"}

OUTPUT_FILE = "hh_kz_data.csv"
TARGET = 50_000       
PER_PAGE = 100
SLEEP = 0.4            
AREA_ID = 40          



def flatten(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def load_existing():
    if not os.path.exists(OUTPUT_FILE):
        return [], set()

    with open(OUTPUT_FILE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    ids = {r["id"] for r in rows if "id" in r}
    return rows, ids


def save_all(rows):
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
    rows, seen_ids = load_existing()
    print(f"▶ Уже собрано: {len(rows)}")

    end = datetime.utcnow()
    start = end - timedelta(days=7)

    while len(rows) < TARGET:
        print(f"\n⏱ Период: {start.date()} → {end.date()}")

        page = 0
        while True:
            params = {
                "area": AREA_ID,
                "page": page,
                "per_page": PER_PAGE,
                "date_from": start.isoformat(),
            }

            r = requests.get(BASE_URL, params=params, headers=HEADERS)

            if r.status_code == 400:
                print("⚠️ 400 — уменьшаем окно")
                break

            if r.status_code == 429:
                print("⏳ 429 Too Many Requests — жду 60 сек")
                time.sleep(60)
                continue

            if r.status_code != 200:
                print("❌ Ошибка:", r.status_code)
                break

            items = r.json().get("items", [])
            if not items:
                break

            new = 0
            for it in items:
                if it["id"] not in seen_ids:
                    seen_ids.add(it["id"])
                    rows.append({k: flatten(v) for k, v in it.items()})
                    new += 1

            print(f"📦 +{new} | всего: {len(rows)}")

            if len(rows) >= TARGET:
                break

            page += 1
            time.sleep(SLEEP)

        save_all(rows)

        end = start
        start -= timedelta(days=7)

    print(f"\n✅ ГОТОВО! Всего сохранено: {len(rows)}")


if __name__ == "__main__":
    try:
        collect()
    except KeyboardInterrupt:
        print("\n⛔ Остановка пользователем — сохраняем...")
