import requests
import sqlite3
import json
import csv
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.hh.ru/vacancies"
HEADERS = {"User-Agent": "Mozilla/5.0 (DataMining; contact: you@example.com)"}

AREA_ID = 40          
TARGET = 10_000       
PER_PAGE = 100
SLEEP = 0.35

DB_PATH = "hh_kz.db"
CSV_PATH = "hh_kz_export.csv"

STOP = False


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def setup_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            id TEXT PRIMARY KEY,
            payload TEXT NOT NULL
        )
    """)
    con.commit()
    return con


def db_count(con) -> int:
    (n,) = con.execute("SELECT COUNT(*) FROM vacancies").fetchone()
    return int(n)


def upsert_many(con, items):
    cur = con.cursor()
    inserted = 0
    for it in items:
        vid = str(it.get("id"))
        if not vid:
            continue
        payload = json.dumps(it, ensure_ascii=False)
        try:
            cur.execute("INSERT INTO vacancies (id, payload) VALUES (?, ?)", (vid, payload))
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    con.commit()
    return inserted


def request_page(date_from: datetime, date_to: datetime, page: int):
    params = {
        "area": AREA_ID,
        "per_page": PER_PAGE,
        "page": page,
        "date_from": iso(date_from),
        "date_to": iso(date_to),
        "order_by": "publication_time",
    }
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=25)
    return r


def window_saturated(first_page_json: dict) -> bool:
    found = first_page_json.get("found", 0)
    if isinstance(found, int) and found > 2000:
        return True
    if isinstance(found, int) and found == 2000:
        return True
    pages = first_page_json.get("pages", 0)
    if isinstance(pages, int) and pages >= 20:
        return True
    return False


def export_csv(con):
    rows = con.execute("SELECT payload FROM vacancies").fetchall()
    dicts = [json.loads(p[0]) for p in rows]

    keys = set()
    for d in dicts:
        keys.update(d.keys())
    fieldnames = sorted(keys)

    def flatten(v):
        return json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for d in dicts:
            w.writerow({k: flatten(d.get(k)) for k in fieldnames})

    print(f"✅ Экспортировано в CSV: {CSV_PATH} | строк: {len(dicts)} | колонок: {len(fieldnames)}")


def handle_sigint(signum, frame):
    global STOP
    STOP = True
    print("\n⛔ Ctrl+C пойман. Останавливаюсь аккуратно: сохраню всё в SQLite и сделаю экспорт... (жми ещё раз чтобы форс)")
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(1))


def collect():
    global STOP
    signal.signal(signal.SIGINT, handle_sigint)

    con = setup_db()
    print(f"▶ Уже в базе: {db_count(con)}")

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)

    stack = [(start, now)]

    try:
        while stack and not STOP and db_count(con) < TARGET:
            date_from, date_to = stack.pop()

            if (date_to - date_from) < timedelta(hours=6):
                pass

            r0 = request_page(date_from, date_to, 0)
            if r0.status_code == 429:
                print("⚠️ 429 Too Many Requests — жду 60 сек")
                time.sleep(60)
                stack.append((date_from, date_to))
                continue
            if r0.status_code == 403:
                print("⚠️ 403 Forbidden — жду 120 сек (часто временный бан)")
                time.sleep(120)
                stack.append((date_from, date_to))
                continue
            if r0.status_code != 200:
                print(f"⚠️ HTTP {r0.status_code} на окне {date_from.date()} → {date_to.date()} | пропускаю")
                continue

            data0 = r0.json()

            if window_saturated(data0) and (date_to - date_from) > timedelta(hours=6):
                mid = date_from + (date_to - date_from) / 2
                stack.append((date_from, mid))
                stack.append((mid, date_to))
                continue

            pages = int(data0.get("pages", 0))
            items0 = data0.get("items", [])
            inserted = upsert_many(con, items0)

            for page in range(1, pages):
                if STOP or db_count(con) >= TARGET:
                    break
                rp = request_page(date_from, date_to, page)
                if rp.status_code != 200:
                    print(f"⚠️ HTTP {rp.status_code} на page={page} окна {date_from.date()}→{date_to.date()} | стоп окна")
                    break
                dp = rp.json()
                inserted += upsert_many(con, dp.get("items", []))
                time.sleep(SLEEP)

            total = db_count(con)
            print(f"⏱ {date_from.date()} → {date_to.date()} | +{inserted} | всего: {total}/{TARGET}")
            time.sleep(SLEEP)

    finally:
        total = db_count(con)
        print(f"💾 В базе сейчас: {total}")
        export_csv(con)
        con.close()


if __name__ == "__main__":
    collect()
