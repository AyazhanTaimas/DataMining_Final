import ast
import json
import re
from typing import Any, Dict

import pandas as pd

INPUT_FILE = "hh_kz_combined.csv"
OUTPUT_FILE = "hh_kz_sorted.csv"


def parse_json(x: Any) -> Any:
    if isinstance(x, (dict, list)):
        return x
    if not isinstance(x, str) or not x:
        return {}

    for parser in (json.loads, ast.literal_eval):
        try:
            result = parser(x)
            if isinstance(result, (dict, list)):
                return result
        except (ValueError, SyntaxError, json.JSONDecodeError):
            continue

    return {}


def get(obj, path):
    for p in path.split("."):
        if isinstance(obj, list):
            obj = obj[0] if obj else None
        if not isinstance(obj, dict):
            return None
        obj = obj.get(p)
    return obj


df = pd.read_csv(INPUT_FILE, dtype=str, keep_default_na=False)


GENDER_REGEXPS = [
    (re.compile(r"(?:только|требуется|нужн[аяый]?|ищем|предпочтительно|предпочитаем|приоритет|рассматрива(?:ем|ются)?|подходят).*женщ", re.I), "female"),
    (re.compile(r"(?:для|требуется|нужен|нужна|нужны|ищем|предпочтительно|предпочитаем|приоритет|рассматрива(?:ем|ются)?|подходят).*женщ", re.I), "female"),
    (re.compile(r"женский\s+пол", re.I), "female"),
    (re.compile(r"\bfemale\b", re.I), "female"),
    (re.compile(r"(?:только|требуется|нужн[ый]?|ищем|предпочтительно|предпочитаем|приоритет|рассматрива(?:ем|ются)?|подходят).*мужч", re.I), "male"),
    (re.compile(r"(?:для|требуется|нужен|нужна|нужны|ищем|предпочтительно|предпочитаем|приоритет|рассматрива(?:ем|ются)?|подходят).*мужч", re.I), "male"),
    (re.compile(r"мужской\s+пол", re.I), "male"),
    (re.compile(r"\bmale\b", re.I), "male"),
]

DEGREE_KEYWORDS = [
    ("doctor", ("доктор", "phd")),
    ("master", ("магистр", "master")),
    ("bachelor", ("бакалавр", "bachelor")),
    ("specialist", ("специалитет", "specialist degree")),
    ("higher", ("высшее", "higher education", "higher professional", "higher technical", "higher specialized")),
]


def detect_gender(name: str, requirement: str) -> str:
    text = " ".join(filter(None, [name, requirement])).lower()
    for regexp, value in GENDER_REGEXPS:
        if regexp.search(text):
            return value
    return "any"


def detect_degree(requirement: str) -> str:
    if not requirement:
        return "any"
    text = requirement.lower()
    for label, keywords in DEGREE_KEYWORDS:
        for keyword in keywords:
            if keyword in text:
                return label
    return "any"

rows = []

for _, row in df.iterrows():
    address = parse_json(row.get("address"))
    employer = parse_json(row.get("employer"))
    salary = parse_json(row.get("salary"))
    salary_range = parse_json(row.get("salary_range"))
    schedule = parse_json(row.get("schedule"))
    snippet = parse_json(row.get("snippet"))
    employment = parse_json(row.get("employment"))
    experience = parse_json(row.get("experience"))
    work_format = parse_json(row.get("work_format"))
    work_schedule_by_days = parse_json(row.get("work_schedule_by_days"))
    working_hours = parse_json(row.get("working_hours"))
    vacancy_type = parse_json(row.get("type"))
    requirement_text = get(snippet, "requirement")

    rows.append({
        "id": row.get("id"),
        "vacancy": row.get("name"),

        "address": get(address, "raw"),
        "city": get(address, "city"),

        "url": row.get("alternate_url"),
        "published_at": row.get("published_at"),

        "employer": get(employer, "name"),
        "employer_id": get(employer, "id"),

        "employment": get(employment, "name"),
        "experience": get(experience, "name"),

        "internship": row.get("internship"),
        "nightshift": row.get("night_shifts"),

        "salary_from": get(salary, "from"),
        "salary_to": get(salary, "to"),
        "currency": get(salary, "currency"),

        "payment_by": get(salary_range, "frequency.id"),

        "schedule": get(schedule, "name"),
        "requirement": requirement_text,

        "gender": detect_gender(row.get("name"), requirement_text),
        "degree": detect_degree(requirement_text),

        "work_type": get(vacancy_type, "id"),
        "work_format": get(work_format, "id"),
        "work_schedule_by_days": get(work_schedule_by_days, "name"),
        "working_hours": get(working_hours, "id"),
    })

df_out = pd.DataFrame(rows)
df_out.to_csv("hh_kz_sorted.csv", index=False)

print("✅ Готово: hh_kz_sorted.csv создан")
