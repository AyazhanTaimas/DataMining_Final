import pandas as pd

INPUT_FILE = "hh_kz_sorted.csv"
OUTPUT_FILE = "hh_kz_preprocessed.csv"

# Курсы валют по состоянию на момент сбора (в KZT за единицу валюты)
CURRENCY_RATES = {
    "KZT": 1.0,
    "USD": 510.0,
    "EUR": 600.0,
    "RUR": 6.6,  # приближённый курс, российских рублей мало, но приводим к тенге
}

# Переводим период выплат к месячной норме
PERIOD_MULTIPLIERS = {
    "HOURLY": 160.0,  # 40 часов в неделю * 4 недели
    "DAILY": 22.0,  # среднее число рабочих дней в месяце
    "WEEKLY": 4.33,  # недель в месяце
    "MONTHLY": 1.0,
    "TWICE_PER_MONTH": 2.0,  # 2 выплаты в месяц ~= удвоение
}

# Используется чтобы перевести месячную зарплату к почасовой для новых колонок
MONTHLY_TO_HOURLY = 160.0


def convert_amount(amount: float, currency: str, period: str) -> float | None:
    """
    Переводит число из заданной валюты и периода в KZT/месяц.
    Народные значения периодов, не представленные в словаре, игнорируются.
    """
    if pd.isna(amount):
        return None
    rate = CURRENCY_RATES.get(currency)
    multiplier = PERIOD_MULTIPLIERS.get(period)
    if rate is None or multiplier is None:
        return None
    return float(amount) * rate * multiplier


def preprocess() -> None:
    # Загрузка и первичная подготовка
    df = pd.read_csv(INPUT_FILE, dtype=str)
    df["salary_from"] = pd.to_numeric(df["salary_from"], errors="coerce")
    df["salary_to"] = pd.to_numeric(df["salary_to"], errors="coerce")

    # Конвертация зарплат в KZT/месяц
    df["salary_from_kzt"] = df.apply(
        lambda row: convert_amount(row["salary_from"], row["currency"], row["payment_by"]),
        axis=1,
    )
    df["salary_to_kzt"] = df.apply(
        lambda row: convert_amount(row["salary_to"], row["currency"], row["payment_by"]),
        axis=1,
    )

    # Удаляем строки, где ни одно значение не конвертировалось
    df = df[df["salary_from_kzt"].notna() | df["salary_to_kzt"].notna()].copy()

    # Средняя зарплата (в KZT) по наличию одного или двух границ
    df["salary_avg_kzt"] = df[["salary_from_kzt", "salary_to_kzt"]].mean(axis=1)

    # Приводим к почасовой ставке
    df["salary_hourly_kzt"] = df["salary_avg_kzt"] / MONTHLY_TO_HOURLY

    # Удаляем оригинальные salary_from/ salary_to, оставляя нормализованные колонки
    df = df.drop(columns=["salary_from", "salary_to"])

    # Удаляем дубликаты и отрицательные значения
    df = df.drop_duplicates(subset="id")
    df = df[df["salary_avg_kzt"] > 0]

    # Сохраняем результат до этапа EDA
    df.to_csv(OUTPUT_FILE, index=False)
    print("Предобработанный набор данных сохранён в", OUTPUT_FILE)
    print("Размер набора:", df.shape)


if __name__ == "__main__":
    preprocess()
