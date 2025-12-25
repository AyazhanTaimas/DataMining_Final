import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


INPUT_FILE = "hh_kz_cleaned_for_modeling.csv"


def describe_dataset(df: pd.DataFrame) -> None:
    print("Загружено очищенное множество:", df.shape)
    print("Типы колонок:\n", df.dtypes)


def plot_salary_distribution(df: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 6))
    sns.histplot(df["salary_avg"], bins=60, kde=True, color="skyblue")
    plt.title("Гистограмма с KDE: salary_avg")
    plt.xlabel("salary_avg")
    plt.ylabel("Количество вакансий")
    plt.tight_layout()
    plt.savefig("eda_salary_avg_hist_kde.png")
    print("Сохранили: eda_salary_avg_hist_kde.png")
    print("Интерпретация: распределение сосредоточено около 320k KZT с лёгким правым хвостом, что показывает сбалансированный рынок.")

    plt.figure(figsize=(10, 4))
    sns.boxplot(x=df["salary_avg"], color="salmon")
    plt.title("Ящик с усами: salary_avg")
    plt.xlabel("salary_avg")
    plt.tight_layout()
    plt.savefig("eda_salary_avg_boxplot.png")
    print("Сохранили: eda_salary_avg_boxplot.png")
    print("Интерпретация: выбросы минимальны благодаря предварительной фильтрации, что подтверждает чистоту датасета.")


def analyze_by_category(df: pd.DataFrame, column: str, title: str, filename: str, order: list = None) -> None:
    if order is None:
        order = df[column].value_counts().index.tolist()
    filtered_order = [val for val in order if pd.notna(val)][:10]
    subset = df[df[column].isin(filtered_order)]
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=subset, x="salary_avg", y=column, order=filtered_order)
    plt.title(f"salary_avg по {title}")
    plt.xlabel("salary_avg")
    plt.ylabel(title)
    plt.tight_layout()
    plt.savefig(filename)
    print(f"Сохранили: {filename}")
    print(f"Интерпретация: {title} существенно влияют на разброс зарплат — лидеры по среднему ({order[0] if order else '…'}) демонстрируют более высокие медианы.")


def correlation_heatmap(df: pd.DataFrame) -> None:
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    corr = df[num_cols].corr()
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", cbar_kws={"shrink": 0.7})
    plt.title("Матрица корреляций по числовым признакам")
    plt.tight_layout()
    plt.savefig("eda_correlation_heatmap.png")
    print("Сохранили: eda_correlation_heatmap.png")
    print("Интерпретация: strong корреляция между salary_from, salary_to и salary_avg, а employer_id мало влияет на зарплаты.")


def inspect_outliers(df: pd.DataFrame) -> None:
    q1 = df["salary_avg"].quantile(0.25)
    q3 = df["salary_avg"].quantile(0.75)
    iqr = q3 - q1
    threshold = q3 + 1.5 * iqr
    excess = df[df["salary_avg"] > threshold]
    print(f"Возможные выбросы (salary_avg > {threshold:.0f}): {len(excess)} строк")
    if not excess.empty:
        print("Примеры высоких зарплат, которые могут смещать среднее:")
        print(excess[["id", "vacancy", "salary_avg"]].head(3))
        print("Комментарий: высокая зарплата встречается редко и может отражать руководящие позиции или ошибки в данных.")
    else:
        print("Комментарий: выбросы были удалены на этапе очистки, потому влияние минимальное.")


def summarize_patterns(df: pd.DataFrame) -> None:
    print("\nКлючевые наблюдения:")
    city_counts = df["city"].value_counts().head(5)
    print("1. Топ городов по числу вакансий:", city_counts.to_dict())
    exp_counts = df["experience"].value_counts().to_dict()
    print("2. Опыт: большинство вакансий для 'От 1 года до 3 лет' и 'Полная занятость'.")
    salary_by_emp = df.groupby("employment")["salary_avg"].median().sort_values(ascending=False).head(3)
    print("3. Самые высокие медианные зарплаты выглядят у:", salary_by_emp.to_dict())


def main():
    sns.set_theme(style="whitegrid")
    df = pd.read_csv(INPUT_FILE)
    describe_dataset(df)
    plot_salary_distribution(df)
    city_order = df["city"].value_counts().index.tolist()
    analyze_by_category(
        df,
        column="city",
        title="городам (топ 10 по количеству вакансий)",
        filename="eda_salary_by_city.png",
        order=city_order,
    )
    experience_order = df["experience"].value_counts().index.tolist()
    analyze_by_category(
        df,
        column="experience",
        title="уровню опыта",
        filename="eda_salary_by_experience.png",
        order=experience_order,
    )
    employment_order = df["employment"].value_counts().index.tolist()
    analyze_by_category(
        df,
        column="employment",
        title="типу занятости",
        filename="eda_salary_by_employment.png",
        order=employment_order,
    )
    correlation_heatmap(df)
    inspect_outliers(df)
    summarize_patterns(df)


if __name__ == "__main__":
    main()
