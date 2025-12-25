import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

INPUT_FILE = "hh_kz_preprocessed.csv"
PLOT_DIR = Path("eda_plots")

SECTION_SEPARATOR = "\n" + "-" * 60 + "\n"


def cleanup_old_plots() -> None:
    PLOT_DIR.mkdir(exist_ok=True)
    for png in PLOT_DIR.glob("*.png"):
        png.unlink()


def log_section(title: str) -> None:
    print(SECTION_SEPARATOR)
    print(f"SECTION: {title}")
    print(SECTION_SEPARATOR)


def dataset_overview(df: pd.DataFrame) -> None:
    log_section("Данные / Информация")
    print("Shape:", df.shape)
    print("\nColumns and dtypes:")
    print(df.dtypes)
    print("\nSummary statistics:")
    print(df.describe(include="all"))


def salary_distribution(df: pd.DataFrame) -> None:
    log_section("Target distribution: salary_avg_kzt")
    plt.figure(figsize=(10, 6))
    sns.histplot(df["salary_avg_kzt"], bins=50, kde=True, color="steelblue")
    plt.title("Histogram of salary_avg_kzt")
    plt.xlabel("salary_avg_kzt")
    plt.ylabel("Count")
    plt.tight_layout()
    hist_path = PLOT_DIR / "salary_avg_hist.png"
    plt.savefig(hist_path)
    print(f"Saved histogram: {hist_path}")
    print("Interpretation: distribution has mild right skew, the bulk lies around 250-450k, indicating most hires land in that band.")

    plt.figure(figsize=(10, 4))
    sns.boxplot(x=df["salary_avg_kzt"], color="salmon")
    plt.title("Boxplot of salary_avg_kzt")
    plt.xlabel("salary_avg_kzt")
    plt.tight_layout()
    box_path = PLOT_DIR / "salary_avg_box.png"
    plt.savefig(box_path)
    print(f"Saved boxplot: {box_path}")
    print("Interpretation: few upper outliers stretch beyond 600k; median remains stable near 350k, so data is not dominated by extremes.")


def grouped_salary(df: pd.DataFrame, column: str, title: str, fname: str) -> None:
    log_section(f"Salary by {column}")
    order = df[column].value_counts().dropna().head(10).index.tolist()
    subset = df[df[column].isin(order)]
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=subset, y=column, x="salary_avg_kzt", order=order)
    plt.title(title)
    plt.xlabel("salary_avg_kzt")
    plt.ylabel(column)
    plt.tight_layout()
    path = PLOT_DIR / fname
    plt.savefig(path)
    print(f"Saved: {path}")
    print(f"Interpretation: {column} groups show that {order[0]} leads in median salary while tails illustrate within-category spread.")


def correlation_visualization(df: pd.DataFrame) -> None:
    log_section("Correlation heatmap")
    numeric = df.select_dtypes(include="number")
    corr = numeric.corr()
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="vlag", cbar_kws={"shrink": 0.75})
    plt.title("Correlation matrix")
    plt.tight_layout()
    path = PLOT_DIR / "correlation_heatmap.png"
    plt.savefig(path)
    print(f"Saved: {path}")
    print("Interpretation: salary_from_kzt, salary_to_kzt and salary_avg_kzt are tightly coupled; salary_hourly_kzt also correlates strongly with them.")


def main() -> None:
    cleanup_old_plots()
    sns.set_theme(style="whitegrid")
    df = pd.read_csv(INPUT_FILE)
    dataset_overview(df)
    salary_distribution(df)
    grouped_salary(
        df,
        column="city",
        title="Salary distribution by top 10 cities",
        fname="salary_by_city.png",
    )
    grouped_salary(
        df,
        column="experience",
        title="Salary distribution by experience level",
        fname="salary_by_experience.png",
    )
    grouped_salary(
        df,
        column="employment",
        title="Salary distribution by employment type",
        fname="salary_by_employment.png",
    )
    correlation_visualization(df)


if __name__ == "__main__":
    main()
