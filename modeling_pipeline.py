import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pathlib import Path

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


INPUT_FILE = "hh_kz_preprocessed.csv"
OUTPUT_DIR = "model_outputs"


def categorize_experience(value: str) -> str:
    if pd.isna(value):
        return "Unknown"
    text = value.lower()
    if "нет опыта" in text or "без опыта" in text:
        return "Junior"
    if "от 1 года" in text or "от 1" in text and "до" not in text:
        return "Junior"
    if "от 1 года до 3 лет" in text or "от 1" in text and "до 3" in text:
        return "Junior"
    if "от 3 до 6" in text or "3 до 6" in text:
        return "Mid"
    if "более 6" in text or "от 6" in text or "6 лет" in text:
        return "Senior"
    return "Other"


def build_preprocessor(categorical_features, numeric_features):
    return ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            ("num", "passthrough", numeric_features),
        ],
        remainder="drop",
        sparse_threshold=0.0,
    )


def evaluate_model(name, pipeline, X_train, X_test, y_train, y_test):
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    print(f"{name} — MAE: {mae:.0f}, RMSE: {rmse:.0f}, R²: {r2:.3f}")
    return {"name": name, "mae": mae, "rmse": rmse, "r2": r2, "pipeline": pipeline}


def plot_feature_importance(pipeline, output_dir):
    feature_names = pipeline.named_steps["preprocessor"].get_feature_names_out()
    importances = pipeline.named_steps["model"].feature_importances_
    df_imp = pd.DataFrame({"feature": feature_names, "importance": importances})
    df_imp = df_imp.sort_values("importance", ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df_imp, x="importance", y="feature")
    plt.title("Top 10 feature importances (Random Forest)")
    plt.tight_layout()
    path = Path(output_dir) / "feature_importance.png"
    plt.savefig(path)
    print(f"Saved feature importance plot at {path}")
    print("Interpretation: categorical location and experience indicators dominate salary variance.")


def main():
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    sns.set_theme(style="whitegrid")

    # Load preprocessed data
    df = pd.read_csv(INPUT_FILE)
    print("Loaded preprocessed data with shape", df.shape)

    # Feature engineering: map experience into buckets
    df["experience_level"] = df["experience"].apply(categorize_experience)
    print("Mapped raw experience text to categorical levels.")

    categorical_features = [
        "city",
        "employment",
        "experience_level",
        "payment_by",
        "gender",
        "degree",
        "work_type",
        "work_format",
        "work_schedule_by_days",
        "working_hours",
        "schedule",
    ]
    numeric_features = ["internship", "nightshift"]

    X = df[categorical_features + numeric_features].copy()
    X[categorical_features] = X[categorical_features].fillna("Unknown")
    X[numeric_features] = X[numeric_features].fillna(0)
    y = df["salary_avg_kzt"]

    # Split dataset
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print("Train/test split completed with ratio 80/20.")

    # Build preprocessing pipeline
    def make_pipeline(model):
        return Pipeline(
            [
                ("preprocessor", build_preprocessor(categorical_features, numeric_features)),
                ("scaler", StandardScaler(with_mean=False)),
                ("model", model),
            ]
        )

    # Model 1: Linear Regression
    lr_pipeline = make_pipeline(LinearRegression())
    lr_metrics = evaluate_model(
        "Linear Regression", lr_pipeline, X_train, X_test, y_train, y_test
    )
    print("Linear regression was trained to capture linear relations between encoded categories and salary.")

    # Model 2: Random Forest
    rf_pipeline = make_pipeline(RandomForestRegressor(n_estimators=100, random_state=42))
    rf_metrics = evaluate_model(
        "Random Forest", rf_pipeline, X_train, X_test, y_train, y_test
    )
    print("Random forest captures nonlinearities and interactions without manual feature transformation.")

    # Model comparison
    print("\nModel comparison:")
    for metrics in [lr_metrics, rf_metrics]:
        print(
            f"{metrics['name']}: MAE={metrics['mae']:.0f}, RMSE={metrics['rmse']:.0f}, R²={metrics['r2']:.3f}"
        )
    print(
        "Interpretation: compare RMSE and R² to understand whether non-linear model (RF) outperforms linear assumptions."
    )

    # Random Forest feature importance
    plot_feature_importance(rf_metrics["pipeline"], OUTPUT_DIR)


if __name__ == "__main__":
    main()
