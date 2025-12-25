from pathlib import Path

import pandas as pd


def main() -> None:
    workdir = Path(__file__).resolve().parent
    merged_path = workdir / "hh_kz_combined.csv"
    csv_files = sorted(
        f
        for f in workdir.glob("hh_kz*.csv")
        if f.resolve() != merged_path.resolve()
    )
    outputs = []
    for csv_file in csv_files:
        outputs.append(pd.read_csv(csv_file, dtype=str, keep_default_na=False))

    if not outputs:
        raise SystemExit("no CSV files found to combine")

    combined = pd.concat(outputs, ignore_index=True, sort=False)

    if "id" not in combined.columns:
        raise SystemExit("missing `id` column in merged CSVs")
    combined = combined.drop_duplicates(subset="id", keep="first")
    combined.to_csv(workdir / "hh_kz_combined.csv", index=False)


if __name__ == "__main__":
    main()
