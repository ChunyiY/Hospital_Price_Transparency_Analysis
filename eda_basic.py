# eda_basic.py
import pandas as pd

INP = "./processed/merged_standardized.parquet"  
USE_PARQUET = True

def load():
    return pd.read_parquet(INP) if USE_PARQUET else pd.read_csv(INP, low_memory=False)

def main():
    df = load()
    by_hosp = df.groupby("hospital").size().rename("rows").reset_index()
    print("\n[Rows by hospital]")
    print(by_hosp)

    price_cols = ["gross_charge","discounted_cash","negotiated_dollar","min_charge","max_charge","negotiated_percentage"]
    avail = [c for c in price_cols if c in df.columns]
    nn = df.groupby("hospital")[avail].apply(lambda g: g.notna().sum()).reset_index()
    print("\n[Non-null counts by hospital]")
    print(nn)

    if "code_type" in df.columns:
        ct = (df.groupby(["hospital","code_type"]).size()
                .rename("rows").reset_index()
                .sort_values(["hospital","rows"], ascending=[True,False]))
        print("\n[code_type distribution]")
        print(ct.head(30))

    by_hosp.to_csv("./processed/summary_rows_by_hospital.csv", index=False)
    nn.to_csv("./processed/summary_nonnull_by_hospital.csv", index=False)
    if "code_type" in df.columns:
        ct.to_csv("./processed/summary_code_type.csv", index=False)

if __name__ == "__main__":
    main()
