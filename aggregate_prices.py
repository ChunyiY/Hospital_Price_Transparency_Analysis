# aggregate_prices.py
import pandas as pd
import numpy as np

INP = "./processed/merged_standardized.parquet"
OUT = "./processed/agg_by_hosp_code.parquet"

PRICE_COLS = ["gross_charge","discounted_cash","negotiated_dollar","min_charge","max_charge"]

def main():
    df = pd.read_parquet(INP)
    keep = ["hospital","code","code_type","description"] + [c for c in PRICE_COLS if c in df.columns]
    df = df[keep].copy()

    df["description"] = df["description"].fillna(method="ffill")

    agg = (df.groupby(["hospital","code","code_type"])
             .agg({c:"median" for c in PRICE_COLS if c in df.columns})
             .reset_index())

    if set(["gross_charge","discounted_cash"]).issubset(agg.columns):
        agg["cash_vs_gross_pct"] = 100.0 * (agg["discounted_cash"] / agg["gross_charge"])
    if set(["min_charge","max_charge"]).issubset(agg.columns):
        agg["range_width"] = agg["max_charge"] - agg["min_charge"]

    agg.to_parquet(OUT, index=False)
    print(f"[✓] Aggregated saved: {OUT}, rows={len(agg)}")
    print(agg.head(10))

if __name__ == "__main__":
    main()
