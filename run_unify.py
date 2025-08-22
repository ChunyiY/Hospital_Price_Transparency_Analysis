# -*- coding: utf-8 -*-
import os
import pandas as pd
from unify_schema import normalize_one_file, merge_and_save
from save_parquet import merge_and_save_parquet
DATA_FILES = [
    # (path, hospital_name, header_index)
    ("/Users/chunyiyang/hospital-price-transparency/data/520591656_JohnsHopkinsHospital_standardcharges.csv",                "Johns Hopkins Hospital", 2),
    ("/Users/chunyiyang/hospital-price-transparency/data/860800150_mayo-clinic-arizona_standardcharges.csv",       "Mayo Clinic Arizona",    2),
    ("/Users/chunyiyang/hospital-price-transparency/data/135564934_mount-sinai-brooklyn_standardcharges.csv", "Mount Sinai Brooklyn", 2),
    ("/Users/chunyiyang/hospital-price-transparency/data/340714585_the-cleveland-clinic-foundation_standardcharges.csv",   "Cleveland Clinic",       2),
]

OUT_CSV = "./processed/merged_standardized.csv"

def main():
    normalized_list = []
    for path, hosp, hdr in DATA_FILES:
        print(f"[+] Normalizing: {hosp}  <- {path}")
        df_std = normalize_one_file(path=path, hospital_name=hosp, header=hdr)
        print(f"    Rows: {len(df_std)}, Columns: {len(df_std.columns)}")

        price_cols = ["gross_charge","discounted_cash","negotiated_dollar","min_charge","max_charge"]
        present = [c for c in price_cols if c in df_std.columns]
        if present:
            nonnull = df_std[present].notna().sum().to_dict()
            print(f"    Non-null price cells: {nonnull}")
        normalized_list.append(df_std)

    merged = merge_and_save(normalized_list, OUT_CSV)
    print(f"\n[✓] Merged saved to: {OUT_CSV}")
    print(f"    Total rows: {len(merged)}")
    
    OUT_PARQUET = "./processed/merged_standardized.parquet"
    merged = merge_and_save_parquet(normalized_list, OUT_PARQUET)
    print(f"[✓] Parquet saved to: {OUT_PARQUET}, rows={len(merged)}")

    preview_cols = ["hospital","description","code","code_type","gross_charge","discounted_cash","negotiated_dollar","min_charge","max_charge","payer_name","plan_name"]
    preview_cols = [c for c in preview_cols if c in merged.columns]
    print("\n[Preview]")
    print(merged[preview_cols].head(5).to_string(index=False))

if __name__ == "__main__":
    main()



