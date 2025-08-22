# save_as_parquet.py
import os
import pandas as pd

def merge_and_save_parquet(dfs, out_parquet):
    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)
    dfs = [df for df in dfs if not df.dropna(how="all").empty]
    merged = pd.concat(dfs, ignore_index=True)
    for col in ["hospital","code","code_type","payer_name","plan_name","billing_class","setting"]:
        if col in merged.columns:
            merged[col] = merged[col].astype("category")
    merged.to_parquet(out_parquet, index=False)
    return merged

if __name__ == "__main__":
    pass
