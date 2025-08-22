# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from typing import Dict, List, Optional

STANDARD_COLUMNS: List[str] = [
    "hospital",               
    "facility_id",            
    "description",
    "code",
    "code_type",
    "billing_class",
    "setting",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "modifiers",
    "gross_charge",
    "discounted_cash",
    "negotiated_dollar",
    "negotiated_percentage", 
    "negotiated_algorithm",
    "estimated_amount",
    "min_charge",
    "max_charge",
    "payer_name",
    "plan_name",
    "methodology",
    "additional_notes",
    "additional_payer_notes",  
    "count_of_compared_rates", 
    "footnote"                
]


COMMON_RENAMES: Dict[str, str] = {
    "description": "description",
    "billing_class": "billing_class",
    "setting": "setting",
    "drug_unit_of_measurement": "drug_unit_of_measurement",
    "drug_type_of_measurement": "drug_type_of_measurement",
    "modifiers": "modifiers",


    "standard_charge|gross": "gross_charge",
    "standard_charge|discounted_cash": "discounted_cash",
    "standard_charge|negotiated_dollar": "negotiated_dollar",
    "standard_charge|negotiated_percentage": "negotiated_percentage",
    "standard_charge|negotiated_algorithm": "negotiated_algorithm",
    "standard_charge|min": "min_charge",
    "standard_charge|max": "max_charge",
    "estimated_amount": "estimated_amount",


    "payer_name": "payer_name",
    "plan_name": "plan_name",


    "standard_charge|methodology": "methodology",
    "additional_generic_notes": "additional_notes",
    "additional_payer_notes": "additional_payer_notes",


    "facility_id": "facility_id",
    "count_of_compared_rates": "count_of_compared_rates",
    "footnote": "footnote",
}

CODE_COLUMNS = ["code|1", "code|2", "code|3", "code|4"]
CODE_TYPE_COLUMNS = ["code|1|type", "code|2|type", "code|3|type", "code|4|type"]

MONEY_COLS = ["gross_charge", "discounted_cash", "negotiated_dollar", "estimated_amount", "min_charge", "max_charge"]
PCT_COLS   = ["negotiated_percentage"]


def read_csv_third_row_header(path: str) -> pd.DataFrame:

    return pd.read_csv(path, header=2, low_memory=False)


def _first_non_null(series_list: List[pd.Series]) -> pd.Series:

    out = pd.Series([None] * len(series_list[0]), index=series_list[0].index, dtype="object")
    for s in series_list:
        mask = out.isna() & s.notna()
        out.loc[mask] = s[mask]
    return out


def _clean_money_to_float(s: pd.Series) -> pd.Series:

    if s is None or s.dtype == float:
        return s
    return (
        s.astype(str)
         .str.replace(r"[,$\s]", "", regex=True)
         .str.replace(r"[^\d\.\-eE]", "", regex=True)  # 保守去杂
         .replace({"": None})
         .astype(float)
    )


def _clean_percent_to_float(s: pd.Series) -> pd.Series:
    if s is None:
        return s
    return (
        s.astype(str)
         .str.strip()
         .str.replace("%", "", regex=False)
         .replace({"": None})
         .astype(float)
    )


def _strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": None, "None": None})
    return df


def normalize_one_file(
    path: str,
    hospital_name: str,
    header: Optional[int] = 2
) -> pd.DataFrame:
    if header is None:
        raw = pd.read_csv(path, low_memory=False)
    else:
        raw = pd.read_csv(path, header=header, low_memory=False)

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    out = pd.DataFrame(index=df.index)

    out["hospital"] = hospital_name

    available_map = {k: v for k, v in COMMON_RENAMES.items() if k in df.columns}
    tmp = df.rename(columns=available_map)

    for col in set(available_map.values()):
        out[col] = tmp[col]

    have_codes = [c for c in CODE_COLUMNS if c in df.columns]
    if have_codes:
        out["code"] = _first_non_null([df[c] for c in have_codes])
    else:
        out["code"] = None

    have_code_types = [c for c in CODE_TYPE_COLUMNS if c in df.columns]
    if have_code_types:
        out["code_type"] = _first_non_null([df[c] for c in have_code_types])
    else:
        out["code_type"] = None

    out = _strip_strings(out)

    for c in MONEY_COLS:
        if c in out.columns:
            out[c] = _clean_money_to_float(out[c])

    for c in PCT_COLS:
        if c in out.columns:
            out[c] = _clean_percent_to_float(out[c])
    price_like = [c for c in MONEY_COLS + ["negotiated_percentage"] if c in out.columns]
    if price_like:
        price_nonnull = pd.concat([out[c].notna() for c in price_like], axis=1).any(axis=1)
    else:
        price_nonnull = pd.Series([True] * len(out), index=out.index)  # 没有价格列则不筛

    desc_nonnull = out["description"].notna() if "description" in out.columns else pd.Series([True]*len(out))
    out = out[price_nonnull | desc_nonnull].copy()
    for col in STANDARD_COLUMNS:
        if col not in out.columns:
            out[col] = None

    out = out[STANDARD_COLUMNS].reset_index(drop=True)
    return out


def merge_and_save(dfs: List[pd.DataFrame], out_csv: str) -> pd.DataFrame:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    merged = pd.concat(dfs, ignore_index=True)
    merged.to_csv(out_csv, index=False)
    return merged
