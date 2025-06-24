import pandas as pd
import numpy as np
from typing import Tuple
import re
from typing import List
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

NAMES_CONFIG = {
    'OASIS':{'match_col':'', 'select_cols':[]}, 
    'FR':{'match_col':'', 'select_cols':[]}, 
    'CONTINGENCY':{'match_col':'', 'select_cols':[]}, 
}

def normalize_name(name: str) -> str:
    """Normalize club names by stripping and lowering."""
    return re.sub(r'\s+', ' ', name.strip().lower())

def match_dataframes_by_club_name(df_main, df_other, main_col='club_name', other_col='club_name', threshold=0.9, model=None):
    """Match rows from df_other to df_main using cosine similarity of name embeddings. Also return unmatched rows."""
    df_main = df_main.copy()
    df_other = df_other.copy()

    df_main['_norm_name'] = df_main[main_col].apply(normalize_name)
    df_other['_norm_name'] = df_other[other_col].apply(normalize_name)

    if model is None:
        model = SentenceTransformer("intfloat/e5-large-v2")

    main_embeddings = model.encode(df_main['_norm_name'].tolist(), convert_to_numpy=True, normalize_embeddings=True) # suppose size (m x 1)
    other_embeddings = model.encode(df_other['_norm_name'].tolist(), convert_to_numpy=True, normalize_embeddings=True) # suppose size (t x 1)

    sim_matrix = cosine_similarity(main_embeddings, other_embeddings) # size (m x t)
    # value (i, j) means the cosine similarity of the ith name embed in main_embeddings with the jth name embed in other_embeddings

    best_matches = np.argmax(sim_matrix, axis=1) # size (m x 1) with each element being the index of other_embeddings that returns the best match
    best_scores = np.max(sim_matrix, axis=1) # size (m x 1) with each element being the cosine similarity score with thebest match
    match_indices = [idx if score >= threshold else None for idx, score in zip(best_matches, best_scores)] # size (m x 1)

    matched_rows = []
    unmatched_rows = []

    for i, idx in enumerate(match_indices): # loops though m times --> corresponding to number of rows in best_matches
        if idx is None:
            matched_rows.append([np.nan] * len(df_other.columns))
            unmatched_rows.append(df_main.iloc[i])
        else:
            matched_rows.append(df_other.iloc[idx].tolist())

    matched_df = pd.DataFrame(matched_rows, columns=[f"{col}_matched" for col in df_other.columns])
    final_df = pd.concat([df_main.drop(columns=['_norm_name']).reset_index(drop=True), matched_df], axis=1) # index-wise concatentaion places ith elem of matched_rows next to ith row of df_main
    unmatched_df = pd.DataFrame(unmatched_rows).drop(columns=['_norm_name']) if unmatched_rows else pd.DataFrame(columns=df_main.columns)

    return final_df, unmatched_df

def clean_fr_resolution(df_fr: pd.DataFrame) -> pd.DataFrame:
    """Filter FR resolutions to only include Type == 'Contingency'."""
    if "Type" not in df_fr.columns:
        raise ValueError("FR dataframe must contain 'Type' column.")
    return df_fr[df_fr["Type"].str.strip().str.lower() == "contingency"].copy()

def select_oasis_columns(df_oasis: pd.DataFrame) -> pd.DataFrame:
    """Select necessary columns from OASIS."""
    required_cols = ["Org Type", "BlueHeart", "Org ID Status", "club_name"]
    missing_cols = [col for col in required_cols if col not in df_oasis.columns]
    if missing_cols:
        raise ValueError(f"OASIS dataframe missing required columns: {missing_cols}")
    return df_oasis[required_cols].copy()

def select_fr_columns(df_fr: pd.DataFrame) -> pd.DataFrame:
    """Select necessary columns from FR."""
    required_cols = ["club_name", "Amount Requested"]
    missing_cols = [col for col in required_cols if col not in df_fr.columns]
    if missing_cols:
        raise ValueError(f"FR dataframe missing required columns: {missing_cols}")
    return df_fr[required_cols].copy()

def select_contingency_columns(df_cont: pd.DataFrame) -> pd.DataFrame:
    """Return all columns from Contingency."""
    return df_cont.copy()

def extract_date_string(filename: str) -> str:
    """Extracts date in mm_dd format from filename."""
    match = re.search(r"\d{1,2}[_/-]\d{1,2}", filename)
    if match:
        return match.group(0).replace("-", "_").zfill(5)  # e.g., 4_1 → 04_01
    return None

def process_weekly_pipeline(oasis_df: pd.DataFrame,
                            fr_dfs: List[pd.DataFrame],
                            cont_dfs: List[pd.DataFrame],
                            fr_names: List[str],
                            cont_names: List[str],
                            threshold: float = 0.9,
                            year: str = "FY25") -> Tuple[List[dict[str, pd.DataFrame]], List[str]]:
    """
    Matches FR and Contingency files by date in filename, then joins each with OASIS.
    Only processes weeks where both FR and Contingency exist for the same date.
    """
    assert re.match(r'FY\d{1,2}', year) is not None, f"Year should be formatted 'FYdd' but is {year}"

    model = SentenceTransformer("intfloat/e5-large-v2")
    processed_outputs = []
    cleaned_names = []
    oasis_selected = select_oasis_columns(oasis_df)

    # --- Map FR and Cont files by extracted date ---
    fr_map = {
        extract_date_string(name): (df, name)
        for df, name in zip(fr_dfs, fr_names)
        if extract_date_string(name) is not None
    }
    cont_map = {
        extract_date_string(name): (df, name)
        for df, name in zip(cont_dfs, cont_names)
        if extract_date_string(name) is not None
    }

    # --- Process only matching dates ---
    shared_dates = sorted(set(fr_map.keys()) & set(cont_map.keys())) # set(A & B) returns a set object containing elements at the intersection of collections A and B

    for i, date_str in enumerate(shared_dates):
        df_fr, fr_name = fr_map[date_str]
        df_cont, cont_name = cont_map[date_str]

        df_fr_cleaned = clean_fr_resolution(df_fr)
        df_fr_selected = select_fr_columns(df_fr_cleaned)
        df_cont_selected = select_contingency_columns(df_cont)

        # Match FR → OASIS (correct direction)
        fr_oasis_merged, unmatched_fr = match_dataframes_by_club_name(
            df_main=df_fr_selected,
            df_other=oasis_selected,
            main_col="club_name",
            other_col="club_name",
            threshold=threshold,
            model=model
        )

        # Match Contingency → OASIS (correct direction)
        cont_oasis_merged, unmatched_cont = match_dataframes_by_club_name(
            df_main=df_cont_selected,
            df_other=oasis_selected,
            main_col="club_name",
            other_col="club_name",
            threshold=threshold,
            model=model
        )

        # Merge on OASIS metadata matched columns
        merged_all = fr_oasis_merged.merge(
            cont_oasis_merged,
            on=["club_name", "Org Type_matched", "BlueHeart_matched", "Org ID Status_matched"],
            how="outer",
            suffixes=("_FR", "_Contingency")
        )

        processed_outputs.append({
            "merged": merged_all,
            "unmatched_fr": unmatched_fr,
            "unmatched_cont": unmatched_cont
        })

        cleaned_name = f"Ficomm-Combined-{date_str}-{year}-GF"
        cleaned_names.append(cleaned_name)

    return processed_outputs, cleaned_names





