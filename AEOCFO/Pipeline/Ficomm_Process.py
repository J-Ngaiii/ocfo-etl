import pandas as pd
import numpy as np
import re
from typing import List
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

def normalize_name(name: str) -> str:
    """Normalize club names by stripping and lowering."""
    return re.sub(r'\s+', ' ', name.strip().lower())

def match_dataframes_by_club_name(df_main, df_other, main_col='club_name', other_col='club_name', threshold=0.85, model=None):
    """Match rows from df_other to df_main using cosine similarity of name embeddings. Also return unmatched rows."""
    df_main = df_main.copy()
    df_other = df_other.copy()

    df_main['_norm_name'] = df_main[main_col].apply(normalize_name)
    df_other['_norm_name'] = df_other[other_col].apply(normalize_name)

    if model is None:
        model = SentenceTransformer("intfloat/e5-large-v2")

    main_embeddings = model.encode(df_main['_norm_name'].tolist(), convert_to_numpy=True, normalize_embeddings=True)
    other_embeddings = model.encode(df_other['_norm_name'].tolist(), convert_to_numpy=True, normalize_embeddings=True)

    sim_matrix = cosine_similarity(main_embeddings, other_embeddings)

    best_matches = np.argmax(sim_matrix, axis=1)
    best_scores = np.max(sim_matrix, axis=1)
    match_indices = [idx if score >= threshold else None for idx, score in zip(best_matches, best_scores)]

    matched_rows = []
    unmatched_rows = []

    for i, idx in enumerate(match_indices):
        if idx is None:
            matched_rows.append([np.nan] * len(df_other.columns))
            unmatched_rows.append(df_main.iloc[i])
        else:
            matched_rows.append(df_other.iloc[idx].tolist())

    matched_df = pd.DataFrame(matched_rows, columns=[f"{col}_matched" for col in df_other.columns])
    final_df = pd.concat([df_main.drop(columns=['_norm_name']).reset_index(drop=True), matched_df], axis=1)
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

def process_weekly_pipeline(oasis_df: pd.DataFrame,
                            fr_dfs: List[pd.DataFrame],
                            cont_dfs: List[pd.DataFrame],
                            threshold: float = 0.85) -> List[dict[str, pd.DataFrame]]:
    """
    For each week, match FR and Contingency to the same OASIS dataset.
    Returns:
        List of dicts per week: {
            "merged": <merged DataFrame>,
            "unmatched_fr": <OASIS rows unmatched with FR>,
            "unmatched_cont": <OASIS rows unmatched with Contingency>
        }
    """
    model = SentenceTransformer("intfloat/e5-large-v2")
    processed_outputs = []
    oasis_selected = select_oasis_columns(oasis_df)

    for df_fr, df_cont in zip(fr_dfs, cont_dfs):
        df_fr_cleaned = clean_fr_resolution(df_fr)
        df_fr_selected = select_fr_columns(df_fr_cleaned)
        df_cont_selected = select_contingency_columns(df_cont)

        # Match FR → OASIS
        fr_oasis_merged, unmatched_fr = match_dataframes_by_club_name(
            oasis_selected, df_fr_selected,
            main_col="club_name", other_col="club_name",
            threshold=threshold, model=model
        )

        # Match Contingency → OASIS
        cont_oasis_merged, unmatched_cont = match_dataframes_by_club_name(
            oasis_selected, df_cont_selected,
            main_col="club_name", other_col="club_name",
            threshold=threshold, model=model
        )

        # Merge on shared OASIS metadata
        merged_all = fr_oasis_merged.merge(
            cont_oasis_merged,
            on=["club_name", "Org Type", "BlueHeart", "Org ID Status"],
            how="outer",
            suffixes=("_FR", "_Contingency")
        )

        processed_outputs.append({
            "merged": merged_all,
            "unmatched_fr": unmatched_fr,
            "unmatched_cont": unmatched_cont
        })

    return processed_outputs




