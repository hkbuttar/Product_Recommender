"""
combine_datasets.py
-------------------
Merges the cleaned Sephora and Ulta product CSVs into a single dataset,
using fuzzy matching on brand + product name to identify and collapse
cross-retailer duplicates so each product appears only once.

Deduplication strategy
----------------------
- Concatenate both files, tagging each row with its source retailer.
- Build a match key: lowercase(brand) + " " + lowercase(product_name),
  with common noise stripped (punctuation, filler words like "the", "a").
- For each Ulta product, find its closest Sephora counterpart using
  rapidfuzz token_sort_ratio (handles word-order differences).
- Pairs scoring >= FUZZY_THRESHOLD (85) are considered duplicates.
  The Ulta row is kept; the Sephora row is dropped.
  Rationale: Ulta data tends to have fuller ingredient lists.
- All remaining rows (non-duplicate Ulta + all Sephora) are concatenated
  and written to data/processed/combined_products.csv.

Usage
-----
    python src/combine_datasets.py

Output
------
    data/processed/combined_products.csv
"""

import re
import sys
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_REPO_ROOT    = Path(__file__).resolve().parent.parent
PROCESSED_DIR = _REPO_ROOT / "data" / "processed"

SEPHORA_PATH = PROCESSED_DIR / "sephora_products_clean.csv"
ULTA_PATH    = PROCESSED_DIR / "ulta_products_clean.csv"
OUTPUT_PATH  = PROCESSED_DIR / "combined_products.csv"

FUZZY_THRESHOLD = 85   # minimum token_sort_ratio score to call two products the same


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOISE = re.compile(r"[^\w\s]")          # strip punctuation
_FILLER = re.compile(r"\b(the|a|an)\b")  # strip common filler words


def _match_key(brand: str, name: str) -> str:
    """Normalised brand + product name used as the fuzzy match key."""
    raw = f"{brand} {name}".lower()
    raw = _NOISE.sub(" ", raw)
    raw = _FILLER.sub(" ", raw)
    return re.sub(r"\s+", " ", raw).strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def combine(
    sephora_path: str | Path = SEPHORA_PATH,
    ulta_path:    str | Path = ULTA_PATH,
    output_path:  str | Path = OUTPUT_PATH,
    threshold:    int        = FUZZY_THRESHOLD,
) -> pd.DataFrame:

    print("Loading cleaned datasets …")
    sephora = pd.read_csv(sephora_path)
    ulta    = pd.read_csv(ulta_path)

    sephora["source"] = "sephora"
    ulta["source"]    = "ulta"

    print(f"  Sephora : {len(sephora):>5} products")
    print(f"  Ulta    : {len(ulta):>5} products")

    # Build match keys
    sephora["_key"] = sephora.apply(
        lambda r: _match_key(str(r.get("brand", "")), str(r.get("product_name", ""))), axis=1
    )
    ulta["_key"] = ulta.apply(
        lambda r: _match_key(str(r.get("brand", "")), str(r.get("product_name", ""))), axis=1
    )

    sephora_keys = sephora["_key"].tolist()

    # For each Sephora product find its best Ulta match
    print("Running fuzzy deduplication …")
    duplicate_sephora_indices = []

    for idx, sephora_key in enumerate(sephora["_key"]):
        result = process.extractOne(
            sephora_key,
            ulta["_key"].tolist(),
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        if result is not None:
            duplicate_sephora_indices.append(idx)

    n_dupes = len(duplicate_sephora_indices)
    print(f"  Found {n_dupes} Sephora products that duplicate an Ulta product "
          f"(threshold={threshold}) — dropping Sephora copies.")

    sephora_unique = sephora.drop(index=duplicate_sephora_indices).copy()

    # Combine and clean up
    combined = pd.concat([ulta, sephora_unique], ignore_index=True)
    combined.drop(columns=["_key"], inplace=True)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)

    print(f"\nCombined : {len(combined):>5} products  →  {output_path}")
    return combined


if __name__ == "__main__":
    combine()