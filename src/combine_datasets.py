"""
combine_datasets.py
-------------------
Merges the cleaned Sephora and Ulta product CSVs into a single dataset,
using fuzzy matching on brand + product name to identify and collapse
cross-retailer duplicates so each product appears only once.

Deduplication fix (2026-04-02)
------------------------------
Some retailers append the brand name inside the product name string
(e.g. Sephora lists "Moisture Lock Styling Curl Butter - Ouidad" while
Ulta lists "Moisture Lock Curl Butter").  The old _match_key included
brand + full product name, so "ouidad" appeared twice in the Sephora key,
inflating its length and dropping token_sort_ratio from ~89 to ~81 — just
below the 85 threshold.  Fix: strip the brand name from inside the product
name before building the key, so each brand appears exactly once.
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

FUZZY_THRESHOLD          = 85
CATEGORY_FUZZY_THRESHOLD = 80


# ---------------------------------------------------------------------------
# Match key
# ---------------------------------------------------------------------------

_NOISE  = re.compile(r"[^\w\s]")
_FILLER = re.compile(r"\b(the|a|an)\b")


def _match_key(brand: str, name: str) -> str:
    """
    Normalised key used for fuzzy deduplication.

    Brand name is stripped from within the product name string before
    concatenation so brands that append themselves to their product names
    (e.g. "Moisture Lock Curl Butter - Ouidad") don't skew the ratio.
    """
    bc = brand.strip().lower()
    nc = name.strip().lower()
    # Remove brand name from product name (handles "- BrandName" suffixes)
    if bc:
        nc = re.sub(re.escape(bc), "", nc).strip(" \t-\u2013\u2014")
    raw = f"{bc} {nc}"
    raw = _NOISE.sub(" ", raw)
    raw = _FILLER.sub(" ", raw)
    return re.sub(r"\s+", " ", raw).strip()


# ---------------------------------------------------------------------------
# Category normalisation
# ---------------------------------------------------------------------------

CANONICAL_CATEGORIES = [
    "Blush & Bronzer", "Body Lotion & Oil", "Body Scrub & Exfoliant",
    "Cleanser", "Color Correct & Primer", "Concealer", "Eye Cream",
    "Eye Liner", "Eye Shadow", "Foundation", "Face Mask", "Face Oil",
    "Face Serum", "Hair Care", "Highlighter", "Lip Balm & Treatment",
    "Lip Gloss", "Lip Liner", "Lipstick", "Mascara", "Moisturizer",
    "Nail Polish", "Perfume & Fragrance", "Self Tanner",
    "Setting Spray & Powder", "Sunscreen", "Toner & Essence",
    "Value & Gift Sets",
]

_CATEGORY_MAP: dict[str, str] = {
    # ── Sephora ──────────────────────────────────────────────────────────────
    "blush & bronzer":                    "Blush & Bronzer",
    "body lotions & oils":                "Body Lotion & Oil",
    "body scrubs & exfoliants":           "Body Scrub & Exfoliant",
    "cleanser":                           "Cleanser",
    "face cleansers":                     "Cleanser",
    "color correct & primers":            "Color Correct & Primer",
    "color correcting & primers":         "Color Correct & Primer",
    "concealers":                         "Concealer",
    "eye creams":                         "Eye Cream",
    "eyeliner":                           "Eye Liner",
    "eyeshadow":                          "Eye Shadow",
    "foundation":                         "Foundation",
    "face masks & treatments":            "Face Mask",
    "face oils":                          "Face Oil",
    "face serums":                        "Face Serum",
    "hair":                               "Hair Care",
    "highlighter":                        "Highlighter",
    "lip balm & lip treatments":          "Lip Balm & Treatment",
    "lip glosses":                        "Lip Gloss",
    "lip liner":                          "Lip Liner",
    "lip liners":                         "Lip Liner",
    "lipstick":                           "Lipstick",
    "mascara":                            "Mascara",
    "moisturizers":                       "Moisturizer",
    "moisturizer":                        "Moisturizer",
    "nail polish":                        "Nail Polish",
    "perfume & cologne":                  "Perfume & Fragrance",
    "self-tanners":                       "Self Tanner",
    "setting sprays & powders":           "Setting Spray & Powder",
    "sunscreen":                          "Sunscreen",
    "toners":                             "Toner & Essence",
    "value & gift sets":                  "Value & Gift Sets",
    # ── Ulta ─────────────────────────────────────────────────────────────────
    "blush":                              "Blush & Bronzer",
    "bronzer":                            "Blush & Bronzer",
    "body lotion":                        "Body Lotion & Oil",
    "body oil":                           "Body Lotion & Oil",
    "body scrub":                         "Body Scrub & Exfoliant",
    "face wash & cleanser":               "Cleanser",
    "face wash":                          "Cleanser",
    "color correcting primer":            "Color Correct & Primer",
    "concealer":                          "Concealer",
    "eye cream":                          "Eye Cream",
    "eye liner":                          "Eye Liner",
    "eyeliner & brow":                    "Eye Liner",
    "eye shadow":                         "Eye Shadow",
    "eye shadow palette":                 "Eye Shadow",
    "face mask":                          "Face Mask",
    "face oil":                           "Face Oil",
    "serum":                              "Face Serum",
    "face serum":                         "Face Serum",
    "hair care":                          "Hair Care",
    "highlighter & luminizer":            "Highlighter",
    "lip treatment":                      "Lip Balm & Treatment",
    "lip balm":                           "Lip Balm & Treatment",
    "lip gloss":                          "Lip Gloss",
    "lip liner":                          "Lip Liner",
    "lipstick":                           "Lipstick",
    "mascara":                            "Mascara",
    "moisturizer":                        "Moisturizer",
    "face moisturizer":                   "Moisturizer",
    "nail color":                         "Nail Polish",
    "nail polish":                        "Nail Polish",
    "fragrance":                          "Perfume & Fragrance",
    "perfume":                            "Perfume & Fragrance",
    "self tanner":                        "Self Tanner",
    "setting spray":                      "Setting Spray & Powder",
    "setting powder":                     "Setting Spray & Powder",
    "sunscreen":                          "Sunscreen",
    "spf":                                "Sunscreen",
    "toner":                              "Toner & Essence",
    "toner & essence":                    "Toner & Essence",
    "essence":                            "Toner & Essence",
    "gift set":                           "Value & Gift Sets",
    "sets & kits":                        "Value & Gift Sets",
}


def normalize_category(raw: str) -> str:
    key = raw.strip().lower()
    if key in _CATEGORY_MAP:
        return _CATEGORY_MAP[key]
    result = process.extractOne(
        key,
        [c.lower() for c in CANONICAL_CATEGORIES],
        scorer=fuzz.token_sort_ratio,
        score_cutoff=CATEGORY_FUZZY_THRESHOLD,
    )
    if result is not None:
        return CANONICAL_CATEGORIES[
            [c.lower() for c in CANONICAL_CATEGORIES].index(result[0])
        ]
    return raw.strip()


CATEGORY_MAP: dict[str, str] = {
    "Body Lotion":                     "Body Lotion & Moisturiser",
    "Body Lotions":                    "Body Lotion & Moisturiser",
    "Moisturizers":                    "Face Moisturiser",
    "Face Moisturizer":                "Face Moisturiser",
    "Face Serums":                     "Face Serum & Treatment",
    "Eye Cream":                       "Eye Cream & Treatment",
    "Eye Creams & Treatments":         "Eye Cream & Treatment",
    "Cleansers":                       "Face Cleanser",
    "Face Wash":                       "Face Cleanser",
    "Exfoliators":                     "Face Exfoliator & Peel",
    "Face Masks":                      "Face Mask",
    "Toners":                          "Toner & Essence",
    "Face Oils":                       "Face Oil",
    "Sunscreen":                       "Sunscreen",
    "Foundation":                      "Foundation",
    "Concealer":                       "Concealer",
    "Blush":                           "Blush",
    "Bronzer":                         "Bronzer",
    "Highlighter":                     "Highlighter",
    "Eyeshadow":                       "Eyeshadow",
    "Eyeliner":                        "Eyeliner",
    "Mascara":                         "Mascara",
    "Lipstick":                        "Lipstick",
    "Lip Gloss":                       "Lip Gloss",
    "Lip Liner":                       "Lip Liner",
    "Lip Balms & Treatments":          "Lip Balm & Treatment",
    "Nail Polish":                     "Nail Polish",
    "Shampoo":                         "Shampoo",
    "Conditioner":                     "Conditioner",
    "Hair Masks":                      "Hair Mask",
    "Dry Shampoo":                     "Dry Shampoo",
    "Hair Oil":                        "Hair Oil",
    "Styling Products":                "Hair Styling Products",
    "Hair Care":                       "Hair Care",
    "Perfume":                         "Perfume",
    "Cologne":                         "Cologne",
    "Fragrance":                       "Fragrance",
    "Value & Gift Sets":               "Gift & Value Sets",
    "Setting Spray & Powder":          "Setting Spray & Powder",
}


def _normalise_category(raw: str) -> str:
    return CATEGORY_MAP.get(str(raw).strip(), str(raw).strip())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def combine(
    sephora_path: "str | Path" = SEPHORA_PATH,
    ulta_path:    "str | Path" = ULTA_PATH,
    output_path:  "str | Path" = OUTPUT_PATH,
    threshold:    int          = FUZZY_THRESHOLD,
) -> pd.DataFrame:

    print("Loading cleaned datasets …")
    sephora = pd.read_csv(sephora_path)
    ulta    = pd.read_csv(ulta_path)

    sephora["source"] = "sephora"
    ulta["source"]    = "ulta"

    # Sephora appends brand/variant suffixes after the last " - "
    # e.g. "Moisture Lock Styling Curl Butter - Ouidad" -> "Moisture Lock Styling Curl Butter"
    sephora["product_name"] = sephora["product_name"].apply(
        lambda n: n.rsplit(" - ", 1)[0].strip() if isinstance(n, str) and " - " in n else n
    )

    sephora["category"] = sephora["category"].apply(_normalise_category)
    ulta["category"]    = ulta["category"].apply(_normalise_category)

    print(f"  Sephora : {len(sephora):>5} products")
    print(f"  Ulta    : {len(ulta):>5} products")

    print("Normalising categories …")
    for df, label in [(sephora, "Sephora"), (ulta, "Ulta")]:
        original   = df["category"].astype(str)
        normalised = original.apply(normalize_category)
        changed    = original[original != normalised]
        df["category"] = normalised
        if not changed.empty:
            print(f"  {label}: remapped {len(changed)} category values")

    # Build match keys (brand stripped from product name)
    sephora["_key"] = sephora.apply(
        lambda r: _match_key(str(r.get("brand", "")), str(r.get("product_name", ""))),
        axis=1,
    )
    ulta["_key"] = ulta.apply(
        lambda r: _match_key(str(r.get("brand", "")), str(r.get("product_name", ""))),
        axis=1,
    )

    print("Running fuzzy deduplication …")
    duplicate_sephora_indices = []

    ulta_keys = ulta["_key"].tolist()
    for idx, sephora_key in enumerate(sephora["_key"]):
        result = process.extractOne(
            sephora_key,
            ulta_keys,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        if result is not None:
            duplicate_sephora_indices.append(idx)

    n_dupes = len(duplicate_sephora_indices)
    print(
        f"  Found {n_dupes} Sephora products that duplicate an Ulta product "
        f"(threshold={threshold}) — dropping Sephora copies."
    )

    sephora_unique = sephora.drop(index=duplicate_sephora_indices).copy()

    combined = pd.concat([ulta, sephora_unique], ignore_index=True)
    combined.drop(columns=["_key"], inplace=True)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)

    print(f"\nCombined : {len(combined):>5} products  →  {output_path}")
    return combined


if __name__ == "__main__":
    combine()