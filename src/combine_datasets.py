"""
combine_datasets.py
-------------------
Merges the cleaned Sephora and Ulta product CSVs into a single dataset,
using fuzzy matching on brand + product name to identify and collapse
cross-retailer duplicates so each product appears only once.

Category normalisation
----------------------
Sephora and Ulta use different category taxonomies (e.g. Sephora's
"Face Masks & Treatments" vs Ulta's "Face Mask"). Before combining,
every category value from both retailers is mapped to a shared canonical
name via a two-layer system:
  1. Manual mapping dict — explicit, high-confidence name pairs.
  2. Fuzzy fallback — any category not in the manual map is matched
     against the canonical list using token_sort_ratio; if the best
     score >= CATEGORY_FUZZY_THRESHOLD it is remapped, otherwise the
     original value is kept and printed so you can add it to the map.

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

FUZZY_THRESHOLD = 85          # min score to call two products duplicates
CATEGORY_FUZZY_THRESHOLD = 80  # min score for fallback category remapping


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
# Category normalisation
# ---------------------------------------------------------------------------

# Canonical category names — the shared taxonomy both retailers are mapped to.
CANONICAL_CATEGORIES = [
    "Blush & Bronzer",
    "Body Lotion & Oil",
    "Body Scrub & Exfoliant",
    "Cleanser",
    "Color Correct & Primer",
    "Concealer",
    "Eye Cream",
    "Eye Liner",
    "Eye Shadow",
    "Foundation",
    "Face Mask",
    "Face Oil",
    "Face Serum",
    "Hair Care",
    "Highlighter",
    "Lip Balm & Treatment",
    "Lip Gloss",
    "Lip Liner",
    "Lipstick",
    "Mascara",
    "Moisturizer",
    "Nail Polish",
    "Perfume & Fragrance",
    "Self Tanner",
    "Setting Spray & Powder",
    "Sunscreen",
    "Toner & Essence",
    "Value & Gift Sets",
]

# Explicit retailer-specific → canonical mappings.
# Add entries here whenever the fuzzy fallback produces a wrong result.
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
    """
    Map a raw retailer category string to the shared canonical name.
    Falls back to fuzzy matching against CANONICAL_CATEGORIES if the
    category isn't in the manual map.
    """
    key = raw.strip().lower()

    # 1. Manual map (fast, exact)
    if key in _CATEGORY_MAP:
        return _CATEGORY_MAP[key]

    # 2. Fuzzy fallback
    result = process.extractOne(
        key,
        [c.lower() for c in CANONICAL_CATEGORIES],
        scorer=fuzz.token_sort_ratio,
        score_cutoff=CATEGORY_FUZZY_THRESHOLD,
    )
    if result is not None:
        # Return the properly-cased canonical name
        return CANONICAL_CATEGORIES[[c.lower() for c in CANONICAL_CATEGORIES].index(result[0])]

    # 3. Nothing matched — return as-is (will be printed for review)
    return raw.strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Category normalisation
# ---------------------------------------------------------------------------

# Maps raw scraped category names → canonical category names.
# Unmapped categories fall through unchanged (see _normalise_category).
CATEGORY_MAP: dict[str, str] = {
    # ── Skincare: moisturisers ──
    "Body Lotion":                     "Body Lotion & Moisturiser",
    "Body Lotions":                    "Body Lotion & Moisturiser",
    "Body Lotion & Creams":            "Body Lotion & Moisturiser",
    "Body Lotions & Body Oils":        "Body Lotion & Moisturiser",
    "Body Moisturizers":               "Body Lotion & Moisturiser",
    "Body Butters":                    "Body Lotion & Moisturiser",
    "Moisturizers":                    "Face Moisturiser",
    "Moisturizers & Treatments":       "Face Moisturiser",
    "Face Moisturizer":                "Face Moisturiser",
    "Face Creams":                     "Face Moisturiser",
    "Night Cream":                     "Face Moisturiser",
    "Night Creams":                    "Face Moisturiser",
    "Tinted Moisturizer":              "Face Moisturiser",
    "Hydration":                       "Face Moisturiser",
    "Decollete & Neck Creams":         "Face Moisturiser",
    "Neck Cream":                      "Face Moisturiser",
    # ── Skincare: serums & treatments ──
    "Face Serums":                     "Face Serum & Treatment",
    "Oils & Serums":                   "Face Serum & Treatment",
    "Treatment":                       "Face Serum & Treatment",
    "Treatment & Serums":              "Face Serum & Treatment",
    "Treatments & Serums":             "Face Serum & Treatment",
    "Skincare":                        "Face Serum & Treatment",
    "Eye Serums":                      "Eye Cream & Treatment",
    "Eye Cream":                       "Eye Cream & Treatment",
    "Eye Creams & Treatments":         "Eye Cream & Treatment",
    "Eye Treatments":                  "Eye Cream & Treatment",
    "Eye Masks":                       "Eye Cream & Treatment",
    "Eye Sets":                        "Eye Cream & Treatment",
    # ── Skincare: cleansers ──
    "Cleansers":                       "Face Cleanser",
    "Face Wash":                       "Face Cleanser",
    "Face Wash & Cleansers":           "Face Cleanser",
    "Cleansing Balms & Oils":          "Face Cleanser",
    "Makeup Remover":                  "Face Cleanser",
    "Makeup Removers":                 "Face Cleanser",
    "Face Wipes":                      "Face Cleanser",
    "Cleansing Exfoliators":           "Face Cleanser",
    # ── Skincare: exfoliators ──
    "Exfoliators":                     "Face Exfoliator & Peel",
    "Face Peels & Exfoliators":        "Face Exfoliator & Peel",
    "Facial Peels":                    "Face Exfoliator & Peel",
    "Scrub & Exfoliants":              "Body Scrub & Exfoliator",
    "Body Scrubs & Exfoliants":        "Body Scrub & Exfoliator",
    # ── Skincare: masks ──
    "Face Masks":                      "Face Mask",
    "Masks":                           "Face Mask",
    "Sheet Masks":                     "Face Mask",
    # ── Skincare: toner / mist / essence ──
    "Toner":                           "Toner & Essence",
    "Toners":                          "Toner & Essence",
    "Face Mists & Essences":           "Toner & Essence",
    "Mists & Essences":                "Toner & Essence",
    # ── Skincare: face oil ──
    "Face Oils":                       "Face Oil",
    # ── Skincare: primer ──
    "Face Primer":                     "Face Primer",
    "Eye Primer":                      "Eye Primer & Base",
    "Eye Primer & Base":               "Eye Primer & Base",
    # ── Skincare: sunscreen ──
    "Face Sunscreen":                  "Sunscreen",
    "Body Sunscreen":                  "Sunscreen",
    "Sunscreen":                       "Sunscreen",
    "Suncare":                         "Sunscreen",
    "Self Tanners":                    "Self-Tanner & Bronzing",
    "Self-Tanning & Bronzing":         "Self-Tanner & Bronzing",
    # ── Skincare: acne ──
    "Acne & Blemish Treatments":       "Acne & Blemish Treatment",
    "Acne & Blemishes":                "Acne & Blemish Treatment",
    "Blemish & Acne Treatments":       "Acne & Blemish Treatment",
    # ── Skincare: anti-aging ──
    "Anti-Aging":                      "Anti-Aging",
    # ── Skincare: body treatments ──
    "Body Treatments":                 "Body Treatment & Serum",
    "Body Serums & Oils":              "Body Treatment & Serum",
    "Stretch Marks & Firming":         "Body Treatment & Serum",
    "For Body":                        "Body Treatment & Serum",
    "For Face":                        "Face Serum & Treatment",
    # ── Body care ──
    "Body Care":                       "Body Care",
    "Body Essentials":                 "Body Care",
    "Body Wash & Shower Gel":          "Body Wash & Shower Gel",
    "Shower Gel & Body Wash":          "Body Wash & Shower Gel",
    "Body Mist & Hair Mist":           "Body Mist",
    "Hand Cream & Foot Cream":         "Hand & Foot Cream",
    "Hand & Foot Treatment":           "Hand & Foot Cream",
    "Deodorant":                       "Deodorant & Antiperspirant",
    "Deodorant & Antiperspirant":      "Deodorant & Antiperspirant",
    # ── Makeup: face ──
    "Foundation":                      "Foundation",
    "Concealer":                       "Concealer",
    "Under-Eye Concealer":             "Concealer",
    "Color Correct":                   "Colour Corrector",
    "Color Correcting":                "Colour Corrector",
    "BB & CC Cream":                   "BB & CC Cream",
    "BB & CC Creams":                  "BB & CC Cream",
    "Blush":                           "Blush",
    "Bronzer":                         "Bronzer",
    "Highlighter":                     "Highlighter",
    "Contour":                         "Contour & Contouring",
    "Contouring":                      "Contour & Contouring",
    "Setting Spray & Powder":          "Setting Spray & Powder",
    "Blotting Papers":                 "Setting Spray & Powder",
    "Body Makeup":                     "Body Makeup",
    # ── Makeup: eyes ──
    "Eyeshadow":                       "Eyeshadow",
    "Eyeshadow Palettes":              "Eyeshadow",
    "Eye Palettes":                    "Eyeshadow",
    "Eyeliner":                        "Eyeliner",
    "Mascara":                         "Mascara",
    "Eyelashes":                       "False Eyelashes & Lash Care",
    "False Eyelashes":                 "False Eyelashes & Lash Care",
    "Lash Primer & Serums":            "False Eyelashes & Lash Care",
    "Eyebrow":                         "Eyebrow",
    "Eyebrows":                        "Eyebrow",
    # ── Makeup: lips ──
    "Lipstick":                        "Lipstick",
    "Liquid Lipstick":                 "Lipstick",
    "Lip Gloss":                       "Lip Gloss",
    "Gloss & Shine":                   "Lip Gloss",
    "Lip Stain":                       "Lip Stain",
    "Lip Liner":                       "Lip Liner",
    "Lip Balms":                       "Lip Balm & Treatment",
    "Lip Balms & Treatments":          "Lip Balm & Treatment",
    "Lip Treatments":                  "Lip Balm & Treatment",
    "Lip Oil":                         "Lip Balm & Treatment",
    "Lip Plumper":                     "Lip Plumper",
    "Lip Plumpers":                    "Lip Plumper",
    # ── Makeup: palettes & sets ──
    "Cheek Palettes":                  "Makeup Palettes",
    "Makeup Palettes":                 "Makeup Palettes",
    "Makeup":                          "Makeup",
    # ── Nails ──
    "Nail Polish":                     "Nail Polish",
    "Gel Nail Polish":                 "Nail Polish",
    "Nail Care":                       "Nail Care",
    "Nail":                            "Nail Care",
    "Top & Base Coats":                "Nail Care",
    "Press On Nails":                  "Nail Care",
    "Nail Polish Stickers":            "Nail Care",
    # ── Hair ──
    "Shampoo":                         "Shampoo",
    "Shampoo & Conditioner":           "Shampoo",
    "Conditioner":                     "Conditioner",
    "Co-Wash":                         "Conditioner",
    "Leave-In Conditioner":            "Leave-In Conditioner & Treatment",
    "Leave-In Treatment":              "Leave-In Conditioner & Treatment",
    "Hair Masks":                      "Hair Mask",
    "Dry Shampoo":                     "Dry Shampoo",
    "Hair Oil":                        "Hair Oil",
    "Heat Protectant":                 "Heat Protectant",
    "Hair Spray":                      "Hair Spray",
    "Hairspray":                       "Hair Spray",
    "Styling Products":                "Hair Styling Products",
    "Hair Styling Products":           "Hair Styling Products",
    "Hair Styling & Treatments":       "Hair Styling Products",
    "Styling":                         "Hair Styling Products",
    "Wax & Pomade":                    "Hair Styling Products",
    "Volume & Texture":                "Hair Styling Products",
    "Smoothing":                       "Hair Styling Products",
    "Curl Enhancing":                  "Hair Styling Products",
    "Gloss & Shine":                   "Hair Styling Products",
    "Color Care":                      "Hair Colour & Treatment",
    "Hair Color":                      "Hair Colour & Treatment",
    "Hair Color & Bleach":             "Hair Colour & Treatment",
    "Hair Dye & Root Touch-Ups":       "Hair Colour & Treatment",
    "Root Touch Up":                   "Hair Colour & Treatment",
    "Scalp Care":                      "Scalp Care & Treatment",
    "Scalp Treatments":                "Scalp Care & Treatment",
    "Hair Thinning & Hair Loss":       "Scalp Care & Treatment",
    "Damaged Hair":                    "Hair Mask",
    "Hair Primers":                    "Hair Styling Products",
    "Hair Care":                       "Hair Care",
    "Hair":                            "Hair Care",
    # ── Fragrance ──
    "Perfume":                         "Perfume",
    "Women's Fragrance":               "Perfume",
    "Cologne":                         "Cologne",
    "Fragrance":                       "Fragrance",
    "Unisex Fragrance":                "Fragrance",
    "Rollerballs & Travel Size":       "Fragrance",
    "Aromatherapy":                    "Aromatherapy",
    # ── Tools & accessories ──
    "Makeup Brushes":                  "Makeup Brushes & Tools",
    "Face Brushes":                    "Makeup Brushes & Tools",
    "Eye Brushes":                     "Makeup Brushes & Tools",
    "Brush Sets":                      "Makeup Brushes & Tools",
    "Sponges & Applicators":           "Makeup Brushes & Tools",
    "Beauty Tools":                    "Beauty Tools & Devices",
    "High Tech Tools":                 "Beauty Tools & Devices",
    "Facial Rollers":                  "Beauty Tools & Devices",
    "Cleansing Brushes":               "Beauty Tools & Devices",
    "Flat Irons":                      "Hair Tools & Devices",
    "Hair Dryers":                     "Hair Tools & Devices",
    "Hot Brushes":                     "Hair Tools & Devices",
    "Hair Brushes & Combs":            "Hair Tools & Devices",
    "Brow & Lash Tools":               "Beauty Tools & Devices",
    "Manicure & Pedicure Tools":       "Nail Care",
    "Mirrors":                         "Beauty Tools & Devices",
    # ── Bath & body ──
    "Bath Bombs & Shower Steamers":    "Bath & Body",
    "Bath Soaks & Bubble Bath":        "Bath & Body",
    "Bubble Bath & Soaks":             "Bath & Body",
    "Spa Essentials":                  "Bath & Body",
    # ── Wellness / supplements ──
    "Beauty Supplements":              "Supplements & Wellness",
    "Daily Vitamins & Supplements":    "Supplements & Wellness",
    "Hair Supplements":                "Supplements & Wellness",
    "Digestion & Gut Health":          "Supplements & Wellness",
    "Holistic Wellness":               "Supplements & Wellness",
    "Movement & Fitness":              "Supplements & Wellness",
    "Protein & Fitness":               "Supplements & Wellness",
    "Relief & Recovery":               "Supplements & Wellness",
    "Sleep & Stress Relief":           "Supplements & Wellness",
    "Sleep Support":                   "Supplements & Wellness",
    "Women's Health":                  "Supplements & Wellness",
    "Menopausal Care":                 "Supplements & Wellness",
    # ── Oral / personal care ──
    "Oral Care":                       "Oral Care",
    "Deodorant & Antiperspirant":      "Deodorant & Antiperspirant",
    "Feminine Care":                   "Intimate & Feminine Care",
    "Feminine Hygiene":                "Intimate & Feminine Care",
    "Intimate Care":                   "Intimate & Feminine Care",
    "Period Care":                     "Intimate & Feminine Care",
    "Sexual Wellness":                 "Intimate & Feminine Care",
    "Shaving Cream & Razors":          "Shaving & Hair Removal",
    "Hair Removal":                    "Shaving & Hair Removal",
    "Hair Removal Tools":              "Shaving & Hair Removal",
    "Aftershave":                      "Shaving & Hair Removal",
    "Beard Care":                      "Men's",
    "Hand Sanitizer & Hand Soap":      "Hand Soap & Sanitiser",
    "Hand Soap & Sanitizers":          "Hand Soap & Sanitiser",
    # ── Gift / value sets ──
    "Value & Gift Sets":               "Gift & Value Sets",
    "Value Sets":                      "Gift & Value Sets",
    "Fragrance Gift Sets":             "Gift & Value Sets",
    "Fragrance Gifts":                 "Gift & Value Sets",
    "Fragrance Sets":                  "Gift & Value Sets",
    "Cologne Gift Sets":               "Gift & Value Sets",
    "Perfume Gift Sets":               "Gift & Value Sets",
    "Hair Gifts":                      "Gift & Value Sets",
    "Skin Gifts":                      "Gift & Value Sets",
    "Body Care Gifts":                 "Gift & Value Sets",
    "Makeup & Nail Gifts":             "Gift & Value Sets",
    "Men's Gifts":                     "Gift & Value Sets",
    "Face Sets":                       "Gift & Value Sets",
    "Lip Sets":                        "Gift & Value Sets",
    "Eye Sets":                        "Gift & Value Sets",
    "Tool & Brush Gifts":              "Gift & Value Sets",
    # ── Travel / mini ──
    "Mini Size":                       "Travel & Mini Size",
    "Travel Size Body Care":           "Travel & Mini Size",
    "Travel Size Hair Care":           "Travel & Mini Size",
    "Travel Size Makeup":              "Travel & Mini Size",
    "Travel Size Skin Care":           "Travel & Mini Size",
    # ── Catch-alls / misc ──
    "$100 and Under":                  "Gift & Value Sets",
    "$30 and Under":                   "Gift & Value Sets",
    "$50 and Under":                   "Gift & Value Sets",
    "Accessories":                     "Accessories",
    "Styling Accessories":             "Accessories",
    "Trend & Fashion Accessories":     "Accessories",
    "Clips & Bobby Pins":              "Accessories",
    "Elastics":                        "Accessories",
    "Scrunchies & Hair Ties":          "Accessories",
    "Headbands":                       "Accessories",
    "Silk Pillowcases":                "Accessories",
    "Hair Towels & Shower Caps":       "Accessories",
    "Hair Rollers":                    "Hair Tools & Devices",
    "Hair Extensions":                 "Accessories",
    "Brush Cleaner":                   "Makeup Brushes & Tools",
    "Brush Cleaners":                  "Makeup Brushes & Tools",
    "Sharpeners":                      "Makeup Brushes & Tools",
    "Makeup Bags & Organizers":        "Accessories",
    "Candles":                         "Candles & Home Fragrance",
    "Candles & Home Fragrance":        "Candles & Home Fragrance",
    "Candles & Home Scents":           "Candles & Home Fragrance",
    "Diffusers":                       "Candles & Home Fragrance",
    "Dermatologist Recommended":       "Face Serum & Treatment",
    "K-Beauty":                        "Face Serum & Treatment",
    "After Sun Care":                  "Sunscreen",
    "Bath & Body for Mom & Baby":      "Mother & Baby",
    "Mother & Baby":                   "Mother & Baby",
    "Kid's Haircare":                  "Mother & Baby",
    "Pet Care":                        "Pet Care",
    "Unisex / Genderless":             "Fragrance",
    "Men's":                           "Men's",
    "Women's":                         "Fragrance",
    "Trend Color":                     "Nail Polish",
    "Bath Sponges, Gloves & Brushes":  "Bath & Body",
}


def _normalise_category(raw: str) -> str:
    """Return the canonical category for a raw scraped value, or the raw value if unmapped."""
    return CATEGORY_MAP.get(str(raw).strip(), str(raw).strip())

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

    sephora["category"] = sephora["category"].apply(_normalise_category)
    ulta["category"]    = ulta["category"].apply(_normalise_category)

    print(f"  Sephora : {len(sephora):>5} products")
    print(f"  Ulta    : {len(ulta):>5} products")

    # Normalise categories to a shared canonical taxonomy
    print("Normalising categories …")
    for df, label in [(sephora, "Sephora"), (ulta, "Ulta")]:
        original   = df["category"].astype(str)
        normalised = original.apply(normalize_category)
        changed    = original[original != normalised]
        unmapped   = normalised[normalised == original]  # nothing matched
        df["category"] = normalised
        if not changed.empty:
            print(f"  {label}: remapped {len(changed)} category values")
        unique_unmapped = set(unmapped.str.strip().str.lower()) - \
                          set(c.lower() for c in CANONICAL_CATEGORIES) - \
                          set(_CATEGORY_MAP.keys())
        if unique_unmapped:
            print(f"  {label}: {len(unique_unmapped)} categories not in map "
                  f"(add to _CATEGORY_MAP if needed):")
            for c in sorted(unique_unmapped):
                print(f"    · {c}")

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