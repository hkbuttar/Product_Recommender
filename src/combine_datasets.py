"""
combine_datasets.py
-------------------
Merges the cleaned Sephora and Ulta product CSVs into a single dataset,
using fuzzy matching on brand + product name to identify and collapse
cross-retailer duplicates so each product appears only once.
"""

import re
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
    bc = brand.strip().lower()
    nc = name.strip().lower()
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
    "Acne Treatment",
    "BB & CC Cream",
    "Blush & Bronzer",
    "Body Lotion & Oil",
    "Body Scrub & Exfoliant",
    "Body Wash & Bath",
    "Cleanser",
    "Color Correct & Primer",
    "Concealer",
    "Eye Cream",
    "Eye Liner",
    "Eye Shadow",
    "Face Mask",
    "Face Oil",
    "Face Serum",
    "Foundation",
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
    "Other / Uncategorized",
]

# Exhaustive map — covers every raw string seen in Sephora + Ulta CSVs.
# Keys are lowercase stripped versions of raw category strings.
_CATEGORY_MAP: dict[str, str] = {

    # ── Acne / Blemish ───────────────────────────────────────────────────────
    "acne & blemishes":                       "Acne Treatment",
    "acne & blemish treatments":              "Acne Treatment",
    "acne treatment":                         "Acne Treatment",
    "blemish & acne treatments":              "Acne Treatment",
    "blemish treatments":                     "Acne Treatment",
    "pimple patches":                         "Acne Treatment",

    # ── BB / CC ───────────────────────────────────────────────────────────────
    "bb & cc cream":                          "BB & CC Cream",
    "bb & cc creams":                         "BB & CC Cream",
    "bb cream":                               "BB & CC Cream",
    "cc cream":                               "BB & CC Cream",

    # ── Blush / Bronzer / Contour ────────────────────────────────────────────
    "blush & bronzer":                        "Blush & Bronzer",
    "blush":                                  "Blush & Bronzer",
    "bronzer":                                "Blush & Bronzer",
    "contour":                                "Blush & Bronzer",
    "contour & blush":                        "Blush & Bronzer",
    "contouring":                             "Blush & Bronzer",
    "cheek palettes":                         "Blush & Bronzer",

    # ── Body Lotion & Oil ────────────────────────────────────────────────────
    "body lotions & oils":                    "Body Lotion & Oil",
    "body lotion":                            "Body Lotion & Oil",
    "body lotions":                           "Body Lotion & Oil",
    "body oil":                               "Body Lotion & Oil",
    "body lotion & creams":                   "Body Lotion & Oil",
    "body lotion & moisturiser":              "Body Lotion & Oil",
    "body moisturizer":                       "Body Lotion & Oil",
    "body moisturizers":                      "Body Lotion & Oil",
    "body cream":                             "Body Lotion & Oil",
    "body butter":                            "Body Lotion & Oil",
    "body butters":                           "Body Lotion & Oil",
    "body serums & oils":                     "Body Lotion & Oil",
    "body treatments":                        "Body Lotion & Oil",
    "body essentials":                        "Body Lotion & Oil",
    "body care":                              "Body Lotion & Oil",
    "for body":                               "Body Lotion & Oil",
    "hand cream & foot cream":                "Body Lotion & Oil",
    "hand & foot treatment":                  "Body Lotion & Oil",
    "hand cream":                             "Body Lotion & Oil",
    "hand lotion":                            "Body Lotion & Oil",
    "hand & nail cream":                      "Body Lotion & Oil",
    "foot cream":                             "Body Lotion & Oil",
    "decollete & neck creams":                "Body Lotion & Oil",
    "neck cream":                             "Body Lotion & Oil",
    "bath & body for mom & baby":             "Body Lotion & Oil",
    "mother & baby":                          "Body Lotion & Oil",

    # ── Body Scrub & Exfoliant ───────────────────────────────────────────────
    "body scrubs & exfoliants":               "Body Scrub & Exfoliant",
    "body scrub":                             "Body Scrub & Exfoliant",
    "body scrubs":                            "Body Scrub & Exfoliant",
    "exfoliators":                            "Body Scrub & Exfoliant",
    "exfoliants":                             "Body Scrub & Exfoliant",
    "chemical exfoliants":                    "Body Scrub & Exfoliant",
    "face exfoliator & peel":                 "Body Scrub & Exfoliant",
    "face peels & exfoliators":               "Body Scrub & Exfoliant",
    "facial peels":                           "Body Scrub & Exfoliant",
    "face scrubs":                            "Body Scrub & Exfoliant",
    "cleansing exfoliators":                  "Body Scrub & Exfoliant",
    "peels":                                  "Body Scrub & Exfoliant",

    # ── Body Wash & Bath ─────────────────────────────────────────────────────
    "body wash":                              "Body Wash & Bath",
    "body wash & shower gel":                 "Body Wash & Bath",
    "shower gel & body wash":                 "Body Wash & Bath",
    "shower gel":                             "Body Wash & Bath",
    "bath soaks & bubble bath":               "Body Wash & Bath",
    "bubble bath & soaks":                    "Body Wash & Bath",
    "bath & shower":                          "Body Wash & Bath",
    "bath salts":                             "Body Wash & Bath",
    "bubble bath":                            "Body Wash & Bath",
    "bar soap":                               "Body Wash & Bath",
    "hand wash":                              "Body Wash & Bath",
    "hand soap & sanitizers":                 "Body Wash & Bath",
    "hand sanitizer & hand soap":             "Body Wash & Bath",
    "cleansing bars":                         "Body Wash & Bath",
    "bath bombs & shower steamers":           "Body Wash & Bath",
    "spa essentials":                         "Body Wash & Bath",

    # ── Cleanser ─────────────────────────────────────────────────────────────
    "cleanser":                               "Cleanser",
    "cleansers":                              "Cleanser",
    "face cleansers":                         "Cleanser",
    "face wash & cleanser":                   "Cleanser",
    "face wash & cleansers":                  "Cleanser",
    "face wash":                              "Cleanser",
    "face cleanser":                          "Cleanser",
    "makeup remover":                         "Cleanser",
    "makeup removers":                        "Cleanser",
    "cleansing oil":                          "Cleanser",
    "cleansing balm":                         "Cleanser",
    "cleansing balms & oils":                 "Cleanser",
    "micellar water":                         "Cleanser",
    "face wipes":                             "Cleanser",
    "co-wash":                                "Cleanser",

    # ── Color Correct & Primer ───────────────────────────────────────────────
    "color correct & primers":                "Color Correct & Primer",
    "color correcting & primers":             "Color Correct & Primer",
    "color correcting primer":                "Color Correct & Primer",
    "color correcting":                       "Color Correct & Primer",
    "color correct":                          "Color Correct & Primer",
    "primer":                                 "Color Correct & Primer",
    "primers":                                "Color Correct & Primer",
    "face primer":                            "Color Correct & Primer",
    "hair primers":                           "Hair Care",  # hair, not face
    "eye primer & base":                      "Color Correct & Primer",
    "eye primer":                             "Color Correct & Primer",
    "color corrector":                        "Color Correct & Primer",

    # ── Concealer ────────────────────────────────────────────────────────────
    "concealer":                              "Concealer",
    "concealers":                             "Concealer",
    "color correcting concealer":             "Concealer",
    "under-eye concealer":                    "Concealer",

    # ── Eye Cream ────────────────────────────────────────────────────────────
    "eye creams":                             "Eye Cream",
    "eye cream":                              "Eye Cream",
    "eye creams & treatments":                "Eye Cream",
    "eye cream & treatment":                  "Eye Cream",
    "eye patches":                            "Eye Cream",
    "eye masks":                              "Eye Cream",
    "eye gel":                                "Eye Cream",
    "under eye":                              "Eye Cream",
    "eye treatments":                         "Eye Cream",
    "eye serums":                             "Eye Cream",

    # ── Eye Liner / Brow ─────────────────────────────────────────────────────
    "eyeliner":                               "Eye Liner",
    "eye liner":                              "Eye Liner",
    "eyeliner & brow":                        "Eye Liner",
    "brow":                                   "Eye Liner",
    "brow gel":                               "Eye Liner",
    "brow pencil":                            "Eye Liner",
    "eyebrow":                                "Eye Liner",
    "eyebrows":                               "Eye Liner",
    "brow & lash tools":                      "Eye Liner",

    # ── Eye Shadow ───────────────────────────────────────────────────────────
    "eyeshadow":                              "Eye Shadow",
    "eye shadow":                             "Eye Shadow",
    "eye shadow palette":                     "Eye Shadow",
    "eyeshadow palettes":                     "Eye Shadow",
    "eye palettes":                           "Eye Shadow",
    "eye palette":                            "Eye Shadow",
    "makeup palettes":                        "Eye Shadow",

    # ── Face Mask ────────────────────────────────────────────────────────────
    "face masks & treatments":                "Face Mask",
    "face mask":                              "Face Mask",
    "face masks":                             "Face Mask",
    "masks":                                  "Face Mask",
    "sheet masks":                            "Face Mask",
    "clay masks":                             "Face Mask",
    "overnight mask":                         "Face Mask",
    "sleeping mask":                          "Face Mask",

    # ── Face Oil ─────────────────────────────────────────────────────────────
    "face oils":                              "Face Oil",
    "face oil":                               "Face Oil",

    # ── Face Serum ───────────────────────────────────────────────────────────
    "face serums":                            "Face Serum",
    "face serum":                             "Face Serum",
    "serum":                                  "Face Serum",
    "face serum & treatment":                 "Face Serum",
    "oils & serums":                          "Face Serum",
    "treatment":                              "Face Serum",
    "treatments":                             "Face Serum",
    "treatment & serums":                     "Face Serum",
    "treatments & serums":                    "Face Serum",
    "facial treatments":                      "Face Serum",
    "skin treatments":                        "Face Serum",
    "for face":                               "Face Serum",
    "ampoules":                               "Face Serum",
    "anti-aging":                             "Face Serum",
    "anti-aging treatments":                  "Face Serum",
    "dark spot treatments":                   "Face Serum",
    "spot treatments":                        "Face Serum",
    "vitamin c serums":                       "Face Serum",
    "retinol":                                "Face Serum",
    "retinol & retinoids":                    "Face Serum",
    "hyaluronic acid":                        "Face Serum",
    "hydration":                              "Face Serum",
    "face mists & essences":                  "Toner & Essence",
    "mists & essences":                       "Toner & Essence",

    # ── Foundation ───────────────────────────────────────────────────────────
    "foundation":                             "Foundation",
    "tinted moisturizer":                     "Foundation",
    "tinted moisturizers":                    "Foundation",
    "tinted spf":                             "Foundation",
    "body makeup":                            "Foundation",

    # ── Hair Care ────────────────────────────────────────────────────────────
    "hair":                                   "Hair Care",
    "hair care":                              "Hair Care",
    "shampoo":                                "Hair Care",
    "shampoo & conditioner":                  "Hair Care",
    "conditioner":                            "Hair Care",
    "hair mask":                              "Hair Care",
    "hair masks":                             "Hair Care",
    "hair oil":                               "Hair Care",
    "hair oils":                              "Hair Care",
    "dry shampoo":                            "Hair Care",
    "hair styling products":                  "Hair Care",
    "styling products":                       "Hair Care",
    "styling":                                "Hair Care",
    "hair treatment":                         "Hair Care",
    "hair treatments":                        "Hair Care",
    "hair styling & treatments":              "Hair Care",
    "hair color":                             "Hair Care",
    "hair color & bleach":                    "Hair Care",
    "hair dye & root touch-ups":              "Hair Care",
    "root touch up":                          "Hair Care",
    "hair loss":                              "Hair Care",
    "hair thinning & hair loss":              "Hair Care",
    "scalp care":                             "Hair Care",
    "scalp treatments":                       "Hair Care",
    "leave-in conditioner":                   "Hair Care",
    "leave-in treatment":                     "Hair Care",
    "heat protectant":                        "Hair Care",
    "hairspray":                              "Hair Care",
    "hair spray":                             "Hair Care",
    "wax & pomade":                           "Hair Care",
    "curl enhancing":                         "Hair Care",
    "volume & texture":                       "Hair Care",
    "smoothing":                              "Hair Care",
    "gloss & shine":                          "Hair Care",
    "travel size hair care":                  "Hair Care",
    "hair supplements":                       "Hair Care",
    "kid's haircare":                         "Hair Care",
    "damaged hair":                           "Hair Care",
    "color care":                             "Hair Care",

    # ── Highlighter ──────────────────────────────────────────────────────────
    "highlighter":                            "Highlighter",
    "highlighter & luminizer":                "Highlighter",
    "luminizer":                              "Highlighter",
    "shimmer":                                "Highlighter",

    # ── Lip ──────────────────────────────────────────────────────────────────
    "lip balm & lip treatments":              "Lip Balm & Treatment",
    "lip balms & treatments":                 "Lip Balm & Treatment",
    "lip balms":                              "Lip Balm & Treatment",
    "lip balm":                               "Lip Balm & Treatment",
    "lip treatment":                          "Lip Balm & Treatment",
    "lip treatments":                         "Lip Balm & Treatment",
    "lip mask":                               "Lip Balm & Treatment",
    "lip care":                               "Lip Balm & Treatment",
    "lip plumper":                            "Lip Balm & Treatment",
    "lip plumpers":                           "Lip Balm & Treatment",
    "lip glosses":                            "Lip Gloss",
    "lip gloss":                              "Lip Gloss",
    "lip oil":                                "Lip Gloss",
    "lip liner":                              "Lip Liner",
    "lip liners":                             "Lip Liner",
    "lipstick":                               "Lipstick",
    "lip color":                              "Lipstick",
    "liquid lipstick":                        "Lipstick",
    "lip stain":                              "Lipstick",

    # ── Mascara / Lashes ─────────────────────────────────────────────────────
    "mascara":                                "Mascara",
    "eyelashes":                              "Mascara",
    "false eyelashes":                        "Mascara",
    "lash primer & serums":                   "Mascara",

    # ── Moisturizer ──────────────────────────────────────────────────────────
    "moisturizers":                           "Moisturizer",
    "moisturizer":                            "Moisturizer",
    "face moisturizer":                       "Moisturizer",
    "face moisturiser":                       "Moisturizer",
    "face lotion":                            "Moisturizer",
    "face cream":                             "Moisturizer",
    "face creams":                            "Moisturizer",
    "night cream":                            "Moisturizer",
    "night creams":                           "Moisturizer",
    "moisturizers & treatments":              "Moisturizer",
    "skincare":                               "Moisturizer",

    # ── Nail ─────────────────────────────────────────────────────────────────
    "nail polish":                            "Nail Polish",
    "nail color":                             "Nail Polish",
    "nail care":                              "Nail Polish",
    "nail":                                   "Nail Polish",
    "gel nail polish":                        "Nail Polish",
    "nail treatment":                         "Nail Polish",
    "press on nails":                         "Nail Polish",
    "nail polish stickers":                   "Nail Polish",
    "top & base coats":                       "Nail Polish",
    "nail art & design":                      "Nail Polish",
    "trend color":                            "Nail Polish",

    # ── Perfume / Fragrance ──────────────────────────────────────────────────
    "perfume & cologne":                      "Perfume & Fragrance",
    "perfume":                                "Perfume & Fragrance",
    "cologne":                                "Perfume & Fragrance",
    "fragrance":                              "Perfume & Fragrance",
    "body spray":                             "Perfume & Fragrance",
    "body mist & hair mist":                  "Perfume & Fragrance",
    "rollerballs & travel size":              "Perfume & Fragrance",
    "aromatherapy":                           "Perfume & Fragrance",
    "candles":                                "Perfume & Fragrance",
    "candles & home fragrance":               "Perfume & Fragrance",
    "candles & home scents":                  "Perfume & Fragrance",
    "home fragrance":                         "Perfume & Fragrance",
    "diffusers":                              "Perfume & Fragrance",
    "aftershave":                             "Perfume & Fragrance",
    "unisex fragrance":                       "Perfume & Fragrance",
    "women's fragrance":                      "Perfume & Fragrance",
    "unisex / genderless":                    "Perfume & Fragrance",

    # ── Self Tanner ──────────────────────────────────────────────────────────
    "self-tanners":                           "Self Tanner",
    "self tanner":                            "Self Tanner",
    "self tanners":                           "Self Tanner",
    "tanning":                                "Self Tanner",
    "self-tanning & bronzing":                "Self Tanner",
    "after sun care":                         "Self Tanner",

    # ── Setting Spray & Powder ───────────────────────────────────────────────
    "setting sprays & powders":               "Setting Spray & Powder",
    "setting spray":                          "Setting Spray & Powder",
    "setting powder":                         "Setting Spray & Powder",
    "face powder":                            "Setting Spray & Powder",
    "translucent powder":                     "Setting Spray & Powder",
    "loose powder":                           "Setting Spray & Powder",
    "blotting papers":                        "Setting Spray & Powder",

    # ── Sunscreen ────────────────────────────────────────────────────────────
    "sunscreen":                              "Sunscreen",
    "spf":                                    "Sunscreen",
    "sun care":                               "Sunscreen",
    "suncare":                                "Sunscreen",
    "face sunscreen":                         "Sunscreen",
    "body sunscreen":                         "Sunscreen",
    "mineral sunscreen":                      "Sunscreen",
    "spf moisturizer":                        "Sunscreen",

    # ── Toner & Essence ──────────────────────────────────────────────────────
    "toners":                                 "Toner & Essence",
    "toner":                                  "Toner & Essence",
    "toner & essence":                        "Toner & Essence",
    "essence":                                "Toner & Essence",
    "mist":                                   "Toner & Essence",
    "facial mist":                            "Toner & Essence",

    # ── Value & Gift Sets ────────────────────────────────────────────────────
    "value & gift sets":                      "Value & Gift Sets",
    "gift & value sets":                      "Value & Gift Sets",
    "gift set":                               "Value & Gift Sets",
    "sets & kits":                            "Value & Gift Sets",
    "value sets":                             "Value & Gift Sets",
    "gifts with purchase":                    "Value & Gift Sets",
    "skin gifts":                             "Value & Gift Sets",
    "hair gifts":                             "Value & Gift Sets",
    "body care gifts":                        "Value & Gift Sets",
    "men's gifts":                            "Value & Gift Sets",
    "tool & brush gifts":                     "Value & Gift Sets",
    "fragrance gifts":                        "Value & Gift Sets",
    "perfume gift sets":                      "Value & Gift Sets",
    "cologne gift sets":                      "Value & Gift Sets",
    "fragrance gift sets":                    "Value & Gift Sets",
    "makeup & nail gifts":                    "Value & Gift Sets",
    "eye sets":                               "Value & Gift Sets",
    "face sets":                              "Value & Gift Sets",
    "lip sets":                               "Value & Gift Sets",
    "fragrance sets":                         "Value & Gift Sets",
    "travel size makeup":                     "Value & Gift Sets",
    "travel size body care":                  "Value & Gift Sets",
    "travel size skin care":                  "Value & Gift Sets",
    "mini size":                              "Value & Gift Sets",
    "bestsellers":                            "Value & Gift Sets",
    "$100 and under":                         "Value & Gift Sets",
    "$50 and under":                          "Value & Gift Sets",
    "$30 and under":                          "Value & Gift Sets",

    # ── Other / Uncategorized ────────────────────────────────────────────────
    "accessories":                            "Other / Uncategorized",
    "beauty accessories":                     "Other / Uncategorized",
    "beauty tools":                           "Other / Uncategorized",
    "tools":                                  "Other / Uncategorized",
    "skincare tools":                         "Other / Uncategorized",
    "makeup brushes":                         "Other / Uncategorized",
    "face brushes":                           "Other / Uncategorized",
    "eye brushes":                            "Other / Uncategorized",
    "lip brushes":                            "Other / Uncategorized",
    "brush sets":                             "Other / Uncategorized",
    "brush cleaner":                          "Other / Uncategorized",
    "brush cleaners":                         "Other / Uncategorized",
    "brushes & combs":                        "Other / Uncategorized",
    "hair brushes & combs":                   "Other / Uncategorized",
    "brushes & applicators":                  "Other / Uncategorized",
    "sponges & applicators":                  "Other / Uncategorized",
    "eyelash curlers":                        "Other / Uncategorized",
    "tweezers & eyebrow tools":               "Other / Uncategorized",
    "facial rollers":                         "Other / Uncategorized",
    "scalp massagers & rollers":              "Other / Uncategorized",
    "makeup bags & organizers":               "Other / Uncategorized",
    "makeup & travel cases":                  "Other / Uncategorized",
    "mirrors":                                "Other / Uncategorized",
    "sharpeners":                             "Other / Uncategorized",
    "curling irons & stylers":                "Other / Uncategorized",
    "curling irons":                          "Other / Uncategorized",
    "flat irons":                             "Other / Uncategorized",
    "hair straighteners & flat irons":        "Other / Uncategorized",
    "hair dryers":                            "Other / Uncategorized",
    "blow dry brushes":                       "Other / Uncategorized",
    "hot brushes":                            "Other / Uncategorized",
    "high tech tools":                        "Other / Uncategorized",
    "hair cutting tools":                     "Other / Uncategorized",
    "hair tool attachments & diffusers":      "Other / Uncategorized",
    "hair rollers":                           "Other / Uncategorized",
    "hair extensions":                        "Other / Uncategorized",
    "hair towels & shower caps":              "Other / Uncategorized",
    "hair towels":                            "Other / Uncategorized",
    "hair removal":                           "Other / Uncategorized",
    "hair removal tools":                     "Other / Uncategorized",
    "shaving cream & razors":                 "Other / Uncategorized",
    "beard care":                             "Other / Uncategorized",
    "clips & bobby pins":                     "Other / Uncategorized",
    "hair clips & claw clips":                "Other / Uncategorized",
    "headbands":                              "Other / Uncategorized",
    "scrunchies & hair ties":                 "Other / Uncategorized",
    "elastics":                               "Other / Uncategorized",
    "styling accessories":                    "Other / Uncategorized",
    "trend & fashion accessories":            "Other / Uncategorized",
    "manicure & pedicure tools":              "Other / Uncategorized",
    "bath sponges, gloves & brushes":         "Other / Uncategorized",
    "bath & body accessories":                "Other / Uncategorized",
    "showerheads & filters":                  "Other / Uncategorized",
    "silk pillowcases":                       "Other / Uncategorized",
    "beauty supplements":                     "Other / Uncategorized",
    "daily vitamins & supplements":           "Other / Uncategorized",
    "protein & fitness":                      "Other / Uncategorized",
    "digestion & gut health":                 "Other / Uncategorized",
    "sleep support":                          "Other / Uncategorized",
    "sleep & stress relief":                  "Other / Uncategorized",
    "relief & recovery":                      "Other / Uncategorized",
    "holistic wellness":                      "Other / Uncategorized",
    "wellness":                               "Other / Uncategorized",
    "movement & fitness":                     "Other / Uncategorized",
    "menopausal care":                        "Other / Uncategorized",
    "women's health":                         "Other / Uncategorized",
    "sexual wellness":                        "Other / Uncategorized",
    "intimate care":                          "Other / Uncategorized",
    "feminine hygiene":                       "Other / Uncategorized",
    "feminine care":                          "Other / Uncategorized",
    "period care":                            "Other / Uncategorized",
    "oral care":                              "Other / Uncategorized",
    "deodorant":                              "Other / Uncategorized",
    "deodorant & antiperspirant":             "Other / Uncategorized",
    "pet care":                               "Other / Uncategorized",
    "men's":                                  "Other / Uncategorized",
    "women's":                                "Other / Uncategorized",
    "men's grooming":                         "Other / Uncategorized",
    "k-beauty":                               "Other / Uncategorized",
    "only at sephora":                        "Other / Uncategorized",
    "new arrivals":                           "Other / Uncategorized",
    "new":                                    "Other / Uncategorized",
    "makeup":                                 "Other / Uncategorized",
    "dermatologist recommended":              "Other / Uncategorized",
    "nan":                                    "Other / Uncategorized",
    "none":                                   "Other / Uncategorized",
}

_CANONICAL_LOWER = [c.lower() for c in CANONICAL_CATEGORIES]


def normalize_category(raw: str) -> str:
    key = str(raw).strip().lower()
    if key in _CATEGORY_MAP:
        return _CATEGORY_MAP[key]
    # Fuzzy fallback for anything not explicitly mapped
    result = process.extractOne(
        key,
        _CANONICAL_LOWER,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=CATEGORY_FUZZY_THRESHOLD,
    )
    if result is not None:
        return CANONICAL_CATEGORIES[_CANONICAL_LOWER.index(result[0])]
    return "Other / Uncategorized"


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

    sephora["product_name"] = sephora["product_name"].apply(
        lambda n: n.rsplit(" - ", 1)[0].strip() if isinstance(n, str) and " - " in n else n
    )

    print(f"  Sephora : {len(sephora):>5} products")
    print(f"  Ulta    : {len(ulta):>5} products")

    print("Normalising categories …")
    for df, label in [(sephora, "Sephora"), (ulta, "Ulta")]:
        before     = df["category"].astype(str)
        normalised = before.apply(normalize_category)
        df["category"] = normalised
        n_other    = (normalised == "Other / Uncategorized").sum()
        still_raw  = normalised[~normalised.isin(CANONICAL_CATEGORIES)].unique()
        print(f"  {label}: Other/Uncategorized={n_other} | "
              f"still outside canonical list={len(still_raw)}")
        for v in sorted(still_raw)[:10]:
            print(f"    → '{v}'")

    print("Building match keys …")
    for df in [sephora, ulta]:
        df["_key"] = df.apply(
            lambda r: _match_key(str(r.get("brand", "")), str(r.get("product_name", ""))),
            axis=1,
        )

    print("Running fuzzy deduplication …")
    duplicate_sephora_indices = []
    ulta_keys = ulta["_key"].tolist()
    for idx, key in enumerate(sephora["_key"]):
        result = process.extractOne(
            key, ulta_keys,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        if result is not None:
            duplicate_sephora_indices.append(idx)

    n_dupes = len(duplicate_sephora_indices)
    print(f"  Found {n_dupes} Sephora duplicates of Ulta products — dropping.")

    sephora_unique = sephora.drop(index=duplicate_sephora_indices).copy()
    combined = pd.concat([ulta, sephora_unique], ignore_index=True)
    combined.drop(columns=["_key"], inplace=True)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)

    print(f"\nCombined : {len(combined):>5} products  →  {output_path}")
    print(f"Categories ({combined['category'].nunique()}):")
    print(combined["category"].value_counts().to_string())
    return combined


if __name__ == "__main__":
    combine()