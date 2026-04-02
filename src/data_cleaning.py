"""
data_cleaning.py
----------------
Cleans Sephora and Ulta product CSVs.

Pipeline per ingredients cell
------------------------------
 1.  Collapse embedded newlines / carriage returns
 1b. Decode HTML entities  (&nbsp; → space, etc.)
 1c. Strip stray leading quote character  (" or ')
 2.  Strip "INGREDIENTS :" / "ingredients/ingrédients:" labels
 3.  Normalise bullet separators  (● → comma)
 4.  Strip shade/colour-name prefix  ("01 Always Red - Isododecane, …")
 5.  Strip marketing blurbs  ("- Description text.")
 6.  Lowercase
 7.  Normalise whitespace
 7b. Per-token cleaning (splits on comma, cleans each token, rejoins):
       • Decode any residual HTML entities inside tokens
       • Strip leading quote chars  (" ')
       • Strip #number shade prefixes  (#01:, #09247/a, #16153 …)
       • Strip parenthetical section headers
           (+/ shade name: ingredient  →  ingredient
           (* step 2: water            →  water
           ( base coat inci:) x        →  x
           ( theobroma cacao )         →  theobroma cacao
           lone ( or (+/              →  dropped
       • Strip multi-word product-name-colon prefixes
           curl gel: water/aqua/eau   →  water/aqua/eau
           'replica' fragrance: alcohol → alcohol
 8.  Strip stray leading/trailing commas
 9.  Collapse double commas
"""

import html as _html_module
import re
import sys
from io import StringIO
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_newlines(text: str) -> str:
    return re.sub(r"[\r\n]+", " ", text)


def _decode_html_entities(text: str) -> str:
    """Unescape HTML entities left by the scraper (&nbsp;, &amp;, &#160; …)."""
    return _html_module.unescape(text)


def _strip_leading_quote(text: str) -> str:
    """Remove a stray opening straight or curly quote from the whole string."""
    return re.sub(r'^["\'\u2018\u2019\u201c\u201d]+', '', text)


def _strip_ingredients_label(text: str) -> str:
    """
    Remove leading label variants:
      "INGREDIENTS :"
      "Ingredients:"
      "ingredients/ingrédients:"   ← new: bilingual label
    """
    return re.sub(
        r'(?i)^ingr[e\u00e9]dients?(?:/[^\s:,]+)?\s*:\s*',
        '',
        text,
    )


def _normalize_bullet_separators(text: str) -> str:
    return re.sub(r"\s*●\s*", ", ", text)


def _remove_color_prefix(text: str) -> str:
    """
    Strip shade/colour-name prefixes that precede the INCI list, e.g.:
      "01 Always Red - Isododecane, …"
    Pattern: no-comma text followed by " - " and an uppercase letter.
    """
    return re.sub(r"^[^,]+ - (?=[A-Z])", "", text)


def _remove_blurbs(text: str) -> str:
    """
    Remove marketing blurbs of the form "- Description text."
    See original docstring for full explanation of the regex logic.
    """
    pattern = re.compile(
        r"(?<!\w)-"
        r"\s*"
        r".+?"
        r"\."
        r"(?!\s*,)"
        r"(?=\s|$)",
        re.DOTALL,
    )
    prev = None
    while prev != text:
        prev = text
        text = pattern.sub("", text)
    return text


# ---------------------------------------------------------------------------
# Per-token cleaning  (applied after splitting on commas, post-lowercase)
# ---------------------------------------------------------------------------

def _clean_token(token: str) -> str:
    """
    Clean a single comma-separated ingredient token.

    Order matters — each step may expose the next pattern.

    Patterns handled
    ----------------
    #01: mica                                    →  mica
    #09247/a alcohol                             →  alcohol      (CI color code)
    #16153 aqua (water)                          →  aqua (water)
    "aqua/water/eau                              →  aqua/water/eau
    'serine                                      →  serine
    &nbsp; glycerin                              →  glycerin  (entities decoded earlier)
    & red 7 (ci 15850)                           →  red 7 (ci 15850)
    & xanthan gum                                →  xanthan gum
    ( base coat inci:) di-hema …                 →  di-hema …
    (* step 2: water/aqua/eau                    →  water/aqua/eau
    (+/ all access: kaolin                       →  kaolin
    (+/-): titanium dioxide ci 77891             →  titanium dioxide ci 77891
    (+/ the list of ingredients is subject …     →  ''  (dropped — disclaimer)
    ( theobroma cacao )                          →  theobroma cacao
    (   (lone bracket)                           →  ''  (dropped)
    (+/                                          →  ''  (dropped)
    (1%); hydrolyzed pea protein                 →  hydrolyzed pea protein
    (1) polybutylene terephthalate               →  polybutylene terephthalate
    curl gel: water/aqua/eau                     →  water/aqua/eau
    'replica' beach walk … : alcohol             →  alcohol
    ingredients/ingrédients: alcohol             →  alcohol  (belt-and-suspenders)
    """
    token = token.strip()
    if not token:
        return ''

    # ── 1. Residual HTML entities ──────────────────────────────────────────
    token = _html_module.unescape(token)

    # ── 2. Drop disclaimer / legal notice tokens ───────────────────────────
    _DISCLAIMERS = (
        "list of ingredients is subject to change",
        "subject to change",
        "please consult the packaging",
        "may vary",
        "ingredients may vary",
    )
    tl = token.lower()
    if any(d in tl for d in _DISCLAIMERS):
        return ''

    # ── 3. Strip leading quote characters ──────────────────────────────────
    token = re.sub(r'^["\'\u2018\u2019\u201c\u201d]+', '', token)

    # ── 4. Strip leading & or + continuation operators ─────────────────────
    #   "& red 7 (ci 15850)"  →  "red 7 (ci 15850)"
    #   "+ xanthan gum"       →  "xanthan gum"
    token = re.sub(r'^[&+]\s+', '', token)

    # ── 5. Strip CI / shade-number prefixes ────────────────────────────────
    #   #01:  #09247/a  #16153  (with or without colon/space)
    token = re.sub(r'^#\d+(?:/\w+)?\s*:?\s*', '', token)

    # ── 6. Strip parenthetical section/shade headers ───────────────────────
    #   (+/ shade:   (* step:   ( section:)   (unclosed)
    token = re.sub(r'^\([\+\*]?[-/+]*[^):]*:\)?\s*', '', token)

    # ── 6b. Strip closed-paren-then-colon prefixes ─────────────────────────
    #   (+/-): titanium dioxide ci 77891  →  titanium dioxide ci 77891
    #   Only fires when ')' is followed by ':' (distinguishes from step 8).
    token = re.sub(r'^\([^)]*\)\s*:\s*', '', token)

    # ── 7. Strip concentration / numbered-list prefixes ────────────────────
    #   (1%); hydrolyzed pea protein   →  hydrolyzed pea protein
    #   (1) polybutylene terephthalate →  polybutylene terephthalate
    #   (0.5%) retinol                 →  retinol
    token = re.sub(r'^\(\d+\.?\d*%?\)\s*;?\s*', '', token)

    # ── 8. Unwrap balanced parentheticals with no colon ────────────────────
    #   ( theobroma cacao )  →  theobroma cacao
    m = re.match(r'^\(\s*([^()]+?)\s*\)$', token)
    if m:
        token = m.group(1).strip()

    # ── 9. Drop tokens that are only stray bracket/operator chars ──────────
    if re.match(r'^[\(\)\+\*/\\&-]+\s*$', token):
        return ''

    # ── 10. Strip product-name-colon prefix ────────────────────────────────
    #   Fires when prefix contains ≥1 space and is followed by ': ' + letter.
    #   Catches:  curl gel: …   'replica' beach walk: …   shampoo name: water …
    token = re.sub(r'^[^,]*\s[^,:]*:\s+(?=[a-z])', '', token)

    return token.strip()


# ---------------------------------------------------------------------------
# Main cleaning pipeline
# ---------------------------------------------------------------------------

def clean_ingredients(raw) -> "str | float":
    if pd.isna(raw) or not isinstance(raw, str):
        return float("nan")

    text = raw

    # 1. Collapse multi-line content
    text = _normalize_newlines(text)

    # 1b. Decode HTML entities
    text = _decode_html_entities(text)

    # 1c. Strip stray leading quote
    text = _strip_leading_quote(text)

    # 2. Remove leading "INGREDIENTS :" label (including bilingual variant)
    text = _strip_ingredients_label(text)

    # 3. Normalise bullet separators  (● → comma)
    text = _normalize_bullet_separators(text)

    # 4. Strip shade/color-name prefix  ("01 Always Red - Isododecane, …")
    text = _remove_color_prefix(text)

    # 5. Strip marketing blurbs  ("- Description text.")
    text = _remove_blurbs(text)

    # 6. Lowercase
    text = text.lower()

    # 7. Normalise whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # 7b. Per-token cleaning
    tokens = [_clean_token(t) for t in text.split(",")]
    tokens = [t for t in tokens if t.strip()]
    text   = ", ".join(tokens)

    # 8. Strip stray leading/trailing commas
    text = text.strip(",").strip()

    # 9. Collapse double commas produced by blurb/token removal
    text = re.sub(r",\s*,+", ",", text)

    return text if text else float("nan")


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

_N_COLS = 8


def load_csv(path: "str | Path") -> pd.DataFrame:
    """
    Load a CSV, correctly handling two real-world scrape artefacts:

    1. Mixed / embedded line terminators
    2. Unquoted ingredients field (causes extra columns)
    """
    import csv as _csv

    raw_text = Path(path).read_bytes().decode("utf-8")

    reader  = _csv.reader(StringIO(raw_text))
    out_buf = StringIO()
    writer  = _csv.writer(out_buf, quoting=_csv.QUOTE_ALL)

    for row in reader:
        if len(row) > _N_COLS:
            row = row[: _N_COLS - 1] + [",".join(row[_N_COLS - 1 :])]
        writer.writerow(row)

    out_buf.seek(0)
    return pd.read_csv(out_buf)


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["ingredients"]).copy()
    df = df[df["ingredients"].astype(str).str.strip() != ""]
    df["ingredients"] = df["ingredients"].apply(clean_ingredients)
    df = df.dropna(subset=["ingredients"])
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT    = Path(__file__).resolve().parent.parent
RAW_DIR       = _REPO_ROOT / "data" / "raw"
PROCESSED_DIR = _REPO_ROOT / "data" / "processed"


def main(
    sephora_path: "str | Path" = RAW_DIR / "sephora_products.csv",
    ulta_path:    "str | Path" = RAW_DIR / "ulta_products.csv",
    output_dir:   "str | Path" = PROCESSED_DIR,
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading data …")
    sephora = load_csv(sephora_path)
    ulta    = load_csv(ulta_path)

    print("Cleaning Sephora products …")
    sephora_clean = clean_products(sephora)

    print("Cleaning Ulta products …")
    ulta_clean = clean_products(ulta)

    sephora_out = output_dir / "sephora_products_clean.csv"
    ulta_out    = output_dir / "ulta_products_clean.csv"

    sephora_clean.to_csv(sephora_out, index=False)
    ulta_clean.to_csv(ulta_out,    index=False)

    print(
        f"\nSephora : {len(sephora):>5} → {len(sephora_clean):>5} products  "
        f"({len(sephora) - len(sephora_clean)} dropped)  →  {sephora_out}"
    )
    print(
        f"Ulta    : {len(ulta):>5} → {len(ulta_clean):>5} products  "
        f"({len(ulta) - len(ulta_clean)} dropped)  →  {ulta_out}"
    )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) >= 3:
        _output = sys.argv[3] if len(sys.argv) > 3 else PROCESSED_DIR
        main(sys.argv[1], sys.argv[2], _output)
    else:
        print(
            "Usage:\n"
            "  python data_cleaning.py\n"
            "  python data_cleaning.py <sephora> <ulta> [outdir]"
        )
        sys.exit(1)