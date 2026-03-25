"""
data_cleaning.py
----------------
Cleans Sephora and Ulta product CSVs:
  - Drops products with missing or empty ingredients
  - Strips leading "INGREDIENTS :" / "Ingredients:" labels
  - Normalises bullet-point separators (● → comma)
  - Strips shade/colour-name prefixes (e.g. "01 Always Red - Isododecane, ...")
  - Removes marketing blurbs from ingredient strings
      Blurbs: start with "- " (hyphen + space) and end with "."
      INCI hyphens (e.g. "PPG-10/1", "N-Prolyl") are mid-word and never
      followed by a space, so the regex targets only blurb hyphens.
  - Normalises multi-line ingredient cells (collapses \\r / \\n to a space)
  - Lowercases all ingredient strings
  - Fixes the pandas ParserWarning about mixed line-terminators
        by normalising line endings in raw bytes before parsing

Usage
-----
    python data_cleaning.py <sephora_csv> <ulta_csv> [output_dir]

Outputs
-------
    <output_dir>/sephora_products_clean.csv
    <output_dir>/ulta_products_clean.csv
"""

import re
import sys
from io import StringIO
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_newlines(text: str) -> str:
    """Collapse \\r\\n / \\r / \\n into a single space."""
    return re.sub(r"[\r\n]+", " ", text)


def _strip_ingredients_label(text: str) -> str:
    """
    Remove leading "INGREDIENTS :" / "Ingredients:" / "ingredients :" labels.

    Some Ulta/retailer listings prepend the column name inline, e.g.:
        "INGREDIENTS : Polyglyceryl-2 Triisostearate ● ..."
        "Ingredients: Aqua, Glycerin, ..."

    Pattern: case-insensitive "ingredients" followed by optional whitespace,
    a colon, and more optional whitespace — anchored to the start of the string.
    """
    return re.sub(r"(?i)^ingredients\s*:\s*", "", text)


def _normalize_bullet_separators(text: str) -> str:
    """
    Replace bullet-point separators (●) with commas.

    Some products use ● as the ingredient separator instead of commas, e.g.:
        "Polyglyceryl-2 Triisostearate ● Polybutene ● Pentaerythrityl ..."

    Normalises to standard comma-separated INCI format.
    Surrounding whitespace around the bullet is collapsed into ", ".
    """
    return re.sub(r"\s*●\s*", ", ", text)


def _remove_color_prefix(text: str) -> str:
    """
    Remove shade/color-name prefixes that precede the INCI ingredient list.

    Some products encode the shade inline with the ingredients, e.g.:
        "01 Always Red - Isododecane, Disteardimonium Hectorite, ..."
        "Berry Bliss - Aqua (Water/Eau), Glycerin, ..."

    Pattern: from the start of the string, any text that contains no commas
    (commas are the INCI list separator and cannot appear in a shade name),
    followed by " - ", where the very next character is an uppercase letter
    (start of the first INCI ingredient name).

    Products without a shade prefix are unaffected.
    """
    return re.sub(r"^[^,]+ - (?=[A-Z])", "", text)


def _remove_blurbs(text: str) -> str:
    """
    Remove marketing blurbs of the form:
        "- Description text ending in a full sentence."

    Examples:
        "- Stabilized Vitamins C + E: Helps visibly brighten skin."
        "- Five Unique Types of Hyaluronic Acid: Sustained hydration."

    Two subtleties handled:

    1.  INCI hyphens (e.g. "N-Prolyl", "PPG-10/1", "caprylic/capric") are always
        mid-word — preceded by a letter or digit. The blurb marker uses a
        negative lookbehind (?<![\\w])- so only hyphens at the start of the
        string or after whitespace are treated as blurb markers. No-space blurbs
        like "-coconut: rich in fatty acids." are now caught too.

    2.  Ingredient abbreviations contain periods too (e.g. "Alcohol Denat.",
        "ext.", "no.").  The body uses non-greedy .+? to span them.

        The sentence-ending period is identified by a lookahead:
            \\s*$       — end of string
            \\s*-\\s  — another blurb immediately follows
            \\s+[A-Z]   — a capitalised word follows (start of ingredient list)

        Key insight: INCI abbreviation periods are ALWAYS followed by a comma
        because commas are the INCI list separator ("Alcohol Denat., Aqua").
        So the sentence-end test is simply: period NOT followed by a comma.
        This works regardless of case, so blurbs in all-lowercase raw data
        (e.g. "-coconut: rich in fatty acids.") are caught just as reliably
        as title-case blurbs.

    The pattern is applied repeatedly until stable to handle back-to-back blurbs.
    """
    pattern = re.compile(
        r"(?<!\w)-"      # blurb start: hyphen NOT preceded by a word char
        r"\s*"           # optional space (handles both "- text" and "-text")
        r".+?"            # blurb body (non-greedy; abbreviation periods OK)
        r"\."             # a period …
        r"(?!\s*,)"      # … NOT followed by a comma — commas only follow
                         # abbreviation periods in INCI lists (e.g. "Denat., Aqua")
                         # a period without a trailing comma is always a sentence end
        r"(?=\s|$)",     # … followed by whitespace or end-of-string
        re.DOTALL,
    )
    prev = None
    while prev != text:
        prev = text
        text = pattern.sub("", text)
    return text


def clean_ingredients(raw) -> str | float:
    """
    Full cleaning pipeline for a single ingredients cell.
    Returns float NaN if the value is empty / NaN after cleaning.
    """
    if pd.isna(raw) or not isinstance(raw, str):
        return float("nan")

    text = raw

    # 1. Collapse multi-line content
    text = _normalize_newlines(text)

    # 2. Remove leading "INGREDIENTS :" / "Ingredients:" label
    text = _strip_ingredients_label(text)

    # 3. Normalise bullet separators (● → ,)
    text = _normalize_bullet_separators(text)

    # 4. Strip shade/color-name prefix ("01 Always Red - Isododecane, ...")
    text = _remove_color_prefix(text)

    # 5. Strip marketing blurbs ("- Description text.")
    text = _remove_blurbs(text)

    # 6. Lowercase
    text = text.lower()

    # 7. Normalise whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # 8. Remove stray leading/trailing commas that may remain after blurb removal
    text = text.strip(",").strip()

    # 9. Collapse double commas produced by blurb removal
    text = re.sub(r",\s*,+", ",", text)

    return text if text else float("nan")


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

# Expected number of columns in both CSVs (0-indexed: 0..7, ingredients = col 7)
_N_COLS = 8


def load_csv(path: str | Path) -> pd.DataFrame:
    """
    Load a CSV, correctly handling two real-world scrape artefacts:

    1. Mixed / embedded line terminators
       Ingredient cells sometimes contain literal \r\n or \r (e.g. multi-line
       blurbs), while the row separator may be different.  We read raw bytes and
       let csv.reader parse the quoted fields first — embedded newlines inside a
       quoted field are preserved and not mistaken for row boundaries.

    2. Unquoted ingredients field
       Some rows omit quotes around the ingredients column even though it
       contains commas, causing pandas to see far more than 8 fields.
       csv.reader parses properly-quoted rows correctly; for rows that yield
       more than _N_COLS fields (ingredients were unquoted and got split),
       we rejoin everything from column index 7 onward into a single string.
       csv.writer re-emits every row with full RFC 4180 quoting so pandas
       always receives a clean, consistent file.
    """
    import csv as _csv

    raw_text = Path(path).read_bytes().decode("utf-8")

    reader  = _csv.reader(StringIO(raw_text))
    out_buf = StringIO()
    writer  = _csv.writer(out_buf, quoting=_csv.QUOTE_ALL)

    for row in reader:
        if len(row) > _N_COLS:
            # Ingredients were unquoted — commas inside them split the row.
            # Rejoin everything from col 7 onward into a single field.
            row = row[: _N_COLS - 1] + [",".join(row[_N_COLS - 1 :])]
        writer.writerow(row)

    out_buf.seek(0)
    return pd.read_csv(out_buf)
def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Drop ingredient-less rows and apply ingredient cleaning."""
    # Drop rows where ingredients is missing or blank
    df = df.dropna(subset=["ingredients"]).copy()
    df = df[df["ingredients"].astype(str).str.strip() != ""]

    # Clean ingredients
    df["ingredients"] = df["ingredients"].apply(clean_ingredients)

    # Drop rows whose ingredients became empty after blurb removal
    df = df.dropna(subset=["ingredients"])

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Paths are anchored to the repo root (one level up from src/)
# so the script works regardless of which directory you run it from.
_REPO_ROOT    = Path(__file__).resolve().parent.parent
RAW_DIR       = _REPO_ROOT / "data" / "raw"
PROCESSED_DIR = _REPO_ROOT / "data" / "processed"


def main(
    sephora_path: str | Path = RAW_DIR / "sephora_products.csv",
    ulta_path:    str | Path = RAW_DIR / "ulta_products.csv",
    output_dir:   str | Path = PROCESSED_DIR,
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
        # No arguments — use the hardcoded defaults above
        main()
    elif len(sys.argv) >= 3:
        _output = sys.argv[3] if len(sys.argv) > 3 else PROCESSED_DIR
        main(sys.argv[1], sys.argv[2], _output)
    else:
        print(
            "Usage:\n"
            "  python data_cleaning.py                           # uses data/raw/ defaults\n"
            "  python data_cleaning.py <sephora> <ulta> [outdir]"
        )
        sys.exit(1)