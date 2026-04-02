"""
clean_ingredients.py
Cleans the 'ingredients' column in Sephora / Ulta product CSVs.

Usage (from your project root):
    python src/clean_ingredients.py                          # uses defaults below
    python src/clean_ingredients.py --sephora path/to/s.csv --ulta path/to/u.csv
    python src/clean_ingredients.py --combined path/to/combined_products.csv

The cleaner works in two passes:
  1. Row-level  – strips marketing blurbs, shade prefixes, disclaimers, etc.
  2. Ingredient-level – after comma-splitting, validates and cleans each token.
"""

import re
import argparse
import pandas as pd
from pathlib import Path

# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────

# Whole-row patterns → set ingredients to NaN
NON_INGREDIENT_ROW = [
    r'(?i)(ask a question|\$\d+\.\d+|payments of|Auto-Replenish)',
    r'(?i)^see packaging for details\.?$',
    r'(?i)^sponge\s*:',
    r'(?i)^bristles\s*:',
    r'(?i)^ferrule\s*:',
    r'(?i)^handle\s*:',
    r'(?i)^box\s*:',
    r'(?i)^\d+\s*%\s+(cotton|nylon|polyester|spandex|medical\s+grade)',
    r'(?i)^100\s*%\s+(certified\s+organic\s+cotton|medical\s+grade)',
    r'(?i)^thermoplastic\s+elastomer',
    r'(?i)^stainless\s+steel',
    r'(?i)^synthetic\s+(dual\s+)?fibers?$',
    r'(?i)^ps\s+\(polystyrene\)',
    r'[％﹪]',                            # fullwidth percent = textile/garment material
    r'(?i)^(bonnet|bow\s+filling|shell|lining)\s*[:\-]',
    r'(?i)^\d+\s*%\s*(recycled|woven|knitted|satin|velvet|silk\b)',
]

# Known Water/Aqua/Eau variants → normalise to single form
WATER_ALTS = [
    (r'(?i)\bAqua\s*/\s*Water\s*/\s*Eau\b',       'Aqua'),
    (r'(?i)\bWater\s*/\s*Aqua\s*/\s*Eau\b',       'Water'),
    (r'(?i)\bWater\s*/\s*Eau\s*/\s*Aqua\b',       'Water'),
    (r'(?i)\bEau\s*/\s*Aqua\s*/?\s*(Water)?\b',   'Aqua'),
    (r'(?i)\bWater\s*\(Aqua\)(\s*\(Eau\))?',      'Water'),
    (r'(?i)\bWater\s*\(Aqua,?\s*Eau\)',            'Water'),
    (r'(?i)\bAqua\s*\(Water,?\s*Eau?\)',           'Aqua'),
    (r'(?i)\bAqua\s*\(Water\)',                    'Aqua'),
    (r'(?i)\bWater\s*\(Eau\)(\s*\(Aqua\))?',      'Water'),
    (r'(?i)\bWater\s*/\s*Eau\b',                   'Water'),
    (r'(?i)\bWater\s*/\s*Eau\s*\(Aqua\)',          'Water'),
]

# Other known slash/paren alternate-name pairs
OTHER_ALTS = [
    (r'(?i)\bMicrocrystalline\s+Wax\s*/\s*Cera\s+Microcristallina\s*/?\s*(Cire\s+Microcristalline?)?\b',
     'Microcrystalline Wax'),
    (r'(?i)\bCera\s+Microcristallina\s*/\s*Microcrystalline\s+Wax\s*/?\s*(Cire\s+Microcristalline?)?\b',
     'Microcrystalline Wax'),
    (r'(?i)\bCire\s+Microcristalline?\s*/\s*(Cera\s+Microcristallina\s*/?\s*)?(Microcrystalline\s+Wax)?\b',
     'Microcrystalline Wax'),
    (r'(?i)\bCera\s+Microcristallina\b',           'Microcrystalline Wax'),
    (r'(?i)\bParfum\s*/\s*Fragrance\b',            'Fragrance'),
    (r'(?i)\bFragrance\s*/\s*Parfum\b',            'Fragrance'),
    (r'(?i)\bParfum\s*\(Fragrance\)',              'Fragrance'),
    (r'(?i)\bFragrance\s*\(Parfum\)',              'Fragrance'),
    (r'(?i)\bFlavor\s*/\s*Aroma\b',               'Flavor'),
    (r'(?i)\bAroma\s*/\s*Fragrance\b',             'Fragrance'),
    (r'(?i)\bColophonium\s*/\s*Rosin\s*/?\s*(Colophane)?\b',  'Colophonium'),
    (r'(?i)\bRosin\s*/\s*Colophonium\s*/?\s*(Colophane)?\b',  'Colophonium'),
    (r'(?i)\bSd\s*Alcohol\s*40-B\s*/\s*Alcohol\s*Denat\.?\b', 'Alcohol Denat.'),
    (r'(?i)\bAlcohol\s*Denat\.?\s*/\s*Sd\s*Alcohol\b',        'Alcohol Denat.'),
]

KNOWN_ALTS = WATER_ALTS + OTHER_ALTS

# Descriptor words that appear inside parentheses but are NOT part of the INCI name
DESCRIPTOR_PARENS = re.compile(
    r'\s*\(\s*(?:'
    r'organic|certified\s+organic|usda\s+organic|ecocert|rspo\s+certified'
    r'|emollient|humectant|solvent|antistatic(?:\s+agent)?|preservative'
    r'|antiperspirant|sunscreen|antioxidant|conditioner|thickener|stabilizer'
    r'|skin\s+conditioner|film\s+former|viscosity\s+agent|surfactant'
    r'|to\s+prevent\s+sticking|natural\s+deodori[sz]er'
    r'|f\.i\.l\.[^)]*'          # F.I.L. codes
    r'|d\d{5,}[^)]*'            # lot/batch codes like D268223/1
    r'|b/\d[^)]*'               # B/051022 etc
    r'|v/\d[^)]*'               # V/111424a etc
    r'|il[nN]\d{4,}[^)]*'       # ILN codes
    r'|[a-z]\d{6,}[^)]*'        # arbitrary internal codes starting letter+digits
    r'|w/w|d\.v\.'              # w/w percentage, daily value
    r')\s*\)',
    re.IGNORECASE,
)

# ──────────────────────────────────────────────────────────────
# CASE NORMALISATION + SYNONYM MAP
# Applied as the final step in _clean_single so all case variants
# and true synonyms collapse to one canonical form for matching.
# ──────────────────────────────────────────────────────────────

_ACRONYMS = {
    'ci', 'peg', 'ppg', 'bht', 'bha', 'edta', 'dna', 'rna',
    'vp', 'va', 'uv', 'uva', 'uvb', 'sls', 'sles', 'amp',
    'hdi', 'mdi', 'pdrn', 'aha', 'dha', 'epa',
}

# Detects INCI botanical names following "Genus Species" convention.
# Matches both "Butyrospermum parkii" (raw INCI) and "Butyrospermum Parkii" (title-cased).
# Genus must be 2+ syllables (≥5 chars) to avoid false positives on short English words.
# Latin binomial: genus (≥5 lower chars, no hyphen) + species (≥4 lower chars)
# Works on lowercase strings (all ingredients are lowercased before this point).
_LATIN_BINOMIAL = re.compile(r'^[a-z]{5,}\s+[a-z]{4,}')



# ──────────────────────────────────────────────────────────────
# INCI TYPO CORRECTION MAP
# Maps misspelled/variant INCI prefixes to their correct forms.
# Applied before paren-stripping so the canonical base name is used.
# Key: lowercase regex pattern   Value: correct replacement string
# ──────────────────────────────────────────────────────────────
INCI_TYPO_MAP = [
    # Helianthus annuus (Sunflower) Seed Oil — many scraper typos
    (r'\bhelianthus\s+ann[iu]{1,3}[js]?\b', 'helianthus annuus'),
    # Butyrospermum parkii (Shea Butter)
    (r'\bbutyrospermum\s+park[iai\"\'f]+\b', 'butyrospermum parkii'),
    # Rosmarinus officinalis (Rosemary)
    (r'\brosmarinus\s+o[fc][if]?c?i?n?a?l?i?s?\b', 'rosmarinus officinalis'),
    (r'\brosmarinus\s+of_?cinalis\b', 'rosmarinus officinalis'),
    # Prunus amygdalus dulcis (Sweet Almond)
    (r"\bprunus\s+amygdal[au][s']?\b", 'prunus amygdalus'),
    (r'\bprunus\s+amygda\b', 'prunus amygdalus'),
    # Cocos nucifera (Coconut)
    (r'\bcocos\s+nij?c[iu]fera\b', 'cocos nucifera'),
    # Camellia sinensis (Green Tea) — distinct from japonica/oleifera
    (r'\bcamellia\s+sinens[ie]s\b', 'camellia sinensis'),
    (r'\bcamellia\s+cinensi[s]?\b', 'camellia sinensis'),
    (r'\bcamellia\s+sinenesis\b', 'camellia sinensis'),
    # Oryza sativa (Rice)
    (r'\boryza\s+saliva\b', 'oryza sativa'),
    (r'\boryza\s+sati?va\b', 'oryza sativa'),
    # Benzyl alcohol typos
    (r'\bbenzyl\s+alcc?ohol\b', 'benzyl alcohol'),
    # Lavandula angustifolia (Lavender)
    (r'\blavandula\s+angustif[oi]l[ia]+\b', 'lavandula angustifolia'),
    (r'\blavandula\s+angustifola\b', 'lavandula angustifolia'),
    # Simmondsia chinensis (Jojoba)
    (r'\bsimmondsia\s+ch[ie]nensis\b', 'simmondsia chinensis'),
    # Tocopheryl acetate / tocopherol — preserve as-is (both valid)
    # Disodium EDTA
    (r'\bdisodium\s+e\.?d\.?t\.?a\.?\b', 'disodium edta'),
    # Phenoxyethanol
    (r'\bphenoxyethano[il]\b', 'phenoxyethanol'),
    # 2-hexanediol variants (catch any remaining)
    (r'\b2[\s\-\.]+hex[ae]ne?dio[il]?\b', '2-hexanediol'),
    # Sodium hyaluronate spacing
    (r'\bsodium\s+hyal[uo]r[ou]nate\b', 'sodium hyaluronate'),
    # Panthenol / panthenol
    (r'\bpante?nol\b', 'panthenol'),
    (r'\bpantothenic\s+acid\b', 'panthenol'),
]

# Patterns on the ingredient string that indicate trailing garbage
# after the real INCI name — strip from this point onward
_TRAILING_GARBAGE = re.compile(
    r'\s*(?:'
    # "(and) AnotherIngredient" — two ingredients concatenated
    r'\(and\)\s+[A-Za-z]'
    # "(oat milk)", "(honey extract)" — marketing parenthetical after INCI paren
    r'|\(\s*(?:oat|honey|argan)\s+(?:milk|water|oil|extract)\s*\)'
    # Bilingual slash: MUST have space before slash so "caprylic/capric" is safe
    # "X oil / huile de Y" or "X oil/Y Z" (space in the translated part)
    r'|\s+/\s*[a-z]{3,}\s+[a-z]{3,}'
    # "(6 ppm", "(a …" — trace annotation
    r'|\(\s*\d+\s*(?:ppm|ppb|%)'
    r'|\(\s*[a-z]\s+'
    r')',
    re.IGNORECASE,
)


def _apply_typo_corrections(s):
    """Apply INCI_TYPO_MAP corrections to an ingredient string."""
    for pattern, replacement in INCI_TYPO_MAP:
        s = re.sub(pattern, replacement, s, flags=re.IGNORECASE)
    return s


def _canonicalize_inci(s):
    """
    Normalise an ingredient name to its canonical INCI form.

    Steps:
      1. Invisible unicode / stray symbols
      2. Typo correction (genus/species misspellings)
      3. Trailing garbage annotation stripping
      4. Bilingual slash inside parens: (Sunflower/Tournesol) → (Sunflower)
      5. Organic/certified qualifier inside common-name paren
      6. Non-botanical: strip all trailing parentheticals (source annotations)
      7. Whitespace normalisation
    """
    if not s:
        return s

    # 1. Invisible Unicode + trailing stray symbols
    s = re.sub(r'[\u202c\u202d\u2060\u200b\u200e\u200f\ufeff\u202a\u202b]+', '', s)
    s = re.sub(r'[\s~#^`\u2018\u2019\u201c\u201d\u2019\u2018]+$', '', s).strip()
    # Normalize typography ligatures: ﬁ→fi, ﬂ→fl, ﬀ→ff, ﬃ→ffi, ﬄ→ffl
    s = s.replace('\ufb01', 'fi').replace('\ufb02', 'fl').replace('\ufb00', 'ff')
    s = s.replace('\ufb03', 'ffi').replace('\ufb04', 'ffl')
    # Fix missing space before "unsaponifiables" or other word glued to prior word
    s = re.sub(r'(?<=[a-z])(unsaponifiables|unsaponifiable)', r' \1', s)

    # 2. Typo corrections
    s = _apply_typo_corrections(s)

    # 3. Detect botanical vs non-botanical BEFORE stripping anything
    is_botanical = bool(_LATIN_BINOMIAL.match(s))

    # 4. Strip trailing garbage annotation — non-botanical only
    # For botanicals, "(sunflower)", "(rosemary)" etc. are the INCI common name and must stay.
    if not is_botanical:
        m = _TRAILING_GARBAGE.search(s)
        if m:
            s = s[:m.start()].strip().rstrip('.,;')

    if is_botanical:
        # ── BOTANICAL ─────────────────────────────────────────────────────
        # Known single-word common names that must be preserved (not treated as bilingual)
        _KEEP_COMMON = re.compile(
            r'^(sunflower|coconut|jojoba|rosemary|lavender|chamomile|calendula|'
            r'argan|shea|avocado|almond|castor|hemp|marula|moringa|baobab|'
            r'tamanu|bakuchiol|turmeric|ginger|peppermint|eucalyptus|tea\s+tree|'
            r'cloudberry|raspberry|blueberry|cranberry|pomegranate|papaya|mango|'
            r'carrot|tomato|lotus|orchid|jasmine|neroli|ylang|bergamot|rose|'
            r'gardenia|tiare|monoi|kukui|meadowfoam|safflower|chia|flax|grape|'
            r'olive|rice|corn|wheat|oat|barley|soy|soybean|sesame|aloe|'
            r'green\s+tea|black\s+tea|white\s+tea|rooibos|coffee|cacao|cocoa|'
            r'vanilla|lemon|lime|orange|grapefruit|mandarin|tangerine|peach|'
            r'apricot|cherry|plum|pomelo|yuzu|acai|goji|noni|sea\s+buckthorn|'
            r'borage|evening\s+primrose|carrot|pumpkin|watermelon|cucumber|'
            r'willow|birch|pine|cedar|sandalwood|frankincense|myrrh|patchouli'
            r')$',
            re.IGNORECASE,
        )

        # Fix "(shea butter)" → "(shea) butter"
        s = re.sub(r'\(shea\s+butter\)', '(shea) butter', s, flags=re.IGNORECASE)
        # Fix "(shea butter/beurre de karité)" → "(shea) butter"
        s = re.sub(r'\(shea\s+butter[^)]*\)', '(shea) butter', s, flags=re.IGNORECASE)

        def _strip_bilingual(m):
            inner = m.group(1).strip()
            if '/' in inner:
                parts = [p.strip() for p in inner.split('/', 1)]
                a, b = parts[0], parts[1]
                # Known translation words (some lack accents in scraped data)
                _translations = {'tournesol','huile','karité','karite','beurre','cire',
                                 'extrait','feuille','graine','fleur','peau','huiles',
                                 'noix','noyer','soja','avoine','mais','avocado'}
                is_bilingual = (
                    re.search(r'[àáâãäåæçèéêëìíîïðñòóôõöùúûüý]', b, re.I)
                    or ' ' in b
                    or b.lower() in _translations
                )
                if is_bilingual:
                    return f'({a})'
            # Strip organic/certified qualifier at start of paren
            inner = re.sub(
                r'(?i)^(?:organic|certified\s+organic|usda\s+organic|cold[- ]pressed|'
                r'unrefined|virgin|extra[- ]virgin|raw|fair\s+trade|wildcrafted|'
                r'fractionated|hydrogenated)\s+',
                '', inner).strip()
            # Normalize common OCR typos in the common name before keep-common check
            inner = re.sub(r'sun[fﬂ]?l[eo][wt]?h?[eaou][yr]r?', 'sunflower', inner, flags=re.I)
            inner = re.sub(r'sun[ow]+[ea]r', 'sunflower', inner, flags=re.I)
            # Strip single-word common name OCR typos — replace with correct form
            if _KEEP_COMMON.match(inner):
                # Normalize known OCR errors in common names
                inner = re.sub(r'sunfl[eo][wt]?h?e[yr]r?', 'sunflower', inner, flags=re.I)
                inner = re.sub(r'sun[fﬂ]lower', 'sunflower', inner, flags=re.I)
                inner = re.sub(r'sunow[ea]r', 'sunflower', inner, flags=re.I)
                inner = re.sub(r'sun[ﬂfl]?ow[ea]r', 'sunflower', inner, flags=re.I)
            return f'({inner})'

        s = re.sub(r'\(([^()]+)\)', _strip_bilingual, s)

        # Strip trailing garbage for botanicals:
        # "(and) AnotherIngredient" concatenation
        s = re.sub(r'\s*\(and\)\s+\S.*$', '', s, flags=re.IGNORECASE)
        # Slash alternate after the full name: "X oil / X oil" or "X/traduction"
        # Only strip if the part after / has a space (bilingual) or accent
        def _strip_trailing_slash(m):
            after = m.group(1)
            if ' ' in after or re.search(r'[àáâãäåæçèéêëìíîïðñòóôõöùúûüý]', after, re.I):
                return ''
            return m.group(0)  # keep chemical slashes like caprylic/capric
        s = re.sub(r'\s*/\s*([a-z].*)$', _strip_trailing_slash, s)
        # "[..." bracket artifacts: "X oil [X oil" → "X oil"
        s = re.sub(r'\s*\[.*$', '', s).strip()
        # Trailing period + junk: "X oil. [+/..." → "X oil"
        s = re.sub(r'\.\s*\[.*$', '', s).strip()
        s = re.sub(r'\.\s+\[?[+/].*$', '', s).strip()
        # Trailing unclosed paren junk: "(coconut", "(oat milk", "(a …"
        s = re.sub(r'\s*\([^)]{0,30}$', '', s).strip()
        # Trailing trademark/numeric paren
        s = re.sub(r'\s*\([A-Za-z0-9\-]+[®™]\)\s*$', '', s)
        s = re.sub(r'\s*\(\d[\d\s]*\)\s*$', '', s)
        # Strip trailing " organic" / " certified" word
        s = re.sub(r'(?i)\s+(organic|certified)\s*$', '', s)

    else:
        # ── NON-BOTANICAL ─────────────────────────────────────────────────
        # Strip everything from the first "(" onward (source annotation, not INCI)
        # unless the token STARTS with a paren like "(F)D&C Yellow"
        if '(' in s:
            before = s[:s.index('(')].rstrip()
            if len(re.sub(r'\s', '', before)) >= 3:
                s = before
        # Strip slash alternates: "alcohol denat. / sd alcohol 40-b" → "alcohol denat."
        # Require space before slash so "caprylic/capric" is preserved
        s = re.sub(r'\s+/\s*\S.*$', '', s)
        # Strip trailing bare slash: "cocos nucifera oil/" → "cocos nucifera oil"
        s = s.rstrip('/').strip()
        # Strip semicolon concatenations: "benzyl alcohol; fragrance" → "benzyl alcohol"
        s = re.sub(r'\s*;.*$', '', s)
        # Strip "[..." bracket artifacts
        s = re.sub(r'\s*\[.*$', '', s).strip()
        # Strip known junk suffixes
        s = re.sub(r'(?i)(ur\s+healthcare|le\s+baume\b)', '', s).strip()

    # Whitespace normalisation
    s = re.sub(r'\(\s+', '(', s)
    s = re.sub(r'\s+\)', ')', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()




# lowercase key → canonical display form
SYNONYM_MAP = {
    # Water / Aqua / Eau
    'aqua':                         'water',
    'eau':                          'water',
    'aqua (water)':                 'water',
    'water (aqua)':                 'water',
    'aqua/water/eau':               'water',
    'water/aqua/eau':               'water',
    'eau/aqua/water':               'water',
    # Glycerin
    'glycerine':                    'glycerin',
    'glycerol':                     'glycerin',
    # Fragrance
    'parfum':                       'fragrance',
    'aroma':                        'fragrance',
    # Microcrystalline Wax
    'cera microcristallina':        'microcrystalline wax',
    'cire microcristalline':        'microcrystalline wax',
    # Alcohol
    'alcohol denat':                'alcohol denat.',
    'denatured alcohol':            'alcohol denat.',
    'sd alcohol 40-b':              'alcohol denat.',
    'ethanol':                      'alcohol',
    # Vitamins / common actives
    'nicotinamide':                 'niacinamide',
    'pantothenol':                  'panthenol',
    'd-panthenol':                  'panthenol',
    'dl-panthenol':                 'panthenol',
    'vitamin e':                    'tocopherol',
    'alpha-tocopherol':             'tocopherol',
    'vitamin c':                    'ascorbic acid',
    'vitamin a':                    'retinol',
    'hyaluronic acid':              'sodium hyaluronate',
    # Waxes / botanicals
    'cera alba':                    'beeswax',
    'carnauba':                     'copernicia cerifera (carnauba) wax',
    'carnuba wax':                  'copernicia cerifera (carnauba) wax',
    'rosin':                        'colophonium',
    'colophane':                    'colophonium',
    # Aloe
    'aloe vera':                    'aloe barbadensis leaf juice',
    'aloe barbadensis':             'aloe barbadensis leaf juice',
    # Colorants
    'iron oxide':                   'iron oxides',
    'mica ci 77019':                'mica',
    'titanium dioxide ci 77891':    'titanium dioxide',
    'zinc oxide ci 77947':          'zinc oxide',
    # 2-Hexanediol variants
    '2-hexanediol':                 '2-hexanediol',
    '2 hexanediol':                 '2-hexanediol',
    '2hexanediol':                  '2-hexanediol',
    '2- hexanediol':                '2-hexanediol',
    '2 - hexanediol':               '2-hexanediol',
    '2-- hexanediol':               '2-hexanediol',
    '2‑hexanediol':                 '2-hexanediol',
    '2?hexanediol':                 '2-hexanediol',
    '2 hexandiol':                  '2-hexanediol',
    '2 hexanedoil':                 '2-hexanediol',
    '2-hexane-diol':                '2-hexanediol',
    '2-hexanedio':                  '2-hexanediol',
    '1-2 hexanediol':               '2-hexanediol',
    '1.2 hexanediol':               '2-hexanediol',
    '1.2-hexanediol':               '2-hexanediol',
    '1,2 hexanediol':               '2-hexanediol',
    '1,2-hexanediol':               '2-hexanediol',
    '2 hexene':                     '2-hexanediol',
    '2-hexandiol':                  '2-hexanediol',
    # Propanediol
    '3 propanediol':                'propanediol',
    '3-butanediol':                 '1,3-butanediol',
    '3 butanediol':                 '1,3-butanediol',
    # Shade concatenation artifacts
    '2water':                       'water',
    '2water / aqua / eau':          'water',
}

REJECT_INGREDIENT = [
    r'^[\s\(\)\[\]\{\}\+\-\.,;:&\|/\\#!?%@=~`^\'\"]*$',   # only punctuation/symbols
    r'^//?',                         # starts with / or // — MAY CONTAIN fragment
    r'^/-',                          # starts with /-
    r'^\+/?-',                       # +/- fragment
    r'^\d+\)',                        # "9))Tridecane" fragment
    r'^[)\]}>}]',                    # starts with closing bracket
    r'^&\s',                         # & continuation fragment
    r'^[A-Z]\d{4,}$',               # bare batch code e.g. B03877
    r'^[A-Z]/\d',                    # B/051022 style
    r'^V/\d',                        # V/111424
    r'^MN\s*\d+$',                   # MN 2, MN 5 (bare)
    r'^EE\d+$',                      # EE11, EE12 (bare)
    r'^\d+$',                        # bare number
    r'^\d+\.?\d*\s*%\s*$',          # bare percentage: "76%", "23%"
    r'^F\.I\.L\.',                   # bare F.I.L. code
    r'^\(F\.I\.L\.',                 # (F.I.L. code)
    r'^[0-9a-f]{8}-[0-9a-f]{4}',   # UUID
    r'^always\s+check',
    r'^rspo\s+certif',
    r'(?i)^(stainless\s+steel|synthetic\s+(dual\s+)?fibers?|rubber\s+latex)$',
    r'(?i)^(daily\s+value|dv\s+not\s+established)',
    r'(?i)^(caution|warning|do\s+not\s+use|for\s+external)',
    r'(?i)^(this\s+product|this\s+shade|this\s+formula)',
    r'(?i)www\.|\.com',
    # Measurement/concentration garbage
    r'(?i)^\d+\s*(ppm|ppb|ppt|iu|mg|mcg|gdu|mtu|nfu)\b',
    r'(?i)^[<>]?\d+\.?\d*\s*(ppm|ppb|%)\s*$',
    # Stereochemistry fragments: "3ar", "3as", "4a", "6s", "7r"
    r'^[0-9]{1,2}[a-z]{1,2}$',
    # Incomplete bracket fragments: "3z)-", "3r)-"
    r'^\d+[a-z]?\)[-\s]',
    # Disclaimer variants that slip through as individual tokens
    r'(?i)ingredient\s+lists?\s+and\s+claims\s+may\s+change',
    r'(?i)ingredient\s+lists?.+may\s+differ\b',
    r'(?i)ingredients?\s+listed\s+may\s+vary',
    r'(?i)\bplease\s+refer\s+to\s+the\s+ingredient\s+list\b',
    r'(?i)\balways\s+read\s+the\s+ingredients?\b',
    r'(?i)\bis\s+a\s+blend\s+of\s+botanical\b',
    r'(?i)\bpka\s+is\s+the\s+most\s+important\b',
    r'(?i)\bpermanently\s+labeled\b',
    r'(?i)\bare\s+updated\s+(from\s+time\s+to\s+time|periodically)\b',
    r'(?i)\bautomatically\s+(reduces|adjusts|controls)\b',
    r'(?i)^(refresh|retouch|resume)\s+mode\b',
    r'(?i)\bfor\s+(a\s+)?smoother\b|\bfor\s+a\s+dewy\b|\bfor\s+a\s+hydrat',
    r'(?i)\bcontains\s+glycerin\s+and\b',
    # Supplement dosage fragments
    r'(?i)^\d+\s+(billion|million|trillion)\b',
    r'(?i)^\d+\.?\d*\s*(billion|million)\s+(cfu|iu|mg|mcg)\b',
    # "Ingredients may vary in color and consistency" type disclaimers
    r'(?i)ingredients?\s+may\s+vary\s+in\s+(color|colour)',
    r'(?i)\bsome\s+ingredients?\s+may\s+vary\b',
    r'(?i)ingredient\s+list\s+shown\s+here\s+may',
    # Marketing verbs at the start of a sentence
    r'(?i)^helps?\s+(maintain|reduce|improve|prevent|support|boost|strengthen|protect|fight|repair)\b',
    r'(?i)^effectively\s+(removes?|reduces?|improves?|supports?)\b',
    r'(?i)^delivers?\s+(precise|customized|personalized|even|consistent|targeted|heat\b)\b',
    # Separator lines: "|---------..." or "==========..."
    r'^[|\-=_]{5,}',
    # Supplement dosage with units (Da=Daltons, kDa, nm)
    r'(?i)^\d+\.?\d*\s*(da|kda)\s+\w',
    r'(?i)^\d+\s+types?\s+of\b',
    # "000" garbage — catches "000pm", "000 Calorie Diet", "000: Water"
    r'^0{3,}',
    # All-digit lot codes (long number strings)
    r'^\d{6,}\s',
    # "N Percent Vol." bare token
    r'(?i)^\d+\.?\d*\s+percent\s+vol\.?\s*$',
    # Garment/fabric material tokens
    r'(?i)^\d+\s*%\s*(recycled|woven|knitted|satin|velvet)',
    r'(?i)^(bow\s+filling|shell\s*:|lining\s*:)',
    # Partial fragment with unclosed bracket: "1-(1", "1-(2", "1-(3"
    r'^\d+-\(\d*$',
    # Very short digit+letter fragments: "1h", "2h", "5b", "6c"
    r'^\d+[a-z]$',
]

# Fragrance note descriptors to strip from ingredient names
_NOTE_DESCRIPTOR = re.compile(
    r'\s*\(\s*\w[\w\s,/]*\bNote\b[^)]*\)',   # (Musk Note / Safe Synthetic)
    re.IGNORECASE,
)
_NOTE_DESCRIPTOR_UNCLOSED = re.compile(
    r'\s*\(\s*\w[\w\s,/]*\bNote\b[^)]*$',    # (herbal Note/Safe Synthetic  [no closing )]
    re.IGNORECASE,
)


# Code prefixes on an individual token — strip prefix, keep ingredient
CODE_PREFIXES = re.compile(
    r'^(?:'
    r'[A-Z]{1,3}\d+[A-Z]?\s*:\s*'               # EE11:  EE12:  MN10:
    r'|#\d+[A-Za-z0-9/]*\s+(?=[A-Z])'           # #18475 Synthetic…
    r'|#\d+[A-Za-z0-9/]*\s*:\s*'                # #01:
    r'|\d+[A-Z]+\s*:\s*'                         # 200N:  29N:
    r'|[A-Z]/\d+\w*\s+\w+\s*:\s*'              # B/051022 CLONED:
    r'|[A-Z]\d{4,}\s+'                           # B03877 Aqua
    r'|\d{4,}\s+(?=[A-Z])'                       # 17440 Aqua  2023656 3:
    r')',
    re.IGNORECASE,
)


# ──────────────────────────────────────────────────────────────
# ROW-LEVEL HELPERS
# ──────────────────────────────────────────────────────────────

def _handle_active_inactive(text):
    """Extract ingredient names from ACTIVE / INACTIVE sections."""
    active_m   = re.search(r'(?i)\bACTIVE\s*:\s*(.*?)(?=\bINACTIVE\b|$)', text, re.DOTALL)
    inactive_m = re.search(r'(?i)\bINACTIVE\s*:\s*(.*?)$', text, re.DOTALL)
    parts = []
    if active_m:
        ap = active_m.group(1).strip().rstrip('.')
        if ',' not in ap and re.search(r'\d%', ap):
            parts.extend(t.strip() for t in re.split(r'(?<=\%)\s+', ap) if t.strip())
        else:
            parts.append(ap)
    if inactive_m:
        parts.append(inactive_m.group(1).strip().rstrip('.'))
    return ', '.join(parts) if parts else text


def _strip_backslash_alts(text):
    """'NameA\\NameB\\NameC' → 'NameA'"""
    def _first(m):
        return m.group(0).split('\\')[0].strip()
    return re.sub(r'[A-Za-z][^,()\n]*?\\[A-Za-z][^,()\n]*', _first, text)


def _strip_leading_prefix(text):
    """Remove shade / variant / section labels from the start of a row."""
    text = re.sub(r'^\([0-9]+\)\s*', '', text)                              # (1)
    text = re.sub(r'^[A-Z][A-Z\s|/]+\s*[:|]\s*', '', text)                 # SHADOW | OMBRE:
    text = re.sub(r'^\d+[A-Za-z\s]*?\s*-\s*(?=[A-Z][a-z])', '', text)      # 01 Red -
    text = re.sub(r'^-[A-Z][^:,\n]{0,40}:\s+', '', text)                   # -Matte:
    text = re.sub(r'(?i)^[^,\n]{0,60}\bingredients?\s*:\s*', '', text)     # Foil Shadow Ingredients:
    text = re.sub(r'(?i)^formula\s*:\s*', '', text)
    m = re.match(r'^([A-Z\'\'&][^:,\n]{0,80}):\s+([A-Z].*)', text, re.DOTALL)
    if m:
        prefix = m.group(1).strip()
        words  = prefix.split()
        inci_ends = ('triglyceride','copolymer','glycol','siloxane','dimethicone',
                     'acrylate','stearate','palmitate','behenate','benzoate',
                     'sulfonate','gluconate','carbonate','citrate','lactate')
        looks_inci = ((len(words) == 1 and len(prefix) > 20)
                      or any(prefix.lower().endswith(e) for e in inci_ends))
        if not looks_inci and len(words) <= 12:
            text = m.group(2)
    return text


def _period_to_comma(m):
    before = m.string[max(0, m.start() - 4):m.start()]
    if re.search(r'\b(Ext|St|Jr|Dr|Mr|No|Vol)\s*$', before, re.I):
        return m.group(0)
    return ', ' + m.group(1)


def _clean_row(text):
    """Pass 1: row-level cleaning. Returns cleaned string or None."""
    if not text or not text.strip():
        return None

    # Whole-row rejects
    for pat in NON_INGREDIENT_ROW:
        if re.search(pat, text):
            return None

    # HTML entities
    text = re.sub(r'&[Nn]bsp;?', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)

    # Strip leading/trailing quote artifacts
    text = text.strip('"\'')

    # Strip inline footnote markers EARLY (so Fragrance*(Parfum) → Fragrance(Parfum))
    text = re.sub(r'\*+', '', text)
    text = re.sub(r'[†‡♥ᵻϮ]', '', text)

    # Bullet separators → comma  (both ● and •)
    text = re.sub(r'\s*[●•]\s*', ', ', text)

    # Remove bullet blurbs:  -Key: description sentence.
    text = re.sub(r'(?m)-\s*[\w™®°#%][^:]*:[^.]*\.[ ]*', '', text)
    text = text.strip()
    if not text:
        return None

    # ACTIVE / INACTIVE labels
    text = re.sub(r'(?i)\bsunscreen\s*/?\s*active\s*:\s*',  'ACTIVE: ', text)
    text = re.sub(r'(?i)\bactive\s+ingredients?\s*:\s*',    'ACTIVE: ', text)
    text = re.sub(r'(?i)\binactive\s+ingredients?\s*:\s*',  'INACTIVE: ', text)
    text = re.sub(r'(?i)\bactive\s+ingredient\s+purpose\b', 'ACTIVE: ', text)
    text = re.sub(r'(?i)\bactive\s*:\s*',                   'ACTIVE: ', text)
    text = re.sub(r'(?i)\binactive\s*:\s*',                 'INACTIVE: ', text)
    text = re.sub(r'(?i)(?<=[a-z])Inactive\s*:',            ', INACTIVE: ', text)
    text = re.sub(r'(?i)\b[\w][A-Za-z\s/&]{2,50}(?:\s*-\s*|\s*:\s*)(?=ACTIVE\s*:)', '', text)
    if re.search(r'(?i)\bACTIVE\s*:', text):
        text = _handle_active_inactive(text)
    text = re.sub(r'(?i),?\s*INACTIVE\s*:\s*', ', ', text)

    # Percentage amounts on ingredient names
    text = re.sub(r'\s*\(\d+\.?\d*\s*%\)',          '', text)
    text = re.sub(r'\s+\d+\.?\d*\s*%',              '', text)
    text = re.sub(r'\s*\(\d+\.?\d*\s*%\s+[^)]+\)',  '', text)
    text = re.sub(r'^\d+\.?\d*\s*%\s+[A-Za-z][^:]{0,50}:\s*', '', text)

    # MAY CONTAIN — English + French + Spanish + Italian + ± symbol
    # Also catches multilingual slash-notation: // Puede Contener, / (±):, //+/-:
    text = re.sub(r'[±]', '+/-', text)   # normalise Unicode ± to +/-
    may = (
        r'\.?\s*(?:'
        r'MAY\s+CONTAIN|May\s+Contain'
        r'|PEUT\s+CONTENIR|Peut\s+Contenir'
        r'|PUEDE\s+CONTENER|Puede\s+Contener'
        r'|PU[OÒ]\s+CONTENERE|Pu[oò]\s+Contenere'
        r'|KANN\s+ENTHALTEN|Kann\s+Enthalten'
        r')\s*'
        r'(?:[·•\-/]\s*(?:MAY\s+CONTAIN|PEUT\s+CONTENIR|PUEDE\s+CONTENER|PU[OÒ]\s+CONTENERE)\s*)?'
        r'(?:\s*[\(\[]\s*\+/-\s*[\)\]])?\s*[:\[]*\s*'
    )
    text = re.sub(may, ', ', text, flags=re.IGNORECASE)
    # Catch remaining slash-notation fragments from multilingual labels:
    # "// Puede Contener [", "/ (±):", "/ / +/-:", "//+/-:"
    text = re.sub(r'(?i)//\s*(?:Puede\s+Contener|Kann\s+Enthalten|Pu[oò]\s+Contenere)[^,]{0,100}', ', ', text)
    text = re.sub(r'(?i)\(\+/-\)',            '', text)
    text = re.sub(r'(?i)\bPEUT\s+CONTENIR\b', '', text)
    text = re.sub(r'(?i)\bPUEDE\s+CONTENER\b','', text)
    text = re.sub(r'(?i)\bPU[OÒ]\s+CONTENERE\b','', text)

    # Strip leading shade / section prefix
    text = _strip_leading_prefix(text)

    # Backslash alternate names → first part
    text = _strip_backslash_alts(text)

    # Known forward-slash / parenthetical alternate names
    for pat, repl in KNOWN_ALTS:
        text = re.sub(pat, repl, text)

    # Period as separator → comma
    text = re.sub(r'\.\s+([A-Z][a-z])', _period_to_comma, text)

    # Disclaimers
    disc = [
        r'(?i),?\s*(?:The list of )?ingredients?\s+(?:list\s+)?(?:may\s+change|is\s+subject\s+to\s+change|lists?\s+may\s+change|are\s+subject\s+to\s+change)[^.]{0,300}\.?',
        r'(?i),?\s*This\s+list\s+(?:of\s+ingredients?\s+)?(?:is|may\s+be)\s+subject\s+to\s+change[^.]{0,200}\.?',
        r'(?i),?\s*Please\s+note\s+the\s+ingredient\s+lists?[^.]{0,200}\.?',
        r'(?i),?\s*Disclaimer\s*:\s*This\s+list[^.]{0,200}\.?',
        r'(?i),?\s*Please\s+(?:consult|refer\s+to)\s+the\s+(?:product\s+)?packaging[^.]{0,200}\.?',
        r'(?i),?\s*Please\s+be\s+aware\s+that[^.]{0,300}\.?',
        r'(?i),?\s*(?:Customers?|Consumers?)\s+should\s+refer[^.]{0,200}\.?',
        r'(?i),?\s*For\s+the\s+most\s+(?:complete|up-to-date)[^.]{0,200}\.?',
        r'(?i),?\s*Please\s+refer\s+to\s+the\s+information[^.]{0,200}\.?',
        r'(?i),?\s*Rael\s+is\s+dedicated\s+to[^.]{0,200}\.?',
        r'(?i),?\s*Warnings?\s+&?\s+Disclosures?[^.]{0,200}\.?',
        r'(?i),?\s*For\s+external\s+use\s+only[^.]{0,200}\.?',
        r'(?i),?\s*Avoid\s+(?:contact\s+with\s+eyes|eye\s+area)[^.]{0,100}\.?',
        r'(?i),?\s*Keep\s+out\s+of\s+reach[^.]{0,100}\.?',
        r'(?i),?\s*\*?Refer\s+to\s+package[^.]{0,100}\.?',
        r'(?i),?\s*Always\s+check\s+the\s+packaging[^.]{0,100}\.?',
    ]
    for pat in disc:
        text = re.sub(pat, '', text)

    # Misc row-level artefacts
    text = re.sub(r'(?i)\bCOSMETIC\s+INGREDIENTS\s+', ', ', text)
    text = re.sub(r'\s*<[A-Za-z]{2,4}\d{4,8}>\s*', ' ', text)  # <ILNxxxxx>
    text = re.sub(r'\s*\]\s*', ', ', text)                       # ] artefact
    text = re.sub(r'(?<!\w),?\s*\bSunscreen\b,?\s*(?=[A-Z])', ', ', text)
    text = re.sub(r'\\+\*?', '', text)                           # stray backslashes
    text = re.sub(r'(?i)\bCOSMETIC\s+INGREDIENTS\b', '', text)

    # Trailing footnote explanation text  (e.g. ". Certified Organic …")
    text = re.sub(
        r'[.,]\s*(?:Certified Organic|Naturally\s+[Dd]erived|Natural\s+Fragrance|'
        r'Essential\s+Oil|Organic\s+Ingredient|Plant\s+[Oo]rigin|Vegetable\s+[Dd]erived|'
        r'Antistatic\s+[Aa]gent|Organic\s+[Ee]xtracts?|[Dd]enotes|PITERA|Hadasei|'
        r'Ingredient\s+[Ff]rom\s+[Oo]rganic|Ingredient\s+lists?)[^,\n]{0,200}$',
        '', text)

    # Final tidy
    text = re.sub(r',\s*,+', ', ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip().strip(',').rstrip('.').strip()
    return text if text else None


# ──────────────────────────────────────────────────────────────
# INGREDIENT-LEVEL HELPERS
# ──────────────────────────────────────────────────────────────

def _clean_single(ing):
    """Pass 2: clean one ingredient token (post-split). Returns '' to signal discard."""
    ing = ing.strip().strip('"\'[').strip()
    if not ing:
        return ''

    # HTML entities
    ing = re.sub(r'&[Nn]bsp;?', ' ', ing)
    ing = re.sub(r'&amp;', '&', ing)

    # ± already normalised at row level, but catch any remaining
    ing = re.sub(r'[±]', '', ing)

    # F.I.L. codes
    ing = re.sub(r'\s*\((?:Code\s+)?F\.I\.L\.[^)]*\)', '', ing)
    ing = re.sub(r'F\.I\.L\.\s+\S+', '', ing)
    ing = re.sub(r'\.\s+[A-Z]\d{5,}[/\w]*$', '', ing)

    # Fragrance note descriptors in parentheses: "(Musk Note / Safe Synthetic)" etc.
    ing = _NOTE_DESCRIPTOR.sub('', ing)
    ing = _NOTE_DESCRIPTOR_UNCLOSED.sub('', ing)   # unclosed: "(herbal Note/Safe Synthetic"

    # Descriptor tags in parentheses
    ing = DESCRIPTOR_PARENS.sub('', ing)

    # Category prefix labels
    ing = re.sub(r'^\(Sunscreen\)\s*;?\s*', '', ing, flags=re.IGNORECASE)
    ing = re.sub(r'^\(Antiperspirant\)\s*;?\s*', '', ing, flags=re.IGNORECASE)
    ing = re.sub(r'^\(w/w\)\s*:?\s*', '', ing, flags=re.IGNORECASE)

    # Pure numeric shade prefix: "30: Aqua", "3.5: Aqua", "11: Mica", "649: Mica"
    ing = re.sub(r'^\d+\.?\d*\s*:\s+', '', ing)

    # "N Percent Vol. Alcohol Denat" → "Alcohol Denat"
    ing = re.sub(r'(?i)^\d+\.?\d*\s+percent\s+vol\.?\s+', '', ing)

    # Leading percentage: "68% Zinc Oxide" → "Zinc Oxide"
    ing = re.sub(r'^\d+\.?\d*\s*%\s+', '', ing)

    # Bilingual section labels: "Ingredients/Ingrédients:"
    ing = re.sub(r'(?i)^Ingr[eé]di[ea]nts?(?:/[A-Za-zÀ-ÿ\s]+)?\s*:\s*', '', ing)

    # Code prefixes: "EE12: Talc" → "Talc", "#01: Mica" → "Mica"
    m = CODE_PREFIXES.match(ing)
    if m:
        ing = ing[m.end():].strip()

    # Shade/product name prefix — loop up to 3 times to handle nested prefixes like
    # "Proprietary Blend: 50 Billion CFU: 120 mg" (two colons, needs two strip passes)
    # Handles: "11 11 EDP:", "11 11 (0.06 Oz):", "01 Cream Espresso:", "Royal Affair:"
    for _ in range(3):
        shade_m = re.match(
            r"^(\d[\d\s\(\)\.]{0,20}[A-Za-z][^:,\n]{0,80}|[A-Za-z][^:,\n]{0,80}):\s+(.+)",
            ing)
        if not shade_m:
            break
        prefix    = shade_m.group(1).strip()
        remainder = shade_m.group(2).strip()
        words     = prefix.split()
        inci_ends = ('triglyceride','copolymer','glycol','siloxane','dimethicone',
                     'acrylate','stearate','palmitate','behenate','benzoate',
                     'sulfonate','gluconate','carbonate','citrate','lactate')
        single_chem = (len(words) == 1 and len(prefix) > 20
                       and re.search(r'[a-z]{4,}', prefix)
                       and not re.search(r'[A-Z][a-z]+[A-Z]', prefix))
        looks_inci = (any(prefix.lower().endswith(e) for e in inci_ends) or single_chem)
        _marketing = re.compile(
            r'(?i)^(contains\s+\w|provides?\b|helps?\b|derived\b|made\s+with\b'
            r'|formulated\b|designed\b|clinically\b|our\s+\b|your\s+\b)', re.I)
        if not looks_inci and len(words) <= 10 and not _marketing.match(remainder):
            ing = remainder
        else:
            break

    # Strip leading junk
    ing = re.sub(r'^[\s\(\)\[\]\+\-;:.,/]+', '', ing)
    ing = re.sub(r'[\s\(\)\[\]\+\-;:,]+$', '', ing)
    ing = ing.strip()

    # Re-run CODE_PREFIXES and percent-vol strip after shade_m may have exposed them.
    # e.g. "02 Soft Fair: #22971 Hydrogenated Polydecene" → shade_m strips "02 Soft Fair:"
    # leaving "#22971 Hydrogenated Polydecene" which CODE_PREFIXES then catches.
    m2 = CODE_PREFIXES.match(ing)
    if m2:
        ing = ing[m2.end():].strip()
    ing = re.sub(r'(?i)^\d+\.?\d*\s+percent\s+vol\.?\s+', '', ing)

    # Shade code digit concatenated directly with ingredient (no separator):
    # "2water / Aqua / Eau" → "water / Aqua / Eau", "2paraffinum Liquidum" → "paraffinum Liquidum"
    # Preserve legitimate "2-Hexanediol" (digit-hyphen = INCI name, not concatenation)
    if re.match(r'^\d[a-z]', ing):
        ing = re.sub(r'^\d+', '', ing)

    if not ing:
        return ''

    # Reject patterns
    for pat in REJECT_INGREDIENT:
        if re.search(pat, ing, re.IGNORECASE):
            return ''

    # Too short after stripping non-word chars
    if len(re.sub(r'\W', '', ing)) < 2:
        return ''

    # Space-separated list heuristic — discard if too long with many cap words
    if len(ing) > 100:
        cap_words = re.findall(r'\b[A-Z][a-z]{2,}', ing)
        if len(cap_words) > 5:
            return ''
        # All-caps space-separated list (e.g. "GLYCERIN WATER CETEARYL ALCOHOL ...")
        alpha = [c for c in ing if c.isalpha()]
        if alpha and sum(c.isupper() for c in alpha) / len(alpha) > 0.7:
            return ''

    # Sentence/marketing text detector
    if len(ing) > 80:
        _func = {'a','an','the','and','or','for','of','to','in','with','is','are',
                 'be','been','was','were','that','which','this','from','on','at',
                 'by','as','your','our','its','their','may','can','will','has',
                 'have','had','not','no','if','but','so','than','up','out'}
        all_w = re.findall(r'\b[a-z]+\b', ing.lower())
        uniq_func  = len(set(all_w) & _func)
        total_func = sum(1 for w in all_w if w in _func)
        # Longer strings need fewer unique func words to trigger rejection
        threshold = 2 if len(ing) > 120 else 3
        if uniq_func >= threshold or total_func >= 5:
            return ''





    # ── Canonicalise INCI form, normalise case, apply synonym map ───────
    ing = _canonicalize_inci(ing)
    if not ing:
        return ''
    ing = ing.lower()
    ing = SYNONYM_MAP.get(ing, ing)

    return ing



# ──────────────────────────────────────────────────────────────
# MAIN CLEANER
# ──────────────────────────────────────────────────────────────

def clean_ingredients(raw):
    """Full two-pass cleaner. Returns clean comma-separated string or None."""
    if pd.isna(raw) or str(raw).strip() == '':
        return None

    row = _clean_row(str(raw).strip())
    if not row:
        return None

    parts = []
    for tok in row.split(','):
        cleaned = _clean_single(tok)
        if cleaned:
            parts.append(cleaned)

    return ', '.join(parts) if parts else None


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Clean the ingredients column in Sephora and Ulta product CSVs.'
    )
    parser.add_argument('--sephora',     default='data/raw/sephora_products.csv')
    parser.add_argument('--ulta',        default='data/raw/ulta_products.csv')
    parser.add_argument('--out-sephora', default='data/processed/sephora_products_clean.csv')
    parser.add_argument('--out-ulta',    default='data/processed/ulta_products_clean.csv')
    args = parser.parse_args()

    for in_path, out_path, label in [
        (args.sephora, args.out_sephora, 'Sephora'),
        (args.ulta,    args.out_ulta,    'Ulta'),
    ]:
        in_path  = Path(in_path)
        out_path = Path(out_path)

        if not in_path.exists():
            print(f'[SKIP] {label}: file not found → {in_path}')
            continue

        df     = pd.read_csv(in_path)
        before = df['ingredients'].notna().sum()
        df['ingredients'] = df['ingredients'].apply(clean_ingredients)
        after  = df['ingredients'].notna().sum()

        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        print(f'{label}: {before:,} → {after:,} ({before - after} nulled)  →  {out_path}')