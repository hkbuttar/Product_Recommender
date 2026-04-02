"""
app.py
------
Streamlit ingredient-matching product recommender.

Tabs
----
  1. Similar Products  — find same-category products ranked by a blended
                         Jaccard + TF-IDF cosine similarity score (ML).
  2. Search by Ingredient — select any ingredient(s) from a full searchable
                            multiselect; browse every product that contains them.

Run:
    streamlit run src/app.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_REPO_ROOT    = Path(__file__).resolve().parent.parent
DATA_PATH     = _REPO_ROOT / "data" / "processed" / "combined_products.csv"
MAX_RESULTS   = 20
COSINE_WEIGHT = 0.55   # weight for TF-IDF cosine in blended score; rest = Jaccard


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_PATH)
    df = df[df["ingredients"].notna() & (df["ingredients"].astype(str).str.strip() != "")]
    df["ingredients"]   = df["ingredients"].astype(str).str.lower().str.strip()
    df["product_name"]  = df["product_name"].astype(str).str.strip()
    df["brand"]         = df["brand"].astype(str).str.strip()
    df["category"]      = df["category"].astype(str).str.strip()
    df["price"]         = pd.to_numeric(df["price"],  errors="coerce")
    df["rating"]        = pd.to_numeric(df["rating"], errors="coerce")
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# TF-IDF model — built once, reused for all similarity queries
# ---------------------------------------------------------------------------

@st.cache_resource
def build_tfidf_model():
    """
    Fit a TF-IDF vectoriser treating each comma-separated ingredient as a token.
    IDF down-weights ubiquitous ingredients (water, glycerin) and up-weights
    rare/distinctive ones — giving a more nuanced similarity than raw Jaccard.

    Returns
    -------
    vectorizer : fitted TfidfVectorizer
    matrix     : sparse (n_products × vocab) TF-IDF matrix aligned with df rows
    """
    df = load_data()

    def _tokenize(text: str) -> list[str]:
        return [t.strip().rstrip(".") for t in text.split(",") if t.strip()]

    vectorizer = TfidfVectorizer(
        tokenizer=_tokenize,
        token_pattern=None,
        lowercase=True,
        norm="l2",
        sublinear_tf=True,   # log(1+tf) dampens very frequent ingredients
    )
    matrix = vectorizer.fit_transform(df["ingredients"])
    return vectorizer, matrix


# ---------------------------------------------------------------------------
# All unique ingredients (for search tab multiselect)
# ---------------------------------------------------------------------------

@st.cache_data
def get_all_ingredients(df: pd.DataFrame) -> list[str]:
    """Sorted list of every unique ingredient in the dataset."""
    ings: set[str] = set()
    for raw in df["ingredients"]:
        for token in raw.split(","):
            clean = token.strip().rstrip(".")
            if clean:
                ings.add(clean)
    return sorted(ings)


def parse_ingredients(raw: str) -> set[str]:
    return {t.strip().rstrip(".") for t in raw.split(",") if t.strip()}


# ---------------------------------------------------------------------------
# Recommendation: blended Jaccard + TF-IDF cosine similarity
# ---------------------------------------------------------------------------

def recommend(
    query_idx: int,
    df: pd.DataFrame,
    tfidf_matrix,
    exclude_idx: int | None = None,
) -> pd.DataFrame:
    """
    Score ALL products by a two-component similarity (no category restriction):

      cosine_sim  — TF-IDF cosine similarity (IDF-weighted; treats rare
                    ingredients as stronger signals than ubiquitous ones)
      jaccard     — exact set overlap / union ratio (fully interpretable)

      blend_score = COSINE_WEIGHT × cosine + (1−COSINE_WEIGHT) × jaccard

    Results are sorted by blend_score desc, then shared_count desc, then
    rating desc to break ties.
    """
    pool = df.copy()
    if exclude_idx is not None:
        pool = pool[pool.index != exclude_idx]
    if pool.empty:
        return pool

    query_ings = parse_ingredients(df.loc[query_idx, "ingredients"])

    # ── TF-IDF cosine similarity ─────────────────────────────────────────
    query_vec   = tfidf_matrix[query_idx]         # sparse (1 × vocab)
    pool_vecs   = tfidf_matrix[pool.index]        # sparse (n × vocab)
    cosine_sims = cosine_similarity(query_vec, pool_vecs).flatten()
    pool = pool.copy()
    pool["cosine_sim"] = cosine_sims

    # ── Set-based Jaccard + raw shared count ─────────────────────────────
    def _set_scores(row) -> tuple[int, float, float]:
        prod_ings = parse_ingredients(row["ingredients"])
        shared    = query_ings & prod_ings
        n_shared  = len(shared)
        union_n   = len(query_ings | prod_ings)
        jaccard   = n_shared / union_n  if union_n else 0.0
        match_pct = n_shared / len(query_ings) * 100 if query_ings else 0.0
        return n_shared, jaccard, match_pct

    scores = pool.apply(_set_scores, axis=1, result_type="expand")
    scores.columns = ["shared_count", "jaccard", "match_pct"]
    pool = pool.join(scores)

    # ── Blended score ─────────────────────────────────────────────────────
    pool["blend_score"] = (
        COSINE_WEIGHT       * pool["cosine_sim"]
        + (1 - COSINE_WEIGHT) * pool["jaccard"]
    )

    pool = pool[pool["shared_count"] > 0]
    pool = pool.sort_values(
        ["blend_score", "shared_count", "rating"],
        ascending=[False, False, False],
    )
    return pool.head(MAX_RESULTS).reset_index(drop=True)


def get_shared_ingredients(query_ings: set[str], product_ings_raw: str) -> list[str]:
    return sorted(query_ings & parse_ingredients(product_ings_raw))


# ---------------------------------------------------------------------------
# Ingredient search
# ---------------------------------------------------------------------------

def search_by_ingredients(
    selected_ings: list[str],
    df: pd.DataFrame,
    match_all: bool = True,
    category_filter: str | None = None,
) -> pd.DataFrame:
    """
    Return products containing any or all of `selected_ings`.
    Sorted by number of selected ingredients present, then by rating.
    """
    if not selected_ings:
        return pd.DataFrame()

    pool      = df if not category_filter else df[df["category"] == category_filter]
    query_set = {s.lower() for s in selected_ings}

    def _count(raw: str) -> int:
        return len(query_set & parse_ingredients(raw))

    pool = pool.copy()
    pool["ing_match_count"] = pool["ingredients"].apply(_count)

    if match_all:
        pool = pool[pool["ing_match_count"] == len(selected_ings)]
    else:
        pool = pool[pool["ing_match_count"] > 0]

    return pool.sort_values(
        ["ing_match_count", "rating"],
        ascending=[False, False],
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _rating_stars(rating: float) -> str:
    if pd.isna(rating):
        return "—"
    full = int(rating)
    half = "½" if (rating - full) >= 0.4 else ""
    return "★" * full + half


def _source_badge(source: str) -> str:
    colors = {"sephora": "#E2A8B8", "ulta": "#C9A96E"}
    color  = colors.get(str(source).lower(), "#aaa")
    label  = str(source).upper()
    return (
        f'<span style="background:{color};color:#1a1a1a;'
        f'font-size:0.65rem;font-weight:700;letter-spacing:0.08em;'
        f'padding:2px 8px;border-radius:20px;">{label}</span>'
    )


def _score_pill(label: str, value: str, color: str = "#c9a96e") -> str:
    return (
        f'<span style="font-family:\'DM Mono\',monospace;font-size:0.6rem;'
        f'letter-spacing:0.1em;color:{color};border:1px solid {color}33;'
        f'padding:2px 8px;border-radius:20px;">{label}&nbsp;{value}</span>'
    )


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Ingredient Match",
    page_icon="✦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,600;1,300;1,400&family=DM+Mono:wght@300;400&display=swap');

html, body, [class*="css"] {
    background-color: #0f0d0b !important;
    color: #e8ddd0 !important;
    font-family: 'Cormorant Garamond', Georgia, serif !important;
}

/* ── Header ── */
.main-title {
    font-family: 'Cormorant Garamond', serif;
    font-size: 4rem;
    font-weight: 300;
    letter-spacing: 0.15em;
    color: #e8ddd0;
    line-height: 1;
    margin-bottom: 0.1rem;
}
.main-subtitle {
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    letter-spacing: 0.25em;
    color: #8a7d6e;
    text-transform: uppercase;
    margin-bottom: 2.5rem;
}
.divider { border: none; border-top: 1px solid #2a2520; margin: 1.5rem 0; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: #130f0c !important;
    border-right: 1px solid #2a2520;
}
[data-testid="stSidebar"] * {
    font-family: 'Cormorant Garamond', serif !important;
    color: #e8ddd0 !important;
}
.sidebar-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    color: #8a7d6e !important;
    text-transform: uppercase;
    margin-bottom: 0.3rem;
}

/* ── Inputs ── */
[data-testid="stSelectbox"] > div > div,
[data-testid="stTextInput"] > div > div > input,
[data-testid="stMultiSelect"] > div > div {
    background-color: #1e1a16 !important;
    border: 1px solid #3a3028 !important;
    border-radius: 2px !important;
    color: #e8ddd0 !important;
    font-family: 'Cormorant Garamond', serif !important;
    font-size: 1rem !important;
}
[data-testid="stSelectbox"] svg,
[data-testid="stMultiSelect"] svg { color: #8a7d6e !important; }

/* ── Tabs ── */
[data-testid="stTabs"] [role="tab"] {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.65rem !important;
    letter-spacing: 0.2em !important;
    text-transform: uppercase !important;
    color: #8a7d6e !important;
    border-bottom: 1px solid #2a2520 !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #c9a96e !important;
    border-bottom: 1px solid #c9a96e !important;
}

/* ── Buttons ── */
[data-testid="stButton"] > button {
    background: transparent !important;
    border: 1px solid #c9a96e !important;
    color: #c9a96e !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.7rem !important;
    letter-spacing: 0.15em !important;
    text-transform: uppercase !important;
    border-radius: 1px !important;
    padding: 0.5rem 1.5rem !important;
}
[data-testid="stButton"] > button:hover {
    background: #c9a96e !important;
    color: #0f0d0b !important;
}

/* ── Toggle ── */
[data-testid="stToggle"] label {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.65rem !important;
    letter-spacing: 0.15em !important;
    text-transform: uppercase !important;
    color: #8a7d6e !important;
}

/* ── Metric cards ── */
.metric-row { display: flex; gap: 1rem; margin: 1rem 0 1.5rem; }
.metric-card {
    background: #1a1612;
    border: 1px solid #2a2520;
    padding: 1rem 1.5rem;
    flex: 1;
}
.metric-value { font-size: 2rem; font-weight: 300; color: #c9a96e; line-height: 1; }
.metric-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem; letter-spacing: 0.2em;
    color: #8a7d6e; text-transform: uppercase; margin-top: 0.3rem;
}

/* ── ML score badge ── */
.ml-badge {
    display: inline-block;
    font-family: 'DM Mono', monospace;
    font-size: 0.58rem;
    letter-spacing: 0.12em;
    color: #7a9e8a;
    border: 1px solid #7a9e8a44;
    padding: 2px 8px;
    border-radius: 20px;
    text-transform: uppercase;
}

/* ── Query product card ── */
.query-card {
    background: #1a1612;
    border: 1px solid #3a3028;
    border-left: 3px solid #c9a96e;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1.5rem;
}
.query-brand {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem; letter-spacing: 0.2em;
    color: #8a7d6e; text-transform: uppercase;
}
.query-name { font-size: 1.4rem; font-weight: 300; color: #e8ddd0; margin: 0.2rem 0 0.4rem; }
.query-category {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem; letter-spacing: 0.15em;
    color: #c9a96e; text-transform: uppercase;
}

/* ── Result cards ── */
.result-card {
    background: #141210;
    border: 1px solid #2a2520;
    padding: 1.1rem 1.4rem;
    margin-bottom: 0.75rem;
    position: relative;
}
.result-card:hover { border-color: #3a3028; }
.result-rank {
    position: absolute; top: 1rem; right: 1.2rem;
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem; letter-spacing: 0.15em; color: #5a5048;
}
.result-brand {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem; letter-spacing: 0.2em;
    color: #8a7d6e; text-transform: uppercase;
}
.result-name {
    font-size: 1.15rem; font-weight: 400;
    color: #e8ddd0; margin: 0.2rem 0 0.5rem; padding-right: 3rem;
}
.result-meta {
    display: flex; align-items: center;
    gap: 0.7rem; flex-wrap: wrap; margin-bottom: 0.5rem;
}
.match-bar-wrap { margin: 0.6rem 0 0.4rem; }
.match-bar-bg { background: #2a2520; height: 3px; border-radius: 2px; }
.match-bar-fill {
    height: 3px; border-radius: 2px;
    background: linear-gradient(90deg, #c9a96e, #e8c898);
}
.cosine-bar-fill {
    height: 3px; border-radius: 2px;
    background: linear-gradient(90deg, #7a9e8a, #a8c8b8);
}
.bar-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.55rem; letter-spacing: 0.1em;
    color: #5a5048; text-transform: uppercase;
    margin-bottom: 3px;
}
.shared-ings {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem; letter-spacing: 0.05em;
    color: #6a6058; line-height: 1.6; margin-top: 0.4rem;
}
.shared-ings-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem; letter-spacing: 0.15em;
    color: #8a7d6e; text-transform: uppercase; margin-bottom: 0.2rem;
}
.price-tag { font-family: 'DM Mono', monospace; font-size: 0.75rem; color: #8a7d6e; }
.rating-tag { font-size: 0.9rem; color: #c9a96e; letter-spacing: 0.05em; }
.no-results {
    font-family: 'DM Mono', monospace;
    font-size: 0.75rem; letter-spacing: 0.15em;
    color: #5a5048; text-align: center;
    padding: 3rem 0; text-transform: uppercase;
}
.section-heading {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem; letter-spacing: 0.25em;
    color: #8a7d6e; text-transform: uppercase; margin-bottom: 1rem;
}
/* ingredient search pill tags */
.ing-pill {
    display: inline-block;
    background: #1e1a16;
    border: 1px solid #3a3028;
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.08em;
    color: #c9a96e;
    padding: 2px 10px;
    border-radius: 20px;
    margin: 2px 3px;
}
.ing-pill-found { border-color: #c9a96e55; color: #c9a96e; }
.ing-pill-missing { border-color: #3a302833; color: #5a5048; }
.ing-match-count {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: #7a9e8a;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------

df = load_data()
_vectorizer, _tfidf_matrix = build_tfidf_model()
all_ingredients  = get_all_ingredients(df)
categories       = sorted(df["category"].dropna().unique().tolist())


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown('<div class="main-title">INGREDIENT<br>MATCH</div>', unsafe_allow_html=True)
st.markdown('<div class="main-subtitle">✦ product recommender ✦</div>', unsafe_allow_html=True)
st.markdown('<hr class="divider"/>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_similar, tab_search = st.tabs(["  Similar Products  ", "  Search by Ingredient  "])


# ============================================================
# TAB 1 — Similar Products
# ============================================================

with tab_similar:

    # ── Sidebar controls ──────────────────────────────────────
    with st.sidebar:
        st.markdown('<div class="sidebar-label">Find a product</div>', unsafe_allow_html=True)

        selected_category = st.selectbox(
            "Category",
            options=["All categories"] + categories,
            label_visibility="collapsed",
            key="cat_similar",
        )

        cat_df = df if selected_category == "All categories" else df[df["category"] == selected_category]

        st.markdown('<div class="sidebar-label" style="margin-top:1.2rem">Product name or brand</div>', unsafe_allow_html=True)
        search_query = st.text_input(
            "Search",
            placeholder="e.g. Laneige, toner, serum …",
            label_visibility="collapsed",
            key="search_similar",
        )

        if search_query.strip():
            q    = search_query.strip().lower()
            mask = (
                cat_df["product_name"].str.lower().str.contains(q, na=False)
                | cat_df["brand"].str.lower().str.contains(q, na=False)
            )
            filtered = cat_df[mask]
        else:
            filtered = cat_df

        st.markdown('<div class="sidebar-label" style="margin-top:1.2rem">Select product</div>', unsafe_allow_html=True)

        if filtered.empty:
            st.markdown('<div style="font-size:0.8rem;color:#5a5048">No products found.</div>', unsafe_allow_html=True)
            selected_idx = None
        else:
            labels     = [f"{r['brand']} — {r['product_name']}" for _, r in filtered.iterrows()]
            choice     = st.selectbox(
                "Product", options=range(len(filtered)),
                format_func=lambda i: labels[i],
                label_visibility="collapsed",
                key="product_similar",
            )
            selected_idx = filtered.index[choice]

        st.markdown('<hr class="divider"/>', unsafe_allow_html=True)
        st.markdown(
            f'<div style="font-family:\'DM Mono\',monospace;font-size:0.6rem;'
            f'letter-spacing:0.1em;color:#3a3028;line-height:1.8">'
            f'{len(df):,} products<br>'
            f'TF-IDF cosine · Jaccard blend<br>'
            f'cross-category matching'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Main content ─────────────────────────────────────────
    if selected_idx is None:
        st.markdown(
            '<div class="no-results" style="padding-top:5rem">'
            'select a product in the sidebar to begin'
            '</div>', unsafe_allow_html=True,
        )
        st.stop()

    product    = df.loc[selected_idx]
    query_ings = parse_ingredients(product["ingredients"])
    p_category = product["category"]
    p_price    = f"${product['price']:.2f}" if pd.notna(product.get("price")) else "—"
    p_rating   = product.get("rating", float("nan"))
    p_source   = product.get("source", "")

    # Query card
    st.markdown('<div class="section-heading">Selected product</div>', unsafe_allow_html=True)
    st.markdown(f"""
<div class="query-card">
    <div class="query-brand">{product['brand']}&nbsp;&nbsp;{_source_badge(p_source)}</div>
    <div class="query-name">{product['product_name']}</div>
    <div style="display:flex;gap:1.5rem;align-items:center;flex-wrap:wrap">
        <div class="query-category">{p_category}</div>
        <div class="price-tag">{p_price}</div>
        <div class="rating-tag">{_rating_stars(p_rating)} {f'{p_rating:.2f}' if pd.notna(p_rating) else ''}</div>
    </div>
</div>
""", unsafe_allow_html=True)

    results = recommend(selected_idx, df, _tfidf_matrix, exclude_idx=selected_idx)

    # Metrics
    st.markdown(f"""
<div class="metric-row">
    <div class="metric-card">
        <div class="metric-value">{len(query_ings)}</div>
        <div class="metric-label">ingredients</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{len(results)}</div>
        <div class="metric-label">matches found</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{results['shared_count'].max() if not results.empty else 0}</div>
        <div class="metric-label">max shared</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{results['blend_score'].max()*100 if not results.empty else 0:.0f}%</div>
        <div class="metric-label">top blend score</div>
    </div>
</div>
""", unsafe_allow_html=True)

    st.markdown('<hr class="divider"/>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-heading">'
        'Recommendations · TF-IDF cosine + Jaccard blended score · all categories'
        '</div>', unsafe_allow_html=True,
    )

    if results.empty:
        st.markdown(
            '<div class="no-results">no matching products found</div>',
            unsafe_allow_html=True,
        )
    else:
        for rank, (_, row) in enumerate(results.iterrows(), start=1):
            shared    = get_shared_ingredients(query_ings, row["ingredients"])
            price_str = f"${row['price']:.2f}" if pd.notna(row.get("price")) else "—"
            r_val     = row.get("rating", float("nan"))
            match_pct = row["match_pct"]
            cosine    = row["cosine_sim"]
            blend     = row["blend_score"]
            source_b  = _source_badge(row.get("source", ""))
            n_shared  = row["shared_count"]
            bar_w     = min(100, match_pct)
            cos_bar_w = min(100, cosine * 100)
            category_tag = row.get("category", "")

            shown = ", ".join(shared[:12])
            extra = f" +{len(shared) - 12} more" if len(shared) > 12 else ""

            st.markdown(f"""
<div class="result-card">
    <div class="result-rank">#{rank:02d}</div>
    <div class="result-brand">{row['brand']}&nbsp;&nbsp;{source_b}</div>
    <div class="result-name">{row['product_name']}</div>
    <div class="result-meta">
        <div class="price-tag">{price_str}</div>
        <div class="rating-tag">{_rating_stars(r_val)} {f'{r_val:.2f}' if pd.notna(r_val) else ''}</div>
        <div style="font-family:'DM Mono',monospace;font-size:0.6rem;letter-spacing:0.1em;color:#8a7d6e">{category_tag}</div>
        <span class="ml-badge">cosine&nbsp;{cosine:.3f}</span>
        <span class="ml-badge" style="color:#c9a96e;border-color:#c9a96e44">
            jaccard&nbsp;{row['jaccard']:.3f}
        </span>
        <span class="ml-badge" style="color:#b89a6e;border-color:#b89a6e44">
            blend&nbsp;{blend:.3f}
        </span>
    </div>
    <div class="bar-label">ingredient match — {n_shared} shared · {match_pct:.1f}%</div>
    <div class="match-bar-bg"><div class="match-bar-fill" style="width:{bar_w}%"></div></div>
    <div class="bar-label" style="margin-top:6px">tfidf cosine similarity</div>
    <div class="match-bar-bg"><div class="cosine-bar-fill" style="width:{cos_bar_w}%"></div></div>
    <div class="shared-ings-label" style="margin-top:0.6rem">shared ingredients</div>
    <div class="shared-ings">{shown}{extra}</div>
</div>
""", unsafe_allow_html=True)


# ============================================================
# TAB 2 — Search by Ingredient
# ============================================================

with tab_search:

    st.markdown('<div class="section-heading" style="margin-top:0.5rem">Search by ingredient</div>', unsafe_allow_html=True)

    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.markdown('<div class="sidebar-label">Select ingredients</div>', unsafe_allow_html=True)
        selected_ings = st.multiselect(
            "Ingredients",
            options=all_ingredients,
            placeholder="Type to search ingredients…",
            label_visibility="collapsed",
            key="ing_search",
        )

    with col_right:
        st.markdown('<div class="sidebar-label">Category filter (optional)</div>', unsafe_allow_html=True)
        ing_category = st.selectbox(
            "Category filter",
            options=["All categories"] + categories,
            label_visibility="collapsed",
            key="cat_ing_search",
        )
        cat_filter = None if ing_category == "All categories" else ing_category

        st.markdown('<div class="sidebar-label" style="margin-top:0.8rem">Match mode</div>', unsafe_allow_html=True)
        match_all = st.toggle(
            "Require ALL selected ingredients",
            value=True,
            key="match_all_toggle",
        )

    st.markdown('<hr class="divider"/>', unsafe_allow_html=True)

    if not selected_ings:
        st.markdown(
            '<div class="no-results">select one or more ingredients above to search</div>',
            unsafe_allow_html=True,
        )
    else:
        results_ing = search_by_ingredients(
            selected_ings, df,
            match_all=match_all,
            category_filter=cat_filter,
        )

        mode_label = "ALL" if match_all else "ANY"
        st.markdown(f"""
<div class="metric-row">
    <div class="metric-card">
        <div class="metric-value">{len(selected_ings)}</div>
        <div class="metric-label">ingredients selected</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{len(results_ing)}</div>
        <div class="metric-label">products found</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{mode_label}</div>
        <div class="metric-label">match mode</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{ing_category.split()[0] if ing_category != 'All categories' else 'All'}</div>
        <div class="metric-label">category</div>
    </div>
</div>
""", unsafe_allow_html=True)

        # Selected ingredient pills
        pills_html = "".join(
            f'<span class="ing-pill">{ing}</span>' for ing in selected_ings
        )
        st.markdown(
            f'<div style="margin-bottom:1rem">'
            f'<div class="sidebar-label" style="margin-bottom:0.4rem">Active filter</div>'
            f'{pills_html}</div>',
            unsafe_allow_html=True,
        )

        if results_ing.empty:
            st.markdown(
                '<div class="no-results">no products found — try switching to "any" mode or removing a filter</div>',
                unsafe_allow_html=True,
            )
        else:
            query_set = {s.lower() for s in selected_ings}

            st.markdown(
                '<div class="section-heading">Results · sorted by ingredient match count</div>',
                unsafe_allow_html=True,
            )

            display_results = results_ing.head(MAX_RESULTS)

            for rank, (_, row) in enumerate(display_results.iterrows(), start=1):
                price_str = f"${row['price']:.2f}" if pd.notna(row.get("price")) else "—"
                r_val     = row.get("rating", float("nan"))
                source_b  = _source_badge(row.get("source", ""))
                n_matched = row["ing_match_count"]
                prod_ings = parse_ingredients(row["ingredients"])

                # Build pills: green if present, dim if missing
                pills = ""
                for ing in selected_ings:
                    found = ing.lower() in prod_ings
                    cls   = "ing-pill-found" if found else "ing-pill-missing"
                    pills += f'<span class="ing-pill {cls}">{ing}</span>'

                bar_w = min(100, n_matched / len(selected_ings) * 100) if selected_ings else 0

                st.markdown(f"""
<div class="result-card">
    <div class="result-rank">#{rank:02d}</div>
    <div class="result-brand">{row['brand']}&nbsp;&nbsp;{source_b}</div>
    <div class="result-name">{row['product_name']}</div>
    <div class="result-meta">
        <div class="price-tag">{price_str}</div>
        <div class="rating-tag">{_rating_stars(r_val)} {f'{r_val:.2f}' if pd.notna(r_val) else ''}</div>
        <span class="ing-match-count">{n_matched}/{len(selected_ings)} ingredients matched</span>
        <div style="font-family:\'DM Mono\',monospace;font-size:0.6rem;letter-spacing:0.1em;color:#8a7d6e">
            {row['category']}
        </div>
    </div>
    <div class="match-bar-wrap">
        <div class="match-bar-bg">
            <div class="cosine-bar-fill" style="width:{bar_w:.0f}%"></div>
        </div>
    </div>
    <div class="shared-ings-label" style="margin-top:0.5rem">ingredient filter status</div>
    <div style="margin-top:0.3rem">{pills}</div>
</div>
""", unsafe_allow_html=True)

            if len(results_ing) > MAX_RESULTS:
                st.markdown(
                    f'<div style="font-family:\'DM Mono\',monospace;font-size:0.6rem;'
                    f'letter-spacing:0.12em;color:#5a5048;text-align:center;padding:1rem 0;">'
                    f'showing top {MAX_RESULTS} of {len(results_ing)} results'
                    f'</div>',
                    unsafe_allow_html=True,
                )