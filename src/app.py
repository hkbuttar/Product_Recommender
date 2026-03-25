"""
app.py
------
Streamlit ingredient-matching product recommender.

Run:
    streamlit run src/app.py
"""

from pathlib import Path
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_PATH    = _REPO_ROOT / "data" / "processed" / "combined_products.csv"
MAX_RESULTS  = 20


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_PATH)
    # Ensure ingredients is a string; drop anything that slipped through
    df = df[df["ingredients"].notna() & (df["ingredients"].astype(str).str.strip() != "")]
    df["ingredients"] = df["ingredients"].astype(str).str.lower().str.strip()
    df["product_name"] = df["product_name"].astype(str).str.strip()
    df["brand"] = df["brand"].astype(str).str.strip()
    df["category"] = df["category"].astype(str).str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    return df.reset_index(drop=True)


def parse_ingredients(raw: str) -> set[str]:
    """Split a comma-separated ingredient string into a cleaned set."""
    return {i.strip().rstrip(".") for i in raw.split(",") if i.strip()}


# ---------------------------------------------------------------------------
# Recommendation logic
# ---------------------------------------------------------------------------

def recommend(
    query_ingredients: set[str],
    df: pd.DataFrame,
    category: str,
    exclude_idx: int | None = None,
) -> pd.DataFrame:
    """
    Score every product in `category` by exact ingredient overlap with
    `query_ingredients`. Returns a sorted DataFrame, highest match first.
    """
    pool = df[df["category"] == category].copy()
    if exclude_idx is not None:
        pool = pool[pool.index != exclude_idx]

    if pool.empty:
        return pool

    def _score(row) -> tuple[int, int, float]:
        product_ings = parse_ingredients(row["ingredients"])
        shared       = query_ingredients & product_ings
        n_shared     = len(shared)
        n_total      = len(query_ingredients | product_ings)
        jaccard      = n_shared / n_total if n_total else 0.0
        return n_shared, len(product_ings), jaccard

    scores = pool.apply(_score, axis=1, result_type="expand")
    scores.columns = ["shared_count", "total_ings", "jaccard"]

    pool = pool.join(scores)
    pool["match_pct"] = (pool["shared_count"] / len(query_ingredients) * 100).round(1)
    pool = pool[pool["shared_count"] > 0]
    pool = pool.sort_values(
        ["shared_count", "jaccard", "rating"],
        ascending=[False, False, False],
    )
    return pool.head(MAX_RESULTS).reset_index(drop=True)


def get_shared_ingredients(query_ings: set[str], product_ings_raw: str) -> list[str]:
    product_ings = parse_ingredients(product_ings_raw)
    return sorted(query_ings & product_ings)


# ---------------------------------------------------------------------------
# UI Helpers
# ---------------------------------------------------------------------------

def _rating_stars(rating: float) -> str:
    if pd.isna(rating):
        return "—"
    full  = int(rating)
    frac  = rating - full
    half  = "½" if frac >= 0.4 else ""
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

/* ── Base ── */
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
.divider {
    border: none;
    border-top: 1px solid #2a2520;
    margin: 1.5rem 0;
}

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

/* ── Selectbox / inputs ── */
[data-testid="stSelectbox"] > div > div,
[data-testid="stTextInput"] > div > div > input {
    background-color: #1e1a16 !important;
    border: 1px solid #3a3028 !important;
    border-radius: 2px !important;
    color: #e8ddd0 !important;
    font-family: 'Cormorant Garamond', serif !important;
    font-size: 1rem !important;
}
[data-testid="stSelectbox"] svg { color: #8a7d6e !important; }

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
    transition: all 0.2s ease !important;
}
[data-testid="stButton"] > button:hover {
    background: #c9a96e !important;
    color: #0f0d0b !important;
}

/* ── Metric cards ── */
.metric-row {
    display: flex;
    gap: 1rem;
    margin: 1rem 0 1.5rem;
}
.metric-card {
    background: #1a1612;
    border: 1px solid #2a2520;
    padding: 1rem 1.5rem;
    flex: 1;
}
.metric-value {
    font-size: 2rem;
    font-weight: 300;
    color: #c9a96e;
    line-height: 1;
}
.metric-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.2em;
    color: #8a7d6e;
    text-transform: uppercase;
    margin-top: 0.3rem;
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
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    color: #8a7d6e;
    text-transform: uppercase;
}
.query-name {
    font-size: 1.4rem;
    font-weight: 300;
    color: #e8ddd0;
    margin: 0.2rem 0 0.4rem;
}
.query-category {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.15em;
    color: #c9a96e;
    text-transform: uppercase;
}

/* ── Result cards ── */
.result-card {
    background: #141210;
    border: 1px solid #2a2520;
    padding: 1.1rem 1.4rem;
    margin-bottom: 0.75rem;
    transition: border-color 0.2s;
    position: relative;
}
.result-card:hover { border-color: #3a3028; }
.result-rank {
    position: absolute;
    top: 1rem;
    right: 1.2rem;
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.15em;
    color: #5a5048;
}
.result-brand {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.2em;
    color: #8a7d6e;
    text-transform: uppercase;
}
.result-name {
    font-size: 1.15rem;
    font-weight: 400;
    color: #e8ddd0;
    margin: 0.2rem 0 0.5rem;
    padding-right: 3rem;
}
.result-meta {
    display: flex;
    align-items: center;
    gap: 1rem;
    flex-wrap: wrap;
    margin-bottom: 0.5rem;
}
.match-bar-wrap {
    margin: 0.6rem 0 0.4rem;
}
.match-bar-bg {
    background: #2a2520;
    height: 3px;
    border-radius: 2px;
    width: 100%;
}
.match-bar-fill {
    height: 3px;
    border-radius: 2px;
    background: linear-gradient(90deg, #c9a96e, #e8c898);
}
.match-pct-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: #c9a96e;
}
.shared-ings {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.05em;
    color: #6a6058;
    line-height: 1.6;
    margin-top: 0.4rem;
}
.shared-ings-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.15em;
    color: #8a7d6e;
    text-transform: uppercase;
    margin-bottom: 0.2rem;
}
.price-tag {
    font-family: 'DM Mono', monospace;
    font-size: 0.75rem;
    color: #8a7d6e;
}
.rating-tag {
    font-size: 0.9rem;
    color: #c9a96e;
    letter-spacing: 0.05em;
}
.no-results {
    font-family: 'DM Mono', monospace;
    font-size: 0.75rem;
    letter-spacing: 0.15em;
    color: #5a5048;
    text-align: center;
    padding: 3rem 0;
    text-transform: uppercase;
}
.section-heading {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.25em;
    color: #8a7d6e;
    text-transform: uppercase;
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

df = load_data()
categories = sorted(df["category"].dropna().unique().tolist())


# ---------------------------------------------------------------------------
# Sidebar — search controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown('<div class="sidebar-label">Find a product</div>', unsafe_allow_html=True)

    # Category filter
    selected_category = st.selectbox(
        "Category",
        options=["All categories"] + categories,
        label_visibility="collapsed",
    )

    cat_df = df if selected_category == "All categories" else df[df["category"] == selected_category]

    # Product search
    st.markdown('<div class="sidebar-label" style="margin-top:1.2rem">Product name or brand</div>', unsafe_allow_html=True)
    search_query = st.text_input(
        "Search",
        placeholder="e.g. Laneige, toner, serum …",
        label_visibility="collapsed",
    )

    if search_query.strip():
        q = search_query.strip().lower()
        mask = (
            cat_df["product_name"].str.lower().str.contains(q, na=False)
            | cat_df["brand"].str.lower().str.contains(q, na=False)
        )
        filtered = cat_df[mask]
    else:
        filtered = cat_df

    # Product selector
    st.markdown('<div class="sidebar-label" style="margin-top:1.2rem">Select product</div>', unsafe_allow_html=True)

    if filtered.empty:
        st.markdown('<div style="font-size:0.8rem;color:#5a5048">No products found.</div>', unsafe_allow_html=True)
        selected_idx = None
    else:
        labels = [
            f"{row['brand']} — {row['product_name']}"
            for _, row in filtered.iterrows()
        ]
        choice = st.selectbox(
            "Product",
            options=range(len(filtered)),
            format_func=lambda i: labels[i],
            label_visibility="collapsed",
        )
        selected_idx = filtered.index[choice]

    st.markdown('<hr class="divider"/>', unsafe_allow_html=True)
    st.markdown(
        '<div style="font-family:\'DM Mono\',monospace;font-size:0.6rem;'
        'letter-spacing:0.1em;color:#3a3028;line-height:1.6">'
        f'{len(df):,} products · exact ingredient matching · same-category only'
        '</div>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

st.markdown('<div class="main-title">INGREDIENT<br>MATCH</div>', unsafe_allow_html=True)
st.markdown('<div class="main-subtitle">✦ product recommender ✦</div>', unsafe_allow_html=True)
st.markdown('<hr class="divider"/>', unsafe_allow_html=True)

if selected_idx is None:
    st.markdown(
        '<div class="no-results" style="padding-top:5rem">'
        'select a product in the sidebar to begin'
        '</div>',
        unsafe_allow_html=True,
    )
    st.stop()

# Selected product
product    = df.loc[selected_idx]
query_ings = parse_ingredients(product["ingredients"])
p_category = product["category"]
p_price    = f"${product['price']:.2f}" if pd.notna(product.get("price")) else "—"
p_rating   = product.get("rating", float("nan"))
p_source   = product.get("source", "")

# ── Query product card ──
st.markdown('<div class="section-heading">Selected product</div>', unsafe_allow_html=True)
st.markdown(f"""
<div class="query-card">
    <div class="query-brand">{product['brand']}&nbsp;&nbsp;{_source_badge(p_source)}</div>
    <div class="query-name">{product['product_name']}</div>
    <div style="display:flex;gap:1.5rem;align-items:center">
        <div class="query-category">{p_category}</div>
        <div class="price-tag">{p_price}</div>
        <div class="rating-tag">{_rating_stars(p_rating)} {f'{p_rating:.2f}' if pd.notna(p_rating) else ''}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Metrics ──
results = recommend(query_ings, df, p_category, exclude_idx=selected_idx)

st.markdown(f"""
<div class="metric-row">
    <div class="metric-card">
        <div class="metric-value">{len(query_ings)}</div>
        <div class="metric-label">ingredients in product</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{len(results)}</div>
        <div class="metric-label">matches found</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{results['shared_count'].max() if not results.empty else 0}</div>
        <div class="metric-label">max shared ingredients</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">{results['match_pct'].max() if not results.empty else 0:.0f}%</div>
        <div class="metric-label">highest match rate</div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown('<hr class="divider"/>', unsafe_allow_html=True)

# ── Results ──
st.markdown('<div class="section-heading">Recommended products · sorted by ingredient overlap</div>', unsafe_allow_html=True)

if results.empty:
    st.markdown(
        '<div class="no-results">no matching products found in this category</div>',
        unsafe_allow_html=True,
    )
else:
    for rank, (_, row) in enumerate(results.iterrows(), start=1):
        shared     = get_shared_ingredients(query_ings, row["ingredients"])
        price_str  = f"${row['price']:.2f}" if pd.notna(row.get("price")) else "—"
        rating_val = row.get("rating", float("nan"))
        match_pct  = row["match_pct"]
        bar_w      = min(100, match_pct)
        source_b   = _source_badge(row.get("source", ""))
        n_shared   = row["shared_count"]

        # Shared ingredients — show first 12, summarise rest
        if len(shared) > 12:
            shown = ", ".join(shared[:12])
            extra = f" +{len(shared) - 12} more"
        else:
            shown = ", ".join(shared)
            extra = ""

        st.markdown(f"""
<div class="result-card">
    <div class="result-rank">#{rank:02d}</div>
    <div class="result-brand">{row['brand']}&nbsp;&nbsp;{source_b}</div>
    <div class="result-name">{row['product_name']}</div>
    <div class="result-meta">
        <div class="price-tag">{price_str}</div>
        <div class="rating-tag">{_rating_stars(rating_val)} {f'{rating_val:.2f}' if pd.notna(rating_val) else ''}</div>
        <div class="match-pct-label">{n_shared} shared · {match_pct:.1f}% match</div>
    </div>
    <div class="match-bar-wrap">
        <div class="match-bar-bg">
            <div class="match-bar-fill" style="width:{bar_w}%"></div>
        </div>
    </div>
    <div class="shared-ings-label">shared ingredients</div>
    <div class="shared-ings">{shown}{extra}</div>
</div>
""", unsafe_allow_html=True)