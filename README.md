# Product Recommender

A Streamlit dashboard that recommends beauty products based on exact ingredient overlap. Given any product in the dataset, it finds the most ingredient-similar products in the same category — ranked by how many ingredients they share.

---

## Project Structure

```
Product_Recommender/
├── data/
│   ├── raw/
│   │   ├── sephora_products.csv
│   │   ├── ulta_products.csv
│   │   ├── scraped_brand_slugs.txt        # Sephora resume state
│   │   └── ulta_scraped_brand_slugs.txt   # Ulta resume state
│   └── processed/
│       ├── sephora_products_clean.csv
│       ├── ulta_products_clean.csv
│       └── combined_products.csv
├── src/
│   ├── scrape_sephora.py
│   ├── scrape_ulta.py
│   ├── data_cleaning.py
│   ├── combine_datasets.py
│   └── app.py
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Create and activate a virtual environment

```bash
python3 -m venv venv_ingredients
source venv_ingredients/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Chrome requirement

Both scrapers use `undetected-chromedriver`, which requires Google Chrome to be installed on your machine. The driver version is matched automatically.

---

## Usage

Run the scripts in order.

---

### Step 1a — Scrape Sephora

Collects product data from sephora.com by crawling brand pages.

```bash
python src/scrape_sephora.py
```

What it does:
- Fetches all brand URLs from `/brands-list`
- Visits each brand page, scrolls to load the full product grid, and collects product URLs
- Visits each product page and extracts: brand, product name, category (from breadcrumb), price, rating, and ingredients
- Ingredients are extracted from the `__NEXT_DATA__` JSON blob embedded in the page, with DOM accordion and regex fallbacks
- Saves incrementally after every brand — safe to interrupt and resume
- Rotates the browser session every 8 brands and inserts randomised delays to avoid detection

**Key config constants** (top of file):

| Constant | Default | Description |
|---|---|---|
| `BRAND_LIMIT` | 500 | Max brands to crawl |
| `PRODUCTS_PER_BRAND` | 50 | Max product URLs per brand |
| `PRODUCT_LIMIT_TOTAL` | 50000 | Global product cap |
| `BRANDS_PER_SESSION` | 8 | Browser session rotation frequency |

**Resume behaviour:** Scraped brand slugs are written to `data/raw/scraped_brand_slugs.txt`. Already-seen product IDs are loaded from any existing `sephora_products*.csv` files. Re-running the script skips both.

Output: `data/raw/sephora_products.csv`

---

### Step 1b — Scrape Ulta

Collects product data from ulta.com by crawling brand pages.

```bash
python src/scrape_ulta.py
```

What it does:
- Fetches all brand URLs from `/brand/all`
- Visits each brand page, scrolls to load the full product grid, and collects product URLs (identified by `pimprod` ID in the URL)
- Visits each product page and extracts: brand, product name, category, price, rating, and ingredients
- Ingredients are extracted from `__NEXT_DATA__` first, with DOM and regex fallbacks
- Saves incrementally after every brand — safe to interrupt and resume

**Key config constants** (top of file):

| Constant | Default | Description |
|---|---|---|
| `BRAND_LIMIT` | 5000 | Max brands to crawl |
| `PRODUCTS_PER_BRAND` | 50 | Max product URLs per brand |
| `PRODUCT_LIMIT_TOTAL` | 50000 | Global product cap |

**Resume behaviour:** Scraped brand slugs are written to `data/raw/ulta_scraped_brand_slugs.txt`. Already-seen product IDs are loaded from any existing `ulta_products.csv` file. Re-running the script skips both.

Output: `data/raw/ulta_products.csv`

---

### Step 2 — Clean the raw data

Cleans both raw CSVs and writes them to `data/processed/`.

```bash
python src/data_cleaning.py
```

What it does:
- Drops products with missing ingredients
- Fixes mixed line-terminator issues in raw scraped files (reads raw bytes, normalises before parsing)
- Ensures the ingredients field is always quoted correctly (some rows omit quotes despite ingredients containing commas)
- Removes marketing blurbs (e.g. `- Rich in fatty acids, helps nourish dry skin.`)
- Strips shade/colour-name prefixes (e.g. `01 Always Red - Isododecane, ...`)
- Normalises multi-line ingredient cells
- Lowercases all ingredient strings

Output: `data/processed/sephora_products_clean.csv`, `data/processed/ulta_products_clean.csv`

---

### Step 3 — Combine and deduplicate

Merges both cleaned files into a single dataset.

```bash
python src/combine_datasets.py
```

What it does:
- Tags each product with its source retailer (`sephora` / `ulta`)
- Normalises all 261 raw category names down to ~55 canonical categories
- Uses fuzzy matching (`rapidfuzz` token sort ratio, threshold 85) on brand + product name to identify duplicates across retailers
- Keeps the Ulta copy on a match (fuller ingredient lists), drops the Sephora duplicate

Output: `data/processed/combined_products.csv`

---

### Step 4 — Run the app

```bash
streamlit run src/app.py
```

Then open [http://localhost:8501](http://localhost:8501) in your browser.

---

## How the Recommender Works

1. Select a category and search for a product in the sidebar
2. The app parses the selected product's ingredient list into a set of individual ingredient names
3. Every other product in the same category is scored by exact ingredient overlap — no fuzzy matching or standardisation, ingredient names must match exactly
4. Results are ranked by:
   - **Primary:** number of shared ingredients (descending)
   - **Tiebreak:** Jaccard similarity — shared / union — to penalise products that share ingredients only because they have very long lists
   - **Secondary tiebreak:** rating (descending)
5. Each result card shows the match percentage, a visual match bar, and the full list of shared ingredients

Up to 20 results are returned, all within the same category as the query product.

---

## Notes

- All ingredient matching is case-insensitive (ingredients are lowercased during cleaning)
- Products without ingredients are excluded entirely from the dataset
- The `source` column in the combined file indicates which retailer the product came from (`sephora` or `ulta`)
- Both scrapers run with `headless=False` by default so the browser window is visible; set to `True` in `make_driver()` to run headlessly