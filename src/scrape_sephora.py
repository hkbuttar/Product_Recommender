# scrape_sephora.py
# Selenium (undetected-chromedriver) Sephora scraper
# - Collects brand URLs from /brands-list
# - Visits each brand landing page, scrolls to load product grid, extracts product URLs
# - Visits each product page to extract: brand, name, category (breadcrumb), price, rating, ingredients
#
# Outputs:
#   data/raw/sephora_products.csv

import time
import random
import re
import json
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# -------------------------
# Config
# -------------------------

BASE = "https://www.sephora.com"
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Tune these for your scale
BRAND_LIMIT = 500         # how many brands to scrape
PRODUCTS_PER_BRAND = 50   # max product URLs per brand
MAX_BRAND_SCROLLS = 40    # scroll iterations per brand page
PRODUCT_LIMIT_TOTAL = 5000 # global cap across brands (safety)
# Selenium timeouts
PAGE_LOAD_SLEEP = 5
WAIT_GRID_SECONDS = 20
WAIT_PRODUCT_SECONDS = 20


# -------------------------
# Helpers
# -------------------------

# 1. Increase polite_sleep ranges significantly
def polite_sleep(a=3.0, b=7.0):
    time.sleep(random.uniform(a, b))

def norm_url(u: str) -> str:
    if not u:
        return u
    u = u.split("?")[0].strip()
    if u.startswith("/"):
        u = BASE + u
    return u

def extract_product_id(url: str):
    m = re.search(r"P(\d+)", url or "")
    return m.group(1) if m else None

def is_access_denied(html: str) -> bool:
    if not html:
        return False
    h = html.lower()
    return ("access denied" in h) or ("reference #" in h) or ("errors.edgesuite.net" in h)

def is_404_url(current_url: str) -> bool:
    return "error/404" in (current_url or "").lower()

def safe_text(soup, selector):
    el = soup.select_one(selector)
    return el.get_text(strip=True) if el else None

def safe_attr(soup, selector, attr):
    el = soup.select_one(selector)
    return el.get(attr) if el and el.has_attr(attr) else None

def extract_price_from_json(html):
    try:
        start = html.find("__NEXT_DATA__")
        if start == -1:
            return None

        json_start = html.find("{", start)
        json_end = html.find("</script>", json_start)
        data = json.loads(html[json_start:json_end])

        product = (
            data.get("props", {})
                .get("pageProps", {})
                .get("product", {})
        )

        # Try all known price locations
        price_fields = [
            ("currentSku", "listPrice"),
            ("currentSku", "salePrice"),
            ("regularChildSkus", 0, "listPrice"),
            ("regularChildSkus", 0, "salePrice"),
        ]

        for path in price_fields:
            node = product
            for p in path:
                if isinstance(p, int):
                    node = node[p] if isinstance(node, list) and len(node) > p else None
                else:
                    node = node.get(p) if isinstance(node, dict) else None
                if node is None:
                    break
            if node:
                return f"${node}"

        return None

    except Exception:
        return None


# -------------------------
# Selenium driver
# -------------------------

def make_driver(headless=False):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")

    driver = uc.Chrome(options=options, headless=headless)
    driver.set_page_load_timeout(60)

    # Warm-up — visit homepage and browse a bit like a real user
    driver.get(BASE + "/")
    time.sleep(10)
    driver.execute_script("window.scrollBy(0, 300);")
    time.sleep(3)
    return driver

# -------------------------
# Brand URL collection
# -------------------------

def get_brand_urls(driver, limit=10):
    driver.get(BASE + "/brands-list")
    time.sleep(PAGE_LOAD_SLEEP)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    links = soup.select("a[href^='/brand/']")

    urls = []
    seen = set()

    for a in links:
        href = a.get("href")
        if not href:
            continue
        full = norm_url(href)

        # Only keep /brand/<slug> (no extra path segments)
        parts = full.rstrip("/").split("/")
        if len(parts) != 5:
            continue

        slug = parts[-1].lower()

        # Skip known non-brand collections/pages
        if slug in {"sephora-favorites", "offers", "sale", "new", "gifts"}:
            continue

        if full not in seen:
            seen.add(full)
            urls.append(full)

        if len(urls) >= limit:
            break

    return urls


# -------------------------
# Product URL extraction from brand page (rendered grid)
# -------------------------

def wait_for_any(driver, selectors, timeout=20):
    """Wait until ANY selector is present in DOM."""
    end = time.time() + timeout
    while time.time() < end:
        for sel in selectors:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return sel
        time.sleep(0.5)
    return None

def scroll_and_collect_product_links(driver, limit=50, max_scrolls=20):
    """
    Collect product links from rendered page.
    Uses multiple selectors & scroll-to-load.
    """
    product_urls = []
    seen = set()

    # These cover multiple Sephora layouts
    link_selectors = [
        "a[href*='/product/']",
        "a[data-at*='product_item']",      # sometimes used
        "a[data-comp*='ProductTile']",     # tile link
    ]

    last_count = 0
    stagnant_rounds = 0

    for _ in range(max_scrolls):
        # parse current DOM
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select("a[href*='/product/']"):
            href = a.get("href")
            if not href:
                continue
            full = norm_url(href)
            if not full:
                continue
            if not re.search(r"/product/.*P\d+", full):
                continue
            if "subscription" in full.lower():
                continue
            if full not in seen:
                seen.add(full)
                product_urls.append(full)
                if len(product_urls) >= limit:
                    return product_urls

        # scroll down to trigger lazy loading
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
        time.sleep(2.5)

        # detect stagnation
        if len(product_urls) == last_count:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
            last_count = len(product_urls)

        # if we stop getting new products for a few scrolls, break
        if stagnant_rounds >= 3:
            break

    return product_urls

def get_product_urls_from_brand(driver, brand_url, limit=50):
    """
    Visit brand URL and extract product URLs.
    Includes retries if page partially hydrates or route is wrong.
    """
    # Normalize brand URL to /brand/<slug> only
    base = brand_url.split("?")[0].rstrip("/")
    parts = base.split("/")
    if len(parts) > 5:
        base = "/".join(parts[:5])

    # Try a couple times (refresh helps when React hydration fails)
    for attempt in range(1, 4):
        driver.get(base)
        time.sleep(PAGE_LOAD_SLEEP)

        cur = driver.current_url
        html = driver.page_source

        if is_404_url(cur):
            # Some brands auto-redirect; try stripping further (rare)
            print("404 page detected. Skipping brand.")
            return []

        if is_access_denied(html):
            print("Access Denied detected. Skipping brand.")
            return []

        # Wait for something that indicates product listing exists.
        # Brand pages vary; these are common containers.
        grid_sel = wait_for_any(
            driver,
            selectors=[
                "div[data-comp='ProductGrid']",
                "div[data-comp='ProductTile']",
                "div[data-at='product_grid']",
                "div[class*='ProductGrid']",
                "div[class*='ProductTile']",
                "main",
            ],
            timeout=WAIT_GRID_SECONDS
        )

        # Even if we can't "see" the grid selector, links may still be present after a bit
        if not grid_sel:
            time.sleep(3)

        urls = scroll_and_collect_product_links(driver, limit=limit, max_scrolls=MAX_BRAND_SCROLLS)

        if urls:
            return urls

        # If no URLs found, refresh and retry
        print(f"No product tiles found (attempt {attempt}/3). Refreshing…")
        driver.refresh()
        time.sleep(6)

    return []


# -------------------------
# Product page scraping (price + meta + ingredients)
# -------------------------

def scrape_product_page(driver, url):
    driver.get(url)
    time.sleep(PAGE_LOAD_SLEEP)

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    p_url_id = re.search(r"P(\d+)", url).group(1) if re.search(r"P(\d+)", url) else None

    brand = None
    name = None
    price = None
    rating = None
    category = None
    ingredients = None

    # --- Extract BV product ID from JS context ---
    try:
        js_data = driver.execute_script("""
            return {
                pId: (window.Sephora && Sephora.productPage) ? Sephora.productPage.productId : null,
                brand: (window.Sephora && Sephora.productPage) ? Sephora.productPage.brandName : null,
                name: (window.Sephora && Sephora.productPage) ? Sephora.productPage.displayName : null
            };
        """)
        brand = js_data.get('brand')
        name = js_data.get('name')
    except Exception:
        pass

    # -------------------------------------------------------
    # INGREDIENTS — Strategy 1: __NEXT_DATA__ JSON blob
    # Most reliable; Sephora embeds the full product object
    # including currentSku.ingredients in the page source.
    # -------------------------------------------------------
    ingredients = extract_ingredients_from_json(html)

    # -------------------------------------------------------
    # INGREDIENTS — Strategy 2: window.Sephora JS context
    # Rendered React apps sometimes surface currentSku data
    # on the global Sephora object before __NEXT_DATA__ does.
    # -------------------------------------------------------
    if not ingredients:
        try:
            js_ingredients = driver.execute_script("""
                try {
                    var page = window.Sephora && window.Sephora.productPage;
                    if (page && page.currentSku && page.currentSku.ingredients)
                        return page.currentSku.ingredients;
                    if (page && page.product && page.product.currentSku)
                        return page.product.currentSku.ingredients || null;
                    return null;
                } catch(e) { return null; }
            """)
            if js_ingredients and isinstance(js_ingredients, str) and js_ingredients.strip():
                ingredients = js_ingredients.strip()
        except Exception:
            pass

    # -------------------------------------------------------
    # INGREDIENTS — Strategy 3: DOM accordion / tab panel
    # Sephora renders an "Ingredients" expandable section.
    # The heading text is "Ingredients"; the sibling or child
    # div holds the raw ingredient list.
    # Selectors ordered from most to least specific.
    # -------------------------------------------------------
    if not ingredients:
        # 3a. data-at attribute (most stable across redesigns)
        for sel in [
            "[data-at='ingredients_section']",
            "[data-at='ingredients-section']",
            "[data-comp*='Ingredients']",
        ]:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator=" ", strip=True)
                if text:
                    ingredients = text
                    break

    if not ingredients:
        # 3b. Find any element whose visible text starts with "Ingredients"
        #     then grab its next sibling or inner paragraph.
        for heading in soup.find_all(
            lambda tag: tag.name in ("h2", "h3", "h4", "button", "span", "div")
            and tag.get_text(strip=True).lower() in ("ingredients", "ingredients:")
        ):
            # Look for the content container right after the heading
            sibling = heading.find_next_sibling()
            if sibling:
                text = sibling.get_text(separator=" ", strip=True)
                if len(text) > 20:   # guard against empty/decorative elements
                    ingredients = text
                    break
            # Some layouts wrap both heading + content in the same parent
            parent = heading.parent
            if parent:
                full = parent.get_text(separator=" ", strip=True)
                # Strip the heading text so we only keep the ingredient list
                cleaned = re.sub(r"^ingredients:?\s*", "", full, flags=re.IGNORECASE).strip()
                if len(cleaned) > 20:
                    ingredients = cleaned
                    break

    if not ingredients:
        # 3c. Raw regex over page HTML as last resort —
        #     catches JSON-in-script or inline data attributes
        m = re.search(
            r'"ingredients"\s*:\s*"([^"]{30,})"',
            html,
            re.IGNORECASE,
        )
        if m:
            ingredients = m.group(1).encode("utf-8").decode("unicode_escape").strip()

    # Sanitize: collapse extra whitespace from HTML entity expansion
    if ingredients:
        ingredients = re.sub(r"\s{2,}", " ", ingredients).strip()

    # --- Strategy 1: CSS selectors (most reliable for name) ---
    brand_selectors = [
        "a[data-at='brand_name']",
        "a[data-comp*='BrandName']",
        "span[data-at='brand_name']",
    ]
    name_selectors = [
        "span[data-at='product_title']",
        "h1[data-at='product_title']",
        "span[data-comp*='DisplayName']",
    ]
    price_selectors = [
        "[data-at='price']",
        "p[data-comp*='Price']",
        "div[data-at='price']",
        "b[data-at='price']",
    ]
    rating_selectors = [
        "span[data-at='rating']",
        "div[data-at='rating']",
    ]

    if not brand:
        for sel in brand_selectors:
            brand = safe_text(soup, sel)
            if brand:
                break

    if not name:
        for sel in name_selectors:
            name = safe_text(soup, sel)
            if name:
                break

    if not price:
        for sel in price_selectors:
            price = safe_text(soup, sel)
            if price:
                break

    if not rating:
        for sel in rating_selectors:
            r = safe_attr(soup, sel, "aria-label") or safe_text(soup, sel)
            if r:
                rating = r
                break

    # --- Strategy 2: Meta tags ---
    if not name:
        og_title = safe_attr(soup, "meta[property='og:title']", "content")
        if og_title:
            name = og_title.split("|")[0].strip()
    if not brand:
        brand = safe_attr(soup, "meta[property='product:brand']", "content")
    if not price:
        price = (
            safe_attr(soup, "meta[property='product:price:amount']", "content")
            or safe_attr(soup, "meta[property='og:price:amount']", "content")
        )
        if price and not price.startswith("$"):
            price = f"${price}"

    # --- Strategy 3: JSON-LD (strict — only @type Product) ---
    for script in soup.select("script[type='application/ld+json']"):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, list):
                ld = ld[0]
            if ld.get("@type") != "Product":
                continue
            if not name:
                name = ld.get("name")
            if not brand:
                b = ld.get("brand", {})
                brand = b.get("name") if isinstance(b, dict) else b
            if not price:
                offers = ld.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0]
                if isinstance(offers, dict):
                    p = offers.get("price") or offers.get("lowPrice")
                    if p:
                        price = f"${p}"
            if not rating:
                agg = ld.get("aggregateRating", {})
                if isinstance(agg, dict):
                    rating = agg.get("ratingValue")
            if not category:
                category = ld.get("category")
        except Exception:
            continue

    # --- Rating fallback: check ALL JSON-LD blocks for aggregateRating ---
    if not rating:
        for script in soup.select("script[type='application/ld+json']"):
            try:
                ld = json.loads(script.string)
                if isinstance(ld, list):
                    ld = ld[0]
                agg = ld.get("aggregateRating", {})
                if isinstance(agg, dict) and agg.get("ratingValue"):
                    rating = agg["ratingValue"]
                    break
            except Exception:
                continue

    # --- Price fallback: regex raw HTML for "price" field ---
    if not price:
        m = re.search(r'"price"\s*:\s*"(\d+\.?\d*)"', html)
        if m:
            price = f"${m.group(1)}"

    # --- Price fallback: JS extraction from rendered DOM ---
    if not price:
        try:
            js_price = driver.execute_script("""
                var el = document.querySelector('[data-at="price"]')
                    || document.querySelector('[data-comp*="Price"]');
                return el ? el.innerText : null;
            """)
            if js_price:
                m = re.search(r"\$[\d,.]+", js_price)
                if m:
                    price = m.group(0)
        except Exception:
            pass

    # --- Category from breadcrumbs ---
    if not category:
        bc = driver.find_elements(By.CSS_SELECTOR, "nav[aria-label='Breadcrumb'] ol li")
        if bc:
            category = bc[-1].text

    # Clean up product name (sometimes includes brand prefix)
    if name and brand and name.startswith(brand):
        name = name[len(brand):].strip(" -–—")

    return {
        "product_id": p_url_id,
        "product_url": url,
        "brand": brand,
        "product_name": name,
        "category": category,
        "price": price,
        "rating": rating,
        "ingredients": ingredients,
    }

# -------------------------
# Main
# -------------------------

def main():
    driver = make_driver(headless=False)

    # Load ALL existing product IDs across all files
    all_existing_pids = set()

    for csv_file in OUTPUT_DIR.glob("sephora_products*.csv"):
        try:
            df = pd.read_csv(csv_file)
            all_existing_pids.update(df["product_id"].dropna().astype(str))
            print(f"📂 Loaded {len(df)} products from {csv_file.name}")
        except Exception:
            continue

    print(f"Total already-scraped products: {len(all_existing_pids)}")

    # Track scraped brand slugs using exact URL slugs (not name conversion)
    brands_done_path = OUTPUT_DIR / "scraped_brand_slugs.txt"
    if brands_done_path.exists():
        all_existing_brand_slugs = set(brands_done_path.read_text().strip().splitlines())
    else:
        all_existing_brand_slugs = set()

    print(f"Already-scraped brand slugs: {len(all_existing_brand_slugs)}")

    print("Collecting brands...")
    brand_urls = get_brand_urls(driver, limit=BRAND_LIMIT)
    print(f"Collected {len(brand_urls)} real brand URLs")

    # Output files for this run
    products_path = OUTPUT_DIR / "sephora_products.csv"
    # Load partial progress from sephora_products
    if products_path.exists():
        try:
            existing_run = pd.read_csv(products_path)
            all_existing_pids.update(existing_run["product_id"].dropna().astype(str))
            print(f"Resuming: {len(existing_run)} products already in {products_path.name}")
        except Exception:
            pass

    seen_product_ids = set(all_existing_pids)
    new_products_this_run = 0

    for brand_url in brand_urls:
        brand_slug = brand_url.rstrip("/").split("/")[-1].lower()

        # Skip brands already scraped (exact URL slug match)
        if brand_slug in all_existing_brand_slugs:
            print(f"Skipping already-scraped brand: {brand_slug}")
            continue

        if len(seen_product_ids) >= PRODUCT_LIMIT_TOTAL:
            print("Reached global product cap; stopping.")
            break

        print(f"\nBrand: {brand_url}")

        product_urls = get_product_urls_from_brand(driver, brand_url, limit=PRODUCTS_PER_BRAND)
        if not product_urls:
            if is_access_denied(driver.page_source):
                print("Access Denied! Restarting browser with fresh session...")
                driver.quit()
                time.sleep(random.uniform(60, 90))
                driver = make_driver(headless=False)
                # Retry this brand once with the new session
                product_urls = get_product_urls_from_brand(driver, brand_url, limit=PRODUCTS_PER_BRAND)

            if not product_urls:
                print("No product URLs found. Skipping brand.")
                all_existing_brand_slugs.add(brand_slug)
                with open(brands_done_path, "a") as f:
                    f.write(brand_slug + "\n")
                continue

        # Early exit: if first 3 real products are all already known, skip this brand
        first_pids = []
        for u in product_urls[:5]:
            m = re.search(r"P(\d+)", u)
            if m:
                first_pids.append(m.group(1))
        if first_pids and all(pid in seen_product_ids for pid in first_pids):
            print(f"  All sample products already scraped. Skipping brand.")
            all_existing_brand_slugs.add(brand_slug)
            with open(brands_done_path, "a") as f:
                f.write(brand_slug + "\n")
            continue

        brand_products = []

        for url in product_urls:
            if len(seen_product_ids) >= PRODUCT_LIMIT_TOTAL:
                break

            pid_match = re.search(r"P(\d+)", url)
            if pid_match and pid_match.group(1) in seen_product_ids:
                print(f"  Already have {url}, skipping.")
                continue

            print(f"  Product: {url}")

            prod = scrape_product_page(driver, url)

            if not prod:
                print("    Failed to parse product page.")
                continue

            pid = prod["product_id"]
            if pid in seen_product_ids:
                continue
            seen_product_ids.add(pid)

            # Log ingredient extraction outcome for debugging
            if prod.get("ingredients"):
                print(f"    Ingredients: {prod['ingredients'][:80]}...")
            else:
                print(f"    Ingredients: not found")

            brand_products.append(prod)


            polite_sleep(5.0, 12.0)

        # --- Save after every brand (incremental / crash-safe) ---
        if brand_products:
            pd.DataFrame(brand_products).to_csv(
                products_path,
                mode="a",
                header=not products_path.exists(),
                index=False,
            )
            new_products_this_run += len(brand_products)


        # Mark brand as done with exact URL slug
        all_existing_brand_slugs.add(brand_slug)
        with open(brands_done_path, "a") as f:
            f.write(brand_slug + "\n")

        print(f"  Saved {len(brand_products)} products for {brand_slug}")

    driver.quit()

    print("\n--- Scrape Complete ---")
    print(f"New products this run: {new_products_this_run}")
    print(f"Total products across all files: {len(seen_product_ids)}")


if __name__ == "__main__":
    main()