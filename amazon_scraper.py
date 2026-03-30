"""Amazon product scraper using SerpApi — verified real products only."""
import re, time, json, logging, os
from dataclasses import dataclass, asdict
from urllib.parse import quote_plus, unquote_plus
import requests
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SERPAPI_KEY = "b886d3c465f5a5aa1dfeed224a07ac36c4787e59be8f17bb7364218321d81d90"
ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "viciousstudio-20")
ASIN_RE = re.compile(r'^[A-Z0-9]{10}$')


@dataclass
class Product:
    asin: str
    title: str
    price: str
    rating: str
    image: str
    url: str
    search_query: str


def scrape_amazon_products(search_query: str, associate_tag: str = None, num_results: int = 5) -> list:
    """Fetch real Amazon products via SerpApi."""
    if associate_tag is None:
        associate_tag = ASSOCIATE_TAG

    api_url = "https://serpapi.com/search.json"
    params = {
        "engine": "amazon",
        "amazon_domain": "amazon.com",
        "k": search_query,
        "api_key": SERPAPI_KEY,
    }

    try:
        resp = requests.get(api_url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"SerpApi request failed for '{search_query}': {e}")
        return []

    time.sleep(0.5)

    if "error" in data:
        log.warning(f"SerpApi error for '{search_query}': {data['error']}")
        return []

    organic = data.get("organic_results", [])
    if not organic:
        log.warning(f"No organic results for '{search_query}'")
        return []

    products = []
    seen_asins = set()

    for result in organic:
        asin = result.get("asin", "").strip()
        if not asin or not ASIN_RE.match(asin) or asin in seen_asins:
            continue

        title = result.get("title", "").strip()
        if not title:
            continue

        # Price extraction
        price = ""
        price_info = result.get("price", {})
        if isinstance(price_info, dict):
            raw = price_info.get("raw", "")
            if raw:
                price = raw
            else:
                extracted = price_info.get("extracted_price") or price_info.get("value")
                if extracted:
                    price = f"${float(extracted):.2f}"
        elif isinstance(price_info, (int, float)):
            price = f"${float(price_info):.2f}"

        if not price:
            extracted = result.get("extracted_price")
            if extracted:
                price = f"${float(extracted):.2f}"
            else:
                price = result.get("price_raw", "")

        # Rating
        rating_val = result.get("rating")
        rating = f"{rating_val} out of 5 stars" if rating_val else ""

        # Image
        image = result.get("thumbnail", "")
        if not image.startswith("https://"):
            continue

        seen_asins.add(asin)
        products.append(Product(
            asin=asin,
            title=title,
            price=price if price else "See Amazon for price",
            rating=rating if rating else "Highly rated",
            image=image,
            url=f"https://www.amazon.com/dp/{asin}?tag={associate_tag}",
            search_query=search_query,
        ))

        if len(products) >= num_results:
            break

    log.info(f"Scraped {len(products)} products for '{search_query}'")
    return products


def verify_products(products: list, associate_tag: str = None) -> tuple:
    """Verify all product data is valid. Returns (valid, errors)."""
    if associate_tag is None:
        associate_tag = ASSOCIATE_TAG
    valid = []
    errors = []
    for p in products:
        product = p if isinstance(p, Product) else Product(**p)
        issues = []
        if not ASIN_RE.match(product.asin):
            issues.append(f"Invalid ASIN format: {product.asin}")
        if not product.title or len(product.title) > 500:
            issues.append(f"Bad title for {product.asin}")
        if f"/dp/{product.asin}" not in product.url:
            issues.append(f"URL doesn't match ASIN {product.asin}")
        if f"tag={associate_tag}" not in product.url:
            issues.append(f"Missing tag in URL for {product.asin}")
        if not product.image.startswith("https://"):
            issues.append(f"Bad image for {product.asin}")
        if issues:
            errors.extend(issues)
        else:
            valid.append(product)
    return valid, errors


def self_test() -> bool:
    """Run a test search and verify results."""
    print("=" * 60)
    print("AMAZON SCRAPER SELF-TEST (SerpApi)")
    print("=" * 60)

    products = scrape_amazon_products("led desk lamp", ASSOCIATE_TAG, 3)
    valid, errors = verify_products(products)

    print(f"\nResults: {len(valid)} valid products, {len(errors)} errors")
    for p in valid:
        print(f"  ASIN: {p.asin}")
        print(f"  Title: {p.title[:80]}")
        print(f"  Price: {p.price}")
        print(f"  Rating: {p.rating}")
        print(f"  Image: {p.image[:60]}...")
        print(f"  URL: {p.url}")
        print()

    for e in errors:
        print(f"  ERROR: {e}")

    if len(valid) >= 1:
        print("SELF-TEST: PASS ✅")
        return True
    else:
        print("SELF-TEST: FAIL ❌")
        return False


if __name__ == "__main__":
    self_test()
