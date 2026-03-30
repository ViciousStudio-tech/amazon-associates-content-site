"""Amazon product scraper with full verification."""
import re, time, logging, hashlib
from dataclasses import dataclass, asdict
from urllib.parse import quote_plus, unquote_plus
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

@dataclass
class Product:
    asin: str
    title: str
    price: str
    rating: str
    image: str
    url: str
    search_query: str

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ASIN_RE = re.compile(r'^[A-Z0-9]{10}$')

def scrape_amazon_products(search_query: str, associate_tag: str, num_results: int = 5) -> list:
    url = f"https://www.amazon.com/s?k={quote_plus(search_query)}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        time.sleep(1.5)
    except Exception as e:
        log.warning(f"Request failed for '{search_query}': {e}")
        return []

    if "validateCaptcha" in resp.text or "robot" in resp.text.lower()[:5000]:
        log.warning(f"CAPTCHA detected for '{search_query}'")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    products = []
    seen_asins = set()

    for div in soup.select('[data-asin]'):
        asin = div.get('data-asin', '').strip()
        if not asin or not ASIN_RE.match(asin) or asin in seen_asins:
            continue

        title_el = div.select_one('h2 a span')
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        price = ""
        price_whole = div.select_one('.a-price-whole')
        price_frac = div.select_one('.a-price-fraction')
        if price_whole:
            p = price_whole.get_text(strip=True).rstrip('.')
            f = price_frac.get_text(strip=True) if price_frac else "00"
            price = f"${p}.{f}"

        rating = ""
        rating_el = div.select_one('span.a-icon-alt')
        if rating_el:
            rating = rating_el.get_text(strip=True)

        image = ""
        img_el = div.select_one('img.s-image')
        if img_el:
            image = img_el.get('src', '')
        if not image.startswith('https://'):
            continue

        seen_asins.add(asin)
        products.append(Product(
            asin=asin,
            title=title,
            price=price,
            rating=rating,
            image=image,
            url=f"https://www.amazon.com/dp/{asin}?tag={associate_tag}",
            search_query=search_query
        ))

        if len(products) >= num_results:
            break

    return products

def verify_products(products, associate_tag="viciousstudio-20"):
    valid, errors = [], []
    for p in products:
        if not ASIN_RE.match(p.asin):
            errors.append(f"Invalid ASIN format: {p.asin}")
            continue
        if not p.title or len(p.title) > 300:
            errors.append(f"Bad title for {p.asin}: '{p.title[:50]}'")
            continue
        if f"/dp/{p.asin}" not in p.url:
            errors.append(f"URL doesn't match ASIN {p.asin}: {p.url}")
            continue
        if f"tag={associate_tag}" not in p.url:
            errors.append(f"Missing affiliate tag in URL for {p.asin}")
            continue
        if not p.image.startswith("https://"):
            errors.append(f"Bad image URL for {p.asin}")
            continue
        valid.append(p)
    return valid, errors

def self_test():
    print("Running scraper self-test...")
    products = scrape_amazon_products("led desk lamp", "viciousstudio-20", 3)
    valid, errors = verify_products(products)
    print(f"SCRAPER SELF-TEST: {len(valid)} valid products, {len(errors)} errors")
    for p in valid:
        print(f"  {p.asin} | {p.title[:60]} | {p.price} | {p.url}")
    for e in errors:
        print(f"  ERROR: {e}")
    if len(valid) >= 1:
        print("SELF-TEST: PASS")
        return True
    else:
        print("SELF-TEST: FAIL")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    self_test()
