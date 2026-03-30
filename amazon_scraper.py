"""
amazon_scraper.py
Scrapes real Amazon product data from search results.
Returns structured product dicts with ASIN, title, price, rating, image, and affiliate URL.
"""

import time
import logging
import re
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Cache-Control": "max-age=0",
}


def scrape_amazon_products(search_query: str, associate_tag: str, num_results: int = 5) -> list:
    """
    Scrape Amazon search results for real product data.

    Args:
        search_query: The product search keyword (e.g. "best baby thermometer")
        associate_tag: Amazon Associates tag (e.g. "viciousstudio-20")
        num_results: Number of products to return (default 5)

    Returns:
        List of dicts with keys: asin, title, price, rating, image, url
        Returns empty list if blocked or no results found.
    """
    url = f"https://www.amazon.com/s?k={quote_plus(search_query)}"
    log.info(f"Scraping Amazon: {url}")

    session = requests.Session()
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"Request failed for '{search_query}': {e}")
        return []

    # Sleep 1 second after request
    time.sleep(1)

    soup = BeautifulSoup(resp.text, "lxml")

    # Check for captcha / block
    if soup.find("form", {"action": "/errors/validateCaptcha"}) or "captcha" in resp.text.lower()[:2000]:
        log.warning(f"Amazon CAPTCHA detected for query: '{search_query}'")
        return []

    # Find all search result items
    result_divs = soup.find_all("div", {"data-asin": True, "data-component-type": "s-search-result"})

    if not result_divs:
        # Fallback: try any div with data-asin
        result_divs = soup.find_all("div", {"data-asin": True})

    if not result_divs:
        log.warning(f"No search results found for query: '{search_query}'")
        return []

    products = []
    for div in result_divs:
        if len(products) >= num_results:
            break

        asin = div.get("data-asin", "").strip()
        if not asin:
            continue

        # Title
        title = None
        h2 = div.find("h2")
        if h2:
            a_tag = h2.find("a")
            if a_tag:
                span = a_tag.find("span")
                if span:
                    title = span.get_text(strip=True)
                else:
                    title = a_tag.get_text(strip=True)
            else:
                title = h2.get_text(strip=True)

        if not title:
            continue

        # Price
        price = None
        price_whole = div.find("span", class_="a-price-whole")
        if price_whole:
            price_text = price_whole.get_text(strip=True).rstrip(".")
            price_fraction = div.find("span", class_="a-price-fraction")
            if price_fraction:
                price = f"${price_text}.{price_fraction.get_text(strip=True)}"
            else:
                price = f"${price_text}.00"

        # Rating
        rating = None
        rating_span = div.find("span", class_="a-icon-alt")
        if rating_span:
            rating = rating_span.get_text(strip=True)

        # Image
        image = None
        img = div.find("img", class_="s-image")
        if img:
            image = img.get("src", "")

        # Product URL
        product_url = f"https://www.amazon.com/dp/{asin}?tag={associate_tag}"

        products.append({
            "asin": asin,
            "title": title,
            "price": price,
            "rating": rating,
            "image": image,
            "url": product_url,
        })

    log.info(f"Found {len(products)} products for '{search_query}'")
    return products


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Quick test
    results = scrape_amazon_products("baby thermometer ear", "viciousstudio-20", 5)
    for p in results:
        print(f"  {p['asin']}: {p['title'][:60]} — {p['price']} — {p['rating']}")
        print(f"    {p['url']}")
        print(f"    img: {p['image'][:80] if p['image'] else 'N/A'}")
        print()
