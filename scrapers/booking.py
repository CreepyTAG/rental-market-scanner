"""
Booking.com scraper — apartments category.
Extracts listings + geocoding via Nominatim.
"""

import asyncio
import json
import os
import re
import ssl
import hashlib
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import certifi
from playwright.async_api import Page
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from browser.brave import get_new_page, random_delay, human_scroll

# Python 3.14 on macOS ships without a configured CA bundle; point geopy at certifi's.
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

console = Console()
RAW_CACHE_DIR = Path("raw_cache/booking")
RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_geocoder = Nominatim(
    user_agent="rental-market-scanner/1.0",
    ssl_context=ssl.create_default_context(cafile=certifi.where()),
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _cache_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _save_raw(key: str, data: dict) -> None:
    path = RAW_CACHE_DIR / f"{key}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def _build_search_url(city_name: str, bbox: dict, offset: int = 0) -> str:
    """
    Build Booking.com search URL filtering for apartments.
    Dates are required (no dates → no prices rendered).
    selected_currency=EUR is required (the default may be USD depending on the browser profile).
    Uses a 1-night stay so the displayed price equals the per-night price.
    """
    clean_city = city_name.replace("'", "%27").replace(" ", "+")
    checkin = (date.today() + timedelta(days=30)).isoformat()
    checkout = (date.today() + timedelta(days=31)).isoformat()
    return (
        f"https://www.booking.com/searchresults.fr.html"
        f"?ss={clean_city}"
        f"&nflt=ht_id%3D201%3Bht_id%3D220%3Bht_id%3D216%3Bht_id%3D212"  # Appartements, Maisons, Chalets, Villas
        f"&checkin={checkin}"
        f"&checkout={checkout}"
        f"&group_adults=2"
        f"&no_rooms=1"
        f"&selected_currency=EUR"
        f"&offset={offset}"
        f"&bbox={bbox['west']},{bbox['south']},{bbox['east']},{bbox['north']}"
        f"&order=popularity"
    )


# ── Geocoding ─────────────────────────────────────────────────────────────────

async def _geocode_address(address: str, city: str) -> tuple[Optional[float], Optional[float]]:
    """
    Geocode an address string using Nominatim (async wrapper).
    Tries a few variants since Booking addresses are usually only a commune name
    and our city label can include suffixes Nominatim doesn't recognise
    (e.g. "Angers Agglomération").
    """
    # Strip suffixes Nominatim doesn't know
    city_clean = re.sub(r"\s+(Agglomération|Agglo|Centre|Métropole)\s*$", "", city, flags=re.IGNORECASE).strip()
    candidates = []
    if address:
        candidates.append(address)
        if city_clean and city_clean.lower() not in address.lower():
            candidates.append(f"{address}, {city_clean}")
    elif city_clean:
        candidates.append(city_clean)


    loop = asyncio.get_event_loop()
    for q in candidates:
        try:
            location = await loop.run_in_executor(
                None,
                lambda qq=q: _geocoder.geocode(qq, timeout=5),
            )
            if location:
                return location.latitude, location.longitude
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            console.log(f"[yellow]Géocodage échoué pour '{q}': {e}[/yellow]")
            continue
    return None, None


# ── Price parsing ─────────────────────────────────────────────────────────────

def _parse_price(raw: str) -> Optional[float]:
    """Extract numeric price from booking price strings."""
    cleaned = re.sub(r"[^\d,.]", "", raw.replace("\u00a0", "").replace(",", "."))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_rating(raw: str) -> Optional[float]:
    """Parse booking score (e.g. '8,5' or '9.2')."""
    match = re.search(r"(\d+[.,]\d+)", raw)
    if match:
        return float(match.group(1).replace(",", "."))
    # Single integer score
    match = re.search(r"(\d+)", raw)
    if match:
        val = float(match.group(1))
        return val if val <= 10 else val / 10
    return None


def _parse_review_count(raw: str) -> Optional[int]:
    match = re.search(r"(\d[\d\s]*)", raw.replace("\u00a0", " "))
    if match:
        return int(match.group(1).replace(" ", ""))
    return None


# ── Listing extraction ────────────────────────────────────────────────────────

async def _extract_listings_from_page(page: Page, city_name: str) -> list[dict]:
    """Parse all property cards from Booking search results page."""
    listings = []

    # Booking uses different selectors depending on version
    cards = await page.query_selector_all('[data-testid="property-card"]')

    if not cards:
        cards = await page.query_selector_all('div[class*="sr_item"], div[class*="bui-card"]')

    console.log(f"[dim]{len(cards)} cards détectées[/dim]")

    for card in cards:
        try:
            listing: dict = {}

            # Name
            name_el = await card.query_selector(
                '[data-testid="title"], div[class*="fcab3ed991"]'
            )
            listing["titre"] = (await name_el.inner_text()).strip() if name_el else None

            # Price per night (URL is built for a 1-night stay, so this IS per-night)
            price_el = await card.query_selector(
                '[data-testid="price-and-discounted-price"], span[class*="prco-valign"]'
            )
            if price_el:
                listing["prix_nuit"] = _parse_price(await price_el.inner_text())
            else:
                listing["prix_nuit"] = None

            # Rating score
            score_el = await card.query_selector(
                '[data-testid="review-score"] div[class*="ac4a7896c7"], '
                'div[class*="bui-review-score__badge"]'
            )
            if score_el:
                listing["note"] = _parse_rating(await score_el.inner_text())
            else:
                listing["note"] = None

            # Review count
            reviews_el = await card.query_selector(
                '[data-testid="review-score"] div[class*="d8eab2cf7f"], '
                'span[class*="review-score-widget__subtext"]'
            )
            if reviews_el:
                listing["nb_avis"] = _parse_review_count(await reviews_el.inner_text())
            else:
                listing["nb_avis"] = None

            # Address (Booking only exposes the city/commune here, not a street)
            addr_el = await card.query_selector(
                '[data-testid="address-link"], [data-testid="address"]'
            )
            raw_address = (await addr_el.inner_text()).strip() if addr_el else ""
            listing["adresse"] = raw_address

            # URL
            link_el = await card.query_selector("a[href*='/hotel/']")
            if link_el:
                href = await link_el.get_attribute("href")
                listing["url"] = href if href else None
                # Extract hotel ID from URL
                hotel_id = re.search(r"hotel/[a-z]{2}/([^.]+)\.", href or "")
                listing["id_externe"] = hotel_id.group(1) if hotel_id else None
            else:
                listing["url"] = None
                listing["id_externe"] = None

            # Coordinates will be filled by geocoding
            listing["lat"] = None
            listing["lng"] = None
            listing["source"] = "booking"
            listing["ville"] = city_name
            listing["jours_indispo"] = None  # Booking doesn't expose calendars easily

            if listing["titre"]:
                listings.append(listing)

        except Exception as e:
            console.log(f"[red]Erreur parsing card Booking : {e}[/red]")
            continue

    return listings


# ── Main scraper ──────────────────────────────────────────────────────────────

async def scrape_booking(
    browser,
    city_name: str,
    bbox: dict,
    dry_run: bool = False,
    max_pages: int = 5,
) -> list[dict]:
    """
    Scrape Booking.com apartments for a city/bbox.
    Returns list of enriched listing dicts.
    """
    console.rule(f"[bold blue]Booking.com — {city_name}[/bold blue]")
    all_listings: list[dict] = []

    page = await get_new_page(browser)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console,
    ) as progress:
        search_task = progress.add_task("Scraping pages Booking...", total=max_pages)

        for page_idx in range(max_pages):
            offset = page_idx * 25  # Booking shows 25 results per page
            url = _build_search_url(city_name, bbox, offset=offset)
            console.log(f"[cyan]Page {page_idx + 1}/{max_pages} → {url}[/cyan]")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await random_delay(2, 5)
                await human_scroll(page, steps=6)
                await random_delay(1, 3)

                # Dismiss cookie banner if present
                try:
                    cookie_btn = await page.query_selector(
                        '#onetrust-accept-btn-handler, button[id*="accept"], '
                        'button[class*="cookie-accept"]'
                    )
                    if cookie_btn:
                        await cookie_btn.click()
                        await asyncio.sleep(1)
                except Exception:
                    pass

                # Save raw HTML
                raw_html = await page.content()
                cache_key = _cache_key(url)
                if not dry_run:
                    _save_raw(
                        f"search_{cache_key}",
                        {"url": url, "city": city_name, "page": page_idx, "html": raw_html},
                    )

                listings = await _extract_listings_from_page(page, city_name)
                console.log(f"[green]{len(listings)} listings Booking trouvés[/green]")

                if not listings:
                    console.log("[yellow]Aucun listing, fin de la pagination[/yellow]")
                    break

                # Geocode addresses
                geo_task = progress.add_task(
                    f"  Géocodage (page {page_idx + 1})...",
                    total=len(listings),
                )

                for listing in listings:
                    if listing.get("adresse") and not dry_run:
                        try:
                            lat, lng = await _geocode_address(listing["adresse"], city_name)
                            listing["lat"] = lat
                            listing["lng"] = lng
                            await asyncio.sleep(1.1)  # Nominatim rate limit: 1 req/sec
                        except Exception as e:
                            console.log(f"[yellow]Géocodage échoué : {e}[/yellow]")

                    all_listings.append(listing)
                    progress.advance(geo_task)

                progress.advance(search_task)

                # Check for next page button
                next_btn = await page.query_selector(
                    '[aria-label="Page suivante"], button[class*="pagination-next"]'
                )
                if not next_btn:
                    console.log("[yellow]Pas de page suivante Booking, fin[/yellow]")
                    break

                await random_delay(3, 6)

            except Exception as e:
                console.log(f"[red]Erreur page Booking {page_idx}: {e}[/red]")
                progress.advance(search_task)
                continue

    await page.close()
    console.log(
        f"[bold green]Total Booking : {len(all_listings)} listings pour {city_name}[/bold green]"
    )
    return all_listings
