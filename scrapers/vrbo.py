"""
Vrbo / Abritel scraper — locations de vacances (pas d'hôtels par nature).
Abritel.fr est la marque française de Vrbo (groupe Expedia).
"""

import asyncio
import json
import re
import hashlib
from pathlib import Path
from typing import Optional

from playwright.async_api import Page
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from browser.brave import get_new_page, random_delay, human_scroll

console = Console()
RAW_CACHE_DIR = Path("raw_cache/vrbo")
RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_geocoder = Nominatim(user_agent="rental-market-scanner/1.0")


def _cache_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _save_raw(key: str, data: dict) -> None:
    path = RAW_CACHE_DIR / f"{key}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def _build_search_url(city_name: str, bbox: dict, page_num: int = 1) -> str:
    """Build Abritel.fr search URL constrained to the city bbox."""
    clean_city = city_name.replace(" ", "%20")
    return (
        f"https://www.abritel.fr/search"
        f"?q={clean_city}"
        f"&sort=RECOMMENDED"
        f"&page={page_num}"
        f"&neLat={bbox['north']}&neLng={bbox['east']}"
        f"&swLat={bbox['south']}&swLng={bbox['west']}"
    )


async def _geocode_address(address: str, city: str) -> tuple[Optional[float], Optional[float]]:
    full_address = f"{address}, {city}"
    try:
        loop = asyncio.get_event_loop()
        location = await loop.run_in_executor(
            None,
            lambda: _geocoder.geocode(full_address, timeout=5),
        )
        if location:
            return location.latitude, location.longitude
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        console.log(f"[yellow]Géocodage échoué pour '{full_address}': {e}[/yellow]")
    return None, None


def _parse_price(raw: str) -> Optional[float]:
    cleaned = re.sub(r"[^\d,.]", "", raw.replace("\u00a0", "").replace(",", "."))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_rating(raw: str) -> Optional[float]:
    match = re.search(r"(\d+[.,]\d+)", raw)
    if match:
        return float(match.group(1).replace(",", "."))
    match = re.search(r"(\d+)", raw)
    if match:
        val = float(match.group(1))
        return val if val <= 5 else val / 2
    return None


def _parse_review_count(raw: str) -> Optional[int]:
    match = re.search(r"(\d[\d\s]*)", raw.replace("\u00a0", " "))
    if match:
        return int(match.group(1).replace(" ", ""))
    return None


async def _extract_listings_from_page(page: Page, city_name: str) -> list[dict]:
    """Parse all property cards from Vrbo/Abritel search results page."""
    listings: list[dict] = []

    cards = await page.query_selector_all('[data-stid="property-listing"]')
    if not cards:
        cards = await page.query_selector_all('div[data-wdio="search-result"], li[data-stid*="lodging"]')

    console.log(f"[dim]{len(cards)} cards Vrbo détectées[/dim]")

    for card in cards:
        try:
            listing: dict = {}

            name_el = await card.query_selector('h3, [data-stid="content-hotel-title"]')
            listing["titre"] = (await name_el.inner_text()).strip() if name_el else None

            price_el = await card.query_selector(
                '[data-stid="price-summary-line"], [data-test-id="price"], div[class*="price"] span'
            )
            listing["prix_nuit"] = _parse_price(await price_el.inner_text()) if price_el else None

            score_el = await card.query_selector('[data-stid="content-hotel-reviews-rating"] span')
            listing["note"] = _parse_rating(await score_el.inner_text()) if score_el else None

            reviews_el = await card.query_selector('[data-stid*="review-count"], span[class*="review"]')
            listing["nb_avis"] = _parse_review_count(await reviews_el.inner_text()) if reviews_el else None

            addr_el = await card.query_selector('[data-stid="content-hotel-neighborhood"], span[class*="location"]')
            listing["adresse"] = (await addr_el.inner_text()).strip() if addr_el else ""

            link_el = await card.query_selector('a[href*="/unit/"], a[href*="/location/"], a[data-stid*="open-hotel"]')
            if link_el:
                href = await link_el.get_attribute("href")
                if href and href.startswith("/"):
                    href = f"https://www.abritel.fr{href}"
                listing["url"] = href
                unit_id = re.search(r"/(?:unit|location)/([^/?#]+)", href or "")
                listing["id_externe"] = unit_id.group(1) if unit_id else None
            else:
                listing["url"] = None
                listing["id_externe"] = None

            listing["lat"] = None
            listing["lng"] = None
            listing["source"] = "vrbo"
            listing["ville"] = city_name
            listing["jours_indispo"] = None

            if listing["titre"]:
                listings.append(listing)

        except Exception as e:
            console.log(f"[red]Erreur parsing card Vrbo : {e}[/red]")
            continue

    return listings


async def scrape_vrbo(
    browser,
    city_name: str,
    bbox: dict,
    dry_run: bool = False,
    max_pages: int = 5,
) -> list[dict]:
    """Scrape Vrbo/Abritel vacation rentals for a city/bbox."""
    console.rule(f"[bold blue]Vrbo / Abritel — {city_name}[/bold blue]")
    all_listings: list[dict] = []

    page = await get_new_page(browser)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console,
    ) as progress:
        search_task = progress.add_task("Scraping pages Vrbo...", total=max_pages)

        for page_idx in range(max_pages):
            url = _build_search_url(city_name, bbox, page_num=page_idx + 1)
            console.log(f"[cyan]Page {page_idx + 1}/{max_pages} → {url}[/cyan]")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await random_delay(2, 5)
                await human_scroll(page, steps=6)
                await random_delay(1, 3)

                try:
                    cookie_btn = await page.query_selector(
                        'button[data-testid="accept-all-cookies"], '
                        'button[id*="accept"], button[class*="cookie-accept"]'
                    )
                    if cookie_btn:
                        await cookie_btn.click()
                        await asyncio.sleep(1)
                except Exception:
                    pass

                raw_html = await page.content()
                cache_key = _cache_key(url)
                if not dry_run:
                    _save_raw(
                        f"search_{cache_key}",
                        {"url": url, "city": city_name, "page": page_idx, "html": raw_html},
                    )

                listings = await _extract_listings_from_page(page, city_name)
                console.log(f"[green]{len(listings)} listings Vrbo trouvés[/green]")

                if not listings:
                    console.log("[yellow]Aucun listing, fin de la pagination[/yellow]")
                    break

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
                            await asyncio.sleep(1.1)
                        except Exception as e:
                            console.log(f"[yellow]Géocodage échoué : {e}[/yellow]")
                    all_listings.append(listing)
                    progress.advance(geo_task)

                progress.advance(search_task)

                next_btn = await page.query_selector(
                    'a[data-stid="next-button"], button[aria-label*="suivant"], a[aria-label*="Next"]'
                )
                if not next_btn:
                    console.log("[yellow]Pas de page suivante Vrbo, fin[/yellow]")
                    break

                await random_delay(3, 6)

            except Exception as e:
                console.log(f"[red]Erreur page Vrbo {page_idx}: {e}[/red]")
                progress.advance(search_task)
                continue

    await page.close()
    console.log(
        f"[bold green]Total Vrbo/Abritel : {len(all_listings)} listings pour {city_name}[/bold green]"
    )
    return all_listings
