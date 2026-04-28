"""
Airbnb scraper — entire homes/apartments only.

Two-phase approach:
  Phase 1 — Collect all listing IDs from search pages (fast, DOM only).
             Supports bbox tiling for large areas.
  Phase 2 — Visit each listing page, extract data from DOM + intercept
             calendar API for 365-day availability.
"""

import asyncio
import json
import math
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Optional

from playwright.async_api import Page
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn

from browser.brave import get_new_page, random_delay, adaptive_delay, human_scroll, detect_block

console = Console()
RAW_CACHE_DIR = Path("raw_cache/airbnb")
RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_DIR = Path("checkpoints")
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


# ── Checkpoint ────────────────────────────────────────────────────────────────

def checkpoint_path(city_key: str) -> Path:
    return CHECKPOINT_DIR / f"{city_key}.json"


def load_checkpoint(city_key: str) -> dict:
    path = checkpoint_path(city_key)
    if path.exists():
        try:
            data = json.loads(path.read_text())
            console.log(
                f"[cyan]Checkpoint chargé : "
                f"{len(data.get('collected_ids', []))} IDs collectés, "
                f"{len(data.get('processed_ids', []))} traités[/cyan]"
            )
            return data
        except Exception:
            pass
    return {"city_key": city_key, "collected_ids": [], "processed_ids": []}


def save_checkpoint(city_key: str, data: dict) -> None:
    checkpoint_path(city_key).write_text(json.dumps(data, ensure_ascii=False, indent=2))


def clear_checkpoint(city_key: str) -> None:
    path = checkpoint_path(city_key)
    if path.exists():
        path.unlink()
        console.log(f"[dim]Checkpoint supprimé : {path}[/dim]")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_search_url(city_name: str, bbox: dict, page_offset: int = 0) -> str:
    params = (
        f"?refinement_paths[]=entire_home"
        f"&ne_lat={bbox['north']}&ne_lng={bbox['east']}"
        f"&sw_lat={bbox['south']}&sw_lng={bbox['west']}"
        f"&search_type=filter_change"
        f"&query={city_name.replace(' ', '%20')}"
        f"&items_offset={page_offset}"
        f"&room_types[]=Entire%20home%2Fapt"
    )
    return f"https://www.airbnb.fr/s/homes{params}"


def _subdivide_bbox(bbox: dict, tile_lat: float, tile_lng: float) -> list[dict]:
    """Split a large bbox into smaller tiles for better search coverage."""
    tiles = []
    lat = bbox["south"]
    while lat < bbox["north"] - 1e-6:
        lng = bbox["west"]
        while lng < bbox["east"] - 1e-6:
            tiles.append({
                "south": round(lat, 6),
                "north": round(min(lat + tile_lat, bbox["north"]), 6),
                "west":  round(lng, 6),
                "east":  round(min(lng + tile_lng, bbox["east"]), 6),
            })
            lng += tile_lng
        lat += tile_lat
    return tiles


def _compute_zone_geo(
    lat: Optional[float],
    lng: Optional[float],
    center_lat: float,
    center_lng: float,
) -> Optional[str]:
    if lat is None or lng is None:
        return None
    THRESHOLD = 0.006
    dlat = lat - center_lat
    dlng = lng - center_lng
    near_ns = abs(dlat) < THRESHOLD
    near_ew = abs(dlng) < THRESHOLD
    if near_ns and near_ew:
        return "Centre"
    if near_ns:
        return "Est" if dlng > 0 else "Ouest"
    if near_ew:
        return "Nord" if dlat > 0 else "Sud"
    return f"{'Nord' if dlat > 0 else 'Sud'}-{'Est' if dlng > 0 else 'Ouest'}"


def _parse_price(raw: str) -> Optional[float]:
    cleaned = re.sub(r"[^\d,.]", "", raw.replace(",", "."))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _count_unavailable_days(availability: dict[str, bool], days: int = 90) -> int:
    today = date.today()
    return sum(
        1 for i in range(days)
        if (d := (today + timedelta(days=i)).isoformat()) in availability
        and not availability[d]
    )


def _monthly_occupancy(availability: dict[str, bool]) -> dict[str, float]:
    from collections import defaultdict
    month_total: dict[str, int] = defaultdict(int)
    month_booked: dict[str, int] = defaultdict(int)
    for date_str, is_available in availability.items():
        month_key = date_str[:7]
        month_total[month_key] += 1
        if not is_available:
            month_booked[month_key] += 1
    return {
        k: round(month_booked[k] / month_total[k] * 100, 1)
        for k in sorted(month_total)
        if month_total[k] > 0
    }


# ── Phase 1 : collect listing IDs from search pages ───────────────────────────

async def _extract_ids_from_page(page: Page) -> list[str]:
    """Return unique listing IDs found on the current search results page."""
    ids = await page.evaluate(
        "() => {"
        "  const seen = new Set();"
        "  for (const a of document.querySelectorAll('a[href*=\"/rooms/\"]')) {"
        "    const m = a.getAttribute('href').match(/\\/rooms\\/(\\d+)/);"
        "    if (m) seen.add(m[1]);"
        "  }"
        "  return [...seen];"
        "}"
    )
    return ids or []


async def _collect_ids_for_bbox(
    browser,
    city_name: str,
    bbox: dict,
    max_pages: int,
    global_seen: set[str],
    tile_idx: int,
    tile_total: int,
    dry_run: bool = False,
) -> list[str]:
    """
    Phase 1 for one bbox tile.
    Paginates search pages and returns new listing IDs (not in global_seen).
    """
    new_ids: list[str] = []
    page = await get_new_page(browser)

    try:
        for page_idx in range(max_pages if not dry_run else 1):
            offset = page_idx * 20
            url = _build_search_url(city_name, bbox, offset)
            label = f"tuile {tile_idx+1}/{tile_total}" if tile_total > 1 else "page"
            console.log(f"[cyan]{label} — page {page_idx + 1} → offset {offset}[/cyan]")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await random_delay(1, 2)
                await human_scroll(page, steps=4)
                await asyncio.sleep(0.7)

                ids = await _extract_ids_from_page(page)
                fresh = [id_ for id_ in ids if id_ not in global_seen]
                global_seen.update(fresh)
                new_ids.extend(fresh)

                dupes = len(ids) - len(fresh)
                console.log(
                    f"[green]{len(fresh)} nouveaux IDs[/green]"
                    + (f" [dim]({dupes} doublons)[/dim]" if dupes else "")
                )

                if not fresh:
                    console.log("[yellow]Aucun nouvel ID sur cette page, fin pagination[/yellow]")
                    break

                next_btn = await page.query_selector(
                    '[aria-label="Suivant"], [data-testid="pagination-next-btn"]'
                )
                if not next_btn:
                    console.log("[yellow]Pas de page suivante[/yellow]")
                    break

                await random_delay(1.3, 2.7)

            except Exception as e:
                console.log(f"[red]Erreur page {page_idx}: {e}[/red]")
                break
    finally:
        await page.close()

    return new_ids


# ── Phase 2 : scrape individual listing pages ─────────────────────────────────

# Amenities keywords checked against full page text (FR + EN)
_AMENITY_KEYWORDS = [
    "Cuisine équipée", "Cuisine", "Four", "Micro-ondes", "Réfrigérateur",
    "Lave-vaisselle", "Cafetière", "Grille-pain",
    "Lave-linge", "Sèche-linge",
    "Climatisation", "Chauffage",
    "Wifi", "TV", "Télévision",
    "Parking gratuit", "Parking payant", "Garage",
    "Piscine", "Jacuzzi", "Bain à remous",
    "Terrasse", "Jardin", "Balcon", "Barbecue",
    "Cheminée", "Sauna",
    "Animaux acceptés",
    "Espace de travail dédié", "Bureau",
    "Lit bébé", "Chaise haute",
    "Alarme incendie", "Extincteur", "Trousse de premiers secours",
]


def _compute_weekday_weekend_prices(day_prices: dict[str, float]) -> tuple[Optional[float], Optional[float]]:
    """Average price Monday–Thursday vs Friday–Sunday."""
    from datetime import date as date_t
    wd, we = [], []
    for ds, p in day_prices.items():
        if not p or p <= 0:
            continue
        try:
            dow = date_t.fromisoformat(ds).weekday()  # 0=Mon … 6=Sun
        except ValueError:
            continue
        (we if dow >= 4 else wd).append(p)  # Fri/Sat/Sun = weekend
    prix_semaine = round(sum(wd) / len(wd), 2) if wd else None
    prix_weekend = round(sum(we) / len(we), 2) if we else None
    return prix_semaine, prix_weekend


async def _scrape_listing_page(page: Page, listing_url: str) -> dict:
    """
    Load a listing page and extract all available data:
    - Identité      : titre, type_bien, superhost, instant_book
    - Capacité      : nb_voyageurs, nb_chambres, nb_lits, nb_sdb
    - Localisation  : lat, lng, code_postal, neighbourhood
    - Tarifs        : prix_nuit, prix_semaine, prix_weekend, cleaning_fee
    - Disponibilité : availability (365 j), jours_indispo, taux mensuel
    - Qualité       : note, nb_avis, photos_count
    - Logement      : amenities, minimum_nights
    """
    if "check_in" not in listing_url:
        checkin = date.today() + timedelta(days=30)
        checkout = checkin + timedelta(days=1)
        sep = "&" if "?" in listing_url else "?"
        listing_url += f"{sep}check_in={checkin.isoformat()}&check_out={checkout.isoformat()}"

    result: dict = {
        # Dispo
        "availability": {},
        "day_prices": {},          # date → prix par nuit (pour weekday/weekend)
        # Tarifs
        "prix_nuit": None,
        "prix_semaine": None,
        "prix_weekend": None,
        "cleaning_fee": None,
        # Identité
        "titre": None,
        "type_bien": None,
        "superhost": None,
        "instant_book": None,
        # Capacité
        "nb_voyageurs": None,
        "nb_chambres": None,
        "nb_lits": None,
        "nb_sdb": None,
        # Localisation
        "lat": None,
        "lng": None,
        "code_postal": None,
        "neighbourhood": None,
        # Qualité
        "note": None,
        "nb_avis": None,
        "photos_count": None,
        # Logement
        "amenities": None,
        "minimum_nights": None,
    }

    api_calendar: list[dict] = []
    api_sections: list[dict] = []

    async def handle_response(response):
        try:
            if "PdpAvailabilityCalendar" in response.url:
                api_calendar.append(await response.json())
            elif "StaysPdpSections" in response.url:
                api_sections.append(await response.json())
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        await page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(2.7)

        # Give extra time if API responses haven't arrived yet
        if not api_sections:
            await asyncio.sleep(2)

        if not api_sections and not api_calendar:
            console.log("[yellow]Aucune réponse API interceptée — prix via DOM uniquement[/yellow]")

        # ── One DOM call: extract everything at once ───────────────────────────
        dom = await page.evaluate("""
            () => {
                const body = document.body.innerText || '';
                const bl   = body.toLowerCase();

                // Title — multiple selectors, fallback to og:title meta
                let titre = null;
                const titleSels = [
                    '[data-section-id="TITLE_DEFAULT"] h1',
                    '[data-testid="listing-page-title"]',
                    'h1[elementtiming]',
                    'h1',
                ];
                for (const sel of titleSels) {
                    const el = document.querySelector(sel);
                    if (el && el.innerText && el.innerText.trim().length > 2) {
                        titre = el.innerText.trim();
                        break;
                    }
                }
                if (!titre) {
                    const og = document.querySelector('meta[property="og:title"]');
                    if (og) titre = og.getAttribute('content');
                }

                // Photos count
                const photoSels = [
                    '[data-testid="photo-viewer-section"] img',
                    '[data-section-id="HERO_DEFAULT"] img',
                    'picture img',
                ];
                let photos_count = 0;
                for (const sel of photoSels) {
                    const imgs = document.querySelectorAll(sel);
                    if (imgs.length > photos_count) photos_count = imgs.length;
                }

                // Superhost
                const superhost = bl.includes('superhôte') || bl.includes('superhost');

                // Instant book
                const instant_book = bl.includes('réservation instantanée')
                                  || bl.includes('instant book');

                // Minimum nights
                let minimum_nights = null;
                const mnMatch = body.match(/(\\d+)\\s*nuits?\\s*minimum/i)
                             || body.match(/séjour\\s*minimum\\s*:\\s*(\\d+)/i)
                             || body.match(/durée\\s*minimum.*?(\\d+)\\s*nuit/i);
                if (mnMatch) minimum_nights = parseInt(mnMatch[1]);

                // nb_avis — DOM: rating header "4,95 · 127 avis"
                let nb_avis_dom = null;
                const ratingSelectors = [
                    '[data-section-id="OVERVIEW_DEFAULT"] button[aria-label*="avis"]',
                    'a[href*="reviews"]',
                    'button[aria-label*="avis"]',
                    'span[aria-label*="avis"]',
                ];
                for (const sel of ratingSelectors) {
                    for (const el of document.querySelectorAll(sel)) {
                        const t = el.innerText || el.getAttribute('aria-label') || '';
                        const m = t.match(/(\\d[\\d\\s]*?)\\s*avis/i);
                        if (m) {
                            const n = parseInt(m[1].replace(/\\s/g, ''));
                            if (n > 0) { nb_avis_dom = n; break; }
                        }
                    }
                    if (nb_avis_dom) break;
                }
                // Fallback: scan body text for "X avis" / "X commentaires"
                if (!nb_avis_dom) {
                    const bm = body.match(/(\\d+)\\s*(?:commentaires?|avis)/i);
                    if (bm) nb_avis_dom = parseInt(bm[1]);
                }

                // Amenities
                const amenitySels = [
                    '[data-section-id="AMENITIES"] li',
                    '[data-testid="amenity-row"]',
                    'section[aria-label*="équipement"] li',
                ];
                let amenityTexts = [];
                for (const sel of amenitySels) {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 2) {
                        amenityTexts = [...els]
                            .map(e => e.innerText.trim().split('\\n')[0])
                            .filter(t => t.length > 1 && t.length < 80);
                        break;
                    }
                }

                return { titre, photos_count, superhost, instant_book,
                         minimum_nights, amenityTexts, nb_avis_dom };
            }
        """)

        if dom.get("titre"):
            result["titre"] = dom["titre"][:120]
        result["superhost"]      = dom.get("superhost")
        result["instant_book"]   = dom.get("instant_book")
        result["minimum_nights"] = dom.get("minimum_nights")
        result["photos_count"]   = dom.get("photos_count") or None
        result["_nb_avis_dom"]   = dom.get("nb_avis_dom")

        # ── Full body text (used for amenities fallback + capacity) ───────────
        body_text: str = await page.evaluate("() => document.body.innerText")

        # ── Amenities: DOM result or keyword scan ─────────────────────────────
        dom_amenities = dom.get("amenityTexts") or []
        if len(dom_amenities) >= 3:
            result["amenities"] = ", ".join(dom_amenities[:40])
        else:
            found = [kw for kw in _AMENITY_KEYWORDS if kw.lower() in body_text.lower()]
            if found:
                result["amenities"] = ", ".join(found)

        # ── Capacity + type from body text ────────────────────────────────────
        type_m = re.search(
            r"(?:Logement entier|Hébergement)\s*[:·]\s*(\w[\w\s\-']+)",
            body_text, re.IGNORECASE,
        )
        if type_m:
            result["type_bien"] = type_m.group(1).strip()[:50]

        def _ei(pattern: str, text: str) -> Optional[int]:
            m = re.search(pattern, text, re.IGNORECASE)
            return int(m.group(1)) if m else None

        result["nb_voyageurs"] = _ei(r"(\d+)\s*voyageur", body_text)
        result["nb_chambres"]  = _ei(r"(\d+)\s*chambre", body_text)
        result["nb_lits"]      = _ei(r"(\d+)\s*lit\b", body_text)
        result["nb_sdb"]       = _ei(r"(\d+)\s*salle", body_text)

        if result["minimum_nights"] is None:
            result["minimum_nights"] = _ei(r"(\d+)\s*nuits?\s*minimum", body_text)

        # ── JSON-LD: coords, address, rating ──────────────────────────────────
        json_ld_text = await page.evaluate(
            "() => { const s = document.querySelector('script[type=\"application/ld+json\"]'); "
            "return s ? s.textContent : ''; }"
        )
        if json_ld_text:
            try:
                ld = json.loads(json_ld_text)
                geo = ld.get("geo", {})
                if geo.get("latitude"):
                    result["lat"] = float(geo["latitude"])
                    result["lng"] = float(geo["longitude"])
                addr = ld.get("address", {})
                if isinstance(addr, dict):
                    result["code_postal"]   = addr.get("postalCode")
                    result["neighbourhood"] = addr.get("addressLocality")
                rating = ld.get("aggregateRating", {})
                if rating.get("ratingValue"):
                    result["note"]    = float(str(rating["ratingValue"]).replace(",", "."))
                    result["nb_avis"] = int(rating.get("reviewCount", 0)) or None
            except Exception:
                pass

        # Fallback nb_avis: DOM extraction if JSON-LD returned nothing
        if not result["nb_avis"] and result.get("_nb_avis_dom"):
            result["nb_avis"] = result["_nb_avis_dom"]

        # ── __NEXT_DATA__: coords (Airbnb masque geo dans JSON-LD) ────────────
        if result["lat"] is None:
            next_raw = await page.evaluate(
                "() => { const el = document.getElementById('__NEXT_DATA__'); "
                "return el ? el.textContent : ''; }"
            )
            if next_raw:
                # Look for lat/lng pairs close together in the JSON blob
                for lat_pat, lng_pat in [
                    (r'"lat"\s*:\s*(-?\d{2,3}\.\d+)', r'"lng"\s*:\s*(-?\d{1,3}\.\d+)'),
                    (r'"latitude"\s*:\s*(-?\d{2,3}\.\d+)', r'"longitude"\s*:\s*(-?\d{1,3}\.\d+)'),
                ]:
                    lats = re.findall(lat_pat, next_raw)
                    lngs = re.findall(lng_pat, next_raw)
                    if lats and lngs:
                        try:
                            result["lat"] = float(lats[0])
                            result["lng"] = float(lngs[0])
                            break
                        except ValueError:
                            pass
                # postalCode from __NEXT_DATA__ if still missing
                if result["code_postal"] is None:
                    m_cp = re.search(r'"postalCode"\s*:\s*"(\d{5})"', next_raw)
                    if m_cp:
                        result["code_postal"] = m_cp.group(1)
                # neighbourhood from __NEXT_DATA__ if still missing
                if result["neighbourhood"] is None:
                    m_loc = re.search(r'"addressLocality"\s*:\s*"([^"]{2,60})"', next_raw)
                    if m_loc:
                        result["neighbourhood"] = m_loc.group(1)

        # ── Code postal fallback: body text ───────────────────────────────────
        if result["code_postal"] is None and body_text:
            # Match "49100 Angers" or "CP : 49100" style patterns
            m_cp = re.search(r'\b(\d{5})\b', body_text)
            if m_cp:
                result["code_postal"] = m_cp.group(1)

        # ── Calendar API: availability + per-day prices ───────────────────────
        for data in api_calendar:
            months = (
                data.get("data", {})
                    .get("merlin", {})
                    .get("pdpAvailabilityCalendar", {})
                    .get("calendarMonths", [])
            )
            for month in months:
                for day in month.get("days", []):
                    ds = day.get("calendarDate")
                    if not ds:
                        continue
                    result["availability"][ds] = bool(day.get("available", True))
                    raw_p = (day.get("price") or {}).get("localPriceFormatted", "")
                    if raw_p:
                        p = _parse_price(raw_p)
                        if p and p > 0:
                            result["day_prices"][ds] = p
                            if result["prix_nuit"] is None:
                                result["prix_nuit"] = p

        if result["availability"]:
            console.log(f"[green]Calendrier : {len(result['availability'])} jours, "
                        f"{len(result['day_prices'])} prix[/green]")
            ps, pw = _compute_weekday_weekend_prices(result["day_prices"])
            result["prix_semaine"] = ps
            result["prix_weekend"] = pw

        # ── Sections API: price, coords, cleaning fee ─────────────────────────
        for data in api_sections:
            raw_json = json.dumps(data, ensure_ascii=False)

            # Per-night price — patterns ordered by reliability
            if result["prix_nuit"] is None:
                price_patterns = [
                    # "1 nuit x 85 €" or "2 nuits x 120 €"
                    r'"description"\s*:\s*"\d+\s*nuit[s]?\s*x\s*([\d\s,.]+\s*€)"',
                    # structuredDisplayPrice primaryLine
                    r'"primaryLine"\s*:\s*\{[^}]*"price"\s*:\s*"([\d\s,.]+\s*€)"',
                    # qualifiedDisplayPrice / displayPrice / originalPrice
                    r'"qualifiedDisplayPrice"\s*:\s*"([\d\s,.]+\s*€)',
                    r'"displayPrice"\s*:\s*"([\d\s,.]+\s*€)"',
                    r'"originalPrice"\s*:\s*"([\d\s,.]+\s*€)"',
                    # BOOK_IT sidebar: price shown as "XX €"
                    r'"price"\s*:\s*"([\d\s,.]+\s*€)"(?:[^}]{0,50}"qualifier")',
                    r'"formattedAmount"\s*:\s*"([\d\s,.]+\s*€)"',
                    r'"localizedPrice"\s*:\s*"([\d\s,.]+\s*€)"',
                    # numeric fallbacks
                    r'"nightlyPrice"\s*:\s*(\d+(?:\.\d+)?)',
                    r'"rate"\s*:\s*(\d+(?:\.\d+)?)',
                ]
                for pattern in price_patterns:
                    m = re.search(pattern, raw_json, re.IGNORECASE)
                    if m:
                        p = _parse_price(m.group(1))
                        if p and 5 < p < 10000:
                            result["prix_nuit"] = p
                            break

            # priceString last resort
            if result["prix_nuit"] is None:
                for ps in re.findall(r'"priceString"\s*:\s*"([^"]+)"', raw_json):
                    p = _parse_price(ps)
                    if p and 5 < p < 10000:
                        result["prix_nuit"] = p
                        break

            # Cleaning fee
            if result["cleaning_fee"] is None:
                for pattern in [
                    r'"Frais de ménage"[^}]{0,200}"price"\s*:\s*"([^"]+)"',
                    r'"cleaning[_\s]fee[^"]*"[^}]{0,200}"localizedPrice"\s*:\s*"([^"]+)"',
                    r'"type"\s*:\s*"CLEANING_FEE"[^}]{0,300}"price"\s*:\s*"([^"]+)"',
                    r'"type"\s*:\s*"CLEANING_FEE"[^}]{0,300}"localizedPrice"\s*:\s*"([^"]+)"',
                    r'"type"\s*:\s*"CLEANING_FEE"[^}]{0,300}"formattedAmount"\s*:\s*"([^"]+)"',
                ]:
                    m = re.search(pattern, raw_json, re.IGNORECASE | re.DOTALL)
                    if m:
                        p = _parse_price(m.group(1))
                        if p and p >= 0:
                            result["cleaning_fee"] = p
                            break

            # Coordinates from sections
            if result["lat"] is None:
                sections = (
                    data.get("data", {})
                        .get("presentation", {})
                        .get("stayProductDetailPage", {})
                        .get("sections", {})
                        .get("sections", [])
                )
                for section in sections:
                    meta = section.get("section", {}) or {}
                    lat = meta.get("lat") or meta.get("latitude")
                    lng = meta.get("lng") or meta.get("longitude")
                    if lat and lng:
                        result["lat"] = float(lat)
                        result["lng"] = float(lng)
                        break

        # ── Price fallback: body text ──────────────────────────────────────────
        if result["prix_nuit"] is None and body_text:
            body_price_patterns = [
                # "120 € par nuit" or "120 €/nuit"
                r"([\d][\d\s]*(?:[,\.]\d+)?)\s*€\s*par\s*nuit",
                r"([\d][\d\s]*(?:[,\.]\d+)?)\s*€\s*/\s*nuit",
                # "par nuit\n120 €" (Airbnb React layout)
                r"par nuit\s*\n\s*([\d][\d\s]*(?:[,\.]\d+)?)\s*€",
                r"nuit\s*\n\s*([\d][\d\s]*(?:[,\.]\d+)?)\s*€",
                # "120 €\nnuit"
                r"([\d][\d\s]*(?:[,\.]\d+)?)\s*€\s*\n\s*nuit",
            ]
            for bp in body_price_patterns:
                m = re.search(bp, body_text, re.IGNORECASE)
                if m:
                    p = _parse_price(m.group(1))
                    if p and 5 < p < 10000:
                        result["prix_nuit"] = p
                        break

        # ── Price fallback: DOM elements ───────────────────────────────────────
        if result["prix_nuit"] is None:
            price_text = await page.evaluate(
                "() => Array.from(document.querySelectorAll("
                "  '[data-testid*=\"price\"], [data-testid=\"book-it-default\"], "
                "  span[class*=\"price\"], ._tyxjp1'"
                ")).map(e => e.innerText).join(' ')"
            )
            if price_text:
                m = re.search(r"([\d][\d\s]*(?:[,\.]\d+)?)\s*€\s*(?:par\s*nuit|/\s*nuit)", price_text, re.I)
                if m:
                    p = _parse_price(m.group(1))
                    if p and 5 < p < 10000:
                        result["prix_nuit"] = p

    except Exception as e:
        console.log(f"[red]Impossible de scraper {listing_url}: {e}[/red]")
    finally:
        page.remove_listener("response", handle_response)

    return result


# ── Main scraper ──────────────────────────────────────────────────────────────

async def scrape_airbnb(
    browser,
    city_key: str,
    city_name: str,
    bbox: dict,
    city_center: dict,
    dry_run: bool = False,
    max_pages: int = 15,
    skip_ids: set[str] | None = None,
    resume: bool = False,
    tile_lat: float = 0.0,
    tile_lng: float = 0.0,
    save_batch_fn=None,
    batch_size: int = 20,
) -> list[dict]:
    """
    Two-phase Airbnb scraper.

    Phase 1: collect all listing IDs from search pages (with optional tiling).
    Phase 2: scrape each listing page for full data + calendar availability.

    Args:
        tile_lat/tile_lng: if > 0, subdivide bbox into tiles of that degree size.
        skip_ids: listing IDs (internal format "airbnb_XXX") to skip detail scraping.
        resume: load checkpoint and continue from where we left off.
    """
    console.rule(f"[bold blue]Airbnb — {city_name}[/bold blue]")
    skip_ids = skip_ids or set()

    # ── Checkpoint ────────────────────────────────────────────────────────────
    checkpoint: dict = {}
    if resume:
        checkpoint = load_checkpoint(city_key)
    else:
        checkpoint = {"city_key": city_key, "collected_ids": [], "processed_ids": []}

    processed_ids: set[str] = set(checkpoint.get("processed_ids", []))
    all_skip_ids = skip_ids | processed_ids

    # ── Tiles ─────────────────────────────────────────────────────────────────
    if tile_lat > 0 and tile_lng > 0:
        tiles = _subdivide_bbox(bbox, tile_lat, tile_lng)
        console.log(f"[cyan]Bbox découpée en {len(tiles)} tuiles ({tile_lat}°×{tile_lng}°)[/cyan]")
    else:
        tiles = [bbox]

    center_lat = city_center["lat"]
    center_lng = city_center["lng"]

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 1 — collect all listing IDs
    # ──────────────────────────────────────────────────────────────────────────
    collected_ids: list[str] = list(checkpoint.get("collected_ids", []))
    global_seen: set[str] = set(collected_ids)

    if not collected_ids or not resume:
        console.rule("[bold]Phase 1 — Collecte des URLs[/bold]", style="cyan")
        for i, tile_bbox in enumerate(tiles):
            new_ids = await _collect_ids_for_bbox(
                browser, city_name, tile_bbox,
                max_pages=max_pages,
                global_seen=global_seen,
                tile_idx=i,
                tile_total=len(tiles),
                dry_run=dry_run,
            )
            collected_ids.extend(new_ids)
            if not dry_run:
                checkpoint["collected_ids"] = collected_ids
                save_checkpoint(city_key, checkpoint)

        console.log(f"[bold green]Phase 1 terminée : {len(collected_ids)} listings uniques[/bold green]")
    else:
        console.log(f"[cyan]Phase 1 skippée (resume) : {len(collected_ids)} IDs depuis checkpoint[/cyan]")

    if dry_run:
        console.print(f"\n[yellow][DRY RUN] {len(collected_ids)} listings détectés (pas de scraping détaillé).[/yellow]")
        for lid in collected_ids[:5]:
            console.print(f"  - https://www.airbnb.fr/rooms/{lid}")
        if len(collected_ids) > 5:
            console.print(f"  ... et {len(collected_ids) - 5} autres.")
        return []

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 2 — scrape each listing page
    # ──────────────────────────────────────────────────────────────────────────
    console.rule("[bold]Phase 2 — Scraping des annonces[/bold]", style="green")
    all_listings: list[dict] = []
    save_buffer: list[dict] = []
    request_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Annonces...", total=len(collected_ids))

        for listing_id in collected_ids:
            internal_id = f"airbnb_{listing_id}"
            listing_url = f"https://www.airbnb.fr/rooms/{listing_id}"

            listing: dict = {
                "source": "airbnb",
                "ville": city_name,
                "id_externe": listing_id,
                "url": listing_url,
                "titre": None,
                "prix_nuit": None,
                "prix_semaine": None,
                "prix_weekend": None,
                "cleaning_fee": None,
                "note": None,
                "nb_avis": None,
                "lat": None,
                "lng": None,
                "zone_geo": None,
                "code_postal": None,
                "neighbourhood": None,
                "type_bien": None,
                "nb_voyageurs": None,
                "nb_chambres": None,
                "nb_lits": None,
                "nb_sdb": None,
                "superhost": None,
                "instant_book": None,
                "minimum_nights": None,
                "amenities": None,
                "photos_count": None,
                "jours_indispo": None,
                "availability": {},
            }

            should_skip = internal_id in all_skip_ids

            if should_skip:
                console.log(f"[dim]Skip (récemment scanné) : {listing_url}[/dim]")
            else:
                cal_page = None
                try:
                    cal_page = await get_new_page(browser)
                    enriched = await _scrape_listing_page(cal_page, listing_url)

                    # Merge all enriched fields
                    for key in (
                        "titre", "type_bien", "superhost", "instant_book",
                        "nb_voyageurs", "nb_chambres", "nb_lits", "nb_sdb",
                        "lat", "lng", "code_postal", "neighbourhood",
                        "prix_nuit", "prix_semaine", "prix_weekend", "cleaning_fee",
                        "note", "nb_avis", "photos_count",
                        "amenities", "minimum_nights",
                    ):
                        if enriched.get(key) is not None:
                            listing[key] = enriched[key]

                    availability = enriched["availability"]
                    listing["jours_indispo"]     = _count_unavailable_days(availability, 90)
                    listing["jours_indispo_365"] = _count_unavailable_days(availability, 365)
                    listing["occupancy_monthly"] = _monthly_occupancy(availability)
                    listing["availability"]       = availability
                    listing["last_scanned_at"]   = datetime.now(UTC).isoformat()

                    # Checkpoint
                    processed_ids.add(internal_id)
                    checkpoint["processed_ids"] = list(processed_ids)
                    save_checkpoint(city_key, checkpoint)

                    request_count += 1

                except Exception as e:
                    console.log(f"[red]Listing échoué {listing_url}: {e}[/red]")
                finally:
                    if cal_page:
                        try:
                            await cal_page.close()
                        except Exception:
                            pass

            # Zone géo (computed from coords regardless of skip)
            listing["zone_geo"] = _compute_zone_geo(
                listing.get("lat"), listing.get("lng"),
                center_lat, center_lng,
            )

            all_listings.append(listing)
            save_buffer.append(listing)
            progress.advance(task)

            if save_batch_fn and len(save_buffer) >= batch_size:
                save_batch_fn(save_buffer.copy())
                save_buffer.clear()
                console.log(f"[dim]Sauvegarde intermédiaire : {len(all_listings)} listings enregistrés[/dim]")

            if not should_skip:
                await adaptive_delay(request_count)

    if save_batch_fn and save_buffer:
        save_batch_fn(save_buffer)

    clear_checkpoint(city_key)
    console.log(f"[bold green]Total Airbnb : {len(all_listings)} listings pour {city_name}[/bold green]")
    return all_listings
