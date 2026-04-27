"""
CLI entry point for manual scans.
Usage:
    python run_scan.py --city "Saint-Barthélemy-d'Anjou"
    python run_scan.py --city "Saint-Barthélemy-d'Anjou" --resume
    python run_scan.py --all
    python run_scan.py --all --dry-run
"""

import argparse
import asyncio
import sys
from pathlib import Path

import yaml
from rich.console import Console
from rich.panel import Panel

from browser.brave import get_browser
from scrapers.airbnb import scrape_airbnb
from scrapers.booking import scrape_booking
from scrapers.vrbo import scrape_vrbo
from db.storage import (
    get_connection,
    save_listings_batch,
    get_recently_scanned_ids,
    log_scan_start,
    log_scan_end,
)
from analysis.stats import print_city_summary
from export.excel import export_to_excel

console = Console()


def load_cities() -> dict:
    with open("config/cities.yaml") as f:
        return yaml.safe_load(f)["cities"]


def find_city(cities: dict, name: str) -> tuple[str, dict] | None:
    """Find city config by display name (case-insensitive)."""
    name_lower = name.lower().strip()
    for key, cfg in cities.items():
        if cfg["name"].lower() == name_lower:
            return key, cfg
    return None


async def scan_city(
    city_key: str,
    city_cfg: dict,
    browser,
    source: str | None = None,
    dry_run: bool = False,
    resume: bool = False,
    skip_days: int = 7,
    max_pages: int = 15,
) -> None:
    city_name = city_cfg["name"]
    bbox = city_cfg["bbox"]
    city_center = city_cfg["center"]

    console.print(
        Panel(
            f"[bold]Scan de [cyan]{city_name}[/cyan][/bold]"
            + (" [yellow](DRY RUN)[/yellow]" if dry_run else "")
            + (" [cyan](RESUME)[/cyan]" if resume else ""),
            expand=False,
        )
    )

    # Get recently scanned IDs to skip
    skip_ids: set[str] = set()
    if not dry_run and skip_days > 0:
        conn = get_connection()
        skip_ids = get_recently_scanned_ids(conn, city_name, max_age_days=skip_days)
        conn.close()
        if skip_ids:
            console.log(f"[dim]{len(skip_ids)} listings scannés récemment (< {skip_days}j), seront skippés[/dim]")

    async def run_source(src_name: str, scrape_coro) -> list:
        """Run one source end-to-end, persist results, log the scan."""
        log_conn = get_connection() if not dry_run else None
        scan_id = log_scan_start(log_conn, city_name, src_name, dry_run) if log_conn else None
        try:
            listings = await scrape_coro
            console.log(f"[green]{len(listings)} listings {src_name.capitalize()}[/green]")
            if log_conn and listings:
                counts = save_listings_batch(log_conn, listings)
                log_scan_end(
                    log_conn, scan_id,
                    status="success",
                    nb_listings=len(listings),
                    nb_inserted=counts["inserted"],
                    nb_updated=counts["updated"],
                    nb_errors=counts["errors"],
                )
            elif log_conn:
                log_scan_end(log_conn, scan_id, status="success", nb_listings=0)
            return listings
        except Exception as e:
            console.log(f"[red]{src_name.capitalize()} échoué : {e}[/red]")
            if log_conn:
                log_scan_end(log_conn, scan_id, status="error", message=str(e)[:500])
            return []
        finally:
            if log_conn:
                log_conn.close()

    all_listings: list = []

    if source in (None, "airbnb"):
        # Airbnb: progressive saves every 20 listings to avoid losing data on crash
        airbnb_conn = get_connection() if not dry_run else None
        airbnb_scan_id = log_scan_start(airbnb_conn, city_name, "airbnb", dry_run) if airbnb_conn else None
        airbnb_counts = {"inserted": 0, "updated": 0, "errors": 0}

        def _save_airbnb_batch(batch: list) -> None:
            if airbnb_conn and batch:
                c = save_listings_batch(airbnb_conn, batch)
                for k in airbnb_counts:
                    airbnb_counts[k] += c[k]

        try:
            airbnb_listings = await scrape_airbnb(
                browser,
                city_key=city_key,
                city_name=city_name,
                bbox=bbox,
                city_center=city_center,
                dry_run=dry_run,
                skip_ids=skip_ids,
                resume=resume,
                max_pages=max_pages,
                tile_lat=city_cfg.get("tile_lat", 0.0),
                tile_lng=city_cfg.get("tile_lng", 0.0),
                save_batch_fn=_save_airbnb_batch,
                batch_size=20,
            )
            console.log(f"[green]{len(airbnb_listings)} listings Airbnb[/green]")
            if airbnb_conn:
                log_scan_end(
                    airbnb_conn, airbnb_scan_id,
                    status="success",
                    nb_listings=len(airbnb_listings),
                    nb_inserted=airbnb_counts["inserted"],
                    nb_updated=airbnb_counts["updated"],
                    nb_errors=airbnb_counts["errors"],
                )
            all_listings += airbnb_listings
        except Exception as e:
            console.log(f"[red]Airbnb échoué : {e}[/red]")
            if airbnb_conn:
                log_scan_end(airbnb_conn, airbnb_scan_id, status="error", message=str(e)[:500])
        finally:
            if airbnb_conn:
                airbnb_conn.close()

    if source in (None, "booking"):
        all_listings += await run_source(
            "booking",
            scrape_booking(browser, city_name=city_name, bbox=bbox, dry_run=dry_run),
        )

    if source in (None, "vrbo"):
        all_listings += await run_source(
            "vrbo",
            scrape_vrbo(browser, city_name=city_name, bbox=bbox, dry_run=dry_run),
        )

    if dry_run:
        console.print(
            f"\n[yellow][DRY RUN] {len(all_listings)} listings auraient été sauvegardés.[/yellow]"
        )
        for lst in all_listings[:5]:
            console.print(f"  - [{lst['source']}] {lst.get('titre', '?')[:50]}  —  {lst.get('prix_nuit', '?')} €")
        if len(all_listings) > 5:
            console.print(f"  ... et {len(all_listings) - 5} autres.")
    else:
        conn = get_connection()
        # Summary (from DB, using bbox so listings tagged other villes are included)
        print_city_summary(conn, city_name, bbox=bbox)

        # Auto-export Excel (single rolling file per city)
        export_to_excel(conn, city_name, bbox=bbox)
        conn.close()


async def main(args: argparse.Namespace) -> None:
    # Configure DB path before any storage import resolves DB_PATH
    if args.db_path:
        import os
        os.environ["RENTAL_DB_PATH"] = args.db_path
        import db.storage as _st
        _st.DB_PATH = __import__("pathlib").Path(args.db_path)

    cities = load_cities()

    if args.dry_run:
        console.print("[bold yellow]Mode DRY RUN activé — aucune donnée ne sera sauvegardée.[/bold yellow]")

    # Determine which cities to scan
    if args.all:
        target_cities = list(cities.items())
    elif args.city:
        result = find_city(cities, args.city)
        if not result:
            available = ", ".join(c["name"] for c in cities.values())
            console.print(
                f"[red]Ville '{args.city}' inconnue. Villes disponibles : {available}[/red]"
            )
            sys.exit(1)
        target_cities = [result]
    else:
        console.print("[red]Spécifiez --city <nom> ou --all[/red]")
        sys.exit(1)

    # Launch browser
    playwright, browser, brave_process = await get_browser(headless=args.headless)

    try:
        for city_key, city_cfg in target_cities:
            await scan_city(
                city_key,
                city_cfg,
                browser,
                source=args.source,
                dry_run=args.dry_run,
                resume=args.resume,
                skip_days=args.skip_days,
                max_pages=args.max_pages,
            )
    finally:
        await browser.close()
        await playwright.stop()
        if brave_process:
            brave_process.terminate()

    console.print("\n[bold green]Scan terminé.[/bold green]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rental Market Scanner — Scan Airbnb",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python run_scan.py --city "Saint-Barthélemy-d'Anjou"
  python run_scan.py --city "Saint-Barthélemy-d'Anjou" --resume
  python run_scan.py --all
  python run_scan.py --all --dry-run
        """,
    )
    parser.add_argument(
        "--city",
        metavar="NOM",
        help="Nom de la ville à scraper (doit correspondre à config/cities.yaml)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Scanner toutes les villes configurées",
    )
    parser.add_argument(
        "--source",
        choices=["airbnb", "booking", "vrbo"],
        default=None,
        help="Limiter à une seule source (défaut : toutes)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simuler le scan sans sauvegarder en base",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reprendre un scan interrompu (charge le checkpoint)",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        default=True,
        help="Lancer Brave en mode visible (défaut : headless)",
    )
    parser.add_argument(
        "--skip-days",
        type=int,
        default=7,
        help="Nombre de jours avant re-scan d'un listing (défaut: 7)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=15,
        help="Nombre max de pages de résultats à scraper par ville (défaut: 15)",
    )
    parser.add_argument(
        "--db-path",
        metavar="PATH",
        default=None,
        help="Chemin vers la base DuckDB (défaut: rental_market.db ou $RENTAL_DB_PATH)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))
