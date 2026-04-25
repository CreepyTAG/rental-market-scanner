"""
APScheduler — weekly scan of all configured cities.
Run: python scheduler.py
"""

import asyncio
import logging
from datetime import datetime

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from rich.console import Console

from browser.brave import get_browser
from scrapers.airbnb import scrape_airbnb
from scrapers.booking import scrape_booking
from db.storage import get_connection, save_listings_batch

console = Console()
logging.basicConfig(level=logging.INFO)


def load_cities() -> dict:
    with open("config/cities.yaml") as f:
        return yaml.safe_load(f)["cities"]


async def scan_city(city_key: str, city_cfg: dict, browser, dry_run: bool = False) -> None:
    """Run a full scan for a single city (Airbnb + Booking)."""
    city_name = city_cfg["name"]
    bbox = city_cfg["bbox"]

    console.rule(f"[bold]Scan — {city_name}[/bold]")

    conn = get_connection()

    try:
        # Airbnb
        try:
            airbnb_listings = await scrape_airbnb(browser, city_name, bbox, dry_run=dry_run)
            if not dry_run:
                save_listings_batch(conn, airbnb_listings)
        except Exception as e:
            console.log(f"[red]Airbnb scan échoué pour {city_name}: {e}[/red]")

        # Booking
        try:
            booking_listings = await scrape_booking(browser, city_name, bbox, dry_run=dry_run)
            if not dry_run:
                save_listings_batch(conn, booking_listings)
        except Exception as e:
            console.log(f"[red]Booking scan échoué pour {city_name}: {e}[/red]")

    finally:
        conn.close()

    console.log(f"[green]Scan terminé pour {city_name}[/green]")


async def run_full_scan(dry_run: bool = False) -> None:
    """Scan all cities sequentially."""
    cities = load_cities()
    console.log(
        f"[bold cyan]Démarrage du scan complet — {datetime.now().strftime('%d/%m/%Y %H:%M')}[/bold cyan]"
    )

    playwright, browser, brave_process = await get_browser()

    try:
        for key, cfg in cities.items():
            await scan_city(key, cfg, browser, dry_run=dry_run)
    finally:
        await browser.close()
        await playwright.stop()
        if brave_process:
            brave_process.terminate()

    console.log("[bold green]Scan complet terminé.[/bold green]")


async def main() -> None:
    scheduler = AsyncIOScheduler()

    # Every Monday at 06:00
    scheduler.add_job(
        run_full_scan,
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0),
        id="weekly_scan",
        name="Scan hebdomadaire complet",
        replace_existing=True,
    )

    scheduler.start()
    console.log("[green]Scheduler démarré — scan hebdomadaire chaque lundi à 06:00[/green]")
    console.log("Appuyez sur Ctrl+C pour arrêter.")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        console.log("[yellow]Scheduler arrêté.[/yellow]")


if __name__ == "__main__":
    asyncio.run(main())
