"""
Brave browser integration via Playwright launch (no CDP handshake required).
"""

import asyncio
import platform
import random
from pathlib import Path

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from rich.console import Console

console = Console()

BRAVE_PATHS = {
    "Darwin": [
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        str(Path.home() / "Desktop/Brave Browser.app/Contents/MacOS/Brave Browser"),
        str(Path.home() / "Applications/Brave Browser.app/Contents/MacOS/Brave Browser"),
    ],
    "Linux": [
        "/usr/bin/brave-browser",
        "/usr/bin/brave",
        "/snap/bin/brave",
        "/usr/local/bin/brave",
    ],
    "Windows": [
        r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
        r"C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe",
    ],
}


def detect_brave_path() -> str:
    """Auto-detect Brave executable path based on current OS."""
    os_name = platform.system()
    candidates = BRAVE_PATHS.get(os_name, [])

    for path in candidates:
        if Path(path).exists():
            console.log(f"[green]Brave trouvé :[/green] {path}")
            return path

    raise FileNotFoundError(
        f"Brave Browser introuvable sur {os_name}. "
        f"Chemins testés : {candidates}"
    )


async def get_browser(
    profile_directory: str = "Default",
    auto_launch: bool = True,
    headless: bool = False,
) -> tuple:
    """
    Launch browser via Playwright.
    Uses Brave if available, falls back to bundled Chromium (VPS-compatible).
    Returns (playwright, browser, None).
    """
    playwright = await async_playwright().start()

    launch_args = [
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--disable-extensions",
        "--disable-popup-blocking",
        "--no-sandbox",             # requis sur la plupart des VPS Linux
        "--disable-dev-shm-usage",  # évite l'épuisement de /dev/shm sur VPS
    ]

    try:
        brave_path = detect_brave_path()
        browser = await playwright.chromium.launch(
            executable_path=brave_path,
            headless=headless,
            args=launch_args,
        )
        console.log(f"[green]Brave lancé (headless={headless})[/green]")
    except FileNotFoundError:
        console.log("[yellow]Brave introuvable — Chromium bundled Playwright (mode VPS)[/yellow]")
        browser = await playwright.chromium.launch(
            headless=headless,
            args=launch_args,
        )
        console.log(f"[green]Chromium lancé (headless={headless})[/green]")

    return playwright, browser, None


# ── User-Agent rotation ───────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

_current_ua_index = 0


def _next_user_agent() -> str:
    global _current_ua_index
    ua = USER_AGENTS[_current_ua_index % len(USER_AGENTS)]
    _current_ua_index += 1
    return ua


async def get_new_page(browser: Browser) -> Page:
    """Open a new page in the existing browser context with rotated user-agent."""
    contexts = browser.contexts
    if contexts:
        context: BrowserContext = contexts[0]
    else:
        context = await browser.new_context()

    page = await context.new_page()
    ua = _next_user_agent()
    await page.set_extra_http_headers({"User-Agent": ua})
    return page


# ── Anti-ban helpers ──────────────────────────────────────────────────────────

async def detect_block(page: Page) -> bool:
    """
    Detect if Airbnb is showing a captcha, verification, or block page.
    Returns True if the page appears blocked.
    """
    try:
        page_text = await page.evaluate("() => document.body.innerText.substring(0, 1000)")
        block_signals = [
            "vérification", "verify you are a human", "captcha",
            "veuillez patienter", "please wait", "access denied",
            "automated access", "unusual traffic",
        ]
        text_lower = page_text.lower()
        for signal in block_signals:
            if signal in text_lower:
                console.log(f"[bold red]BLOCAGE DETECTE : '{signal}' trouvé sur la page[/bold red]")
                return True
    except Exception:
        pass
    return False


async def random_delay(min_s: float = 2.0, max_s: float = 5.0) -> None:
    """Wait a random delay to mimic human behavior."""
    delay = random.uniform(min_s, max_s)
    console.log(f"[dim]Pause de {delay:.1f}s...[/dim]")
    await asyncio.sleep(delay)


async def adaptive_delay(consecutive_requests: int) -> None:
    """
    Progressive delay between listing page scrapes.
    Stays reasonable even at high volume — Airbnb tolerates steady pacing.
    """
    if consecutive_requests < 10:
        await random_delay(2, 4)
    elif consecutive_requests < 30:
        await random_delay(3, 6)
    elif consecutive_requests < 60:
        console.log("[yellow]Ralentissement préventif (>30 requêtes)[/yellow]")
        await random_delay(5, 9)
    else:
        console.log("[yellow]Pause longue (>60 requêtes)[/yellow]")
        await random_delay(8, 14)


async def human_scroll(page: Page, steps: int = 5) -> None:
    """Scroll the page progressively to trigger lazy loading."""
    for _ in range(steps):
        await page.evaluate("window.scrollBy(0, window.innerHeight * 0.6)")
        await asyncio.sleep(random.uniform(0.3, 0.8))
