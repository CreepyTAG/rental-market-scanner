"""
Stealth browser via rebrowser-playwright (patched Playwright that bypasses
Cloudflare/PerimeterX bot detection on Runtime.Enable).

Drop-in replacement for browser/brave.py:
- Same public API: get_browser(), get_new_page(), random_delay, adaptive_delay,
  human_scroll, detect_block.
- Uses bundled Chromium (no external Brave install required → VPS-friendly).
- Optional residential proxy via PROXY_URL env var (e.g. http://user:pass@gate.smartproxy.com:7000).
- Persistent user-data dir to retain cookies/storage across runs.
"""

import asyncio
import os
import random
from pathlib import Path
from urllib.parse import urlparse

try:
    from rebrowser_playwright.async_api import async_playwright, Browser, BrowserContext, Page
except ImportError:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from rich.console import Console

console = Console()

USER_DATA_DIR = Path.home() / ".rental-scanner-chromium"
USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

STEALTH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process",
    "--disable-site-isolation-trials",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-dev-shm-usage",
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
]

_current_ua_index = 0


def _get_proxy() -> dict | None:
    """Build Playwright proxy dict from PROXY_URL env var, or None if unset."""
    proxy_url = os.environ.get("PROXY_URL", "").strip()
    if not proxy_url:
        return None

    parsed = urlparse(proxy_url)
    proxy = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    console.log(f"[cyan]Proxy : {parsed.hostname}:{parsed.port}[/cyan]")
    return proxy


async def get_browser(
    profile_directory: str = "Default",  # ignored, kept for API compat
    auto_launch: bool = True,            # ignored, kept for API compat
    headless: bool = False,
) -> tuple:
    """
    Launch stealth Chromium with persistent context.
    Returns (playwright, context, None) — third value kept for API compat.
    Note: returned 'context' is a BrowserContext (not Browser) since we use
    launch_persistent_context. get_new_page handles both transparently.
    """
    playwright = await async_playwright().start()

    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(USER_DATA_DIR),
        headless=headless,
        args=list(STEALTH_ARGS),
        proxy=_get_proxy(),
        viewport={"width": 1920, "height": 1080},
        locale="fr-FR",
        timezone_id="Europe/Paris",
        user_agent=USER_AGENTS[0],
    )

    if headless:
        console.log("[cyan]Mode headless activé[/cyan]")

    return playwright, context, None


async def get_new_page(browser_or_context) -> Page:
    """Open a new page. Accepts either a Browser or BrowserContext."""
    global _current_ua_index

    if hasattr(browser_or_context, "contexts"):
        contexts = browser_or_context.contexts
        context = contexts[0] if contexts else await browser_or_context.new_context()
    else:
        context = browser_or_context

    page = await context.new_page()

    ua = USER_AGENTS[_current_ua_index % len(USER_AGENTS)]
    _current_ua_index += 1
    await page.set_extra_http_headers({"User-Agent": ua})

    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        window.chrome = window.chrome || {runtime: {}};
    """)

    return page


async def detect_block(page: Page) -> bool:
    try:
        page_text = await page.evaluate("() => document.body.innerText.substring(0, 1000)")
        signals = [
            "vérification", "verify you are a human", "captcha",
            "veuillez patienter", "please wait", "access denied",
            "automated access", "unusual traffic", "press and hold",
        ]
        text_lower = page_text.lower()
        for s in signals:
            if s in text_lower:
                console.log(f"[bold red]BLOCAGE DETECTE : '{s}'[/bold red]")
                return True
    except Exception:
        pass
    return False


async def random_delay(min_s: float = 2.0, max_s: float = 5.0) -> None:
    delay = random.uniform(min_s, max_s)
    console.log(f"[dim]Pause de {delay:.1f}s...[/dim]")
    await asyncio.sleep(delay)


async def adaptive_delay(consecutive_requests: int) -> None:
    if consecutive_requests < 5:
        await random_delay(2, 4)
    elif consecutive_requests < 15:
        await random_delay(4, 8)
    elif consecutive_requests < 30:
        console.log("[yellow]Ralentissement préventif (>15 requêtes)[/yellow]")
        await random_delay(8, 15)
    else:
        console.log("[yellow]Pause longue (>30 requêtes)[/yellow]")
        await random_delay(15, 30)


async def human_scroll(page: Page, steps: int = 5) -> None:
    for _ in range(steps):
        await page.evaluate("window.scrollBy(0, window.innerHeight * 0.6)")
        await asyncio.sleep(random.uniform(0.3, 0.8))
