import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.abritel.fr/search?destination=Puerto+Iguazú", wait_until="domcontentloaded")
        await asyncio.sleep(5)
        # Try to find common listing elements
        elements = await page.evaluate("""
            () => {
                const nodes = document.querySelectorAll('*');
                let classCounts = {};
                for (let n of nodes) {
                    if (n.className && typeof n.className === 'string') {
                        let cls = n.className.split(' ');
                        for (let c of cls) {
                            classCounts[c] = (classCounts[c] || 0) + 1;
                        }
                    }
                }
                
                // Return cards with links to property
                const links = Array.from(document.querySelectorAll('a[href*="/location/"], a[href*="/unit/"]'));
                const listItems = Array.from(document.querySelectorAll('li'));
                
                return {
                    links: links.map(a => a.className + ' | ' + a.href).slice(0, 5),
                    dataStids: Array.from(document.querySelectorAll('[data-stid]')).map(n => n.getAttribute('data-stid')).filter((v, i, a) => a.indexOf(v) === i)
                }
            }
        """)
        print(elements)
        await browser.close()

asyncio.run(main())
