"""Tier 3: Playwright headless browser — handles JS rendering and light anti-bot."""
import time
from smart_fetch.config import RATE_LIMITS, BRIGHTDATA_PROXY_HOST, BRIGHTDATA_PROXY_PORT, BRIGHTDATA_PROXY_USER, BRIGHTDATA_PROXY_PASS

def fetch(url, wait_selector=None, timeout_ms=30000, use_proxy=False, **kwargs):
    """Fetch URL with headless Chromium. use_proxy=True routes through BrightData residential."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "error": "playwright not installed"}

    try:
        with sync_playwright() as p:
            launch_args = ["--disable-blink-features=AutomationControlled"]

            browser = p.chromium.launch(headless=True, args=launch_args)

            ctx_kwargs = {
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "viewport": {"width": 1440, "height": 900},
            }
            if use_proxy:
                ctx_kwargs["proxy"] = {
                    "server": f"http://{BRIGHTDATA_PROXY_HOST}:{BRIGHTDATA_PROXY_PORT}",
                    "username": BRIGHTDATA_PROXY_USER,
                    "password": BRIGHTDATA_PROXY_PASS,
                }

            context = browser.new_context(**ctx_kwargs)
            page = context.new_page()

            # Capture API responses
            captured_json = []
            def on_response(resp):
                ct = resp.headers.get("content-type", "")
                if "json" in ct:
                    try:
                        captured_json.append(resp.json())
                    except:
                        pass
            page.on("response", on_response)

            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=10000)
                except:
                    pass
            else:
                page.wait_for_timeout(3000)

            html = page.content()
            title = page.title()

            browser.close()

            status = response.status if response else 0
            title_lc = title.lower()
            blocked = (
                status >= 400
                or "access denied" in title_lc
                or "captcha" in title_lc
                or "access to this page has been denied" in html
                or "px-captcha-error-message" in html
            )
            if blocked:
                return {
                    "success": False,
                    "error": f"blocked (status={status}, title={title[:80]!r})",
                    "status": status,
                    "html": html,
                    "title": title,
                    "source": "playwright",
                }

            result = {"success": True, "html": html, "title": title, "status": status, "source": "playwright"}
            if captured_json:
                result["captured_json"] = captured_json

            time.sleep(RATE_LIMITS["playwright"]["delay_s"])
            return result
    except Exception as e:
        return {"success": False, "error": str(e)}
