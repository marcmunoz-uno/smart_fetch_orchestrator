"""Tier 4: Browserbase cloud browser — stealth remote Chromium with residential IPs."""
import time
import requests
from smart_fetch.config import BROWSERBASE_API_KEY, BROWSERBASE_PROJECT_ID, RATE_LIMITS

_BROWSERBASE_BASE = "https://api.browserbase.com"


def _create_session():
    """Create a Browserbase cloud browser session. Returns (session_id, connect_url) or (error_msg, None)."""
    try:
        resp = requests.post(
            f"{_BROWSERBASE_BASE}/v1/sessions",
            headers={"x-bb-api-key": BROWSERBASE_API_KEY, "Content-Type": "application/json"},
            json={"projectId": BROWSERBASE_PROJECT_ID, "browserSettings": {"stealth": True}},
            timeout=30,
        )
    except Exception as e:
        return str(e), None

    if resp.status_code != 201:
        return f"session create failed: {resp.status_code} {resp.text[:200]}", None

    data = resp.json()
    return data.get("id"), data.get("connectUrl")


def _stop_session(session_id):
    """Best-effort session cleanup."""
    if not session_id:
        return
    try:
        requests.post(
            f"{_BROWSERBASE_BASE}/v1/sessions/{session_id}/stop",
            headers={"x-bb-api-key": BROWSERBASE_API_KEY},
            timeout=10,
        )
    except Exception:
        pass


def fetch(url, wait_selector=None, timeout_ms=30000, **kwargs):
    """
    Fetch URL via Browserbase cloud browser.

    Connects Playwright to a remote Browserbase session (stealth mode, cloud IP),
    navigates to the URL, and returns the rendered HTML.

    Returns:
        {"success": bool, "html": str, "title": str, "status": int,
         "source": "browserbase", "captured_json": [...]}  # captured_json only when present
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "error": "playwright not installed"}

    session_id, connect_url = _create_session()
    if connect_url is None:
        # session_id holds the error message when connect_url is None
        return {"success": False, "error": session_id}

    try:
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(connect_url)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.pages[0] if context.pages else context.new_page()

            # Capture XHR / fetch JSON responses
            captured_json = []

            def on_response(resp):
                if "json" in (resp.headers.get("content-type") or ""):
                    try:
                        captured_json.append(resp.json())
                    except Exception:
                        pass

            page.on("response", on_response)

            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=10000)
                except Exception:
                    pass
            else:
                page.wait_for_timeout(3000)

            html = page.content()
            title = page.title()

            browser.close()

            result = {
                "success": True,
                "html": html,
                "title": title,
                "status": response.status if response else 0,
                "source": "browserbase",
            }
            if captured_json:
                result["captured_json"] = captured_json

            time.sleep(RATE_LIMITS.get("browserbase", {}).get("delay_s", 3.0))
            return result

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        _stop_session(session_id)


def fetch_with_extraction(url, extraction_prompt, **kwargs):
    """
    Navigate to URL and extract structured data using page.evaluate().

    Uses a DOM heuristic to pull rows/listings from the page, guided loosely
    by extraction_prompt (logged for future LLM-guided extraction).

    extraction_prompt examples:
        "Extract all property listings with address, price, beds, baths"
        "Find the search results table and extract each row"

    Returns:
        {"success": bool, "extracted": [...], "source": "browserbase_extract"}
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "error": "playwright not installed"}

    session_id, connect_url = _create_session()
    if connect_url is None:
        return {"success": False, "error": session_id}

    try:
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(connect_url)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.pages[0] if context.pages else context.new_page()

            page.goto(url, wait_until="domcontentloaded", timeout=kwargs.get("timeout_ms", 30000))

            wait_selector = kwargs.get("wait_selector")
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=10000)
                except Exception:
                    pass
            else:
                page.wait_for_timeout(3000)

            extracted = page.evaluate("""
                () => {
                    const rows = document.querySelectorAll(
                        'tr, .listing, .property, article, [data-testid]'
                    );
                    return Array.from(rows).map(el => ({
                        text: el.innerText.trim().slice(0, 500),
                        href: el.querySelector('a') ? el.querySelector('a').href : '',
                    })).filter(r => r.text.length > 20);
                }
            """)

            browser.close()

            time.sleep(RATE_LIMITS.get("browserbase", {}).get("delay_s", 3.0))
            return {
                "success": True,
                "extracted": extracted,
                "extraction_prompt": extraction_prompt,
                "source": "browserbase_extract",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        _stop_session(session_id)
