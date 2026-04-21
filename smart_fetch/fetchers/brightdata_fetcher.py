"""Tier 2: BrightData MCP — Zillow structured data + general scraping."""
import json, time, requests
from smart_fetch.config import BRIGHTDATA_MCP_BASE, BRIGHTDATA_MCP_PARAMS, RATE_LIMITS

_session_id = None

def _init_session():
    global _session_id
    if _session_id:
        return _session_id
    resp = requests.post(
        f"{BRIGHTDATA_MCP_BASE}{BRIGHTDATA_MCP_PARAMS}",
        headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": "initialize",
              "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                         "clientInfo": {"name": "smart-fetch", "version": "1.0"}}},
        timeout=15)
    _session_id = resp.headers.get("mcp-session-id")
    return _session_id

def _call_tool(tool_name, arguments, timeout=60):
    session_id = _init_session()
    if not session_id:
        return None
    resp = requests.post(
        f"{BRIGHTDATA_MCP_BASE}{BRIGHTDATA_MCP_PARAMS}",
        headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                 "mcp-session-id": session_id},
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/call",
              "params": {"name": tool_name, "arguments": arguments}},
        timeout=timeout)
    for line in resp.text.split('\n'):
        if line.startswith('data: '):
            data = json.loads(line[6:])
            if 'result' in data:
                content = data['result'].get('content', [])
                for c in content:
                    try:
                        return json.loads(c.get('text', ''))
                    except:
                        return c.get('text', '')
    return None

def fetch_zillow_listing(url, **kwargs):
    """Fetch structured Zillow listing data via BrightData MCP tool."""
    try:
        result = _call_tool("web_data_zillow_properties_listing", {"url": url})
        time.sleep(RATE_LIMITS["brightdata_zillow"]["delay_s"])

        if isinstance(result, list) and result:
            item = result[0]
            if "error" in item:
                return {"success": False, "error": item["error"]}
            return {"success": True, "properties": [item], "json": item, "source": "brightdata_zillow"}
        return {"success": False, "error": "no data returned"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def fetch_scrape(url, **kwargs):
    """Fetch any URL via BrightData's scrape_as_markdown tool."""
    try:
        result = _call_tool("scrape_as_markdown", {"url": url})
        time.sleep(RATE_LIMITS["brightdata_zillow"]["delay_s"])

        if result and isinstance(result, str) and len(result) > 100:
            if "not available for immediate" in result.lower():
                return {"success": False, "error": "KYC required for this site"}
            return {"success": True, "html": result, "source": "brightdata_scrape"}
        return {"success": False, "error": "empty or invalid response"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def fetch(url, **kwargs):
    """Auto-detect: if Zillow PDP, use listing tool. Otherwise use scrape."""
    if "zillow.com/homedetails/" in url:
        return fetch_zillow_listing(url, **kwargs)
    return fetch_scrape(url, **kwargs)
