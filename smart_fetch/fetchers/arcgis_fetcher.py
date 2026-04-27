"""ArcGIS REST query fetcher — direct queries against ArcGIS MapServer/FeatureServer.

Most US counties expose parcel and tax data via ArcGIS REST endpoints at
`/arcgis/rest/services/.../MapServer/<layer>/query`. Returns features as GeoJSON
or Esri JSON.

Used as `method: arcgis_query` in the county portal spider config. The portal
config provides the full query URL or a base + layer + where clause.
"""

import requests
from typing import Optional


def fetch(
    url: Optional[str] = None,
    *,
    base: Optional[str] = None,
    layer: Optional[int] = None,
    where: str = "1=1",
    out_fields: str = "*",
    return_geometry: bool = False,
    f: str = "json",  # "json" (Esri) or "geojson"
    result_record_count: int = 2000,
    max_records: int = 50_000,
    timeout: int = 30,
    **_,
) -> dict:
    """Run an ArcGIS REST query and auto-paginate until done or max_records.

    Two call styles:
      fetch(url="https://arcgis.example.gov/arcgis/rest/services/X/MapServer/0/query",
            where="STATUS='LIS PENDENS'")
      fetch(base="https://arcgis.example.gov/arcgis/rest/services/X/MapServer",
            layer=0, where="...")

    Returns:
      {"success": bool, "features": [...], "row_count": int, "source": "arcgis_rest",
       "url": str, "error": str?}
    """
    if url is None:
        if not base or layer is None:
            return {"success": False, "error": "must provide url OR (base + layer)"}
        url = f"{base.rstrip('/')}/{layer}/query"
    elif not url.endswith("/query"):
        url = url.rstrip("/") + "/query"

    all_features: list = []
    offset = 0

    while True:
        params = {
            "where":              where,
            "outFields":          out_fields,
            "returnGeometry":     "true" if return_geometry else "false",
            "f":                  f,
            "resultOffset":       offset,
            "resultRecordCount":  min(result_record_count, max_records - len(all_features)),
        }

        try:
            resp = requests.get(url, params=params, timeout=timeout)
        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": f"arcgis timed out after {timeout}s",
                "url": url,
                "features": all_features,
            }
        except Exception as exc:
            return {"success": False, "error": f"arcgis error: {exc}", "url": url}

        if resp.status_code != 200:
            return {
                "success": False,
                "error": f"arcgis HTTP {resp.status_code}: {resp.text[:200]}",
                "url": url,
                "features": all_features,
            }

        try:
            data = resp.json()
        except Exception as exc:
            return {"success": False, "error": f"arcgis JSON parse: {exc}", "url": url}

        # Esri error envelope
        if isinstance(data, dict) and data.get("error"):
            return {
                "success": False,
                "error": f"arcgis error: {data['error']}",
                "url": url,
                "features": all_features,
            }

        features = data.get("features") if f == "json" else data.get("features", [])
        if not features:
            break

        all_features.extend(features)

        # exceededTransferLimit signals "more available, paginate"
        if not data.get("exceededTransferLimit") and len(features) < result_record_count:
            break
        if len(all_features) >= max_records:
            break

        offset = len(all_features)

    return {
        "success": True,
        "features": all_features[:max_records],
        "row_count": min(len(all_features), max_records),
        "source": "arcgis_rest",
        "url": url,
    }


def normalize_features(features: list, *, deal_type: str = "") -> list:
    """Flatten ArcGIS features into the spider's standard property dict shape.

    Heuristic field matching against `attributes` — looks for address, price,
    parcel/case ID, owner. Geometry x/y stored as `_lat`/`_lon`.
    """
    out = []
    for feat in features:
        if not isinstance(feat, dict):
            continue
        attrs = feat.get("attributes") or feat.get("properties") or {}
        if not isinstance(attrs, dict):
            continue

        rec: dict = {"_raw": {k: str(v) for k, v in attrs.items() if v is not None}}
        if deal_type:
            rec["deal_type"] = deal_type

        for k, v in attrs.items():
            if v is None or v == "":
                continue
            kl = k.lower()
            if "address" in kl or "situs" in kl or "location" in kl:
                if "address" not in rec:
                    rec["address"] = str(v)
            elif kl in ("city", "municipality"):
                rec.setdefault("city", str(v))
            elif kl in ("state", "state_code"):
                rec.setdefault("state", str(v))
            elif kl in ("zip", "zipcode", "zip_code", "postal", "postal_code"):
                rec.setdefault("zip_code", str(v))
            elif "owner" in kl:
                rec.setdefault("owner", str(v))
            elif any(p in kl for p in ("price", "value", "amount", "sale", "judgment")):
                if "price" not in rec:
                    try:
                        rec["price"] = str(int(float(str(v).replace("$", "").replace(",", ""))))
                    except (ValueError, TypeError):
                        pass
            elif "parcel" in kl or "folio" in kl or "apn" in kl or "pin" in kl:
                rec.setdefault("parcel_id", str(v))
            elif "case" in kl or "docket" in kl:
                rec.setdefault("case_number", str(v))

        geom = feat.get("geometry") or {}
        if isinstance(geom, dict):
            if "x" in geom: rec["_lon"] = geom["x"]
            if "y" in geom: rec["_lat"] = geom["y"]
            # GeoJSON style
            coords = geom.get("coordinates")
            if isinstance(coords, list) and len(coords) >= 2:
                rec["_lon"], rec["_lat"] = coords[0], coords[1]

        if rec.get("address") or rec.get("parcel_id") or rec.get("case_number"):
            out.append(rec)

    return out
