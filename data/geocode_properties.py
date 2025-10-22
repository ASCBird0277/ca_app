
#!/usr/bin/env python3
"""
Geocode missing Latitude/Longitude in an Excel sheet using OpenStreetMap Nominatim.

Usage:
  python geocode_properties.py --in Properties.xlsx --sheet "Sheet1" --email adrian@ca-mgmt.com

Notes:
- Please provide a real contact email via --email (Nominatim asks for this in the User-Agent).
- Be kind to the service: the script sleeps ~1s between requests.
- It preserves any existing Latitude/Longitude and only fills blanks.
- Adds columns: GeoSource, GeocodeStatus, GeocodeTimestamp
- Writes output to "<input_basename>_geocoded.xlsx"
"""

import argparse
import time
import sys
from datetime import datetime
import requests
import pandas as pd

DEFAULT_STREET_COL = "Street Address"
DEFAULT_CITY_COL = "City"
DEFAULT_STATE_COL = "State"
DEFAULT_ZIP_COL = "ZIP"
DEFAULT_LAT_COL = "Latitude"
DEFAULT_LON_COL = "Longitude"

def make_address(row, street_col, city_col, state_col, zip_col):
    parts = []
    for col in [street_col, city_col, state_col, zip_col]:
        val = str(row.get(col, "")).strip()
        if val and val != "nan":
            parts.append(val)
    return ", ".join(parts)

def geocode_nominatim(query, email, timeout=20):
    """
    Return (lat, lon, status) where status is "ok" or "not_found" or "error:<msg>"
    """
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": query,
                "format": "json",
                "addressdetails": 0,
                "limit": 1,
            },
            headers={
                "User-Agent": f"Chamberlin-Geocoder/1.0 ({email})"
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            return (None, None, f"error:http_{resp.status_code}")
        data = resp.json()
        if not data:
            return (None, None, "not_found")
        top = data[0]
        lat = float(top.get("lat"))
        lon = float(top.get("lon"))
        return (lat, lon, "ok")
    except requests.Timeout:
        return (None, None, "error:timeout")
    except Exception as e:
        return (None, None, f"error:{type(e).__name__}")

def main():
    ap = argparse.ArgumentParser(description="Geocode missing lat/lon in an Excel sheet using Nominatim")
    ap.add_argument("--in", dest="infile", required=True, help="Path to Excel workbook (e.g., Properties.xlsx)")
    ap.add_argument("--sheet", dest="sheet", default="Sheet1", help="Worksheet name (default: Sheet1)")
    ap.add_argument("--email", dest="email", required=True, help="Contact email for Nominatim User-Agent")
    ap.add_argument("--street-col", dest="street_col", default=DEFAULT_STREET_COL, help=f'Street column name (default: "{DEFAULT_STREET_COL}")')
    ap.add_argument("--city-col", dest="city_col", default=DEFAULT_CITY_COL, help=f'City column name (default: "{DEFAULT_CITY_COL}")')
    ap.add_argument("--state-col", dest="state_col", default=DEFAULT_STATE_COL, help=f'State column name (default: "{DEFAULT_STATE_COL}")')
    ap.add_argument("--zip-col", dest="zip_col", default=DEFAULT_ZIP_COL, help=f'ZIP column name (default: "{DEFAULT_ZIP_COL}")')
    ap.add_argument("--lat-col", dest="lat_col", default=DEFAULT_LAT_COL, help=f'Latitude column name (default: "{DEFAULT_LAT_COL}")')
    ap.add_argument("--lon-col", dest="lon_col", default=DEFAULT_LON_COL, help=f'Longitude column name (default: "{DEFAULT_LON_COL}")')
    ap.add_argument("--sleep", dest="sleep", type=float, default=1.1, help="Seconds to sleep between requests (default: 1.1)")
    ap.add_argument("--retry", dest="retry", type=int, default=2, help="Number of retry attempts per address (default: 2)")
    args = ap.parse_args()

    # Load workbook
    try:
        df = pd.read_excel(args.infile, sheet_name=args.sheet)
    except Exception as e:
        print(f"ERROR: failed to read {args.infile} sheet={args.sheet}: {e}", file=sys.stderr)
        sys.exit(2)

    # Ensure output columns exist
    for col in [args.lat_col, args.lon_col]:
        if col not in df.columns:
            df[col] = None
    if "GeoSource" not in df.columns:
        df["GeoSource"] = ""
    if "GeocodeStatus" not in df.columns:
        df["GeocodeStatus"] = ""
    if "GeocodeTimestamp" not in df.columns:
        df["GeocodeTimestamp"] = ""

    # Simple cache to avoid duplicate lookups
    cache = {}

    total = len(df)
    now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    looked_up = 0
    filled = 0

    for idx, row in df.iterrows():
        lat = row.get(args.lat_col)
        lon = row.get(args.lon_col)

        # Skip if already filled
        if pd.notna(lat) and pd.notna(lon):
            continue

        # Build address
        query = make_address(row, args.street_col, args.city_col, args.state_col, args.zip_col)
        if not query:
            df.at[idx, "GeocodeStatus"] = "skipped:no_address"
            continue

        # Cache
        if query in cache:
            result = cache[query]
        else:
            status = None
            glat = glon = None
            attempt = 0
            while attempt <= args.retry:
                attempt += 1
                glat, glon, status = geocode_nominatim(query, args.email)
                if status == "ok":
                    break
                # backoff a bit between retries
                time.sleep(args.sleep + 0.5)
            result = (glat, glon, status)
            cache[query] = result
            looked_up += 1

            # Be friendly to Nominatim
            time.sleep(args.sleep)

        glat, glon, status = result
        df.at[idx, "GeocodeStatus"] = status
        df.at[idx, "GeocodeTimestamp"] = now_str

        if status == "ok":
            df.at[idx, args.lat_col] = glat
            df.at[idx, args.lon_col] = glon
            df.at[idx, "GeoSource"] = "OSM Nominatim"
            filled += 1

        # Optional: fallbacks (city+state only) if strict address failed
        # You can uncomment this block to try a looser query if the first failed.
        # if status != "ok":
        #     loose_query = ", ".join([str(row.get(args.city_col, "")).strip(),
        #                              str(row.get(args.state_col, "")).strip(),
        #                              str(row.get(args.zip_col, "")).strip()])
        #     if loose_query.replace(",", "").strip():
        #         glat, glon, status2 = geocode_nominatim(loose_query, args.email)
        #         df.at[idx, "GeocodeStatus"] = f"{status};loose:{status2}"
        #         if status2 == "ok":
        #             df.at[idx, args.lat_col] = glat
        #             df.at[idx, args.lon_col] = glon
        #             df.at[idx, "GeoSource"] = "OSM Nominatim(loose)"
        #             filled += 1
        #         time.sleep(args.sleep)

    # Save output
    base = args.infile.rsplit(".", 1)[0]
    out_path = f"{base}_geocoded.xlsx"
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=args.sheet, index=False)
    except Exception as e:
        print(f"ERROR: failed to write output: {e}", file=sys.stderr)
        sys.exit(3)

    print(f"Done. Looked up {looked_up} unique addresses, filled {filled} rows.")
    print(f"Output: {out_path}")

if __name__ == "__main__":
    main()
