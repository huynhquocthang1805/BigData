"""Tải dữ liệu chất lượng nước liên tục (continuous, ~15 phút) từ USGS Water Data OGC API.

Kết quả là các file CSV dạng "long" (mỗi dòng 1 giá trị đo), bố cục kiểu Hive partition để
Spark đọc trực tiếp và tự suy ra cột station_id / parameter_code:

    data/raw/usgs/station_id=USGS-05586300/parameter_code=00300/2024.csv
    (cột: time,value,approval_status,qualifier)

Ví dụ:
    python -m ingestion.download_usgs                          # 10 trạm trong config/stations.json, 2023-2025
    python -m ingestion.download_usgs --sites USGS-05586300 --start 2024-01-01 --end 2024-02-01
    python -m ingestion.download_usgs --refresh-metadata       # cập nhật tên/tọa độ trạm vào config

API: https://api.waterdata.usgs.gov/ogcapi/v1  (không cần key; có key thì đặt biến USGS_API_KEY
để tăng hạn mức). Lỗi 429/5xx được retry với exponential backoff.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from wq import contracts

API_BASE = os.getenv("USGS_API_BASE", "https://api.waterdata.usgs.gov/ogcapi/v1")
PAGE_LIMIT = 50_000
OUT_COLUMNS = ["time", "value", "approval_status", "qualifier"]
DEFAULT_STATIONS_FILE = Path("config/stations.json")


def _request(url: str, retries: int = 6) -> bytes:
    headers = {"User-Agent": "wq-bigdata-student-project/1.0"}
    if os.getenv("USGS_API_KEY"):
        headers["X-Api-Key"] = os.environ["USGS_API_KEY"]
    delay = 2.0
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=180) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = float(e.headers.get("Retry-After") or delay)
                print(f"  HTTP {e.code}, retry sau {wait:.0f}s ...", file=sys.stderr)
                time.sleep(wait)
                delay *= 2
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < retries - 1:
                print(f"  {e}, retry sau {delay:.0f}s ...", file=sys.stderr)
                time.sleep(delay)
                delay *= 2
                continue
            raise
    raise RuntimeError("unreachable")


def fetch_continuous(site: str, param: str, start: datetime, end: datetime) -> list[dict]:
    """Lấy toàn bộ giá trị trong [start, end). Nếu chạm PAGE_LIMIT thì chia đôi khoảng thời gian."""
    query = {
        "f": "csv",
        "monitoring_location_id": site,
        "parameter_code": param,
        "time": f"{start:%Y-%m-%dT%H:%M:%SZ}/{end:%Y-%m-%dT%H:%M:%SZ}",
        "limit": PAGE_LIMIT,
        "skipGeometry": "true",
        "properties": ",".join(OUT_COLUMNS),
    }
    url = f"{API_BASE}/collections/continuous/items?{urllib.parse.urlencode(query)}"
    text = _request(url).decode("utf-8")
    rows = [
        {k: r.get(k, "") for k in OUT_COLUMNS}
        for r in csv.DictReader(io.StringIO(text))
    ]
    if len(rows) >= PAGE_LIMIT:  # có thể bị cắt => chia đôi rồi gộp
        mid = start + (end - start) / 2
        return fetch_continuous(site, param, start, mid) + fetch_continuous(site, param, mid, end)
    # API trả khoảng đóng [start, end] -> bỏ điểm đúng bằng end để không trùng giữa các năm
    end_s = f"{end:%Y-%m-%d %H:%M:%S}"
    return [r for r in rows if not r["time"].startswith(end_s)]


def year_chunks(start: datetime, end: datetime):
    y = start.year
    while True:
        a = max(start, datetime(y, 1, 1, tzinfo=timezone.utc))
        b = min(end, datetime(y + 1, 1, 1, tzinfo=timezone.utc))
        if a >= b:
            break
        yield y, a, b
        y += 1


def download_one(out_dir: Path, site: str, param: str, year: int, a: datetime, b: datetime, force: bool) -> tuple[str, int]:
    path = out_dir / f"station_id={site}" / f"parameter_code={param}" / f"{year}.csv"
    if path.exists() and not force:
        return f"skip {path}", -1
    rows = fetch_continuous(site, param, a, b)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)  # ghi nguyên tử: không để lại file dở dang nếu bị ngắt
    return f"ok   {path} ({len(rows)} rows)", len(rows)


def refresh_metadata(sites: list[str], stations_file: Path) -> None:
    """Lấy tên, bang, tọa độ... của trạm từ collection monitoring-locations và ghi vào config."""
    query = {
        "f": "json",
        "id": ",".join(sites),
        "limit": 500,
        "properties": "monitoring_location_name,state_name,site_type,drainage_area,hydrologic_unit_code",
    }
    data = json.loads(_request(f"{API_BASE}/collections/monitoring-locations/items?{urllib.parse.urlencode(query)}"))
    meta = {}
    for feat in data["features"]:
        p = feat["properties"]
        lon, lat = (feat.get("geometry") or {}).get("coordinates", [None, None])[:2]
        meta[feat["id"]] = {
            "station_id": feat["id"],
            "name": p.get("monitoring_location_name"),
            "state": p.get("state_name"),
            "site_type": p.get("site_type"),
            "latitude": lat,
            "longitude": lon,
            "drainage_area_sqmi": p.get("drainage_area"),
            "huc": p.get("hydrologic_unit_code"),
        }
    stations = [meta.get(s, {"station_id": s}) for s in sites]
    cfg = {"contract_version": contracts.CONTRACT_VERSION, "source": f"{API_BASE}/collections/continuous",
           "parameters": contracts.NAME_TO_USGS_CODE, "stations": stations}
    stations_file.parent.mkdir(parents=True, exist_ok=True)
    stations_file.write_text(json.dumps(cfg, indent=2, ensure_ascii=False) + "\n")
    print(f"Đã ghi metadata {len(stations)} trạm vào {stations_file}")


def load_station_ids(stations_file: Path) -> list[str]:
    cfg = json.loads(stations_file.read_text())
    return [s["station_id"] for s in cfg["stations"]]


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stations-file", type=Path, default=DEFAULT_STATIONS_FILE)
    ap.add_argument("--sites", nargs="*", help="Danh sách station_id (mặc định lấy từ --stations-file)")
    ap.add_argument("--params", nargs="*", default=list(contracts.USGS_CODE_TO_NAME), help="USGS parameter codes")
    ap.add_argument("--start", default=contracts.DATA_START)
    ap.add_argument("--end", default=contracts.DATA_END, help="exclusive")
    ap.add_argument("--out", type=Path, default=Path("data/raw/usgs"))
    ap.add_argument("--workers", type=int, default=4, help="Số request song song (đừng để quá cao để tránh 429)")
    ap.add_argument("--force", action="store_true", help="Tải lại cả file đã có")
    ap.add_argument("--refresh-metadata", action="store_true", help="Cập nhật tên/tọa độ trạm vào --stations-file")
    args = ap.parse_args(argv)

    sites = args.sites or load_station_ids(args.stations_file)
    if args.refresh_metadata:
        refresh_metadata(sites, args.stations_file)
        return

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    jobs = [(s, p, y, a, b) for s in sites for p in args.params for (y, a, b) in year_chunks(start, end)]
    print(f"{len(sites)} trạm x {len(args.params)} thông số x {len(list(year_chunks(start, end)))} năm = {len(jobs)} file")

    t0, total = time.time(), 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(download_one, args.out, *job, args.force): job for job in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                msg, n = fut.result()
                total += max(n, 0)
                print(f"[{i}/{len(jobs)}] {msg}")
            except Exception as e:  # không dừng cả quá trình vì 1 file lỗi
                print(f"[{i}/{len(jobs)}] FAIL {futs[fut][:3]}: {e}", file=sys.stderr)
    print(f"Xong: {total:,} giá trị mới trong {time.time() - t0:.0f}s -> {args.out}")


if __name__ == "__main__":
    main()
