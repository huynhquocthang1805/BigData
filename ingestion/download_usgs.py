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
import gzip
import http.client
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from wq import contracts
from wq.config import get_settings

API_BASE = os.getenv("USGS_API_BASE", "https://api.waterdata.usgs.gov/ogcapi/v1")
PAGE_LIMIT = 50_000
OUT_COLUMNS = ["time", "value", "approval_status", "qualifier"]


def _request(url: str, retries: int = 6) -> bytes:
    headers = {"User-Agent": "wq-bigdata-student-project/1.0"}
    if os.getenv("USGS_API_KEY"):
        headers["X-Api-Key"] = os.environ["USGS_API_KEY"]
    delay = 2.0
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=600) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = float(e.headers.get("Retry-After") or delay)
                print(f"  HTTP {e.code}, retry sau {wait:.0f}s ...", file=sys.stderr)
                time.sleep(wait)
                delay *= 2
                continue
            raise
        except (urllib.error.URLError, TimeoutError, http.client.IncompleteRead, ConnectionError) as e:
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


LEGACY_BASE = os.getenv("USGS_LEGACY_BASE", "https://nwis.waterservices.usgs.gov/nwis/iv/")


def fetch_legacy_site_year(site: str, params: list[str], a: datetime, b: datetime) -> dict[str, list[dict]]:
    """Dịch vụ Instantaneous Values (IV) cũ của USGS: lấy MỌI thông số của 1 trạm trong 1 request.

    Cùng dữ liệu đo tức thời như collection `continuous` của API mới, nhưng ít request hơn ~6 lần
    (API mới giới hạn số request/giờ). Thời gian trả về theo giờ địa phương -> đổi về UTC.
    """
    out: dict[str, list[dict]] = {p: [] for p in params}
    # Chia theo quý: phản hồi nhỏ (~3-4 MB) => ít bị cắt kết nối giữa chừng hơn 1 request cả năm (~15 MB)
    edges = [a] + [datetime(a.year, m, 1, tzinfo=timezone.utc) for m in (4, 7, 10) if a < datetime(a.year, m, 1, tzinfo=timezone.utc) < b] + [b]
    for qa, qb in zip(edges[:-1], edges[1:]):
        query = {"format": "json", "sites": site.replace("USGS-", ""), "parameterCd": ",".join(params),
                 "startDT": f"{qa:%Y-%m-%dT%H:%MZ}", "endDT": f"{qb - timedelta(minutes=1):%Y-%m-%dT%H:%MZ}",
                 "siteStatus": "all"}
        data = json.loads(_request(f"{LEGACY_BASE}?{urllib.parse.urlencode(query)}"))
        for ts in data["value"]["timeSeries"]:
            code = ts["variable"]["variableCode"][0]["value"]
            if code not in out:
                continue
            nodata = ts["variable"].get("noDataValue")
            for block in ts["values"]:
                for v in block["value"]:
                    t = datetime.fromisoformat(v["dateTime"]).astimezone(timezone.utc)
                    if not (qa <= t < qb):
                        continue
                    val = v.get("value", "")
                    if nodata is not None and val not in ("", None) and float(val) == float(nodata):
                        val = ""
                    quals = v.get("qualifiers") or []
                    approval = "Approved" if "A" in quals else ("Provisional" if "P" in quals else "")
                    out[code].append({"time": f"{t:%Y-%m-%d %H:%M:%S}+00:00", "value": val, "approval_status": approval,
                                      "qualifier": ",".join(q for q in quals if q not in ("A", "P"))})
    return out


def write_rows(path: Path, rows: list[dict], gzip_out: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / ("." + path.name + ".tmp")  # tên bắt đầu bằng "." => Spark bỏ qua nếu bị ngắt giữa chừng
    opener = (lambda f: gzip.open(f, "wt", newline="")) if gzip_out else (lambda f: open(f, "w", newline=""))
    with opener(tmp) as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def download_legacy_one(out_dir: Path, site: str, params: list[str], year: int, a: datetime, b: datetime,
                        force: bool, gzip_out: bool) -> tuple[str, int]:
    name = f"{year}.csv.gz" if gzip_out else f"{year}.csv"
    paths = {p: out_dir / f"station_id={site}" / f"parameter_code={p}" / name for p in params}
    todo = [p for p in params if force or not paths[p].exists()]
    if not todo:
        return f"skip {site} {year}", -1
    data = fetch_legacy_site_year(site, todo, a, b)
    n = 0
    for p in todo:
        if force or not paths[p].exists():   # tiến trình khác có thể vừa ghi xong file này
            write_rows(paths[p], data[p], gzip_out)
            n += len(data[p])
    return f"ok   {site} {year} ({len(todo)} thông số, {n} rows)", n


def year_chunks(start: datetime, end: datetime):
    y = start.year
    while True:
        a = max(start, datetime(y, 1, 1, tzinfo=timezone.utc))
        b = min(end, datetime(y + 1, 1, 1, tzinfo=timezone.utc))
        if a >= b:
            break
        yield y, a, b
        y += 1


def download_one(out_dir: Path, site: str, param: str, year: int, a: datetime, b: datetime, force: bool,
                 gzip_out: bool = False) -> tuple[str, int]:
    name = f"{year}.csv.gz" if gzip_out else f"{year}.csv"
    path = out_dir / f"station_id={site}" / f"parameter_code={param}" / name
    if path.exists() and not force:
        return f"skip {path}", -1
    rows = fetch_continuous(site, param, a, b)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / ("." + name + ".tmp")  # tên bắt đầu bằng "." => Spark bỏ qua nếu bị ngắt giữa chừng
    opener = (lambda f: gzip.open(f, "wt", newline="")) if gzip_out else (lambda f: open(f, "w", newline=""))
    with opener(tmp) as f:
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
    cfg = get_settings()
    ap.add_argument("--stations-file", type=Path, default=Path(cfg.stations_file))
    ap.add_argument("--sites", nargs="*", help="Danh sách station_id (mặc định lấy từ --stations-file)")
    ap.add_argument("--params", nargs="*", default=list(contracts.USGS_CODE_TO_NAME), help="USGS parameter codes")
    ap.add_argument("--start", default=contracts.DATA_START)
    ap.add_argument("--end", default=contracts.DATA_END, help="exclusive")
    ap.add_argument("--out", type=Path, default=Path(cfg.landing_path), help="landing zone (mặc định WQ_LANDING_PATH)")
    ap.add_argument("--workers", type=int, default=4, help="Số request song song (đừng để quá cao để tránh 429)")
    ap.add_argument("--force", action="store_true", help="Tải lại cả file đã có")
    ap.add_argument("--gzip", action="store_true", default=cfg.raw_gzip,
                    help="Ghi CSV nén gzip (dataset lớn; Spark đọc trực tiếp .csv.gz). Mặc định theo WQ_RAW_GZIP")
    ap.add_argument("--quiet", action="store_true", help="Chỉ in tiến độ mỗi 100 file")
    ap.add_argument("--source", choices=["ogc", "legacy"], default="ogc",
                    help="ogc = API mới (1 request/thông số/năm); legacy = dịch vụ IV cũ (1 request/trạm/năm, mọi thông số)")
    ap.add_argument("--reverse", action="store_true", help="duyệt danh sách trạm từ cuối (chạy song song 2 nguồn)")
    ap.add_argument("--refresh-metadata", action="store_true", help="Cập nhật tên/tọa độ trạm vào --stations-file")
    args = ap.parse_args(argv)

    sites = args.sites or load_station_ids(args.stations_file)
    if args.refresh_metadata:
        refresh_metadata(sites, args.stations_file)
        return

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    if args.reverse:
        sites = list(reversed(sites))
    chunks = list(year_chunks(start, end))
    if args.source == "legacy":
        jobs = [(s, y, a, b) for s in sites for (y, a, b) in chunks]
        submit = lambda ex, job: ex.submit(download_legacy_one, args.out, job[0], args.params, *job[1:], args.force, args.gzip)
        print(f"[legacy] {len(sites)} trạm x {len(chunks)} năm = {len(jobs)} request (mỗi request {len(args.params)} thông số)")
    else:
        jobs = [(s, p, y, a, b) for s in sites for p in args.params for (y, a, b) in chunks]
        submit = lambda ex, job: ex.submit(download_one, args.out, *job, args.force, args.gzip)
        print(f"{len(sites)} trạm x {len(args.params)} thông số x {len(chunks)} năm = {len(jobs)} file")

    t0, total = time.time(), 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {submit(ex, job): job for job in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                msg, n = fut.result()
                total += max(n, 0)
                if not args.quiet or i % 25 == 0 or i == len(jobs):
                    print(f"[{i}/{len(jobs)}] {msg}  | tổng {total:,} giá trị, {time.time() - t0:.0f}s", flush=True)
            except Exception as e:  # không dừng cả quá trình vì 1 file lỗi
                print(f"[{i}/{len(jobs)}] FAIL {futs[fut][:3]}: {e}", file=sys.stderr, flush=True)
    print(f"Xong: {total:,} giá trị mới trong {time.time() - t0:.0f}s -> {args.out}")


if __name__ == "__main__":
    main()
