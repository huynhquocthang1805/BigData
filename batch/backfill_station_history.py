"""Nạp quan trắc Silver vào PostgreSQL cho dashboard, theo từng ngày UTC."""
from __future__ import annotations

import argparse
from datetime import date, timedelta

from wq import contracts, db
from wq.config import get_settings
from wq.io import core_station_ids, read_silver


def merge_observations(conn, frame):
    """Combine partial Silver exports at the same key, preserving measurements and QC exclusions."""
    variables = contracts.VARIABLE_NAMES
    columns = [*contracts.MEASUREMENT_COLUMNS, "qc_flags"]
    frame = frame.assign(qc_flags=frame.get("qc_flags", 0))
    frame["qc_flags"] = frame.qc_flags.fillna(0).astype(int)
    rows = frame[columns].astype(object).where(frame[columns].notna(), None)
    names = ", ".join(columns)
    # Only fill a missing channel when neither source has flagged that reading.
    updates, changes, null_bits = [], [], []
    for var in variables:
        bit = contracts.VARIABLE_BIT[var]
        eligible = (f"m.{var} IS NULL AND EXCLUDED.{var} IS NOT NULL "
                    f"AND ((m.qc_flags | EXCLUDED.qc_flags) & {bit}) = 0")
        changes.append(f"({eligible})")
        updates.append(f"{var} = CASE WHEN {eligible} THEN EXCLUDED.{var} ELSE m.{var} END")
        null_bits.append(f"(CASE WHEN m.{var} IS NULL THEN {bit} ELSE 0 END)")
    flags = "m.qc_flags | (EXCLUDED.qc_flags & (" + " | ".join(null_bits) + "))"
    updates.append(f"qc_flags = {flags}")
    changes.append(f"m.qc_flags <> ({flags})")
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE silver_day ON COMMIT DROP AS SELECT {names} FROM measurements WITH NO DATA")
        with cur.copy(f"COPY silver_day ({names}) FROM STDIN") as copy:
            for row in rows.itertuples(index=False, name=None):
                copy.write_row(row)
        cur.execute(f"INSERT INTO measurements AS m ({names}) SELECT {names} FROM silver_day ORDER BY station_id, ts "
                    f"ON CONFLICT (station_id, ts) DO UPDATE SET {', '.join(updates)} WHERE {' OR '.join(changes)}")
        return cur.rowcount


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--station", help="Mã một trạm")
    group.add_argument("--stations", choices=["all"], help="Mọi trạm trong WQ_STATIONS_FILE")
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="Ngày kết thúc, không gồm")
    parser.add_argument("--lake", help="Thư mục gốc chứa silver/ (mặc định WQ_LAKE_PATH)")
    args = parser.parse_args()
    if args.end <= args.start:
        parser.error("--end phải sau --start")
    if args.stations == "all":
        settings = get_settings()
        stations = core_station_ids(settings.stations_file)
        db.seed_stations(settings)
    else:
        stations = [args.station]
    seen = set()
    day = args.start
    with db.connect() as conn:
        while day < args.end:
            end = day + timedelta(days=1)
            frame = read_silver(stations, day.isoformat(), end.isoformat(), args.lake)
            observed = frame.dropna(subset=contracts.VARIABLE_NAMES, how="all")
            changed = 0
            if not observed.empty:
                with conn.transaction():
                    changed = merge_observations(conn, observed)
                seen.update(observed.station_id.unique())
            print(f"{day} observations={len(observed)} inserted_or_enriched={changed} "
                  f"stations={observed.station_id.nunique()}", flush=True)
            day = end
    print(f"stations_with_observations={len(seen)}/{len(stations)}", flush=True)
    if not seen:
        raise SystemExit("Silver không có quan trắc trong khoảng đã chọn.")


if __name__ == "__main__":
    main()
