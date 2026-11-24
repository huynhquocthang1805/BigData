"""Kafka replay producer: phát lại dữ liệu silver như thể các trạm cảm biến đang gửi dữ liệu thời gian thực.

- Mỗi message = 1 trạm x 1 mốc 15 phút (JSON), key = station_id
  => mọi bản ghi của 1 trạm vào cùng partition => giữ đúng thứ tự theo trạm.
- Dòng mà mọi cảm biến đều NULL bị bỏ qua (giả lập trạm mất kết nối => streaming phải tự phát hiện khoảng trống).
- Hai chế độ điều tốc:
    --speedup S : S giây dữ liệu được phát trong 1 giây thực (3600 => 1 giờ dữ liệu / giây) -> demo
    --rate R    : R message/giây cố định, bỏ qua thời gian sự kiện -> benchmark throughput
- Có thể bơm dữ liệu "bẩn" để kiểm chứng streaming: --late-fraction (đến trễ), --duplicate-fraction (trùng lặp).
- Producer bật idempotence (enable.idempotence=true): Kafka loại bỏ bản ghi trùng do producer retry.

Ví dụ:
    python -m ingestion.replay_producer --split test --speedup 3600
    python -m ingestion.replay_producer --split train --rate 20000 --replicate 4   # benchmark
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from wq import contracts
from wq.config import get_settings


def load_replay_frame(lake_path: str, start: str, end: str, stations: list[str] | None) -> pd.DataFrame:
    dataset = ds.dataset(f"{lake_path}/silver/measurements", format="parquet", partitioning="hive")
    ts_type = dataset.schema.field("ts").type  # ép kiểu mốc lọc theo đúng kiểu cột ts trong file

    def bound(x: str) -> pa.Scalar:
        t = pd.Timestamp(x, tz="UTC")
        return pa.scalar(t if getattr(ts_type, "tz", None) else t.tz_localize(None), type=ts_type)

    flt = (ds.field("ts") >= bound(start)) & (ds.field("ts") < bound(end))
    if stations:
        flt = flt & ds.field("station_id").isin(stations)
    df = dataset.to_table(filter=flt, columns=["ts", *contracts.VARIABLE_NAMES]
                          + ["station_id"]).to_pandas()
    df["station_id"] = df["station_id"].astype(str)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.dropna(subset=contracts.VARIABLE_NAMES, how="all")  # trạm mất kết nối => không có message
    return df.sort_values(["ts", "station_id"], kind="stable").reset_index(drop=True)


def serialize_bodies(df: pd.DataFrame) -> list[str]:
    """Tiền-serialize phần JSON sau station_id (trừ producer_ts/seq gắn lúc gửi) => producer đạt throughput cao."""
    ts_str = df["ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    cols = {c: df[c].to_numpy() for c in contracts.VARIABLE_NAMES}
    bodies = []
    for i in range(len(df)):
        parts = [f'"event_time":"{ts_str[i]}"']
        for c in contracts.VARIABLE_NAMES:
            v = cols[c][i]
            parts.append(f'"{c}":{"null" if math.isnan(v) else repr(float(v))}')
        bodies.append("," + ",".join(parts))
    return bodies


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=15).topics:
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic]
    try:
        fut.result(15)
        print(f"Đã tạo topic {topic} ({partitions} partitions)")
    except Exception as e:  # topic có thể vừa được tạo bởi tiến trình khác
        print(f"create_topics: {e}")


def main(argv: list[str] | None = None) -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=s.kafka_bootstrap)
    ap.add_argument("--topic", default=s.topic_raw)
    ap.add_argument("--partitions", type=int, default=contracts.TOPIC_PARTITIONS)
    ap.add_argument("--lake", default=s.lake_path)
    ap.add_argument("--split", default="test", choices=list(contracts.SPLITS), help="giai đoạn dữ liệu phát lại")
    ap.add_argument("--start", help="ghi đè thời điểm bắt đầu (ISO)")
    ap.add_argument("--end", help="ghi đè thời điểm kết thúc (ISO, exclusive)")
    ap.add_argument("--stations", nargs="*")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--speedup", type=float, default=3600.0)
    mode.add_argument("--rate", type=float, help="message/giây cố định (benchmark)")
    ap.add_argument("--limit", type=int, help="dừng sau N message")
    ap.add_argument("--replicate", type=int, default=1,
                    help="nhân bản mỗi trạm K lần (station_id thêm hậu tố -rK) để giả lập nhiều cảm biến hơn")
    ap.add_argument("--late-fraction", type=float, default=0.0, help="tỉ lệ message bị giữ lại rồi gửi trễ")
    ap.add_argument("--late-max-seconds", type=float, default=3.0, help="độ trễ tối đa (giây thực) của message trễ")
    ap.add_argument("--duplicate-fraction", type=float, default=0.0, help="tỉ lệ message bị gửi 2 lần")
    ap.add_argument("--late-log", help="ghi danh sách message bị làm trễ ra CSV (để kiểm chứng watermark)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--dry-run", action="store_true", help="in vài message rồi thoát, không cần Kafka")
    args = ap.parse_args(argv)

    lo, hi = contracts.SPLITS[args.split]
    start, end = args.start or lo, args.end or hi
    t_load = time.time()
    df = load_replay_frame(args.lake, start, end, args.stations)
    if df.empty:
        sys.exit(f"Không có dữ liệu silver trong [{start}, {end}). Hãy chạy batch ETL trước.")
    if args.limit:  # chỉ chuẩn bị đúng số bản ghi cần gửi (nạp nhanh hơn khi benchmark)
        df = df.iloc[: math.ceil(args.limit / max(1, args.replicate))].reset_index(drop=True)
    bodies = serialize_bodies(df)
    # epoch giây, không phụ thuộc độ phân giải (ns/us) mà pyarrow trả về
    event_s = ((df["ts"] - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).to_numpy()
    print(f"Nạp {len(df):,} bản ghi ({df['station_id'].nunique()} trạm, {start} -> {end}) trong {time.time() - t_load:.1f}s")

    if args.dry_run:
        sids = df["station_id"].tolist()
        for i in range(3):
            print(f'{{"station_id":"{sids[i]}"{bodies[i]},"producer_ts":{int(time.time() * 1000)},"seq":{i}}}')
        return

    # Lịch gửi: thời điểm (giây thực, tính từ lúc bắt đầu) mà message gốc i đến hạn
    n, K = len(bodies), max(1, args.replicate)
    if args.rate:
        due = np.arange(n, dtype="float64") * K / args.rate
    else:
        due = (event_s - event_s[0]).astype("float64") / args.speedup
    rng = random.Random(args.seed)
    if args.late_fraction > 0:  # message trễ: lùi thời điểm gửi => đến Kafka SAU các message mới hơn
        late = np.array([rng.random() < args.late_fraction for _ in range(n)])
        delay = late * np.array([rng.uniform(0.5, 1.0) * args.late_max_seconds for _ in range(n)])
        due = due + delay
        if args.late_log:
            speed = args.speedup if not args.rate else float("nan")
            pd.DataFrame({"station_id": df["station_id"], "ts": df["ts"], "delay_s": delay,
                          "event_delay_min": delay * speed / 60})[late].to_csv(args.late_log, index=False)
    order = np.argsort(due, kind="stable")
    dup = {i for i in range(n) if rng.random() < args.duplicate_fraction} if args.duplicate_fraction > 0 else set()

    from confluent_kafka import Producer

    ensure_topic(args.bootstrap, args.topic, args.partitions)
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,
        "linger.ms": 20,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
    })
    errors = {"n": 0}

    def on_delivery(err, _msg):
        if err is not None:
            errors["n"] += 1

    sids = df["station_id"].tolist()
    suffixes = [""] + [f"-r{r}" for r in range(1, K)]
    total = min(args.limit or math.inf, n * K + len(dup) * K)
    sent = 0
    t0 = time.monotonic()
    last_report, last_sent = t0, 0
    print(f"Bắt đầu gửi {total:,} message vào {args.topic} @ {args.bootstrap} "
          f"({'rate=%g msg/s' % args.rate if args.rate else 'speedup=%gx' % args.speedup}, replicate={K})")
    try:
        for k in range(n):
            if sent >= total:
                break
            i = order[k]
            for r, suf in enumerate(suffixes):
                target = t0 + due[i] + (r / args.rate if args.rate else 0.0)
                while (wait := target - time.monotonic()) > 0:
                    producer.poll(min(wait, 0.05))
                key = sids[i] + suf
                for _ in range(2 if i in dup else 1):
                    payload = f'{{"station_id":"{key}"{bodies[i]},"producer_ts":{int(time.time() * 1000)},"seq":{sent}}}'
                    while True:
                        try:
                            producer.produce(args.topic, key=key, value=payload, on_delivery=on_delivery)
                            break
                        except BufferError:  # hàng đợi nội bộ đầy => chờ bớt
                            producer.poll(0.05)
                    sent += 1
                producer.poll(0)
            now = time.monotonic()
            if now - last_report >= 5:
                ev = datetime.fromtimestamp(int(event_s[i]), tz=timezone.utc)
                print(f"  sent={sent:,}  rate={(sent - last_sent) / (now - last_report):,.0f} msg/s  event_time={ev:%Y-%m-%d %H:%M}")
                last_report, last_sent = now, sent
    except KeyboardInterrupt:
        print("Dừng theo yêu cầu.")
    finally:
        producer.flush(30)
    dt = time.monotonic() - t0
    print(f"Hoàn tất: {sent:,} message trong {dt:.1f}s ({sent / max(dt, 1e-9):,.0f} msg/s), lỗi giao nhận={errors['n']}")


if __name__ == "__main__":
    main()
