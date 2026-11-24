"""Kiểm chứng tính đúng của luồng streaming sau một lần phát lại (dùng cho Chương 5 và demo chịu lỗi).

So sánh 3 nguồn: (1) dữ liệu silver lẽ ra phải đến, (2) số message thực có trong Kafka (gồm cả bản trùng),
(3) bảng `measurements` trong PostgreSQL. Trả lời: có mất dữ liệu không? có ghi trùng không? bao nhiêu bản ghi
trễ bị watermark loại bỏ, và chúng trễ bao lâu?

    python -m scripts.verify_stream --start 2025-07-01 --end 2025-07-04 --late-log data/late.csv
"""
from __future__ import annotations

import argparse

import pandas as pd
import psycopg

from ingestion.replay_producer import load_replay_frame
from wq.config import get_settings


def kafka_message_count(bootstrap: str, topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "wq-verify", "enable.auto.commit": False})
    parts = c.list_topics(topic, timeout=15).topics[topic].partitions
    total = 0
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        total += hi - lo
    c.close()
    return total


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--late-log")
    ap.add_argument("--watermark-min", type=float, default=120, help="watermark (phút) đang cấu hình")
    args = ap.parse_args()

    expected = load_replay_frame(s.lake_path, args.start, args.end, None)[["station_id", "ts"]]
    with psycopg.connect(s.pg_dsn) as conn:
        db = pd.DataFrame(conn.execute(
            "SELECT station_id, ts FROM measurements WHERE ts >= %s AND ts < %s",
            (pd.Timestamp(args.start, tz="UTC"), pd.Timestamp(args.end, tz="UTC"))).fetchall(),
            columns=["station_id", "ts"])
    db["ts"] = pd.to_datetime(db["ts"], utc=True)
    exp_keys = set(zip(expected.station_id, expected.ts))
    db_keys = set(zip(db.station_id, db.ts))
    missing = exp_keys - db_keys
    extra = db_keys - exp_keys
    n_kafka = kafka_message_count(s.kafka_bootstrap, s.topic_raw)

    print(f"Bản ghi lẽ ra phải đến (silver, bỏ mốc toàn NULL): {len(exp_keys):,}")
    print(f"Message trong Kafka topic {s.topic_raw}:            {n_kafka:,}  (chênh {n_kafka - len(exp_keys):+,} = bản trùng cố ý/khác)")
    print(f"Dòng trong PostgreSQL (khoảng thời gian này):     {len(db):,}  | trùng khóa: {len(db) - len(db_keys)}")
    print(f"Thiếu trong DB: {len(missing):,} | Thừa (không có trong silver): {len(extra):,}")
    if args.late_log:
        late = pd.read_csv(args.late_log, parse_dates=["ts"])
        late["ts"] = pd.to_datetime(late["ts"], utc=True)
        late_keys = set(zip(late.station_id, late.ts))
        dropped = late[[k in missing for k in zip(late.station_id, late.ts)]]
        kept = late[[k not in missing for k in zip(late.station_id, late.ts)]]
        print(f"Message bị làm trễ cố ý: {len(late_keys):,} | bị watermark loại: {len(dropped):,} | vẫn được nhận: {len(kept):,}")
        if len(late):
            med_drop = f"{dropped.event_delay_min.median():.0f}" if len(dropped) else "—"
            print(f"  độ trễ (phút dữ liệu) — bị loại: trung vị {med_drop}, "
                  f"được nhận: trung vị {kept.event_delay_min.median():.0f} (watermark {args.watermark_min:.0f} phút)")
        unexplained = missing - late_keys
        print(f"Thiếu KHÔNG giải thích được bởi dữ liệu trễ: {len(unexplained):,}  (0 = không mất dữ liệu)")
    else:
        print("(thêm --late-log để phân tích dữ liệu trễ)")


if __name__ == "__main__":
    main()
