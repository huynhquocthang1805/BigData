"""Benchmark STREAMING: throughput & độ trễ end-to-end (producer -> Kafka -> Spark -> PostgreSQL) theo tốc độ phát.

Mỗi mức tốc độ R (msg/s) chạy độc lập: topic Kafka mới + checkpoint mới + DB riêng `wq_bench`
(không ảnh hưởng dữ liệu demo). Stream processor chạy thật (subprocess `streaming.stream_processor`).

    # trong container (cần kafka + postgres; chế độ cluster cần thêm spark-master/worker):
    python -m benchmarks.bench_streaming --rates 1000 2000 4000 8000 --duration 60
    python -m benchmarks.bench_streaming --rates 20000 --cores 1 2 4 --master spark://spark-master:7077  # scale-out

Kết quả: benchmarks/results/stream_scaling.csv
Đọc kết quả: tốc độ R còn "bền vững" nếu processed ≈ R và độ trễ p95 không tăng dần theo thời gian.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import signal
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psycopg

from wq.config import get_settings

BENCH_DB = "wq_bench"
FIELDS = ["mode", "cores", "ml", "target_rate", "messages", "producer_rate", "duration_s", "processed_rows",
          "throughput_rows_s", "spark_processed_rps_median", "latency_p50_ms", "latency_p95_ms",
          "latency_max_ms", "batches", "batch_ms_median", "drain_s", "host_cpus", "stations"]


def ensure_bench_db(s) -> None:
    with psycopg.connect(s.pg_dsn, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (BENCH_DB,)).fetchone():
            conn.execute(f"CREATE DATABASE {BENCH_DB}")
    sql = Path("deploy/postgres/init.sql").read_text()
    with psycopg.connect(s.pg_dsn.replace(f"dbname={s.pg_db}", f"dbname={BENCH_DB}"), autocommit=True) as conn:
        conn.execute(sql)


def bench_conn(s):
    return psycopg.connect(s.pg_dsn.replace(f"dbname={s.pg_db}", f"dbname={BENCH_DB}"), autocommit=True)


def run_one(s, args, rate: int, cores: int, run_id: str) -> dict:
    topic = f"wq.bench.{run_id}.r{rate}.c{cores}"
    ckpt = f"{args.work}/ckpt/{topic}"
    with bench_conn(s) as c:
        c.execute("TRUNCATE measurements, measurements_imputed, measurements_hourly, predictions, alerts, "
                  "stream_progress, batch_latency")
    env = {**os.environ, "WQ_PG_DB": BENCH_DB, "WQ_TOPIC_RAW": topic, "WQ_CHECKPOINT_PATH": ckpt,
           "WQ_STARTING_OFFSETS": "earliest", "WQ_SPARK_MASTER": args.master, "WQ_TRIGGER": args.trigger,
           "WQ_MAX_OFFSETS_PER_TRIGGER": str(args.max_offsets), "WQ_SHUFFLE_PARTITIONS": str(max(2, cores)),
           "WQ_SPARK_LOG_LEVEL": "ERROR"}
    if args.master.startswith("spark://"):
        env.update({"WQ_CORES_MAX": str(cores), "WQ_EXECUTOR_CORES": "1",
                    "WQ_SPARK_DRIVER_HOST": socket.gethostbyname(socket.gethostname())})
    else:
        env["WQ_SPARK_MASTER"] = f"local[{cores}]"

    # 1) tạo topic + khởi động stream processor, chờ nó sẵn sàng
    subprocess.run([sys.executable, "-c", f"from ingestion.replay_producer import ensure_topic; "
                    f"ensure_topic('{s.kafka_bootstrap}', '{topic}', 6)"], check=True, env=env)
    cmd = [sys.executable, "-m", "streaming.stream_processor", "--queries", "serving"] + ([] if args.ml else ["--no-ml"])
    log_path = Path(args.work) / f"{topic}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_f = open(log_path, "w")
    sp = subprocess.Popen(cmd, env=env, stdout=log_f, stderr=subprocess.STDOUT, text=True)
    t_wait = time.time()
    while "Đang chạy" not in log_path.read_text(errors="ignore"):
        if sp.poll() is not None or time.time() - t_wait > 180:
            raise RuntimeError(f"stream processor không khởi động được, xem {log_path}")
        time.sleep(1)
    time.sleep(args.warmup)

    # 2) producer bắn đúng tốc độ R trong `duration` giây
    n_msgs = rate * args.duration
    t0 = time.time()
    prod = subprocess.run([sys.executable, "-m", "ingestion.replay_producer", "--topic", topic,
                           "--start", "2023-01-01", "--end", "2026-01-01", "--rate", str(rate),
                           "--limit", str(n_msgs), "--replicate", str(args.replicate)]
                          + (["--max-stations", str(args.max_stations)] if args.max_stations else []),
                          env=env, capture_output=True, text=True)
    t_prod = time.time() - t0
    m = re.search(r"Hoàn tất: ([\d,]+) message trong ([\d.]+)s", prod.stdout)
    if m:  # thời gian gửi thực sự (không gồm lúc nạp/serialize dữ liệu)
        sent, send_s = int(m.group(1).replace(",", "")), float(m.group(2))
    else:
        sent, send_s = n_msgs, t_prod
    producer_rate = sent / send_s

    # 3) chờ Spark xử lý hết backlog (số dòng trong DB ngừng tăng)
    last, stable_since, t_drain0 = -1, time.time(), time.time()
    with bench_conn(s) as c:
        while time.time() - t_drain0 < args.max_drain:
            n = c.execute("SELECT count(*) FROM measurements").fetchone()[0]
            if n != last:
                last, stable_since = n, time.time()
            elif time.time() - stable_since > max(10, 3 * float(args.trigger.split()[0])):
                break
            time.sleep(1)
        drain_s = round(max(0.0, stable_since - (t0 + t_prod)), 1)
        lat = c.execute("SELECT latency_p50_ms, latency_p95_ms, latency_max_ms, total_ms, n_rows, recorded_at "
                        "FROM batch_latency ORDER BY id").fetchall()
        prog = c.execute("SELECT processed_rows_per_sec FROM stream_progress WHERE query_name = 'serving_sink' "
                         "AND num_input_rows > 0 ORDER BY id").fetchall()
        n_stations = c.execute("SELECT count(DISTINCT station_id) FROM measurements").fetchone()[0]
    sp.send_signal(signal.SIGTERM)
    try:
        sp.wait(60)
    except subprocess.TimeoutExpired:
        sp.kill()
    log_f.close()
    if prod.returncode != 0:
        print(prod.stdout[-2000:], prod.stderr[-2000:])

    rows = sum(r[4] for r in lat)
    span = (lat[-1][5] - lat[0][5]).total_seconds() if len(lat) > 1 else float("nan")
    steady = [r for r in lat if r[4] > 0][1:] or lat  # bỏ batch đầu (khởi động)
    return {
        "mode": "cluster" if args.master.startswith("spark://") else "local", "cores": cores, "ml": args.ml,
        "target_rate": rate, "messages": sent, "producer_rate": round(producer_rate), "duration_s": round(send_s, 1),
        "processed_rows": rows, "throughput_rows_s": round(rows / (send_s + drain_s)) if rows else 0,
        "spark_processed_rps_median": round(statistics.median([p[0] for p in prog])) if prog else None,
        "latency_p50_ms": round(statistics.median([r[0] for r in steady])) if steady else None,
        "latency_p95_ms": round(statistics.median([r[1] for r in steady])) if steady else None,
        "latency_max_ms": round(max(r[2] for r in lat)) if lat else None,
        "batches": len(lat), "batch_ms_median": round(statistics.median([r[3] for r in lat])) if lat else None,
        "drain_s": drain_s, "host_cpus": os.cpu_count(), "span_s": span, "stations": n_stations,
    }


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rates", type=int, nargs="+", default=[1000, 2000, 4000, 8000])
    ap.add_argument("--cores", type=int, nargs="+", default=[4])
    ap.add_argument("--master", default="local", help="'local' hoặc spark://spark-master:7077")
    ap.add_argument("--duration", type=int, default=60, help="số giây producer phát ở mỗi mức tốc độ")
    ap.add_argument("--replicate", type=int, default=1)
    ap.add_argument("--trigger", default="2 seconds")
    ap.add_argument("--max-offsets", type=int, default=500000)
    ap.add_argument("--warmup", type=int, default=10)
    ap.add_argument("--max-drain", type=int, default=300)
    ap.add_argument("--ml", action="store_true", help="bật điền khuyết + dự báo online trong lúc đo")
    ap.add_argument("--max-stations", type=int, help="chỉ phát N trạm (so sánh theo số trạm trên cùng dữ liệu)")
    ap.add_argument("--work", default="data/bench")
    ap.add_argument("--out", default="benchmarks/results/stream_scaling.csv")
    args = ap.parse_args()

    ensure_bench_db(s)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    new_file = not Path(args.out).exists()
    # file cũ giữ nguyên header của nó (vd. chưa có cột stations) để các dòng mới vẫn khớp cột
    fields = FIELDS if new_file else Path(args.out).open().readline().strip().split(",")
    run_id = time.strftime("%H%M%S")
    with open(args.out, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new_file:
            w.writeheader()
        for cores in args.cores:
            for rate in args.rates:
                print(f"=== cores={cores} rate={rate} msg/s, {args.duration}s ===", flush=True)
                r = run_one(s, args, rate, cores, run_id)
                w.writerow(r)
                f.flush()
                print({k: r[k] for k in ("producer_rate", "processed_rows", "throughput_rows_s",
                                         "latency_p50_ms", "latency_p95_ms", "drain_s")}, flush=True)
    print(f"Đã ghi {args.out}")


if __name__ == "__main__":
    main()
