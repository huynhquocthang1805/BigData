"""Nạp dữ liệu thô từ vùng tiếp nhận (landing zone, ổ đĩa local) lên HDFS — bước "ingest" của data lake.

    landing (local)                    HDFS (phân tán, nhân bản 2 bản trên 2 DataNode)
    data/raw/usgs_large/station_id=…/parameter_code=…/2024.csv.gz  ->  hdfs://namenode:8020/wq/raw/usgs/…

Dùng Hadoop FileSystem API qua Spark (không cần cài Hadoop CLI trong container ứng dụng).
In ra số file, dung lượng, số block và hệ số nhân bản để đưa vào báo cáo.

    python -m batch.ingest_hdfs --src /app/data/raw/usgs_large --dst hdfs://namenode:8020/wq/raw/usgs
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from wq.config import get_settings
from wq.spark_utils import build_spark


def hdfs_summary(spark, path: str) -> dict:
    sc = spark.sparkContext
    jvm, conf = sc._jvm, sc._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(conf)
    cs = fs.getContentSummary(p)
    # đếm block + hệ số nhân bản thực tế của từng file
    it = fs.listFiles(p, True)
    blocks, repl = 0, set()
    while it.hasNext():
        st = it.next()
        blocks += len(st.getBlockLocations())
        repl.add(int(st.getReplication()))
    return {"path": path, "files": int(cs.getFileCount()), "bytes": int(cs.getLength()),
            "bytes_on_disk_all_replicas": int(cs.getSpaceConsumed()), "blocks": blocks,
            "replication": sorted(repl), "block_size_mb": int(fs.getDefaultBlockSize(p)) // (1024 * 1024)}


def ingest(spark, src: str, dst: str, overwrite: bool = False) -> bool:
    """Chép cây thư mục landing (local) sang dst (HDFS, hoặc file:// khi test). Trả về True nếu đã chép."""
    sc = spark.sparkContext
    jvm, conf = sc._jvm, sc._jsc.hadoopConfiguration()
    dst_p = jvm.org.apache.hadoop.fs.Path(dst)
    fs = dst_p.getFileSystem(conf)
    if fs.exists(dst_p):
        if not overwrite:
            print(f"{dst} đã tồn tại -> bỏ qua (thêm --overwrite để nạp lại)")
            return False
        # copyFromLocalFile vào một thư mục ĐÃ CÓ sẽ chép nguồn thành thư mục con dst/<tên nguồn> và giữ bản cũ
        # (Spark đọc theo glob station_id=* sẽ chỉ thấy dữ liệu cũ) => xóa bản cũ để --overwrite đúng nghĩa nạp lại
        fs.delete(dst_p, True)
    t0 = time.perf_counter()
    fs.mkdirs(dst_p.getParent())
    # copyFromLocalFile(delSrc, overwrite, src, dst): chép cả cây thư mục, giữ nguyên phân vùng station_id=/parameter_code=
    fs.copyFromLocalFile(False, True, jvm.org.apache.hadoop.fs.Path("file://" + src), dst_p)
    print(f"Đã nạp lên {dst} trong {time.perf_counter() - t0:.1f}s")
    return True


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    cfg = get_settings()
    ap.add_argument("--src", default=cfg.landing_path, help="thư mục landing trên máy (mặc định WQ_LANDING_PATH)")
    ap.add_argument("--dst", default=cfg.raw_path, help="đích trên HDFS (mặc định WQ_RAW_PATH)")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()
    if not args.dst.startswith("hdfs://"):
        print(f"WQ_RAW_PATH={args.dst} không phải HDFS -> không cần ingest (chế độ local)")
        return
    args.src = str(Path(args.src).resolve())

    spark = build_spark("wq-ingest-hdfs", master="local[2]")
    ingest(spark, args.src, args.dst, args.overwrite)
    print(json.dumps(hdfs_summary(spark, args.dst), indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
