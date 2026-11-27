"""Data contract dùng chung cho cả nhóm (TV1 quản lý).

Mọi module (ETL, streaming, imputation, prediction, API, dashboard) đều đọc tên cột,
đơn vị, ngưỡng hợp lệ... từ file này. Muốn đổi contract: mở issue/PR, cả nhóm đồng ý,
tăng CONTRACT_VERSION. File này KHÔNG import pyspark để TV3/TV4 dùng được trong notebook.
"""
from __future__ import annotations

from dataclasses import dataclass

CONTRACT_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Thời gian: lưới đều 15 phút, luôn dùng UTC
# ---------------------------------------------------------------------------
FREQ = "15min"
FREQ_MINUTES = 15
STEPS_PER_HOUR = 60 // FREQ_MINUTES  # 4
STEPS_PER_DAY = 24 * STEPS_PER_HOUR  # 96


@dataclass(frozen=True)
class Variable:
    name: str            # tên cột chuẩn trong mọi bảng / DataFrame
    usgs_code: str       # USGS parameter code
    unit: str
    label_vi: str
    valid_min: float     # ngoài khoảng vật lý này => coi là lỗi cảm biến (set NULL + bật cờ QC)
    valid_max: float
    core: bool = True    # True = biến chất lượng nước chính (impute + dự báo); False = biến phụ trợ


VARIABLES: tuple[Variable, ...] = (
    Variable("water_temp", "00010", "degC", "Nhiệt độ nước", -5.0, 45.0),
    Variable("specific_conductance", "00095", "uS/cm @25C", "Độ dẫn điện riêng", 0.0, 100_000.0),
    Variable("ph", "00400", "pH", "Độ pH", 0.0, 14.0),
    Variable("dissolved_oxygen", "00300", "mg/L", "Oxy hòa tan (DO)", 0.0, 25.0),
    Variable("turbidity", "63680", "FNU", "Độ đục", 0.0, 4_000.0),
    Variable("discharge", "00060", "ft3/s", "Lưu lượng dòng chảy", -1_000_000.0, 5_000_000.0, core=False),
)

VARIABLE_NAMES: list[str] = [v.name for v in VARIABLES]
CORE_VARIABLES: list[str] = [v.name for v in VARIABLES if v.core]
USGS_CODE_TO_NAME: dict[str, str] = {v.usgs_code: v.name for v in VARIABLES}
NAME_TO_USGS_CODE: dict[str, str] = {v.name: v.usgs_code for v in VARIABLES}
VALID_RANGES: dict[str, tuple[float, float]] = {v.name: (v.valid_min, v.valid_max) for v in VARIABLES}
UNITS: dict[str, str] = {v.name: v.unit for v in VARIABLES}
LABELS_VI: dict[str, str] = {v.name: v.label_vi for v in VARIABLES}

KEY_COLUMNS: list[str] = ["station_id", "ts"]
MEASUREMENT_COLUMNS: list[str] = KEY_COLUMNS + VARIABLE_NAMES

# Bit i của qc_flags / imputed_mask tương ứng VARIABLE_NAMES[i]
VARIABLE_BIT: dict[str, int] = {name: 1 << i for i, name in enumerate(VARIABLE_NAMES)}

# ---------------------------------------------------------------------------
# Chia dữ liệu theo thời gian (KHÔNG xáo trộn) - TV3/TV4 bắt buộc dùng chung
# Khoảng nửa mở [start, end)
# ---------------------------------------------------------------------------
DATA_START = "2023-01-01"
DATA_END = "2026-01-01"
SPLITS: dict[str, tuple[str, str]] = {
    "train": ("2023-01-01", "2025-01-01"),
    "valid": ("2025-01-01", "2025-07-01"),
    "test": ("2025-07-01", "2026-01-01"),  # streaming demo phát lại đúng giai đoạn này
}

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
TOPIC_SENSOR_RAW = "wq.sensor.raw"
TOPIC_PARTITIONS = 6
# Message JSON (key = station_id để mọi bản ghi của 1 trạm vào cùng partition => giữ thứ tự)
SENSOR_MESSAGE_FIELDS: list[str] = ["station_id", "event_time", *VARIABLE_NAMES, "producer_ts", "seq"]

# ---------------------------------------------------------------------------
# Dự báo (TV4 có thể đổi target/horizon nhưng phải cập nhật contract)
# ---------------------------------------------------------------------------
FORECAST_TARGETS: list[str] = ["dissolved_oxygen"]
FORECAST_HORIZONS_STEPS: list[int] = [4, 12, 24, 96]  # 1h, 3h, 6h, 24h


@dataclass(frozen=True)
class AlertRule:
    parameter: str
    op: str          # "<" hoặc ">"
    threshold: float
    severity: str    # "warning" | "critical"
    rule: str        # mã luật ngắn
    message_vi: str


# Ngưỡng tuyệt đối dùng trong streaming (mỗi bản ghi). TV2 sở hữu; chỉ giữ ngưỡng có tiêu chí số được trích dẫn
# (danh mục nguồn đầy đủ: wq.wqi.SOURCES):
#   [EPA1986DO] U.S. EPA (1986). Ambient Water Quality Criteria for Dissolved Oxygen. EPA 440/5-86-003.
#               Nước ấm, giai đoạn sớm của sinh vật: DO tối thiểu 1 ngày 5,0 mg/L.
#   [Rabalais2002] Rabalais, Turner & Wiseman (2002). Gulf of Mexico hypoxia. Annu. Rev. Ecol. Syst. 33:235-263.
#               Hypoxia: DO < 2 mg/L.
#   [EPA1986]   U.S. EPA (1986). Quality Criteria for Water 1986 ("Gold Book"). EPA 440/5-86-001.
#               pH 6,5-9,0 cho đời sống thủy sinh nước ngọt.
# Nhiệt độ, độ dẫn điện, độ đục KHÔNG có tiêu chí số chung toàn quốc (độ dẫn điện nước lợ vốn > 2000 µS/cm; hướng
# dẫn độ đục của CCME 2002 tính theo mức tăng so với nền) => đã bỏ TEMP_HIGH, SC_HIGH, TURB_HIGH khỏi danh sách này
# và thay bằng luật theo mức nền của từng trạm + gộp theo sự kiện trong batch/alerts_tv2.py (TV2.2c).
ALERT_RULES: tuple[AlertRule, ...] = (
    AlertRule("dissolved_oxygen", "<", 5.0, "warning", "DO_LOW",
              "DO < 5 mg/L: dưới mức tối thiểu cho thủy sinh nước ấm (EPA 1986)"),
    AlertRule("dissolved_oxygen", "<", 2.0, "critical", "DO_HYPOXIA",
              "DO < 2 mg/L: thiếu oxy (hypoxia, Rabalais et al. 2002)"),
    AlertRule("ph", "<", 6.5, "warning", "PH_LOW", "pH < 6.5: ngoài khoảng 6,5-9,0 (EPA 1986)"),
    AlertRule("ph", ">", 9.0, "warning", "PH_HIGH",
              "pH > 9.0: ngoài khoảng 6,5-9,0 (EPA 1986), có thể do tảo nở hoa"),
)


def qc_bitmask(names: list[str]) -> int:
    """Trả về bitmask cho danh sách tên biến."""
    mask = 0
    for n in names:
        mask |= VARIABLE_BIT[n]
    return mask


def decode_bitmask(mask: int) -> list[str]:
    """Giải mã bitmask (qc_flags / imputed_mask) thành danh sách tên biến."""
    return [n for n, b in VARIABLE_BIT.items() if mask & b]
