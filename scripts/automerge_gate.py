"""Cổng tự động merge PR (chạy trong .github/workflows/automerge.yml sau khi CI pass).

Script chỉ đọc PR qua GitHub API, KHÔNG chạy code của PR. PR được merge ngay vào `main` nếu đạt mọi luật
máy kiểm tra được (docs/TEAM_TASKS.md mục 6.4). Không đạt thì để lại một comment nêu lý do (một lần cho mỗi
head SHA) và chờ bot review hoặc TV1. Những điều cần phán đoán (đạt tiêu chí của việc chưa, rò rỉ dữ liệu,
mô hình trên dữ liệu thật, số liệu báo cáo) do bot review chấm sau khi merge.

Biến môi trường: GITHUB_TOKEN, REPO (owner/name), HEAD_SHA, PRS (JSON danh sách PR của lượt CI).
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request

API = "https://api.github.com"
BASE_BRANCH = "main"
MAX_FILE_MB = 5
TEAM_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}
MARKER = "<!-- automerge-gate:{sha} -->"

# Sửa, thêm hay xóa bất kỳ file nào ở đây => không tự merge (hạ tầng, contract, quy trình, cấu hình test).
BLOCKED = (".github/", "docker/", "deploy/", "streaming/", "data/", "docker-compose.yml", "Makefile",
           "wq/contracts.py", "docs/data_contract.md", "docs/TEAM_TASKS.md",
           "pytest.ini", "setup.cfg", "pyproject.toml", "tox.ini")
# File đã có của TV1: không được sửa, đổi tên hay xóa; THÊM file mới vào các thư mục này thì được.
TV1_OWNED = ("api/", "benchmarks/", "scripts/", "ingestion/replay_producer.py", "batch/ingest_hdfs.py",
             "wq/config.py", "wq/db.py", "wq/io.py", "wq/timeseries.py", "wq/spark_utils.py",
             "wq/spark_transforms.py", "wq/models/base.py", "wq/models/registry.py", "wq/models/validate.py",
             "wq/models/baselines.py", "tests/test_contracts.py", "tests/test_models.py",
             "tests/test_producer.py", "tests/test_spark.py", "tests/test_tv1_tools.py",
             "tests/test_automerge_gate.py", "docs/TV1_GUIDE.md", "docs/architecture.md",
             "docs/report/ch1_gioi_thieu.md", "docs/report/ch2_ly_thuyet_bigdata_tv1.md",
             "docs/report/ch4_thiet_ke_he_thong_tv1.md", "docs/report/ch5_hieu_nang_tv1.md",
             "docs/report/ch6_ket_luan.md")
BINARY_EXT = (".joblib", ".pkl", ".pickle", ".pt", ".pth", ".h5", ".onnx", ".parquet")
SKIP_RE = re.compile(r"pytest\.mark\.(skip|skipif|xfail)\b|pytest\.(skip|xfail)\(|unittest\.skip")


def is_blocked(path: str) -> bool:
    return path.startswith(BLOCKED) or os.path.basename(path).startswith(".env")


def is_test_file(path: str) -> bool:
    return path.startswith("tests/") or os.path.basename(path) == "conftest.py"


def check(pr: dict, files: list[dict], sizes: dict[str, int], repo: str) -> list[str]:
    """Danh sách lý do KHÔNG tự merge (rỗng = được merge). `files` theo định dạng API pulls/{n}/files."""
    problems: list[str] = []
    if pr["base"]["ref"] != BASE_BRANCH:
        problems.append(f"PR vào `{pr['base']['ref']}`, không phải `{BASE_BRANCH}`.")
    head_repo = (pr["head"].get("repo") or {}).get("full_name")
    if head_repo != repo:
        problems.append("PR từ fork: chỉ PR từ nhánh nằm trong repo này mới được tự merge.")
    if pr.get("author_association") not in TEAM_ASSOCIATIONS:
        problems.append(f"Tài khoản `{pr['user']['login']}` không phải thành viên có quyền ghi của repo.")
    for f in files:
        name, status, prev = f["filename"], f["status"], f.get("previous_filename") or ""
        paths = [name] + ([prev] if prev else [])
        hit = next((p for p in paths if is_blocked(p)), None)
        if hit:
            problems.append(f"`{hit}`: file hạ tầng, contract hoặc quy trình, cần TV1 duyệt.")
        if status != "added" and any(p.startswith(TV1_OWNED) for p in paths):
            problems.append(f"`{name}`: sửa file đã có của TV1 ({status}), cần TV1 duyệt.")
        if status != "removed" and name.lower().endswith(BINARY_EXT):
            problems.append(f"`{name}`: không commit mô hình hoặc dữ liệu nhị phân; ghi lệnh tái lập trong PR.")
        if status in ("removed", "renamed") and any(is_test_file(p) for p in paths):
            problems.append(f"`{prev or name}`: không được xóa hoặc đổi tên test.")
        if is_test_file(name) and status != "added":  # test đã có; test mới được skip có điều kiện (vd. thiếu dữ liệu)
            added = [ln for ln in (f.get("patch") or "").splitlines() if ln.startswith("+") and not ln.startswith("+++")]
            if any(SKIP_RE.search(ln) for ln in added):
                problems.append(f"`{name}`: thêm skip hoặc xfail vào test đã có.")
        size = sizes.get(name, 0)
        if status != "removed" and size > MAX_FILE_MB * 1024 * 1024:
            problems.append(f"`{name}`: {size / 1e6:.1f} MB, vượt giới hạn {MAX_FILE_MB} MB.")
    return list(dict.fromkeys(problems))  # bỏ trùng, giữ thứ tự


# ---------------------------------------------------------------------------- GitHub API
def api(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
    req = urllib.request.Request(
        API + path, method=method, data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "wq-automerge-gate"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"message": raw.decode(errors="replace")}


def paged(path: str) -> list:
    out, page = [], 1
    while True:
        st, data = api("GET", f"{path}{'&' if '?' in path else '?'}per_page=100&page={page}")
        if st != 200 or not data:
            return out
        out += data
        if len(data) < 100:
            return out
        page += 1


def comment_once(repo: str, number: int, sha: str, text: str) -> None:
    marker = MARKER.format(sha=sha)
    if any(marker in (c.get("body") or "") for c in paged(f"/repos/{repo}/issues/{number}/comments")):
        return
    api("POST", f"/repos/{repo}/issues/{number}/comments", {"body": f"{marker}\n{text}"})


def handle(repo: str, number: int, head_sha: str) -> None:
    st, pr = api("GET", f"/repos/{repo}/pulls/{number}")
    if st != 200 or pr["state"] != "open":
        print(f"PR #{number}: bỏ qua (state={pr.get('state') if isinstance(pr, dict) else st})")
        return
    if pr["head"]["sha"] != head_sha:
        print(f"PR #{number}: head đã đổi sau lượt CI này, chờ lượt CI mới")
        return
    if pr.get("draft"):
        print(f"PR #{number}: Draft, bỏ qua")
        return
    files = paged(f"/repos/{repo}/pulls/{number}/files")
    st, tree = api("GET", f"/repos/{repo}/git/trees/{head_sha}?recursive=1")
    sizes = {t["path"]: t.get("size", 0) for t in (tree or {}).get("tree", []) if t.get("type") == "blob"} if st == 200 else {}
    problems = check(pr, files, sizes, repo)
    if problems:
        print(f"PR #{number}: không tự merge\n  " + "\n  ".join(problems))
        comment_once(repo, number, head_sha,
                     "Chưa tự động merge PR này. CI đã pass, nhưng PR rơi vào luật ở mục 6.4 của `docs/TEAM_TASKS.md`:\n\n"
                     + "\n".join(f"- {p}" for p in problems)
                     + "\n\nBot review hoặc TV1 sẽ xem ở lượt kế tiếp. Sửa được thì push tiếp lên cùng nhánh: "
                       "CI và cổng tự động sẽ chạy lại.")
        return
    st, res = api("PUT", f"/repos/{repo}/pulls/{number}/merge",
                  {"merge_method": "merge", "sha": head_sha, "commit_title": f"Merge PR #{number}: {pr['title']}"})
    if st == 200 and res.get("merged"):
        print(f"PR #{number}: đã merge vào {BASE_BRANCH} ({res.get('sha')})")
        comment_once(repo, number, head_sha,
                     f"Đã tự động merge vào `{BASE_BRANCH}`: CI pass và PR đạt các luật tự động ở mục 6.4 của "
                     "`docs/TEAM_TASKS.md`. Bot review sẽ chấm từng ô \"Kết quả cần đạt\", kiểm tra mô hình trên "
                     "dữ liệu thật (nếu có) và cập nhật bảng trạng thái ở lượt kế tiếp. Nhớ `git pull origin main` "
                     "vào nhánh của mình trước khi làm tiếp.")
        # Merge bằng GITHUB_TOKEN không tự kích hoạt workflow khác => chạy lại CI trên main sau khi merge
        api("POST", f"/repos/{repo}/actions/workflows/ci.yml/dispatches", {"ref": BASE_BRANCH})
    elif st == 409:
        print(f"PR #{number}: head đã đổi khi merge, chờ lượt CI mới")
    else:
        msg = res.get("message") if isinstance(res, dict) else res
        print(f"PR #{number}: merge lỗi {st}: {msg}")
        hint = ("PR đang xung đột với `main`: gộp `main` vào nhánh của bạn (`git pull origin main`), "
                "sửa xung đột rồi push lại." if st == 405 else f"GitHub báo lỗi {st}: {msg}")
        comment_once(repo, number, head_sha, f"CI pass nhưng chưa merge được. {hint}")


def main() -> int:
    repo, head_sha = os.environ["REPO"], os.environ["HEAD_SHA"]
    numbers = [p["number"] for p in json.loads(os.environ.get("PRS") or "[]")]
    if not numbers:  # dự phòng: tìm PR mở theo commit
        st, data = api("GET", f"/repos/{repo}/commits/{head_sha}/pulls")
        numbers = [p["number"] for p in (data if st == 200 else []) if p.get("state") == "open"]
    if not numbers:
        print("Không có PR nào gắn với lượt CI này")
    for n in numbers:
        handle(repo, n, head_sha)
    return 0


if __name__ == "__main__":
    sys.exit(main())
