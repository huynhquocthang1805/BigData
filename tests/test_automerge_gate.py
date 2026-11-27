"""Luật của cổng tự động merge (scripts/automerge_gate.py) trên các PR giả lập."""
from scripts.automerge_gate import check

REPO = "huynhquocthang1805/BigData"


def pr(base="main", head_repo=REPO, assoc="COLLABORATOR"):
    return {"base": {"ref": base}, "head": {"repo": {"full_name": head_repo} if head_repo else None},
            "author_association": assoc, "user": {"login": "member"}}


def f(name, status="added", patch="", prev=None):
    d = {"filename": name, "status": status, "patch": patch}
    if prev:
        d["previous_filename"] = prev
    return d


TV3_PR = [f("wq/evaluation.py"), f("wq/models/imputers_tv3.py"), f("tests/test_evaluation.py", patch="+def test_x():"),
          f("models/results_imputation.csv"), f("models/README.md", "modified"), f("benchmarks/bench_imputation_tv3.py"),
          f("docs/report/ch5_dien_khuyet_tv3.md"), f("docs/images/tv3_rmse_by_rate.png"), f("requirements.txt", "modified")]


def test_member_pr_with_own_files_passes():
    assert check(pr(), TV3_PR, {"docs/images/tv3_rmse_by_rate.png": 196_364}, REPO) == []


def test_wrong_base_fork_and_outsider_are_blocked():
    probs = check(pr(base="thang/TV1", head_repo="someone/BigData", assoc="CONTRIBUTOR"), [], {}, REPO)
    assert len(probs) == 3
    assert check(pr(head_repo=None), [], {}, REPO)  # fork đã bị xóa


def test_infrastructure_contract_and_task_file_are_blocked():
    for name in [".github/workflows/ci.yml", "docker-compose.yml", "streaming/stream_processor.py", "wq/contracts.py",
                 "docs/TEAM_TASKS.md", ".env.bigdata", "pytest.ini", "data/raw/x.csv"]:
        assert check(pr(), [f(name, "modified")], {}, REPO), name


def test_tv1_files_can_be_added_to_but_not_modified():
    assert check(pr(), [f("benchmarks/bench_new_tv4.py")], {}, REPO) == []
    assert check(pr(), [f("benchmarks/bench_large.py", "modified")], {}, REPO)
    assert check(pr(), [f("wq/models/base.py", "modified")], {}, REPO)
    assert check(pr(), [f("wq/models/my_base.py", "renamed", prev="wq/models/base.py")], {}, REPO)


def test_tests_cannot_be_deleted_renamed_or_skipped():
    assert check(pr(), [f("tests/test_models.py", "removed")], {}, REPO)
    assert check(pr(), [f("tests/test_new.py", "renamed", prev="tests/test_old.py")], {}, REPO)
    skip = "@@ -1 +1,2 @@\n+@pytest.mark.skip(reason='slow')\n def test_a(): pass"
    assert check(pr(), [f("tests/test_imputers_tv3.py", "modified", skip)], {}, REPO)
    assert check(pr(), [f("tests/conftest.py", "modified", "+    pytest.skip('x')")], {}, REPO)
    assert check(pr(), [f("tests/conftest.py", "modified", "+def new_fixture(): ...")], {}, REPO) == []
    # test MỚI được skip có điều kiện, ví dụ khi CI không có dữ liệu thật
    need_data = "+    if not lake:\n+        pytest.skip('Cần đường dẫn silver USGS')"
    assert check(pr(), [f("tests/test_imputers_tv3.py", "added", need_data)], {}, REPO) == []


def test_models_data_and_large_files_are_blocked():
    assert check(pr(), [f("models/imputer_kalman.joblib")], {}, REPO)
    assert check(pr(), [f("notebooks/tv4_out.parquet")], {}, REPO)
    assert check(pr(), [f("notebooks/tv2_eda.ipynb")], {"notebooks/tv2_eda.ipynb": 6 * 1024 * 1024}, REPO)
    assert check(pr(), [f("models/old.joblib", "removed")], {}, REPO) == []
