"""Static consistency checks (no workspace needed):  python3 -m pytest tests -q"""
import csv, pathlib, subprocess, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_generated_jobs_are_current():
    out = subprocess.run([sys.executable, str(ROOT / "scripts/gen_jobs.py")], capture_output=True, text=True, check=True).stdout
    assert out == (ROOT / "resources/jobs.yml").read_text(), "run: python3 scripts/gen_jobs.py > resources/jobs.yml"


def test_assumption_rows_reference_real_jobs_and_steps():
    jobs = (ROOT / "resources/jobs.yml").read_text()
    notebooks = {p.stem: p.read_text() for p in (ROOT / "src").glob("*.py")}
    for row in csv.DictReader((ROOT / "docs/model_assumptions.csv").open()):
        assert f"\n    {row['job_key']}:" in jobs, row["job_key"]
        assert any(f'"{row["notebook_step"]}"' in src or f'f"{row["notebook_step"]}' in src for src in notebooks.values()), row["notebook_step"]


def test_every_notebook_parses():
    import ast
    for p in (ROOT / "src").glob("*.py"):
        ast.parse(p.read_text())
