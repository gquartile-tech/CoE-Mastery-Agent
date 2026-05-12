"""
CoE Account Mastery Analysis Tool — Flask backend
Run:  py app.py
Open: http://127.0.0.1:8503
"""

from __future__ import annotations

import os
import sys
import traceback
import re
from datetime import datetime
from pathlib import Path

from flask import Flask, request, jsonify, send_file, render_template, Response
from werkzeug.utils import secure_filename

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent.resolve()
UPLOAD_DIR    = BASE_DIR / "uploads"
OUTPUT_DIR    = BASE_DIR / "outputs"
TEMPLATE_FILE = BASE_DIR / "CoE_Account_Mastery_Analysis_Templates.xlsm"

UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Import analysis modules ───────────────────────────────────────────────────
sys.path.insert(0, str(BASE_DIR))

from reader_databricks_mastery import load_databricks_context
from rules_engine_mastery import evaluate_all, build_summary, compute_score
from writer_account_mastery import write_mastery_output

MIN_OUTPUT_BYTES = 5_000

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024


def _safe_fn(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[^a-zA-Z0-9 \-_]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "UNKNOWN_ACCOUNT"


def run_full_analysis(input_path: str) -> dict:
    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(f"Template not found: {TEMPLATE_FILE}")

    ctx = load_databricks_context(input_path)
    hash_name = getattr(ctx, "hash_name", "") or "UNKNOWN_ACCOUNT"
    safe_hash = _safe_fn(hash_name)

    results = evaluate_all(ctx)
    summary = build_summary(ctx, results)
    penalty, score, grade, findings = compute_score(results)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    download_name = f"{safe_hash} - Account Mastery Analysis - {ts}.xlsm"
    download_path = OUTPUT_DIR / download_name

    write_mastery_output(
        template_path=str(TEMPLATE_FILE),
        output_path=str(download_path),
        summary=summary,
        results=results,
        penalty=penalty,
        score=score,
        grade=grade,
        findings=findings,
        ctx=ctx,
    )

    size = download_path.stat().st_size if download_path.exists() else 0
    print(f"  Output written: {download_path} ({size} bytes)")

    if not download_path.exists() or size < MIN_OUTPUT_BYTES:
        raise RuntimeError(f"Output file missing or too small ({size} bytes).")

    ok_count      = sum(1 for r in results.values() if r.status == "OK")
    flag_count    = sum(1 for r in results.values() if r.status == "FLAG")
    partial_count = sum(1 for r in results.values() if r.status == "PARTIAL")

    return {
        "download_filename": download_name,
        "account":           hash_name,
        "window":            f"{ctx.window_start} to {ctx.window_end}" if ctx.window_start and ctx.window_end else "",
        "ref_date":          str(ctx.ref_date or ""),
        "downloaded":        str(ctx.downloaded or ""),
        "score":             round(score, 1),
        "grade":             grade,
        "ok":                ok_count,
        "flag":              flag_count,
        "partial":           partial_count,
        "flag_ids":          [cid for cid, r in results.items() if r.status == "FLAG"],
        "partial_ids":       [cid for cid, r in results.items() if r.status == "PARTIAL"],
    }


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400
    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify({"error": "No file selected."}), 400
    _, ext = os.path.splitext(uploaded.filename.lower())
    if ext not in {".xlsx", ".xlsm"}:
        return jsonify({"error": "Only .xlsx or .xlsm files accepted."}), 400

    safe_name  = secure_filename(uploaded.filename)
    input_path = str(UPLOAD_DIR / safe_name)
    uploaded.save(input_path)

    try:
        info = run_full_analysis(input_path)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Analysis failed: {e}"}), 500

    info["download_url"] = f"/download/{info['download_filename']}"
    return jsonify(info)


@app.route("/download/<path:filename>")
def download(filename):
    from urllib.parse import unquote
    filename = unquote(filename)
    p = OUTPUT_DIR / filename

    if not p.exists():
        xlsm_files = sorted(OUTPUT_DIR.glob("*.xlsm"), key=lambda f: f.stat().st_mtime, reverse=True)
        if xlsm_files:
            p = xlsm_files[0]
            filename = p.name
            print(f"  Fallback download: {filename}")
        else:
            return f"No output files found in {OUTPUT_DIR}", 404

    print(f"  Serving download: {p} ({p.stat().st_size} bytes)")
    data = p.read_bytes()
    return Response(
        data,
        mimetype="application/vnd.ms-excel.sheet.macroEnabled.12",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(data)),
        }
    )


@app.route("/favicon.ico")
def favicon():
    return "", 204


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n  CoE Account Mastery Analysis Tool")
    print("  ─────────────────────────────────────────────────")
    print(f"  Template : {TEMPLATE_FILE}")
    print(f"  Template exists: {TEMPLATE_FILE.exists()}")
    print(f"  Outputs  : {OUTPUT_DIR}")
    print("  Open → http://127.0.0.1:8503\n")
    app.run(host="127.0.0.1", port=8503, debug=True)
