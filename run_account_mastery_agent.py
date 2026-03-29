from __future__ import annotations

import os
import sys
import traceback
import pandas as pd

from reader_databricks_mastery import load_databricks_context
from rules_engine_mastery import build_summary, compute_score, evaluate_all
from writer_account_mastery import write_mastery_output

BASE = os.getenv('MASTERY_BASE', '/mnt/data')
TEMPLATE = os.getenv('MASTERY_TEMPLATE', os.path.join(BASE, 'CoE_Account_Mastery_Analysis_Templates.xlsm'))


def run_one(input_path: str, output_path: str):
    ctx = load_databricks_context(input_path)
    results = evaluate_all(ctx)
    summary = build_summary(ctx, results)
    penalty, score, grade, findings = compute_score(results)
    write_mastery_output(TEMPLATE, output_path, summary, results, penalty, score, grade, findings, ctx)
    return ctx, results, summary, penalty, score, grade, findings


def main():
    if len(sys.argv) > 1:
        files = [sys.argv[1]]
    else:
        files = [os.path.join(BASE, f) for f in os.listdir(BASE) if f.endswith('.xlsx') and 'Pre_Analysis_Dashboard' in f]

    rows = []
    failed = []

    for path in sorted(files):
        base = os.path.basename(path).replace('.xlsx', '')
        out = os.path.join(BASE, base.replace('seller_Pre_Analysis_Dashboard', 'seller_Account_Mastery_Analysis') + '.xlsm')
        try:
            ctx, _, _, _, score, grade, findings = run_one(path, out)
            top = ' | '.join([f"{x['cid']} {x['status']}: {x['what']}" for x in findings if x['status'] in ('FLAG', 'PARTIAL')][:5])
            rows.append({'account': ctx.hash_name, 'score': score, 'grade': grade, 'top_findings': top, 'output_file': os.path.basename(out)})
            print(f'{ctx.hash_name}: {score:.1f} — {grade}')
        except Exception as exc:
            print(f'[ERROR] {os.path.basename(path)}: {exc}\n{traceback.format_exc()}')
            failed.append({'file': os.path.basename(path), 'error': str(exc)})

    if rows:
        pd.DataFrame(rows).to_excel(os.path.join(BASE, 'mastery_raw_test_summary.xlsx'), index=False)
        pd.DataFrame(rows).to_csv(os.path.join(BASE, 'mastery_raw_test_summary.csv'), index=False)

    if failed:
        pd.DataFrame(failed).to_csv(os.path.join(BASE, 'mastery_failed_files.csv'), index=False)
        print(f'\n{len(failed)} file(s) failed. See mastery_failed_files.csv for details.')


if __name__ == '__main__':
    main()
