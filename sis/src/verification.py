"""Stage 7 - pipeline verification: compares record counts across all stages and explains the differences."""
from __future__ import annotations

from .config import DOCS_DIR, ensure_dirs
from .report import load_report


def build_verification() -> tuple[list[tuple[str, object, str]], bool]:
    r = load_report()
    c, p, k = r.get("collect", {}), r.get("produce", {}), r.get("consume", {})
    v, cl, ld = r.get("validate", {}), r.get("clean", {}), r.get("load", {})
    tests = p.get("test_records_sent", 0)

    rows = [
        ("1. Collected from API", c.get("total_records"), "raw_data.csv, saved before any cleaning"),
        ("2. Published to Kafka", p.get("published"), f"collected + {tests} deliberately invalid test records"),
        ("3. Consumed from Kafka", k.get("consumed"), "must equal published (stop reason: %s)" % k.get("stop_reason")),
        ("4. Valid (Pandera)", v.get("valid"), "consumed - invalid"),
        ("5. Invalid (Pandera)", v.get("invalid"), f"{v.get('test_records_detected')} of {v.get('test_records')} test records detected, "
                                                   f"{v.get('invalid_real_records')} real invalid"),
        ("6. After cleaning", cl.get("output_rows"), f"valid - {cl.get('duplicates_removed')} duplicates - {cl.get('filtered_out')} filtered"),
        ("7. Stored in SQLite", ld.get("stored_records"), "must equal cleaned; second load added 0 rows"),
    ]
    ok = all([
        p.get("published") == (c.get("total_records") or 0) + tests,
        k.get("consumed") == p.get("published"),
        v.get("test_records_detected") == v.get("test_records"),
        v.get("valid") == (k.get("consumed") or 0) - (v.get("invalid") or 0),
        cl.get("output_rows") == ld.get("stored_records"),
        ld.get("idempotent") is True,
        ld.get("duplicate_keys") == 0,
    ])
    return rows, bool(ok)


def run() -> bool:
    ensure_dirs()
    rows, ok = build_verification()
    width = max(len(r[0]) for r in rows)
    lines = ["# Pipeline verification", "", "| Stage | Records | Explanation |", "|---|---:|---|"]
    print("\n=== Pipeline verification (record counts) ===")
    for name, value, note in rows:
        print(f"{name:{width}s} {str(value):>8s}   {note}")
        lines.append(f"| {name} | {value} | {note} |")
    verdict = "ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED - see counts above"
    print(f"\n{verdict}")
    lines += ["", f"**{verdict}**"]
    (DOCS_DIR / "verification_report.md").write_text("\n".join(lines), encoding="utf-8")
    return ok
