"""Tiny helper that collects per-stage counters into data/pipeline_report.json (used for stage 7 verification)."""
from __future__ import annotations

import json
from datetime import datetime

from .config import SETTINGS, ensure_dirs


def load_report() -> dict:
    if SETTINGS.report_json.exists():
        try:
            return json.loads(SETTINGS.report_json.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def update_stage(stage: str, values: dict) -> None:
    ensure_dirs()
    report = load_report()
    report[stage] = {**values, "updated_at": datetime.now().isoformat(timespec="seconds")}
    SETTINGS.report_json.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def reset_report() -> None:
    if SETTINGS.report_json.exists():
        SETTINGS.report_json.unlink()
