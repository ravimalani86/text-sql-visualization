from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logging() -> None:
    # Keep logs in a stable project-level location instead of relying on CWD.
    project_root = Path(__file__).resolve().parents[2]
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    log_path = logs_dir / "app.log"

    has_stdout_handler = any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
        for h in root.handlers
    )
    has_file_handler = any(
        isinstance(h, logging.FileHandler) and Path(getattr(h, "baseFilename", "")).resolve() == log_path.resolve()
        for h in root.handlers
    )

    if not has_stdout_handler:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        root.addHandler(stdout_handler)

    if not has_file_handler:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

