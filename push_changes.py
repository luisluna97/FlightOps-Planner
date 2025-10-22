"""
Helper script to stage, commit, and push repository changes.

Usage:
    python push_changes.py "Commit message"

When the commit message is omitted, a timestamped default is used.
"""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
from typing import Sequence


def run(command: Sequence[str]) -> None:
    """Run a git command and raise a clear error when it fails."""
    result = subprocess.run(command, check=False, text=True)
    if result.returncode != 0:
        raise SystemExit(f"Command failed ({' '.join(command)})")


def main(argv: Sequence[str]) -> None:
    message = (
        argv[1]
        if len(argv) > 1
        else f"Update FlightOps assets ({dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC)"
    )

    status = subprocess.run(
        ["git", "status", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    if status.returncode != 0:
        raise SystemExit("git status failed.")

    if not status.stdout.strip():
        print("Nothing to commit.")
        return

    run(["git", "add", "-A"])
    run(["git", "commit", "-m", message])
    run(["git", "push"])
    print("Changes pushed successfully.")


if __name__ == "__main__":
    main(sys.argv)
