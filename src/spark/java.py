"""Java runtime detection for PySpark (local execution)."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def ensure_java_home() -> str:
    """Set JAVA_HOME before importing PySpark if not already valid."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home and Path(java_home, "bin", "java").exists():
        return java_home

    java_bin = shutil.which("java")
    if java_bin:
        candidate = Path(java_bin).resolve().parent.parent
        if (candidate / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(candidate)
            return str(candidate)

    for candidate in (
        "/usr/lib/jvm/java-21-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-11-openjdk-amd64",
        "/usr/lib/jvm/default-java",
    ):
        if Path(candidate, "bin", "java").exists():
            os.environ["JAVA_HOME"] = candidate
            return candidate

    raise RuntimeError(
        "Java not found. Install OpenJDK 11+ and set JAVA_HOME "
        "(e.g. export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64)."
    )
