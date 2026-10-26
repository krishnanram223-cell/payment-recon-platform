"""Where data lives. One place to change when we move from local disk to ADLS."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def lakehouse(self) -> Path:
        return self.root / "lakehouse"

    @property
    def reference(self) -> Path:
        return self.root / "reference"

    def bronze(self, table: str) -> str:
        return str(self.lakehouse / "bronze" / table)

    def silver(self, table: str) -> str:
        return str(self.lakehouse / "silver" / table)

    def gold(self, table: str) -> str:
        return str(self.lakehouse / "gold" / table)

    def audit(self, table: str) -> str:
        return str(self.lakehouse / "audit" / table)


def default_paths() -> Paths:
    """Data directory from PAYRECON_DATA_DIR, defaulting to ./data."""
    return Paths(Path(os.environ.get("PAYRECON_DATA_DIR", "data")).resolve())
