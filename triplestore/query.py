from typing import Optional

from dataclasses import dataclass


@dataclass
class Clause:
    type: Optional[str] = None
    value: Optional[str] = None


@dataclass
class Query:
    source: Clause = Clause()
    predicate: Clause = Clause()
    target: Clause = Clause()
    weight: Clause = Clause()
    # 0 = no limit
    limit: int = 0
