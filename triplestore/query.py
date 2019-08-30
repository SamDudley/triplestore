from typing import Optional

from dataclasses import dataclass
from enum import Enum


class Type(Enum):
    EQ = '='


@dataclass
class Clause:
    type: Optional[Type] = None
    value: Optional[str] = None

    def is_any(self) -> bool:
        return self.type is None and self.value is None


@dataclass
class Query:
    source: Clause = Clause()
    predicate: Clause = Clause()
    target: Clause = Clause()
    weight: Clause = Clause()
    # 0 = no limit
    limit: int = 0

    def is_any(self) -> bool:
        return all([
            self.source.is_any(),
            self.predicate.is_any(),
            self.target.is_any(),
            self.weight.is_any()
        ])
