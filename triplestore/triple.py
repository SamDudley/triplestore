from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Triple:
    source: str
    predicate: str
    target: str
    weight: float
