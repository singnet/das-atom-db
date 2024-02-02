from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class Link:
    arity_1: Dict[str, Any]
    arity_2: Dict[str, Any]
    arity_n: Dict[str, Any]

    def get_table(self, arity: int):
        if arity == 1:
            return self.arity_1
        if arity == 2:
            return self.arity_2
        if arity > 2:
            return self.arity_n

    def all_tables(self) -> List[Dict[str, Any]]:
        return [self.arity_1, self.arity_2, self.arity_n]


@dataclass
class Database:
    atom_type: Dict[str, Any]
    node: Dict[str, Any]
    link: Link
    outgoing_set: Dict[str, Any]
    incoming_set: Dict[str, Any]
    patterns: Dict[str, List[Tuple]]
    templates: Dict[str, List[Tuple]]
