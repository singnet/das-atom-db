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

    def all_tables(self) -> Dict[str, Any]:
        all_arities = {}
        all_arities.update(self.arity_1)
        all_arities.update(self.arity_2)
        all_arities.update(self.arity_n)
        return all_arities


@dataclass
class Database:
    atom_type: Dict[str, Any]
    node: Dict[str, Any]
    link: Link
    outgoing_set: Dict[str, Any]
    incomming_set: Dict[str, Any]
    patterns: Dict[str, List[Tuple]]
    templates: Dict[str, List[Tuple]]
    names: Dict[str, str]
