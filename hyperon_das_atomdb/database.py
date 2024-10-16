from enum import Enum
from typing import TypeAlias

from hyperon_das_atomdb_cpp import (  # type: ignore[attr-defined]
    TYPE_HASH,
    TYPEDEF_MARK_HASH,
    WILDCARD,
    WILDCARD_HASH,
    AtomDB,
    FieldIndexType,
)
from hyperon_das_atomdb_cpp.document_types import Atom as cppAtom
from hyperon_das_atomdb_cpp.document_types import Link as cppLink
from hyperon_das_atomdb_cpp.document_types import Node as cppNode

# pylint: disable=invalid-name

AtomT: TypeAlias = cppAtom
NodeT: TypeAlias = cppNode
LinkT: TypeAlias = cppLink

HandleT: TypeAlias = str

HandleListT: TypeAlias = list[HandleT]

HandleSetT: TypeAlias = set[HandleT]

IncomingLinksT: TypeAlias = HandleListT | list[AtomT]

# pylint: enable=invalid-name


class FieldNames(str, Enum):
    """Enumeration of field names used in the AtomDB."""

    ID_HASH = "_id"
    COMPOSITE_TYPE = "composite_type"
    COMPOSITE_TYPE_HASH = "composite_type_hash"
    NODE_NAME = "name"
    TYPE_NAME = "named_type"
    TYPE_NAME_HASH = "named_type_hash"
    KEY_PREFIX = "key"
    KEYS = "keys"
    IS_TOPLEVEL = "is_toplevel"
    TARGETS = "targets"
    CUSTOM_ATTRIBUTES = "custom_attributes"


__all__ = [
    "FieldNames",
    "FieldIndexType",
    "AtomDB",
    "WILDCARD",
    "WILDCARD_HASH",
    "TYPE_HASH",
    "TYPEDEF_MARK_HASH",
    "AtomT",
    "NodeT",
    "LinkT",
    "HandleT",
    "HandleListT",
    "HandleSetT",
    "IncomingLinksT",
]
