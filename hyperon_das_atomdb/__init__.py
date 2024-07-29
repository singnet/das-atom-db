"""This module initializes the AtomDB package and imports key components."""

from .database import UNORDERED_LINK_TYPES, WILDCARD, AtomDB
from .exceptions import AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist

__all__ = [
    'AtomDB',
    'WILDCARD',
    'UNORDERED_LINK_TYPES',
    'NodeDoesNotExist',
    'LinkDoesNotExist',
    'AtomDoesNotExist',
]

__version__ = '0.7.0'
