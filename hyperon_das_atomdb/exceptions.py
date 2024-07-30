"""Custom exceptions for Atom DB"""


class AtomDbBaseException(Exception):
    """
    Base class for Atom DB exceptions
    """

    def __init__(self, message: str, details: str = ""):
        self.message = message
        self.details = details

        super().__init__(self.message, self.details)


class ConnectionMongoDBException(AtomDbBaseException):
    """Exception raised for errors in the connection to MongoDB."""


class AtomDoesNotExist(AtomDbBaseException):
    """Exception raised when an atom does not exist."""


class AddNodeException(AtomDbBaseException):
    """Exception raised when adding a node fails."""


class AddLinkException(AtomDbBaseException):
    """Exception raised when adding a link fails."""


class InvalidOperationException(AtomDbBaseException):
    """Exception raised for invalid operations."""


class RetryException(AtomDbBaseException):
    """Exception raised for retryable errors."""


class InvalidAtomDB(AtomDbBaseException):
    """Exception raised for invalid Atom DB operations."""


class InvalidSQL(AtomDbBaseException):
    """Exception raised for invalid SQL operations."""
