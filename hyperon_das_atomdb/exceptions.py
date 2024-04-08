class BaseException(Exception):
    """
    Base class to exceptions
    """

    def __init__(self, message: str, details: str = ""):
        self.message = message
        self.details = details

        super().__init__(self.message, self.details)


class ConnectionMongoDBException(BaseException):
    ...  # pragma no cover


class NodeDoesNotExist(BaseException):
    ...  # pragma no cover


class LinkDoesNotExist(BaseException):
    ...  # pragma no cover


class AtomDoesNotExist(BaseException):
    ...  # pragma no cover


class AddNodeException(BaseException):
    ...  # pragma no cover


class AddLinkException(BaseException):
    ...  # pragma no cover


class InvalidOperationException(BaseException):
    ...  # pragma no cover


class RetryException(BaseException):
    ...  # pragma no cover


class InvalidAtomDB(BaseException):
    ...  # pragma no cover


class InvalidSQL(BaseException):
    ...  # pragma no cover
