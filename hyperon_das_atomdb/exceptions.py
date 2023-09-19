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


class NodeDoesNotExistException(BaseException):
    ...  # pragma no cover


class LinkDoesNotExistException(BaseException):
    ...  # pragma no cover


class AtomDoesNotExistException(BaseException):
    ...  # pragma no cover


class AddNodeException(BaseException):
    ...  # pragma no cover


class AddLinkException(BaseException):
    ...  # pragma no cover
