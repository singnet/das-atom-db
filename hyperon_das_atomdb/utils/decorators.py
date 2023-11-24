import time
from datetime import datetime
from functools import wraps
from typing import Callable

from hyperon_das_atomdb.exceptions import (
    ConnectionServerException,
    RetryException,
)


def retry(attempts: int, timeout_seconds: int):
    def decorator(function: Callable) -> Callable:
        @wraps(function)
        def wrapper(*args, **kwargs):
            waiting_time_seconds = 0.2
            retry_count = 0
            timer_count = 0

            while retry_count < attempts and timer_count < timeout_seconds:
                try:
                    start_time = datetime.now()
                    response = function(*args, **kwargs)
                    end_time = datetime.now()
                    if response is not None:
                        return response
                except Exception as e:
                    raise ConnectionServerException(
                        message="An error occurs while connecting to the server",
                        details=str(e),
                    )
                else:
                    time.sleep(waiting_time_seconds)
                    retry_count += 1
                    timer_count += int((end_time - start_time).total_seconds())

            raise RetryException(
                message='The number of attempts has been exceeded or a timeout has occurred',
                details={
                    'attempts': retry_count,
                    'time': f'{timer_count} seconds',
                },
            )

        return wrapper

    return decorator
