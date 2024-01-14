import functools

from validation_error import ValidationError


def precondition(predicate, msg, exception=ValidationError):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if predicate(*args, **kwargs):
                return func(*args, **kwargs)
            else:
                raise exception(msg)

        return wrapper

    return decorator
