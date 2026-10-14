"""
Utility functions for deprecating parts of the Pycytominer API
"""

import warnings
from functools import wraps
from typing import Any, Callable


def deprecate_renamed_parameter(
    old_name: str, new_name: str
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a function to keep accepting a renamed keyword argument.

    Calls that still pass ``old_name`` as a keyword are forwarded to ``new_name``
    and emit a :class:`DeprecationWarning`. Positional calls are unaffected.

    Parameters
    ----------
    old_name : str
        Deprecated keyword argument name.
    new_name : str
        Keyword argument name that replaces ``old_name``.

    Returns
    -------
    Callable
        Decorator that maps ``old_name`` to ``new_name`` on the wrapped function.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # wraps the function to preserve docstring and function name
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            if old_name in kwargs:
                if new_name in kwargs:
                    raise TypeError(
                        f"{func.__name__}() received both `{old_name}` and "
                        f"`{new_name}`. Use `{new_name}` only."
                    )

                warnings.warn(
                    f"The `{old_name}` parameter in {func.__name__}() is deprecated "
                    f"and will be removed in a future release. Use `{new_name}` "
                    "instead.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                kwargs[new_name] = kwargs.pop(old_name)

            return func(*args, **kwargs)

        return wrapper

    return decorator
