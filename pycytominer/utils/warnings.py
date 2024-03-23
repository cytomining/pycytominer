from typing import Callable
import warnings

import functools


def alias_param(
    param_name: str, param_alias: str, deprecate_warning: bool = False
) -> Callable:
    """
    Decorator for aliasing a param in a function.

    This decorator will allow a function to accept a parameter under a different name.
    It can also raise a deprecation warning when the alias is used to provide advance notice
    to users that the alias will be removed in a future release.

    Args:
        param_name: name of param in function to alias
        param_alias: alias that can be used for this param
        deprecate_warning: whether to raise a deprecation warning when using the alias
    Returns:
    """

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            alias_param_value = kwargs.get(param_alias)
            if alias_param_value:
                if deprecate_warning:
                    warnings.warn(
                        f"Parameter {param_alias} is deprecated and will be removed in a future release. Please use {param_name} instead.",
                        DeprecationWarning,
                    )
                kwargs[param_name] = alias_param_value
                del kwargs[param_alias]
            result = func(*args, **kwargs)
            return result

        return wrapper

    return decorator
