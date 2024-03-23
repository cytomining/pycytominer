import pytest
import warnings
from pycytominer.utils.warnings import alias_param


@pytest.mark.parametrize(
    "param_name,deprecate_warning,expected_warning",
    [
        ("param_original", True, False),
        ("param_original", False, False),
        ("param_alias", True, True),
        ("param_alias", False, False),
    ],
)
def test_alias_param(param_name, deprecate_warning, expected_warning):
    @alias_param("param_original", "param_alias", deprecate_warning=deprecate_warning)
    def test_func(positional_arg, *, param_original):
        return param_original

    # Set up kwargs with a the tested param name
    kwargs = {param_name: "test_value"}
    if expected_warning:
        with pytest.warns(DeprecationWarning):
            result = test_func("positional_arg", **kwargs)
            assert result == "test_value"
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = test_func("positional_arg", **kwargs)
            assert result == "test_value"
