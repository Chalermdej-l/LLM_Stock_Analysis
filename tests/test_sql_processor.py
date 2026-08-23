from types import SimpleNamespace

import pytest
from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, String, VARCHAR

from stock_analysis.helper.sql_processor import CloudSQLDatabase


@pytest.mark.parametrize(
    ("dtype", "big_flag", "expected"),
    [
        ("int64", False, Integer),
        ("int64", True, BigInteger),
        ("Int64", False, Integer),
        ("Int64", True, BigInteger),
        ("float64", False, Float),
        ("float64", True, Float),
        ("object", False, VARCHAR),
        ("object", True, VARCHAR),
        ("bool", False, Boolean),
        ("bool", True, Boolean),
        ("datetime64", False, DateTime),
        ("datetime64", True, DateTime),
        ("category", False, String),
        ("category", True, String),
    ],
)
def test_get_sqlalchemy_type(dtype, big_flag, expected):
    stub = SimpleNamespace(big_flag=big_flag)
    assert CloudSQLDatabase._get_sqlalchemy_type(stub, dtype) is expected
