import pytest

from stock_analysis.settings import SQL_VARS, build_db_url, load_env

SQL_VALUES = {
    "SQL_DATABASE": "stockdb",
    "SQL_USER": "dbuser",
    "SQL_PASSWORD": "dbpass",
    "SQL_PORT": "5432",
    "SQL_HOST": "db.example.com",
}


def _clear_sql_env(monkeypatch):
    for var in SQL_VARS:
        monkeypatch.delenv(var, raising=False)


def test_load_env_returns_requested_vars(monkeypatch, tmp_path):
    _clear_sql_env(monkeypatch)
    for var, value in SQL_VALUES.items():
        monkeypatch.setenv(var, value)
    env_vars = load_env(dotenv_path=str(tmp_path / "missing.env"))
    assert env_vars == SQL_VALUES


def test_load_env_missing_var_raises(monkeypatch, tmp_path):
    _clear_sql_env(monkeypatch)
    for var in SQL_VARS[:-1]:
        monkeypatch.setenv(var, SQL_VALUES[var])
    with pytest.raises(ValueError, match="SQL_HOST"):
        load_env(dotenv_path=str(tmp_path / "missing.env"))


def test_load_env_reads_dotenv_file(monkeypatch, tmp_path):
    _clear_sql_env(monkeypatch)
    env_file = tmp_path / ".env"
    env_file.write_text("\n".join(f"{k}={v}" for k, v in SQL_VALUES.items()))
    env_vars = load_env(dotenv_path=str(env_file))
    assert env_vars == SQL_VALUES


def test_build_db_url_default_driver():
    url = build_db_url(SQL_VALUES)
    assert url == "postgresql+psycopg2://dbuser:dbpass@db.example.com:5432/stockdb"


def test_build_db_url_custom_driver():
    url = build_db_url(SQL_VALUES, driver="asyncpg")
    assert url == "postgresql+asyncpg://dbuser:dbpass@db.example.com:5432/stockdb"
