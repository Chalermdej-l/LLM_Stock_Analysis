import os
from typing import Dict, List
from dotenv import load_dotenv

SQL_VARS = ["SQL_DATABASE", "SQL_USER", "SQL_PASSWORD", "SQL_PORT", "SQL_HOST"]


def load_env(required_vars: List[str] = None, dotenv_path: str = "./.env") -> Dict[str, str]:
    """
    Load the .env file and return the requested environment variables.

    Raises:
        ValueError: If any required environment variable is missing or empty.
    """
    load_dotenv(dotenv_path)
    if required_vars is None:
        required_vars = SQL_VARS
    env_vars = {var: os.getenv(var) for var in required_vars}

    missing = [var for var, value in env_vars.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return env_vars


def build_db_url(env_vars: Dict[str, str], driver: str = "psycopg2") -> str:
    """Build a SQLAlchemy PostgreSQL URL from the SQL_* environment variables."""
    return (
        f"postgresql+{driver}://{env_vars['SQL_USER']}:{env_vars['SQL_PASSWORD']}"
        f"@{env_vars['SQL_HOST']}:{env_vars['SQL_PORT']}/{env_vars['SQL_DATABASE']}"
    )
