import psycopg2
import logging
from app.core.settings.base import settings

__all__ = ["connect"]

CREDS = {
    "host": settings.DB_HOST,
    "database": settings.DB_NAME,
    "user": settings.DB_USER,
    "password": settings.DB_PASSWORD,
    "port": settings.DB_PORT,
}


def connect() -> psycopg2.extensions.connection:
    """
    Connect to the PostgreSQL database server.
    """
    logging.info(f'Connecting to {CREDS.get("database")} database...')
    conn = psycopg2.connect(**CREDS)
    logging.info(f'Connection to {CREDS.get("database")} database successful!')
    return conn
