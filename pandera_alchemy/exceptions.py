"""Custom exceptions for the pandera_alchemy package.

These exceptions are derived from Pandera and SQLAlchemy exceptions as well as
a custom SchemaValidationError class so they can be easily caught and handled.
"""

import sqlalchemy.exc


class SchemaValidationError(Exception):
    """Raised when a Pandera schema does not match a database table."""

    ...


class NoSuchTableError(SchemaValidationError, sqlalchemy.exc.NoSuchTableError):
    """Raised when a table does not exist in the database."""

    ...
