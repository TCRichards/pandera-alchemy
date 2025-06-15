from __future__ import annotations

from dataclasses import dataclass
from typing import Type, Union

import pandas as pd
import pandera as pa
import psycopg2
import sqlalchemy

from pandera_alchemy.exceptions import NoSuchTableError, SchemaValidationError
from pandera_alchemy.logger import logger
from pandera_alchemy.unified_types import NoneType, UnifiedColumn, get_unified_type


@dataclass
class Table:
    """A wrapper around a Pandera schema that can validate the structure of a table in a database."""

    def __init__(
        self,
        name: str,
        db_schema: str,
        table_schema: Union[Type[pa.DataFrameModel], pa.DataFrameSchema],
        print_schema: bool = True,
    ):
        """Initialize the Table object.

        Args:
            name: The name of the table in the database.
            db_schema: Which database schema the table resides in (e.g. "powin", "goblintest01", "analytics").
            table_schema: The Pandera schema representing the structure of the table.
            print_schema: Whether to include the schema or just the name in the table's string representation.
        """
        self.name = name
        self.db_schema = db_schema
        self.table_schema = table_schema
        self.print_schema = print_schema

    def validate(
        self, engine: sqlalchemy.engine.base.Engine, check_nullable: bool = False
    ) -> Table:
        """Validate that the structure of the table in the database matches the Pandera schema.

        Raises a SchemaValidationError if the table structure does not match the Pandera schema.
        Raises a NoSuchTableError if the table does not exist in the database.

        Args:
            engine: The SQLAlchemy engine to use to query the database.
            check_nullable: Whether to check if the Pandera/DB columns have matching nullability.
                            Defaults to False, as Pandera and SQLAlchemy have different default nullability.
                            Pandera defaults to nullable=False, while SQLAlchemy defaults to nullable=True.
                            Using this requires a more verbose Pandera schema which may or may not be desired.

        Returns:
            Itself, for chaining.
        """
        unified_pandera_cols = self._get_unified_pandera_cols()
        unified_db_cols = self._get_unified_db_cols(engine)
        self._raise_errors_if_columns_dont_match(unified_pandera_cols, unified_db_cols, check_nullable)
        return self

    def _get_unified_pandera_cols(self) -> list[UnifiedColumn]:
        """Get the desired columns of the table from the Pandera schema in Unified format."""
        table_schema = (
            self.table_schema
            if isinstance(self.table_schema, pa.DataFrameSchema)
            else self.table_schema.to_schema()
        )

        unified_pa_cols = []
        for col_name, col in table_schema.columns.items():
            unified_pa_cols.append(
                UnifiedColumn(
                    name=col_name,
                    dtype=get_unified_type(col.dtype),
                    nullable=col.properties["nullable"],
                )
            )
        return unified_pa_cols

    def _get_unified_db_cols(
        self, engine: Union[sqlalchemy.engine.base.Engine]
    ) -> list[UnifiedColumn]:
        """Get the actual columns of the table from the database in Unified format."""
        db_columns = self._get_db_table_columns(engine)

        unified_db_cols = []
        for col in db_columns:
            # SQLAlchemy also tracks autoincrement, which we could add later on
            unified_db_cols.append(
                UnifiedColumn(
                    name=col["name"],
                    dtype=get_unified_type(col["type"]),
                    nullable=col["nullable"],
                )
            )
        return unified_db_cols

    def _get_db_table_columns(self, engine: sqlalchemy.engine.base.Engine) -> list[dict]:
        """Use SQLAlchemy inspector to get the information about table columns from the database.

        This works best for permanent tables.
        For temporary tables, we infer the types from a sample query which is less robust and slower.

        Returns:
            A list of dictionaries with the following keys: name, type, nullable
        """
        if self.db_schema != "temp":
            # By default, use the SQLAlchemy inspector which provides a fast and robust way to get column details
            inspector = sqlalchemy.inspect(engine)
            if inspector is not None:
                try:
                    return inspector.get_columns(self.name, schema=self.db_schema)
                except psycopg2.errors.UndefinedTable:
                    raise NoSuchTableError(f"Table {self.name} in schema {self.db_schema} not found")

        """For temporary tables, we need to infer the types from a sample query.

        TODO: This is hacky and slow, but I've tried all sorts of methods that don't work.
        The problem is that Redshift stores temporary tables in an inaccessible schema
        which isn't visible to the inspector or in the information schema.
        The temporary table doesn't even show up in inspector.has_table() or inspector.get_temp_table_names().
        So just query a single row and infer the types from the resulting DataFrame
        """
        df = pd.read_sql(f"SELECT * FROM {self.name} LIMIT 1", engine)
        temp_table_col_info = []
        for col in df.columns:
            # Pandas uses the "object" dtype to represent strings it's not sure about
            # Sometimes these are strings, other times they're datetimes treated as strings...
            # For now, just avoid doing anything with them by marking as None

            # TODO: Break this out into a separate function
            if pd.api.types.is_object_dtype(df[col]):
                mapped_type = None
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                mapped_type = sqlalchemy.DateTime
            elif pd.api.types.is_integer_dtype(df[col]):
                mapped_type = sqlalchemy.Integer
            elif pd.api.types.is_float_dtype(df[col]):
                mapped_type = sqlalchemy.Float
            elif pd.api.types.is_bool_dtype(df[col]):
                mapped_type = sqlalchemy.Boolean
            elif pd.api.types.is_timedelta64_dtype(df[col]):
                mapped_type = sqlalchemy.Interval
            else:
                mapped_type = df[col]

            temp_table_col_info.append(
                {
                    "name": col,
                    "type": mapped_type,
                    "nullable": None,  # We can't infer nullability from a single row
                }
            )

        return temp_table_col_info

    def _raise_errors_if_columns_dont_match(
        self,
        unified_pandera_cols: list[UnifiedColumn],
        unified_db_cols: list[UnifiedColumn],
        check_nullable: bool,
    ):
        """Verify that the columns match between Pandera and SQLAlchemy, and indicate mismatches.

        Args:
            unified_pandera_cols: The columns from the Pandera schema in Unified format.
            unified_db_cols: The columns from the database in Unified format.
            check_nullable: Whether to check if the Pandera/DB columns have matching nullability.
                            Defaults to False, as Pandera and SQLAlchemy have different default nullability.
                            Pandera defaults to nullable=False, while SQLAlchemy defaults to nullable=True.
                            Using this requires a more verbose Pandera schema which may or may not be desired.

        Returns:
            Whether all columns match between the Pandera schema and the table.
        """
        if len(unified_pandera_cols) != len(unified_db_cols):
            raise SchemaValidationError(
                f"Schema validation failed for table {self.name}: "
                f"Number of columns in Pandera schema ({len(unified_pandera_cols)}) "
                f"does not match number of columns in database ({len(unified_db_cols)}).\n"
                f"Pandera columns: {unified_pandera_cols}\nDatabase columns: {unified_db_cols}"
            )

        mismatch_messages = self._get_column_mismatch_messages(
            unified_pandera_cols, unified_db_cols, check_nullable
        )
        if mismatch_messages:
            mismatch_str = "\n\t- ".join(mismatch_messages)
            raise SchemaValidationError(
                f"Schema validation failed for table {self.name}:\n\t- {mismatch_str}"
            )
        else:
            logger.debug(f"Schema validation passed for table {self.name}")

    def _get_column_mismatch_messages(
        self,
        unified_pandera_cols: list[UnifiedColumn],
        unified_db_cols: list[UnifiedColumn],
        check_nullable: bool,
    ) -> list[str]:
        mismatch_messages = []

        for pandera_col in unified_pandera_cols:
            if pandera_col in unified_db_cols:
                # Check if the column is in the database with all matching attributes
                continue

            db_col_match = [c for c in unified_db_cols if c.name == pandera_col.name]
            if db_col_match:
                matching_db_col = db_col_match[0]
                if matching_db_col.dtype == NoneType:
                    # NoneType is a placeholder for when we're not sure about the type, so don't
                    # fail the validation if we encounter it.  This should only happen for temporary tables
                    # where types are inferred from a sample query rather than the database schema.
                    continue
                if pandera_col.dtype != matching_db_col.dtype:
                    mismatch_messages.append(
                        f"Column {pandera_col.name} dtype in the database does not match the Pandera schema: "
                        f"Pandera dtype={pandera_col.dtype}, Database dtype={matching_db_col.dtype}"
                    )
                elif (
                    check_nullable
                    and pandera_col.nullable is not None
                    and matching_db_col.nullable is not None
                    and pandera_col.nullable != matching_db_col.nullable
                ):
                    mismatch_messages.append(
                        f"Column {pandera_col.name} has nullable={pandera_col.nullable} "
                        f"in the schema but nullable={matching_db_col.nullable} in the database"
                    )
            else:
                mismatch_messages.append(f"No column with name {pandera_col.name} found in the database")

        # Also need to check for columns in the database that are not in the Pandera schema
        for db_col in unified_db_cols:
            if not any(pa_col.name == db_col.name for pa_col in unified_pandera_cols):
                mismatch_messages.append(f"Column {db_col} in the database is not found in schema")

        return mismatch_messages

    def __repr__(self):
        if not self.print_schema or self.db_schema == "temp":
            # Temporary tables don't have a schema
            return self.name
        return f"{self.db_schema}.{self.name}"
