import datetime as dt

import pandas as pd
import pandera as pa
import pytest
import pandera_alchemy.exceptions
import sqlalchemy
from pandera_alchemy import Table
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer("postgres:16", dbname="DATAMART") as postgres:
        postgres.start()
        yield postgres


def create_table_from_dataframe(
    postgres: PostgresContainer,
    df: pd.DataFrame,
    table_name: str,
    db_schema: str,
) -> sqlalchemy.engine.base.Engine:
    """Set up a testcase by loading a DataFrame into a table in a Postgres container."""
    engine: sqlalchemy.engine.base.Engine = sqlalchemy.create_engine(postgres.get_connection_url())  # type: ignore

    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")
        df.to_sql(name=table_name, con=conn, schema=db_schema, index=False, if_exists="replace")

    return engine


@pytest.mark.parametrize(
    ("column_type", "column_values"),
    [
        (pa.Bool, [True, False]),
        (pa.Int, [1, 2, 3]),
        (pa.Float, [1.1, 2.2, 3.3]),
        (pa.String, ["hello", "world"]),
        (pa.DateTime, [pd.Timestamp("2021-01-01"), pd.Timestamp("2021-01-02")]),
        (pa.DateTime, [dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1)]),
        (pa.Date, [dt.datetime(2021, 1, 1).date(), dt.datetime(2021, 1, 1).date()]),
        # TODO: Timedeltas are getting stored in the database as integers
        # (pa.Timedelta, [pd.Timedelta("1 days"), pd.Timedelta("2 days")]),
        # (pa.Timedelta, [dt.timedelta(days=1), dt.timedelta(days=2)]),
    ],
)
def test_single_column_valid_dtype(postgres: PostgresContainer, column_type, column_values: list):
    class ValidSchema(pa.DataFrameModel):
        column: column_type

    data = {"column": column_values}
    df = pd.DataFrame(data)

    table_name = "example_table"
    db_schema = "public"
    engine = create_table_from_dataframe(postgres, df, table_name, db_schema)

    example_table = Table(name=table_name, db_schema=db_schema, table_schema=ValidSchema)
    example_table.validate(engine)


@pytest.mark.parametrize(
    ("column_type", "column_values"),
    (
        (pa.Bool, ["True", "False"]),
        (pa.Int, [1.0, 2.0, 3.0]),
        (pa.Int, ["1", "2", "3"]),
        (pa.Float, [1, 2, 3]),
        (pa.Float, ["1.1", "2.2", "3.3"]),
        (pa.String, [True]),
        (pa.String, [17]),
        (pa.DateTime, ["2021-01-01 00:00", "2021-01-02 00:00"]),
        (pa.DateTime, [dt.datetime(2021, 1, 1).date(), dt.datetime(2021, 1, 1).date()]),
        (pa.Date, [dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1)]),
        (pa.Date, ["2021-01-01", "2021-01-02"]),
        (pa.Timedelta, ["1 days", "2 days"]),
    ),
)
def test_single_column_invalid_dtype(postgres: PostgresContainer, column_type, column_values: list):
    table_name = "example_table"
    db_schema = "public"

    class InvalidSchema(pa.DataFrameModel):
        column: column_type

    data = {"column": column_values}
    df = pd.DataFrame(data)

    table_name = "example_table"
    db_schema = "public"
    engine = create_table_from_dataframe(postgres, df, table_name, db_schema)

    example_table = Table(name=table_name, db_schema=db_schema, table_schema=InvalidSchema)
    with pytest.raises(pandera_alchemy.exceptions.SchemaValidationError):
        example_table.validate(engine)


@pytest.mark.parametrize(
    ("is_nullable", "column_values"),
    (
        (True, [None, "hello", "world"]),
        # TODO: pandas.to_sql defaults to nullable -- figure out how to set the column in the database to not nullable
        # (False, [1, 1, 2]),
    ),
)
def test_check_nullable_valid(postgres: PostgresContainer, is_nullable: bool, column_values: list):
    table_name = "example_table"
    db_schema = "public"

    class ExampleSchema(pa.DataFrameModel):
        column: pa.String = pa.Field(nullable=is_nullable)

    example_table = Table(name=table_name, db_schema=db_schema, table_schema=ExampleSchema)

    data = {"column": column_values}
    df = pd.DataFrame(data)

    engine = create_table_from_dataframe(postgres, df, table_name, db_schema)

    example_table.validate(engine, check_nullable=True)


@pytest.mark.parametrize(
    ("is_nullable", "column_values"),
    (
        # TODO: This will cause a SchemaValidationError no matter what I set the value to
        (False, [None, "hello", "world"]),
    ),
)
def test_check_nullable_invalid(postgres: PostgresContainer, is_nullable: bool, column_values: list):
    table_name = "example_table"
    db_schema = "public"

    class ExampleSchema(pa.DataFrameModel):
        column: pa.String = pa.Field(nullable=is_nullable)

    example_table = Table(name=table_name, db_schema=db_schema, table_schema=ExampleSchema)

    data = {"column": column_values}
    df = pd.DataFrame(data)

    engine = create_table_from_dataframe(postgres, df, table_name, db_schema)

    with pytest.raises(pandera_alchemy.exceptions.SchemaValidationError):
        example_table.validate(engine, check_nullable=True)
