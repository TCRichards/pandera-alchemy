# Pandera Alchemy

<img src="./images/pandera_alchemy.png" alt="pandera_alchemy logo" width="700" height="500">

The `pandera_alchemy` package bridges Pandera and SQLAlchemy, allowing users to define the structure of a database table using Pandera DataFrameModels or DataFrameSchemas and validate that the table has the expected structure with SQLAlchemy.

## Motivation

In modern data pipelines, ensuring that the structure of database tables matches the expected schema is crucial for maintaining data integrity and consistency. The `pandera_alchemy` package provides a seamless way to define and validate these structures, leveraging the power of Pandera for schema definitions and SQLAlchemy for database interactions.

## Installation

To add the `pandera_alchemy` as a dependency to a project, add it to the project's `pyproject.toml` file:


Install the dependencies:
    ```sh
    poetry install
    ```