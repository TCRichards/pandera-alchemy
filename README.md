# Pandera Alchemy

<img src="./images/pandera_alchemy.png" alt="pandera_alchemy logo" width="700" height="500">

The `pandera_alchemy` package bridges Pandera and SQLAlchemy, allowing users to define the structure of a database table using Pandera DataFrameModels or DataFrameSchemas and validate that the table has the expected structure with SQLAlchemy.

## Motivation

In modern data pipelines, ensuring that the structure of database tables matches the expected schema is crucial for maintaining data integrity and consistency. The `pandera_alchemy` package provides a seamless way to define and validate these structures, leveraging the power of Pandera for schema definitions and SQLAlchemy for database interactions.

## Installation

### As a Library

To install the `pandera_alchemy` package as a dependency, retrieve it from the Python Package Index (PyPI) using `pip` or `poetry`.

```sh
pip install pandera_alchemy
```

or 

```sh
poetry add pandera_alchemy
```

### Local Development

If you want to contribute to the development of `pandera_alchemy`, you can clone the repository and set up a local development environment.


Clone the repository:
```sh
git clone https://github.com/TCRichards/pandera-alchemy.git
```

Change into the project directory:
```sh
cd pandera-alchemy
```


Create a virtual environment and install dependencies with `poetry`:
```sh
poetry install
```


# License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
