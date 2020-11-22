#!/usr/bin/env python3

"""Update the NHIT Citizen Connect Data dataset:

    https://nhit-odp.data.socrata.com/d/enbi-fu9w

Loops over each specified data source, grabs the resulting dataframe,
validates it against the expected schema, and updates the full dataset
on Socrata. With a row ID set on the dataset, this will be an upsert
rather than an append.
"""

from datetime import datetime as dt
import logging
from logging import Logger, StreamHandler
import os
from typing import Dict, Mapping

from geomet import wkt
import pandas as pd
from pandas import DataFrame, Series
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaError
from socrata import Socrata
from socrata.authorization import Authorization
from socrata.output_schema import OutputSchema
from socrata.revisions import Revision
from socrata.sources import Source
from socrata.views import View

from sources.acs5 import get_source_acs5  # isort:skip

# Initialize logger
logger: Logger = logging.getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel(logging.INFO)

# Set defaults
DOMAIN = "nhit-odp.data.socrata.com"
DATASET_ID = "enbi-fu9w"
SOCRATA_AUTH = (os.environ["SOCRATA_KEY_ID"], os.environ["SOCRATA_KEY_SECRET"])

# Initialize Socrata client
auth = Authorization(DOMAIN, *SOCRATA_AUTH)
client = Socrata(auth)


def validate_year_date(value: str) -> bool:
    """Validate a year_date value against the format YYYY-MM-DD."""
    try:
        dt.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def validate_location(value: str) -> bool:
    """Validate a location value as a WKT Point."""
    try:
        geo_json: dict = wkt.loads(value)
    except ValueError:
        return False
    if isinstance(geo_json, dict) and geo_json.get("type") == "Point":
        return True
    else:
        return False


def make_row_id(row: Mapping) -> str:
    """Construct a row ID string from a row's ID fields."""
    id_fields = ["source", "variable", "denominator_variable", "year", "geo_id"]
    row_id_template: str = "|".join(f"{{{id_field}}}" for id_field in id_fields)
    row_values: Dict[str, str] = {}
    for id_field in id_fields:
        value_is_not_null: bool = row[id_field] is not None and pd.notnull(row[id_field])
        row_values[id_field] = row[id_field] if value_is_not_null else ""
    return row_id_template.format_map(row_values)


def assign_row_id(dataframe: DataFrame) -> DataFrame:
    """Construct a unique row ID column for the given dataframe."""
    row_id: Series = dataframe.apply(make_row_id, axis=1)
    return dataframe.assign(row_id=row_id)


def validate_dataframe(dataframe: DataFrame) -> bool:
    """Validate dataframe against schema."""
    schema = DataFrameSchema(
        {
            "source": Column(pa.String),
            "topic": Column(pa.String, nullable=True),
            "concept": Column(pa.String, nullable=True),
            "variable": Column(pa.String),
            "label": Column(pa.String),
            "value": Column(pa.Float),
            "denominator_variable": Column(pa.String, nullable=True),
            "denominator_label": Column(pa.String, nullable=True),
            "denominator": Column(pa.Float, nullable=True),
            "year": Column(
                pa.Int,
                checks=[
                    Check.less_than_or_equal_to(dt.now().year),
                    Check.greater_than_or_equal_to(2000),
                ],
            ),
            "year_date": Column(pa.String, checks=[Check(validate_year_date, element_wise=True)]),
            "geo_id": Column(pa.String),
            "geo_name": Column(pa.String),
            "geo_type": Column(pa.String),
            "location": Column(pa.String, checks=[Check(validate_location, element_wise=True)]),
            "row_id": Column(pa.String, allow_duplicates=False),
        },
        strict=True,
        coerce=True,
        checks=[
            # Check that year_date and year fields are aligned
            Check(lambda df: df["year_date"][:4] == df["year"].astype(str), element_wise=True),
            # Check that row_id field concatenates other identifying fields as expected
            Check(lambda df: df["row_id"] == df.apply(make_row_id, axis=1)),
        ],
    )

    # Validate dataframe against schema
    try:
        schema.validate(dataframe)
    except SchemaError as error:
        logger.warning(f"Failed to validate dataframe: {error.args[0]}")
        return False
    else:
        return True


def update_socrata_dataset(dataframe: DataFrame) -> str:
    """Use socrata-py to update the Socrata dataset at DATASET_ID."""
    # Look up dataset
    view: View = client.views.lookup(DATASET_ID)

    # Create revision, upload dataframe, and apply
    revision: Revision = view.revisions.create_update_revision()
    upload: Source = revision.create_upload("dataframe")
    source: Source = upload.df(dataframe)
    source.wait_for_finish()
    output_schema: OutputSchema = source.get_latest_input_schema().get_latest_output_schema()
    output_schema.wait_for_finish()
    revision.apply(output_schema=output_schema)

    # Return URL for revision
    return revision.ui_url()


def main() -> None:
    """Update dataset on Socrata for all specified sources."""
    # Mapping of data sources to use
    sources = {"ACS5": get_source_acs5}

    # Iterate over each source, get its data, and use it tos update Socrata dataset
    for source_name, source_function in sources.items():
        logger.info(f"Getting data for source {source_name}")
        dataframe: DataFrame = source_function()
        dataframe = assign_row_id(dataframe)

        logger.info(f"Validating dataframe for source {source_name}")
        dataframe_is_valid: bool = validate_dataframe(dataframe)
        if dataframe_is_valid is not True:
            logger.warning("Validation failed; skipping to next source")
            continue

        # logger.info("Saving CSV to disk")
        # dataframe.to_csv(f"{source_name}.csv", index=False)

        logger.info(f"Updating Socrata dataset for source {source_name}: {DATASET_ID}")
        revision_url: str = update_socrata_dataset(dataframe)
        logger.info(f"Updated Socrata dataset; revision will be published soon: {revision_url}")


if __name__ == "__main__":
    main()
