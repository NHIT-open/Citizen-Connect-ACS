"""Functions representing the ACS5 data source."""

import logging
from logging import Logger

from autocensus import Query
from autocensus.geography import serialize_to_wkt
import numpy as np
import pandas as pd
from pandas import DataFrame

# Initialize logger
logger: Logger = logging.getLogger(__name__)

ACS5_VARIABLES = [
    # Median age
    "DP05_0017E",  # To 2016
    "DP05_0018E",  # From 2017
    # Population 60 years and older
    "S0101_C01_026E",  # To 2016
    "S0101_C01_028E",  # From 2017
    # Population by sex: total male, total female
    "DP05_0002E",
    "DP05_0003E",
    # Population by race
    *["B02001_{:03d}E".format(number) for number in range(2, 9)],
    # Median household income in past 12 months in dollars
    "S1903_C03_001E",
    # Population below poverty level in past 12 months
    "S1701_C02_001E",
    # Civilian occupation (16 years and older)
    *["S2401_C01_{:03d}E".format(number) for number in range(2, 37)],
    # Ancestry
    *["B04006_{:03d}E".format(number) for number in range(2, 108)],
    # Population by place of birth (foreign born)
    "DP02_0092E",
    # Population non-U.S. citizens
    "DP02_0095E",
    # Language variables from C16001_002E through C16001_036E
    "C16001_002E",
    "C16001_003E",
    "C16001_006E",
    "C16001_009E",
    "C16001_012E",
    "C16001_015E",
    "C16001_018E",
    "C16001_021E",
    "C16001_024E",
    "C16001_027E",
    "C16001_030E",
    "C16001_033E",
    "C16001_036E",
    # Percent high school graduate or higher
    "S1501_C01_014E",
    # Population with a disability
    "S1810_C02_001E",
    # Households with no internet access
    "B28002_013E",
]


def transform_dataframe(source_dataframe: DataFrame) -> DataFrame:
    """Transform an autocensus dataframe to match the expected schema."""
    dataframe: DataFrame = source_dataframe.copy()

    schema = [
        "source",
        "topic",
        "concept",
        "variable",
        "label",
        "value",
        "denominator_variable",
        "denominator_label",
        "denominator",
        "year",
        "year_date",
        "geo_id",
        "geo_name",
        "geo_type",
        "location",
    ]

    # Drop rows for variables that changed meaning across years
    dataframe = dataframe.loc[
        ~(
            (
                (
                    dataframe["variable_code"].isin(["DP05_0018E", "S0101_C01_028E"])
                    & (dataframe["year"] <= 2016)
                )
                | (
                    dataframe["variable_code"].isin(["DP05_0017E", "S0101_C01_026E"])
                    & (dataframe["year"] >= 2017)
                )
            )
        )
    ]

    # Transform data
    dataframe["source"] = "ACS5"
    dataframe["variable"] = dataframe["variable_code"]
    dataframe["value"] = dataframe["value"]
    dataframe["denominator_variable"] = pd.NA
    dataframe["denominator_label"] = pd.NA
    dataframe["denominator"] = np.NaN
    dataframe["year_date"] = dataframe["date"]
    dataframe["geo_name"] = dataframe["name"]
    dataframe["location"] = dataframe["geometry"].map(serialize_to_wkt)

    # Load in cleaned-up topics, concepts, labels
    labels: DataFrame = pd.read_csv("sources/acs5_labels.csv")
    dataframe = dataframe.merge(labels, how="left", on="variable")

    # Adopt schema and drop any other columns
    dataframe = dataframe.loc[:, schema]

    # Drop rows with null values
    dataframe = dataframe.loc[dataframe["value"].notnull()]

    return dataframe


def get_source_acs5() -> DataFrame:
    """Get a dataframe representing the ACS5 source.

    Uses autocensus to fetch a dataframe for the specified years and
    variables, then transforms the dataframe so it matches the expected
    schema for use with Citizen Connect.
    """
    logger.debug("Submitting ACS5 query")
    query = Query(
        estimate=5,
        years=[2015, 2016, 2017, 2018],
        variables=ACS5_VARIABLES,
        for_geo="county:*",
        in_geo="state:*",
        geometry="points",
    )
    dataframe: DataFrame = query.run()
    logger.debug("Transforming ACS5 dataframe")
    transformed_dataframe: DataFrame = transform_dataframe(dataframe)
    return transformed_dataframe
