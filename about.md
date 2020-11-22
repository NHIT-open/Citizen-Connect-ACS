# NHIT Citizen Connect Script

A Python script to collect data that powers the NHIT Citizen Connect instance. This script currently retrieves ACS data from the Census API, but is built to anticipate adding other sources down the road.

The script expects that data from any source will be loaded as a [pandas] dataframe, and validates that dataframe against a schema defined using [pandera]. Here's an example that represents the expected schema:

| source | topic           | concept    | variable       | label                                                      | value  | denominator_variable | denominator_label                              | denominator | year | year_date  | geo_id               | geo_name                | geo_type | location                  | row_id                                                           |
|--------|-----------------|------------|----------------|------------------------------------------------------------|--------|----------------------|------------------------------------------------|-------------|------|------------|----------------------|-------------------------|----------|---------------------------|------------------------------------------------------------------|
| ACS5   | Equity & Access | Disability | S1810_C02_001E | Civilian noninstitutionalized population with a disability | 208108 | S1810_C01_001E       | Total civilian noninstitutionalized population | 1897256     | 2018 | 2018-12-31 | 1400000US12011050800 | Broward County, Florida | county   | POINT (-80.17786 26.1618) | ACS5\|S1810_C02_001E\|S1810_C01_001E\|2018\|1400000US12011050800 |

[pandas]: https://pandas.pydata.org/
[pandera]: https://pandera.readthedocs.io/

## Installation

This script requires a recent version of Python 3 (Python 3.7+ should work well). To run the script, you must first install the dependencies specified in `requirements.txt` and then execute the script with Python:

```sh
# Install dependencies
pip install -r requirements.txt

# Execute script (depending on the data sources specified, this may take a while)
python update_socrata_dataset.py
```

## Links

* [NHIT domain](https://nhit-odp.data.socrata.com)
  + [NHIT Citizen Connect Data](https://nhit-odp.data.socrata.com/d/enbi-fu9w)
* [NHIT Citizen Connect instance](http://nhitc.connect.socrata.com)
