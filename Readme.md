# Spark Baseline
Measures the execution times of reading a parquet file and performing a sql query encompassing a filter and an aggregate sum operation.

## Setup
Four parquet files with 10e5, 50e6, 100e6, and 150e6 rows should be present in a datafolder specified by `baseFile`. These filenames should be appended by `10M`, `50M`, `100M`, and `150M` respectively.

TODO: Make base file path input parameter and determine sizes dynamically

## Running
To execute the baseline, run

    sbt compile
    sbt run