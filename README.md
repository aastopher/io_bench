<!-- [![PyPI version](https://badge.fury.io/py/io_bench.svg)](https://badge.fury.io/py/io_bench) -->
[![Documentation Status](https://img.shields.io/badge/docs-online-brightgreen)](https://aastopher.github.io/io_bench/)
[![codecov](https://codecov.io/gh/aastopher/io_bench/graph/badge.svg?token=79V7VRZWV0)](https://codecov.io/gh/aastopher/io_bench)
[![DeepSource](https://app.deepsource.com/gh/aastopher/io_bench.svg/?label=active+issues&show_trend=true&token=3NT8mR1AQRLW9zDNKWQ8vgFl)](https://app.deepsource.com/gh/aastopher/io_bench/)

# IOBench Quick Start Guide

## Generating Sample Data
To generate sample data, initialize the `IOBench` object with the path to the source CSV file and call the `generate_sample` method:

```python
from io_bench import IOBench

bench = IOBench(source_file='./data/source_100K.csv', runs=20, parsers=['avro', 'parquet_polars', 'parquet_arrow', 'parquet_fast', 'feather', 'feather_arrow'])
bench.generate_sample(records=100000) # default value
```
**NOTE:** `source_file` behavior is contextual; providing a desired name for a sample file then calling `generate_sample` will create the file. Otherwise a valid path to an existing file must be provided.

## Converting Data to Partitioned Formats
Convert the generated CSV data to partitioned formats (Avro, Parquet, Feather) will automatically partition on default column selection chunks if not defined.

```python
bench.partition(rows={'avro': 500000, 'parquet': 3000000, 'feather': 1600000})
```

## Running Benchmarks
NOTE: Partition is stateful per bench object. If partition is not called manually it will automatically be called on the first run only assuming a valid source file exists.
### Without Column Selection
Run benchmarks without column selection:

```python
benchmarks_no_select = bench.run(suffix='_no_select')
```

### With Column Selection
Run benchmarks with column selection:

```python
columns = ['Region', 'Country', 'Total Cost']
benchmarks_column_select = bench.run(columns=columns, suffix='_column_select')
```

## Generating Reports
Combine results and generate the final report:

```python
all_benchmarks = benchmarks_no_select + benchmarks_column_select
io_bench.report(all_benchmarks, report_dir='./result')
```

## Full Example

Here is a full example of using `IOBench`:

```python
from io_bench import IOBench

def main() -> None:
    # Initialize the IOBench object with runs and parsers
    bench = IOBench(source_file='./data/source_100K.csv', runs=20, parsers=['avro', 'parquet_polars'])

    # Generate sample data - (optional)
    bench.generate_sample()

    # Convert the source file to partitioned formats - (optional)
    bench.partition(rows={'avro': 500000, 'parquet': 3000000, 'feather': 1600000})

    # Run benchmarks without column selection
    benchmarks_no_select = bench.run(suffix='_no_select')

    # Run benchmarks with column selection
    columns = ['Region', 'Country', 'Total Cost']
    benchmarks_column_select = bench.run(columns=columns, suffix='_column_select')

    # Combine results and generate the final report
    all_benchmarks = benchmarks_no_select + benchmarks_column_select
    bench.report(all_benchmarks, report_dir='./result')

if __name__ == "__main__":
    main()
```