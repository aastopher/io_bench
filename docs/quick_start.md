# Generating Sample Data
To generate sample data, initialize the `IOBench` object with the path to the source CSV file and call the `generate_sample_data` method:
```python
from io_bench import IOBench

io_bench = IOBench(source_file='./data/source_100K.csv', runs=20, parsers=['avro', 'parquet_polars'])
io_bench.generate_sample_data()
```

# Converting Data to Partitioned Formats
Convert the generated CSV data to partitioned formats (Avro, Parquet, Feather):
```python
io_bench.partition(size_mb=10)
```

# Running Benchmarks
Run benchmarks without column selection:
```python
benchmarks_no_columns = io_bench.run_battery(suffix='_no_columns')
```

Run benchmarks with column selection:
```python
columns = ['Region', 'Country', 'Total Cost']
benchmarks_with_columns = io_bench.run_battery(columns=columns, suffix='_with_columns')
```

# Generating Reports
Combine results and generate the final report:
```python
all_benchmarks = benchmarks_no_columns + benchmarks_with_columns
io_bench.gen_report(all_benchmarks, report_dir='./result')
```

# Full Example

Here is a full example of using IO Bench:
```python
from io_bench import IOBench

def main() -> None:
    # Initialize the IOBench object with runs and parsers
    io_bench = IOBench(source_file='./data/source_100K.csv', runs=20, parsers=['avro', 'parquet_polars'])

    # Generate sample data (if needed)
    io_bench.generate_sample_data()

    # Convert the source file to partitioned formats
    io_bench.partition(size_mb=10)

    # Run benchmarks without column selection
    benchmarks_no_columns = io_bench.run_battery(suffix='_no_columns')

    # Run benchmarks with column selection
    columns = ['Region', 'Country', 'Total Cost']
    benchmarks_with_columns = io_bench.run_battery(columns=columns, suffix='_with_columns')

    # Combine results and generate the final report
    all_benchmarks = benchmarks_no_columns + benchmarks_with_columns
    io_bench.gen_report(all_benchmarks, report_dir='./result')

if __name__ == "__main__":
    main()
```