# IO Bench

IO Bench is a library designed to benchmark the performance of different file formats and partitioning schemes for large datasets. It allows users to generate sample data, convert it to various formats, and run benchmarks to measure the performance of these formats.

## Features

- Generate sample data for benchmarking.
- Convert CSV data to various partitioned formats (Avro, Parquet, Feather).
- Benchmark reading performance of different file formats using Polars, PyArrow, and FastParquet.
- Generate comprehensive reports of benchmark results.

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/your-username/io_bench.git
    ```

2. Navigate to the project directory:
    ```sh
    cd io_bench
    ```

3. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

### Generating Sample Data

To generate sample data, initialize the `IOBench` object with the path to the source CSV file and call the `generate_sample_data` method:
```python
from io_bench import IOBench

io_bench = IOBench(source_file='./data/source_100K.csv')
io_bench.generate_sample_data()
```

### Converting Data to Partitioned Formats

Convert the generated CSV data to partitioned formats (Avro, Parquet, Feather):
```python
io_bench.convert_to_partitioned_formats(partition_size_mb=10)
```

### Running Benchmarks

Run benchmarks without column selection:
```python
benchmarks_no_columns = io_bench.run_benchmarks(suffix='no_columns')
```

Run benchmarks with column selection:
```python
columns = ['Region', 'Country', 'Total Cost']
benchmarks_with_columns = io_bench.run_benchmarks(columns=columns, suffix='with_columns')
```

### Generating Reports

Combine results and generate the final report:

```python
all_benchmarks = benchmarks_no_columns + benchmarks_with_columns
io_bench.generate_report(all_benchmarks, report_dir='./result')
```

### Full Example

Here is a full example of using IO Bench:
```python
from io_bench import IOBench

def main():
    # Initialize the IOBench object
    io_bench = IOBench(source_file='./data/source_100K.csv')

    # Generate sample data (if needed)
    io_bench.generate_sample_data()

    # Convert the source file to partitioned formats
    io_bench.convert_to_partitioned_formats(partition_size_mb=10)

    # Run benchmarks without column selection
    benchmarks_no_columns = io_bench.run_benchmarks(suffix='no_columns')

    # Run benchmarks with column selection
    columns = ['Region', 'Country', 'Total Cost']
    benchmarks_with_columns = io_bench.run_benchmarks(columns=columns, suffix='with_columns')

    # Combine results and generate the final report
    all_benchmarks = benchmarks_no_columns + benchmarks_with_columns
    io_bench.generate_report(all_benchmarks, report_dir='./result')

if __name__ == "__main__":
    main()
```
