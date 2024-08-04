from io_bench import IOBench

def main():
    # Initialize the IOBench object with runs and parsers
    io_bench = IOBench(source_file='./data/source_100K.csv', runs=20, parsers=['avro', 'parquet_polars'])

    # Generate sample data (if needed)
    io_bench.generate_sample_data()

    # Convert the source file to partitioned formats
    io_bench.convert_to_partitioned_formats(partition_size_mb=10)

    # Run benchmarks without column selection
    benchmarks_no_columns = io_bench.run_benchmarks(suffix='_no_columns')

    # Run benchmarks with column selection
    columns = ['Region', 'Country', 'Total Cost']
    benchmarks_with_columns = io_bench.run_benchmarks(columns=columns, suffix='_with_columns')

    # Combine results and generate the final report
    all_benchmarks = benchmarks_no_columns + benchmarks_with_columns
    io_bench.generate_report(all_benchmarks, report_dir='./result')

if __name__ == "__main__":
    main()
