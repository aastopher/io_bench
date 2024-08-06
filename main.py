from io_bench import IOBench

def main() -> None:
    # Initialize the IOBench object with runs and parsers
    # io_bench = IOBench(
    #     source_file='./data/source.csv', 
    #     runs=10, 
    #     parsers=[
    #             'avro', 
    #             'parquet_polars', 
    #             'parquet_arrow', 
    #             'parquet_fast', 
    #             'feather', 
    #             'feather_arrow'
    #         ]
    # )

    io_bench = IOBench(
        source_file='./data/source.csv', 
        runs=10, 
        parsers=[
                'avro', 
                'parquet_polars', 
                'feather', 
            ]
    )

    # Generate sample data (if needed)
    io_bench.gen_sample_data(3000000)

    # Convert the source file to partitioned formats
    io_bench.partition(size_mb=10)
    # io_bench.partition(size_mb=10)

    # Run benchmarks without column selection
    benchmarks_no_columns = io_bench.run_battery(suffix='_no_columns')

    # Run benchmarks with column selection
    columns = ['Region', 'Country', 'Total Cost']
    benchmarks_with_columns = io_bench.run_battery(columns=columns, suffix='_with_columns')

    # Combine results and generate the final report
    all_benchmarks = benchmarks_no_columns + benchmarks_with_columns
    io_bench.gen_report(all_benchmarks, report_dir='./result')

    # io_bench.clear_partitions()

if __name__ == "__main__":
    main()
