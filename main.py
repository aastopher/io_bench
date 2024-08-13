from io_bench import IOBench

def main() -> None:
    # Initialize the IOBench object with runs and parsers
    # bench = IOBench(
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

    bench = IOBench(
        source_file='./data/source.csv', 
        runs=10, 
        parsers=[
                'avro', 
                'parquet_polars', 
                'feather', 
            ]
    )

    # Generate sample data (if needed)
    # bench.generate_sample(3000000)

    # Convert the source file to partitioned formats
    # bench.partition()

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
