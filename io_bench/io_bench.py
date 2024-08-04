import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import fastavro
from typing import List, Dict, Any, Optional
from io_bench.utilities.bench import IOBench as Bench
from io_bench.utilities.explain import gen_report
from io_bench.utilities.parsing import AvroParser, PolarsParquetParser, ArrowParquetParser, FastParquetParser, FeatherParser, ArrowFeatherParser

class IOBench:
    def __init__(self, source_file: str, output_dir: str = './data', runs: int = 10, parsers: Optional[List[str]] = None) -> None:
        """
        Benchmark performance of standard flat file formats and partitioning schemes.

        Args:
            source_file (str): Path to the source CSV file.
            output_dir (str): Directory for output files.
            runs (int): Number of benchmark runs.
            parsers (Optional[List[str]]): List of parsers to use.
        """
        self.source_file = source_file
        self.output_dir = output_dir
        self.runs = runs
        self.benchmark_counter = 0

        self.avro_dir = os.path.join(output_dir, 'avro')
        self.parquet_dir = os.path.join(output_dir, 'parquet')
        self.feather_dir = os.path.join(output_dir, 'feather')

        self.available_parsers = {
            'avro': AvroParser(self.avro_dir),
            'parquet_polars': PolarsParquetParser(self.parquet_dir),
            'parquet_arrow': ArrowParquetParser(self.parquet_dir),
            'parquet_fast': FastParquetParser(self.parquet_dir),
            'feather': FeatherParser(self.feather_dir),
            'arrow_feather': ArrowFeatherParser(self.feather_dir)
        }

        self.parsers = parsers if parsers is not None else list(self.available_parsers.keys())

    def gen_sample_data(self, records: int = 100000) -> None:
        """
        Generate sample data and save it to the source file.

        Args:
            records (int): Number of records to generate.
        """
        data = {
            'Region': ['North America', 'Europe', 'Asia'] * (records // 3),
            'Country': ['USA', 'Germany', 'China'] * (records // 3),
            'Total Cost': [1000.0, 1500.5, 2000.75] * (records // 3),
            'Sales': [5000.0, 7000.5, 9000.75] * (records // 3),
            'Profit': [2500.0, 3500.5, 4500.75] * (records // 3)
        }
        
        df = pd.DataFrame(data)
        
        os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
        
        df.to_csv(self.source_file, index=False)

    def partition(self, size_mb: int = 10) -> None:
        """
        Convert the source file to partitioned formats.

        Args:
            size_mb (int): Size of each partition in MB.
        """
        df = pd.read_csv(self.source_file)
        
        os.makedirs(self.avro_dir, exist_ok=True)
        os.makedirs(self.parquet_dir, exist_ok=True)
        os.makedirs(self.feather_dir, exist_ok=True)
        
        partition_size = size_mb * 1024 * 1024
        record_size = df.memory_usage(deep=True).sum() // len(df)
        records_per_partition = partition_size // record_size
        
        for i in range(0, len(df), records_per_partition):
            partition_df = df.iloc[i:i + records_per_partition]
            part_number = i // records_per_partition
            
            self._write_avro(partition_df, os.path.join(self.avro_dir, f'part_{part_number}.avro'))
            self._write_parquet(partition_df, os.path.join(self.parquet_dir, f'part_{part_number}.parquet'))
            self._write_feather(partition_df, os.path.join(self.feather_dir, f'part_{part_number}.feather'))

    def _write_avro(self, df: pd.DataFrame, file_path: str) -> None:
        """
        Write a DataFrame to an Avro file.

        Args:
            df (pd.DataFrame): DataFrame to write.
            file_path (str): Path to the output Avro file.
        """
        records = df.to_dict('records')
        schema = {
            'type': 'record',
            'name': 'Benchmark',
            'fields': [
                {'name': 'Region', 'type': 'string'},
                {'name': 'Country', 'type': 'string'},
                {'name': 'Total Cost', 'type': 'float'},
                {'name': 'Sales', 'type': 'float'},
                {'name': 'Profit', 'type': 'float'}
            ]
        }
        with open(file_path, 'wb') as out:
            fastavro.writer(out, schema, records)

    def _write_parquet(self, df: pd.DataFrame, file_path: str) -> None:
        """
        Write a DataFrame to a Parquet file.

        Args:
            df (pd.DataFrame): DataFrame to write.
            file_path (str): Path to the output Parquet file.
        """
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)

    def _write_feather(self, df: pd.DataFrame, file_path: str) -> None:
        """
        Write a DataFrame to a Feather file.

        Args:
            df (pd.DataFrame): DataFrame to write.
            file_path (str): Path to the output Feather file.
        """
        table = pa.Table.from_pandas(df)
        feather.write_feather(table, file_path)

    def run_battery(self, columns: Optional[List[str]] = None, suffix: Optional[str] = None) -> List[Bench]:
        """
        Run benchmarks using the specified parsers.

        Args:
            columns (Optional[List[str]]): List of columns to select.
            suffix (Optional[str]): Suffix for benchmark IDs.

        Returns:
            List[Bench]: List of benchmark results.
        """
        benchmarks = []

        if suffix is None:
            suffix = f'_{self.benchmark_counter}'
            self.benchmark_counter += 1

        for name in self.parsers:
            if name in self.available_parsers:
                parser = self.available_parsers[name]
                bench = Bench(parser, columns=columns, num_runs=self.runs, id=f'{name}{suffix}').benchmark()
                benchmarks.append(bench)
        
        return benchmarks

    def gen_report(self, benchmark_results: List[Bench], report_dir: str = './result') -> None:
        """
        Generate a report from benchmark results.

        Args:
            benchmark_results (List[Bench]): List of benchmark results.
            report_dir (str): Directory to save the report.
        """
        gen_report(benchmark_results, dir=report_dir)
