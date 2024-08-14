import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import fastavro
from typing import List, Dict, Optional
import asyncio
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from io_bench.utilities.bench import IOBench as Bench
from io_bench.utilities.explain import generate_report
from io_bench.utilities.parsing import (
    AvroParser, 
    PolarsParquetParser, 
    ArrowParquetParser, 
    FastParquetParser, 
    FeatherParser, 
    ArrowFeatherParser
)

class IOBench:
    def __init__(
            self, 
            source_file: str, 
            output_dir: str = './data', 
            runs: int = 10, 
            parsers: Optional[List[str]] = [
                'avro', 
                'parquet_polars', 
                'parquet_arrow', 
                'parquet_fast', 
                'feather', 
                'feather_arrow'
            ]
        ) -> None:
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
        self.partitioned = False
        self.console = Console()

        self.avro_dir = os.path.join(output_dir, 'avro')
        self.parquet_dir = os.path.join(output_dir, 'parquet')
        self.feather_dir = os.path.join(output_dir, 'feather')

        self.available_parsers = {
            'avro': AvroParser(self.avro_dir),
            'parquet_polars': PolarsParquetParser(self.parquet_dir),
            'parquet_arrow': ArrowParquetParser(self.parquet_dir),
            'parquet_fast': FastParquetParser(self.parquet_dir),
            'feather': FeatherParser(self.feather_dir),
            'feather_arrow': ArrowFeatherParser(self.feather_dir)
        }

        self.parsers = parsers if parsers is not None else list(self.available_parsers.keys())
        self.selected_parsers = {name: self.available_parsers[name] for name in self.parsers if name in self.available_parsers}

    def generate_sample(self, records: int = 100000) -> None:
        """
        Generate sample data and save it to the source file.

        Args:
            records (int): Number of records to generate.
        """
        if os.path.exists(self.source_file):
            self.console.print(f"[yellow]Source file '{self.source_file}' already exists. Skipping data generation.")
            return

        # Calculate the exact number of repetitions needed for the records
        base_repeats = records // 3
        remainder = records % 3

        data = {
            'Region': (['North America', 'Europe', 'Asia'] * base_repeats) + ['North America', 'Europe', 'Asia'][:remainder],
            'Country': (['USA', 'Germany', 'China'] * base_repeats) + ['USA', 'Germany', 'China'][:remainder],
            'Total Cost': ([1000.0, 1500.5, 2000.75] * base_repeats) + [1000.0, 1500.5, 2000.75][:remainder],
            'Sales': ([5000.0, 7000.5, 9000.75] * base_repeats) + [5000.0, 7000.5, 9000.75][:remainder],
            'Profit': ([2500.0, 3500.5, 4500.75] * base_repeats) + [2500.0, 3500.5, 4500.75][:remainder]
        }

        df = pd.DataFrame(data)

        os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
        with self.console.status(f'[cyan]Generating {records} records of data ...', spinner='bouncingBar'):
            df.to_csv(self.source_file, index=False)


    def clear_partitions(self) -> None:
        """
        Clear the partition folders by deleting all files in the avro, parquet, and feather directories.
        """
        for directory in [self.avro_dir, self.parquet_dir, self.feather_dir]:
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    self.console.print(f"[red]Failed to delete {file_path}. Reason: {e}")

    def partition(
        self,  
        rows: dict = {
            'avro': 500000,
            'parquet': 3000000,
            'feather': 1600000
        }
    ) -> None:
        """
        Partition the source data into multiple files based on the specified row chunks.

        Args:
            rows (dict): Dictionary specifying the number of rows per partition for each format.
        """
        # map simple chunk sizes
        row_chunks = {
            'avro': rows['avro'],
            'parquet_polars': rows['parquet'],
            'parquet_arrow': rows['parquet'],
            'parquet_fast': rows['parquet'],
            'feather': rows['feather'],
            'feather_arrow': rows['feather']
        }
        with self.console.status(f'[cyan]writing partitioned data...', spinner='dots'):
            asyncio.run(self._partition(row_chunks))
        self.partitioned = True

    async def _partition(self, row_chunks: Dict[str, int]) -> None:
        """
        Asynchronously partition the data into different formats.

        Args:
            row_chunks (Dict[str, int]): Dictionary specifying the number of rows per partition for each format.
        """
        df = pd.read_csv(self.source_file)
        
        os.makedirs(self.avro_dir, exist_ok=True)
        os.makedirs(self.parquet_dir, exist_ok=True)
        os.makedirs(self.feather_dir, exist_ok=True)
        
        total_rows = df.shape[0]

        partition_ranges = {}
        for parser, chunk_size in row_chunks.items():
            if parser in self.selected_parsers:
                partition_ranges[parser] = self._calculate_partition_ranges(total_rows, chunk_size)
       
        self.clear_partitions()
        # self.console.print(partition_ranges)

        tasks = []
        for parser, ranges in partition_ranges.items():
            for partition_id, (start_idx, end_idx) in enumerate(ranges):
                partition_df = df.iloc[start_idx:end_idx]
                if parser == 'avro':
                    tasks.append(self._write_avro(partition_df, os.path.join(self.avro_dir, f'part_{partition_id}.avro')))
                elif parser.startswith('parquet'):
                    tasks.append(self._write_parquet(partition_df, os.path.join(self.parquet_dir, f'{parser}_part_{partition_id}.parquet')))
                elif parser.startswith('feather'):
                    tasks.append(self._write_feather(partition_df, os.path.join(self.feather_dir, f'{parser}_part_{partition_id}.feather')))
        
        await asyncio.gather(*tasks)

    def _calculate_partition_ranges(self, total_rows: int, row_chunks: int) -> List[tuple]:
        """
        Calculate the partition ranges given the total number of rows and chunk size.

        Args:
            total_rows (int): Total number of rows in the DataFrame.
            row_chunks (int): The chunk size in terms of rows.

        Returns:
            List[tuple]: List of tuples where each tuple represents a start and end index for a partition.
        """
        num_partitions = max(1, (total_rows + row_chunks - 1) // row_chunks)  # Ensure at least one partition
        rows_per_partition = total_rows // num_partitions
        remainder = total_rows % num_partitions
        
        partition_ranges = []
        start_idx = 0

        for i in range(num_partitions):
            end_idx = start_idx + rows_per_partition + (1 if i < remainder else 0)
            partition_ranges.append((start_idx, min(end_idx, total_rows)))
            start_idx = end_idx
            
        return partition_ranges

    
    async def _write_avro(self, df: pd.DataFrame, file_path: str) -> None:
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

    async def _write_parquet(self, df: pd.DataFrame, file_path: str) -> None:
        """
        Write a DataFrame to a Parquet file.

        Args:
            df (pd.DataFrame): DataFrame to write.
            file_path (str): Path to the output Parquet file.
        """
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)

    async def _write_feather(self, df: pd.DataFrame, file_path: str) -> None:
        """
        Write a DataFrame to a Feather file.

        Args:
            df (pd.DataFrame): DataFrame to write.
            file_path (str): Path to the output Feather file.
        """
        table = pa.Table.from_pandas(df)
        feather.write_feather(table, file_path)

    def run(self, columns: Optional[List[str]] = None, suffix: Optional[str] = None) -> List[Bench]:
        """
        Run benchmarks using the specified parsers.

        Args:
            columns (Optional[List[str]]): List of columns to select.
            suffix (Optional[str]]): Suffix for benchmark IDs.

        Returns:
            List[Bench]: List of benchmark results.
        """
        if not self.partitioned:
            self.partition()

        benchmarks = []

        if suffix is None:
            suffix = f'_{self.benchmark_counter}'
            self.benchmark_counter += 1

        for name, parser in self.selected_parsers.items():
            bench = Bench(parser, columns=columns, num_runs=self.runs, id=f'{name}{suffix}').benchmark()
            benchmarks.append(bench)
        
        return benchmarks

    def report(self, benchmark_results: List[Bench], report_dir: str = './result') -> None:
        """
        Generate a report from benchmark results.

        Args:
            benchmark_results (List[Bench]): List of benchmark results.
            report_dir (str): Directory to save the report.
        """
        generate_report(benchmark_results, dir=report_dir)
