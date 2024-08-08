import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import fastavro
import tempfile
from typing import List, Dict, Optional
import asyncio
from rich.console import Console
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
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

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

        data = {
            'Region': ['North America', 'Europe', 'Asia'] * (records // 3),
            'Country': ['USA', 'Germany', 'China'] * (records // 3),
            'Total Cost': [1000.0, 1500.5, 2000.75] * (records // 3),
            'Sales': [5000.0, 7000.5, 9000.75] * (records // 3),
            'Profit': [2500.0, 3500.5, 4500.75] * (records // 3)
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
                    elif os.path.isdir(file_path):
                        os.rmdir(file_path)
                except Exception as e:
                    self.console.print(f"[red]Failed to delete {file_path}. Reason: {e}")

    def partition(
        self, 
        size_mb: int = 10, 
        chunk_sizes: Optional[Dict[str, int]] = {
            'avro': 1000,
            'parquet': 10000,
            'feather': 1000
        }
    ) -> None:
        """
        Convert the source file to partitioned formats.

        Args:
            size_mb (int): Size of each partition in MB.
            chunk_sizes (Optional[Dict[str, int]]): Dictionary of chunk sizes for each format.
        """
        asyncio.run(self._partition(size_mb, chunk_sizes))

    def _collect_efficiency(self, ranges, sizes):
        metrics = {}
        for key, value in ranges.items():
            metrics[key] = value[0][1]
        self.console.print(metrics)

    async def _partition(self, size_mb: int, chunk_sizes: Dict[str, int]) -> None:
        df = pd.read_csv(self.source_file)
        
        os.makedirs(self.avro_dir, exist_ok=True)
        os.makedirs(self.parquet_dir, exist_ok=True)
        os.makedirs(self.feather_dir, exist_ok=True)
        
        partition_size = size_mb * 1024 * 1024
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
        ) as progress:
            partition_ranges = {}
            for parser_type in ['avro', 'parquet', 'feather']:
                if parser_type == 'avro' and 'avro' in self.parsers:
                    task = progress.add_task(f"[cyan]Finding {parser_type} partition ranges...", total=len(df))
                    partition_ranges['avro'] = await self._find_partition_ranges(df, partition_size, 'avro', chunk_sizes['avro'], progress, task)
                elif parser_type == 'parquet' and any(parser in self.parsers for parser in ['parquet_polars', 'parquet_arrow', 'parquet_fast']):
                    task = progress.add_task(f"[cyan]Finding {parser_type} partition ranges...", total=len(df))
                    partition_ranges['parquet'] = await self._find_partition_ranges(df, partition_size, 'parquet', chunk_sizes['parquet'], progress, task)
                elif parser_type == 'feather' and any(parser in self.parsers for parser in ['feather', 'feather_arrow']):
                    task = progress.add_task(f"[cyan]Finding {parser_type} partition ranges...", total=len(df))
                    partition_ranges['feather'] = await self._find_partition_ranges(df, partition_size, 'feather', chunk_sizes['feather'], progress, task)

        # automatically clean up partitions from previous runs
        self.clear_partitions()
        
        tasks = []
        with self.console.status(f'[cyan]Writing partitions...', spinner='bouncingBar'):
            for parser_type, ranges in partition_ranges.items():
                for part_number, (start_idx, end_idx) in enumerate(ranges):
                    partition_df = df.iloc[start_idx:end_idx]
                    if parser_type == 'avro':
                        tasks.append(self._write_avro(partition_df, os.path.join(self.avro_dir, f'part_{part_number}.avro')))
                    elif parser_type == 'parquet':
                        for parser in ['parquet_polars', 'parquet_arrow', 'parquet_fast']:
                            if parser in self.parsers:
                                tasks.append(self._write_parquet(partition_df, os.path.join(self.parquet_dir, f'{parser}_part_{part_number}.parquet')))
                    elif parser_type == 'feather':
                        for parser in ['feather', 'feather_arrow']:
                            if parser in self.parsers:
                                tasks.append(self._write_feather(partition_df, os.path.join(self.feather_dir, f'{parser}_part_{part_number}.feather')))
        
            await asyncio.gather(*tasks)
            self._collect_efficiency(partition_ranges, [])

    async def _find_partition_ranges(self, df: pd.DataFrame, target_size: int, parser: str, chunk_size: int, progress: Progress, task: int) -> List[tuple]:
        partition_ranges = []
        start_idx = 0
        while start_idx < len(df):
            end_idx = await self._find_partition_end_index(df, start_idx, target_size, parser, chunk_size)
            if end_idx == start_idx:  # Break if no progress is made
                break
            partition_ranges.append((start_idx, end_idx))
            progress.update(task, completed=end_idx)
            start_idx = end_idx
        if not partition_ranges:  # Handle case where no partitions were added
            partition_ranges.append((0, len(df)))
        return partition_ranges

    async def _find_partition_end_index(self, df: pd.DataFrame, start_idx: int, target_size: int, parser: str, chunk_size: int) -> int:
        end_idx = start_idx
        current_size = 0

        while end_idx < len(df) and current_size < target_size:
            next_end_idx = min(end_idx + chunk_size, len(df))
            partition_df = df.iloc[end_idx:next_end_idx]
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_size = await self._write_temp_file(partition_df, tmp_file.name, parser)
                current_size += tmp_size
            
            if current_size > target_size and end_idx > start_idx:
                break
                
            end_idx = next_end_idx

        return end_idx if end_idx > start_idx else len(df)

    async def _write_temp_file(self, df: pd.DataFrame, tmp_file: str, parser: str) -> int:
        if parser == 'avro':
            await self._write_avro(df, tmp_file)
        elif parser.startswith('parquet'):
            await self._write_parquet(df, tmp_file)
        elif parser.startswith('feather'):
            await self._write_feather(df, tmp_file)
        
        return os.path.getsize(tmp_file)
        
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

    def battery(self, columns: Optional[List[str]] = None, suffix: Optional[str] = None) -> List[Bench]:
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
