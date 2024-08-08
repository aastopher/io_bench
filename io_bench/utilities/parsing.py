import pandas as pd
import os
import glob
import polars as pl
import pyarrow as pa
from typing import Any,List, Optional
from rich.console import Console
from fastparquet import ParquetFile
import pyarrow.feather as feather

class AvroParser:
    def __init__(self, dir: str) -> None:
        """
        Args:
            dir (str): Directory containing Avro files.
        """
        self.dir = dir

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Avro files to a Polars DataFrame.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Avro files.
        """
        self.file_paths = glob.glob(os.path.join(self.dir, "*"))
        dfs = [pl.read_avro(file, columns=columns) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class PolarsParquetParser:
    def __init__(self, dir: str) -> None:
        """
        Args:
            dir (str): Directory containing Parquet files.
        """
        self.dir = dir
        
    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Parquet files to a Polars DataFrame.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Parquet files.
        """
        self.file_paths = glob.glob(os.path.join(self.dir, "*"))
        return pl.read_parquet(f'{self.dir}/*.parquet', columns=columns)

class ArrowParquetParser:
    def __init__(self, dir: str) -> None:
        """
        Args:
            dir (str): Directory containing Parquet files.
        """
        self.dir = dir

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Parquet files to a Polars DataFrame using PyArrow.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Parquet files.
        """
        self.file_paths = glob.glob(os.path.join(self.dir, "*"))
        dfs = [pl.read_parquet(file, columns=columns, use_pyarrow=True) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class FastParquetParser:
    def __init__(self, dir: str) -> None:
        """
        Args:
            dir (str): Directory containing Parquet files.
        """
        self.dir = dir

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Parquet files to a Polars DataFrame using FastParquet.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Parquet files.
        """
        self.file_paths = glob.glob(os.path.join(self.dir, "*"))
        dfs = [pl.from_pandas(ParquetFile(file).to_pandas(columns)) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class FeatherParser:
    def __init__(self, dir: str) -> None:
        """
        Args:
            dir (str): Directory containing Feather files.
        """
        self.dir = dir

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Feather files to a Polars DataFrame.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Feather files.
        """
        self.file_paths = glob.glob(os.path.join(self.dir, "*"))
        dfs = [pl.read_ipc(file, columns=columns, memory_map=False) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class ArrowFeatherParser:
    def __init__(self, dir: str) -> None:
        """
        Args:
            dir (str): Directory containing Feather files.
        """
        self.dir = dir

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Feather files to a Polars DataFrame using PyArrow.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Feather files.
        """
        self.file_paths = glob.glob(os.path.join(self.dir, "*"))
        dfs = [pl.read_ipc(file, columns=columns, memory_map=False, use_pyarrow=True) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class CSV2Partitioned:
    def __init__(self, csv_file: str, out_dir: str, max_rows_per_file: int = 10000, file_types: Optional[List[str]] = None) -> None:
        """
        Args:
            csv_file (str): Path to the source CSV file.
            out_dir (str): Directory for output files.
            max_rows_per_file (int): Maximum number of rows per partition file.
            file_types (Optional[List[str]]): List of file types to convert to.
        """
        self.csv_file = csv_file
        self.out_dir = out_dir
        self.max_rows_per_file = max_rows_per_file
        self.file_types = file_types if file_types else ['avro', 'parquet', 'feather']
        self.console = Console()

    def convert(self) -> None:
        """
        Convert the source CSV file to partitioned formats.
        """
        for file_type in self.file_types:
            if file_type == 'avro':
                self.convert_to_avro()
            elif file_type == 'parquet':
                self.convert_to_parquet()
            elif file_type == 'feather':
                self.convert_to_feather()
            else:
                self.console.print(f"[red]Unsupported file type: {file_type}")

    def convert_to_avro(self) -> None:
        """
        Convert the CSV file to Avro format.
        """
        if self._is_folder_empty("avro"):
            self._create_folder_if_not_exist("avro")
            self._convert(write_func=pl.DataFrame.write_avro, ext="avro")
        else:
            self.console.print(f"[yellow]Skipping conversion: '{os.path.join(self.out_dir, 'avro')}' is not empty.")

    def convert_to_parquet(self) -> None:
        """
        Convert the CSV file to Parquet format.
        """
        if self._is_folder_empty("parquet"):
            self._create_folder_if_not_exist("parquet")
            self._convert(write_func=pl.DataFrame.write_parquet, ext="parquet")
        else:
            self.console.print(f"[yellow]Skipping conversion: '{os.path.join(self.out_dir, 'parquet')}' is not empty.")

    def convert_to_feather(self) -> None:
        """
        Convert the CSV file to Feather format.
        """
        if self._is_folder_empty("feather"):
            self._create_folder_if_not_exist("feather")
            self._convert(write_func=feather.write_feather, ext="feather")
        else:
            self.console.print(f"[yellow]Skipping conversion: '{os.path.join(self.out_dir, 'feather')}' is not empty.")

    def _is_folder_empty(self, ext: str) -> bool:
        """
        Check if a folder is empty.

        Args:
            ext (str): File extension indicating the folder to check.

        Returns:
            bool: True if the folder is empty, False otherwise.
        """
        folder_path = os.path.join(self.out_dir, ext)
        if os.path.exists(folder_path):
            files = os.listdir(folder_path)
            return not files
        return True

    def _create_folder_if_not_exist(self, ext: str) -> None:
        """
        Create a folder if it does not exist.

        Args:
            ext (str): File extension indicating the folder to create.
        """
        folder_path = os.path.join(self.out_dir, ext)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def _convert(self, write_func: Any, ext: str) -> None:
        """
        Convert the DataFrame to a specified format and save it in partitions.

        Args:
            write_func (Any): Function to write the DataFrame.
            ext (str): File extension for the output files.
        """
        df = pd.read_csv(self.csv_file)
        polars_df = pl.from_pandas(df)

        num_rows = len(polars_df)
        slice_pairs = [(start, min(start + self.max_rows_per_file, num_rows)) for start in range(0, num_rows, self.max_rows_per_file)]

        with Progress() as progress:
            run_task = progress.add_task(f'[green]Converting to {ext}', total=len(slice_pairs))
            for i, (start, end) in enumerate(slice_pairs):
                partition = polars_df.slice(start, end)
                if ext == "feather":
                    # Convert Polars DataFrame to PyArrow Table
                    partition = pa.Table.from_pandas(partition.to_pandas())
                out_path = os.path.join(self.out_dir, ext, f"part_{i}.{ext}")
                write_func(partition, out_path)
                progress.update(run_task, advance=1, description=f'[magenta]({i+1}/{len(slice_pairs)}) - [green]Wrote partition {i} to {out_path}')
