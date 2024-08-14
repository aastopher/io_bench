import os
import glob
import polars as pl
from typing import List, Optional
from fastparquet import ParquetFile

class AvroParser:
    def __init__(self, directory: str) -> None:
        """
        Args:
            directory (str): Directory containing Avro files.
        """
        self.directory = directory
        self.file_paths = None

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Avro files to a Polars DataFrame.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Avro files.
        """
        self.file_paths = glob.glob(os.path.join(self.directory, "*"))
        dfs = [pl.read_avro(file, columns=columns) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class PolarsParquetParser:
    def __init__(self, directory: str) -> None:
        """
        Args:
            directory (str): Directory containing Parquet files.
        """
        self.directory = directory
        self.file_paths = None
        
    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Parquet files to a Polars DataFrame.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Parquet files.
        """
        self.file_paths = glob.glob(os.path.join(self.directory, "*"))
        if not self.file_paths:
            raise FileNotFoundError('No data collected!')
        return pl.read_parquet(f'{self.directory}/*.parquet', columns=columns)

class ArrowParquetParser:
    def __init__(self, directory: str) -> None:
        """
        Args:
            directory (str): Directory containing Parquet files.
        """
        self.directory = directory
        self.file_paths = None

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Parquet files to a Polars DataFrame using PyArrow.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Parquet files.
        """
        self.file_paths = glob.glob(os.path.join(self.directory, "*"))
        dfs = [pl.read_parquet(file, columns=columns, use_pyarrow=True) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class FastParquetParser:
    def __init__(self, directory: str) -> None:
        """
        Args:
            directory (str): Directory containing Parquet files.
        """
        self.directory = directory
        self.file_paths = None

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Parquet files to a Polars DataFrame using FastParquet.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Parquet files.
        """
        self.file_paths = glob.glob(os.path.join(self.directory, "*"))
        dfs = [pl.from_pandas(ParquetFile(file).to_pandas(columns)) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class FeatherParser:
    def __init__(self, directory: str) -> None:
        """
        Args:
            directory (str): Directory containing Feather files.
        """
        self.directory = directory
        self.file_paths = None

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Feather files to a Polars DataFrame.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Feather files.
        """
        self.file_paths = glob.glob(os.path.join(self.directory, "*"))
        dfs = [pl.read_ipc(file, columns=columns, memory_map=False) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df

class ArrowFeatherParser:
    def __init__(self, directory: str) -> None:
        """
        Args:
            directory (str): Directory containing Feather files.
        """
        self.directory = directory
        self.file_paths = None

    def to_polars(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert Feather files to a Polars DataFrame using PyArrow.

        Args:
            columns (Optional[List[str]]): List of columns to select.

        Returns:
            pl.DataFrame: Combined DataFrame from Feather files.
        """
        self.file_paths = glob.glob(os.path.join(self.directory, "*"))
        dfs = [pl.read_ipc(file, columns=columns, memory_map=False, use_pyarrow=True) for file in self.file_paths]
        if dfs:
            combined_df = pl.concat(dfs, how="diagonal_relaxed", parallel=True)
        else:
            raise FileNotFoundError('No data collected!')
        return combined_df