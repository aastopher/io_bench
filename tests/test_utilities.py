import os
import pytest
from io_bench.utilities.bench import IOBench as Bench
from io_bench.utilities.parsing import AvroParser, PolarsParquetParser, ArrowParquetParser, FastParquetParser, FeatherParser, ArrowFeatherParser

@pytest.fixture
def setup_empty_directory(tmpdir):
    empty_dir = os.path.join(tmpdir, 'empty_dir')
    os.makedirs(empty_dir, exist_ok=True)
    return empty_dir

def test_avro_parser_no_data(setup_empty_directory):
    parser = AvroParser(dir=setup_empty_directory)
    with pytest.raises(FileNotFoundError, match="No data collected!"):
        parser.to_polars()

def test_polars_parquet_parser_no_data(setup_empty_directory):
    parser = PolarsParquetParser(dir=setup_empty_directory)
    with pytest.raises(FileNotFoundError, match="No data collected!"):
        parser.to_polars()

def test_arrow_parquet_parser_no_data(setup_empty_directory):
    parser = ArrowParquetParser(dir=setup_empty_directory)
    with pytest.raises(FileNotFoundError, match="No data collected!"):
        parser.to_polars()

def test_fast_parquet_parser_no_data(setup_empty_directory):
    parser = FastParquetParser(dir=setup_empty_directory)
    with pytest.raises(FileNotFoundError, match="No data collected!"):
        parser.to_polars()

def test_feather_parser_no_data(setup_empty_directory):
    parser = FeatherParser(dir=setup_empty_directory)
    with pytest.raises(FileNotFoundError, match="No data collected!"):
        parser.to_polars()

def test_arrow_feather_parser_no_data(setup_empty_directory):
    parser = ArrowFeatherParser(dir=setup_empty_directory)
    with pytest.raises(FileNotFoundError, match="No data collected!"):
        parser.to_polars()

def test_bench_initialization():
    mock_parser = lambda x: x  # Mock parser function
    bench = Bench(mock_parser, num_runs=5, id='test_bench')
    
    assert bench.num_runs == 5
    assert bench.id == 'test_bench'
    assert bench.columns is None
