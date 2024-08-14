import os
import pandas as pd
import pytest
from unittest.mock import patch
from io_bench import IOBench

@pytest.fixture
def setup_environment(tmpdir):
    source_file = os.path.join(tmpdir, 'source.csv')
    output_dir = os.path.join(tmpdir, 'output')
    os.makedirs(output_dir, exist_ok=True)

    return {
        'source_file': source_file,
        'output_dir': output_dir,
    }

def test_generate_sample(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'])
    
    bench.generate_sample(records=1000)
    
    # Check if the sample file is created
    assert os.path.exists(config['source_file']), "Sample CSV file was not created"
    
    # Verify the content
    df = pd.read_csv(config['source_file'])
    assert len(df) == 1000, f"Generated sample does not contain the expected number of records - expected 1000 got {len(df)}"

def test_generate_sample_file_exists(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'])

    # Create a dummy file to simulate existing file
    with open(config['source_file'], 'w') as f:
        f.write("dummy data")

    bench.generate_sample(records=1000)

    # Ensure that the original file was not overwritten
    with open(config['source_file'], 'r') as f:
        content = f.read()
    assert content == "dummy data", "Existing file should not be overwritten"

def test_partition(setup_environment):
    # Test partitioning the data
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])
    
    # Generate sample data and partition
    bench.generate_sample(records=1000)
    bench.partition(rows={'avro': 250, 'parquet': 500, 'feather': 500})
    
    # Check if the partitioned directories exist
    assert os.path.exists(os.path.join(config['output_dir'], 'avro')), "Avro directory was not created"
    assert os.path.exists(os.path.join(config['output_dir'], 'parquet')), "Parquet directory was not created"
    assert os.path.exists(os.path.join(config['output_dir'], 'feather')), "Feather directory was not created"
    
    # Check if partition files are created
    assert len(os.listdir(os.path.join(config['output_dir'], 'avro'))) > 0, "No Avro partition files were created"
    assert len(os.listdir(os.path.join(config['output_dir'], 'parquet'))) > 0, "No Parquet partition files were created"
    assert len(os.listdir(os.path.join(config['output_dir'], 'feather'))) > 0, "No Feather partition files were created"

def test_partition_ranges_adjustment(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])
    
    # Generate sample data with a specific number of records that will require adjustment
    records = 750  
    bench.generate_sample(records=records)
    
    # Trigger the partitioning logic
    partition_ranges = bench._calculate_partition_ranges(total_rows=records, row_chunks=500)
    
    # Verify that the partition_ranges were adjusted correctly
    assert partition_ranges[-1][1] == records, "The last partition range was not adjusted to include all rows"

def test_run_triggers_partition(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])

    # Generate the sample data to ensure the source file exists
    bench.generate_sample(records=100)

    # Ensure self.partitioned is False initially
    bench.partitioned = False

    # Mock the partition method to confirm it gets called
    with patch.object(bench, 'partition', wraps=bench.partition) as mock_partition:
        bench.run(suffix='_test')
        mock_partition.assert_called_once()

def test_benchmark_counter_increment(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])
    
    # Generate sample data and partition
    bench.generate_sample(records=100)
    bench.partition()
    
    # Run benchmarks multiple times to check the suffix and counter
    suffix1 = f'_{bench.benchmark_counter}'
    benchmarks1 = bench.run()
    assert suffix1 in benchmarks1[0].id, "Suffix 1 not found in benchmark ID"
    
    suffix2 = f'_{bench.benchmark_counter}'
    benchmarks2 = bench.run()
    assert suffix2 in benchmarks2[0].id, "Suffix 2 not found in benchmark ID"

    # Ensure that the counter has incremented
    assert int(suffix2.strip('_')) == int(suffix1.strip('_')) + 1, "Benchmark counter did not increment correctly"

def test_run_benchmarks(setup_environment):
    # Test running benchmarks
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])
    
    # Generate sample data and partition
    bench.generate_sample(records=1000)
    bench.partition(rows={'avro': 250, 'parquet': 500, 'feather': 500})
    
    # Run benchmarks without column selection
    benchmarks = bench.run(suffix='_test')
    
    # Check if benchmarks were run
    assert len(benchmarks) > 0, "No benchmarks were generated"

def test_run_with_columns(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])

    # Generate sample data and partition
    bench.generate_sample(records=1000)
    bench.partition(rows={'avro': 250, 'parquet': 500, 'feather': 500})

    # Run benchmarks with column selection
    columns = ['Region', 'Country']
    benchmarks = bench.run(columns=columns, suffix='_with_columns')

    # Check if benchmarks were run
    assert len(benchmarks) > 0, "No benchmarks were generated with column selection"

def test_generate_report(setup_environment):
    # Test generating a report
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])
    
    # Generate sample data and partition
    bench.generate_sample(records=1000)
    bench.partition(rows={'avro': 250, 'parquet': 500, 'feather': 500})
    
    # Run benchmarks without column selection
    benchmarks = bench.run(suffix='_test')
    
    # Generate report
    bench.report(benchmarks, report_dir=config['output_dir'])
    
    # Check if report files are created
    assert os.path.exists(os.path.join(config['output_dir'], 'summary_report.html')), "Summary report was not generated"
    assert os.path.exists(os.path.join(config['output_dir'], 'polling_report.html')), "Polling report was not generated"


def test_clear_partitions(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])

    # Create mock files and directories
    mock_files = {
        'avro': ['file1.avro'],
        'parquet': ['file2.parquet'],
        'feather': ['file3.feather']
    }
    
    for folder, files in mock_files.items():
        dir_path = os.path.join(config['output_dir'], folder)
        os.makedirs(dir_path, exist_ok=True)
        for file in files:
            with open(os.path.join(dir_path, file), 'w') as f:
                f.write("dummy data")

    # Mock os.unlink, os.rmdir and os.path.islink to simulate file and directory removal
    with patch('os.unlink') as mock_unlink, patch('os.rmdir') as mock_rmdir, patch('os.path.islink', return_value=False):
        bench.clear_partitions()

        # Check if unlink and rmdir were called correctly
        assert mock_unlink.call_count == 3, "Expected os.unlink to be called three times"
        assert mock_rmdir.call_count == 0, "Expected os.rmdir to not be called"

def test_clear_partitions_with_exception(setup_environment):
    config = setup_environment
    bench = IOBench(source_file=config['source_file'], output_dir=config['output_dir'])

    # Create mock files and directories
    mock_files = {
        'avro': ['file1.avro'],
        'parquet': ['file2.parquet'],
        'feather': ['file3.feather']
    }
    
    for folder, files in mock_files.items():
        dir_path = os.path.join(config['output_dir'], folder)
        os.makedirs(dir_path, exist_ok=True)
        for file in files:
            with open(os.path.join(dir_path, file), 'w') as f:
                f.write("dummy data")

    # Mock os.unlink, os.rmdir, os.path.islink to raise an exception
    with patch('os.unlink', side_effect=Exception("Mock unlink error")), \
         patch('os.rmdir'), \
         patch('os.path.islink', return_value=False), \
         patch.object(bench.console, 'print') as mock_console_print:
        
        bench.clear_partitions()

        # Ensure that the correct error message was printed for each file
        for folder, files in mock_files.items():
            for file in files:
                expected_message = f"[red]Failed to delete {os.path.join(config['output_dir'], folder, file)}. Reason: Mock unlink error"
                mock_console_print.assert_any_call(expected_message)