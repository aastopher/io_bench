import os
import time
import threading
import psutil
from subprocess import check_output
import statistics
from typing import Any, Dict, List, Optional
from rich.progress import Progress

class ContinuousMonitor:
    def __init__(self, interval: float = 0.1) -> None:
        """
        Initialize the ContinuousMonitor object.

        Args:
            interval (float): Interval in seconds for monitoring.
        """
        self.interval = interval
        self.active = True
        self.metrics = []
        self.monitoring_thread = None

    def start(self) -> None:
        """
        Start continuous monitoring in a separate thread.
        """
        self.monitoring_thread = threading.Thread(target=self.monitor)
        self.monitoring_thread.start()

    def stop(self) -> None:
        """
        Stop continuous monitoring.
        """
        self.active = False
        self.monitoring_thread.join()

    def monitor(self) -> None:
        """
        Monitor system metrics at regular intervals.
        """
        start_time = time.perf_counter()  # Use high-resolution performance counter
        while self.active:
            current_time = time.perf_counter() - start_time
            cpu_usage = psutil.cpu_percent(interval=None)
            thread_count_map = self.get_thread_count()
            total_threads = sum(thread_count_map.values())

            # Append a new record for this polling event
            self.metrics.append({
                'time': current_time,
                'cpu_usage': cpu_usage,
                'total_threads': total_threads,
                'thread_count_map': thread_count_map,
            })

            time.sleep(self.interval)

    @staticmethod
    def get_thread_count() -> Dict[int, int]:
        """
        Get the count of threads for each Python process.

        Returns:
            Dict[int, int]: Mapping of process IDs to thread counts.
        """
        thread_count_map = {}
        for pid in map(int, check_output(["/usr/bin/pgrep", "-f", 'python']).split()):
            try:
                thread_count_map[pid] = psutil.Process(pid).num_threads()
            except psutil.NoSuchProcess:
                continue  # Skip processes that have terminated
        return thread_count_map

class IOBench:
    def __init__(self, parser: Any, columns: Optional[List[str]] = None, num_runs: int = 10, id: Optional[str] = None) -> None:
        """
        Initialize the IOBench object for benchmarking.

        Args:
            parser (Any): Parser object to use for benchmarking.
            columns (Optional[List[str]]): List of columns to select.
            num_runs (int): Number of benchmark runs.
            id (Optional[str]): Benchmark ID.
        """
        self.parser = parser
        self.num_runs = num_runs
        self.id = id
        self.columns = columns
        self.summary = {}
        self.polling_metrics = []

    def benchmark(self) -> 'IOBench':
        """
        Run the benchmark and return the benchmark results.

        Returns:
            IOBench: The benchmark object with results.
        """
        return self._run_benchmark()

    def _run_benchmark(self) -> 'IOBench':
        """
        Run the benchmark.

        Returns:
            IOBench: The benchmark object with results.
        """
        monitor = ContinuousMonitor()
        monitor.start()  # Start continuous monitoring

        rows = []
        params = []
        times = []
        sizes = []

        with Progress() as progress:
            run_task = progress.add_task(f'[green]Benchmarking {self.parser.__class__.__name__}', total=self.num_runs)

            for i in range(self.num_runs):
                progress.update(run_task, description=f'[magenta]({i+1}/{self.num_runs}) - [green]{self.id}: [blue]Reading files...')

                start_benchmark_time = time.perf_counter()
                if self.columns:
                    raw_data = self.parser.to_polars(columns=self.columns)
                else:
                    raw_data = self.parser.to_polars()
                end_benchmark_time = time.perf_counter()

                rows.append(len(raw_data))
                params.append(len(raw_data) * len(raw_data.columns))
                times.append(end_benchmark_time - start_benchmark_time)
                sizes.append(sum(os.path.getsize(f) for f in self.parser.file_paths))

                progress.update(run_task, advance=1)

        monitor.stop()  # Stop continuous monitoring

        self.polling_metrics = monitor.metrics

        total_time = sum(times)
        total_rows = sum(rows)
        total_params = sum(params)
        total_size = sum(sizes)
        mean_cpu_usage = statistics.mean([metric['cpu_usage'] for metric in monitor.metrics])
        mean_thread_count = statistics.mean([metric['total_threads'] for metric in monitor.metrics])
        rows_per_sec = total_rows / total_time if total_time else 0
        params_per_sec = total_params / total_time if total_time else 0
        params_per_mb = total_params / (total_size / (1024 * 1024)) if total_size else 0

        self.summary = {
            'id': self.id,
            'total_time': total_time,
            'mean_cpu_usage': mean_cpu_usage,
            'mean_thread_count': mean_thread_count,
            'rows_per_sec': rows_per_sec,
            'params_per_sec': params_per_sec,
            'total_rows': total_rows,
            'total_params': total_params,
            'max_thread_count': max(metric['total_threads'] for metric in monitor.metrics),
            'max_cpu_usage': max(metric['cpu_usage'] for metric in monitor.metrics),
            'params_per_mb': params_per_mb
        }

        return self