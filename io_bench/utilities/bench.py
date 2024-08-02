import time
import threading
import psutil
from subprocess import check_output
import statistics
from rich.progress import Progress

class ContinuousMonitor:
    def __init__(self, interval=0.1):
        self.interval = interval
        self.active = True
        self.metrics = []

    def start(self):
        self.monitoring_thread = threading.Thread(target=self.monitor)
        self.monitoring_thread.start()

    def stop(self):
        self.active = False
        self.monitoring_thread.join()

    def monitor(self):
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
    def get_thread_count():
        thread_count_map = {}
        for pid in map(int, check_output(["pgrep", "-f", 'python']).split()):
            try:
                thread_count_map[pid] = psutil.Process(pid).num_threads()
            except psutil.NoSuchProcess:
                continue  # Skip processes that have terminated
        return thread_count_map

class IOBench:
    def __init__(self, parser, columns: list = None, num_runs: int = 10, id: str = None):
        self.parser = parser
        self.num_runs = num_runs
        self.id = id
        self.columns = columns
        self.summary = {}
        self.polling_metrics = []

    def benchmark(self):
        return self._run_benchmark()

    def _run_benchmark(self):
        monitor = ContinuousMonitor()
        monitor.start()  # Start continuous monitoring

        rows = []
        params = []
        times = []

        with Progress() as progress:
            run_task = progress.add_task(f'[green]Benchmarking {self.parser.__class__.__name__}', total=self.num_runs)

            for i in range(self.num_runs):
                progress.update(run_task, description=f'[magenta]({i+1}/{self.num_runs}) - [green]{self.parser.__class__.__name__}: [blue]Reading files...')

                start_benchmark_time = time.perf_counter()
                if self.columns:
                    raw_data = self.parser.to_polars(columns=self.columns)
                else:
                    raw_data = self.parser.to_polars()
                end_benchmark_time = time.perf_counter()

                rows.append(len(raw_data))
                params.append(sum(len(row) for row in raw_data.rows()))
                times.append(end_benchmark_time - start_benchmark_time)

                progress.update(run_task, advance=1)

        monitor.stop()  # Stop continuous monitoring

        # Use the collected metrics directly
        self.polling_metrics = monitor.metrics

        total_time = sum(times)
        mean_time_per_row = statistics.mean(times) / statistics.mean(rows) if rows else 0
        mean_cpu_usage = statistics.mean([metric['cpu_usage'] for metric in monitor.metrics])
        mean_thread_count = statistics.mean([metric['total_threads'] for metric in monitor.metrics])
        rows_per_sec = statistics.mean(rows) / statistics.mean(times) if times else 0
        params_per_sec = statistics.mean(params) / statistics.mean(times) if times else 0

        self.summary = {
            'id': self.id,
            'total_time': total_time,
            'mean_time_per_row': mean_time_per_row,
            'mean_cpu_usage': mean_cpu_usage,
            'mean_thread_count': mean_thread_count,
            'rows_per_sec': rows_per_sec,
            'params_per_sec': params_per_sec,
            'total_rows': sum(rows),
            'max_thread_count': max([metric['total_threads'] for metric in monitor.metrics]),
            'max_cpu_usage': max([metric['cpu_usage'] for metric in monitor.metrics]),
        }

        return self
