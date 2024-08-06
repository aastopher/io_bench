import os
import plotly.express as px
import polars as pl
from rich.console import Console
from typing import List
from io_bench.utilities.helper import print_wide

console = Console()

def generate_report(benchmark_results: List['IOBench'], dir: str) -> None:
    """
    Generate a report from benchmark results.

    Args:
        benchmark_results (List[IOBench]): List of benchmark results.
        dir (str): Directory to save the report.
    """
    summary_metrics = [
        'total_time', 'mean_time_per_row', 'mean_cpu_usage', 
        'mean_thread_count', 'rows_per_sec', 'params_per_sec', 
        'total_rows', 'max_thread_count', 'max_cpu_usage'
    ]

    # Separate results based on suffix
    suffixes = set(bench.id.split('_')[-1] for bench in benchmark_results)
    summary_data = {}

    def create_summary_data(results, suffix=''):
        data = {f"{metric}{suffix}": [] for metric in summary_metrics}
        data[f'id{suffix}'] = []
        for bench in results:
            data[f'id{suffix}'].append(f'{bench.id}: {bench.summary["total_rows"]} rows')
            for metric in summary_metrics:
                data[f"{metric}{suffix}"].append(float(bench.summary[metric]))
        return data

    for suffix in suffixes:
        results_with_suffix = [bench for bench in benchmark_results if bench.id.endswith(suffix)]
        summary_data.update(create_summary_data(results_with_suffix, suffix=f'_{suffix}'))

    # Combine data
    summary_df = pl.DataFrame(summary_data)

    console.print("Summary Metrics:")
    print_wide(summary_df)

    summary_report_html = "<html><head></head><body>"

    for metric in summary_metrics:
        for suffix in suffixes:
            fig = px.bar(summary_df, x=f'id_{suffix}', y=f'{metric}_{suffix}', color=f'id_{suffix}', title=f'{metric.replace("_", " ").title()} by File Type')
            summary_report_html += f"<div>{fig.to_html(full_html=False)}</div>"

    summary_report_html += "</body></html>"

    os.makedirs(dir, exist_ok=True)
    with open(f'{dir}/summary_report.html', 'w') as f:
        f.write(summary_report_html)

    # Prepare data polling metrics
    polling_metrics_data = {
        'id': [],
        'time': [],
        'thread_count': [],
        'cpu_usage': []
    }

    for bench in benchmark_results:
        time_interval = 0.1 
        for i, metric in enumerate(bench.polling_metrics):
            polling_metrics_data['id'].append(bench.id)
            polling_metrics_data['time'].append(i * time_interval)
            polling_metrics_data['thread_count'].append(metric['total_threads']) 
            polling_metrics_data['cpu_usage'].append(metric['cpu_usage'])

    raw_metrics_df = pl.DataFrame(polling_metrics_data).to_pandas()

    polling_report_html = "<html><head></head><body>"

    for suffix in suffixes:
        metrics_with_suffix = raw_metrics_df[raw_metrics_df['id'].str.endswith(suffix)]
        thread_fig = px.line(metrics_with_suffix, x='time', y='thread_count', color='id', title=f'Thread Count over Time')
        cpu_fig = px.line(metrics_with_suffix, x='time', y='cpu_usage', color='id', title=f'CPU Usage over Time')
        polling_report_html += f"<div>{cpu_fig.to_html(full_html=False)}</div>"
        polling_report_html += f"<div>{thread_fig.to_html(full_html=False)}</div>"

    polling_report_html += "</body></html>"

    with open(f'{dir}/polling_report.html', 'w') as f:
        f.write(polling_report_html)
