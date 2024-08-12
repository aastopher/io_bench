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
        'total_time', 
        'params_per_sec',
        'params_per_mb',  
        'mean_cpu_usage', 
        'mean_thread_count', 
    ]

    summary_data = {}

    def create_summary_data(results):
        data = {metric: [] for metric in summary_metrics}
        data['parser_id'] = []
        for bench in results:
            data['parser_id'].append(f'{bench.id}: {bench.summary["total_params"]} params')
            for metric in summary_metrics:
                data[metric].append(float(bench.summary[metric]))
        return data

    summary_data.update(create_summary_data(benchmark_results))

    # Combine data
    summary_df = pl.DataFrame(summary_data)

    console.print("Summary Metrics:")
    print_wide(summary_df)

    summary_report_html = "<html><head></head><body>"

    for metric in summary_metrics:
        fig = px.bar(summary_df.to_pandas(), x='parser_id', y=metric, color='parser_id', title=f'{metric.replace("_", " ").title()} by Parser')
        summary_report_html += f"</br><div>{fig.to_html(full_html=False)}</div>"

    summary_report_html += "</body></html>"

    os.makedirs(dir, exist_ok=True)
    with open(f'{dir}/summary_report.html', 'w') as f:
        f.write(summary_report_html)

    # Prepare data polling metrics
    polling_metrics_data = {
        'parser_id': [],
        'time': [],
        'thread_count': [],
        'cpu_usage': []
    }

    for bench in benchmark_results:
        time_interval = 0.1 
        for i, metric in enumerate(bench.polling_metrics):
            polling_metrics_data['parser_id'].append(bench.id)
            polling_metrics_data['time'].append(i * time_interval)
            polling_metrics_data['thread_count'].append(metric['total_threads']) 
            polling_metrics_data['cpu_usage'].append(metric['cpu_usage'])

    raw_metrics_df = pl.DataFrame(polling_metrics_data).to_pandas()

    polling_report_html = "<html><head></head><body>"

    thread_fig = px.line(raw_metrics_df, x='time', y='thread_count', color='parser_id', title='Thread Count over Time')
    cpu_fig = px.line(raw_metrics_df, x='time', y='cpu_usage', color='parser_id', title='CPU Usage over Time')
    polling_report_html += f"<div>{cpu_fig.to_html(full_html=False)}</div>"
    polling_report_html += f"<div>{thread_fig.to_html(full_html=False)}</div>"

    polling_report_html += "</body></html>"

    with open(f'{dir}/polling_report.html', 'w') as f:
        f.write(polling_report_html)
