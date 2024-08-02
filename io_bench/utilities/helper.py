import polars as pl

def print_wide(df: pl.DataFrame):
    num_cols = len(df.columns)
    chunk_size = 8

    for i in range(0, num_cols, chunk_size):
        selected_columns = df.columns[i : i + chunk_size]
        print(df.select(selected_columns))
