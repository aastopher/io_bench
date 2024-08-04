import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="io_bench",
    version="0.1.0",
    author="Aaron Stopher",
    packages=setuptools.find_packages(include=["io_bench"]),
    description="IO Bench is a library designed to benchmark the performance of standard flat file formats and partitioning schemes.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aastopher/io_bench",
    project_urls={
        "Documentation": "https://io_bench.readthedocs.io",
        "Bug Tracker": "https://github.com/aastopher/io_bench/issues",
    },
    keywords=['arrow', 'avro', 'feather', 'parquet', 'polars', 'utils','performance counter', 'benchmark'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)