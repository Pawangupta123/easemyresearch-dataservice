"""Install DuckDB's S3 extensions (httpfs, aws) — run once when the image is built:

    uv run python -m scripts.install_duckdb_extensions

The reader never downloads anything during a request (autoinstall is off), so a host without
internet access still reads S3 once this has run.
"""

from dataservice.storage.reader import install_extensions

if __name__ == "__main__":
    install_extensions()
    print("DuckDB extensions installed")
