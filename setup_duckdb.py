import duckdb
import pandas as pd

# Create a persistent DuckDB database in the project folder
con = duckdb.connect("water_quality.duckdb")

# Read parquet files with pandas first, then insert into DuckDB
# This avoids Windows path issues in DuckDB
datasets = {
    "raw_zipcheckup":   r"C:\data\water_quality_zipcheckup.parquet",
    "raw_ccr_enriched": r"C:\data\water_quality_ccr_enriched.parquet",
    "raw_l3_metrics":   r"C:\data\water_quality_l3_metrics.parquet",
}

for table_name, path in datasets.items():
    df = pd.read_parquet(path)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    col_count = con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{table_name}'").fetchone()[0]
    print(f"  {table_name}: {row_count:,} rows, {col_count} columns")

print("\nDone! Open water_quality.duckdb in VSCode to browse the data.")
con.close()
