import duckdb
import os

SRC_FILE = "out/parquet_joined/joined_chunk_000.parquet"
TERMINATIONS_FILE = "out/parquet_terminations/moves_chunk_00000.parquet"
OUT_DIR = "out/parquet_joined_terminations"

os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect()

# Create a view for terminations


out_file = f"{OUT_DIR}/joined_chunk_terminations.parquet"
print(f"Processing {SRC_FILE} -> {out_file}")

sql = f"""
COPY (
    SELECT
        * EXCLUDE (terminations.game_id, terminations.termination, end_reason),
        CASE
            WHEN end_reason = 'unknown' THEN 'Unterminated'
            ELSE end_reason
        END AS end_reason
    FROM parquet_scan('{SRC_FILE}') as original
    LEFT JOIN parquet_scan('{TERMINATIONS_FILE}') as terminations
    ON original.game_id = terminations.game_id
    )
    TO '{out_file}' (FORMAT PARQUET, COMPRESSION 'snappy');
"""

con.execute(sql)

con.close()
print("Done.")
