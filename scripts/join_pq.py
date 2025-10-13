import duckdb
import glob
import os

SRC_CHUNKS = glob.glob("out/parquet/*.parquet")
MOVES_GLOB = "out/parquet_moves/*.parquet"
OUT_DIR = "out/parquet_joined"

os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect()

# Create a view for moves once
con.execute(f"CREATE OR REPLACE VIEW moves AS SELECT * FROM parquet_scan('{MOVES_GLOB}')")

for i, src in enumerate(SRC_CHUNKS):
    out_file = f"{OUT_DIR}/joined_chunk_{i:03d}.parquet"
    print(f"Processing {src} -> {out_file}")

    sql = f"""
    COPY (
        SELECT
            * EXCLUDE (utc_date, utc_time, white_rating_diff, black_rating_diff, white_elo, black_elo),
            CAST(NULLIF(white_elo,'') AS INTEGER) AS white_elo,
            CAST(NULLIF(black_elo,'') AS INTEGER) AS black_elo,
            CASE
                WHEN split_part(event, ' ', 1) IN ('Blitz','Bullet','UltraBullet','Correspondence','Classical')
                    THEN split_part(event, ' ', 1)
                ELSE split_part(event, ' ', 2)
            END AS game_type,
            CAST(replace(utc_date, '.', '-') || ' ' || utc_time AS TIMESTAMP) AS game_datetime,
            CAST(NULLIF(white_rating_diff, '') AS INT32) AS white_rating_diff,
            CAST(NULLIF(black_rating_diff, '') AS INT32) AS black_rating_diff,
            CAST(NULLIF(NULLIF(split_part(timecontrol, '+', 1), ''), '-') AS INT32) AS initial_time_seconds,
            CAST(NULLIF(NULLIF(split_part(timecontrol, '+', 2), ''), '-') AS INT32) AS increment_seconds
        FROM parquet_scan('{src}')
        LEFT JOIN moves USING (game_id)
        WHERE white_title != 'BOT' AND black_title != 'BOT'
        )
        TO '{out_file}' (FORMAT PARQUET, COMPRESSION 'snappy');
    """

    con.execute(sql)

con.close()
print("Done.")
