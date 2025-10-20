import duckdb
import glob
import os

CHUNKS = glob.glob("out/parquet_all/*.parquet")

OUT_DIR = "out/cleaned_parquet_all"

os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect()



for i, src in enumerate(CHUNKS):
    out_file = f"{OUT_DIR}/cleaned_chunk_{i:03d}.parquet"
    print(f"Processing {src} -> {out_file}")

    sql = f"""
    COPY (
        SELECT
            game_id,
            CAST(replace(utc_date, '.', '-') || ' ' || utc_time AS TIMESTAMP) AS game_datetime,
            white,
            black,
            CAST(NULLIF(white_elo,'') AS INTEGER) AS white_elo,
            CAST(NULLIF(black_elo,'') AS INTEGER) AS black_elo,
            CAST(NULLIF(white_rating_diff, '') AS INT32) AS white_rating_diff,
            CAST(NULLIF(black_rating_diff, '') AS INT32) AS black_rating_diff,
            white_title,
            black_title,
            result,
            termination,
            CAST(NULLIF(NULLIF(split_part(timecontrol, '+', 1), ''), '-') AS INT32) AS initial_time_seconds,
            CAST(NULLIF(NULLIF(split_part(timecontrol, '+', 2), ''), '-') AS INT32) AS increment_seconds,
            opening,
            eco,
            event,
            CASE
                WHEN split_part(event, ' ', 1) IN ('Blitz','Bullet','UltraBullet','Correspondence','Classical', 'Rapid')
                    THEN split_part(event, ' ', 1)
                ELSE split_part(event, ' ', 2)
            END AS game_type,
            moves_san,
            CASE
                WHEN moves_san IS NULL THEN NULL
                WHEN RIGHT(moves_san, 1) = '#' THEN TRUE
                ELSE FALSE
            END AS mated
        FROM parquet_scan('{src}')
        WHERE white_title != 'BOT'
        AND black_title != 'BOT'
        AND termination NOT IN ('Unterminated', 'Rules infraction', 'Abandoned')
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION 'snappy');
    """

    con.execute(sql)

con.close()
print("Done.")
