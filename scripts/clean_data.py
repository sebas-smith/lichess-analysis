#!/usr/bin/env python3
"""
Creates cleaned data into duckdb, in this case gets rid of all games played with a bot in it

Usage (example):


  # full run
python ./scripts/clean_data.py --parquet "out/parquet/chunk_*.parquet" --db data/lichess.duckdb
"""
import argparse
import duckdb
import os

def create_games_nobot(parquet_glob: str, db_file: str):
    print("Opening DuckDB:", db_file)
    con = duckdb.connect(db_file)

    print("Creating/replacing permanent table games_nobot from parquet:", parquet_glob)
    # We materialize into a TABLE so subsequent queries are faster and simpler.
    # We also coerce empty rating strings to NULL via NULLIF(...,'').
    # Note: utc_datetime is kept as text (YYYY-MM-DD HH:MM:SS) for now; you can cast later if needed.
    sql = f"""
    CREATE OR REPLACE TABLE games_nobot AS
    SELECT
      game_id,
      utc_date,
      utc_time,
      -- create a convenient datetime string; replace '.' with '-' in dates like 2017.04.01 -> 2017-04-01
      (REPLACE(utc_date, '.', '-') || ' ' || utc_time) AS utc_datetime,
      white,
      black,
      CAST(NULLIF(white_elo,'') AS INTEGER) AS white_elo,
      CAST(NULLIF(black_elo,'') AS INTEGER) AS black_elo,
      white_title,
      black_title,
      result,
      termination,
      timecontrol,
      opening,
      event
    FROM read_parquet('{parquet_glob}')
    WHERE coalesce(white_title, '') NOT LIKE '%BOT%'
      AND coalesce(black_title, '') NOT LIKE '%BOT%';
    """
    con.execute(sql)

    # Quick sanity checks / prints
    total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}')").fetchone()[0]
    no_bot = con.execute("SELECT COUNT(*) FROM games_nobot").fetchone()[0]
    print(f"Total games in parquet glob: {total}")
    print(f"Games after removing BOT titles: {no_bot}")
    print("Example rows from games_nobot:")
    print(con.execute("SELECT utc_datetime, white, black, white_elo, black_elo, white_title, black_title, result FROM games_nobot LIMIT 5").fetchdf())

    con.close()
    print("Done. games_nobot table materialized in", db_file)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--parquet", required=True, help="Parquet glob (e.g. 'parquet_full/chunk_*.parquet')")
    p.add_argument("--db", default="lichess.duckdb", help="DuckDB file to create/use")
    args = p.parse_args()
    # basic sanity
    if "*" not in args.parquet and not os.path.exists(args.parquet):
        print("Warning: parquet path does not exist and no glob used:", args.parquet)
    create_games_nobot(args.parquet, args.db)
