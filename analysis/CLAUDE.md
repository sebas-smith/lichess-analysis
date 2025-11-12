# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with analysis code.

## Graphs

All graphs should use these colours when different time controls are plotted:

colours = {
"Classical":"#9467bd",
"Rapid":"#d62728",
"Blitz":"#2ca02c",
"Bullet":"#ff7f0e",
"UltraBullet":"#1f77b4",
}

## Notebooks

When a new notebook is created correct paths to the data should be set. e.g.

project_root = Path.cwd().parent
games_path = project_root/"out"/"cleaned_games"
terminations_path = project_root/"out"/"terminations"

if both "cleaned_games" and "terminations" are needed.

Only what is needed should be used

Then a view should be made e.g.

query = f"""
CREATE OR REPlACE VIEW games AS
SELECT game_id, game_datetime, white, black, result, game_type
FROM parquet_scan('{games_path}/\*.parquet')
"""
con.execute(query)

# SQL

SQL files should be created in the analysis\sql folder and then used in the notebook through

sql_file = Path(r"sql/filename.sql")
query = sql_file.read_text()

streaks_df = con.execute(query).fetchdf()
