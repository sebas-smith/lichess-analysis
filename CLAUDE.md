# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## General notes

Never add extra bits if they are not required to complete the given task. If you are unsure then ask.

Wherever possible, if you are creating new code, keep the style similar to other code in the repository.

For a newline write /n not //n

## Project Overview

This project analyzes chess game data from Lichess to investigate **tilt** - the psychological phenomenon where players perform worse after losses. The main research question: Do losing streaks affect a player's probability of losing their next game?

The project processes ~10 million chess games through an ETL pipeline and performs statistical analysis on patterns in draw rates, game endings, and win/loss streaks across different rating levels and time controls.

## Data Processing Pipeline

The workflow follows a strict order. Each step produces parquet files consumed by the next:

1. **Extract** (`scripts/extract_all.py`)

   - Input: `in/lichess_db_standard_rated_2025-09.pgn.zst` (28GB compressed PGN)
   - Output: `out/raw_games/*.parquet`
   - Streams compressed PGN, extracts metadata and moves in SAN notation
   - Processes in chunks of 500,000 games

2. **Clean** (`scripts/clean.py`)

   - Input: `out/raw_games/*.parquet`
   - Output: `out/cleaned_games/*.parquet`
   - Removes bot games, invalid terminations
   - Parses time controls, combines UTC timestamps, extracts game_type
   - Adds `mated` boolean field

3. **Extract Game Endings** (`scripts/extract_results.py`)
   - Input: `out/cleaned_games/*.parquet`
   - Output: `out/terminations/*.parquet`
   - Replays games using python-chess library
   - Determines exact ending: checkmate, resignation, timeout, stalemate, threefold repetition, etc.
   - Creates `end_code` (0-10) and `end_reason` columns

## Key Technologies

- **DuckDB**: Primary analysis engine. Used in notebooks to query parquet files with SQL
- **Parquet**: All intermediate and final data stored as parquet with snappy compression
- **python-chess**: Game replay and move validation
- **zstandard**: Decompresses .pgn.zst input files

## Running Scripts

All scripts should be run from the project root directory.

### Full Pipeline Example

```bash
# Extract raw games (use --sample-games for testing)
python scripts\extract_all.py in\lichess_db_standard_rated_2025-09.pgn.zst --out out/raw_games --chunk-games 500000 --sample-games 1000000

# Clean data
python scripts\clean.py

# Extract game endings
python scripts\extract_results.py

# Create per-player view
python scripts\per_player.py
```

### Analysis

Jupyter notebooks in `analysis/` directory:

- `tilt.ipynb` - Loss streak analysis (main research question)
- `draws.ipynb` - Draw rate patterns across ratings and time controls

Notebooks use DuckDB to query parquet files via `parquet_scan()` and create views.

## SQL Query Pattern

Analysis SQL files in `analysis/sql/` are loaded and executed from notebooks:

```python
sql_file = Path(r"sql/streaks.sql")
query = sql_file.read_text()
df = con.execute(query).fetchdf()
```

### Important SQL Queries

- **`streaks.sql`**: Calculates loss streak statistics using window functions

  - Stacks white/black players into single player-centric view
  - Orders games chronologically per player and game_type
  - Computes consecutive previous losses
  - Aggregates to show probability of losing next game after N losses

- **`lossrate.sql`**: Baseline draw rates by game_type

## Data Schema

### Cleaned Games (out/cleaned_games/)

- `game_id`, `game_datetime`, `white`, `black`
- `white_elo`, `black_elo`, `white_rating_diff`, `black_rating_diff`
- `white_title`, `black_title`
- `result`: "1-0", "0-1", or "1/2-1/2"
- `termination`: "Normal", "Time forfeit", etc.
- `initial_time_seconds`, `increment_seconds`
- `opening`, `eco` (ECO code)
- `event`, `game_type` (Bullet, Blitz, Rapid, Classical, UltraBullet)
- `moves_san`: space-separated SAN moves
- `mated`: boolean (True if last move ends with '#')

### Terminations (out/terminations/)

- `game_id`
- `end_code`: 0=unknown, 1=checkmate, 2=resignation, 3=timeout_win, 4=timeout_draw_by_insufficient, 5=stalemate, 6=threefold_repetition, 7=fifty_move_rule, 8=insufficient_material_claimed, 9=insufficient_material_automatic, 10=agreement_draw
- `end_reason`: string version of end_code

### Players (out/players/)

Player-centric normalized view with results from each player's perspective.

## Important Findings

From the analysis performed so far:

1. **Tilt Effect**: Loss streaks do increase probability of losing the next game, but the effect is subtle and varies by time control
2. **Draw Rates**: Lower-rated players (~800-1200 Elo) have higher draw rates in Classical/Rapid due to **accidental stalemates**
3. **Time Control Effects**: In faster time controls (Bullet/UltraBullet), players flag (lose on time) instead of stalemating
4. **Regression to Mean**: Small alternating pattern in streak data suggests both psychological factors and statistical regression

## Development Setup

```bash
# Create virtual environment
python -m venv environment

# Activate (Windows)
environment\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Architecture Notes

- All data operations use parquet files to handle the large dataset (~10M games)
- Scripts process data in chunks (default 500,000 games per file) to manage memory
- DuckDB provides SQL interface over parquet files without loading into memory
- Player-centric analysis requires "stacking" white and black players into a single view (see `streaks.sql` CTE pattern)
- Window functions are critical for streak calculations - they partition by player and game_type, order by datetime

## File Inspection Utility

Use `checker.py` in root to inspect parquet file contents:

```bash
python checker.py out/cleaned_games/chunk_0000.parquet
```
