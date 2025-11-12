# Lichess Analysis

A statistical analysis of ~10 million chess games from the Lichess database

## Key Findings

### 1. Tilt Effect Confirmed
Loss streaks **do** increase the probability of losing the next game:
- The effect is subtle but measurable across all time controls
- Varies by game type (Bullet, Blitz, Rapid, Classical)
- Shows alternating pattern at start suggesting both psychological factors and regression to mean
- Considerably larger in Hyperbullet

### 2. Draw Rate Paradox
Lower-rated players (~800-1200 Elo) have **higher** draw rates than slightly higher rated players than them:
- **Cause**: Accidental stalemates
- Lower-rated players accidentally stalemate opponents instead of delivering checkmate
- This pattern does NOT appear in faster time controls (Bullet/UltraBullet) due to flagging (running out of time) usually occuring before mate is possible

### 3. Time Control Effects
- **Faster time controls**: Players flag (lose on time) instead of stalemating
- **Slower time controls**: More time allows accidental stalemates at lower ratings
- Different psychological patterns emerge in different time pressures

## Technologies Used

- **DuckDB** - SQL analysis engine for querying parquet files
- **Parquet** - Columnar storage format with snappy compression
- **python-chess** - Chess game replay and move validation
- **zstandard** - Decompression of .pgn.zst input files
- **pandas** - Data manipulation and analysis
- **matplotlib** - Data visualization
- **Jupyter Notebooks** - Interactive analysis environment

## Installation & Setup

### Prerequisites
- Python 3.10+
- ~30GB disk space for data

### Setup Instructions

```bash
# Clone the repository
git clone <repository-url>
cd lichess-tilt-analysis

# Create virtual environment
python -m venv environment

# Activate virtual environment (Windows)
environment\Scripts\activate

# Activate virtual environment (Unix/MacOS)
source environment/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Data Processing Pipeline

The project follows a strict ETL (Extract, Transform, Load) pipeline. Each step must be run in order:

### 1. Extract Raw Games
Extract game data from compressed PGN file into parquet format.

```bash
python scripts\extract_all.py in\lichess_db_standard_rated_2025-09.pgn.zst --out out/raw_games --chunk-games 500000 --sample-games 1000000
```

- **Input**: `in/lichess_db_standard_rated_2025-09.pgn.zst` (28GB compressed PGN)
- **Output**: `out/raw_games/*.parquet`
- **What it does**: Streams compressed PGN, extracts metadata and moves in SAN notation
- **Options**: Use `--sample-games` for testing with smaller dataset

### 2. Clean Data
Remove invalid games and normalize data format.

```bash
python scripts\clean.py
```

- **Input**: `out/raw_games/*.parquet`
- **Output**: `out/cleaned_games/*.parquet`
- **What it does**:
  - Removes bot games and invalid terminations
  - Parses time controls into `initial_time_seconds` and `increment_seconds`
  - Combines UTC date/time into single timestamp
  - Extracts `game_type` (Bullet, Blitz, Rapid, Classical, UltraBullet)
  - Adds `mated` boolean field (True if last move ends with '#')

### 3. Extract Game Endings
Determine exactly how each game ended by replaying moves.

```bash
python scripts\extract_results.py
```

- **Input**: `out/cleaned_games/*.parquet`
- **Output**: `out/terminations/*.parquet`
- **What it does**:
  - Replays each game using python-chess library
  - Determines exact ending type (checkmate, resignation, timeout, stalemate, etc.)
  - Creates `end_code` (0-10) and `end_reason` columns

**End Reason Codes**:
- 0: Unknown
- 1: Checkmate
- 2: Resignation
- 3: Timeout (win)
- 4: Timeout draw by insufficient material
- 5: Stalemate
- 6: Threefold repetition
- 7: Fifty-move rule
- 8: Insufficient material (claimed)
- 9: Insufficient material (automatic)
- 10: Draw by agreement

## Analysis Notebooks

All analysis is performed in Jupyter notebooks located in the `analysis/` directory. Open them with:

```bash
jupyter notebook analysis/
```

### Main Analysis Notebooks

#### 1. `tilt.ipynb` - Loss Streak Analysis
**Research Question**: Do losing streaks affect a player's probability of losing their next game?

- Uses SQL query from `analysis/sql/streaks.sql`
- Calculates probability of losing next game after N consecutive losses
- Compares across different time controls (UltraBullet, Bullet, Blitz, Rapid)
- Visualizes how loss probability changes with streak length

**Key Visualization**: Line plot showing next game loss percentage vs streak length for each time control

#### 2. `draws.ipynb` - Draw Rate Analysis
**Research Question**: How do draw rates vary by rating level and time control, and why?

- Analyzes overall draw rate vs average Elo
- Breaks down by time control type
- Investigates specific draw types:
  - Stalemate rates by rating
  - Threefold repetition patterns
  - Timeout draws
  - Insufficient material
- Examines checkmate rates and flag (timeout) rates

**Key Discovery**: U-shaped draw rate curve with minimum at ~1200 Elo due to accidental stalemates at lower ratings

#### 3. `openings.ipynb` - Opening Success Analysis
**Research Question**: How do the three main opening moves perform across different rating levels?

- Analyzes 1.e4, 1.d4, and 1.c4 separately (parsed from moves_san field)
- White win percentage vs Elo by opening
- Draw percentage vs Elo by opening
- Shows which openings are more effective at different skill levels

**Key Insight**: Opening success rates vary significantly by rating range, with some openings performing better at specific Elo levels

## Project Structure

```
lichess-tilt-analysis/
├── analysis/                  # Jupyter notebooks for analysis
│   ├── tilt.ipynb            # Loss streak analysis (main research question)
│   ├── draws.ipynb           # Draw rate patterns across ratings
│   ├── openings.ipynb        # Opening success by rating level
│   └── sql/                  # SQL query files
│       ├── streaks.sql       # Loss streak calculations using window functions
│       └── lossrate.sql      # Baseline loss rates by game type
├── scripts/                   # Data processing pipeline
│   ├── extract_all.py        # Extract games from compressed PGN
│   ├── clean.py              # Clean and normalize data
│   └── extract_results.py    # Determine game ending types
├── in/                        # Input data directory
│   └── lichess_db_standard_rated_2025-09.pgn.zst  # Source data (28GB)
├── out/                       # Output data directory
│   ├── raw_games/            # Extracted parquet files
│   ├── cleaned_games/        # Cleaned parquet files
│   └── terminations/         # Game ending details
├── requirements.txt           # Python dependencies
├── CLAUDE.md                 # Project documentation for AI assistants
└── README.md                 # This file
```

## Data Schema

### Cleaned Games (`out/cleaned_games/*.parquet`)

| Field | Type | Description |
|-------|------|-------------|
| `game_id` | string | Unique game identifier |
| `game_datetime` | timestamp | Combined UTC date and time |
| `white` | string | White player username |
| `black` | string | Black player username |
| `white_elo` | int32 | White player rating |
| `black_elo` | int32 | Black player rating |
| `white_rating_diff` | int32 | Rating change for White |
| `black_rating_diff` | int32 | Rating change for Black |
| `white_title` | string | White player title (GM, IM, FM, etc.) |
| `black_title` | string | Black player title |
| `result` | string | Game result: "1-0", "0-1", or "1/2-1/2" |
| `termination` | string | Termination type: "Normal", "Time forfeit", etc. |
| `initial_time_seconds` | int32 | Starting time in seconds |
| `increment_seconds` | int32 | Time increment per move |
| `opening` | string | Full opening name |
| `eco` | string | ECO (Encyclopedia of Chess Openings) code |
| `event` | string | Event type |
| `game_type` | string | Bullet, Blitz, Rapid, Classical, or UltraBullet |
| `moves_san` | string | Space-separated moves in Standard Algebraic Notation |
| `mated` | boolean | True if game ended with checkmate |

### Terminations (`out/terminations/*.parquet`)

| Field | Type | Description |
|-------|------|-------------|
| `game_id` | string | Unique game identifier (joins with cleaned_games) |
| `end_code` | int | Numeric ending type (0-10) |
| `end_reason` | string | Human-readable ending description |

## Architecture Notes

### Efficient Data Handling
- **Parquet format**: Columnar storage enables fast querying without loading full dataset
- **Chunked processing**: Scripts process 500,000 games at a time to manage memory
- **DuckDB integration**: Provides SQL interface over parquet files without loading into memory

### Analysis Patterns
- **Player-centric views**: "Stacking" white and black players into single view for streak analysis
- **Window functions**: Critical for streak calculations - partition by player and game_type, order by datetime
- **Weighted smoothing**: Rolling averages weighted by game count for robust trend visualization

### Performance
- ~10 million games processed
- Dataset size: ~28GB compressed input → ~2GB parquet output
- Query time: Most analyses complete in seconds using DuckDB

## Contributing

This is a research project analyzing historical Lichess data. The analysis pipeline and notebooks document the methodology used.

## License

Data sourced from [Lichess Open Database](https://database.lichess.org/).

## Acknowledgments

- Lichess for providing open chess game data

