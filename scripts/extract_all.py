#!/usr/bin/env python3
# extract_all.py
"""
Stream a .pgn.zst file and write partitioned parquet files with columns:

FIELDS = [
    "game_id", "utc_date", "utc_time", "white", "black", "white_elo", "black_elo", "white_rating_diff", "black_rating_diff", "white_title", "black_title", "result", "termination", "timecontrol", "opening", "eco", "event", "moves_san"
]


Usage example:
python scripts\extract_all.py in\lichess_db_standard_rated_2025-09.pgn.zst --out out/raw_games --chunk-games 500000 --sample-games 10000000
"""

from __future__ import annotations

import argparse
import io
import logging
import traceback
from pathlib import Path

import chess.pgn
import pandas as pd
import zstandard as zstd

DEFAULT_CHUNK_GAMES = 500_000
FNAME_PAD = 4  # width for chunk file numbers
compression = "snappy"
parquet_engine = "pyarrow"

logger = logging.getLogger(__name__)

def san_moves_from_game(game: chess.pgn.Game) -> str:
    """Return moves in SAN for the mainline of `game` as a single space-separated string."""
    board = game.board()
    tokens: list[str] = []
    for mv in game.mainline_moves():
        san = board.san(mv)
        tokens.append(san)
        board.push(mv)
    return " ".join(tokens)


def process_game(game: chess.pgn.Game, game_index: int | None = None) -> dict[str, str]:
    """
    Extract fields from a chess.pgn.Game object.
    Returns a dict with all keys but replacing site with game_id as they are all unique and all played on lichess

    game_index: optional integer for logging context (may be None).
    """
    h = game.headers
    site = h.get("Site", "")
    gid = site.rsplit("/", 1)[-1] if site else None
    try:
        moves_san = san_moves_from_game(game)
    except Exception:
        moves_san = "<ERROR_SAN>"
        logger.warning("SAN extraction error at idx=%s", game_index)
        logger.debug(traceback.format_exc())

    return {
        "game_id": gid,
        "utc_date": h.get("UTCDate", ""),
        "utc_time": h.get("UTCTime",""),
        "white": h.get("White",""),
        "black": h.get("Black",""),
        "white_elo": h.get("WhiteElo",""),
        "black_elo": h.get("BlackElo",""),
        "white_rating_diff": h.get("WhiteRatingDiff", ""),
        "black_rating_diff": h.get("BlackRatingDiff", ""),
        "white_title": h.get("WhiteTitle", ""),
        "black_title": h.get("BlackTitle", ""),
        "result": h.get("Result",""),
        "termination": h.get("Termination",""),
        "timecontrol": h.get("TimeControl",""),
        "opening": h.get("Opening",""),
        "eco" : h.get("ECO", ""),
        "event": h.get("Event",""),
        "moves_san": moves_san
    }
    
def stream_to_parquet(
    zst_path: str | Path,
    out_dir: str | Path,
    chunk_games: int = DEFAULT_CHUNK_GAMES,
    sample_games: int | None = None,
) -> None:
    """
    Stream a .pgn.zst and write partitioned parquet files to out_dir.
    """
    zst_path = Path(zst_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


    
    ctx = zstd.ZstdDecompressor()
    buf: list[dict[str, str]] = []
    file_idx = 0
    count = 0

    logger.info("Starting stream: %s -> %s (chunk=%d)", zst_path, out_dir, chunk_games)

    with open(zst_path, "rb") as fh, ctx.stream_reader(fh) as reader, \
         io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="\n") as text:
        process = process_game
        while True:
            game = chess.pgn.read_game(text)
            if game is None:
                break
            row = process(game, count)
            buf.append(row)
            count += 1

            if count % 10000 == 0:
                logger.info("Processed %d games... buffer size %d", count, len(buf))

            if len(buf) >= chunk_games:
                df = pd.DataFrame(buf)
                fname = out_dir / f"chunk_{file_idx:0{FNAME_PAD}d}.parquet"
                try:
                    df.to_parquet(fname, index=False, compression=compression, engine=parquet_engine)
                except Exception:
                    logger.exception("Failed writing parquet %s", fname)
                    raise
                logger.info("Wrote %d games to %s", len(df), fname)
                file_idx += 1
                buf = []

            if sample_games and count >= sample_games:
                logger.info("Reached sample_games limit (%d). Stopping early.", sample_games)
                break

    # final flush
    if buf:
        df = pd.DataFrame(buf)
        fname = out_dir / f"chunk_{file_idx:0{FNAME_PAD}d}.parquet"
        try:
            df.to_parquet(fname, index=False, compression=compression, engine=parquet_engine)
        except Exception:
            logger.exception("Failed writing parquet %s", fname)
            raise
        logger.info("Wrote %d games to %s", len(df), fname)
        file_idx += 1

    logger.info("Done. Games written (approx): %d ; parquet files: %d", count, file_idx)


def main() -> None:
    p = argparse.ArgumentParser(description="Stream .pgn.zst -> partitioned parquet")
    p.add_argument("zst_path", help="path to the .pgn.zst file")
    p.add_argument("--out", default="out/parquet_all", help="output directory for parquet chunks")
    p.add_argument("--chunk-games", type=int, default=DEFAULT_CHUNK_GAMES,
                   help="flush every N games (default 100000)")
    p.add_argument("--sample-games", type=int, default=None, help="stop after N games (for quick tests)")
    p.add_argument("--verbose", action="store_true", help="enable verbose logging")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    stream_to_parquet(
        args.zst_path,
        args.out,
        chunk_games=args.chunk_games,
        sample_games=args.sample_games,
    )


if __name__ == "__main__":
    main()