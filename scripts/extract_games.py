#!/usr/bin/env python3
# extract_games_minimal.py
# Streams a lichess .pgn.zst and writes a minimal games parquet (one row per game).

import io, os, argparse, uuid
import chess.pgn
import zstandard as zstd
import pandas as pd

"""
FIELDS = [
    "game_id","utc_date","utc_time","white","black","white_elo","black_elo",
    "white_rating_diff","black_rating_diff","white_title","black_title",
    "result","termination","timecontrol","opening","site","event"
]
"""

CHUNK_GAMES = 2_000_000


def process_game(game):
    h = game.headers
    site = h.get("Site", "")
    gid = site.rsplit("/", 1)[-1] if site else str(uuid.uuid4())[:8]
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
        "site": site,
        "event": h.get("Event",""),
    }


def stream_to_parquet(zst_path, out_dir, sample_games=None):
    """
    Stream a .pgn.zst and write partitioned parquet files to out_dir.
    Usage:
      stream_to_parquet("data/lichess.pgn.zst", "parquet_out", sample_games=5000)
    """
    os.makedirs(out_dir, exist_ok=True)
    ctx = zstd.ZstdDecompressor()
    buf = []
    file_idx = 0
    count = 0

    with open(zst_path, "rb") as fh, ctx.stream_reader(fh) as reader, \
         io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="\n") as text:
        while True:
            game = chess.pgn.read_game(text)
            if game is None:
                break
            row = process_game(game)
            buf.append(row)
            count += 1

            if count % 5000 == 0:
                print(f"Processed {count} games... buffer size {len(buf)}")

            # flush to parquet when buffer is large
            if len(buf) >= CHUNK_GAMES:
                df = pd.DataFrame(buf)
                fname = os.path.join(out_dir, f"chunk_{file_idx:05d}.parquet")
                df.to_parquet(fname, index=False)
                print(f"Wrote {len(df)} games to {fname}")
                file_idx += 1
                buf = []

            if sample_games and count >= sample_games:
                break

    # final flush
    if buf:
        df = pd.DataFrame(buf)
        fname = os.path.join(out_dir, f"chunk_{file_idx:05d}.parquet")
        df.to_parquet(fname, index=False)
        print(f"Wrote {len(df)} games to {fname}")
        file_idx += 1

    print("Done. Games written (approx):", count, "parquet files:", file_idx)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("zst_path", help="path to the .pgn.zst file")
    p.add_argument("--out", default="parquet", help="output directory for parquet chunks")
    p.add_argument("--sample-games", type=int, default=None, help="if set, stop after this many games (for quick test)")
    args = p.parse_args()
    stream_to_parquet(args.zst_path, args.out, args.sample_games)
