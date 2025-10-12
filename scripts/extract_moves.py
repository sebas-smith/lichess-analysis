#!/usr/bin/env python3
# extract_games_minimal.py
# Streams a lichess .pgn.zst and writes a minimal games parquet (one row per game).

import io, os, argparse, uuid
import chess.pgn
import zstandard as zstd
import pandas as pd
import traceback

"""
FIELDS = [
    "game_id","moves_raw", "ECO", "Site"
]
"""

CHUNK_GAMES = 2_000_000




def san_moves_from_game(game):
    board = game.board()
    san_tokens = []
    for mv in game.mainline_moves():
        try:
            san = board.san(mv)
        except Exception:
            san = mv.uci()
        san_tokens.append(san)
        board.push(mv)
    return " ".join(san_tokens)


def process_game(game, exporter, game_index=None):
    """
    Try to extract full moves_raw (including comments).
    On MemoryError (huge game), fall back to SAN-only, then to truncation.
    Returns dict for one row.
    """
    h = game.headers
    site = h.get("Site", "")
    gid = site.rsplit("/", 1)[-1] if site else str(uuid.uuid4())[:8]
    eco = h.get("ECO", "")

    try:
        moves_san = san_moves_from_game(game)
    except Exception:
        moves_san = "<ERROR_SAN>"
        print(f"Warning: SAN extraction error at idx={game_index}")
        traceback.print_exc()

    # Try full raw moves (comments). If it OOMs, fallback to moves_san.
    try:
        moves_raw = game.accept(exporter)  # prefer full raw (with {comments})
        moves_kind = "raw"
    except MemoryError:
        moves_raw = moves_san  # store san so customers won't see NULL, or use placeholder if you prefer
        moves_kind = "san_only"
        print(f"Warning: MemoryError for moves_raw at idx={game_index}; stored SAN-only fallback.")
    except Exception as e:
        moves_raw = "<ERROR_EXTRACTING_RAW>"
        moves_kind = "error"
        print(f"Warning: Exception extracting raw moves at idx={game_index}: {e}")
        traceback.print_exc()

    return {
        "game_id": gid,
        "moves_raw": moves_raw,
        "moves_san": moves_san,
        "moves_kind": moves_kind,
        "ECO": eco,
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

    exporter = chess.pgn.StringExporter(headers=False, variations=False, comments=True)

    with open(zst_path, "rb") as fh, ctx.stream_reader(fh) as reader, \
         io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="\n") as text:
        while True:
            game = chess.pgn.read_game(text)
            if game is None:
                break
            row = process_game(game, exporter, count)
            buf.append(row)
            count += 1

            if count % 500 == 0:
                print(f"Processed {count} games... buffer size {len(buf)}")

            # flush to parquet when buffer is large
            if len(buf) >= CHUNK_GAMES:
                df = pd.DataFrame(buf)
                fname = os.path.join(out_dir, f"moves_chunk_{file_idx:05d}.parquet")
                df.to_parquet(fname, index=False)
                print(f"Wrote {len(df)} games to {fname}")
                file_idx += 1
                buf = []

            if sample_games and count >= sample_games:
                break

    # final flush
    if buf:
        df = pd.DataFrame(buf)
        fname = os.path.join(out_dir, f"moves_chunk_{file_idx:05d}.parquet")
        df.to_parquet(fname, index=False)
        print(f"Wrote {len(df)} games to {fname}")
        file_idx += 1

    print("Done. Games written (approx):", count, "parquet files:", file_idx)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("zst_path", help="path to the .pgn.zst file")
    p.add_argument("--out", default=r"out\parquet_moves", help="output directory for parquet chunks")
    p.add_argument("--sample-games", type=int, default=None, help="if set, stop after this many games (for quick test)")
    args = p.parse_args()
    stream_to_parquet(args.zst_path, args.out, args.sample_games)
