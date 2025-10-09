#!/usr/bin/env python3
# extract_games_minimal.py
# Streams a lichess .pgn.zst and writes a minimal games CSV (one row per game).

import io, os, argparse, csv, uuid
import chess.pgn
import zstandard as zstd

FIELDS = [
    "game_id","utc_date","utc_time","white","black","white_elo","black_elo",
    "white_rating_diff","black_rating_diff","white_title","black_title",
    "result","termination","timecontrol","opening","site","event"
]


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

def stream_to_csv(zst_path, out_csv, sample_games=None):
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    ctx = zstd.ZstdDecompressor()
    with open(zst_path, "rb") as fh, ctx.stream_reader(fh) as reader, \
         io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="\n") as text:
        writer = csv.DictWriter(open(out_csv,"w",newline="",encoding="utf-8"), fieldnames=FIELDS)
        writer.writeheader()
        count = 0
        while True:
            game = chess.pgn.read_game(text)
            if game is None:
                break
            row = process_game(game)
            writer.writerow(row)
            count += 1
            if sample_games and count >= sample_games:
                break
            if count % 500 == 0:
                print(f"Processed {count} games...")
    print("Done — games written:", count, out_csv)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("zst_path")
    p.add_argument("--out", default="out/games.csv")
    p.add_argument("--sample-games", type=int, default=None)
    args = p.parse_args()
    stream_to_csv(args.zst_path, args.out, args.sample_games)
