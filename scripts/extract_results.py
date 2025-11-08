import os
import re
import argparse

import chess
import pandas as pd
import glob

# End reason mapping (authoritative codes)
END_REASON_MAP = {
    0: "unknown",
    1: "checkmate",
    2: "resignation",
    3: "timeout_win",
    4: "timeout_draw_by_insufficient",
    5: "stalemate",
    6: "threefold_repetition",
    7: "fifty_move_rule",
    8: "insufficient_material_claimed",
    9: "insufficient_material_automatic",
    10:"agreement_draw",
}

_trailer_strip = re.compile(r'[+#]+$')
def tokens_from_san_fast(san_str):
    if san_str is None:
        return []
    toks = san_str.split()
    return [ _trailer_strip.sub('', t) for t in toks if t]

# single-game analyzer (returns detection booleans) --
def analyze_single_game_row(moves_san, termination, result):
    if termination != 'Normal' or result != '1/2-1/2':
        return {
            "is_stalemate": False,
            "is_threefold": False,
            "is_fifty_move": False,
            "is_insufficient_material": False
        }

    
    
    board = chess.Board()
    toks = tokens_from_san_fast(moves_san)
    seen = {board._transposition_key(): 1}

    is_stalemate = False
    is_threefold = False
    is_fifty_move = False
    is_insufficient_material = False

    for san in toks:
        if not san:
            continue
        try:
            board.push_san(san)
        except Exception:
            break

        if board.is_stalemate():
            is_stalemate = True
            break
        
        key = board._transposition_key()
        cnt = seen.get(key, 0) + 1
        seen[key] = cnt
        if cnt >= 3:
            is_threefold = True
            break

        if getattr(board, "halfmove_clock", 0) >= 100:
            is_fifty_move = True
            break
        
        if board.is_insufficient_material():
            is_insufficient_material = True
            
    return {
        "is_stalemate": bool(is_stalemate),
        "is_threefold": bool(is_threefold),
        "is_fifty_move": bool(is_fifty_move),
        "is_insufficient_material": bool(is_insufficient_material)
    }

def pick_end_reason_code(dets, termination, result, mated):
    # 1: checkmate
    if bool(mated):
        return 1
    
    # 2: Resignation
    if termination == 'Normal' and result in ('1-0', '0-1'):
        return 2

    # 3 & 4: time forfeit cases
    if termination == 'Time forfeit':
        if result in ('1-0', '0-1'):
            return 3
        if result == '1/2-1/2':
            return 4

    #  5: Stalemate
    if dets.get("is_stalemate"):
        return 5

    # 6: threefold
    if dets.get("is_threefold"):
        return 6

    # 7: fifty-move
    if dets.get("is_fifty_move"):
        return 7

    # 8: insufficient material claimed
    if termination == 'Insufficient material':
        return 8
    # 9: insufficient material both
    if dets.get("is_insufficient_material"):
        return 9
    

    # 10: agreement_draw should be anything else that's not already a draw
    if result == '1/2-1/2':
        return 10

    # 0: Other
    return 0

def process_row(row, row_index = None):
    gid = row.get("game_id")
    moves_san = row.get("moves_san")
    termination = row.get("termination")
    result = row.get("result")
    mated = row.get("mated")
    
    dets = analyze_single_game_row(moves_san, termination, result)
    
    code = pick_end_reason_code(dets, termination, result, mated)
    
    reason = END_REASON_MAP.get(code, "unknown")
    
    return {
        "game_id": gid,
        "end_code": code,
        "end_reason": reason,
    }


def stream_parquet_to_parquet(input_glob, out_dir, sample_games=None):
    """
    Reads all Parquet files matching input_glob and writes a corresponding output file
    (1:1 mapping) into out_dir.

    Each output parquet will have only the desired processed data
    derived from each input file.
    """
    os.makedirs(out_dir, exist_ok=True)

    print(input_glob)

    files = sorted(glob.glob(input_glob))
    
    print(files)

    for i, parquet_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Reading {parquet_path}")

        df = pd.read_parquet(parquet_path)

        # optional sampling for testing
        if sample_games:
            df = df.iloc[:sample_games]

        # process each row (user-defined function)
        processed = [process_row(row) for _, row in df.iterrows()]
        processed_df = pd.DataFrame(processed)

        # build output filename matching input name
        base_name = os.path.basename(parquet_path)
        out_path = os.path.join(out_dir, base_name)

        processed_df.to_parquet(out_path, index=False, compression="snappy")
        print(f"  → wrote {len(processed_df)} rows to {out_path}")

    print(f"Done. {len(files)} parquet files written to {out_dir}")
    
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--parquet_path", default = "out/cleaned_games/*.parquet", help="path to the input Parquet folder")
    p.add_argument("--out", default=r"out\terminations", help="output directory for parquet chunks")
    p.add_argument("--sample-games", type=int, default=None, help="if set, stop after this many games (for quick test)")
    args = p.parse_args()
    stream_parquet_to_parquet(args.parquet_path, args.out, args.sample_games)
