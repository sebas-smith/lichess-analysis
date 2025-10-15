"""
detect_end_reason.py

Reads parquet files (INPUT_GLOB), streams them in batches, replays cleaned SAN with python-chess,
detects stalemate/threefold/50-move/insufficient-material and timeout/resignation logic,
and writes out shards with one authoritative end_reason_code + end_reason, plus is_draw and winner.

Requires: python-chess, pandas, pyarrow, tqdm
"""

import os
import re
from pathlib import Path
from multiprocessing import Pool, cpu_count

import chess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# ---------------- CONFIG ----------------
INPUT_GLOB = "data/parquet_joined/*.parquet"
OUTPUT_DIR = "data/parquet_results"           # dir to write shards
BATCH_ROWS = 2000
N_WORKERS = max(1, cpu_count() - 2)
MAX_BATCHES = None       # set small integer to test, or None for full run
# ----------------------------------------

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
    8: "insufficient_material",
    9: "agreement_draw",
    10:"abandoned",
    11:"Rules infraction"	
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
        }

    
    
    board = chess.Board()
    toks = tokens_from_san_fast(moves_san)
    seen = {board.transposition_key(): 1}

    is_stalemate = False
    is_threefold = False
    is_fifty_move = False

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
        
        key = board.transposition_key()
        cnt = seen.get(key, 0) + 1
        seen[key] = cnt
        if cnt >= 3:
            is_threefold = True
            break

        if getattr(board, "halfmove_clock", 0) >= 100:
            is_fifty_move = True
            break

    return {
        "is_stalemate": bool(is_stalemate),
        "is_threefold": bool(is_threefold),
        "is_fifty_move": bool(is_fifty_move),
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

    # 8: insufficient material
    if termination == 'Insufficient material':
        return 8
    
    # 10: Abandoned
    if termination == 'Abandoned':
        return 10
    
    # 11: Termination
    if termination == 'Rules infraction':
        return 11

    # 9: agreement_draw should be anything else that's not already a draw
    if result == '1/2-1/2':
        return 9

    # 0: Other
    return 0

# Worker: process pandas chunk and produce output columns
def process_df_chunk(df_chunk, moves_col='moves_san', term_col='termination',
                     result_col='result', mated_col='mated', game_id_col = 'game_id'):
    out = {
        "game_id": [],
        "end_reason_code": [],
        "end_reason": [],
    }

    for idx, row in df_chunk.iterrows():
        moves = row.get(moves_col)
        termination = row.get(term_col)
        result = row.get(result_col)
        mated = row.get(mated_col, False)

        dets = analyze_single_game_row(moves, termination, result)
        code = pick_end_reason_code(dets, termination, result, mated)
        reason = END_REASON_MAP.get(code, "unknown")


        out["end_reason_code"].append(int(code))
        out["end_reason"].append(reason)

    return pd.DataFrame(out, index=df_chunk.index)

# Top-level streaming + multiprocessing coordinator
def run_pipeline(input_glob=INPUT_GLOB, output_dir=OUTPUT_DIR, batch_rows=BATCH_ROWS,
                 n_workers=N_WORKERS, max_batches=MAX_BATCHES):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    dataset = ds.dataset(input_glob, format="parquet")
    scanner = dataset.scanner(batch_size=batch_rows)
    batches = scanner.to_batches()

    pool = Pool(n_workers)
    futures = []
    written = 0

    try:
        for i, rb in enumerate(batches):
            if max_batches is not None and i >= max_batches:
                break
            df_chunk = rb.to_pandas()
            # submit
            fut = pool.apply_async(process_df_chunk, (df_chunk,))
            futures.append((i, df_chunk, fut))

            # throttle to keep memory bounded: flush earliest finished futures if too many queued
            while len(futures) > n_workers * 4:
                j, df_in, f = futures.pop(0)
                res_df = f.get()
                out_df = pd.concat([df_in.reset_index(drop=True), res_df.reset_index(drop=True)], axis=1)
                filename = os.path.join(output_dir, f"shard_{j:06d}.parquet")
                table = pa.Table.from_pandas(out_df)
                pq.write_table(table, filename)
                written += 1

        # finish remaining
        for j, df_in, f in futures:
            res_df = f.get()
            out_df = pd.concat([df_in.reset_index(drop=True), res_df.reset_index(drop=True)], axis=1)
            filename = os.path.join(output_dir, f"shard_{j:06d}.parquet")
            table = pa.Table.from_pandas(out_df)
            pq.write_table(table, filename)
            written += 1
    finally:
        pool.close()
        pool.join()

    print(f"Done — wrote {written} shards to {output_dir}")

# Run
if __name__ == "__main__":
    print(f"Running with BATCH_ROWS={BATCH_ROWS}, N_WORKERS={N_WORKERS}. Tune these for your Surface Pro 7.")
    run_pipeline()
