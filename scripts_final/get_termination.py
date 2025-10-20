import os
import re
import argparse

import chess
import pandas as pd
import pyarrow.parquet as pq


# End reason mapping
END_REASON_MAP = {
    0: "unknown",
    1: "checkmate",
    2: "resignation",
    3: "timeout_win",
    4: "insufficient_material_timeout_draw",
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

def analyse_san(moves_san, result, termination):
    
    if termination != 'Normal' or result != '1/2-1/2':
        return pd.Series({
            "is_stalemate": False,
            "is_threefold": False,
            "is_fifty_move": False,
            "is_insufficient_material_both": False
        })

    board = chess.Board()
    toks = tokens_from_san_fast(moves_san)
    seen = {board._transposition_key(): 1}

    is_stalemate = False
    is_threefold = False
    is_fifty_move = False
    is_insufficient_material_both = False

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
            is_insufficient_material_both = True
            
    return pd.Series({
        "is_stalemate": bool(is_stalemate),
        "is_threefold": bool(is_threefold),
        "is_fifty_move": bool(is_fifty_move),
        "is_insufficient_material_both": bool(is_insufficient_material_both)
    })
    
def preprocess_san_to_draw_columns(input_parquet, output_parquet, sample_games=None):
    """
    Reads input Parquet in chunks, computes draw-related columns, writes out new Parquet
    """
    pf = pq.ParquetFile(input_parquet)
    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)
    writer = None
    processed_games = 0

    for rg_idx in range(pf.num_row_groups):
        df_chunk = pf.read_row_group(rg_idx).to_pandas()

        if sample_games and processed_games >= sample_games:
            break
        if sample_games and processed_games + len(df_chunk) > sample_games:
            df_chunk = df_chunk.iloc[:sample_games - processed_games]

        # Vectorized SAN -> draw columns
        
        draw_cols = df_chunk.apply(
            lambda row: analyse_san(row['moves_san'], row['result'], row['termination']),
            axis=1
        )
        df_chunk = pd.concat([df_chunk, draw_cols], axis=1)

        # Write or append
        if writer is None:
            df_chunk.to_parquet(output_parquet, index=False, engine='pyarrow')
        else:
            df_chunk.to_parquet(output_parquet, index=False, engine='pyarrow', append=True)
        writer = True

        processed_games += len(df_chunk)
        print(f"Processed {processed_games} games for draw preprocessing...")

    print("Preprocessing complete:", processed_games, "games written to", output_parquet)


def compute_endings(input_parquet, out_dir, sample_games = None):
    """
    Fully vectorized end_code computation using precomputed draw columns
    """
    pf = pq.ParquetFile(input_parquet)
    os.makedirs(out_dir, exist_ok=True)
    file_idx = 0
    total_count = 0
    processed_games = 0

    for rg_idx in range(pf.num_row_groups):
        df_chunk = pf.read_row_group(rg_idx).to_pandas()

        if sample_games and processed_games >= sample_games:
            break
        if sample_games and processed_games + len(df_chunk) > sample_games:
            df_chunk = df_chunk.iloc[:sample_games - processed_games]

    df = df_chunk.copy()
    
    df['end_code'] = 0  # default unknown

    # 1: checkmate 
    df.loc[df['mated'] == True, 'end_code'] = 1

    # 2: resignation
    mask = (df['termination'] == 'Normal') & (df['result'].isin(['1-0','0-1'])) & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 2

    # 3: timeout_win
    mask = (df['termination'] == 'Time forfeit') & (df['result'].isin(['1-0','0-1'])) & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 3

    # 4: timeout_draw
    mask = (df['termination'] == 'Time forfeit') & (df['result'] == '1/2-1/2') & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 4

    # 5: stalemate
    mask = (df['is_stalemate']==True) & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 5

    # 6: threefold repetition
    mask = (df['is_threefold']==True) & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 6

    # 7: fifty move rule
    mask = (df['is_fifty_move']==True) & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 7

    # 8: insufficient material claimed
    mask = (df['termination']=='Insufficient material') & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 8
    
    # 9: insufficient material both
    mask = (df['is_insufficient_material_both']==True) & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 9

    # 10: agreement draw (remaining 1/2-1/2)
    mask = (df['result']=='1/2-1/2') & (df['end_code']==0)
    df.loc[mask, 'end_code'] = 10

    df['end_reason'] = df['end_code'].map(END_REASON_MAP)

    out_chunk = df[['game_id', 'end_reason']]

    fname = os.path.join(out_dir, f"moves_chunk_{file_idx:05d}.parquet")
    out_chunk.to_parquet(fname, index=False, compression='snappy')
    print(f"Wrote {len(out_chunk)} games to {fname}")

    file_idx += 1
    total_count += len(out_chunk)
    processed_games += len(out_chunk)

    print("Done. Games written (approx):", total_count, "parquet files:", file_idx)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_parquet", help="Path to input Parquet file")
    parser.add_argument("--preprocessed_parquet", help="Path to output preprocessed Parquet with draw columns", default=" out\preprocessing\preprocessed.parquet")
    parser.add_argument("--out_dir", help="Directory for final end_code shards", default="parquet_moves")
    parser.add_argument("--sample_games", type=int, default=None)
    args = parser.parse_args()

    # Step 1: Precompute draw columns
    preprocess_san_to_draw_columns(args.input_parquet, args.preprocessed_parquet, args.sample_games)

    # Step 2: Compute end codes vectorized
    compute_endings(args.preprocessed_parquet, args.out_dir, args.sample_games)