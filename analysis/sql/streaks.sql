WITH stacked AS (
  SELECT game_id, game_datetime, white AS player, 'white' AS role, result, game_type FROM games
  UNION ALL
  SELECT game_id, game_datetime, black AS player, 'black' AS role, result, game_type FROM games
),

ordered AS (
  SELECT
    player,
    game_type,
    role,
    result,
    ROW_NUMBER() OVER (PARTITION BY player, game_type ORDER BY game_datetime, game_id) AS rn
  FROM stacked
),

with_outcome AS (
  SELECT
    player,
    game_type,
    rn,
    CASE
      WHEN result = '1-0' AND role = 'white' THEN 'win'
      WHEN result = '1-0' AND role = 'black' THEN 'loss'
      WHEN result = '0-1' AND role = 'black' THEN 'win'
      WHEN result = '0-1' AND role = 'white' THEN 'loss'
      WHEN result = '1/2-1/2' THEN 'draw'
    END AS outcome
  FROM ordered
),

-- compute consecutive previous losses
prev_losses_calc AS (
  SELECT
    player,
    game_type,
    rn,
    outcome,
    COALESCE(
      rn - MAX(CASE WHEN outcome != 'loss' THEN rn END)
         OVER (PARTITION BY player, game_type ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0
    ) AS prev_losses
  FROM with_outcome
)

SELECT
  game_type,
  prev_losses AS losses_in_streak_before,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) AS loss_count_after,
  ROUND(100.0 * SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS next_loss_pct
FROM prev_losses_calc
GROUP BY game_type, prev_losses
ORDER BY game_type, prev_losses;
