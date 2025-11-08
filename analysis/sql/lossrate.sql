SELECT
    game_type,
    ROUND(100.0 * COUNT(*) FILTER (WHERE result = '1/2-1/2') / COUNT(*), 2) AS draw_pct
FROM games
GROUP BY game_type
ORDER BY game_type;
