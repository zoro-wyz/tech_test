-- ods
WITH unique_users AS (
    SELECT DISTINCT
        login_hash,
        server_hash,
        country_hash,
        currency,
        enable
    FROM
        users
),
-- dwd
trades_with_users AS (
    SELECT
        t.login_hash,
        t.ticket_hash,
        t.server_hash,
        t.symbol,
        t.digits,
        t.cmd,
        t.volume,
        t.open_time,
        (ROUND(t.open_price::numeric, COALESCE(t.digits, 0)::int))::double precision AS open_price,
        t.close_time,
        t.contractsize,
        u.country_hash,
        u.currency,
        u.enable
    FROM
        trades t
    INNER JOIN
        unique_users u
    ON
        t.login_hash = u.login_hash
        AND t.server_hash = u.server_hash
        AND u.enable = 1
),
-- dws 
trades_with_calculations AS (
    SELECT
        TO_CHAR(twu.close_time, 'YYYY-MM-DD')::date AS dt_report,
        twu.login_hash,
        twu.server_hash,
        twu.symbol,
        twu.currency,
        twu.volume,
        SUM(twu.volume) OVER (PARTITION BY twu.login_hash, twu.server_hash, twu.symbol ORDER BY twu.close_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_volume_prev_7d,
        SUM(twu.volume) OVER (PARTITION BY twu.login_hash, twu.server_hash, twu.symbol ORDER BY twu.close_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_volume_prev_all,
        COUNT(*) OVER (PARTITION BY twu.login_hash ORDER BY twu.close_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS count_prev_7d,
        MIN(twu.close_time) OVER (PARTITION BY twu.login_hash, twu.server_hash, twu.symbol) AS date_first_trade
    FROM
        trades_with_users twu
),
-- dwt
trades_report AS(
SELECT
    twc.dt_report,
    twc.login_hash,
    twc.server_hash,
    twc.symbol,
    twc.currency,
    twc.sum_volume_prev_7d::double precision,
    twc.sum_volume_prev_all::double precision,
    DENSE_RANK() OVER (PARTITION BY twc.login_hash,twc.symbol ORDER BY twc.sum_volume_prev_7d ) AS rank_volume_symbol_prev_7d,
    DENSE_RANK() OVER (PARTITION BY twc.login_hash ORDER BY twc.count_prev_7d ) AS rank_count_prev_7d,
    SUM(CASE WHEN TO_CHAR(twc.dt_report, 'YYYY-MM') = '2020-08' THEN twc.volume ELSE 0 END) OVER (PARTITION BY twc.login_hash, twc.server_hash, twc.symbol)::double precision AS sum_volume_2020_08,
    twc.date_first_trade,
    ROW_NUMBER() OVER (ORDER BY twc.dt_report, twc.login_hash, twc.server_hash, twc.symbol) AS row_number
FROM
    trades_with_calculations twc
)
-- ads
SELECT 
    dt_report, 
    login_hash, 
    server_hash, 
    symbol, 
    sum_volume_prev_7d, 
    sum_volume_prev_all, 
    rank_volume_symbol_prev_7d, 
    rank_count_prev_7d, 
    sum_volume_2020_08, 
    date_first_trade, 
    row_number
FROM 
    trades_report 
    WHERE dt_report >= '2020-06-01' AND dt_report <= '2020-09-30'
    ORDER BY row_number DESC
;