-- =====================================================
-- VIEW 1: VIEW GỘP DỮ LIỆU GAME (CHUẨN ANALYTICS)
-- Dùng cho: EDA, BI, ML
-- =====================================================
DROP VIEW IF EXISTS vw_games_full;

CREATE VIEW vw_games_full AS
SELECT
    g.game_id,
    g.name,
    g.game_tag,
    g.release_date,
    EXTRACT(YEAR FROM TO_DATE(g.release_date, 'DD/MM/YYYY')) AS release_year,

    gc.developer,
    gc.publisher,

    gd.final_price_number_original AS original_price,
    gd.final_price_number_discount AS discount_price,

    CASE
        WHEN gd.review_summary ILIKE '%Overwhelmingly Positive%' THEN 5
        WHEN gd.review_summary ILIKE '%Very Positive%' THEN 4
        WHEN gd.review_summary ILIKE '%Mostly Positive%' THEN 3
        WHEN gd.review_summary ILIKE '%Mixed%' THEN 2
        ELSE 1
    END AS review_score,

    gd.review_summary,

    sr.cleaned_cpu,
    sr.minimum_ram_gb,
    sr.minimum_rom_gb

FROM games g
JOIN game_companies gc ON g.game_id = gc.game_id
JOIN game_details gd ON g.game_id = gd.game_id
JOIN system_requirements sr ON g.game_id = sr.game_id;
-- =====================================================
-- VIEW 2: VIEW PHỤC VỤ MACHINE LEARNING (KMEANS)
-- 👉 DÙNG TRỰC TIẾP TRONG game_clustering.py
-- =====================================================
DROP VIEW IF EXISTS vw_game_clustering;

CREATE VIEW vw_game_clustering AS
SELECT
    game_id,
    name,
    game_tag,

    CAST(discount_price AS FLOAT) AS price,

    review_score,
    minimum_ram_gb,
    minimum_rom_gb,
    release_year

FROM vw_games_full
WHERE discount_price IS NOT NULL;
-- =====================================================
-- VIEW 3: VIEW TỔNG HỢP CHO LOOKER DASHBOARD
-- 👉 KPI + INSIGHT
-- =====================================================
DROP VIEW IF EXISTS vw_game_dashboard;

CREATE VIEW vw_game_dashboard AS
SELECT
    game_tag,
    release_year,

    COUNT(*) AS total_games,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(AVG(review_score), 2) AS avg_review_score,
    ROUND(AVG(minimum_ram_gb), 1) AS avg_ram_required,
    ROUND(AVG(minimum_rom_gb), 1) AS avg_storage_required

FROM vw_game_clustering
GROUP BY game_tag, release_year;
-- =====================================================
-- VIEW 4: VIEW PHÂN KHÚC GAME SAU ML (JOIN CLUSTER)
-- 👉 LOOKER SỬ DỤNG VIEW NÀY LÀ ĐẸP NHẤT
-- =====================================================
DROP VIEW IF EXISTS vw_game_clusters_dashboard;

CREATE VIEW vw_game_clusters_dashboard AS
SELECT
    c.cluster_id,
    c.cluster_label,
    g.game_tag,
    g.release_year,

    COUNT(*) AS total_games,
    ROUND(AVG(c.price), 2) AS avg_price,
    ROUND(AVG(c.review_score), 2) AS avg_review_score,
    ROUND(AVG(c.minimum_ram_gb), 1) AS avg_ram,
    ROUND(AVG(c.minimum_rom_gb), 1) AS avg_storage

FROM game_clusters c
JOIN vw_game_clustering g ON c.game_id = g.game_id
GROUP BY
    c.cluster_id,
    c.cluster_label,
    g.game_tag,
    g.release_year;
