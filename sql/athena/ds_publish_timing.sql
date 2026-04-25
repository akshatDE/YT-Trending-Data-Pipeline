SELECT
    trending_date_parsed,
    region,
    SUM(total_views)                                        AS total_views,
    ROUND(AVG(CAST(avg_engagement_rate AS DOUBLE)), 4)      AS avg_engagement_rate,
    ROUND(AVG(CAST(avg_like_ratio AS DOUBLE)), 4)           AS avg_like_ratio
FROM trending_analytics
GROUP BY trending_date_parsed, region
ORDER BY trending_date_parsed