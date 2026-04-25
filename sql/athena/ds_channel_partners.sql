SELECT
    channel_title,
    region,
    times_trending,
    total_views,
    ROUND(CAST(avg_engagement_rate AS DOUBLE), 4)  AS avg_engagement_rate,
    rank_in_region,
    categories
FROM channel_analytics
ORDER BY times_trending DESC