SELECT
    region,
    SUM(total_views)                                                     AS total_views,
    ROUND(AVG(avg_engagement_rate), 4)                                   AS avg_engagement_rate,
    SUM(unique_channels)                                                 AS unique_channels,
    ROUND(
        AVG(avg_engagement_rate) * LN(CAST(SUM(total_views) AS DOUBLE) + 1),
    4)                                                                   AS engagement_quality_score
FROM "yt-pipeline-gold-dev".trending_analytics
GROUP BY region
