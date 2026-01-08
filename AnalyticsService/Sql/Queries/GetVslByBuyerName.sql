SELECT DISTINCT vsl_name
FROM analytics.video_sessions_mv
WHERE buyer_name IN @BuyerNames
ORDER BY vsl_name;