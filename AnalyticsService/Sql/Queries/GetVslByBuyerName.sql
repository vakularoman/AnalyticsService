SELECT DISTINCT vsl_name
FROM analytics.events
WHERE buyer_name IN @BuyerNames
ORDER BY vsl_name;