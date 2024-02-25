-- Resource based CUD total cost


-- SELECT
--     invoice.month AS invoice_month,
--     service.description as service,
--     sku.description as commitment_sku,
--     SUM(cost) as commitment_fees
-- FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
-- WHERE LOWER(sku.description) LIKE "commitment%"
-- AND NOT (LOWER(sku.description) LIKE "%dollar based%")
-- GROUP BY 1, 2, 3
-- ORDER BY 1 DESC, 2 DESC, 3 DESC

-- SELECT SUM(cost) as Resource_Cost, ARRAY_AGG(DISTINCT sku.description) as sku_array,
--                     SUM(usage.amount) as Resource_Usage_Amount
--                     FROM `finops-poc-407205.CUD_data.detailed_billing_export` , UNNEST(credits) as cred
--                     WHERE (sku.description LIKE "%SSD%" AND sku.description LIKE "%Local%")
--                     AND sku.description LIKE "%Americas%"
--                     AND service.description = "Compute Engine"


SELECT *
                    FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
                    -- , UNNEST(credits) as cred
                    WHERE (sku.description LIKE "%SSD%" AND sku.description LIKE "%Local%")
                    AND sku.description LIKE "%Americas%"
                    AND service.description = "Compute Engine"
                    and sku.description not like "Commitment%"
