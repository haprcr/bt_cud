-- Resource based CUD total cost

-- SELECT ARRAY_AGG(distinct(sku_description))
-- FROM(
-- SELECT
--     invoice.month AS invoice_month,
--     sku.description as sku_description,
--     -- service.description,
--     SUM(cost) as commitment_fees
-- FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
-- WHERE LOWER(sku.description) LIKE "commitment%"
-- AND NOT (LOWER(sku.description) LIKE "%dollar based%")
-- GROUP BY 1, 2
-- ORDER BY 1 DESC, 2 DESC
-- )


INSERT INTO `finops-poc-407205.cud_resource_metrics.cud_resource_metric_2_3`(
    Invoice_Month,
    Service,
    Commitment_SKU,
    Commitment_Cost
)
SELECT
    invoice.month AS invoice_month,
    service.description as service,
    sku.description as commitment_sku,
    SUM(cost) as commitment_fees
FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
WHERE LOWER(sku.description) LIKE "commitment%"
AND NOT (LOWER(sku.description) LIKE "%dollar based%")
AND invoice.month >
    (
        SELECT IFNULL(MAX(Invoice_Month), '199901')
        FROM `finops-poc-407205.cud_resource_metrics.cud_resource_metric_2_3`
    )
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2 DESC, 3 DESC
