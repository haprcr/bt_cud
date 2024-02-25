INSERT INTO 
`cme-poc-364409.cud_resource_metrics.cud_resource_metric_2_3_23feb`
(
    Invoice_Month,
    Service,
    Commitment_SKU,
    Commitment_Cost,
    Commitment_usage_amount
)
SELECT
    invoice.month AS invoice_month,
    service.description as service,
    sku.description as commitment_sku,
    SUM(cost) as commitment_fees,
    SUM(Usage.amount) as Commitment_usage_amount
FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
WHERE LOWER(sku.description) LIKE "commitment%"
AND NOT (LOWER(sku.description) LIKE "%dollar based%")
AND invoice.month >
    (
        SELECT IFNULL(MAX(Invoice_Month), '199901')
        FROM 
`cme-poc-364409.cud_resource_metrics.cud_resource_metric_2_3_23feb`
    )
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2 DESC, 3 DESC
