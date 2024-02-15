-- SELECT *
-- FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
-- WHERE LOWER(sku.description) LIKE "%hosted on sole tenancy%"

-- CREATE TABLE `finops-poc-407205.Cud_spend_metrics.cud_spend_metric_4_5`
-- (
--   Invoice_Month STRING,
--   Service STRING,
--   Project_ID STRING,
--   Region STRING, 
--   Commitment_SKU STRING,
--   Commitment_Cost FLOAT64,
--   Total_Resource_Cost FLOAT64,
--   CUD_Credit FLOAT64,
--   CUD_Utilisation FLOAT64
-- );

INSERT INTO `finops-poc-407205.Cud_spend_metrics.cud_spend_metric_4_5`(
    Invoice_Month,
    Service,
    Project_ID,
    Region,
    Commitment_SKU,
    Commitment_Cost
)

SELECT
    invoice.month AS invoice_month,
    service.description as oiservice,
    project.id as Project_ID,
    location.region as region,
    sku.description as commitment_sku,
    SUM(cost) as commitment_fees
FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
WHERE LOWER(sku.description) LIKE "commitment%"
AND LOWER(sku.description) LIKE "%dollar based%"
AND invoice.month >
    (
        SELECT IFNULL(MAX(Invoice_Month), '199901')
        FROM `finops-poc-407205.Cud_spend_metrics.cud_spend_metric_4_5`
    )
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC, 5 DESC


-- truncate table `finops-poc-407205.Cud_spend_metrics.cud_spend_metric_4_5`
