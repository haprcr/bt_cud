WITH
usage_data AS (
  SELECT
    CAST(usage_start_time AS DATE) AS usage_date,
    sku.id AS sku_id,
    sku.description AS sku_description,
    location.region AS region,
    project.id AS project_id,
    project.name AS project_name,
    usage.unit AS unit,
    invoice.month as invoice_month,
    cost,
    usage.amount AS usage_amount,
    credits
  -- *****************************************************************
  -- *** INSERT YOUR BILLING BQ EXPORT TABLE NAME ON THE NEXT LINE ***
  -- *****************************************************************
  FROM  `finops-poc-407205.CUD_data.detailed_billing_export`
  WHERE TRUE
    AND service.description = "Compute Engine"
    -- Filter down to just VM instances usage and commitments
    AND 
    (
      FALSE
      OR (LOWER(sku.description) LIKE "%instance%" OR LOWER(sku.description) LIKE "% intel %")
      OR LOWER(sku.description) LIKE "%memory optimized core%" 
      OR LOWER(sku.description) LIKE "%memory optimized ram%"
      OR LOWER(sku.description) LIKE "%memory-optimized core%" 
      OR LOWER(sku.description) LIKE "%memory-optimized ram%" 
      OR lower(sku.description) LIKE "%megamem%"
      OR lower(sku.description) LIKE "%ultramem%"
      OR LOWER(sku.description) LIKE "%core%"
      OR LOWER(sku.description) LIKE "%ram%"
      OR sku.description LIKE "%GPU%"
      OR sku.description LIKE "%Local Storage%"
    )
    -- Filter out Sole Tenancy skus that do not represent billable compute instance usage
    AND NOT 
    ( FALSE
      -- the VMs that run on sole tenancy nodes are not actually billed. Just the sole tenant node is
      OR LOWER(sku.description) LIKE "%hosted on sole tenancy%"
      -- sole tenancy premium charge is not eligible instance usage
      OR LOWER(sku.description) LIKE "sole tenancy premium%"
      
    )
    AND NOT
    (
        lower(sku.description) LIKE "%preemptible%"
        OR lower(sku.description) LIKE "%micro%"
        OR lower(sku.description) LIKE "%small%"
        OR lower(sku.description) LIKE "%extended%"
    )
    -- Filter to time range when necessary columns (region) were releASed into Billing BQ Export 
    AND CAST(usage_start_time AS DATE) <= CURRENT_DATE()
    -- = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

-- create temporary table prices, in order to calculate unit price per (date, sku, region) tuple.

prices AS (
  SELECT  
    usage_date,
    sku_id,
    -- Only include region if we are looking at data from 9/17 and onwards
    region,
    -- calculate unit price per sku for each day. Catch line items with 0 usage to avoid divide by zero.
    -- using 1 ASsumes that there are no relevant (CUD related) skus with cost but 0 usage, 
    -- which is correct for current billing data
    IF(SUM(usage_amount) = 0, 0, SUM(cost) / SUM(usage_amount)) AS unit_price
  FROM usage_data
  GROUP BY 1,2,3
  ORDER BY 1,2,3
),

-- sku_metadata temporary table captures information about skus, such AS CUD eligibility,
-- whether the sku is vCPU or RAM, etc.
sku_metadata AS (
  SELECT  
    sku_id,
    CASE
      WHEN lower(unit) LIKE "seconds" AND lower(sku_description) LIKE "%core%" THEN "vcpu"
      WHEN lower(unit) LIKE "seconds" AND lower(sku_description) LIKE "%gpu%" THEN "gpu"
      WHEN lower(unit) LIKE "byte-seconds" AND lower(sku_description) LIKE "%ram%" THEN "ram"
      WHEN lower(unit) LIKE "byte-seconds" AND (lower(sku_description) LIKE "%local%" AND  lower(sku_description) LIKE "%ssd%") THEN "ssd"
      ELSE NULL
    END
    AS unit_type
  FROM usage_data
  GROUP BY 1,2
  ORDER BY 1 ASc
),
cud_coverage_data AS(
SELECT
  usage_date,
  region,
  usage_type,
  unit_type,
  project_id,
  project_name,
  sku_id,
  sku_description,
  invoice_month,
  SUM(cud_usage_amount) AS CUD_Credit_Amount,
  SUM(resource_usage_amount) AS Resource_Usage_Amount,
  SUM(cud_credit) AS CUD_Credit,
  SUM(resource_cost) AS Resource_Cost,
  -- cud_coverage
FROM
(
    -- This query pulls out CUD Credit usage. 
    SELECT
      usage_date,
      region,
      unit_type,
      'CUD Credit' AS usage_type,
      project_id,
      project_name,
      sku_id,
      sku_description,
      invoice_month,
      SUM(cud_usage_amount) AS CUD_Usage_Amount,
      SUM(resource_usage_amount) AS Resource_Usage_Amount,
      SUM(cud_credit) AS CUD_Credit,
      SUM(resource_cost) AS Resource_Cost
    FROM
    (
      SELECT
        u.usage_date,
        u.region,
        unit_type,
        'CUD Credit' AS usage_type,
        project_id,
        project_name,
        u.sku_id,
        u.sku_description,
        unit_price,
        invoice_month,
        IF (
          prices.unit_price = 0, 
          0, 
          -1*SUM(cred.amount)/prices.unit_price
        )
        AS cud_usage_amount,
        SUM(u.usage_amount) AS resource_usage_amount,
        SUM(cred.amount) AS cud_credit,
        SUM(U.cost) AS resource_cost
      FROM usage_data AS u, UNNEST(credits) AS cred
      JOIN sku_metadata ON u.sku_id = sku_metadata.sku_id
      JOIN prices 
        ON u.sku_id = prices.sku_id
        AND u.region = prices.region
        AND u.usage_date = prices.usage_date
      -- filter down to just CUD Credits
      WHERE  cred.name like "%Committed Usage Discount:%"
      OR 
      (cred.type like "%COMMITTED_USAGE_DISCOUNT%"
      AND  cred.type not like "%DOLLAR_BASE%")
      GROUP BY 1,2,3,4,5,6,7,8,9,10
    )
    GROUP BY 1,2,3,4,5,6,7,8,9
)
GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT
usage_date as Usage_Date,
project_id as Project_Id,
project_name as Project_Name,
region as Region,
"Compute Engine" as Service,
sku_id as SKU_Id,
sku_description as SKU_Description,
usage_type AS Usage_Type,
unit_type as Unit_Type,
invoice_month as Invoice_Month,
CUD_Credit_Amount as CUD_Credit_Usage_Amount,
Resource_Usage_Amount,
CUD_Credit,
Resource_Cost,
IF (
  Resource_Usage_Amount = 0, 
  0,
  round((CUD_Credit_Amount/Resource_Usage_Amount)*100, 2)
)
AS Cud_Coverage 
FROM cud_coverage_data
