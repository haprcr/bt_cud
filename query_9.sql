-- Resource based CUD coverage per project
-- daily cud coverage of a sku belonging to a particular project

WITH usage_data as
(
  SELECT
    sku.id as sku_id,
    sku.description as sku_description,
    location.region as region,
    project.id as project_id,
    project.name as project_name,
    CASE
      WHEN lower(usage.unit) LIKE "seconds" THEN "vcpu"
      WHEN lower(usage.unit) LIKE "byte-seconds" THEN "ram"
      ELSE NULL
    END
    AS unit_type,
    lower(usage.unit) as usage_unit,
    cost,
    usage.amount as usage_amount,
    credits,
    date(usage_start_time) as usage_date
    -- CAST(DATETIME(usage_start_time, "America/Los_Angeles") AS DATE) as usage_date
  FROM `finops-poc-407205.CUD_data.detailed_billing_export`
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
      OR LOWER(sku.description) LIKE "%commitment%"
    )
    -- Filter out Sole Tenancy skus that do not represent billable compute instance usage
    AND NOT 
    ( FALSE
      -- the VMs that run on sole tenancy nodes are not actually billed. Just the sole tenant node is
      OR LOWER(sku.description) LIKE "%hosted on sole tenancy%"
      -- sole tenancy premium charge is not eligible instance usage
      OR LOWER(sku.description) LIKE "sole tenancy premium%"
    )
    -- Filter to time range when necessary columns (region) were released into Billing BQ Export 
    -- AND CAST(DATETIME(usage_start_time, "America/Los_Angeles") AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND CAST(usage_start_time AS DATE) >= "2018-01-19"
),
prices as (
  SELECT  
    usage_date,
    sku_id,
    region,
    -- calculate unit price per sku for each day. Catch line items with 0 usage to avoid divide by zero.
    -- using 1 assumes that there are no relevant (CUD related) skus with cost but 0 usage, 
    -- which is correct for current billing data
    IF(SUM(usage_amount) = 0, 0, SUM(cost) / SUM(usage_amount)) as unit_price
  FROM usage_data
  GROUP BY 1,2,3
  ORDER BY 1,2,3
),
cud_coverage as(
  SELECT
      usage_date,
      region,
      unit_type,
      'CUD Credit' as usage_type,
      project_id,
      project_name,
      sku_id,
      sku_description,
      usage_unit,
      SUM(cud_usage_amount) as cud_usage_amount,
      SUM(total_usage_amount) as total_usage_amount,
      SUM(cud_credit) as cud_credit,
      SUM(total_resource_cost) as total_resource_cost
    FROM
    (
      SELECT
        u.usage_date,
        u.region,
        unit_type,
        'CUD Credit' as usage_type,
        project_id,
        project_name,
        u.sku_id,
        u.sku_description,
        -- display_unit as unit,
        unit_price,
        usage_unit,
        IF (
          prices.unit_price = 0, 
          0, 
          CASE

            WHEN LOWER(unit_type) LIKE "vcpu" THEN -1*SUM(cred.amount)/prices.unit_price 

            WHEN LOWER(unit_type) = "ram" THEN -1*SUM(cred.amount)/prices.unit_price 
            ELSE NULL
          END
        )
        AS cud_usage_amount,
      SUM(usage_amount) as total_usage_amount,
      SUM(cred.amount) as cud_credit,
      SUM(cost) as total_resource_cost
      FROM usage_data AS u, UNNEST(credits) as cred
      JOIN prices 
        ON u.sku_id = prices.sku_id
        AND u.region = prices.region
        AND u.usage_date = prices.usage_date
      -- filter down to just Resource based CUD Credits
      -- WHERE cred.name like "%Committed%"
      WHERE cred.full_name like "%Committed use discount%"
      GROUP BY 1,2,3,4,5,6,7,8,9,10
)
GROUP BY 1,2,3,4,5,6,7,8,9
)

select *
from(

SELECT *,
IF (
  total_usage_amount = 0, 
  0,
  round((cud_usage_amount/total_usage_amount)*100, 2)
)
AS cud_coverage 
FROM cud_coverage 
ORDER BY usage_date desc, sku_id desc )
where usage_date = "2024-01-18"
and region = "asia-east1"
and unit_type = "ram"
and usage_type = "CUD Credit"
and project_id= "committed-use-discount-test"
and sku_id = "FD4D-A383-8DAB" and
sku_description = "E2 Instance Ram running in APAC"

-- -0.016422
-- 0.346139999999
-- limit 15

-- 0.44668200000000008
-- -0.020943999999
select sum(cost), sum(cred.amount) as credit_amt
from `finops-poc-407205.CUD_data.detailed_billing_export`, unnest(credits) as cred
where date(usage_start_time) = "2024-01-18"
and location.region = "asia-east1"
and project.id= "committed-use-discount-test"
and sku.id = "FD4D-A383-8DAB" and
sku.description = "E2 Instance Ram running in APAC"
and  cred.full_name like "%Committed use discount%"

