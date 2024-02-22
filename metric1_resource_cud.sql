-- INSERT INTO 


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
    )
    -- Filter out Sole Tenancy skus that do not represent billable compute instance usage
    AND NOT 
    ( FALSE
      -- the VMs that run on sole tenancy nodes are not actually billed. Just the sole tenant node is
      OR LOWER(sku.description) LIKE "%hosted on sole tenancy%"
      -- sole tenancy premium charge is not eligible instance usage
      OR LOWER(sku.description) LIKE "sole tenancy premium%"
    )
    -- Filter to time range when necessary columns (region) were releASed into Billing BQ Export 
    AND CAST(usage_start_time AS DATE) >= "2018-09-18"
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
    -- parse sku_description to identify whether usage is CUD eligible, or if the 
    -- line item is for a commitment charge
    CASE
      WHEN lower(sku_description) LIKE "%commitment%" THEN "CUD Commitment"
      WHEN
      (
        lower(sku_description) LIKE "%preemptible%"
        OR lower(sku_description) LIKE "%micro%"
        OR lower(sku_description) LIKE "%small%"
        OR lower(sku_description) LIKE "%extended%"
      ) THEN "Ineligible Usage"
      WHEN 
      (
        (LOWER(sku_description) LIKE "%instance%" OR LOWER(sku_description) LIKE "% intel %") 
        OR LOWER(sku_description) LIKE "%core%"
        OR LOWER(sku_description) LIKE "%ram%"
      ) THEN "Eligible Usage"
      ELSE NULL
      END
    AS usage_type,
    -- for VM skus and commitments, "seconds" unit uniquely identifies vCPU usage
    -- and "byte-seconds" unit uniquely identifies RAM
    CASE
      WHEN lower(unit) LIKE "seconds" THEN "vcpu"
      WHEN lower(unit) LIKE "byte-seconds" THEN "ram"
      ELSE NULL
    END
    AS unit_type,
    CASE
      WHEN lower(unit) LIKE "seconds" THEN "Avg. Concurrent vCPU"
      WHEN lower(unit) LIKE "byte-seconds" THEN "Avg. Concurrent RAM GB"
      ELSE NULL
    END
    AS display_unit
  FROM usage_data
  GROUP BY 1,2,3,4
  ORDER BY 1 ASc
),
cud_coverage_data AS(
SELECT
  usage_date,
  region,
  usage_type,
  project_id,
  project_name,
  sku_id,
  sku_description,
  unit,
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
      unit,
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
        display_unit AS unit,
        unit_price,
        IF (
          prices.unit_price = 0, 
          0, 
          CASE
            -- Divide by # seconds in a day to get to core*days == avg daily concurrent usage
            WHEN LOWER(unit_type) LIKE "vcpu" THEN -1*SUM(cred.amount)/prices.unit_price/ 86400

            -- Divide by # seconds in a day and # bytes in a GB to get to 
            -- GB*days == avg daily concurrent RAM GB         
            WHEN LOWER(unit_type) = "ram" THEN -1*SUM(cred.amount)/prices.unit_price / (86400 * 1073741824)
            ELSE NULL
          END
        )
        AS cud_usage_amount,
        IF (
          SUM(u.usage_amount) = 0, 
          0, 
          CASE
            -- Divide by # seconds in a day to get to core*days == avg daily concurrent usage
            WHEN LOWER(unit_type) LIKE "vcpu" THEN SUM(u.usage_amount)
            -- Divide by # seconds in a day and # bytes in a GB to get to 
            -- GB*days == avg daily concurrent RAM GB         
            WHEN LOWER(unit_type) = "ram" THEN SUM(u.usage_amount) 
            ELSE NULL
          END
        )
        AS resource_usage_amount,
        -- IF (
        --   prices.unit_price = 0, 
        --   0, 
        --   CASE
        --     -- Divide by # seconds in a day to get to core*days == avg daily concurrent usage
        --     WHEN LOWER(unit_type) LIKE "vcpu" THEN -1*SUM(cred.amount)/prices.unit_price
        --     -- Divide by # seconds in a day and # bytes in a GB to get to 
        --     -- GB*days == avg daily concurrent RAM GB         
        --     WHEN LOWER(unit_type) = "ram" THEN -1*SUM(cred.amount)/prices.unit_price 
        --     ELSE NULL
        --   END
        -- )
        -- AS cud_usage_amount,
        -- IF (
        --   SUM(u.usage_amount) = 0, 
        --   0, 
        --   CASE
        --     -- Divide by # seconds in a day to get to core*days == avg daily concurrent usage
        --     WHEN LOWER(unit_type) LIKE "vcpu" THEN SUM(u.usage_amount)
        --     -- Divide by # seconds in a day and # bytes in a GB to get to 
        --     -- GB*days == avg daily concurrent RAM GB         
        --     WHEN LOWER(unit_type) = "ram" THEN SUM(u.usage_amount) 
        --     ELSE NULL
        --   END
        -- )
        -- AS resource_usage_amount,
        SUM(cred.amount) AS cud_credit,
        SUM(U.cost) AS resource_cost
      FROM usage_data AS u, UNNEST(credits) AS cred
      JOIN sku_metadata ON u.sku_id = sku_metadata.sku_id
      JOIN prices 
        ON u.sku_id = prices.sku_id
        AND u.region = prices.region
        AND u.usage_date = prices.usage_date
      -- filter down to just CUD Credits
      WHERE cred.name like "%Committed%"
      GROUP BY 1,2,3,4,5,6,7,8,9,10
    )
    GROUP BY 1,2,3,4,5,6,7,8,9
)
GROUP BY 1,2,3,4,5,6,7,8
)


SELECT *,
IF (
  Resource_Usage_Amount = 0, 
  0,
  round((CUD_Credit_Amount/Resource_Usage_Amount)*100, 2)
)
AS cud_coverage 
FROM cud_coverage_data



