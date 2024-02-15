SELECT sku.description
 , array_agg(distinct cred.full_name IGNORE NULLS)
-- , array_agg( distinct cred.amount IGNORE NULLS)
FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
, unnest(credits) as cred
where service.description = "Cloud Run" 
-- and sku.description like "%Requests%"
group by 1
