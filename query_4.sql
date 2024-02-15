SELECT array_agg(distinct sku.description) FROM `finops-poc-407205.CUD_data.detailed_billing_export` 
where lower(sku.description) like "%ram%" and lower(sku.description) like "%commitment%"
