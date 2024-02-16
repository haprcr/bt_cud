select sku.description, cred.full_name, cred.type, cred.name, -1 * sum(cred.amount)
from `finops-poc-407205.CUD_data.detailed_billing_export`, unnest(credits) as cred
where service.description = "Compute Engine"
group by 1, 2, 3, 4


select usage.amount, cost, cred.amount, cred.type
from `finops-poc-407205.CUD_data.detailed_billing_export`, unnest(credits) as cred
where usage.amount = 0 and cred.amount < 0


-- select *
-- from `finops-poc-407205.CUD_data.detailed_billing_export`, unnest(credits) as cred
-- where location.region = "asia-east1"
-- and date(usage_start_time) = "2024-01-18"
-- and project.id = "committed-use-discount-test"
-- and sku.id = "FD4D-A383-8DAB"
-- and usage.amount = 10629919957479.0
-- and cred.amount * -1 = 0.000774
