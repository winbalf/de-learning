-- Violation count must never be negative
select inspection_id, violation_count
from {{ ref('stg_inspections') }}
where violation_count < 0
