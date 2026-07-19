-- Pass rate must be between 0 and 100
select facility_category, risk_level, pass_rate_pct
from {{ ref('fct_inspection_summary') }}
where pass_rate_pct < 0 or pass_rate_pct > 100
