--Which zip codes face the highest compound risk for home buyers — combining poor water safety scores, high contaminant counts from CCR reports, and infrastructure/environmental burdens like flood cost and maintenance debt?**
--   - `zipcheckup`: home_safety_score, home_safety_grade, total_violations, contaminant_count
--   - `ccr_enriched`: ccr_contaminant_count, ccr_top_contaminants
--   - `l3_metrics`: maintenance_debt_usd, flood_annual_cost_usd, energy_burden_pct

with zipcheckup_risk as (
    select 
        zip,
        city,
        state,
        latitude_longitude,
        home_safety_score,
        home_safety_grade,
        total_violations,
        contaminant_count
    from {{ ref('stg_zipcheckup') }}
),
ccr_risk as(
    select 
        zip,
        ccr_contaminant_count,
        ccr_top_contaminants
    from {{ ref('stg_ccr_enriched') }}
),
l3_metrics_risk as (
    select 
        zip,
        maintenance_debt_usd,
        flood_annual_cost_usd,
        energy_burden_pct
    from {{ ref('stg_l3_metrics') }}
),
final as (
    select 
        z.zip,
        z.city,
        z.state,
        z.latitude_longitude,
        z.home_safety_score,
        z.home_safety_grade,
        z.total_violations,
        z.contaminant_count,
        c.ccr_contaminant_count,
        c.ccr_top_contaminants,
        l.maintenance_debt_usd,
        l.flood_annual_cost_usd,
        l.energy_burden_pct
     from zipcheckup_risk z
     left join ccr_risk c on z.zip = c.zip 
     left join l3_metrics_risk l on z.zip = l.zip
     order by z.home_safety_score asc, c.ccr_contaminant_count desc, l.maintenance_debt_usd desc
)
select * from final