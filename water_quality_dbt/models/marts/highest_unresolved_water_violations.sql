--**Which states and city have the most persistent unresolved violations and elevated lead levels, 
--and how do the CCR-reported lead measurements compare to the predicted lead exposure risk?**
--   - `zipcheckup`: unresolved_violations, lead_level_mg_l, state, population
--   - `ccr_enriched`: ccr_lead_90th_ppb, ccr_violations
--   - `l3_metrics`: lead_exposure_probability, lead_risk_label
with zipcheckup_violations as (
    select 
        zip,
        city,
        state,
        latitude,
        longitude,
        total_violations,
        health_violations,
        unresolved_violations,
        lead_level_mg_l,
        water_source,
        population
    from {{ ref('stg_zipcheckup') }}
),
ccr_violations as(
    select 
        zip,
        ccr_lead_90th_ppb,
        ccr_violations
    from {{ ref('stg_ccr_enriched') }}
),
l3_metrics_violations as (
    select 
        zip,
        lead_exposure_probability,
        lead_risk_label
    from {{ ref('stg_l3_metrics') }}
),
final as (
    select 
        z.zip,
        z.city,
        z.state,
        z.latitude,
        z.longitude,
        z.total_violations,
        z.health_violations,
        z.unresolved_violations,
        z.lead_level_mg_l,
        z.water_source,
        z.population,
        c.ccr_lead_90th_ppb,
        c.ccr_violations,
        l.lead_exposure_probability,
        l.lead_risk_label
    from zipcheckup_violations z
    left join ccr_violations c on z.zip = c.zip 
    left join l3_metrics_violations l on z.zip = l.zip
    where z.unresolved_violations > 0
    order by z.unresolved_violations desc, z.lead_level_mg_l desc
)
select * from final

