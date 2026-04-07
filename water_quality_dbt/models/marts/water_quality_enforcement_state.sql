--How does water quality enforcement vary across states — and are communities with high compliance risk scores actually the ones with the most health violations and enforcement actions?**
--- `zipcheckup`: health_violations, enforcement_action_count, enforcement_health_violations, state
--   - `ccr_enriched`: ccr_violations, ccr_system_name
--  - `l3_metrics`: compliance_risk_score, compliance_risk_label

with zipcheckup_state as (
    select 
        zip,
        city,
        state,
        latitude_longitude,
        health_violations,
        enforcement_action_count,
        enforcement_health_violations
    from {{ ref('stg_zipcheckup') }}
),
ccr_state as(
    select 
        zip,
        ccr_violations,
        ccr_system_name
    from {{ ref('stg_ccr_enriched') }}
),
l3_metrics_state as (
    select 
        zip,
        compliance_risk_score,
        compliance_risk_label
    from {{ ref('stg_l3_metrics') }}
),
final as (
    select 
        z.zip,
        z.city,
        z.state,
        z.latitude_longitude,
        z.health_violations,
        z.enforcement_action_count,
        z.enforcement_health_violations,
        c.ccr_violations,
        c.ccr_system_name,
        l.compliance_risk_score,
        l.compliance_risk_label
     from zipcheckup_state z
     left join ccr_state c on z.zip = c.zip 
     left join l3_metrics_state l on z.zip = l.zip
     order by l.compliance_risk_score desc, z.health_violations desc, z.enforcement_action_count desc
)
select * from final