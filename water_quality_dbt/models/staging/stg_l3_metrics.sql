SELECT 
    CAST(zip AS STRING) AS zip,
    CAST(lead_exposure_probability AS FLOAT64) AS lead_exposure_probability,
    CAST(lead_risk_label AS STRING) AS lead_risk_label,
    CAST(maintenance_debt_usd AS FLOAT64) AS maintenance_debt_usd,
    CAST(compliance_risk_score AS FLOAT64) AS compliance_risk_score,
    CAST(compliance_risk_label AS STRING) AS compliance_risk_label,
    CAST(energy_burden_pct AS FLOAT64) AS energy_burden_pct,
    CAST(energy_burden_label AS STRING) AS energy_burden_label,
    CAST(flood_annual_cost_usd AS FLOAT64) AS flood_annual_cost_usd
FROM {{ source('raw', 'raw_l3_metrics') }}
WHERE zip IS NOT NULL
