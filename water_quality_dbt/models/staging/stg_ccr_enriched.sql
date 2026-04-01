select 
        CAST(zip AS STRING) AS zip,
        CAST(ccr_available AS BOOLEAN) AS ccr_available,
        CAST(ccr_year AS INTEGER) AS ccr_year,
        CAST(ccr_system_name AS STRING) AS ccr_system_name,
        CAST(ccr_pwsid AS STRING) AS ccr_pwsid,
        CAST(ccr_contaminant_count AS INTEGER) AS ccr_contaminant_count,
        CAST(ccr_violations AS INTEGER) AS ccr_violations,
        CAST(ccr_source_type AS STRING) AS ccr_source_type,
        CAST(ccr_lead_90th_ppb AS FLOAT64) AS ccr_lead_90th_ppb,
        CAST(ccr_copper_90th_ppb AS FLOAT64) AS ccr_copper_90th_ppb,
        CAST(ccr_top_contaminants AS STRING) AS ccr_top_contaminants
from {{ source('raw', 'raw_ccr_enriched') }}
where zip is not null