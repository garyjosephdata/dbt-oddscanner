WITH union_metrics AS (
  SELECT a.* FROM {{ ref('fct_sap_dv_metrics') }} a
  JOIN (SELECT sap_advertiser_ruko_id FROM {{ source('sap', 'advertisers') }} WHERE dynamic_variable = 1) b ON a.sap_advertiser_ruko_id = b.sap_advertiser_ruko_id

  UNION ALL

  SELECT a.* FROM {{ ref('fct_sap_ms_metrics') }} a
  JOIN (SELECT sap_advertiser_name FROM {{ source('sap', 'advertisers') }} WHERE dynamic_variable = 0) b ON a.sap_advertiser_name = b.sap_advertiser_name
)

SELECT 'osp_analytics.sap_union_metrics_daily' as table_name, * FROM union_metrics