# 4. Clicks ¦ threshold ¦ weekly
with get_test_4 as (
  select
    sap_account_ruko_id
    , sap_account_name
    , id_test
    , kpi
    , test_type
    , timerange
    , lower
    , upper
    , clicks_last_7d as kpi_value
    , case
        when clicks_last_7d is null then null
        else
          case when clicks_last_7d > lower and clicks_last_7d < upper then 1 else 0 end
      end as result
  from
    {{ ref('get_traffic_last_7_days')}}
    , {{ source('analytics', 'config_sap_tests')}}
  where
    id_test = 4
)

select 'osp_analytics.sap_fraud_test_4' as table_name, * from get_test_4