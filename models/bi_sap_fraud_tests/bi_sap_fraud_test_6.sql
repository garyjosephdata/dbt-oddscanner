with get_test_6 as (
  select
    sap_account_ruko_id
    , sap_account_name
    , id_test
    , kpi
    , test_type
    , timerange
    , lower
    , upper
    , ftds_last_7d as kpi_value
    , case
        when ftds_last_7d is null then null
        else
          case when ftds_last_7d >= lower and ftds_last_7d <= upper then 1 else 0 end
      end as result
  from
    {{ ref('get_traffic_last_7_days')}}
    , {{ source('analytics', 'config_sap_tests')}}
  where
    id_test = 6
)

select 'osp_analytics.sap_fraud_test_6' as table_name, * from get_test_6