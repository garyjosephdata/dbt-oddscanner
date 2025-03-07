with get_test_1 as (
  select
    sap_account_ruko_id
    , sap_account_name
    , id_test
    , kpi
    , test_type
    , timerange
    , lower
    , upper
    , clicks_last_7d
    , ftds_last_7d
    , case
        when clicks_last_7d is null or ftds_last_7d is null or clicks_last_7d = 0 or ftds_last_7d = 0 then null
        else ftds_last_7d / clicks_last_7d 
      end as kpi_value
    , case
        when clicks_last_7d is null or ftds_last_7d is null or clicks_last_7d = 0 or ftds_last_7d = 0 then null
        else
          case
            when ftds_last_7d / clicks_last_7d  >= lower and ftds_last_7d / clicks_last_7d <= upper then 1
            else 0
          end
      end as result
  from
    {{ ref('get_traffic_last_7_days')}}
    , {{ source('analytics', 'config_sap_tests')}}
  where
    id_test = 1
)

select 'osp_analytics.bi_sap_fraud_test_1' as table_name, * from get_test_1