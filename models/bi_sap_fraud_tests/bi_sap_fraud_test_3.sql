with get_test_3 as (
  select
    a.sap_account_ruko_id
    , a.sap_account_name
    , id_test
    , kpi
    , test_type
    , timerange
    , lower
    , upper
    , ngr_last_7d
    , earnings_last_7d
    , case
        when earnings_last_7d is null or earnings_last_7d = 0 then null
        when earnings_last_7d < 0 or ngr_last_7d > 0 then null
        else ngr_last_7d / earnings_last_7d
      end as kpi_value
    , case
        when earnings_last_7d is null or earnings_last_7d = 0 then null
        when earnings_last_7d < 0 or ngr_last_7d > 0 then 1
        else
          case
            when ngr_last_7d / earnings_last_7d >= lower and ngr_last_7d / earnings_last_7d <= upper then 1
            else 0
          end
      end as result
  from
    {{ ref('get_traffic_last_7_days')}} a
    join {{ ref('get_earnings_last_7_days')}} b on a.sap_account_ruko_id = b.sap_account_ruko_id
    cross join {{ source('analytics', 'config_sap_tests')}}
  where
    id_test = 3
)

select 'osp_analytics.sap_fraud_test_3' as table_name, * from get_test_3