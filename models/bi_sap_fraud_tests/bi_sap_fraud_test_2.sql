with get_test_2 as (
  select
    a.sap_account_ruko_id
    , a.sap_account_name
    , id_test
    , kpi
    , test_type
    , timerange
    , lower
    , upper
    , clicks_prev_7d
    , clicks_last_7d
    , case
        when clicks_prev_7d is null then null
        when clicks_prev_7d = 0 then null
        else (clicks_last_7d - clicks_prev_7d)/a.clicks_prev_7d
      end as kpi_value
    , case
        when clicks_prev_7d is null then null
        when clicks_prev_7d = 0 then null
        else
          case
            when
              (clicks_last_7d - clicks_prev_7d)/clicks_prev_7d >= lower and
              (clicks_last_7d - clicks_prev_7d)/clicks_prev_7d <= upper
            then 1
            else 0
          end
      end as result
  from
    {{ ref('get_traffic_prev_7_days')}} a
    join {{ ref('get_traffic_last_7_days')}} b on a.sap_account_ruko_id = b.sap_account_ruko_id
    cross join {{ source('analytics','config_sap_tests')}}
  where
    id_test = 2
)

select 'osp_analytics.sap_fraud_test_2' as table_name, * from get_test_2