with get_test_5 as (
  select
    sap_account_ruko_id
    , sap_account_name
    , id_test
    , kpi
    , test_type
    , timerange
    , lower
    , upper
    , ftds
    , avg_deposit
    , M_3
    , stddev_deposits
    , case
        when stddev_deposits is null then null
        when stddev_deposits = 0 then 99999999
        else M_3 / pow(stddev_deposits, 3)
      end as kpi_value
    , case
        when stddev_deposits is null then null
        when stddev_deposits = 0 then 0
        else
          case
            when M_3/pow(stddev_deposits, 3) >= lower and M_3/pow(stddev_deposits, 3) <= upper
            then 1
            else 0
          end
      end as result 
  from
    {{ ref ("get_account_asymmetry_metrics")}}
    , {{ source('analytics', 'config_sap_tests') }}
  where
    id_test = 5
)

select 'osp_analytics.sap_fraud_test_5' as table_name, * from get_test_5