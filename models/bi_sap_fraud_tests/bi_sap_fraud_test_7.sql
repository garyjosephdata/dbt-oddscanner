with get_test_7 as (
  select
    sap_account_ruko_id
    , sap_account_name
    , id_test
    , kpi
    , test_type
    , timerange
    , lower
    , upper
    , neg_ngr_ftds_lifetime
    , ftds_lifetime
    , case
        when ftds_lifetime is null or ftds_lifetime = 0 then null
        else neg_ngr_ftds_lifetime / ftds_lifetime
      end as kpi_value
    , case
        when ftds_lifetime is null or ftds_lifetime = 0 then null
        else
          case
            when
              neg_ngr_ftds_lifetime / ftds_lifetime >= lower and
              neg_ngr_ftds_lifetime / ftds_lifetime <= upper
              then 1
              else 0
          end
      end as result
  from
    {{ ref('get_account_ftds_metrics_lifetime')}}
    , {{ source('analytics', 'config_sap_tests')}} 
  where
    id_test = 7
)

select 'osp_analytics.sap_fraud_test_7' as table_name, * from get_test_7
