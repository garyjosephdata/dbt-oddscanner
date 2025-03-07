with get_ftds_metrics_lifetime as (
  select
    sap_account_ruko_id
    , sap_account_name
    , tracking_customer_id
    , sum(net_revenue) as ngr
    , sum(deposits) as deposits
  from
    {{ source('sap', 'sap_customers')}}
  where
    tracking_customer_id in (select tracking_customer_id from {{ source('sap', 'sap_customers')}} where ftd = 1)
  group by
    1, 2, 3
)

select * from get_ftds_metrics_lifetime