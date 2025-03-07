with get_account_asymmetry_metrics as (
  select
    sap_account_ruko_id
    , sap_account_name
    , count(distinct tracking_customer_id) as ftds
    , max(acc_avg_deposit) as avg_deposit
    , avg(Y) as M_3
    , stddev(deposits) as stddev_deposits
  from
    (select
      a.sap_account_ruko_id
      , a.sap_account_name
      , a.tracking_customer_id
      , a.deposits
      , b.acc_avg_deposit
      , pow(a.deposits - b.acc_avg_deposit, 3) as Y
    from
      {{ ref('get_ftds_metrics_lifetime')}} a
      left join {{ ref('get_account_ftds_metrics_lifetime')}} b on a.sap_account_ruko_id = b.sap_account_ruko_id
    )
  group by
    1, 2
)

select * from get_account_asymmetry_metrics