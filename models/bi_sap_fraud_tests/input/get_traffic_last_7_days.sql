with get_traffic_last_7_days as (
    
  select
    a.sap_account_ruko_id
    , a.sap_account_name
    , sum(clicks) as clicks_last_7d
    , sum(ftds) as ftds_last_7d
    , sum(net_revenue) as ngr_last_7d
  from {{ ref('bi_sap_union_ct_ms_data')}} a
  left join {{ ref('get_account_max_dates')}} b on a.sap_account_ruko_id = b.sap_account_ruko_id
  where
    # Last 7 days (last date included)
    date > date_sub(last_date, interval 7 day)
  group by
    1, 2

)

select * from get_traffic_last_7_days