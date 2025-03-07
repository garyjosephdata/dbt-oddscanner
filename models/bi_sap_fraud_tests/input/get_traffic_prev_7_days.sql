with get_traffic_prev_7_days as (
  select
    a.sap_account_ruko_id
    , a.sap_account_name
    , sum(clicks) as clicks_prev_7d
    , sum(ftds) as ftds_prev_7d
    , sum(net_revenue) as ngr_prev_7d
  from {{ ref('bi_sap_union_ct_ms_data')}} a
  left join {{ ref('get_account_max_dates')}} b on a.sap_account_ruko_id = b.sap_account_ruko_id
  where
    # Prev 7 days
    date <= date_sub(last_date, interval 7 day)
    and
    date > date_sub(last_date, interval 14 day)
  group by
    1, 2
)

select * from get_traffic_prev_7_days