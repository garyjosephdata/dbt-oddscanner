with get_earnings_last_7_days as (
  select
    sap_account_ruko_id
    , sap_account_name
    , period
    , period_start_date
    , period_end_date
    , total_earnings as earnings_last_7d
  from {{ ref('bi_sap_union_weekly_earnings')}}
  qualify (row_number() over (partition by sap_account_ruko_id order by period desc)) = 1
)

select * from get_earnings_last_7_days