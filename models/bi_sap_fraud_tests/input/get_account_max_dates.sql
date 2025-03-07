with get_account_max_dates as (

  select
    sap_account_ruko_id
    , sap_account_name
    , max(date) as last_date
  from
    {{ ref('bi_sap_union_ct_ms_data')}}
  group by
    1, 2

)

select * from get_account_max_dates