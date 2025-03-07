with union_monthly_earnings as (
  select 
    'sap_dv_monthly_earnings' as source_table,
    'dv' as source,
    'monthly' as frequency,
    sap_account_id,
    sap_account_ruko_id,
    sap_account_name,
    sap_advertiser_id,
    a.sap_advertiser_ruko_id,
    sap_advertiser_name,
    sap_deal_id,
    sap_deal_ruko_id,
    sap_deal_name,
    month as period,
    month_start_date as period_start_date,
    month_end_date as period_end_date,
    cpa_earnings,
    rs_earnings,
    total_earnings,
    invoice_total_earnings
  from {{ source('sap', 'dv_monthly_earnings')}} a
  join (select sap_advertiser_ruko_id from {{ source('sap','advertisers')}} where dynamic_variable = 1) b on a.sap_advertiser_ruko_id = b.sap_advertiser_ruko_id
  
  union all
  
  select 
    'sap_ct_monthly_earnings' as source_table,
    'ms' as source,
    'monthly' as frequency,
    sap_account_id,
    sap_account_ruko_id,
    sap_account_name,
    sap_advertiser_id,
    a.sap_advertiser_ruko_id,
    sap_advertiser_name,
    sap_deal_id,
    sap_deal_ruko_id,
    sap_deal_name,
    month as period,
    month_start_date as period_start_date,
    month_end_date as period_end_date,
    cpa_earnings,
    rs_earnings,
    total_earnings,
    invoice_total_earnings
  from {{ source('sap','ct_monthly_earnings')}} a
  join (select sap_advertiser_ruko_id from {{ source('sap','advertisers')}} where dynamic_variable = 0) b on a.sap_advertiser_ruko_id = b.sap_advertiser_ruko_id
)

select 'osp_analytics.sap_union_monthly_earnings' as table_name, * from union_monthly_earnings
