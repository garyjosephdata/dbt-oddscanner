with get_account_ftds_metrics_lifetime as (
  select
    sap_account_ruko_id
    , sap_account_name
    , count(distinct tracking_customer_id) as ftds_lifetime
    , sum(case
            when ngr is null then 0
            else
              case when ngr <= 0 then 1 else 0 end
          end) as neg_ngr_ftds_lifetime
    , avg(deposits) as acc_avg_deposit
  from
    {{ ref('get_ftds_metrics_lifetime')}}
  group by
    1, 2
)

select * from get_account_ftds_metrics_lifetime