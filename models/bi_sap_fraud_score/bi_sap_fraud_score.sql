# Tests ready: 1, 2, 3, 4, 6, 7
# Tests on hold: 5, 8

with sap_bia_scores as (
select
  a.sap_account_ruko_id
  , a.sap_account_name
  , a.clicks
  , a.signups
  , a.ftds
  , t1.clicks_last_7d
  , t1.ftds_last_7d
  , t1.kpi_value as t1_kpi_value
  , t1.result as t1_result
  , t2.clicks_prev_7d
  , t2.kpi_value as t2_kpi_value
  , t2.result as t2_result
  , t3.ngr_last_7d as ngr_last_7d
  , t3.earnings_last_7d as earnings_last_7d
  , t3.kpi_value as t3_kpi_value
  , t3.result as t3_result
  , t4.kpi_value as t4_kpi_value
  , t4.result as t4_result
  , t5.avg_deposit as avg_deposit
  , t5.M_3 as M_3
  , t5.stddev_deposits as stddev_deposits
  , t5.kpi_value as t5_kpi_value
  , t5.result as t5_result
  , t6.kpi_value as t6_kpi_value
  , t6.result as t6_result
  , t7.neg_ngr_ftds_lifetime
  , t7.kpi_value as t7_kpi_value
  , t7.result as t7_result
  , case
      when (
            case when t1.result is not null then 1 else 0 end +
            case when t2.result is not null then 1 else 0 end +
            case when t3.result is not null then 1 else 0 end +
            case when t4.result is not null then 1 else 0 end +
            case when t5.result is not null then 1 else 0 end +
            case when t6.result is not null then 1 else 0 end +
            case when t7.result is not null then 1 else 0 end
          ) > 0 then 
            ( coalesce(t1.result,0) + 
              coalesce(t2.result,0) + 
              coalesce(t3.result,0) + 
              coalesce(t4.result,0) + 
              coalesce(t5.result,0) + 
              coalesce(t6.result,0) + 
              coalesce(t7.result,0) ) /
            ( case when t1.result is not null then 1 else 0 end +
              case when t2.result is not null then 1 else 0 end +
              case when t3.result is not null then 1 else 0 end +
              case when t4.result is not null then 1 else 0 end +
              case when t5.result is not null then 1 else 0 end +
              case when t6.result is not null then 1 else 0 end +
              case when t7.result is not null then 1 else 0 end
            )
      else null
    end as score
from (
      select
        sap_account_ruko_id
        , sap_account_name
        , sum(clicks) as clicks
        , sum(signups) as signups
        , sum(ftds) as ftds
      from {{ ref('bi_sap_union_ct_ms_data')}}
      group by
        1, 2
      having clicks > 0
      ) a
left join {{ ref('sap_fraud_test_1')}} t1 on a.sap_account_ruko_id = t1.sap_account_ruko_id
left join {{ ref('sap_fraud_test_2')}} t2 on a.sap_account_ruko_id = t2.sap_account_ruko_id
left join {{ ref('sap_fraud_test_3')}} t3 on a.sap_account_ruko_id = t3.sap_account_ruko_id
left join {{ ref('sap_fraud_test_4')}} t4 on a.sap_account_ruko_id = t4.sap_account_ruko_id
left join {{ ref('sap_fraud_test_5')}} t5 on a.sap_account_ruko_id = t5.sap_account_ruko_id
left join {{ ref('sap_fraud_test_6')}} t6 on a.sap_account_ruko_id = t6.sap_account_ruko_id
left join {{ ref('sap_fraud_test_7')}} t7 on a.sap_account_ruko_id = t7.sap_account_ruko_id
)

select 'osp_analytics.bi_sap_fraud_score' as table_name, * from sap_bia_scores
