

with source_t_holdpaymentdetails as (
    select * from {{source('trn_los5_los5db','tr_holdpaymentdetails')}}
),
 source_crm_m_reason as (
    select * from {{source('trn_los5_los5db','crm_m_reason')}}
),

source_on_tbl_trn_qde_sourcing_det as (
    select * from {{source('trn_onlineusr_apidb','on_tbl_trn_qde_sourcing_det')}}
),

 source_tbl_trn_customer_details as (
    select * from {{source('trn_biusr_rptdb_ingest','tbl_trn_customer_details')}}
),

prop1 as (
SELECT FF.propno AS PROPOSALNO
FROM {{source('trn_biusr_rptdb','tbl_trn_proposallist')}} FF 
),


src_deferral_backup as (
    select * from {{source('trn_biusr_rptdb','tbl_trn_deferral')}}
),

trn_deferral as(
    select 
    hp.SZLOANAPPLNNO as PROPNO,
    hp.ISRNO as SL_NO,
    case 
    when gc.SZSYSTEMNAME = '001' AND gc.SZCONDITION = 'HOLDPAYMENTDOCUMENT' 
        then gc.SZDESC else null end as DOCUMENT_NAME,
    case 
    when cr.SZ_REASON_TYPE_CODE = 'HP' then cr.SZ_REASON_DESC else null end as HOLD_REASON,
    hp.IHOLDPERCENT as HOLD_PERCENT,
    hp.FHOLDAMOUNT as HOLD_AMOUNT,
    hp.CRELEASEDYN as HOLD_RELEASE_STATUS,
    hp.DTRELEASED as HOLD_RELEASED_DATE,
    SYSDATE as UPDATE_ON
    from 
    source_t_holdpaymentdetails hp inner join prop1 
    ON hp.SZLOANAPPLNNO = prop1.PROPOSALNO
    left join source_crm_m_reason cr 
    ON hp.SZREASONCODE = cr.SZ_REASON_CODE
    left join {{ref('tbl_mst_generalcondition')}} gc 
    ON hp.SZDOCCODE = hp.SZLOANAPPLNNO
),


carry_forward_deferral as (
       select a.* from src_deferral_backup a LEFT JOIN prop1 b 
    ON A.propno = b.PROPOSALNO where b.PROPOSALNO is null
),

final_trn_deferral as (
    select * from trn_deferral    
    UNION
    select * from carry_forward_deferral 
),

crec as (SELECT T.ENQUIRY_NO as PROPOSALNO
                 FROM source_on_tbl_trn_qde_sourcing_det t
                where TRUNC(t.enquiry_completed_date) between
                      trunc(date_trunc('MONTH',SYSDATE)) and
                      LAST_DAY(TRUNC(TRUNC(SYSDATE)))
                  and t.ENQUIRY_NO not in
                      (SELECT S.propno
                         from source_tbl_trn_customer_details S
                        WHERE S.SOURCE_FROM = 'DIYA')),


trn_deferral_prospect as(
    select 
    hp.SZLOANAPPLNNO as PROPNO,
    hp.ISRNO as SL_NO,
    case 
    when gc.SZSYSTEMNAME = '001' AND gc.SZCONDITION = 'HOLDPAYMENTDOCUMENT' 
        then gc.SZDESC else null end as DOCUMENT_NAME,
    case 
    when cr.SZ_REASON_TYPE_CODE = 'HP' then cr.SZ_REASON_DESC else null end as HOLD_REASON,
    hp.IHOLDPERCENT as HOLD_PERCENT,
    hp.FHOLDAMOUNT as HOLD_AMOUNT,
    hp.CRELEASEDYN as HOLD_RELEASE_STATUS,
    hp.DTRELEASED as HOLD_RELEASED_DATE,
    SYSDATE as UPDATE_ON
   from 
    source_t_holdpaymentdetails hp inner join crec 
    ON hp.SZLOANAPPLNNO = crec.PROPOSALNO
    left join source_crm_m_reason cr 
    ON hp.SZREASONCODE = cr.SZ_REASON_CODE
    left join {{ref('tbl_mst_generalcondition')}} gc 
    ON hp.SZDOCCODE = hp.SZLOANAPPLNNO
),


carry_forward_deferral_2 as (
       select a.* from final_trn_deferral a LEFT JOIN crec b 
    ON A.propno = b.PROPOSALNO where b.PROPOSALNO is null
),

final_trn_deferral_2 as (
    select * from trn_deferral_prospect
    UNION
    select * from carry_forward_deferral_2
)



select * from final_trn_deferral_2