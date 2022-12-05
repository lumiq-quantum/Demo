
with source_t_document_scaning as (
    select * from {{source('trn_los5_los5db','t_document_scaning')}}
),

source_on_tbl_trn_qde_sourcing_det as (
    select * from {{source('trn_onlineusr_apidb','on_tbl_trn_qde_sourcing_det')}}
),

 source_tbl_trn_customer_details as (
    select * from {{source('trn_biusr_rptdb_ingest','tbl_trn_customer_details')}}
),

prop1 as (
SELECT propno
FROM {{source('trn_biusr_rptdb','tbl_trn_proposallist')}}  
),

src_ds_backup as (
    select * from {{source('trn_biusr_rptdb','tbl_trn_document_scaning')}}
),
trn_doc_scaning as (
    SELECT 
        ds.SZ_APPLICATION_NO as PROPNO,
        ds.I_NOTIFICATION_ID as NOTIFICATION_ID,
        ds.I_DOC_REQ_SRNO as DOC_REQ_SRNO,
        ds.SZ_CURRENT_STAGE_CODE as CURRENT_STAGE_CODE,
        ds.SZ_DOC_CODE as DOC_CODE,
        ds.SZ_DOC_DESC as DOC_DESC,
        ds.SZ_DOC_STATUS as DOC_STATUS,
        ds.C_SCAN_YN as SCAN_YN,
        ds.DT_DOC_DATE as DOC_DATE,
        ds.SZ_VERFIY_STATUS as VERFIY_STATUS,
        ds.SZ_VERFIY_RESULTS as VERFIY_RESULTS,
        ds.SZ_VERFIY_REMARKS as VERFIY_REMARKS,
        ds.SZ_PST_VERFIY_STATUS as PST_VERFIY_STATUS,
        ds.SZ_PST_VERFIY_RESULTS as PST_VERFIY_RESULTS,
        ds.SZ_PST_VERFIY_REMARKS as PST_VERFIY_REMARKS,
        ds.C_PRE_SANCTION_RCU_YN as PRE_SANCTION_RCU_YN,
        ds.C_PRE_REROUTE_YN as PRE_REROUTE_YN,
        ds.C_POS_REROUTE_YN as POS_REROUTE_YN,
        ds.SZ_VERIFIED_BY as VERIFIED_BY    
        FROM 
    source_t_document_scaning ds,prop1 
    WHERE ds.sz_application_no = prop1.propno
),


carry_forward_ds as (
    select a.* from src_ds_backup a LEFT JOIN prop1 b 
    ON A.propno = b.propno where b.propno is null
),

final_trn_ds as (
    select * from trn_doc_scaning 
    UNION
    select * from carry_forward_ds
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


trn_doc_scaning_prospect as (
    SELECT 
        ds.SZ_APPLICATION_NO as PROPNO,
        ds.I_NOTIFICATION_ID as NOTIFICATION_ID,
        ds.I_DOC_REQ_SRNO as DOC_REQ_SRNO,
        ds.SZ_CURRENT_STAGE_CODE as CURRENT_STAGE_CODE,
        ds.SZ_DOC_CODE as DOC_CODE,
        ds.SZ_DOC_DESC as DOC_DESC,
        ds.SZ_DOC_STATUS as DOC_STATUS,
        ds.C_SCAN_YN as SCAN_YN,
        ds.DT_DOC_DATE as DOC_DATE,
        ds.SZ_VERFIY_STATUS as VERFIY_STATUS,
        ds.SZ_VERFIY_RESULTS as VERFIY_RESULTS,
        ds.SZ_VERFIY_REMARKS as VERFIY_REMARKS,
        ds.SZ_PST_VERFIY_STATUS as PST_VERFIY_STATUS,
        ds.SZ_PST_VERFIY_RESULTS as PST_VERFIY_RESULTS,
        ds.SZ_PST_VERFIY_REMARKS as PST_VERFIY_REMARKS,
        ds.C_PRE_SANCTION_RCU_YN as PRE_SANCTION_RCU_YN,
        ds.C_PRE_REROUTE_YN as PRE_REROUTE_YN,
        ds.C_POS_REROUTE_YN as POS_REROUTE_YN,
        ds.SZ_VERIFIED_BY as VERIFIED_BY    
        FROM 
    source_t_document_scaning ds,crec 
    WHERE ds.sz_application_no = crec.PROPOSALNO
),

carry_forward_ds_2 as (
    select a.* from final_trn_ds a LEFT JOIN crec b 
    ON A.propno = b.PROPOSALNO where b.PROPOSALNO is null
),

final_trn_ds_2 as (
    select * from trn_doc_scaning_prospect 
    UNION
    select * from carry_forward_ds_2
)

select * from final_trn_ds_2