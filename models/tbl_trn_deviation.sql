

with source_crm_m_reason as (
    select * from {{source('trn_los5_los5db','crm_m_reason')}}
),
source_t_deviation as (
    select * from {{source('trn_los5_los5db','t_deviation')}}
),
source_rsk_m_deviation as (
    select * from {{source('trn_los5_los5db','rsk_m_deviation')}}
),
source_base_sec_mst_systemuser as (
    select * from {{source('trn_ace5_los5db','base_sec_mst_systemuser')}}
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

src_deviation_backup as (
    select * from {{source('trn_biusr_rptdb','tbl_trn_deviation')}}
),

trn_deviation as (
    SELECT 
        td.SZ_APPLICATION_NO as PROPNO,
        td.I_DEVIATION_SRNO as ACTIVITY_ID,
        cast(td.SZ_SEVERITY as int) as SLNO,
        td.C_STATUS as DEVIATION_FLAG, 
        td.SZ_USER_DEVIATION_CODE as DEVIATION_CODE,
        rd.SZ_DEVIATION_DESC as DEVIATION,
        td.SZ_REASON_CODE as REASON_CODE,
        case 
        when cr.SZ_REASON_TYPE_CODE = 'DEV_REJ_REASON' then cr.SZ_REASON_DESC else null end as REASON,
        td.C_MITIGANT_MET_STATUS  as MITIGANT_STATUS,
        cast(td.SZ_SEVERITY as int) as DEVIATION_LEVEL,
        su.SZUSER_NAME as APPORVED_BY,
        DECODE(td.C_USER_SPECIFIED_YN,'Y','Authorized','N','Unauthorized/Rejected') as DEVIATION_STATUS,
        case 
        when cr.SZ_REASON_TYPE_CODE = 'DEV_APP_REASON' then cr.SZ_REASON_DESC else null end as APPR_REASON,
        td.SZ_REMARKS_AUTH as APP_REMARKS,
        'ADMIN' as UPDATE_BY,
        SYSDATE as UPDATED_ON,
        coalesce(td.SZ_APPROVAL_USERID, td.SZ_USERID) as APPROVAL_USERID,
        coalesce(me.DESIGNATION,'') as APPROVAL_DESIGNATION
FROM 
    source_t_deviation td inner join prop1
    ON td.SZ_APPLICATION_NO = prop1.PROPOSALNO
    left join source_rsk_m_deviation rd 
    ON td.SZ_USER_DEVIATION_CODE = rd.SZ_DEVIATION_CODE
    left join {{ref('tbl_mst_employee')}} me 
    ON coalesce(td.SZ_APPROVAL_USERID, td.SZ_USERID) = cast(cast(me.EMP_NO as numeric) as varchar(10000))
    left join source_base_sec_mst_systemuser su 
    ON td.SZ_APPROVAL_USERID = su.SZUSER_CODE
    left join source_crm_m_reason cr 
    ON td.SZ_REASON_CODE = cr.SZ_REASON_CODE
),

carry_forward_deviation as (
    select a.* from src_deviation_backup a LEFT JOIN prop1 b 
    ON A.propno = b.PROPOSALNO where b.PROPOSALNO is null
),

final_trn_deviation as (
    select * from trn_deviation 
    UNION
    select * from carry_forward_deviation
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


trn_deviation_prospect as (
    SELECT 
        td.SZ_APPLICATION_NO as PROPNO,
        td.I_DEVIATION_SRNO as ACTIVITY_ID,
        cast(td.SZ_SEVERITY as int) as SLNO,
        td.C_USER_SPECIFIED_YN as DEVIATION_FLAG, 
        td.SZ_USER_DEVIATION_CODE as DEVIATION_CODE,
        rd.SZ_DEVIATION_DESC as DEVIATION,
        td.SZ_REASON_CODE as REASON_CODE,
        case 
        when cr.SZ_REASON_TYPE_CODE = 'DEV_REJ_REASON' then cr.SZ_REASON_DESC else null end as REASON,
        td.C_MITIGANT_MET_STATUS  as MITIGANT_STATUS,
        cast(td.SZ_SEVERITY as int) as DEVIATION_LEVEL,
        su.SZUSER_NAME as APPORVED_BY,
        DECODE(td.C_USER_SPECIFIED_YN,'Y','Authorized','N','Unauthorized/Rejected') as DEVIATION_STATUS,
        case 
        when cr.SZ_REASON_TYPE_CODE = 'DEV_APP_REASON' then cr.SZ_REASON_DESC else null end as APPR_REASON,
        td.SZ_REMARKS_AUTH as APP_REMARKS,
        'ADMIN' as UPDATE_BY,
        SYSDATE as UPDATED_ON,
        NULL as APPROVAL_USERID,
        NULL as APPROVAL_DESIGNATION
FROM 
    source_t_deviation td inner join crec
    ON td.SZ_APPLICATION_NO = crec.PROPOSALNO
    left join source_rsk_m_deviation rd 
    ON td.SZ_USER_DEVIATION_CODE = rd.SZ_DEVIATION_CODE
    left join {{ref('tbl_mst_employee')}} me 
    ON coalesce(td.SZ_APPROVAL_USERID, td.SZ_USERID) = cast(cast(me.EMP_NO as numeric) as varchar(10000))
    left join source_base_sec_mst_systemuser su 
    ON td.SZ_APPROVAL_USERID = su.SZUSER_CODE
    left join source_crm_m_reason cr 
    ON td.SZ_REASON_CODE = cr.SZ_REASON_CODE
),

carry_forward_deviation_2 as (
    select a.* from final_trn_deviation a LEFT JOIN crec b 
    ON A.propno = b.PROPOSALNO where b.PROPOSALNO is null
),

final_trn_deviation_2 as (
    select * from trn_deviation_prospect
    UNION
    select * from carry_forward_deviation_2
)

select * from final_trn_deviation_2