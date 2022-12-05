
with source_t_document_remarks as (
    select * from {{source('trn_los5_los5db','t_document_remarks')}}

    ),




prop1 as (
SELECT propno 
FROM {{source('trn_biusr_rptdb','tbl_trn_proposallist')}} 
),

src_dr_backup as (
    select * from {{source('trn_biusr_rptdb','tbl_trn_document_remarks')}}
),

trn_doc_remarks as (
SELECT
DR.sz_application_no :: varchar(80) as propno,
DR.i_notification_id :: numeric(38,6) as notifaction_id,
DR.sz_reasoncode :: varchar(80) as reasoncode,
DR.sz_doc_remarks :: varchar(8000) as doc_remarks,
DR.sz_rcudec :: varchar(2) as decision,
DR.sz_userid :: varchar(60) as userid,
DR.sz_activity_code :: varchar(80) as activity_code,
DR.sz_reason_type :: varchar(80) as reason_type,
DR.i_srno ::numeric(38,6) as srno
FROM
source_t_document_remarks DR 
INNER JOIN prop1 PL
ON  DR.sz_application_no = PL.propno 
),

carry_forward_dr as (
    select a.* from src_dr_backup a LEFT JOIN prop1 b 
    ON A.propno = b.propno where b.propno is null
),

final_trn_dr as (
    select * from trn_doc_remarks 
    UNION
    select * from carry_forward_dr
)


select * from final_trn_dr