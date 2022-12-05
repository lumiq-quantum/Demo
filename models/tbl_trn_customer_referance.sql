
with source_t_bureau_references as (
    select * from {{source('trn_los5_los5db','t_bureau_references')}}
),
source_enquiry_address_details as (
    select * from {{source('trn_los5_los5db','enquiry_address_details')}}
),

prop1 as (
SELECT FF.propno AS PROPOSALNO
FROM {{source('trn_biusr_rptdb','tbl_trn_proposallist')}} FF 
),

src_cust_ref_backup as (
    select * from {{source('trn_biusr_rptdb','tbl_trn_bureau_customer_referance')}}
),

crec_ref as (
    SELECT AR.SZ_APPLICATION_NO AS SZLOANAPPLNNO,
            AR.SZ_REF_TYPE AS SZTYPEREF,
            AR.I_SRL_NO AS ISRLNO
    FROM  source_t_bureau_references AR,prop1
    WHERE AR.SZ_APPLICATION_NO = prop1.PROPOSALNO
),

mst_city_dedup as (SELECT * from 
                    (SELECT *,
                    row_number() over (partition by SZSTATECODE,SZCITYCODE order by DTUSERDATETIME desc) as rownum
                    FROM {{ref('tbl_mst_city')}}
                  ) where rownum =1),

mst_taluk_dedup as (SELECT * from 
                    (SELECT *,
                    row_number() over (partition by SZSTATECODE,SZTALUKCODE order by DTUSERDATETIME desc) as rownum
                    FROM  {{ref('tbl_mst_taluk')}} 
                  ) where rownum =1),

mst_town_dedup as (SELECT * from 
                    (SELECT *,
                    row_number() over (partition by SZSTATECODE,SZTALUKCODE,SZTOWNCODE order by DTUSERDATETIME desc) as rownum
                    FROM {{source('trn_losnew_lmsdb','mst_town')}}
                  ) where rownum =1),

source_on_tbl_trn_qde_bureau_sourcing_det as (
    select * from {{source('trn_onlineusr_apidb','on_tbl_trn_qde_bureau_sourcing_det')}}
),

 source_tbl_trn_bureau_customer_details as (
    select * from {{source('trn_biusr_rptdb_ingest','tbl_trn_bureau_customer_details')}}
),




trn_cust_ref as ( 
    SELECT        AR.SZ_APPLICATION_NO as PROPNO,
                   AR.SZ_REF_TYPE as REF_TYPE,
                   row_number() over() as SLNO,
                   coalesce(AR.SZ_FIRST_NAME,'')||' '||coalesce(AR.SZ_MIDDLE_NAME,'')||' '||coalesce(AR.SZ_LAST_NAME,'')  as REF_NAME,
                   AR.SZ_RELATIONSHIP as RELATION,
                   AD.SZ_ADDRESS_1 as ADDRESS1,
                   AD.SZ_ADDRESS_2 as ADDRESS2,
                   NULL as ADDRESS3,
                   AD.SZ_LANDMARK as LANDMARK,
                   AD.SZ_STATE as STATE,
                   coalesce(CT.SZDESC,'')  as CITY,
                   coalesce(TW.SZTOWNDESC, '')  as TOWN,
                   coalesce(TL.SZTALUKDESC, '') as TALUK,
                   SUBSTRING(AD.SZ_POSTAL_CODE, 1, 6) as PINCODE,
                   AD.SZ_LAND_LINE_NO as CONTACT1,
                   NULL as CONTACT2,
                   AD.I_MOBILENO :: varchar(20) as MOBILE1,
                   AD.SZ_EMAIL as EMAIL,
                   NULL as EMAIL2,
                   SYSDATE as UPDATE_ON

    FROM 
    crec_ref INNER JOIN 
    source_t_bureau_references AR
    ON AR.SZ_APPLICATION_NO = CREC_REF.SZLOANAPPLNNO
    AND AR.SZ_REF_TYPE = CREC_REF.SZTYPEREF
    AND AR.I_SRL_NO = CREC_REF.ISRLNO
    INNER JOIN  source_enquiry_address_details AD  ON
    AR.I_ADDRESS_SR_NO = AD.I_ADDRESS_SR_NO
    LEFT JOIN  mst_city_dedup CT ON 
    CT.SZSTATECODE = AD.SZ_STATE AND CT.SZCITYCODE = AD.SZ_CITY
    LEFT JOIN mst_town_dedup TW 
    ON TW.SZTALUKCODE = AD.SZ_TALUKCODE AND TW.SZTOWNCODE = AD.SZ_TOWNCODE
    LEFT JOIN  mst_taluk_dedup TL
    ON TL.SZSTATECODE = AD.SZ_STATE AND TL.SZTALUKCODE = AD.SZ_TALUKCODE
    ),

carry_forward_cust_ref as (
       select a.* from src_cust_ref_backup a LEFT JOIN prop1 b 
    ON A.propno = b.PROPOSALNO where b.PROPOSALNO is null
),

final_trn_cust_ref as (
    select * from trn_cust_ref 
    UNION
    select * from carry_forward_cust_ref
),

prop2 as (
SELECT T.ENQUIRY_NO as PROPOSALNO
                 FROM source_on_tbl_trn_qde_bureau_sourcing_det t
                where TRUNC(t.enquiry_completed_date) between
                      trunc(date_trunc('MONTH',SYSDATE)) and
                      LAST_DAY(TRUNC(TRUNC(SYSDATE)))
                  and t.ENQUIRY_NO not in
                      (SELECT S.propno
                         from source_tbl_trn_customer_details S
                        WHERE S.SOURCE_FROM = 'DIYA') 
),

crec_ref2 as (
    SELECT AR.SZ_APPLICATION_NO AS SZLOANAPPLNNO,
            AR.SZ_REF_TYPE AS SZTYPEREF,
            AR.I_SRL_NO AS ISRLNO
    FROM  source_t_applicant_references AR,prop2
    WHERE AR.SZ_APPLICATION_NO = prop2.PROPOSALNO
),



trn_cust_ref2 as ( 
    SELECT        AR.SZ_APPLICATION_NO as PROPNO,
                   AR.SZ_REF_TYPE as REF_TYPE,
                   row_number() over() as SLNO,
                   coalesce(AR.SZ_FIRST_NAME,'')||' '||coalesce(AR.SZ_MIDDLE_NAME,'')||' '||coalesce(AR.SZ_LAST_NAME,'')  as REF_NAME,
                   AR.SZ_RELATIONSHIP as RELATION,
                   AD.SZ_ADDRESS_1 as ADDRESS1,
                   AD.SZ_ADDRESS_2 as ADDRESS2,
                   NULL as ADDRESS3,
                   AD.SZ_LANDMARK as LANDMARK,
                   AD.SZ_STATE as STATE,
                   coalesce(CT.SZDESC,'')  as CITY,
                   coalesce(TW.SZTOWNDESC, '')  as TOWN,
                   coalesce(TL.SZTALUKDESC, '') as TALUK,
                   SUBSTRING(AD.SZ_POSTAL_CODE, 1, 6) as PINCODE,
                   AD.SZ_LAND_LINE_NO as CONTACT1,
                   NULL as CONTACT2,
                   AD.I_MOBILENO :: varchar(20) as MOBILE1,
                   AD.SZ_EMAIL as EMAIL,
                   NULL as EMAIL2,
                   SYSDATE as UPDATE_ON

    FROM 
    crec_ref2 INNER JOIN 
    source_t_bureau_references AR
    ON AR.SZ_APPLICATION_NO = CREC_REF2.SZLOANAPPLNNO
    AND AR.SZ_REF_TYPE = CREC_REF2.SZTYPEREF
    AND AR.I_SRL_NO = CREC_REF2.ISRLNO
    INNER JOIN  source_enquiry_address_details AD  ON
    AR.I_ADDRESS_SR_NO = AD.I_ADDRESS_SR_NO
    LEFT JOIN  mst_city_dedup CT ON 
    CT.SZSTATECODE = AD.SZ_STATE AND CT.SZCITYCODE = AD.SZ_CITY
    LEFT JOIN mst_town_dedup TW 
    ON TW.SZTALUKCODE = AD.SZ_TALUKCODE AND TW.SZTOWNCODE = AD.SZ_TOWNCODE
    LEFT JOIN  mst_taluk_dedup TL
    ON TL.SZSTATECODE = AD.SZ_STATE AND TL.SZTALUKCODE = AD.SZ_TALUKCODE
    ),

carry_forward_cust_ref2 as (
       select a.* from final_trn_cust_ref a LEFT JOIN prop2 b 
    ON A.propno = b.PROPOSALNO where b.PROPOSALNO is null
),

final_trn_cust_ref2 as (
    select * from trn_cust_ref2 
    UNION
    select * from carry_forward_cust_ref2
)



select PROPNO,
REF_TYPE,
SLNO,
REF_NAME,
RELATION,
ADDRESS1,
ADDRESS2,
ADDRESS3,
LANDMARK,
STATE,
CITY,
TALUK,
TOWN,
PINCODE,
CONTACT1,
CONTACT2,
MOBILE1,
EMAIL,
EMAIL2,
UPDATE_ON from final_trn_cust_ref2

