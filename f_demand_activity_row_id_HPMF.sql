SELECT CASE WHEN count(1) > 0 THEN 'FAILURE' ELSE 'SUCCESS' END as Result,
CASE WHEN count(1) >0 THEN 'MDS to DWH row count failed for f_demand_activity.row_id' ELSE 'SUCCESS' END as Message
from(
select TRGT.row_id from
(select SRC.transaction_id,SRC.sourceinstance,SRC.request_id as proposal_id,SRC3.request_id as project_req_id,SRC3.prj_project_id as project_id
from (select * from  #MDS_TABLE_SCHEMA.hp_kcrt_transactions_final  where cdctype<>'D') SRC
inner join #MDS_TABLE_SCHEMA.hp_kcrt_requests_final SRC1
on SRC1.request_id=SRC.request_id
and SRC1.sourceinstance=SRC.sourceinstance
inner join #MDS_TABLE_SCHEMA.hp_kcrt_request_types_nls_final SRC2
on SRC2.request_type_id=SRC1.request_type_id
and SRC2.sourceinstance=SRC1.sourceinstance
left join #MDS_TABLE_SCHEMA.hp_kcrt_fg_pfm_project_final SRC3
on SRC3.request_id=SRC1.request_id
and SRC3.sourceinstance=SRC1.sourceinstance
left join #MDS_TABLE_SCHEMA.hp_kcrt_fg_pfm_proposal_final SRC4
on SRC4.prop_project_id=SRC3.prj_project_id
and SRC4.sourceinstance=SRC3.sourceinstance
where reference_code in ('_PFM_PROPOSAL') or SRC4.request_id is not null)SRC
left join #DWH_TABLE_SCHEMA.f_demand_activity TRGT
on SRC.transaction_id=TRGT.row_id
and SRC.sourceinstance=TRGT.source_id
where TRGT.primary_sequence_id<>0
and coalesce(SRC.transaction_id,'')<>coalesce(TRGT.row_id,'')
UNION
select TRGT.row_id from
(select SRC.transaction_id,SRC.sourceinstance,SRC.sourceinstance,SRC.request_id as proposal_id,SRC3.request_id as project_req_id,SRC3.prj_project_id as project_id
from (select * from  #MDS_TABLE_SCHEMA.hp_kcrt_transactions_final  where cdctype<>'D') SRC
inner join #MDS_TABLE_SCHEMA.hp_kcrt_requests_final SRC1
on SRC1.request_id=SRC.request_id
and SRC1.sourceinstance=SRC.sourceinstance
inner join #MDS_TABLE_SCHEMA.hp_kcrt_request_types_nls_final SRC2
on SRC2.request_type_id=SRC1.request_type_id
and SRC2.sourceinstance=SRC1.sourceinstance
left join #MDS_TABLE_SCHEMA.hp_kcrt_fg_pfm_project_final SRC3
on SRC3.request_id=SRC1.request_id
and SRC3.sourceinstance=SRC1.sourceinstance
left join #MDS_TABLE_SCHEMA.hp_kcrt_fg_pfm_proposal_final SRC4
on SRC4.prop_project_id=SRC3.prj_project_id
and SRC4.sourceinstance=SRC3.sourceinstance
where reference_code in ('_PFM_PROPOSAL') or SRC4.request_id is not null)SRC
left join #DWH_TABLE_SCHEMA.f_demand_activity TRGT
on SRC.transaction_id=TRGT.row_id
and SRC.sourceinstance=TRGT.source_id

left join #DWH_TABLE_SCHEMA.d_demand d_dem
on d_dem.row_key=TRGT.demand_key
and d_dem.source_id=TRGT.source_id

left join #DWH_TABLE_SCHEMA.f_demand f_dem
on f_dem.demand_key=d_dem.row_key
and f_dem.source_id=d_dem.source_id

where TRGT.primary_sequence_id=0
and coalesce(concat(f_dem.row_id,'&',SRC.OLD_VISIBLE_COLUMN_VALUE,'&',0),'')<>coalesce(TRGT.row_id,'')
)