from __future__ import annotations
import sys, uuid, boto3
from typing import Any, Dict, Set, List
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import StorageLevel
from pyspark.sql import functions as F
from .config_loader import load_runtime_config_from_s3
from .evidence_writer import build_value_samples_for_failed_columns, write_column_counts, write_dup_evidence, write_empty_csv
from .normalization_utils import canon_header, normalize_headers, derive_scale_from_redshift
from .output_writer import write_error_outputs, write_success_outputs
from .reconciliation_utils import build_key_hash_df, build_mismatch_join_for_columns, build_normalized_dataset, build_raw_dataset, build_raw_mismatch_join_for_evidence, compute_column_mismatch_counts, duplicate_pk_df, reconcile_keys
from .redshift_reader import build_redshift_query
from .s3_io import ensure_trailing_slash, resolve_single_s3_file, s3_join, spark_write_csv
from .spark_utils import configure_spark, get_logger, now_utc_iso, run_ts_folder_utc, safe_unpersist
logger=get_logger(__name__)

def _read_redshift(glue,conn,db,tmp_dir,query,ctx):
    return glue.create_dynamic_frame.from_options(connection_type='redshift',connection_options={'connectionName':conn,'database':db,'query':query,'redshiftTmpDir':tmp_dir,'useConnectionProperties':'true'},transformation_ctx=ctx).toDF()

def _read_redshift_dbtable(glue,conn,db,tmp_dir,dbtable,ctx):
    return glue.create_dynamic_frame.from_options(connection_type='redshift',connection_options={'connectionName':conn,'database':db,'dbtable':dbtable,'redshiftTmpDir':tmp_dir,'useConnectionProperties':'true'},transformation_ctx=ctx).toDF()

def main():
    args=getResolvedOptions(sys.argv,['JOB_NAME','BOOTSTRAP_CONFIG_BUCKET','BOOTSTRAP_CONFIG_PREFIX']); job_name=args['JOB_NAME']; run_id=str(uuid.uuid4()); run_ts=run_ts_folder_utc(); s3_client=boto3.client('s3'); cfg={}; payload={}; current_step='START'
    sc=SparkContext.getOrCreate(); glue=GlueContext(sc); spark=glue.spark_session; job=Job(glue); job.init(job_name,{'JOB_NAME':job_name})
    try:
        current_step='LOAD_CONFIG'; cfg=load_runtime_config_from_s3(s3_client,job_name,args['BOOTSTRAP_CONFIG_BUCKET'],args['BOOTSTRAP_CONFIG_PREFIX']); configure_spark(spark,int(cfg['JOIN_REPARTITION']),ensure_trailing_slash(cfg['OUTPUT_ROOT'])+'_checkpoints/')
        key=resolve_single_s3_file(s3_client,cfg['BRONZE_S3_BUCKET'],cfg['BRONZE_S3_PREFIX'].lstrip('/'),cfg['FILENAME_REGEX']); file_name=key.rsplit('/',1)[-1]; file_uri=s3_join(cfg['BRONZE_S3_BUCKET'],key); file_name_no_ext=file_name[:-4] if file_name.lower().endswith('.csv') else file_name
        out_root=ensure_trailing_slash(cfg['OUTPUT_ROOT']); schema_tbl=f"{canon_header(cfg['REDSHIFT_SCHEMA'])}.{canon_header(cfg['REDSHIFT_TABLE'])}".replace('/','_'); safe_file=canon_header(file_name) or file_name; base_root=f'{out_root}{schema_tbl}/{safe_file}/'; run_root=f'{base_root}{run_ts}/'; evidence_root=run_root+'evidence/'
        ev={'s3_pk_duplicates':evidence_root+'s3_pk_duplicates','redshift_pk_duplicates':evidence_root+'redshift_pk_duplicates','s3_only_pk':evidence_root+'s3_only_pk','redshift_only_pk':evidence_root+'redshift_only_pk','hash_mismatch_pk':evidence_root+'hash_mismatch_pk','column_mismatch_counts':evidence_root+'column_mismatch_counts','value_mismatch_samples':evidence_root+'value_mismatch_samples'}
        paths={'base_root':base_root,'run_root':run_root,'summary_uri':run_root+'run_summary.json','error_uri':run_root+'error_summary.json','column_results_uri':run_root+'column_results.json','pk_results_uri':run_root+'pk_results.json','rowcount_results_uri':run_root+'rowcount_results.json','html_summary_uri':run_root+'html_summary.json','latest_uri':base_root+'LATEST.json','evidence_root':evidence_root,'evidence':ev}
        payload={'run_id':run_id,'run_ts_utc':run_ts,'job_name':job_name,'timestamp_utc':now_utc_iso(),'status':'RUNNING','message':'','config':{'bootstrap_uri':cfg['BOOTSTRAP_URI'],'s3_config_uri':cfg['S3_CONFIG_URI'],'redshift_config_uri':cfg['REDSHIFT_CONFIG_URI'],'mapping_uri':cfg['MAPPING_URI'],'config_bucket':cfg['CONFIG_BUCKET'],'config_base_prefix':cfg['CONFIG_BASE_PREFIX'],'config_env':cfg['CONFIG_ENV'],'mapping_name':cfg['MAPPING_NAME']},'input':{'bronze_s3_bucket':cfg['BRONZE_S3_BUCKET'],'bronze_s3_prefix':cfg['BRONZE_S3_PREFIX'],'bronze_s3_key':key,'bronze_file_name':file_name,'filename_regex':cfg['FILENAME_REGEX'],'csv_delimiter':cfg['CSV_DELIMITER'],'pk_columns':cfg['PK_COLUMNS'],'ignore_columns':cfg['IGNORE_COLUMNS'],'pii_columns':cfg['PII_COLUMNS'],'redshift_connection':cfg['REDSHIFT_GLUE_CONNECTION_NAME'],'redshift_database':cfg['REDSHIFT_DATABASE'],'redshift_schema':cfg['REDSHIFT_SCHEMA'],'redshift_table':cfg['REDSHIFT_TABLE'],'redshift_tmp_dir':cfg['REDSHIFT_TMP_DIR'],'redshift_file_name_col':cfg['REDSHIFT_FILE_NAME_COL'],'date_time_filter_enabled':bool(cfg['DATE_TIME_FILTER_ENABLED']),'date_time_filter_column':cfg['DATE_TIME_FILTER_COLUMN'],'date_time_filter_from':cfg['DATE_TIME_FILTER_FROM_RAW'],'date_time_filter_to':cfg['DATE_TIME_FILTER_TO_RAW'],'join_repartition':int(cfg['JOIN_REPARTITION']),'pk_dup_sample_limit':int(cfg['PK_DUP_SAMPLE_LIMIT']),'only_sample_limit':int(cfg['ONLY_SAMPLE_LIMIT']),'hash_mismatch_pk_sample_limit':int(cfg['HASH_MISMATCH_PK_SAMPLE_LIMIT']),'mismatch_sample_per_column':int(cfg['PER_COLUMN_MISMATCH_SAMPLE_LIMIT']),'fail_job_on_dq':bool(cfg['FAIL_JOB_ON_DQ'])},'paths':paths,'counts':{},'rowcount':{},'pk':{},'columns':{},'notes':[]}
        current_step='READ_INPUTS'; bronze=normalize_headers(spark.read.option('header','true').option('sep',cfg['CSV_DELIMITER']).option('quote','"').option('escape','"').option('multiline','false').option('inferSchema','false').csv(file_uri)).withColumn('_source_file',F.lit(file_name))
        rs_query=build_redshift_query(file_name,file_name_no_ext,cfg['REDSHIFT_SCHEMA'],cfg['REDSHIFT_TABLE'],cfg['REDSHIFT_FILE_NAME_COL'],cfg['DATE_TIME_FILTER_ENABLED'],cfg['DATE_TIME_FILTER_COLUMN'],cfg['DATE_TIME_FILTER_FROM_OP'],cfg['DATE_TIME_FILTER_FROM_VAL'],cfg['DATE_TIME_FILTER_TO_OP'],cfg['DATE_TIME_FILTER_TO_VAL']); payload['input']['redshift_query']=rs_query
        redshift=normalize_headers(_read_redshift(glue,cfg['REDSHIFT_GLUE_CONNECTION_NAME'],cfg['REDSHIFT_DATABASE'],cfg['REDSHIFT_TMP_DIR'],rs_query,'read_redshift_filtered')).filter(F.trim(F.lower(F.col(canon_header(cfg['REDSHIFT_FILE_NAME_COL'])))).isin(file_name.lower().strip(),file_name_no_ext.lower().strip()))
        current_step='DETERMINE_COLUMNS'; pk_cols=[canon_header(c) for c in cfg['PK_COLUMNS'] if str(c).strip()]; ignore_set={canon_header(c) for c in cfg['IGNORE_COLUMNS'] if str(c).strip()} | {'_source_file'}; pii_set={canon_header(c) for c in cfg['PII_COLUMNS'] if str(c).strip()}
        if [c for c in pk_cols if c not in bronze.columns]: raise RuntimeError(f"PK column missing in CSV: {[c for c in pk_cols if c not in bronze.columns]}")
        if [c for c in pk_cols if c not in redshift.columns]: raise RuntimeError(f"PK column missing in Redshift: {[c for c in pk_cols if c not in redshift.columns]}")
        bronze_cols={c for c in bronze.columns if c not in ignore_set}; rs_cols={c for c in redshift.columns if c not in ignore_set}; missing=sorted([c for c in bronze_cols if c not in rs_cols and c not in set(pk_cols)])
        if missing: raise RuntimeError(f'Columns present in S3 but missing in Redshift (not ignored): {missing[:200]}')
        compare_cols=sorted([c for c in bronze_cols if c in rs_cols and c not in set(pk_cols)]); payload['counts']['compare_columns']=len(compare_cols)
        if not compare_cols: raise RuntimeError('No compare columns found after exclusions.')
        current_step='NUMERIC_SCALE'; scale_by_col={}
        try:
            meta=_read_redshift_dbtable(glue,cfg['REDSHIFT_GLUE_CONNECTION_NAME'],cfg['REDSHIFT_DATABASE'],cfg['REDSHIFT_TMP_DIR'],'information_schema.columns','read_redshift_column_meta').filter((F.col('table_schema')==F.lit(cfg['REDSHIFT_SCHEMA'])) & (F.col('table_name')==F.lit(cfg['REDSHIFT_TABLE'])))
            for r in meta.select('column_name','data_type','numeric_scale').collect():
                cn=canon_header((r['column_name'] or '').strip()); dt=(r['data_type'] or '').strip().lower(); ns=r['numeric_scale']
                if cn and ('numeric' in dt or 'decimal' in dt) and ns is not None: scale_by_col[cn]=int(ns)
            payload['counts']['redshift_numeric_scale_columns']=len(scale_by_col); payload['notes'].append('numeric_scale_mode=information_schema')
        except Exception as e:
            payload['notes'].append(f'redshift_column_meta_unavailable={str(e)}')
        if not scale_by_col:
            try: scale_by_col=derive_scale_from_redshift(redshift,compare_cols,18); payload['counts']['redshift_derived_scale_columns']=len(scale_by_col); payload['notes'].append('numeric_scale_mode=derived_from_redshift_values')
            except Exception as e: payload['notes'].append(f'redshift_derived_scale_unavailable={str(e)}')
        current_step='NORMALIZE_HASH_NARROW'; b_norm,pk_norm_cols,norm_cols=build_normalized_dataset(bronze,pk_cols,compare_cols,scale_by_col,'b'); r_norm,_,_=build_normalized_dataset(redshift,pk_cols,compare_cols,scale_by_col,'r'); b_norm=b_norm.persist(StorageLevel.MEMORY_AND_DISK); r_norm=r_norm.persist(StorageLevel.MEMORY_AND_DISK); bronze_rows=int(b_norm.count()); redshift_rows=int(r_norm.count())
        payload['counts']['bronze_rows']=bronze_rows; payload['counts']['redshift_rows']=redshift_rows; diff=bronze_rows-redshift_rows; pct=float(diff)/float(redshift_rows if redshift_rows else 1)*100.0; payload['rowcount']={'bronze_rows':bronze_rows,'redshift_rows':redshift_rows,'difference':diff,'percent_diff':float(round(pct,6)),'status':'FAIL' if diff else 'PASS'}
        current_step='PK_DUPLICATES'; pk_norm_pairs=list(zip(pk_norm_cols,pk_cols)); s3_dups=duplicate_pk_df(b_norm,pk_norm_cols).persist(StorageLevel.MEMORY_AND_DISK); rs_dups=duplicate_pk_df(r_norm,pk_norm_cols).persist(StorageLevel.MEMORY_AND_DISK); s3_dup_cnt=int(s3_dups.count()); rs_dup_cnt=int(rs_dups.count()); payload['counts']['s3_pk_duplicates']=s3_dup_cnt; payload['counts']['redshift_pk_duplicates']=rs_dup_cnt
        write_dup_evidence(s3_dups,ev['s3_pk_duplicates'],pk_norm_pairs,pii_set,cfg['PK_DUP_SAMPLE_LIMIT']) if s3_dup_cnt else write_empty_csv(spark,ev['s3_pk_duplicates'],'pk_mask string, count int')
        write_dup_evidence(rs_dups,ev['redshift_pk_duplicates'],pk_norm_pairs,pii_set,cfg['PK_DUP_SAMPLE_LIMIT']) if rs_dup_cnt else write_empty_csv(spark,ev['redshift_pk_duplicates'],'pk_mask string, count int')
        current_step='RECONCILE_KEYS_FULL_OUTER'; b_key=build_key_hash_df(b_norm,pk_norm_cols,pk_norm_pairs,pii_set,'s3'); r_key=build_key_hash_df(r_norm,pk_norm_cols,pk_norm_pairs,pii_set,'rs'); recon=reconcile_keys(b_key,r_key,pk_norm_cols,cfg['JOIN_REPARTITION'],cfg['CHECKPOINT_ENABLED']); s3_only=recon['s3_only'].persist(StorageLevel.MEMORY_AND_DISK); rs_only=recon['rs_only'].persist(StorageLevel.MEMORY_AND_DISK); mismatched_pk=recon['hash_mismatch'].persist(StorageLevel.MEMORY_AND_DISK); s3_only_cnt=int(s3_only.count()); rs_only_cnt=int(rs_only.count()); mismatch_pk_count=int(mismatched_pk.count())
        payload['counts']['s3_only_pk']=s3_only_cnt; payload['counts']['redshift_only_pk']=rs_only_cnt; payload['counts']['hash_mismatch_pk']=mismatch_pk_count
        spark_write_csv(s3_only.select('pk_mask'),ev['s3_only_pk'],cfg['ONLY_SAMPLE_LIMIT']) if s3_only_cnt else write_empty_csv(spark,ev['s3_only_pk'],'pk_mask string')
        spark_write_csv(rs_only.select('pk_mask'),ev['redshift_only_pk'],cfg['ONLY_SAMPLE_LIMIT']) if rs_only_cnt else write_empty_csv(spark,ev['redshift_only_pk'],'pk_mask string')
        spark_write_csv(mismatched_pk.select('pk_mask'),ev['hash_mismatch_pk'],cfg['HASH_MISMATCH_PK_SAMPLE_LIMIT']) if mismatch_pk_count else write_empty_csv(spark,ev['hash_mismatch_pk'],'pk_mask string')
        current_step='COLUMN_COUNTS_AGG_FIRST'; per_rows=[]; failed=[]; per_df=spark.createDataFrame([], 'column_name string, mismatch_count long')
        if mismatch_pk_count>0:
            compare_pk=mismatched_pk.select(*pk_norm_cols)
            if mismatch_pk_count>int(cfg['MAX_HASH_MISMATCH_ROWS_FOR_COLUMN_COMPARE']): compare_pk=compare_pk.sample(False,min(1.0,(float(cfg['MAX_HASH_MISMATCH_ROWS_FOR_COLUMN_COMPARE'])/float(mismatch_pk_count))*1.05),seed=42).limit(int(cfg['MAX_HASH_MISMATCH_ROWS_FOR_COLUMN_COMPARE'])); payload['notes'].append(f"column_level_comparison_sampled=true; used {cfg['MAX_HASH_MISMATCH_ROWS_FOR_COLUMN_COMPARE']} of {mismatch_pk_count} hash mismatch PKs")
            use_pairing=(s3_dup_cnt>0) or (rs_dup_cnt>0); payload['notes'].append(f"column_diff_join_mode={'ROW_NUMBER_PAIRING' if use_pairing else 'DIRECT_PK_JOIN'}")
            jr_norm=build_mismatch_join_for_columns(b_norm,r_norm,compare_pk,pk_norm_cols,compare_cols,cfg['JOIN_REPARTITION'],use_pairing); per_df,per_rows,failed=compute_column_mismatch_counts(jr_norm,compare_cols,cfg['COLUMN_COMPARE_BATCH_SIZE']); write_column_counts(per_df,ev['column_mismatch_counts'])
            if failed:
                b_raw=build_raw_dataset(bronze,pk_cols,failed,'b').persist(StorageLevel.MEMORY_AND_DISK); r_raw=build_raw_dataset(redshift,pk_cols,failed,'r').persist(StorageLevel.MEMORY_AND_DISK); _=b_raw.count(); _=r_raw.count(); ev_jr=build_raw_mismatch_join_for_evidence(b_raw,r_raw,jr_norm,failed,pk_norm_cols,pk_norm_pairs,pii_set,cfg['JOIN_REPARTITION']); samples=build_value_samples_for_failed_columns(ev_jr,failed,pk_norm_pairs,pii_set,cfg['PER_COLUMN_MISMATCH_SAMPLE_LIMIT'],cfg['COLUMN_COMPARE_BATCH_SIZE']); spark_write_csv(samples,ev['value_mismatch_samples'],cfg['VALUE_MISMATCH_GLOBAL_SAMPLE_LIMIT']); safe_unpersist(b_raw,r_raw,ev_jr)
            else: write_empty_csv(spark,ev['value_mismatch_samples'],'pk_mask string, column_name string, s3_value string, redshift_value string')
            safe_unpersist(jr_norm)
        else:
            write_column_counts(per_df,ev['column_mismatch_counts']); write_empty_csv(spark,ev['value_mismatch_samples'],'pk_mask string, column_name string, s3_value string, redshift_value string')
        bycol={r['column_name']:int(r['mismatch_count']) for r in per_rows}; col_status=[{'column_name':c,'status':'FAIL' if bycol.get(c,0)>0 else 'PASS','mismatch_count':int(bycol.get(c,0)),'samples_csv':ev['value_mismatch_samples'],'samples_limit':int(cfg['PER_COLUMN_MISMATCH_SAMPLE_LIMIT'])} for c in compare_cols]
        payload['columns']={'total_compare_columns':len(compare_cols),'failed_columns':sum(1 for x in col_status if x['status']=='FAIL'),'passed_columns':sum(1 for x in col_status if x['status']=='PASS'),'results':col_status}
        payload['pk']={'s3_pk_duplicates':s3_dup_cnt,'redshift_pk_duplicates':rs_dup_cnt,'s3_only_pk':s3_only_cnt,'redshift_only_pk':rs_only_cnt,'hash_mismatch_pk':mismatch_pk_count,'sample_limits':{'pk_dup':int(cfg['PK_DUP_SAMPLE_LIMIT']),'pk_only':int(cfg['ONLY_SAMPLE_LIMIT']),'hash_mismatch_pk':int(cfg['HASH_MISMATCH_PK_SAMPLE_LIMIT'])},'evidence':{'s3_pk_duplicates':ev['s3_pk_duplicates'],'redshift_pk_duplicates':ev['redshift_pk_duplicates'],'s3_only_pk':ev['s3_only_pk'],'redshift_only_pk':ev['redshift_only_pk'],'hash_mismatch_pk':ev['hash_mismatch_pk']}}
        fail=any([s3_dup_cnt>0,rs_dup_cnt>0,s3_only_cnt>0,rs_only_cnt>0,mismatch_pk_count>0,payload['rowcount']['status']=='FAIL']); payload['status']='FAIL' if fail else 'PASS'; payload['message']='DQ FAILED. Evidence written. Job exits successfully even on FAIL.' if fail else 'DQ PASSED. No mismatches found.'
        top_failed=sorted([x for x in col_status if x['status']=='FAIL'],key=lambda x:int(x.get('mismatch_count',0)),reverse=True)[:20]
        html={'run_ts_utc':run_ts,'run_root':run_root,'status':payload['status'],'job_name':job_name,'schema':cfg['REDSHIFT_SCHEMA'],'table':cfg['REDSHIFT_TABLE'],'file_name':file_name,'rowcount':payload['rowcount'],'pk':payload['pk'],'failed_columns_top':top_failed,'paths':payload['paths'],'config':payload['config']}
        write_success_outputs(s3_client,payload,html); safe_unpersist(b_norm,r_norm,s3_dups,rs_dups,s3_only,rs_only,mismatched_pk,recon.get('joined'))
        if fail and cfg['FAIL_JOB_ON_DQ']: raise RuntimeError('DQ FAIL configured to fail job. See run_summary.json + evidence in S3.')
        job.commit(); logger.info(f"Final DQ Status: {payload['status']}")
    except Exception as e:
        err={'run_id':run_id,'run_ts_utc':run_ts,'job_name':job_name,'timestamp_utc':now_utc_iso(),'status':'ERROR','failed_step':current_step,'error':str(e),'paths':payload.get('paths',{}) if payload else {},'input':payload.get('input',{}) if payload else {},'config':payload.get('config',{}) if payload else {}}
        try:
            if payload and payload.get('paths'): write_error_outputs(s3_client,err,payload['paths']['summary_uri'],payload['paths']['error_uri'],payload['paths']['latest_uri'])
        except Exception: pass
        try: job.commit()
        finally: logger.info('Final DQ Status: ERROR'); raise
if __name__=='__main__': main()
