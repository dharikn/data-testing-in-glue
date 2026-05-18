from __future__ import annotations
from typing import Any, Dict
from .s3_io import s3_put_json

def write_success_outputs(s3_client,payload:Dict[str,Any],html_summary:Dict[str,Any])->None:
    p=payload['paths']; s3_put_json(s3_client,p['summary_uri'],payload); s3_put_json(s3_client,p['column_results_uri'],payload['columns']); s3_put_json(s3_client,p['pk_results_uri'],payload['pk']); s3_put_json(s3_client,p['rowcount_results_uri'],payload['rowcount']); s3_put_json(s3_client,p['html_summary_uri'],html_summary)
    s3_put_json(s3_client,p['latest_uri'],{'run_ts_utc':payload['run_ts_utc'],'run_root':p['run_root'],'status':payload['status'],'summary_uri':p['summary_uri'],'html_summary_uri':p['html_summary_uri'],'config':payload['config']})

def write_error_outputs(s3_client,err_payload:Dict[str,Any],summary_uri:str,error_uri:str,latest_uri:str)->None:
    try: s3_put_json(s3_client,error_uri,err_payload)
    except Exception: pass
    try: s3_put_json(s3_client,summary_uri,{'run_id':err_payload.get('run_id'),'run_ts_utc':err_payload.get('run_ts_utc'),'job_name':err_payload.get('job_name'),'timestamp_utc':err_payload.get('timestamp_utc'),'status':'ERROR','message':'Job failed. See error_summary.json for details.','paths':err_payload.get('paths',{}),'config':err_payload.get('config',{})})
    except Exception: pass
    try: s3_put_json(s3_client,latest_uri,{'run_ts_utc':err_payload.get('run_ts_utc'),'run_root':err_payload.get('paths',{}).get('run_root'),'status':'ERROR','summary_uri':summary_uri,'error_uri':error_uri,'config':err_payload.get('config',{})})
    except Exception: pass
