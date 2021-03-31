import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

import os

import viewclust as vc
from viewclust import slurm
from viewclust.target_series import target_series

from viewclust_vis.job_stack import job_stack

#get the current date time and use it to query current job records
now = datetime.now()
d_from = now.strftime('%Y-%m-%dT%H:00:00')
job_frame = slurm.sacct_jobs('', d_from, '')

#reduce jobs to def-* accounts
defs = job_frame['account'].str.contains('def-')
job_frame = job_frame[defs]

cpus = job_frame['account'].str.contains('_cpu')
job_frame = job_frame[cpus]

pends = job_frame['state'].str.contains('PENDING')
job_frame = job_frame[pends]

accts=job_frame.account.unique()

#reduce account list for testing
#accts=accts[0:64]
#accts=accts[45:50]

month_ago = now - dt.timedelta(days=30)
d_from = month_ago.strftime('%Y-%m-%dT%H:00:00')

#use minimum RAC ask as the target
target=50

use_unit='cpu-eqv'
save_folder='outputs'

#account counter
acct_count=0

#initiate variables
rel_queue_h=[]
c_delta=[]
chq_load=[]

#loop thorugh accounts in job records
for acct in accts:
    print(acct)
    acct_count=acct_count+1
    print(acct_count)
    
    now = datetime.now()
    d_to = now.strftime('%Y-%m-%dT%H:%M:%S')
    ref_time = pd.to_datetime(d_to)

    job_frame = slurm.sacct_jobs(acct, d_from, d_to=d_to)

    # TODO This try/except block is problematic because it will ignoge any account
    # that cause a job_use failure... this includes accounts that have pending jobs
    # but no completed jobs. Instead job)use should eb made to be more robust to 
    # account job record states
    try:
        #get the target, queued and running resources from the job_frame
        target, queued, running, delta = vc.job_use(job_frame, d_from,
                                                    target, d_to=d_to,
                                                    use_unit=use_unit)
    except:
        rel_queue_h.append(None)
        c_delta.append(None)
        chq_load.append(None)

        print('No jobs left in the queue for this account')
        continue

    #get the usage of running jobs
    _, _, run_running, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                      use_unit=use_unit, job_state='running', time_ref='req')
    
    #get the waiting load of queued jobs
    _, _, q_queued, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                   use_unit=use_unit, job_state='queued', time_ref='horizon+req')

    #get the imediate usage time series of completed jobs
    _, _, submit_run, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                     use_unit=use_unit, job_state = 'complete', time_ref='sub')

    #get the imediate requested time series of completed jobs
    _, _, submit_req, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                     use_unit=use_unit, job_state = 'complete', time_ref='sub+req')

    #create the insta_plot
    insta_handle = vc.insta_plot(target, queued, running,
                fig_out=save_folder + '/' + acct +'_'+'insta_plot.html',
                submit_run=submit_run,
                submit_req=submit_req,
                running=run_running,
                queued=q_queued,
                query_bounds=True)

    #create the cumulative plot
    cumu_handle = vc.cumu_plot(target, queued, running,
                fig_out=save_folder + '/' + acct + '_' + 'cumu_plot.html',
                query_bounds=False)

    # Calculate the cumulative target and usage over the query period 
    run_cumu = np.cumsum(running).divide(len(running))
    cumu_rel_delta=run_cumu[-1]/50
    cumu_rel_delta=1-cumu_rel_delta
    if cumu_rel_delta<0:
        cumu_rel_delta=0
  
    c_delta.append(cumu_rel_delta)

    # get the horizon queue measure as the mean of pending queued resources beyond the ref_time.
    horizon_q=q_queued[ref_time:]
    horizon_q_mean=horizon_q.mean()

    horizon_target_mean=50

    chq_load.append((horizon_q_mean/horizon_target_mean)*cumu_rel_delta)

    # Calculate the relative queue horizon as the sum horizon queue sum divided by the horizon target sum. 
    rel_queue_h.append(horizon_q_mean/horizon_target_mean)

accts_series = pd.Series(accts) 
rel_queue_series = pd.Series(rel_queue_h) 
c_delta_series = pd.Series(c_delta)
chq_load_series = pd.Series(chq_load)

frame = { 'account': accts_series, 
    'rel_queue_h': rel_queue_series, 
    'cumu_delta': c_delta_series,
    'chq_load': chq_load_series } 

result = pd.DataFrame(frame) 

result=result.sort_values(by=['chq_load','rel_queue_h'],ascending=False)

fig_vals = go.Figure()
fig_vals.add_trace(go.Scatter(x=result['account'], y=result['rel_queue_h'],
                    mode='lines+markers',
                    name='pending',
                    marker_color='rgba(160,160,160, .8)'))
fig_vals.add_trace(go.Scatter(x=result['account'], y=result['cumu_delta'],
                    mode='lines+markers',
                    name='use',
                    marker_color='rgba(80,80,220, .8)'))
fig_vals.add_trace(go.Scatter(x=result['account'], y=result['chq_load'],
                    mode='lines+markers',
                    name='use',
                    marker_color='rgba(220,60,60, .8)'))
fig_vals.write_html('burst_vals.html')

