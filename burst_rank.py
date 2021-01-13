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
#from viewclust_vis.insta_plot import insta_plot
#from viewclust_vis.cumu_plot import cumu_plot

job_frame = slurm.sacct_jobs('', '2021-01-12', '')
print(len(job_frame))

#reduce jobs to def-* accounts
defs = job_frame['account'].str.contains('def-')
job_frame = job_frame[defs]
print(len(job_frame))

cpus = job_frame['account'].str.contains('_cpu')
job_frame = job_frame[cpus]
print(len(job_frame))

pends = job_frame['state'].str.contains('PENDING')
job_frame = job_frame[pends]
print(len(job_frame))
#print(job_frame)

accts=job_frame.account.unique()
#print(accts)
print(len(accts))

#accts=accts[0:12]
accts=accts[45:50]

d_from='2020-11-12'

target=50

use_unit='cpu-eqv'
safe_folder='outputs'

acct_count=0

rel_use_d7=[]
q_load=[]
rel_queue_h=[]
hq_load=[]

for acct in accts:
    print(acct)
    acct_count=acct_count+1
    #if acct_count > 50:
    #    continue
    print(acct_count)
    
    now = datetime.now()
    d_to = now.strftime('%Y-%m-%dT%H:%M:%S')
    ref_time = pd.to_datetime(d_to)
    print(d_to)

    job_frame = slurm.sacct_jobs(acct, d_from, d_to=d_to)
    print('job_frame length:')
    print(len(job_frame))
    #print(job_frame)

    # TODO This try/except block is problematic because it will ignoge any account
    # that cause a job_use failure... this includes accounts that have pending jobs
    # but no completed jobs. Instead job)use should eb made to be more robust to 
    # account job record states
    try:
        target, queued, running, delta = vc.job_use(job_frame, d_from,
                                                    target, d_to=d_to,
                                                    use_unit=use_unit)
    except:
        rel_use_d7.append(None)
        q_load.append(None)
        rel_queue_h.append(None)
        hq_load.append(None)
        print('No jobs left in the queue for this account')
        continue

    _, _, run_running, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                      use_unit=use_unit, job_state='running', time_ref='req')
    _, _, q_queued, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                   use_unit=use_unit, job_state='queued', time_ref='horizon+req')

#    user_running_cat = vc.get_users_run(job_frame, d_from, target, d_to=d_to,
#                                        use_unit=use_unit)

    _, _, submit_run, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                     use_unit=use_unit, job_state = 'complete', time_ref='sub')

    _, _, submit_req, _ = vc.job_use(job_frame, d_from, target, d_to=d_to,
                                     use_unit=use_unit, job_state = 'complete', time_ref='sub+req')

    
    insta_handle = vc.insta_plot(target, queued, running,
                fig_out=safe_folder + '/' + acct +'_'+'insta_plot.html',
#                user_run=user_running_cat,
                submit_run=submit_run,
                submit_req=submit_req,
                running=run_running,
                queued=q_queued,
                query_bounds=True)

    cumu_handle = vc.cumu_plot(target, queued, running,
                fig_out=safe_folder + '/' + acct + '_' + 'cumu_plot.html',
#                user_run=user_running_cat,
#                submit_run=submit_run,
                query_bounds=False)

    # TODO make this robust
    # make the timestamp for 7 days in the past from the ref_time (now-ish)
    d7=(ref_time - pd.Timedelta(7, unit='d'))
    d7t=pd.Timestamp(d7).strftime('%Y-%m-%d %H:00:00')

    # Calculate the cumulative target and usage over the query period 
    target_cumu = np.cumsum(target).divide(len(target))
    run_cumu = np.cumsum(running).divide(len(running))

    # Calculate the area under the cumulative target and use over the past 7 days.
    t=target_cumu[d7t:ref_time].mean()
    r=run_cumu[d7t:ref_time].mean()
    # Calculate the area under the queue load over that past 7 days.
    q=queued[d7t:ref_time].mean()
    
    # Append the realtive use for this account as last 7 day cumulative use 
    # divided by the cumulative target over the same period.
    rel_use_d7.append(r/t)
    # Append the pending queue load for this account as average of the last 7 days. 
    q_load.append(q)

    print(rel_use_d7)

    # Set a time stamp 7 days past the ref_time.
    d7p=(ref_time + pd.Timedelta(7, unit='d'))
    d7pt=pd.Timestamp(d7p).strftime('%Y-%m-%d %H:00:00')

    # get the horizon queue measure as the mean of pending queued resources beyond the ref_time.
    hq=q_queued[ref_time:]
    hql=len(hq)
    hqm=hq.mean()
    hq_load.append(hqm)
    # Calculate the equivalent of the 50 CE target over the period of the horizon queue measure.
    htm=50

    print('hq SUM')
    print(hql)
    print(hqm)
    print(htm)
    # Calculate the relative queue horizon as the sum horizon queue sum divided by the horizon target sum. 
    rel_queue_h.append(hqm/htm)
    print('rel_queue_h')
    print(rel_queue_h)


accts_series = pd.Series(accts) 
rel_use_series = pd.Series(rel_use_d7) 
q_load_series = pd.Series(q_load) 
hq_load_series = pd.Series(hq_load) 
rel_queue_series = pd.Series(rel_queue_h) 
  
frame = { 'account': accts_series, 'rel_use_d7': rel_use_series, 
            'q_load': q_load_series, 'rel_queue_h': rel_queue_series, 
            'hq_load': hq_load_series } 
  
result = pd.DataFrame(frame) 

print(result)
  
result=result.sort_values(by=['rel_queue_h'],ascending=False)



fig = go.Figure()

# Add traces
fig.add_trace(go.Scatter(x=result['account'], y=result['rel_queue_h'],
                    mode='lines+markers',
                    name='pending',
                    marker_color='rgba(220,80,80, .8)'))
fig.add_trace(go.Scatter(x=result['account'], y=result['rel_use_d7'],
                    mode='lines+markers',
                    name='use',
                    marker_color='rgba(80,80,220, .8)'))
fig.write_html('burst_rank.html')

exit()





fig_scat = px.scatter(result,
                        x='account',
                        y='rel_use_d7',
                        color='q_load',
                        opacity=.7)

fig_scat.add_trace(go.Scatter(x=hq_load_series.index, y=hq_load_series,
                            mode='markers',
                            name='horizon',
                            marker_color='rgba(80,80,220, .8)'))

fig_scat.update_layout(
    title=go.layout.Title(
        text="account pain: "
    ),
    xaxis=go.layout.XAxis(
        title=go.layout.xaxis.Title(
            text="Account",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="#7f7f7f"
            )
        )
    ),
    yaxis=go.layout.YAxis(
        title=go.layout.yaxis.Title(
            text='q load / usage',
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="#7f7f7f"
            )
        )
    )
)
fig_scat.write_html('burst_rank.html')
#fig_dict['fig_start_wait'] = fig_scat

#output['acct'] = accts
#output['rel_use'] = rel_use_d7

#output = {'account': accts,
#        'rel_use': rel_use_d7
#        }

#df = pd.DataFrame(output, columns = ['account', 'rel_use_d7'])
#df['rel_use_d7'] = df['rel_use_d7'].astype(float)

#print (df)
