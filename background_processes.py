import time
import datetime
import logging
import shlex
import os
import re
from dateutil import parser as dateparser
from subprocess import Popen,PIPE, CalledProcessError

from config import JOB_ERROR_STATES, JOB_RUNNING_STATES, JOB_WAITING_STATES
from db_connection import get_sites, add_job_batch, get_processing_state_siteids_by_state,get_jobs_by_siteid_and_abs_state,update_job_states,initialize_processing_state,update_processing_state

# Utils
def escape_string(string):
    return re.sub(r'\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))', '', string)

def get_grid_output_dir(base, normalized_name, _id):
    suffix = base + '/outputs/' + normalized_name + "_" + str(_id)
    return os.path.join(base, suffix)

def job_submission(grid_home,jdl_name):
    site_details = get_sites()
    initialize_processing_state()
    base_dir='{}/site-sonar'.format(grid_home)
    job_path = base_dir + '/JDL/' + jdl_name

    for site in site_details:
        num_jobs = 2 * site['num_nodes']+1
        jobs = []
        logging.info('Submitting %s jobs to the Grid site %s',str(num_jobs - 1), site['site_name'])
        for i in range(1, num_jobs):
            output_dir = get_grid_output_dir(base_dir, site['normalized_name'], i)
            logging.debug('Job path: %s',job_path)
            logging.debug('Base dir: %s',base_dir)
            logging.debug('Site name: %s',site['site_name'])
            logging.debug('Output dir:  %s',output_dir)
            command='alien.py submit {} {} {} {}'.format(job_path, base_dir, site['site_name'], output_dir)
            with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
                for line in p.stdout:
                    logging.info('> %s ',line.rstrip()) 
                    if ("Your new job ID is" in line):
                        job_id = line.split(' ')[-1]
                        job_id = escape_string(job_id)
                        jobs.append(int(job_id))
            if p.returncode != 0:
                raise CalledProcessError(p.returncode, p.args)
        add_job_batch(jobs, site['site_id'])

def job_monitor():
    command = 'alien.py ps -j {}'
    while True:
        # Get sites which are currently waiting
        site_ids = get_processing_state_siteids_by_state('WAITING')
        if len(site_ids) == 0:
            logging.info('No sites are in the WAITING state')
            return
        else:
            site_id_string = ','.join(list(map(str, site_ids)))
            logging.debug('Pending sites for job completion: %s', site_id_string)
            for site_id in site_ids:
                # Get already started jobs in current site
                pending_jobs = get_jobs_by_siteid_and_abs_state(site_id,'STARTED')
                completed_jobs = get_jobs_by_siteid_and_abs_state(site_id,'FINISHED') \
                    + get_jobs_by_siteid_and_abs_state(site_id,'ERROR') \
                    + get_jobs_by_siteid_and_abs_state(site_id,'STALLED')
                try:
                    # If more than 90% jobs are finished, mark the site as complete
                    completed_job_ratio = len(completed_jobs)/(len(pending_jobs)+len(completed_jobs))
                    logging.debug ('Job completion ratio of site %s: %s',site_id, completed_job_ratio)
                except ZeroDivisionError:
                    completed_job_ratio = 0
                if completed_job_ratio >= 0.9:
                    update_processing_state(site_id,'COMPLETE')
                    logging.info('Site %s marked as COMPLETE',site_id)
                pending_job_ids_in_site =[]
                if len(pending_jobs)==0:
                    logging.info('All the jobs in %s site have been completed',site_id) 
                else:
                    for job in pending_jobs:
                        pending_job_ids_in_site.append(job['job_id'])    
                    pending_job_ids_in_site = list(map(str, pending_job_ids_in_site))
                    comma_delimited_joblist = ','.join(pending_job_ids_in_site)
                    logging.info('Jobs pending in site %s: %s',site_id,comma_delimited_joblist)
                    updated_command = command.format(comma_delimited_joblist)
                    # Query the status of jobs in the selected site
                    current_state = []
                    with Popen(shlex.split(updated_command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
                            for line in p.stdout:
                                logging.debug('> %s ',line.rstrip()) 
                                delimited_line = line.split()
                                job_id = delimited_line[1]
                                state = delimited_line[3]
                                current_state.append((job_id,state))
                    if p.returncode != 0:
                        raise CalledProcessError(p.returncode, p.args)

                    # Update jobs states
                    current_time = datetime.datetime.now()
                    for job_tuple in current_state:
                        job_id = job_tuple[0]
                        state = job_tuple[1]
                        if job_tuple[1] in JOB_WAITING_STATES:
                            wait_time_in_hrs = (current_time - dateparser.parse(job['timestamp'])).total_seconds()/3600
                            if wait_time_in_hrs > 0.15:
                                abstract_state = 'STALLED'
                                update_job_states(job_id,abstract_state,state)
                                logging.debug('Job with job id %s marked as %s because it is waiting for %.2f hours',str(job_id),abstract_state,wait_time_in_hrs) 
                        elif job_tuple[1] in JOB_ERROR_STATES:
                            abstract_state = 'ERROR'
                            update_job_states(job_id,abstract_state,state) 
                            logging.debug('Job with job id %s marked as %s as it is in %s state',str(job_id),abstract_state,state)
                        elif job_tuple[1] not in JOB_RUNNING_STATES:
                            abstract_state = 'FINISHED'
                            update_job_states(job_id,abstract_state,state)
                            logging.debug('Job with job id %s marked as %s',str(job_id),abstract_state)
                        
        time.sleep(30)