import time
import shlex
import json
import re
from dateutil import parser as dateparser
from subprocess import Popen,PIPE, CalledProcessError

from config import *
from db_connection import *
#from output_parser import clear_output_dir


# Utils
def escape_string(string):
    return re.sub(r'\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))', '', string)


def get_grid_output_dir(base, normalized_name, _id):
    out_dir = base + '/' + normalized_name + "_" + str(_id)
    return os.path.join(out_dir)


def clear_grid_output_dir():
    """ 
    Clear GRID_SITE_SONAR_OUTPUT_DIR of the user if it exists
    """
    logging.info('Clearing User Grid output directory... : %s/%s',GRID_USER_HOME,GRID_SITE_SONAR_OUTPUT_DIR)
    command = 'alien.py rm -rf {}/{}'.format(GRID_USER_HOME,GRID_SITE_SONAR_OUTPUT_DIR)
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            logging.debug('> %s ',line.rstrip()) 
    if p.returncode != 0:
        if p.returncode == 5 :
            logging.debug('Grid output directory does not exist. Skipping...')
        else:
            raise CalledProcessError(p.returncode, p.args)
    else:
        logging.info('Grid output folder cleared succesfully')


def job_submission(jdl_name):
    """ 
    Submit JOB FACTOR number of jobs per each Grid site

    Args:
        GRID_USER_HOME (str):       Path to User Grid Home Directory defined in config
        GRID_SITE_SONAR_HOME (str): Path to User Site sonar Directory defined in config
        jdl_name (str):             Name of the JDL file in the /JDL directory

    """
    site_details = get_sites()
    success_flag = initialize_processing_state()
    if success_flag:
        job_path = GRID_USER_HOME + '/' + GRID_SITE_SONAR_HOME + '/JDL/' + jdl_name

        for site in site_details:
            num_jobs = JOB_FACTOR * site['num_nodes']+1
            sitename = site['site_name']
            logging.info('Submitting %s jobs to the Grid site %s',str(num_jobs - 1), sitename)
            for i in range(1, num_jobs):
                output_dir = get_grid_output_dir(GRID_SITE_SONAR_OUTPUT_DIR, site['normalized_name'], i)
                site_sonar_dir = GRID_USER_HOME + '/' + GRID_SITE_SONAR_HOME
                logging.debug('Job path: %s',job_path)
                logging.debug('Site sonar dir($1): %s',site_sonar_dir)
                logging.debug('Site name($2): %s',sitename)
                logging.debug('Output dir($3):  %s',output_dir)
                command='alien.py submit {} {} {} {}'.format(job_path, site_sonar_dir, sitename, output_dir)
                logging.info(command)
                with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
                    logging.info(shlex.split(command))
                    for line in p.stdout:
                        logging.debug('> %s ',line.rstrip()) 
                        if ("Your new job ID is" in line):
                            job_id = line.split(' ')[-1]
                            job_id = escape_string(job_id)
                if p.returncode != 0:
                    raise CalledProcessError(p.returncode, p.args)
                add_job(job_id, site['site_id'])
            update_site_last_update_time(site['site_id'])
    else:
        logging.error("Aborting the submission process...")

# def job_monitor():
#     """
#     Monitor the state of the jobs running in the Grid.
#     Monitor Grid site coverage
#     """
#     command = 'alien.py ps -j {}'
#     while True:
#         # Get sites which are currently waiting
#         site_ids = get_processing_state_siteids_by_state('WAITING')
#         if len(site_ids) == 0:
#             logging.info('No sites are in the WAITING state')
#             return
#         else:
#             site_id_string = ','.join(list(map(str, site_ids)))
#             for site_id in site_ids:
#                 # Get already started jobs in current site
#                 pending_jobs = get_jobs_by_siteid_and_abs_state(site_id,'STARTED')
#                 completed_jobs = get_jobs_by_siteid_and_abs_state(site_id,'FINISHED') \
#                     + get_jobs_by_siteid_and_abs_state(site_id,'ERROR') \
#                     + get_jobs_by_siteid_and_abs_state(site_id,'STALLED')
#                 erroneous_jobs = get_jobs_by_siteid_and_abs_state(site_id,'ERROR') \
#                     + get_jobs_by_siteid_and_abs_state(site_id,'STALLED')
#                 try:
#                     completed_job_ratio = len(completed_jobs)/(len(pending_jobs)+len(completed_jobs))
#                     logging.info ('Job completion ratio of site %s: %s',site_id, completed_job_ratio)
#                     errorneous_job_ratio = len(erroneous_jobs)/len(completed_jobs)
#                 except ZeroDivisionError:
#                     completed_job_ratio = 0
#                 # If more than 90% jobs are finished, mark the site as complete
#                 if completed_job_ratio >= 0.9:
#                     # If more than 90% jobs out of completed jobs are erroneous,mark the site as erroneous
#                     if errorneous_job_ratio >= 0.9:
#                         update_processing_state(site_id,'ERRONEOUS')
#                         logging.info('Site %s marked as ERRONEOUS',site_id)
#                     else:
#                         update_processing_state(site_id,'COMPLETE')
#                         logging.info('Site %s marked as COMPLETE',site_id)
#                 pending_job_ids_in_site =[]
#                 if len(pending_jobs)==0:
#                     logging.info('No jobs are pending in site %s',site_id) 
#                 else:
#                     for job in pending_jobs:
#                         pending_job_ids_in_site.append(job['job_id'])    
#                     pending_job_ids_in_site = list(map(str, pending_job_ids_in_site))
#                     comma_delimited_joblist = ','.join(pending_job_ids_in_site)
#                     logging.info('Jobs pending in site %s: %s',site_id,comma_delimited_joblist)
#                     updated_command = command.format(comma_delimited_joblist)
#                     # Query the status of jobs in the selected site
#                     current_state = []
#                     with Popen(shlex.split(updated_command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
#                         for line in p.stdout:
#                             logging.debug('> %s ',line.rstrip()) 
#                             delimited_line = line.split()
#                             job_id = delimited_line[1]
#                             state = delimited_line[3]
#                             current_state.append((job_id,state))
#                     if p.returncode != 0:
#                         raise CalledProcessError(p.returncode, p.args)

#                     # Update job states
#                     current_time = datetime.datetime.now()
#                     for job_tuple in current_state:
#                         job_id = job_tuple[0]
#                         state = job_tuple[1]
#                         if job_tuple[1] in JOB_WAITING_STATES:
#                             wait_time_in_hrs = (current_time - dateparser.parse(job['timestamp'])).total_seconds()/3600
#                             if wait_time_in_hrs > JOB_WAITING_TIMEOUT:
#                                 abstract_state = 'STALLED'
#                                 update_job_states(job_id,abstract_state,state)
#                                 logging.debug('Job with job id %s marked as %s because it is waiting for %.2f hours',str(job_id),abstract_state,wait_time_in_hrs) 
#                         elif job_tuple[1] in JOB_ERROR_STATES:
#                             abstract_state = 'ERROR'
#                             update_job_states(job_id,abstract_state,state) 
#                             logging.debug('Job with job id %s marked as %s as it is in %s state',str(job_id),abstract_state,state)
#                         elif job_tuple[1] not in JOB_RUNNING_STATES:
#                             abstract_state = 'FINISHED'
#                             update_job_states(job_id,abstract_state,state)
#                             logging.debug('Job with job id %s marked as %s',str(job_id),abstract_state)
#         time.sleep(SLEEP_BETWEEN_MONITOR_PINGS)
    

# def job_parser():
#     """
#     Parse the results of jobs in completed Grid sites
#     """
#     command = 'alien.py cp -r -T 32 -glob "{}*" alien:{}/{}/ file:{}/{}'
#     while True:
#         all_site_ids = get_site_ids()
#         parsed_site_ids = get_processing_state_siteids_by_state('PARSED') + get_processing_state_siteids_by_state('ERRONEOUS')
#         if len(all_site_ids) == len(parsed_site_ids):
#             logging.info('All sites have been PARSED')
#             return
#         else:
#             completed_site_ids = get_processing_state_siteids_by_state('COMPLETE')
#             logging.debug('Parsing sites: %s', completed_site_ids)
#             for site_id in completed_site_ids:
#                 name = get_normalized_name_by_siteid(site_id)
#                 updated_command = command.format(name,GRID_USER_HOME,GRID_SITE_SONAR_OUTPUT_DIR,OUTPUT_FOLDER, name)
#                 with Popen(shlex.split(updated_command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
#                     for line in p.stdout:
#                         logging.debug('> %s ',line.rstrip()) 
#                 if p.returncode != 0:
#                     raise CalledProcessError(p.returncode, p.args)
#                 parse_output_directory('outputs/'+name)
#                 update_processing_state(site_id,'PARSED')
#                 logging.info('Site %s marked as PARSED',site_id)
#         time.sleep(SLEEP_BETWEEN_PARSER_PINGS)


def search_results(query,site_id):
    query = query.split(':')
    query_key = query[0].strip()
    query_value = query[1].strip()
    supported_sites = 0
    outputs = get_parsed_output_by_siteid(site_id)
    # result_dicts = []
    for node_id in outputs:
        output = json.loads(outputs[node_id])
        # result_dicts.append(output)
        for section in output['sections']:
            current_section = section['data']
            for key in current_section:
                if key == query_key:
                    if current_section[key] == query_value:
                        supported_sites += 1

    collected_nodes = len(outputs)
    total_nodes = get_num_nodes_in_site(site_id)
    coverage = collected_nodes / total_nodes
    supported = supported_sites // total_nodes
    logging.info('%d nodes out of total %d nodes matches the query', supported, total_nodes)
    return total_nodes,coverage,supported

# def create_job_template():
#     with open('../scripts/job.txt') as f:
#         lines = f.readlines()
#         commands = []
#         for line in lines:
#             if not (line.isspace() or line.startswith('#')):
#                 # Ignore the comments
#                 line = line.split('#',1)[0]
#                 # Identify the key value pair separated by ':'
#                 key_val_pair = line.split(':',1)
#                 num_terms = len(key_val_pair)
#                 key = key_val_pair[0].strip()
#                 if num_terms == 1:
#                     logging.warning('Key',key,'does not contain a command.Skipping...')
#                     continue
#                 elif num_terms == 2:
#                     value = key_val_pair[1].strip()
#                 elif num_terms > 2:
#                     logging.warning('Key',key,'in unrecognizable format.Skipping...')
#                 commands.append((key,value))
    # Generate Job.py
