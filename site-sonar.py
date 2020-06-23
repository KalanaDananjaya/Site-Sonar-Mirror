#!/usr/bin/env python3

import argparse,shlex,os,shutil,logging,json
from subprocess import Popen,PIPE, CalledProcessError
from multiprocessing import Process

from db_connection import add_sites_from_csv,initialize_db, clear_db, clear_tables, get_parsed_output_by_siteid, get_num_nodes_in_site
from output_parser import parse_output_directory,clear_output_dir
from config import *
from background_processes import job_submission, job_monitor, job_parser, clear_grid_output_dir


# CLI Functions
def init(args):
    clear_grid_output_dir()
    clear_output_dir(OUTPUT_FOLDER) 
    clear_db()
    initialize_db()
    add_sites_from_csv(SITES_CSV_FILE)
    logging.info('Database initialized using %s file',SITES_CSV_FILE)

def reset(args):
    clear_grid_output_dir()
    clear_output_dir(OUTPUT_FOLDER)
    # A workaround should be used to retain the parsed outputs while a run is going
    clear_tables()
    logging.info('Fresh environment started for a new run')


def stage_jobs(args):
    command = 'bash staging-grid.sh {}'.format(GRID_USER_HOME)
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        logging.info('Staging jobs...')
        for line in p.stdout:
            logging.info('> %s ',line)
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)


def submit_jobs(args):
    grid_home = GRID_USER_HOME 
    jdl_name = JOB_TEMPLATE_NAME
    job_submission(grid_home,jdl_name) 

def monitor_jobs(args):
    job_monitor()

def fetch_results(args):
    dirName = 'outputs'
    absPath = os.getcwd() + '/' + dirName
    if os.path.exists(dirName):
        shutil.rmtree(absPath)
        os.mkdir(absPath)
    logging.info('Downloading the results to  %s ',absPath) 
    command = 'alien.py cp -r -T 32 alien:{}/{}/ file:{}'.format(GRID_USER_HOME,GRID_SITE_SONAR_OUTPUT_DIR,OUTPUT_FOLDER)
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            logging.info('> %s ',line) 
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)

def parse_output(args):
    """
    Return True if all the files in the directory was parsed successfully
    """
    logging.info('Output parsing started...') 
    parsed = parse_output_directory('outputs')
    
def submit_and_monitor(args):
    if args.submit:
        job_submission(GRID_USER_HOME,JOB_TEMPLATE_NAME)
    parser = Process(target=job_parser)
    parser.start()
    job_monitor()
    parser.join()
    logging.INFO('Grid site data collection process complete.')

def search(args):
    query = args.query.split(':')
    query_key = query[0].strip()
    query_value = query[1].strip()
    site_id = args.site_id
    supported_sites = 0
    outputs = get_parsed_output_by_siteid(site_id)
    for node_id in outputs:
        output = json.loads(outputs[node_id])
        for section in output['sections']:
            print (section)
            current_section = section['data']
            for key in current_section:
                if key == query_key:
                    if current_section[key] == query_value:
                        supported_sites += 1

    collected_nodes = len(outputs)
    total_nodes = get_num_nodes_in_site(site_id)
    coverage = collected_nodes / total_nodes
    supported = supported_sites // total_nodes
    logging.info('%d sites out of total %d sites matches the query',supported,total_nodes)

def abort(args):
    job_ids = None
    if args.all:
        job_ids = get_all_job_ids_by_abs_state('STARTED')
        job_ids = ','.join(job_ids)
    if args.job_id:
        job_ids = args.job_id
    
    if job_ids:
        command = 'alien.py kill {}'.format(job_ids)
        with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                logging.debug('> %s ',line) 
            logging.info ('Jobs killed succesfully')
        if p.returncode != 0:
            raise CalledProcessError(p.returncode, p.args)
    else:
        logging.info('No Job IDs were given to kill or failed to retrieve the Job IDs from the database')

def get_log_lvl(lvl):
    levels = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
    }
    level = levels[lvl]
    return level
    

parser = argparse.ArgumentParser()
parser.add_argument('-v','--version', action='version', version='0.1')
parser.add_argument('-l','--log', default='DEBUG')
subparsers = parser.add_subparsers()

stage_jobs_parser = subparsers.add_parser('stage')
stage_jobs_parser.set_defaults(func=stage_jobs)

submit_jobs_parser = subparsers.add_parser('submit')
submit_jobs_parser.set_defaults(func=submit_jobs)

monitor_jobs_parser = subparsers.add_parser('monitor')
monitor_jobs_parser.set_defaults(func=monitor_jobs)

fetch_results_parser = subparsers.add_parser('fetch')
fetch_results_parser.set_defaults(func=fetch_results)

init_parser = subparsers.add_parser('init')
init_parser.set_defaults(func=init)

parse_outputs_parser = subparsers.add_parser('parse')
parse_outputs_parser.set_defaults(func=parse_output)

background_parser = subparsers.add_parser('bg')
background_parser.add_argument('-s','--submit',action='store_true', help='Enable job submission')
background_parser.set_defaults(func=submit_and_monitor)

search_parser = subparsers.add_parser('search')
search_parser.add_argument('-q','--query',help='Key value pair to search')
search_parser.add_argument('-sid','--site_id', help = 'ID of the Grid site')
search_parser.add_argument('-st','--section.title', help = 'Title of the section')
search_parser.set_defaults(func=search)

abort_parser = subparsers.add_parser('kill')
abort_parser.add_argument('-id','--job_id',help='Comma separated job IDs to kill')
abort_parser.add_argument('-a','--all',action='store_true' help = 'Kill all the running jobs')
abort_parser.set_defaults(func=abort)

reset_parser = subparsers.add_parser('reset')
reset_parser.set_defaults(func=reset)

if __name__ == '__main__':

    args = parser.parse_args()
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=get_log_lvl(args.log.lower()), 
    handlers=[
        logging.FileHandler(LOG_FILE,'w'),
        logging.StreamHandler() 
        ])
    logging.info('Site Sonar application started')
    args.func(args)
