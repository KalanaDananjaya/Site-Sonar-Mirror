#!/usr/bin/env python3

import argparse,shlex,os,shutil,logging,json
from subprocess import Popen,PIPE, CalledProcessError
from multiprocessing import Process

from db_connection import add_sites_from_csv, clear_tables, get_all_job_ids_by_state, update_job_state_by_job_id, start_new_run,change_run_state,increment_run_id, update_processing_state
from config import *
from processes import job_submission, clear_grid_output_dir


# CLI Functions
def init(args):
    clear_tables(all=True)
    add_sites_from_csv(SITES_CSV_FILE)
    increment_run_id()
    logging.info('Database initialized using %s file',SITES_CSV_FILE)

def reset(args):
    clear_grid_output_dir()
    start_new_run()
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
    jdl_name = JOB_TEMPLATE_NAME
    job_submission(jdl_name)


def fetch_results(args):
    absPath = os.getcwd() + '/' + OUTPUT_FOLDER
    if os.path.exists(OUTPUT_FOLDER):
        shutil.rmtree(absPath)
        os.mkdir(absPath)
    logging.info('Downloading the results to %s',absPath) 
    command = 'alien.py cp -r -T 32 alien:{}/{}/ file:{}'.format(GRID_USER_HOME,GRID_SITE_SONAR_OUTPUT_DIR,OUTPUT_FOLDER)
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            logging.info('> %s ',line) 
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)


def search(args):
    search_results(args.query,args.site_id)

def abort(args):
    # Kill all jobs
    if args.all:
        started_job_ids = get_all_job_ids_by_state('STARTED')
        stalled_job_ids = get_all_job_ids_by_state('STALLED')
        job_ids = started_job_ids + stalled_job_ids
        num_jobs = len(job_ids)
        if num_jobs != 0:
            logging.debug('Started killing %d jobs...',num_jobs)
            start = 0
            end = 500
            while True:
                logging.debug('Killing %d to %d jobs',start,end)
                if (end > num_jobs):
                    end = num_jobs
                job_ids_slice = job_ids[start:end]
                job_ids_slice_string = ' '.join(map(str,job_ids_slice))
                command = 'alien.py kill {}'.format(job_ids_slice_string)
                with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
                    for line in p.stdout:
                        logging.debug('> %s ',line) 
                if p.returncode != 0:
                    raise CalledProcessError(p.returncode, p.args)
                if end == num_jobs:
                    logging.info ('Total of %d jobs killed succesfully',num_jobs)
                    break
                else:
                    start += 500
                    end += 500
            update_job_state_by_job_id(job_ids,'KILLED') 
        else:
            logging.info("No jobs to kill")
        if args.clean:
            abort_proc_state = update_processing_state('STALLED',initialize=False)
            change_run_state('TIMED_OUT')
        else:
            abort_proc_state = update_processing_state('ABORTED',initialize=False)
            change_run_state('ABORTED')

            
    # Kill jobs with given ids
    elif args.job_id:
        job_ids = ' '.join(args.job_id.split(",") )
        command = 'alien.py kill {}'.format(job_ids)
        with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                logging.debug('> %s ',line) 
            logging.info ('Job(s) killed succesfully')
        if p.returncode != 0:
            raise CalledProcessError(p.returncode, p.args)
        update_job_state_by_job_id(job_ids,'KILLED')
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

fetch_results_parser = subparsers.add_parser('fetch')
fetch_results_parser.set_defaults(func=fetch_results)

init_parser = subparsers.add_parser('init')
init_parser.set_defaults(func=init)

search_parser = subparsers.add_parser('search')
search_parser.add_argument('-q','--query',help='Key value pair to search')
search_parser.add_argument('-sid','--site_id', help = 'ID of the Grid site')
search_parser.add_argument('-st','--section.title', help = 'Title of the section')
search_parser.set_defaults(func=search)

abort_parser = subparsers.add_parser('abort')
abort_parser.add_argument('-id','--job_id',help='Comma separated job IDs to kill')
abort_parser.add_argument('-a','--all',action='store_false',help ='Kill all the running jobs. Set to false to kill selected jobs')
abort_parser.add_argument('-c','--clean',action='store_true',help='Kill all remaining jobs and mark all remaining jobs')
abort_parser.set_defaults(func=abort)

reset_parser = subparsers.add_parser('reset')
reset_parser.set_defaults(func=reset)

if __name__ == '__main__':

    args = parser.parse_args()
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=get_log_lvl(args.log.lower()), 
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler() 
        ])
    logging.info('Site Sonar application started')
    args.func(args)
