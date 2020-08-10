#!/usr/bin/env python3

import argparse
import shlex
import shutil
from subprocess import Popen, PIPE, CalledProcessError

from config import *
from db_connection import *
from processes import job_submission, clear_grid_output_dir, stage_jobs_to_grid


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
    stage_jobs_to_grid('JDL')
    stage_jobs_to_grid('scripts')


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


def abort(args):
    # Kill all jobs
    if args.all:
        job_ids = get_all_job_ids_by_state('STARTED')
        num_jobs = len(job_ids)
        if num_jobs != 0:
            logging.debug('Started killing %d jobs...',num_jobs)
            start = 0
            end = 500
            while True:
                if (end > num_jobs):
                    end = num_jobs
                logging.debug('Killing %d to %d jobs',start,end)
                job_ids_slice = job_ids[start:end]
                job_ids_slice_string = ' '.join(map(str,job_ids_slice))
                command = 'alien.py kill {}'.format(job_ids_slice_string)
                with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
                    for line in p.stdout:
                        logging.debug('> %s ',line) 
                # Exit code 121 is returned if the job does not exists/cannot be killed - It is not caught as an exception
                if p.returncode != 0 and p.returncode != 121:
                    raise CalledProcessError(p.returncode, p.args)
                if p.returncode == 121:
                    logging.warning(p.returncode, p.args)
                if end == num_jobs:
                    logging.info ('Total of %d jobs killed succesfully',num_jobs)
                    break
                else:
                    start += 500
                    end += 500
                update_job_state_by_job_id(job_ids_slice,'KILLED') 
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
        job_ids = ' '.join(args.job_id.split(","))
        command = 'alien.py kill {}'.format(job_ids)
        with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                logging.debug('> %s ',line) 
            logging.info('Job(s) killed succesfully')
        if p.returncode != 0:
            raise CalledProcessError(p.returncode, p.args)
        update_job_state_by_job_id(job_ids,'KILLED')
    else:
        logging.info('No Job IDs were given to kill or failed to retrieve the Job IDs from the database')

def summary(args):
    import requests,json
    from prettytable import PrettyTable

    if args.run_id:
        url = BACKEND_URL + '/all_sites'
        res = requests.get(url)
        sites = json.loads(res.text)
        sitenames = {}
        for element in sites:
            sitenames.update({ str(element['site_id']):element['site_name'] })

        url = BACKEND_URL + '/jobs'
        res = requests.post(url, json = {'RunId': args.run_id})
        res = json.loads(res.text)
    
        total_submitted_jobs = 0
        total_completed_jobs = 0
        total_killed_jobs = 0
       
        data_dict = {}
        for site_id in sorted(map(int,res.keys())):
            site_id = str(site_id)
            if not(res[site_id].get('STARTED')):
                res[site_id].update({ 'STARTED':0 })
            if not(res[site_id].get('COMPLETED')):
                res[site_id].update({ 'COMPLETED':0 })
            if not(res[site_id].get('KILLED')):
                res[site_id].update({ 'KILLED':0 })

            submitted_jobs = res[site_id]['STARTED']
            completed_jobs = res[site_id]['COMPLETED']
            killed_jobs = res[site_id]['KILLED']

            data_dict.update({
                int(site_id): {
                    'sitename': sitenames[site_id],
                    'site_nodes': '-', 
                    'covered_nodes': '-',
                    'node_coverage': '-',
                    'pending_jobs': submitted_jobs,
                    'completed_jobs': completed_jobs,
                    'killed_jobs': killed_jobs,
                    'completed_job_percentage': str(round((completed_jobs/(completed_jobs+submitted_jobs+killed_jobs))*100))+' %'
                }
            })

            total_submitted_jobs += submitted_jobs
            total_completed_jobs += completed_jobs
            total_killed_jobs += killed_jobs
            
        url = BACKEND_URL +'/run_summary'
        res = requests.post(url, json = {'RunId': args.run_id})
        run_res = json.loads(res.text)
        attempted_sites = len(run_res)

        tot_nodes = 0
        covered_nodes = 0
       
        for element in run_res:
            data_dict[element['site_id']]['sitename'] = element['sitename']
            data_dict[element['site_id']]['site_nodes'] = element['total_nodes']
            data_dict[element['site_id']]['covered_nodes'] = element['covered_nodes']

            data_dict[element['site_id']]['node_coverage'] = str(round(element['coverage']))+' %'
            tot_nodes += int(element['total_nodes'])
            covered_nodes += int(element['covered_nodes'])       

        table = PrettyTable(['Id', 'Site Name', 'Pending Jobs', 'Completed Jobs', 'Killed Jobs', 'Job completion', 'Total Nodes', 'Covered Nodes', 'Coverage'])
        for site_id in data_dict.keys():
            table.add_row([site_id, data_dict[site_id]['sitename'], data_dict[site_id]['pending_jobs'] , data_dict[site_id]['completed_jobs'], data_dict[site_id]['killed_jobs'], data_dict[site_id]['completed_job_percentage'], data_dict[site_id]['site_nodes'],data_dict[site_id]['covered_nodes'],data_dict[site_id]['node_coverage']])
        print (table)

        print ("============== Node Coverage Summary ==============+")
        print('Total Sites Attempted:', attempted_sites)
        print('Total Nodes in Attempted Sites:', tot_nodes)
        print('Covered Nodes in Attempted Sites:', covered_nodes )
        print()

        print ("============== Job Completion Summary ==============")
        print('Total Started Jobs:', total_submitted_jobs)
        print('Total Completed Jobs:', total_completed_jobs)
        print('Total Killed Jobs:', total_killed_jobs)
        print('Job Completion Percentage:', round((total_completed_jobs/(total_submitted_jobs+total_completed_jobs+total_killed_jobs))*100),'%')

    else:
        url = BACKEND_URL +'/all_runs_cli'
        res = requests.get(url)
        res = json.loads(res.text)
        table = PrettyTable(['Run Id', 'Started At', 'Finished At', 'State'])
        for element in res:
            table.add_row([element['run_id'], element['started_at'], element['finished_at'], element['state']])
        print(table)

def search(args):
    import requests,json
    from prettytable import PrettyTable

    key_val = {}
    for element in args.query :
        key_val.update({element[0]:{
            'query_key': element[1].split(':')[0].strip(),
            'query_value': element[1].split(':')[1].strip()
        }})
    if args.site_id:
        data = {
            'SearchFields': key_val,
            'Equation': args.equation,
            'SiteId': args.site_id,
            'RunId': args.run_id
        }
    else:
        data = {
            'SearchFields': key_val,
            'Equation': args.equation,
            'SiteId': 'all',
            'RunId': args.run_id
        }
    url = BACKEND_URL +'/search_site'
    res = requests.post(url, json={'SearchFormInput': data})
    res = json.loads(res.text)

    if res['grid_search']:
        total_sites = res['total_sites']
        covered_sites = res['covered_sites']
        matching_sites = res['matching_sites']
        matching_sites_list = res['matching_sites_list']
        unmatching_sites_list = res['unmatching_sites_list']
        incomplete_sites_list = res['incomplete_sites_list']

        result_table = PrettyTable(['Total Sites', 'Covered Sites', 'Matching Sites', 'Matching Percentage'])
        result_table.add_row([total_sites, covered_sites, matching_sites, (matching_sites/covered_sites)*100])
        print(result_table)

        site_table = PrettyTable(['Site Name', 'State'])
        for site in matching_sites_list:
            site_table.add_row([site, 'Matching'])
        for site in unmatching_sites_list:
            site_table.add_row([site, 'Not Matching'])
        for site in incomplete_sites_list:
            site_table.add_row([site, 'N/A'])
        print(site_table)
    
    else:
        total_nodes = res['total_nodes']
        covered_nodes = res['covered_nodes']
        matching_nodes = res['matching_nodes']
        matching_nodes_data = res['matching_nodes_data']
        unmatching_nodes_data = res['unmatching_nodes_data']

        result_table = PrettyTable(['Total Nodes', 'Covered Nodes', 'Matching Nodes', 'Matching Percentage'])
        result_table.add_row([total_nodes, covered_nodes, matching_nodes, (matching_nodes/covered_nodes)*100])
        print(result_table)

        from textwrap import fill
        node_table = PrettyTable(['Node Name', 'State', 'Data'],align='l')
        for key in matching_nodes_data:
            node_table.add_row([key, 'Matching', fill(str(matching_nodes_data[key]), width=70)])
        for node in unmatching_nodes_data:
            node_table.add_row([key, 'Not Matching', fill(str(unmatching_nodes_data[key]), width=70)])
        print (node_table)

def testdb(args):
    cursor,conn = get_connection(auto_commit=False)
    if conn:
        logging.info("Database connection successfully establised. Please proceed.")
    else:
        logging.error("Cannot establish database connection")


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
parser.add_argument('-v', '--version', action='version', version='0.1')
parser.add_argument('-l', '--log', default='DEBUG')
subparsers = parser.add_subparsers()

stage_jobs_parser = subparsers.add_parser('stage')
stage_jobs_parser.set_defaults(func=stage_jobs)

submit_jobs_parser = subparsers.add_parser('submit')
submit_jobs_parser.set_defaults(func=submit_jobs)

fetch_results_parser = subparsers.add_parser('fetch')
fetch_results_parser.set_defaults(func=fetch_results)

init_parser = subparsers.add_parser('init')
init_parser.set_defaults(func=init)

abort_parser = subparsers.add_parser('abort')
abort_parser.add_argument('-id', '--job_id', help='Comma separated job IDs to kill')
abort_parser.add_argument('-a', '--all',action='store_false', help='Kill all the running jobs. Set to false to kill selected jobs')
abort_parser.add_argument('-c', '--clean',action='store_true', help='Kill all remaining jobs and mark all remaining jobs')
abort_parser.set_defaults(func=abort)

reset_parser = subparsers.add_parser('reset')
reset_parser.set_defaults(func=reset)

testdb_parser = subparsers.add_parser('testdb')
testdb_parser.set_defaults(func=testdb)

summary_parser = subparsers.add_parser('summary')
summary_parser.add_argument('-r', '--run_id', default=None, help='Run ID')
summary_parser.set_defaults(func=summary)

search_parser = subparsers.add_parser('search')
search_parser.add_argument('-s', '--site_id', default=None, help='Site ID')
search_parser.add_argument('-q', '--query', action='append', required=True, type=lambda kv: kv.split("="), help='Search Query Dictionary')
search_parser.add_argument('-eq', '--equation', required=True, help='Search Query Dictionary')
search_parser.add_argument('-r', '--run_id', required=True, help='Run ID')
search_parser.set_defaults(func=search)

if __name__ == '__main__':

    args = parser.parse_args()
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=get_log_lvl(args.log.lower()),
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler() 
        ])
    logging.info('Site Sonar application started')
    args.func(args)
