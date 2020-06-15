import argparse,shlex,os,shutil,logging,time
from subprocess import Popen,PIPE, CalledProcessError
from multiprocessing import Process, set_start_method

from db_connection import add_sites_from_csv,initialize_db, clear_db
from output_parser import parse_output_directory,clear_output_dir
from config import DATABASE_FILE, SITES_CSV_FILE, LOG_FILE, SLEEP_BETWEEN_SUBMIT_AND_MONITOR, GRID_USER_HOME, JOB_TEMPLATE_NAME, SLEEP_BETWEEN_MONITOR_AND_PARSER, OUTPUT_FOLDER
from background_processes import job_submission, job_monitor, job_parser


# CLI Functions
def init(args):
    # Clear grid output directory as well
    clear_output_dir(OUTPUT_FOLDER)
    clear_db(DATABASE_FILE)
    initialize_db(DATABASE_FILE)
    add_sites_from_csv(SITES_CSV_FILE)
    logging.info('Database initialized using %s file',SITES_CSV_FILE)


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
    command = 'alien_cp -r -T 32 alien:{}/site-sonar/outputs/ file:outputs'.format(GRID_USER_HOME)
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
    grid_home = GRID_USER_HOME 
    jdl_name = JOB_TEMPLATE_NAME
    job_submission(grid_home,jdl_name)
    
    monitor = Process(target=job_monitor)
    parser = Process(target=job_parser)
    monitor.start()
    time.sleep(SLEEP_BETWEEN_MONITOR_AND_PARSER)
    parser.start()
    monitor.join()
    parser.join()

parser = argparse.ArgumentParser()
parser.add_argument('--version', action='version', version='0.1')
parser.add_argument('--log', default='INFO')
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
background_parser.set_defaults(func=submit_and_monitor)

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',handlers=[
        logging.FileHandler(LOG_FILE,'w'),
        logging.StreamHandler() 
        ])
    logging.info('Site Sonar application started')
    args = parser.parse_args()
    args.func(args)
