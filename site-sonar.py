import argparse,shlex,os,shutil,logging,time
from subprocess import Popen,PIPE, CalledProcessError
from multiprocessing import Process

from db_connection import add_sites_from_csv,initialize_db
from output_parser import parse_output_directory
from config import DATABASE_FILE, SITES_CSV_FILE, LOG_FILE
from background_processes import job_submission,job_monitor

# CLI Functions
def init(args):
    if os.path.exists(DATABASE_FILE): 
        os.remove(DATABASE_FILE) 
    initialize_db(DATABASE_FILE)
    add_sites_from_csv(SITES_CSV_FILE)
    logging.info('Database initialized using %s file',SITES_CSV_FILE)


def stage_jobs(args):
    command = 'bash staging-grid.sh {}'.format(args.grid_home)
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        logging.info('Staging jobs...')
        for line in p.stdout:
            logging.info('> %s ',line)
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)


def submit_jobs(args):
    grid_home = args.grid_home or '/alice/cern.ch/user/k/kwijethu' 
    jdl_name = args.template or 'job_template.jdl'
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
    command = 'alien_cp -r -T 32 alien:{}/site-sonar/outputs/ file:outputs'.format(args.grid_home)
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
    parsed = parse_output_directory()
    
def submit_and_monitor(args):
    grid_home = '/alice/cern.ch/user/k/kwijethu' 
    jdl_name = 'job_template.jdl'
    submit = Process(target=job_submission, args=(grid_home,jdl_name))
    monitor = Process(target=job_monitor())
    submit.start()
    time.sleep(30)
    monitor.start()
    submit.join()
    monitor.join()

parser = argparse.ArgumentParser()
parser.add_argument('--version', action='version', version='0.1')
parser.add_argument('--grid-home', default='/alice/cern.ch/user/k/kwijethu', help="Grid Home Path")
parser.add_argument('--log', default='INFO')
subparsers = parser.add_subparsers()

stage_jobs_parser = subparsers.add_parser('stage')
stage_jobs_parser.set_defaults(func=stage_jobs)

submit_jobs_parser = subparsers.add_parser('submit')
submit_jobs_parser.add_argument('-l','--list', help="Grid Site List")
submit_jobs_parser.add_argument('-n','--jobs_per_site', default=1, help="Number of jobs submitted to each site")
submit_jobs_parser.add_argument('-s','--site',help="Target Site")
submit_jobs_parser.add_argument('-t', '--template', help="Custom Job Template JDL(Must be in $GRID_HOME/site-sonar/JDL/ directory")
submit_jobs_parser.set_defaults(func=submit_jobs)

monitor_jobs_parser = subparsers.add_parser('monitor')
monitor_jobs_parser.set_defaults(func=monitor_jobs)

fetch_results_parser = subparsers.add_parser('fetch')
fetch_results_parser.add_argument('-d','--directory', help="File path of the output directory")
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
