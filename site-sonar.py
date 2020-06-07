import argparse,shlex,os,shutil,re,logging
from subprocess import Popen,PIPE, CalledProcessError

from db_connection import get_sites, add_job_batch, add_sites_from_csv,initialize_db
from output_parser import parse_output_directory

from config import DATABASE_FILE, SITES_CSV_FILE


# Utils
def escape_string(string):
    return re.sub(r'\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))', '', string)

def get_grid_output_dir(base, normalized_name, _id):
    suffix = base + '/outputs/' + normalized_name + "_" + str(_id)
    return os.path.join(base, suffix)

# CLI Functions
def init(args):
    if os.path.exists(DATABASE_FILE): 
        os.remove(DATABASE_FILE) 
    initialize_db(DATABASE_FILE)
    add_sites_from_csv(SITES_CSV_FILE)
    logging.info('Databased initialized using %s file',SITES_CSV_FILE)


def stage_jobs(args):
    command = 'bash staging-grid.sh {}'.format(args.grid_home)
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        logging.info('Staging jobs...')
        for line in p.stdout:
            logging.info('> %s ',line)
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)


def submit_jobs(args):
    site_details = get_sites()

    grid_home = args.grid_home
    jdl_name = args.template or 'job_template.jdl' 
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
                    logging.info('> %s ',line) 
                    if ("Your new job ID is" in line):
                        job_id = line.split(' ')[-1]
                        job_id = escape_string(job_id)
                        jobs.append(int(job_id))
            if p.returncode != 0:
                raise CalledProcessError(p.returncode, p.args)
        add_job_batch(jobs, site['site_id'])
        

def watch_jobs(args):
    command = 'watch "alien.py ps | tail -n 100"' 
    print(shlex.split(command))
    with Popen(shlex.split(command), stdout=PIPE, stderr=PIPE, bufsize=1, universal_newlines=True) as p:
        print('test')
        output, error = p.communicate()
        print(p)
        print(output,error)
        for line in p.stdout:
            print('> ', line, end='') 
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)

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
            print('> ', line, end='') 
            logging.info('> %s ',line) 
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)

def parse_output(args):
    """
    Return True if all the files in the directory was parsed successfully
    """
    logging.info('Output parsing started...') 
    parsed = parse_output_directory()
    

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

watch_jobs_parser = subparsers.add_parser('watch')
watch_jobs_parser.set_defaults(func=watch_jobs)

fetch_results_parser = subparsers.add_parser('fetch')
fetch_results_parser.add_argument('-d','--directory', help="File path of the output directory")
fetch_results_parser.set_defaults(func=fetch_results)

init_parser = subparsers.add_parser('init')
init_parser.set_defaults(func=init)

parse_outputs_parser = subparsers.add_parser('parse')
parse_outputs_parser.set_defaults(func=parse_output)


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',handlers=[
        logging.FileHandler("site-sonar.log",'w'),
        logging.StreamHandler() 
        ])
    logging.info ('Site Sonar application started')
    args = parser.parse_args()
    args.func(args)
