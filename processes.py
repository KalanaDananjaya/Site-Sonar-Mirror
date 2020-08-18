import time
import shlex
import json
import re
from dateutil import parser as dateparser
from subprocess import Popen,PIPE, CalledProcessError

from config import *
from db_connection import *

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
    success_flag = update_processing_state('WAITING', initialize=True)
    if success_flag:
        from scripts.stat import monitors 
        job_keys = json.dumps(list(monitors.keys()))
        add_job_keys(job_keys)
        job_path = GRID_USER_HOME + '/' + GRID_SITE_SONAR_HOME + '/JDL/' + jdl_name

        for site in site_details:
            num_nodes = site['num_nodes']
            if num_nodes == -1:
                num_nodes = 40
            num_jobs = JOB_FACTOR * num_nodes +1
            sitename = site['site_name']
            logging.info('Submitting %s jobs to the Grid site %s', str(num_jobs - 1), sitename)
            for i in range(1, num_jobs):
                output_dir = get_grid_output_dir(GRID_SITE_SONAR_OUTPUT_DIR, site['normalized_name'], i)
                site_sonar_dir = GRID_USER_HOME + '/' + GRID_SITE_SONAR_HOME
                logging.debug('Job path: %s', job_path)
                logging.debug('Site sonar dir($1): %s', site_sonar_dir)
                logging.debug('Site name($2): %s', sitename)
                logging.debug('Output dir($3):  %s', output_dir)
                command='alien.py submit {} {} {} {}'.format(job_path, site_sonar_dir, sitename, output_dir)
                logging.debug(command)
                with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
                    logging.info(shlex.split(command))
                    for line in p.stdout:
                        logging.debug('> %s ',line.rstrip()) 
                        if ("Your new job ID is" in line):
                            job_id = line.split(' ')[-1]
                            job_id = escape_string(job_id)
                # Exit code 121 is given if a job is rejected from the task queue. It is ignored to move forward submitting other jobs
                if p.returncode != 0 and p.returncode != 121:
                    raise CalledProcessError(p.returncode, p.args)
                if p.returncode == 121:
                    logging.warning(p.returncode, p.args)
                add_job(job_id, site['site_id'])
            update_site_last_update_time(site['site_id'])
    else:
        logging.error("Aborting the submission process...")


def stage_jobs_to_grid(dirname):
    GRID_DIR = "{}/{}/{}".format(GRID_USER_HOME, GRID_SITE_SONAR_HOME, dirname)
    command = "alien.py rm -rf {}/*".format(GRID_DIR)
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        logging.info('Clearing %s grid directory...', GRID_DIR)
        for line in p.stdout:
            logging.info('> %s ', line.rstrip())
    if p.returncode != 0 and p.returncode !=2:
        raise CalledProcessError(p.returncode, p.args)

    for file in os.listdir(dirname):
        command = 'alien.py cp -T 32 file:{}/{} alien:{}/{}@disk:1'.format(dirname, file, GRID_DIR, file)
        with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
            logging.info('Staging jobs ./%s/%s to %s/%s', dirname, file, GRID_DIR, file)
            for line in p.stdout:
                logging.info('> %s ', line.rstrip())
        if p.returncode != 0:
            raise CalledProcessError(p.returncode, p.args)
