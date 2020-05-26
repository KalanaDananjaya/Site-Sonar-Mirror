import argparse,shlex,os,shutil
from subprocess import Popen,PIPE, CalledProcessError

from generate_jdl import get_grid_output_dir

def stage_jobs(args):
    command = 'bash staging-grid.sh {}'.format(args.grid_home)
    print(shlex.split(command))
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            print('> ', line, end='') 
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)


def submit_jobs(args):
    grid_home = args.grid_home
    jobs_per_site = args.jobs_per_site
    jdl_name = args.template or 'job_template.jdl' 
    base_dir='{}/site-sonar'.format(grid_home)
    job_path = base_dir + '/JDL/' + jdl_name

    if args.list:
        with open(args.list) as f:
            sites = [line.strip() for line in f]
    else:
        sites = args.site
    print(sites)
    for site in sites:
        print(site)
        for i in range(1, int(jobs_per_site)+1):
            output_dir = get_grid_output_dir(base_dir, site, i)
            print("Job path: ",job_path)
            print("Base dir: ",base_dir)
            print("Site name: ",site)
            print("Output dir: ",output_dir)
            command='alien.py submit {} {} {} {}'.format(job_path, base_dir, site, output_dir)
            print (command)
            with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
                for line in p.stdout:
                    print('> ', line, end='') 
            if p.returncode != 0:
                raise CalledProcessError(p.returncode, p.args)


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
    print ('Downloading the results to ' + absPath)
    command = 'alien_cp -r -T 32 alien:{}/site-sonar/outputs/ file:outputs'.format(args.grid_home)
    print(shlex.split(command))
    with Popen(shlex.split(command), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            print('> ', line, end='') 
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)


parser = argparse.ArgumentParser()
parser.add_argument('--version', action='version', version='0.1')
parser.add_argument('--grid-home', default='/alice/cern.ch/user/k/kwijethu', help="Grid Home Path")
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


if __name__ == '__main__':
    args = parser.parse_args()
    args.func(args)
