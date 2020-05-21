import argparse
from subprocess import Popen,PIPE, CalledProcessError

from generate_jdl import generate_template

def stage(args):
    command = 'bash staging-grid.sh %s' % args.grid_home
    print(command.split())
    with Popen(command.split(), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            print('> ', line, end='') # process line here
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)

def generate(args):
    with open(args.list) as f:
        sites = [line.split() for line in f]
    for site in sites:
        generate_template(template=args.template, write=args.write, id=args.jobs_per_site, target_ce=site[0], grid_home=args.grid_home)


parser = argparse.ArgumentParser()
parser.add_argument('--version', action='version', version='0.1')
parser.add_argument('--grid-home', default='/alice/cern.ch/user/k/kwijethu', help="Grid Home Path")
subparsers = parser.add_subparsers()

stage_parser = subparsers.add_parser('stage')
stage_parser.set_defaults(func=stage)

generate_template_parser = subparsers.add_parser('generate')
generate_template_parser.add_argument('-l','--list', help="Grid Site List")
generate_template_parser.add_argument('-t', '--template', help="Job Template JDL")
generate_template_parser.add_argument('-w','--write',action='store_true')
generate_template_parser.add_argument('-n','--jobs_per_site', default=1, help="Number of jobs submitted to each site")
generate_template_parser.add_argument('-ce','--target_ce',help="Target Site")
generate_template_parser.set_defaults(func=generate)

if __name__ == '__main__':
    args = parser.parse_args()
    args.func(args)
# hello_parser.add_argument('name', help='name of the person to greet')
# hello_parser.add_argument('--greeting', default='Hello', help='word to use for the greeting')
# hello_parser.add_argument('--caps', action='store_true', help='uppercase the output')