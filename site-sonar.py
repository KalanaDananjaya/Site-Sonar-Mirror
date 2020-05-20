import argparse
from subprocess import Popen,PIPE, CalledProcessError

def stage(args):
    command = 'bash staging-grid.sh %s' % args.grid_home
    print(command.split())
    with Popen(command.split(), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            print('> ', line, end='') # process line here

    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)


parser = argparse.ArgumentParser()
parser.add_argument('--version', action='version', version='0.1')
parser.add_argument('--grid-home', default='/alice/cern.ch/user/k/kwijethu', help="Grid Home Path")
subparsers = parser.add_subparsers()

stage_parser = subparsers.add_parser('stage')
stage_parser.set_defaults(func=stage)

if __name__ == '__main__':
    args = parser.parse_args()
    args.func(args)
# hello_parser.add_argument('name', help='name of the person to greet')
# hello_parser.add_argument('--greeting', default='Hello', help='word to use for the greeting')
# hello_parser.add_argument('--caps', action='store_true', help='uppercase the output')