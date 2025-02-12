import argparse
from .project import RelionProject


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('path', metavar="PROJECT_PATH",
                   help="Project path")
    g = p.add_mutually_exclusive_group()
    g.add_argument('--clean', '-c', action='store_true',
                   help="Clean project files")
    g.add_argument('--update', '-u', action='store_true',
                   help="Update job status and pipeline star file.")
    g.add_argument('--run', '-r', nargs=2, metavar=('JOB_TYPE', 'COMMAND'))

    args = p.parse_args()

    rlnProject = RelionProject(args.path)

    if args.clean:
        rlnProject.clean()
    elif args.update:
        rlnProject.update()
    elif args.run:
        folder, cmd = args.run
        rlnProject.run(folder, cmd)

