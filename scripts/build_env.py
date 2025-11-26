"""
Create a conda environment (or just the command to do so) from the packages imported in all python scripts for the project

Usage:
    build_env.py <proj_dir> <env_name> [--python_version=<str>] [--create]

Examples:
    python build_env.py path/to/some/dir/with/python/scripts new_env
    python build_env.py path/to/some/dir/with/python/scripts new_env --python_version=3.8 --create

Required parameters:
    proj_dir    Path to highest level directory containing python scripts. This directory will be recursively
                searched

    env_name    Name of the conda environment to create

Options:
    -h, --help                  Show this screen.
    -v, --python_version=<str>  Python version to create the new environment with. The default is '3' which will
                                create the most recent 3.x version of Python available [default: 3]
    -c, --create                Run the command that is generated to create the environment
"""

import sys
import os
import re
import subprocess
from glob import glob
from contextlib import contextmanager
from importlib import import_module

import utils

@contextmanager
def ignore_site_packages_paths():
    paths = sys.path
    # remove all third-party paths
    # so that only stdlib imports will succeed
    sys.path = list(filter(
        None,
        filter(lambda i: 'site-packages' not in i, sys.path)
    ))
    yield
    sys.path = paths


def is_std_lib(module):
    if module in sys.builtin_module_names:
        return True
    with ignore_site_packages_paths():
        imported_module = sys.modules.pop(module, None)
        try:
            import_module(module)
        except ImportError:
            return False
        else:
            return True
        finally:
            if imported_module:
                sys.modules[module] = imported_module

def get_package_from_import(import_line):


    package_lists = [[p.split('.')[0] for p in package_list] for package_list in import_line.split(',')]




def main(proj_dir, env_name, python_version='3', create=False):
    packages = []
    scripts = glob(os.path.join(proj_dir, '**', '*.py'), recursive=True)
    script_names = [os.path.basename(p).rstrip('.py') for p in scripts]
    for s in scripts:
        with open(s) as f:
            for line in f.readlines():
                if line.startswith('import ') or line.startswith('from '):
                    package_list = re.sub('import |from ', '', line).split(',')
                    packages.extend([p.split('.')[0].split()[0] for p in package_list if p not in script_names])
            #script_packages = [line.split()[1] for line in f.readlines() if line.startswith('import ') or line.startswith('from ')]
            #packages.extend([p for p in script_packages if p not in script_names])
    import pdb; pdb.set_trace()
    install_packages = [p for p in sorted(set(packages)) if not is_std_lib(p)]

    conda_cmd = f'''conda create -n {env_name} python={python_version} {' '.join(install_packages)}'''

    if create:
        try:
            print('Running conda command: ' + conda_cmd)
            result = subprocess.run(conda_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except:
            raise RuntimeError(f'Unable to create env {env_name}.\nconda create command output:\n{result}')
    else:
        print(conda_cmd)


if __name__ == '__main__':
    args = utils.get_cl_args(__doc__)
    sys.exit(main(**args))