# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

"""This is a helper script to extract the generated templates into a signac project for easier viewing."""
import os
import argparse

import signac
import flow
import generate_template_reference_data as gen

PROJECT_DIR = os.path.join(
    os.path.dirname(__file__), './template_reference_data')

def main(args):
    if not os.path.exists(PROJECT_DIR):
        os.makedirs(PROJECT_DIR)
    elif args.force:
        import shutil
        shutil.rmtree(PROJECT_DIR)
        os.makedirs(PROJECT_DIR)
    else:
        return

    p = signac.init_project(name=gen.PROJECT_NAME, root=PROJECT_DIR)
    p.import_from(origin=gen.ARCHIVE_DIR)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate reference submission scripts for various environments")
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help="Recreate the data space even if the ARCHIVE_DIR already exists"
    )
    main(parser.parse_args())
