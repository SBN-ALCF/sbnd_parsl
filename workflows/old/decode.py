#!/usr/bin/env python

# This workflow runs the decoders (TPC, PMT, CRT) on raw data files

import sys, os, re
import json
import pathlib
from typing import Dict, List

import parsl
from parsl.app.app import bash_app
from parsl.data_provider.files import File

from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE


@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """ Return formatted bash script which produces each future when executed """
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1],
        **larsoft_opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def process_single_file(workdir: pathlib.Path, input_filename: pathlib.Path, larsoft_opts: Dict, fcls: List[Dict]):
    last_future = None
    input_file = File(str(input_filename))

    # get the run info from the filename
    # Ex.: data_evb01_run12104_99_20240322T161413.root
    regexp = re.compile('data_evb(\d+)_run(\d+)_(\d+)_.*')
    evb, runnum, subrun = (int(s) for s in regexp.findall(input_filename.name)[0])

    for i, fcl_info in enumerate(fcls):
        this_workdir = workdir / f'{runnum:06d}' / f'{subrun:03d}'
        output_file = this_workdir / f"{fcl_info['stage']}_{input_filename.name}"
        this_future = fcl_future(
            workdir = str(this_workdir),
            stdout = str(this_workdir / pathlib.Path(f"larStage{i}.out")),
            stderr = str(this_workdir / pathlib.Path(f"larStage{i}.err")),
            template = SINGLE_FCL_TEMPLATE,
            larsoft_opts = larsoft_opts,
            inputs=[fcl_info['fcl'], input_file],
            outputs=[File(str(output_file))],
        )
        input_file = this_future.outputs[0]
        last_future = this_future.outputs[0]

    # input file is set to the last future of this job
    return last_future


def main(settings: json):
    output_dir = pathlib.Path(settings['run']['output'])
    output_dir.mkdir(parents=True, exist_ok=True)

    fcl_dir = pathlib.Path(settings['run']['fclpath'])

    user_opts = create_default_useropts()
    user_opts.update(settings['queue'])
    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"

    config = create_parsl_config(user_opts)
    print(user_opts)
    print(config)
    parsl.clear()
    parsl.load(config)

    larsoft_opts = settings['larsoft']
    fcls = settings['fcls']
    workflow_opts = settings['workflow']

    futures = []
    
    input_dir = pathlib.Path(settings['workflow']['data_dir'])
    filenames = sorted(list(input_dir.glob("**/data_evb*run*.root")))

    print(filenames)
    # create futures for MC files
    for i, fname in enumerate(filenames):
        futures.append(
            process_single_file(
                workdir=output_dir, 
                input_filename=fname,
                larsoft_opts=larsoft_opts,
                fcls=[{'stage': stage, 'fcl': str(fcl_dir / pathlib.Path(fcl))} \
                      for stage, fcl in fcls.items()],
            )
        )

    print(f'Submitted {len(futures)} future.')
    for f in futures:
        try:
            print(f.result())
        except Exception as e:
            print(f'FAILED {f.filepath}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
