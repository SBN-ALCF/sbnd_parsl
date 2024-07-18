#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import json
import pathlib
from typing import Dict, List

import parsl
from parsl.app.app import bash_app
from parsl.data_provider.files import File

from sbnd_parsl.workflow import StageType, Stage, Workflow
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE


UNIQUE_RUN_NUMBER = 0


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


def runfunc(fcl, input_files, output_dir):
    output_filename = os.path.basename(fcl).replace(".fcl", ".root")
    output_file = output_dir / Path(output_filename)


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    ''' create directory structure like XX00/XXXX '''
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings: json):
    output_dir = pathlib.Path(settings['run']['output'])
    output_dir.mkdir(parents=True, exist_ok=True)

    fcl_dir = pathlib.Path(settings['run']['fclpath'])
    nsubruns = settings['run']['nsubruns']

    user_opts = create_default_useropts()
    user_opts.update(settings['queue'])

    user_opts["run_dir"] = str(output_dir / 'runinfo')
    print(user_opts)

    config = create_parsl_config(user_opts)
    print(config)

    larsoft_opts = settings['larsoft']
    fcls = settings['fcls']
    workflow_opts = settings['workflow']
    mc_meta = MetadataGenerator(settings['metadata'], fcls, defer_check=True)

    futures = []
    # parsl.clear()
    # parsl.load(config)
    
    stage_order = (
        StageType.GEN, StageType.G4, StageType.DETSIM,
        StageType.RECO1, StageType.RECO2
    )
    wf = Workflow(stage_order, fcls)

    for i in range(nsubruns):
        s = Stage(StageType.RECO2)
        s.run_dir = get_subrun_dir(output_dir, i)
        wf.add_final_stage(s)
    wf.run()

    '''
    print(f'Submitted {len(futures)} futures.')
        
    for f in futures:
        try:
            print(f.result())
        except Exception as e:
            print(f'FAILED {f.filepath}')
    '''


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
