#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import json
import pathlib
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE, \
    SINGLE_FCL_TEMPLATE_SPACK
from sbnd_parsl.utils import hash_name

@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1],
        **larsoft_opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def runfunc(self, fcl, input_files, run_dir, var_name, executor):
    """Method bound to each Stage object and run during workflow execution."""

    fcl_fullpath = executor.fcl_dir / fcl
    inputs = [str(fcl_fullpath), None]
    if input_files is not None:
        inputs = [str(fcl_fullpath)] + input_files

    run_dir.mkdir(parents=True, exist_ok=True)
    output_dir = executor.output_dir / var_name / self.stage_type.value
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = ''.join([
        str(self.stage_type.value), '-', var_name, '-',
        hash_name(os.path.basename(fcl) + executor.name_salt + str(executor.lar_run_counter)),
        ".root"
    ])
    executor.lar_run_counter += 1

    output_filepath = output_dir / output_filename
    mg_cmd = executor.meta[var_name].run_cmd(
        output_filename + '.json', os.path.basename(fcl), check_exists=False)

    if self.stage_type == StageType.GEN:
        executor.unique_run_number += 1
        run_number = 1 + (executor.unique_run_number // 100)
        subrun_number = executor.unique_run_number % 100
        mg_cmd = '\n'.join([mg_cmd,
            f'echo "source.firstRun: {run_number}" >> {os.path.basename(fcl)}',
            f'echo "source.firstSubRun: {subrun_number}" >> {os.path.basename(fcl)}'
        ])

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = SINGLE_FCL_TEMPLATE,
        larsoft_opts = executor.larsoft_opts,
        inputs = inputs,
        outputs = [File(str(output_filepath))],
        pre_job_hook = mg_cmd
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return future.outputs


class Reco2FromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.unique_run_number = 0
        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.subruns_per_caf = settings['workflow']['subruns_per_caf']

    def setup_single_workflow(self, iteration: int):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            s = Stage(StageType.RECO2)

            # each reco2 file will have its own directory
            s.run_dir = get_subrun_dir(self.output_dir, i)
            s.runfunc = runfunc
            workflow.add_final_stage(s)
        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings):
    wfe = Reco2FromGenExecutor(settings)
    wfe.execute()
    print(f'Submitted {len(wfe.futures)} futures.')
        
    for f in wfe.futures:
        try:
            print(f'[SUCCESS] task {f.tid} {f.filepath} {f.result()}')
        except Exception as e:
            print(f'[FAILED] task {f.tid} {f.filepath}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
