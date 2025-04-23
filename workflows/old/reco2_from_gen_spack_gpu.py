#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import json
import pathlib
import functools
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE_SPACK, \
    SINGLE_FCL_TEMPLATE_SPACK
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name

@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook='', parsl_resource_specification={}):
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


def runfunc(self, fcl, input_files, run_dir, executor):
    """Method bound to each Stage object and run during workflow execution."""

    fcl_fullpath = executor.fcl_dir / fcl
    inputs = [str(fcl_fullpath), None]
    if input_files is not None:
        inputs = [str(fcl_fullpath)] + input_files

    run_dir.mkdir(parents=True, exist_ok=True)
    output_dir = executor.output_dir / self.stage_type.value
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = ''.join([
        str(self.stage_type.value), '-',
        hash_name(os.path.basename(fcl) + executor.name_salt + str(executor.lar_run_counter)),
        ".root"
    ])
    executor.lar_run_counter += 1

    output_filepath = output_dir / output_filename
    # mg_cmd = executor.meta.run_cmd(
    #     output_filename + '.json', os.path.basename(fcl), check_exists=False)

    if self.stage_type == StageType.GEN:
        executor.unique_run_number += 1
        run_number = 1 + (executor.unique_run_number // 100)
        subrun_number = executor.unique_run_number % 100
        # mg_cmd = '\n'.join([mg_cmd,
        #     f'echo "source.firstRun: {run_number}" >> {os.path.basename(fcl)}',
        #     f'echo "source.firstSubRun: {subrun_number}" >> {os.path.basename(fcl)}'
        # ])

    task_parsl_resource_specification = {'cores': 1, 'memory': 100, 'disk': 500, 'priority':10}
    if self.stage_type == StageType.DETSIM:
        task_parsl_resource_specification = {'cores': 1, 'memory': 100, 'disk': 500, 'gpus': 1, 'priority':100}

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = SINGLE_FCL_TEMPLATE_SPACK,
        larsoft_opts = executor.larsoft_opts,
        inputs = inputs,
        outputs = [File(str(output_filepath))],
        parsl_resource_specification = task_parsl_resource_specification
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])
    return future.outputs


def runfunc_caf(self, fcl, input_files, run_dir, executor):
    """Method bound to each Stage object and run during workflow execution."""

    fcl_fullpath = executor.fcl_dir / fcl
    caf_input_arg = ' '.join([f'{fname.filename}' for fname in input_files])
    inputs = [str(fcl_fullpath), caf_input_arg] + input_files

    run_dir.mkdir(parents=True, exist_ok=True)
    output_dir = executor.output_dir / self.stage_type.value
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = os.path.splitext(os.path.basename(input_files[0].filename))[0] + '.flat.caf.root'
    executor.lar_run_counter += 1

    output_filepath = output_dir / output_filename
    mg_cmd = ''
    # mg_cmd = executor.meta.run_cmd(
    #     output_filename + '.json', os.path.basename(fcl), check_exists=False)

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = CAF_TEMPLATE_SPACK,
        larsoft_opts = executor.larsoft_opts,
        inputs = inputs,
        outputs = [File(output_filepath)],
        pre_job_hook = mg_cmd,
        parsl_resource_specification = {'cores': 1, 'memory': 100, 'disk': 500, 'priority': 10}
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return future.outputs


class Reco2FromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.unique_run_number = 0
        self.lar_run_counter = 0
        # self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.subruns_per_caf = settings['workflow']['subruns_per_caf']
        self.name_salt = str(settings['run']['seed']) + str(self.output_dir)

    def setup_single_workflow(self, iteration: int):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, executor=self)
        runfunc_caf_ = functools.partial(runfunc_caf, executor=self)
        s = Stage(StageType.CAF)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        workflow.add_final_stage(s)
        s.runfunc = runfunc_caf_

        workflow.add_final_stage(s)
        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            s2 = Stage(StageType.RECO2)
            s2.runfunc = runfunc_

            # each reco2 file will have its own directory
            s2.run_dir = get_subrun_dir(self.output_dir, inst)
            s.add_parents(s2, workflow.default_fcls)

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings):
    # parsl
    user_opts = create_default_useropts()
    user_opts['run_dir'] = str(pathlib.Path(settings['run']['output']) / 'runinfo')
    user_opts.update(settings['queue'])
    parsl_config = create_parsl_config(user_opts, [settings['larsoft']['spack_top'],settings['larsoft']['version']])
    print(parsl_config)
    parsl.clear()
    parsl.load(parsl_config)

    wfe = Reco2FromGenExecutor(settings)
    wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
