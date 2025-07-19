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
from sbnd_parsl.templates import CMD_TEMPLATE_SPACK
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name, subrun_dir


def fcl_future(workdir, stdout, stderr, template, cmd, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1],
        cmd=cmd,
        **larsoft_opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def runfunc(self, fcl, inputs, run_dir, executor):
    """Method bound to each Stage object and run during workflow execution."""

    output_filename = ''.join([
        str(self.stage_type.value), '-',
        hash_name(os.path.basename(fcl) + executor.name_salt + str(executor.lar_run_counter)),
        ".root"
    ])
    executor.lar_run_counter += 1

    output_dir = executor.output_dir / self.stage_type.value
    if self.combine:
        # if we are combining, save this stage's result only on node-local disk
        # output_dir.name gets the instance number for this task
        run_dir = pathlib.Path('/local/scratch') / run_dir.name
        output_dir = run_dir
    else:
        # save this result to filesystem (eagle). Make sure it exists
        run_dir = pathlib.Path('/tmp') / run_dir.name
        output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / output_filename
    output_file_arg_str = f'--output {str(output_file)}'
    mg_cmd = executor.meta.run_cmd(
        output_filename + '.json', os.path.basename(fcl), check_exists=False)

    if self.stage_type == StageType.GEN:
        executor.unique_run_number += 1
        run_number = 1 + (executor.unique_run_number // 100)
        subrun_number = executor.unique_run_number % 100
        mg_cmd = '\n'.join([mg_cmd,
            f'echo "source.firstRun: {run_number}" >> {os.path.basename(fcl)}',
            f'echo "source.firstSubRun: {subrun_number}" >> {os.path.basename(fcl)}'
        ])
    
    input_file_arg_str = ''
    parent_cmd = ''
    input_arg = [str(fcl), None]
    if inputs is not None:
        input_files = inputs[0]
        parent_cmd = inputs[1]
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' if not isinstance(file, parsl.app.futures.DataFuture) else f'-s {str(file.filepath)}' for file in input_files])
        input_arg = [str(fcl)] + [str(f) if not isinstance(f, parsl.app.futures.DataFuture) else f for f in input_files]

    cmd = f'mkdir -p {run_dir} && cd {run_dir} && lar -c {fcl} {input_file_arg_str} {output_file_arg_str}'

    if self.stage_type == StageType.GEN:
        cmd += f' --nevts {executor.larsoft_opts["nevts"]}'

    if parent_cmd != '':
        cmd = ' && '.join([parent_cmd, cmd])

    if self.combine:
        # don't submit work, just forward commands to the next task
        return [[output_file], cmd]

    # submit cmd as a parsl bash_app
    future_func = functools.partial(fcl_future)
    future_func.__name__ = self.stage_type.value
    app = bash_app(future_func, cache=True)

    future = app(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = CMD_TEMPLATE_SPACK,
        cmd=cmd,
        larsoft_opts = executor.larsoft_opts,
        inputs = input_arg,
        outputs = [File(str(output_file))],
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return [future.outputs, '']


class Reco2FromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.unique_run_number = 0
        self.lar_run_counter = 0
        self.meta = MetadataGenerator(settings.get('metadata', {}), self.fcls, defer_check=True)
        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.subruns_per_caf = settings['workflow']['subruns_per_caf']
        self.name_salt = str(settings['run']['seed']) + str(self.output_dir)

    def setup_single_workflow(self, iteration: int, filelist=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, executor=self)
        s = Stage(StageType.CAF)
        s.run_dir = subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            s2 = Stage(StageType.RECO2)

            # each reco2 file will have its own directory
            s2.run_dir = subrun_dir(self.output_dir, inst)
            s.add_parents(s2, self.fcls)

        return workflow


def main(settings):
    user_opts = create_default_useropts(**settings['queue'])
    user_opts['run_dir'] = str(pathlib.Path(settings['run']['output']) / 'runinfo')
    parsl_config = create_parsl_config(user_opts, [settings['larsoft']['spack_top'], settings['larsoft']['version'], settings['larsoft']['software']])
    print(parsl_config)
    parsl.clear()
    with parsl.load(parsl_config):
        wfe = Reco2FromGenExecutor(settings)
        wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
