#!/usr/bin/env python

# This workflow runs the decoder on raw data files

import sys, os
import re
import json
import time
import pathlib
import functools
import itertools
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import CMD_TEMPLATE_SPACK
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name

# SBND_RAWDATA_REGEXP = re.compile(r".*data_.*run(\d+)_.*\.root")

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


def runfunc(self, fcl, inputs, run_dir, executor, nevts=-1, nskip=0):
    """Method bound to each Stage object and run during workflow execution."""

    run_dir.mkdir(parents=True, exist_ok=True)

    # decode stage takes a string filename
    first_file_name = inputs[0]
    if self.stage_type == StageType.DECODE:
        inputs = [inputs, '']
    else:
        first_file_name = inputs[0][0].filename

    # from string or posixpath input
    output_filename = '-'.join([
        str(self.stage_type.value), f'{nskip:03d}', os.path.basename(first_file_name), 
    ])

    # run_number_str = SBND_RAWDATA_REGEXP.match(first_file_name).groups()[0]
    # output_dir = executor.output_dir / self.stage_type.value / run_number_str
    # output_dir.mkdir(parents=True, exist_ok=True)

    output_dir = executor.output_dir / self.stage_type.value / f'{executor.run_counter // 10:06d}'
    executor.run_counter += 1
    if self.combine:
        # if we are combining, save this stage's result only on node-local disk
        # output_dir.name gets the instance number for this task
        run_dir = pathlib.Path('/local/scratch') / run_dir.name
        output_dir = run_dir
    else:
        # save this result to filesystem (eagle). Make sure it exists
        output_dir.mkdir(parents=True, exist_ok=True)


    output_file = output_dir / output_filename
    output_file_arg_str = ''
    if self.stage_type != StageType.CAF:
        output_file_arg_str = f'--output {str(output_file)}'

    # output_filepath = output_dir / output_filename

    input_file_arg_str = ''
    parent_cmd = ''
    input_arg = [str(fcl), None]
    if inputs is not None:
        input_files = list(itertools.chain.from_iterable(inputs[0::2]))
        parent_cmds = '&&'.join(inputs[1::2])
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' if not isinstance(file, parsl.app.futures.DataFuture) else f'-s {str(file.filepath)}' for file in input_files])
        input_arg = [str(fcl)] + [str(f) if not isinstance(f, parsl.app.futures.DataFuture) else f for f in input_files]

    cmd = f'mkdir -p {run_dir} && cd {run_dir} && lar -c {fcl} {input_file_arg_str} {output_file_arg_str} --nevts={nevts} --nskip={nskip}'

    print(cmd)
    # decode and reco1 stages have two output streams due to art filters
    # if self.stage_type == StageType.DECODE or self.stage_type == StageType.RECO1:
    #     cmd += f'--output out1:{str(output_filepath)} --output out2:skipped.root'

    if parent_cmd != '':
        cmd = ' && '.join([parent_cmd, cmd])

    if self.combine:
        # don't submit work, just forward commands to the next task
        return [[output_file], cmd]

    mg_cmd = ''
    # executor.meta.run_cmd(
    #     output_filename + '.json', os.path.basename(fcl), check_exists=False)

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


class DecoderExecutor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        # self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.files_per_subrun = settings['run']['files_per_subrun']
        self.run_list = None
        if 'run_list' in settings['workflow']:
            with open(settings['workflow']['run_list'], 'r') as f:
                self.run_list = [int(l.strip()) for l in f.readlines()]

        self.rawdata_path = pathlib.Path(settings['workflow']['rawdata_path'])

        # for organizing outputs
        self.run_counter = 0

    def file_generator(self):
        path_generators = [self.rawdata_path.rglob('*.root')]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(self, iteration: int, rawdata_files: List[pathlib.Path]):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, executor=self)
        s = Stage(StageType.DECODE)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        for file in rawdata_files:
            s.add_input_file(str(file))

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings):
    # parsl
    user_opts = create_default_useropts()
    user_opts['run_dir'] = str(pathlib.Path(settings['run']['output']) / 'runinfo')
    user_opts.update(settings['queue'])
    parsl_config = create_parsl_config(user_opts, [settings['larsoft']['spack_top'], settings['larsoft']['version'], settings['larsoft']['software']])
    print(parsl_config)
    parsl.clear()

    with parsl.load(parsl_config):
        wfe = DecoderExecutor(settings)
        wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
