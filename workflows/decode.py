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
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE, \
    SINGLE_FCL_TEMPLATE_SPACK, CAF_TEMPLATE_SPACK
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name


SBND_RAWDATA_REGEXP = re.compile(r".*data_.*run(\d+)_.*\.root")

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


def runfunc(self, fcl, input_files, run_dir, executor, nevts=-1, nskip=0):
    """Method bound to each Stage object and run during workflow execution."""

    fcl_fullpath = executor.fcl_dir / fcl
    inputs = [str(fcl_fullpath), None]
    if input_files is not None:
        inputs = [str(fcl_fullpath)] + input_files

    run_dir.mkdir(parents=True, exist_ok=True)

    # decode stage takes a string filename
    first_file_name = input_files[0]
    if self.stage_type != StageType.DECODE:
        # but other stages take a datafuture input
        first_file_name = input_files[0].filename

    run_number_str = SBND_RAWDATA_REGEXP.match(first_file_name).groups()[0]
    output_dir = executor.output_dir / self.stage_type.value / run_number_str
    output_dir.mkdir(parents=True, exist_ok=True)

    if self.stage_type != StageType.CAF:
        # from string or posixpath input
        output_filename = '-'.join([
            str(self.stage_type.value), os.path.basename(first_file_name)
        ])
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.flat.caf.root'

    output_filepath = output_dir / output_filename

    # decode and reco1 stages have two output streams due to art filters
    if self.stage_type == StageType.DECODE or self.stage_type == StageType.RECO1:
        output_cmd = f'--output out1:{str(output_filepath)} --output out2:skipped.root'
    else:
        output_cmd = f'--output {str(output_filepath)}'
    opts = executor.larsoft_opts.copy()
    opts.update({'output_cmd': output_cmd, 'nevts': nevts, 'nskip': nskip})

    mg_cmd = executor.meta.run_cmd(
        output_filename + '.json', os.path.basename(fcl), check_exists=False)

    template = SINGLE_FCL_TEMPLATE_SPACK
    if self.stage_type == StageType.CAF:
        template = CAF_TEMPLATE_SPACK

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = template,
        larsoft_opts = opts,
        inputs = inputs,
        outputs = [File(str(output_filepath))],
        pre_job_hook = mg_cmd
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return future.outputs


class DecoderExecutor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.files_per_subrun = settings['workflow']['files_per_subrun']
        self.run_list = None
        with open(settings['workflow']['run_list'], 'r') as f:
            self.run_list = [int(l.strip()) for l in f.readlines()]

        self.rawdata_path = pathlib.Path(settings['workflow']['rawdata_path'])


    def execute(self):
        """Override to create workflows from N files instead of iteration number."""
        rawdata_generators = [self.rawdata_path.rglob(f'{run:06d}/data*Muon*.root') for run in self.run_list]
        rawdata_files = itertools.chain(*rawdata_generators)

        nsubruns = self.run_opts['nsubruns']

        idx_cycle = itertools.cycle(range(nsubruns))
        wfs = [None] * nsubruns
        skip_idx = set()

        while len(skip_idx) < nsubruns:
            idx = next(idx_cycle)
            if idx in skip_idx:
                continue

            if wfs[idx] is None:
                rawdata_slice = list(itertools.islice(rawdata_files, self.files_per_subrun))
                if not rawdata_slice:
                    skip_idx.add(idx)
                    continue
                wfs[idx] = self.setup_single_workflow(idx, rawdata_slice)

            # rate-limit the number of concurrent futures to avoid using too
            # much memory on login nodes
            while len(self.futures) > self.max_futures:
                self.get_task_results()
                print(f'Waiting: Current futures={len(self.futures)}')
                time.sleep(10)

            try:
                while True:
                    next(wfs[idx].get_next_task())
                    # if not self.futures[-1].done():
                    #     break
            except StopIteration:
                skip_idx.add(idx)

                # let garbage collection happen
                # del wfs[idx]
                wfs[idx] = None
        
        while len(self.futures) > 0:
            self.get_task_results()
            time.sleep(10)

    def setup_single_workflow(self, iteration: int, rawdata_files: List[pathlib.Path]):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, executor=self)
        s = Stage(StageType.CAF)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        for i, file in enumerate(rawdata_files):
            s2 = Stage(StageType.RECO2)
            s2.run_dir = get_subrun_dir(self.output_dir, iteration * self.files_per_subrun + i)

            # break reco1 into multiple jobs to avoid timing out while
            # processing large files
            s3 = Stage(StageType.RECO1)

            s4 = Stage(StageType.DECODE)
            s4.add_input_file(str(file))

            s.add_parents(s2, workflow.default_fcls)
            s2.add_parents(s3, workflow.default_fcls)
            s3.add_parents(s4, workflow.default_fcls)

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings):
    # parsl
    user_opts = create_default_useropts()
    user_opts['run_dir'] = str(pathlib.Path(settings['run']['output']) / 'runinfo')
    user_opts.update(settings['queue'])
    parsl_config = create_parsl_config(user_opts)
    parsl_config = create_parsl_config(user_opts, [settings['larsoft']['spack_top'],settings['larsoft']['version']])
    print(parsl_config)
    parsl.clear()
    parsl.load(parsl_config)

    wfe = DecoderExecutor(settings)
    wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
