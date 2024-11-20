#!/usr/bin/env python

# This workflow runs SPINE on the larcv files produced at the reco1 stage

import sys, os
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
from sbnd_parsl.templates import SPINE_TEMPLATE
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name

@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[0],
        **opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def runfunc(self, fcl, input_files, run_dir, executor):
    """Method bound to each Stage object and run during workflow execution."""
    run_dir.mkdir(parents=True, exist_ok=True)
    output_dir = executor.output_dir / self.stage_type.value
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = '-'.join([
        str(self.stage_type.value), os.path.basename(input_files[0])
    ])

    output_filepath = output_dir / output_filename
    opts = executor.spine_settings

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = SPINE_TEMPLATE,
        opts = opts,
        inputs = input_files,
        outputs = [File(str(output_filepath))],
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return future.outputs


class SPINEfromReco1Executor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.stage_order = [StageType.SPINE]
        self.fcls = {}
        self.spine_settings = settings['spine']
        self.files_per_subrun = settings['workflow']['files_per_subrun']
        self.reco1_path = pathlib.Path(settings['workflow']['reco1_path'])


    def execute(self):
        """Override to create workflows from N files instead of iteration number."""

        larcv_files = self.reco1_path.rglob('larcv.root')
        nsubruns = self.run_opts['nsubruns']

        idx_cycle = itertools.cycle(range(nsubruns))
        wfs = [None] * nsubruns
        skip_idx = set()

        while len(skip_idx) < nsubruns:
            idx = next(idx_cycle)
            if idx in skip_idx:
                continue

            wf = wfs[idx]
            if wf is None:
                file_slice = list(itertools.islice(larcv_files, self.files_per_subrun))
                if not file_slice:
                    skip_idx.add(idx)
                    continue
                wf = self.setup_single_workflow(idx, file_slice)
                wfs[idx] = wf

            # rate-limit the number of concurrent futures to avoid using too
            # much memory on login nodes
            while len(self.futures) > self.max_futures:
                self.get_task_results()
                print(f'Waiting: Current futures={len(self.futures)}')
                time.sleep(10)

            try:
                next(wf.get_next_task())
            except StopIteration:
                skip_idx.add(idx)

                # let garbage collection happen
                wfs[idx] = None
        
        while len(self.futures) > 0:
            self.get_task_results()
            time.sleep(10)

    def setup_single_workflow(self, iteration: int, reco1_files: List[pathlib.Path]):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, executor=self)
        s = Stage(StageType.SPINE)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        if not reco1_files:
            raise RuntimeError()

        for file in reco1_files:
            s.add_input_file(str(file))

        workflow.add_final_stage(s)
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
    print(parsl_config)
    parsl.clear()
    parsl.load(parsl_config)

    wfe = SPINEfromReco1Executor(settings)
    wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
