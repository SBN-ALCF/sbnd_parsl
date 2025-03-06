#!/usr/bin/env python

# This workflow runs SPINE on a list of larcv files

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
from sbnd_parsl.templates import SPINE_TEMPLATE
from sbnd_parsl.utils import create_default_useropts, create_parsl_config


@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, spine_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        workdir=workdir,
        input=inputs[0],
        **spine_opts,
        output=outputs[0],
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def runfunc(self, fcl, input_files, run_dir, iteration, executor):
    """Method bound to each Stage object and run during workflow execution."""

    run_dir.mkdir(parents=True, exist_ok=True)
    output_dir = executor.output_dir / self.stage_type.value
    output_dir.mkdir(parents=True, exist_ok=True)

    # DataFuture for this task will be the final h5 file of all inputs
    last_file = None
    input_file = run_dir / 'filelist.txt'
    with open(input_file, 'w') as f:
        for file in input_files:
            f.write(f'{file}\n')
            last_file = file
    input_str = str(input_file)

    output_filepath = output_dir / pathlib.Path(f"{last_file.replace('.root', '')}_spine.h5").name
    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / 'spine.out'),
        stderr = str(run_dir / 'spine.err'),
        template = SPINE_TEMPLATE,
        spine_opts = executor.spine_opts,
        inputs = [input_str],
        outputs = [File(str(output_filepath))],
    )

    executor.futures.append(future.outputs[0])

    return future.outputs


class SpineExecutor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.stage_order = [StageType.SPINE]
        self.files_per_subrun = settings['workflow']['files_per_subrun']
        self.larcv_path = pathlib.Path(settings['workflow']['larcv_path'])
        self.spine_opts = settings['spine']

        self.spine_opts.update({'cores_per_worker': settings['workflow']['cores_per_worker']})

    def execute(self):
        """Override to create workflows from N files instead of iteration number."""
        spine_input_generator = self.larcv_path.rglob('larcv*.root')
        nsubruns = self.run_opts['nsubruns']

        idx_cycle = itertools.cycle(range(nsubruns))
        wfs = [None] * nsubruns
        skip_idx = set()

        while len(skip_idx) < nsubruns:
            idx = next(idx_cycle)
            if idx in skip_idx:
                continue

            if wfs[idx] is None:
                input_slice = list(itertools.islice(spine_input_generator, self.files_per_subrun))
                if not input_slice:
                    skip_idx.add(idx)
                    continue
                wfs[idx] = self.setup_single_workflow(idx, input_slice)

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

    def setup_single_workflow(self, iteration: int, larcv_files: List[pathlib.Path]):
        if not larcv_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, iteration=iteration, executor=self)
        s = Stage(StageType.SPINE)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_

        for i, file in enumerate(larcv_files):
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
    user_opts['cores_per_worker'] = settings['workflow']['cores_per_worker']
    user_opts.update(settings['queue'])
    parsl_config = create_parsl_config(user_opts)
    print(parsl_config)
    parsl.clear()
    parsl.load(parsl_config)

    wfe = SpineExecutor(settings)
    wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
