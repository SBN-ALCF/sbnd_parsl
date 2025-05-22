#!/usr/bin/env python3

import os
import pathlib
from typing import List
import functools

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor
from sbnd_parsl.utils import create_default_useropts, create_parsl_config


@bash_app
def future(workdir, stdout, stderr, inputs=[], outputs=[]):
    return f"mkdir -p {workdir} && cd {workdir} && echo {inputs[0]}"

def runfunc(self, fcl, input_files, output_dir, executor):
    """Default function called when each stage is run."""
    input_file_arg_str = ''
    if input_files is not None:
        input_file_arg_str = \
            ' '.join([f'-s {str(file.filepath)}' for file in input_files])

    output_filename = os.path.basename(fcl).replace(".fcl", ".root")
    if output_dir is None:
        output_dir = pathlib.Path('.')
    output_file = output_dir / pathlib.Path(output_filename)
    output_file_arg_str = f'--output {str(output_file)}'
    print(f'lar -c {fcl} {input_file_arg_str} {output_file_arg_str}')

    result = future(
        workdir=str(output_dir),
        stdout=str(output_dir / 'test.out'),
        stderr=str(output_dir / 'test.err'), 
        inputs=[input_file_arg_str],
        outputs=[File(str(output_file))]
    )

    executor.futures.append(result.outputs[0])
    return [result.outputs[0]]


class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_single_workflow(self, iteration, filelist):
        stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]

        workflow = Workflow(stage_order, self.fcls)
        s = Stage(StageType.DETSIM)
        _runfunc = functools.partial(runfunc, executor=self)
        s.runfunc = _runfunc
        s.run_dir = pathlib.Path(self.run_opts['output']) / str(iteration)

        s2 = Stage(StageType.DETSIM)
        s2.runfunc = _runfunc
        s2.run_dir = pathlib.Path(self.run_opts['output']) / str(iteration) / 'a'

        # workflow will automatically fill in g4 and gen stages, with
        # run_dir inherited from detsim stage
        workflow.add_final_stage(s)
        workflow.add_final_stage(s2)

        s0 = Stage(StageType.G4)
        s.add_parents(s0, self.fcls)

        return workflow


def main(settings):
    user_opts = create_default_useropts()
    user_opts.update(settings['queue'])
    user_opts.update({'run_dir': '.'})
    parsl_config = create_parsl_config(user_opts)
    print(parsl_config)
    parsl.clear()

    with parsl.load(parsl_config):
        wfe = SimpleWorkflowExecutor(settings)
        wfe.execute()

if __name__ == '__main__':
    settings = {
        'run': {
            'output': '/flare/neutrinoGPU/parsl_test',
            'fclpath': 'fcls',
            'nsubruns': 10,
            'max_futures': 10
        },
        'larsoft': {},
        'fcls': {
            'gen': 'gen.fcl',
            'g4': 'g4.fcl',
            'detsim': 'detsim.fcl'
        },
        'queue': {
            'allocation': 'gpu_hack',
        },
        'workflow': {}
    }
    # or load from a JSON file

    main(settings)
