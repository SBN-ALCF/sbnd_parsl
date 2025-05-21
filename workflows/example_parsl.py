#!/usr/bin/env python3

import pathlib

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor
from sbnd_parsl.utils import create_default_useropts, create_parsl_config


class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_single_workflow(self, iteration, filelist):
        stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]

        workflow = Workflow(stage_order, self.fcls)
        s = Stage(StageType.DETSIM)
        s.run_dir = str(pathlib.Path(self.run_opts['output']) / str(iteration))

        s2 = Stage(StageType.DETSIM)
        s2.run_dir = str(pathlib.Path(self.run_opts['output']) / str(iteration) / 'a')

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
            'output': 'output',
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
            'allocation': 'dummy'
        },
        'workflow': {}
    }
    # or load from a JSON file

    main(settings)
