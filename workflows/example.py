#!/usr/bin/env python3

import pathlib

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor


class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_single_workflow(self, iteration):
        stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]

        workflow = Workflow(stage_order, self.fcls)
        s = Stage(StageType.DETSIM)
        s.run_dir = str(pathlib.Path(self.run_opts['output']) / str(iteration))

        s0 = Stage(StageType.G4)
        s.add_parents(s0)

        s2 = Stage(StageType.DETSIM)
        s2.run_dir = str(pathlib.Path(self.run_opts['output']) / str(iteration) / 'a')

        # workflow will automatically fill in g4 and gen stages, with
        # run_dir inherited from detsim stage
        workflow.add_final_stage(s)
        # workflow.add_final_stage(s2)
        return workflow


def main(settings):
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
        'queue': {},
        'workflow': {}
    }
    # or load from a JSON file

    main(settings)
