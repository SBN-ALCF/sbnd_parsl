#!/usr/bin/env python3

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor


class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_workflow(self):
        stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]

        self.workflow = Workflow(stage_order, self.fcls)
        s = Stage(StageType.DETSIM)
        s.run_dir = self.run_opts['output']

        # workflow will automatically fill in g4 and gen stages, with
        # run_dir inherited from detsim stage
        self.workflow.add_final_stage(s)

def main(settings):
    wfe = SimpleWorkflowExecutor(settings)
    wfe.execute()

if __name__ == '__main__':
    settings = {
        'run': {
            'output': 'output',
            'fclpath': 'fcls'
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
