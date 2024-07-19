#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import json
import pathlib
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE


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


class WorkflowExecutor: 
    """Class to wrap settings and workflow objects."""
    def __init__(self, settings: json):
        self.larsoft_opts = settings['larsoft']
        self.output_dir = pathlib.Path(settings['run']['output'])
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.fcl_dir = pathlib.Path(settings['run']['fclpath'])
        self.fcls = settings['fcls']

        self.user_opts = create_default_useropts()
        self.user_opts.update(settings['queue'])

        self.user_opts["run_dir"] = str(self.output_dir / 'runinfo')
        print(self.user_opts)

        self.config = create_parsl_config(self.user_opts)
        print(self.config)

        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)

        self.workflow_opts = settings['workflow']

        self.unique_run_number = 0
        self.futures = []
        parsl.clear()
        parsl.load(self.config)

        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.workflow = Workflow(self.stage_order, self.fcls)
        nsubruns = settings['run']['nsubruns']
        for i in range(nsubruns):
            # create reco2 file from MC, only need to specify the last stage since
            # there are no inputs
            s = Stage(StageType.RECO2)
            s.run_dir = get_subrun_dir(self.output_dir, i)
            s.runfunc = self.runfunc_generator()
            self.workflow.add_final_stage(s)


    def execute(self):
        self.workflow.run()


    def runfunc_generator(self):
        """Create a runfunc for each workflow stage using this class' members."""
        self.unique_run_number += 1

        def runfunc(fcl, input_files, output_dir):
            """
            Submit a future for this stage and return the Parsl File object
            for this stage's output. This function is called by each stage
            object, and sets the stage's output file.
            """

            run_number = 1 + (self.unique_run_number // 100)
            subrun_number = self.unique_run_number % 100

            fcl_order = [self.fcls[m.value] for m in self.stage_order]
            idx = fcl_order.index(fcl)

            fcl_fullpath = self.fcl_dir / fcl
            inputs = [str(fcl_fullpath), None]
            if input_files is not None:
                inputs = [str(fcl_fullpath)] + input_files

            output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = os.path.basename(fcl).replace(".fcl", ".root")
            output_filepath = output_dir / pathlib.Path(output_filename)
            output_file = [File(str(output_filepath))]

            mg_cmd = f'PATH={self.larsoft_opts["larsoft_top"]}/sbndutil/v09_88_00_02/bin:$PATH {self.meta.run_cmd(os.path.basename(fcl), check_exists=False)}'
            if idx == 0:
                mg_cmd = '\n'.join([mg_cmd,
                    f'echo "source.firstRun: {run_number}" >> {os.path.basename(fcl)}',
                    f'echo "source.firstSubRun: {subrun_number}" >> {os.path.basename(fcl)}'
                ])

            future = fcl_future(
                workdir = str(output_dir),
                stdout = str(output_dir / f'larStage{idx}.out'),
                stderr = str(output_dir / f'larStage{idx}.err'),
                template = SINGLE_FCL_TEMPLATE,
                larsoft_opts = self.larsoft_opts,
                inputs = inputs,
                outputs = output_file,
                pre_job_hook = mg_cmd
            )
            self.futures.append(future)

            return future.outputs

        return runfunc


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    ''' create directory structure like XX00/XXXX '''
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings):
    wfe = WorkflowExecutor(settings)
    wfe.execute()

    print(f'Submitted {len(wfe.futures)} futures.')
        
    for f in wfe.futures:
        try:
            print(f'[SUCCESS] task {f.tid} {f.filepath} {f.result()}')
        except Exception as e:
            print(f'[FAILED] task {f.tid} {f.filepath}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
