#!/usr/bin/env python3

# This is intended to be an all-encompassing workflow that generates a central
# value (CV) MC sample, then scrubs the reco1 files and re-simulates them under
# different detector conditions. All reco2 files are put into CAF files

import sys, os
import json
import pathlib
from types import MethodType
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE, SINGLE_FCL_TEMPLATE_SPACK


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


class CAFFromGenDetsysExecutor(WorkflowExecutor):
    """
    Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 -> CAF
                                       |
                                       V
                                     Scrub -> G4 (variation) \
                                           -> Detsim (variation) -> Reco1 \
                                           -> Reco2 -> CAF
    workflow.
    """
    def __init__(self, settings: json):
        super().__init__(settings)

        self.unique_run_number = 0
        self.default_fcls = self.fcls['default']
        self.meta = MetadataGenerator(settings['metadata'], self.default_fcls, defer_check=True)

        self.variations = [key for key in self.fcls.keys() if key != 'default']

        # fill in the default fcls for each variation
        for var in self.variations:
            for key, val in self.default_fcls.items():
                if key in self.fcls[var]:
                    continue
                self.fcls[var][key] = val

        # the default (CV) stage order
        self.stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM, \
                            StageType.RECO1, StageType.RECO2, StageType.CAF]

        # when we construct the scrub stage, this will let the workflow know to
        # treat a reco1 stage as the parent of scrub, instead of a fixed input file
        self.scrub_stage_order = [StageType.RECO1, StageType.SCRUB]
        self.var_stage_order = [StageType.SCRUB, StageType.G4, StageType.DETSIM, \
                                StageType.RECO1, StageType.RECO2, StageType.CAF]

    def setup_workflow(self):
        self.workflow = Workflow(self.stage_order, default_fcls=self.default_fcls)
        nsubruns = self.run_opts['nsubruns']
        for i in range(nsubruns):
            # define the function to run at each stage
            def runfunc(fcl, input_files, output_dir):
                """
                Submit a future for this stage and return the Parsl File object
                for this stage's output. This function is called by each stage
                object, and sets the stage's output file.
                """

                fcl_fullpath = self.fcl_dir / fcl
                inputs = [str(fcl_fullpath), None]
                if input_files is not None:
                    inputs = [str(fcl_fullpath)] + input_files

                output_dir.mkdir(parents=True, exist_ok=True)
                output_filename = os.path.basename(fcl).replace(".fcl", ".root")
                output_filepath = output_dir / pathlib.Path(output_filename)
                output_file = [File(str(output_filepath))]

                mg_cmd = '' # self.meta.run_cmd(os.path.basename(fcl), check_exists=False)

                idx = -1
                for fcl_list in self.fcls.values():
                    try:
                        idx = [fcl_list[m.value] for m in self.stage_order].index(fcl)
                        break
                    except ValueError:
                        pass

                if idx == 0:
                    # increment the event number and subrun number for each gen stage file
                    self.unique_run_number += 1
                    run_number = 1 + (self.unique_run_number // 100)
                    subrun_number = self.unique_run_number % 100
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
                self.futures.append(future.outputs[0])

                return future.outputs

            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            # central value CAF
            cv_dir = self.output_dir / 'cv'
            s = Stage(StageType.CAF)
            s.run_dir = cv_dir / 'caf'
            # s.runfunc = runfunc

            # CAF for each variation
            var_caf_stages = {}
            var_dirs = {}
            for var in self.variations:
                svar = Stage(StageType.CAF, stage_order=self.var_stage_order)
                var_dir = self.output_dir / var
                svar.run_dir = var_dir / 'caf'
                svar.runfunc = runfunc
                var_caf_stages[var] = svar
                var_dirs[var] = var_dir

            # each CAF gets 20 subruns
            for j in range(20):
                # central value CAF, just delcare reco1 stage to be used below
                # workflow will fill in reco2 & other stages
                subrun_dir = get_subrun_dir(cv_dir, j)
                s2 = Stage(StageType.RECO1)
                s2.runfunc = runfunc
                s2.run_dir = subrun_dir
                s.add_ancestors(s2)

                s1 = Stage(StageType.RECO2)
                s1.runfunc = runfunc
                s1.run_dir = subrun_dir
                s.add_ancestors(s2)

                # scrub stage for variations: takes reco1 from CV as input
                s3 = Stage(StageType.SCRUB, stage_order=self.scrub_stage_order)
                s3.runfunc = runfunc2
                s3.run_dir = subrun_dir
                s3.add_ancestors(s2)

                for var in self.variations:
                    var_subrun_dir = get_subrun_dir(var_dirs[var], j)
                    # variation fcl
                    # variations may change any of the preceding stages, so
                    # we enumerate all of them
                    s4 = Stage(StageType.G4, stage_order=self.var_stage_order)
                    s4.fcl = self.fcls[var]['g4']
                    s4.runfunc = runfunc
                    s4.run_dir = var_subrun_dir
                    s4.add_ancestors(s3)

                    s5 = Stage(StageType.DETSIM, stage_order=self.var_stage_order)
                    s5.fcl = self.fcls[var]['detsim']
                    s5.runfunc = runfunc
                    s5.run_dir = var_subrun_dir
                    s5.add_ancestors(s4)

                    s6 = Stage(StageType.RECO1, stage_order=self.var_stage_order)
                    s6.fcl = self.fcls[var]['reco1']
                    s6.runfunc = runfunc
                    s6.run_dir = var_subrun_dir
                    s6.add_ancestors(s5)

                    s7 = Stage(StageType.RECO2, stage_order=self.var_stage_order)
                    s7.fcl = self.fcls[var]['reco2']
                    s7.runfunc = runfunc
                    s7.run_dir = var_subrun_dir
                    s7.add_ancestors(s6)

                    # finally add the variation built from scrub stage to the
                    # variation caf.  workflow will fill in the other stages
                    var_caf_stages[var].add_ancestors(s7)


            # each reco2 file will have its own directory
            self.workflow.add_final_stage(s)
            for var, svar in var_caf_stages.items():
                self.workflow.add_final_stage(svar)


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings):
    wfe = CAFFromGenDetsysExecutor(settings)
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
