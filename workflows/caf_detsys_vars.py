#!/usr/bin/env python3

# This is intended to be an all-encompassing workflow that generates a central
# value (CV) MC sample, then scrubs the reco1 files and re-simulates them under
# different detector conditions. All reco2 files are put into CAF files

# output file structure:
"""
./
    cv/
        gen/
            gen-...-1.root
            gen-...-2.root
            gen-...-3.root
            ...
        ...
        caf/
            ...
    var1/
        gen/
            ...
        ...
    ...
    varN/
        ...
"""

import sys, os
import time
import json
import pathlib
import functools
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE, \
    SINGLE_FCL_TEMPLATE_SPACK
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name


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



def runfunc(self, fcl, input_files, run_dir, var_name, executor):
    """Method bound to each Stage object and run during workflow execution."""

    fcl_fullpath = executor.fcl_dir / fcl
    inputs = [str(fcl_fullpath), None]
    if input_files is not None:
        inputs = [str(fcl_fullpath)] + input_files

    run_dir.mkdir(parents=True, exist_ok=True)
    output_dir = executor.output_dir / var_name / self.stage_type.value
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = ''.join([
        str(self.stage_type.value), '-', var_name, '-',
        hash_name(os.path.basename(fcl) + executor.name_salt + str(executor.lar_run_counter)),
        ".root"
    ])
    executor.lar_run_counter += 1

    output_filepath = output_dir / output_filename
    mg_cmd = executor.meta[var_name].run_cmd(
        output_filename + '.json', os.path.basename(fcl), check_exists=False)

    if self.stage_type == StageType.GEN:
        executor.unique_run_number += 1
        run_number = 1 + (executor.unique_run_number // 100)
        subrun_number = executor.unique_run_number % 100
        mg_cmd = '\n'.join([mg_cmd,
            f'echo "source.firstRun: {run_number}" >> {os.path.basename(fcl)}',
            f'echo "source.firstSubRun: {subrun_number}" >> {os.path.basename(fcl)}'
        ])

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = SINGLE_FCL_TEMPLATE,
        larsoft_opts = executor.larsoft_opts,
        inputs = inputs,
        outputs = [File(str(output_filepath))],
        pre_job_hook = mg_cmd
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return future.outputs
            

class CAFFromGenDetsysExecutor(WorkflowExecutor):
    """
    Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 -> CAF
                                       |
                                       V
                                     Scrub -> G4 (variation) \
                                           -> Detsim (variation) \
                                           -> Reco1 (variation) \
                                           -> Reco2 -> CAF
    workflow.
    """
    def __init__(self, settings: json):
        super().__init__(settings)

        # use this to assign the run/subrun of generated events
        self.unique_run_number = 0

        # use this to count the number of larsoft runs
        self.lar_run_counter = 0

        # same seed + same output dir should produce the same file names
        # in case we need to re-run the workflow
        self.name_salt = str(settings['run']['seed']) + str(self.output_dir)

        # fcl structure in settings is a nested dict for this workflow
        self.default_fcls = self.fcls['cv']
        self.variations = [key for key in self.fcls.keys() if key != 'cv']

        # fill in the default fcls for each variation
        for var in self.variations:
            for key, val in self.default_fcls.items():
                if key in self.fcls[var]:
                    continue
                self.fcls[var][key] = val

        # use a separate metadata generator for each variation. This ensures
        # the fcl order is correct
        self.meta = {
            key: MetadataGenerator(
                settings['metadata'], fcls, defer_check=True) \
            for key, fcls in self.fcls.items()
        }

        # the default (CV) stage order
        self.stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM, \
                            StageType.RECO1, StageType.RECO2, StageType.CAF]

        # when we construct the scrub stage, this will let the workflow know to
        # treat a reco1 stage as the parent of scrub, instead of a fixed input file
        self.scrub_stage_order = [StageType.RECO1, StageType.SCRUB]

        # the stage order for the variations
        self.var_stage_order = [StageType.SCRUB, StageType.G4, StageType.DETSIM, \
                                StageType.RECO1, StageType.RECO2, StageType.CAF]


    def setup_single_workflow(self, iteration: int):
        """Create the workflow object with defaults for the CV, then add all the
        necessary variation stages and ancestry."""
        cv_dir = self.output_dir / 'cv'
        cv_runfunc = functools.partial(runfunc, var_name='cv', executor=self)

        var_dirs = {}
        var_runfuncs = {}
        for var in self.variations:
            var_dirs[var] = self.output_dir / var
            var_runfuncs[var] = functools.partial(runfunc, var_name=var, executor=self)

        workflow = Workflow(self.stage_order, default_fcls=self.default_fcls)

        # create reco2 file from MC, only need to specify the last stage
        # since there are no inputs for these
        # central value CAF stage, pass in required settings
        s = Stage(StageType.CAF)
        s.run_dir = cv_dir / 'caf'
        s.runfunc = cv_runfunc

        # CAF for each variation
        var_caf_stages = {}
        for var in self.variations:
            svar = Stage(StageType.CAF, stage_order=self.var_stage_order)
            svar.run_dir = var_dirs[var] / 'caf'
            svar.runfunc = var_runfuncs[var]
            var_caf_stages[var] = svar

        # each CAF gets 20 subruns
        for j in range(20):
            inst = iteration * 20 + j
            # define reco1 stage explicitly, so that we can link reco2 CV
            # and detsys scrub stages to it. Only need to set the run_dir
            # for RECO2, it will be inherited by parent stages!
            s1 = Stage(StageType.RECO1)
            s2 = Stage(StageType.RECO2)
            s2.run_dir = get_subrun_dir(cv_dir, inst)
            s2.add_parents(s1)

            # also add the reco2 stage to the CV caf stage
            s.add_parents(s2)

            # scrub stage for variations: takes reco1 from CV as input
            # note: manually set runfunc and subrun dir to CV, since
            # otherwise these will get set from variation child
            s3 = Stage(StageType.SCRUB, stage_order=self.scrub_stage_order)
            s3.runfunc = cv_runfunc
            s3.run_dir = get_subrun_dir(cv_dir, inst)
            s3.add_parents(s1)

            for var, svar in var_caf_stages.items():
                # variation fcl
                # variations may change any of the preceding stages, so
                # we enumerate all of them
                s4 = Stage(StageType.G4, stage_order=self.var_stage_order)
                s4.fcl = self.fcls[var]['g4']
                s4.add_parents(s3)

                s5 = Stage(StageType.DETSIM, stage_order=self.var_stage_order)
                s5.fcl = self.fcls[var]['detsim']
                s5.add_parents(s4)

                s6 = Stage(StageType.RECO1, stage_order=self.var_stage_order)
                s6.fcl = self.fcls[var]['reco1']
                s6.add_parents(s5)

                s7 = Stage(StageType.RECO2, stage_order=self.var_stage_order)
                s7.fcl = self.fcls[var]['reco2']
                s7.run_dir = get_subrun_dir(var_dirs[var], inst)
                s7.add_parents(s6)

                # finally add the variation built from scrub stage to the
                # variation caf. workflow will fill in the other stages
                svar.add_parents(s7)
 
        workflow.add_final_stage(s)
        for svar in var_caf_stages.values():
            workflow.add_final_stage(svar)
        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"{subrun:06d}"


def main(settings):
    # parsl
    user_opts = create_default_useropts()
    user_opts.update(settings['queue'])
    parsl_config = create_parsl_config(user_opts)
    print(parsl_config)
    parsl.clear()
    parsl.load(parsl_config)

    wfe = CAFFromGenDetsysExecutor(settings)
    wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
