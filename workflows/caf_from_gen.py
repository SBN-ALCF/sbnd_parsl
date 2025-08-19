#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import json
import pathlib
import functools
import itertools
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
        DefaultStageTypes
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import CMD_TEMPLATE_SPACK, CMD_TEMPLATE_CONTAINER
from sbnd_parsl.components import mc_runfunc
from sbnd_parsl.utils import create_default_useropts, create_parsl_config
from sbnd_parsl.dfk_hacks import apply_hacks


class Reco2FromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.unique_run_number = 0
        self.lar_run_counter = 0
        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [DefaultStageTypes.from_str(key) for key in self.fcls.keys()]
        self.subruns_per_caf = settings['workflow']['subruns_per_caf']
        self.name_salt = str(settings['run']['seed']) + str(self.output_dir)
        self.runfunc = functools.partial(mc_runfunc, executor=self, 
                                         template=CMD_TEMPLATE_CONTAINER,
                                         meta=self.meta)

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = self.runfunc
        s = Stage(DefaultStageTypes.CAF)
        s.runfunc = self.runfunc
        workflow.add_final_stage(s)
        s.run_dir = get_caf_dir(self.output_dir, iteration)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            s2 = Stage(DefaultStageTypes.RECO2)
            s.add_parents(s2, workflow.default_fcls)

            # each reco2 file will have its own directory
            s2.run_dir = get_subrun_dir(self.output_dir, inst)

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / f"{(subrun//100):06d}" / f"subrun_{subrun:06d}"

def get_caf_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/caf/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / 'caf' / f"subrun_{subrun:06d}"


def main(settings):
    # parsl
    user_opts = create_default_useropts()
    user_opts['run_dir'] = str(pathlib.Path(settings['run']['output']) / 'runinfo')
    user_opts.update(settings['queue'])
    parsl_config = create_parsl_config(user_opts)
    print(parsl_config)
    parsl.clear()

    with parsl.load(parsl_config) as dfk:
        apply_hacks(dfk)
        wfe = Reco2FromGenExecutor(settings)
        wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
