#!/usr/bin/env python3
"""
Classes for organizing LArSoft workflows for SBND

Reconstructed events either start as raw data or generated MC. Raw data must be
decoded, and generated MC vectors must be simulated at the Geant4 and the
detector response level,

Raw -> Decode
Gen -> G4 -> Detsim

After these steps, data and MC are handled in the same way,
Decode -> Reco1 -> Reco2 -> CAF
Detsim -> Reco1 -> Reco2 -> CAF

For simulating detector systematic variation samples, we can also "scrub" Reco1
MC files to get back to the same generated event:
Reco1 -> Scrub (Gen) -> G4 -> Detsim -> Reco1 -> Reco2 -> CAF

CAF files can also have multiple input Reco2 files
Reco2 + Reco2 + ... -> CAF

These classes implement this structure and generate jobs based on what you have
and what you want, automatically filling in intermediate steps as needed.
"""

from enum import Enum, auto
from pathlib import Path
from typing import List, Dict

# from parsl.data_provider.files import File

class NoInputFileException(Exception):
    pass

class NoFclFileException(Exception):
    pass

class StageType(Enum):
    # EMPTY = auto()
    GEN = auto()
    G4 = auto()
    DETSIM = auto()
    RECO1 = auto()
    RECO2 = auto()
    # RAW = auto()
    DECODE = auto()
    CAF = auto()
    SCRUB = auto()


# @bash_app(cache=True)
# def future(bash_script, inputs=[], outputs=[]):
#     """ Return formatted bash script which produces each future when executed """
#     return bash_script

class Stage:
    def __init__(self, stage_type: StageType, fcl: str=''):
        self._stage_type: StageType = stage_type
        self.fcl = fcl

        self._output_files = None
        self._ancestors = {}
        self._input_filenames = []

    @property
    def stage_type(self):
        return self._stage_type

    @property
    def output_files(self):
        if self._output_files is None:
            self.run()
        return self._output_files

    @property
    def ancestors(self):
        return self._ancestors

    def complete(self):
        return self._output_files is not None

    def add_input_file(self, filename):
        self._input_filenames.append(filename)

    def dryrun(self):
        '''
        produces the output file for this stage. recurse through previous
        stages if necessary
        '''
        if self.fcl == '':
            raise NoFclFileException(f'Attempt to run stage {self._stage_type} with no fcl provided and no default')

        input_file_arg_str = '' 
        if self._stage_type in [StageType.GEN]:
            # these stages have no input files, do nothing for now
            pass
        else:
            if len(self._input_filenames) == 0:
                raise NoInputFileException(f'Tried to run stage of type {self._stage_type} which requires at least one input file, but it was not set.')
            input_file_arg_str = \
                ' '.join([f'-s {file}' for file in self._input_filenames])

        output_file_arg_str = f'--output {self._stage_type}test.root'
        print(f'lar -c {self.fcl} {input_file_arg_str} {output_file_arg_str}')
        self._output_files = [f'{self._stage_type}test.root']
        # lar -c self._fcl -s parent_file --output next_filename

    def run(self):
        # if dryrun doesn't crash then submit futures
        self.dryrun()
        
    def clean(self):
        ''' delete the output file on disk '''
        self._output_files = None

    def add_ancestors(self, stages):
        '''
        Add an ancestry stage to this one.
        '''
        for s in stages:
            try: 
                self._ancestors[s.stage_type] += s
            except KeyError:
                self._ancestors[s.stage_type] = [s]


class Workflow:
    '''
    collection of stages and order to run the stages
    fills in the gaps between inputs and outputs
    '''
    def __init__(self, stage_order: List[StageType], default_fcls: Dict):
        self._stage_order = stage_order
        self._default_fcls = default_fcls
        self._stages = []

    def add_final_stage(self, stage: Stage):
        '''
        Add the final stage to a workflow
        '''
        self._stages.append(stage)

    def run(self):
        for s in self._stages:
            self.run_stage(s)

    def run_stage(self, stage: Stage):
        if stage.complete():
            return

        if stage.fcl == '':
            stage.fcl = self._default_fcls[stage.stage_type]

        # stages with no parent
        if stage.stage_type in [StageType.DECODE, StageType.SCRUB, StageType.GEN]:
            stage.run()
            return

        # stages with parents
        stage_idx = self._stage_order.index(stage.stage_type)
        parent_type = self._stage_order[stage_idx - 1]
        # assume user wants us to build the ancestry map if the parent type doesn't exist
        if parent_type not in stage.ancestors:
            parent_stage = Stage(parent_type)

            # copy over the known ancestors to this stage too
            for a in stage.ancestors.values():
                parent_stage.add_ancestors(a)

            self.run_stage(parent_stage)
            stage.add_ancestors([parent_stage])

        for a in stage.ancestors[parent_type]:
            self.run_stage(a)
            for f in a.output_files:
                stage.add_input_file(f)

        stage.run()


if __name__ == '__main__':
    # describe what you want... and what you have
    # e.g. 200 reco2 files from scrub reco1 files
    # e.g. 10 caf files from 200 reco2 files

    # stage_order = (StageType.SCRUB, StageType.G4, StageType.DETSIM, StageType.RECO1, StageType.RECO2)
    stage_order = (StageType.GEN, StageType.G4, StageType.DETSIM, StageType.RECO1, StageType.RECO2)
    default_fcls = {
        StageType.GEN: 'gen.fcl',
        StageType.SCRUB: 'scrub.fcl',
        StageType.G4: 'g4.fcl',
        StageType.DETSIM: 'detsim.fcl',
        StageType.RECO1: 'reco1.fcl',
        StageType.RECO2: 'reco2.fcl',
    }
    wf = Workflow(stage_order, default_fcls)

    for i in range(10):
        # define your inputs
        # s1 = Stage(StageType.SCRUB)
        # s1.add_input_file(f'reco1{i}.root')
        # s2 = Stage(StageType.G4, fcl='override.fcl')
        # s2.add_ancestors([s1])

        # assign them to your final stage. OK to leave gaps
        s3 = Stage(StageType.RECO2)
        # s3.add_ancestors([s2])

        # add final stage to the workflow. Workflow will fill in any gaps using
        # the order and default fcls
        wf.add_final_stage(s3)

    # run all stages
    wf.run()
