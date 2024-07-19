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

import os
from enum import Enum, auto
from pathlib import Path
from typing import List, Dict, Optional, Callable


class NoInputFileException(Exception):
    pass


class NoFclFileException(Exception):
    pass


class StageType(Enum):
    GEN = 'gen'
    G4 = 'g4'
    DETSIM = 'detsim'
    RECO1 = 'reco1'
    RECO2 = 'reco2'
    DECODE = 'decode'
    CAF = 'caf'
    SCRUB = 'scrub'
    EMPTY = 'empty'

    @staticmethod
    def from_str(text: str):
        try:
            return StageType[text.upper()]
        except KeyError:
            return StageType.Empty


class Stage:
    def __init__(self, stage_type: StageType, fcl: Optional[str]=None, runfunc: Callable=None):
        self._stage_type: StageType = stage_type
        self.fcl = fcl
        self.run_dir = None
        self.runfunc = runfunc

        self._input_files = None
        self._output_files = None
        self._ancestors = {}

    @property
    def stage_type(self) -> StageType:
        return self._stage_type

    @property
    def output_files(self) -> List[str]:
        if self._output_files is None:
            self.run()
        return self._output_files

    @property
    def ancestors(self) -> Dict:
        return self._ancestors

    def complete(self) -> bool:
        return self._output_files is not None

    def add_input_file(self, file) -> None:
        if self._input_files is None:
            self._input_files = [file]
        else:
            self._input_files.append(file)

    def run(self) -> None:
        """Produces the output file for this stage."""
        if self.fcl is None:
            raise NoFclFileException(f'Attempt to run stage {self._stage_type} with no fcl provided and no default')

        if self._stage_type in [StageType.GEN]:
            # these stages have no input files, do nothing for now
            pass
        else:
            if self._input_files is None:
                raise NoInputFileException(f'Tried to run stage of type {self._stage_type} which requires at least one input file, but it was not set.')

        self._output_files = self.runfunc(self.fcl, self._input_files, self.run_dir)

    def clean(self) -> None:
        """Delete the output file on disk."""
        self._output_files = None

    def add_ancestors(self, stages: List) -> None:
        """Add a list of known prior stages (ancestors) to this one."""
        for s in stages:
            try: 
                self._ancestors[s.stage_type].append(s)
            except KeyError:
                self._ancestors[s.stage_type] = [s]


class Workflow:
    """
    Collection of stages and order to run the stages
    fills in the gaps between inputs and outputs

    """

    @staticmethod
    def default_runfunc(fcl, input_files, output_dir) -> List[Path]:
        """Default function called when each stage is run."""
        input_file_arg_str = ''
        if input_files is not None:
            input_file_arg_str = \
                ' '.join([f'-s {str(file)}' for file in input_files])

        output_filename = os.path.basename(fcl).replace(".fcl", ".root")
        output_file = output_dir / Path(output_filename)
        output_file_arg_str = f'--output {str(output_file)}'
        print(f'lar -c {fcl} {input_file_arg_str} {output_file_arg_str}')
        return [output_file]

    def __init__(self, stage_order: List[StageType], default_fcls: Dict, run_dir: Path=Path(), runfunc: Callable=None):
        self._stage_order = stage_order

        self._default_fcls = {}
        for k, v in default_fcls.items():
            if not isinstance(k, StageType):
                self._default_fcls[StageType.from_str(k)] = v
            else:
                self._default_fcls[k] = v


        self._stages = []
        self._run_dir = run_dir
        self._default_runfunc = runfunc
        if self._default_runfunc is None:
            self._default_runfunc = Workflow.default_runfunc

    def add_final_stage(self, stage: Stage):
        """Add the final stage to a workflow."""
        self._stages.append(stage)

    def run(self):
        """Run the workflow by individually running the added stages."""
        for s in self._stages:
            self.run_stage(s)

    def run_stage(self, stage: Stage):
        """Run an individual stage, recursing to parent stages as necessary."""

        # fill any un-specified components with workflow defaults
        if stage.fcl is None:
            stage.fcl = self._default_fcls[stage.stage_type]

        if stage.run_dir is None:
            stage.run_dir = self._run_dir
        if stage.runfunc is None:
            stage.runfunc = self._default_runfunc

        # some stage types should not have any parents
        if stage.stage_type in [StageType.DECODE, StageType.SCRUB, StageType.GEN]:
            stage.run()
            return

        # stages with parents
        try:
            stage_idx = self._stage_order.index(stage.stage_type)
        except ValueError:
            # also OK to use strings
            stage_idx = self._stage_order.index(stage.stage_type.value)
        parent_type = self._stage_order[stage_idx - 1]
        # assume user wants us to build the ancestry map if the parent type doesn't exist
        if parent_type not in stage.ancestors:
            parent_stage = Stage(parent_type)

            # parent will inherit run dir and runfunc
            parent_stage.run_dir = stage.run_dir
            parent_stage.runfunc = stage.runfunc

            # copy over the known ancestors to this stage too
            for a in stage.ancestors.values():
                parent_stage.add_ancestors(a)

            stage.add_ancestors([parent_stage])

        # check to make sure the parents have been run before running this stage
        for a in stage.ancestors[parent_type]:
            self.run_stage(a)
            for f in a.output_files:
                stage.add_input_file(f)

        # now this stage is good to go!
        stage.run()


if __name__ == '__main__':
    # describe what you have and what you want
    # below we state that we want some reco2 files. The inputs are scrubbed
    # reco1 files, and they should be processed with a special fcl at the g4
    # stage. We define the workflow stage order, and provide some defaults

    stage_order = (StageType.SCRUB, StageType.G4, StageType.DETSIM, StageType.RECO1, StageType.RECO2)
    # stage_order = (StageType.GEN, StageType.G4, StageType.DETSIM, StageType.RECO1, StageType.RECO2, StageType.CAF)
    default_fcls = {
        StageType.GEN: 'gen.fcl',
        StageType.SCRUB: 'scrub.fcl',
        StageType.G4: 'g4.fcl',
        StageType.DETSIM: 'detsim.fcl',
        StageType.RECO1: 'reco1.fcl',
        StageType.RECO2: 'reco2.fcl',
        StageType.CAF: 'caf.fcl',
    }
    wf = Workflow(stage_order, default_fcls, run_dir='../')

    for i in range(10):
        # define your inputs
        # s1 = Stage(StageType.SCRUB)
        # s1.add_input_file(f'reco1_{i:02d}.root')
        # s2 = Stage(StageType.G4, fcl='override.fcl')
        # s2.add_ancestors([s1])

        # assign them to your final stage. OK to leave gaps
        s2a = Stage(StageType.RECO2)
        s2b = Stage(StageType.RECO2)
        s3 = Stage(StageType.CAF)
        s3.add_ancestors([s2a, s2b])

        # add final stage to the workflow. Workflow will fill in any gaps using
        # the order and default fcls
        wf.add_final_stage(s3)

    # run all stages
    wf.run()
