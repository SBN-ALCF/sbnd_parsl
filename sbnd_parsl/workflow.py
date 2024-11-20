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
import sys
import json
import time
from types import MethodType
import itertools
import pathlib
from collections import deque

from enum import Enum, auto
from pathlib import Path
from typing import List, Dict, Optional, Callable

import parsl
from sbnd_parsl.utils import create_default_useropts, create_parsl_config


class NoInputFileException(Exception):
    pass

class NoFclFileException(Exception):
    pass

class WorkflowException(Exception):
    pass

class StageAncestorException(Exception):
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
    SUPER = 'super'

    @staticmethod
    def from_str(text: str):
        try:
            return StageType[text.upper()]
        except KeyError:
            return StageType.Empty


class Stage:
    def __init__(self, stage_type: StageType, fcl: Optional[str]=None,
                 runfunc: Optional[Callable]=None, stage_order: Optional[List[StageType]]=None):
        self._stage_type: StageType = stage_type
        self.fcl = fcl
        self.run_dir = None
        self.runfunc = None

        # override for custom stage order, otherwise this is set by the Workflow
        self.stage_order = stage_order

        self._complete = False
        self._input_files = None
        self._output_files = None
        # self._ancestors = {}
        self._parents_iterators = deque()
        # print(f'constructed stagetype {self._stage_type}')

    # def __del__(self):
    #     print(f'deleted stagetype {self._stage_type}')

    @property
    def stage_type(self) -> StageType:
        return self._stage_type

    @property
    def output_files(self) -> List:
        if self._output_files is None:
            print(f'Warning: Running stage of type {self.stage_type} via output_files method')
            self.run()
        return self._output_files

    @property
    def input_files(self) -> List:
        return self._input_files

    @property
    def parent_type(self) -> Optional[StageType]:
        """Return the stage type of the stage before this one."""
        parent_idx = self.stage_order.index(self.stage_type) - 1
        if parent_idx < 0:
            raise StageAncestorException(f'No ancestor for {self.stage_type} in list {self.stage_order}')
        return self.stage_order[parent_idx]

    # @property
    # def ancestors(self) -> Dict:
    #     return self._ancestors

    # @ancestors.setter
    # def ancestors(self, ancestors: Dict) -> None:
    #     self._ancestors = ancestors

    @property
    def parents(self) -> List:
        # return self._parents
        return len(self._parents_iterators) > 0

    @property
    def complete(self) -> bool:
        return self._complete

    def add_input_file(self, file) -> None:
        if self._input_files is None:
            self._input_files = [file]
        else:
            self._input_files.append(file)

    def run(self, rerun: bool=False) -> None:
        """Produces the output file for this stage."""
        # if calling run method directly instead of asking for output files,
        # must specify rerun option to avoid calling runfunc multiple times!
        if self._output_files is not None and not rerun:
            return

        if self._stage_type == StageType.SUPER:
            print('Congratulations, you ran all the stages!') 
            self._complete = True
            return

        # make sure we delete references to the parents once they are run
        if self.parents:
            raise RuntimeError(f'Attempt to run stage {self._stage_type} while it still holds references to its parents')


        if self.fcl is None:
            raise NoFclFileException(f'Attempt to run stage {self._stage_type} with no fcl provided and no default')

        if self._stage_type in [StageType.GEN]:
            # these stages have no input files, do nothing for now
            pass
        else:
            if self._input_files is None:
                raise NoInputFileException(f'Tried to run stage of type {self._stage_type} which requires at least one input file, but it was not set.')

        # bind the func to this object with MethodType so self resolves as if
        # it were a member function
        self._complete = True
        func = MethodType(self.runfunc, self)
        self._output_files = func(self.fcl, self._input_files, self.run_dir)

    # def clean(self) -> None:
    #     """Delete the output file on disk."""
    #     self._output_files = None

    def add_parents(self, stages: List, fcls: Optional[Dict]=None) -> None:
        """Add a list of known prior stages to this one."""
        if not isinstance(stages, list):
            stages = [stages]

        for s in stages:
            # TODO error if we try to add a parent of the wrong type; this
            # requires knowing stage order but user code lets the workflow fill
            # it in, so may be None
            # if s.stage_type != self.parent_type:
            #     raise StageAncestorException(f"Tried to add stage of type {s.stage_type} as a parent to a stage with type {stage.stage_type}")
            # self._parents.append(s)
            if s.run_dir is None:
                s.run_dir = self.run_dir
            if s.runfunc is None:
                s.runfunc = self.runfunc
            if s.stage_order is None:
                s.stage_order = self.stage_order
            if s.fcl is None:
                s.fcl = fcls[s.stage_type]
            self._parents_iterators.append((s, run_stage(s, fcls)))

    def get_next_task(self, mode='cycle'):
        """
        Run the workflow by individually running the added stages. Can either
        cycle through end stages (grab one task from each stage at a time) or
        not (grab all tasks from first stage before continuing)
        """
        while self._parents_iterators:
            # remove 
            parent, iterator = self._parents_iterators.popleft()
            try:
                next(iterator)
                # put back if not done. Either at the back of the deque or in place
                if mode == 'cycle':
                    self._parents_iterators.append((parent, iterator))
                else:
                    self._parents_iterators.appendleft((parent, iterator))
                yield
            except StopIteration:
                for f in parent.output_files:
                    self.add_input_file(f)
        return


def run_stage(stage: Stage, fcls: Optional[Dict]=None):
    """Run an individual stage, recursing to parent stages as necessary.
    Note that this is a generator: Call next(Workflow.run_stage(stage)) to
    get the next task."""

    if stage.complete:
        return

    # some stage types should not have any parents
    if stage.stage_type == StageType.GEN:
        stage.run()
        yield

    # if we have our inputs already, can run
    if stage.input_files is not None and not stage.parents:
        stage.run()
        return

    # this raises StopIteration
    if stage.complete:
        return

    if not stage.parents:
        # no inputs and no parents, create a parent stage
        parent_stage = Stage(stage.parent_type)
        stage.add_parents(parent_stage, fcls)

    # run parent stages
    while stage.parents:
        try:
            next(stage.get_next_task())
            yield
        except StopIteration:
            pass

    stage.run()
    yield
        

class Workflow:
    """
    Collection of stages and order to run the stages
    fills in the gaps between inputs and outputs
    """

    @staticmethod
    def default_runfunc(stage_self, fcl, input_files, output_dir) -> List[Path]:
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

    def __init__(self, stage_order: List[StageType], default_fcls: Optional[Dict]=None, run_dir: Path=Path(), runfunc: Optional[Callable]=None):
        self._stage_order = stage_order

        self.default_fcls = {}
        if default_fcls is not None:
            for k, v in default_fcls.items():
                if not isinstance(k, StageType):
                    self.default_fcls[StageType.from_str(k)] = v
                else:
                    self.default_fcls[k] = v

        self._run_dir = run_dir
        self._default_runfunc = runfunc
        if self._default_runfunc is None:
            self._default_runfunc = Workflow.default_runfunc
        self._stage = Stage(StageType.SUPER)
        self._stage.run_dir = self._run_dir
        self._stage.runfunc = self._default_runfunc
        self._stage.stage_order = self._stage_order

    def add_final_stage(self, stage: Stage):
        """Add the final stage to the workflow as a generator expression."""
        self._stage.add_parents(stage, self.default_fcls)

    def get_next_task(self, mode='cycle'):
        """
        Run the workflow by individually running the added stages. Can either
        cycle through end stages (grab one task from each stage at a time) or
        not (grab all tasks from first stage before continuing)
        """
        try:
            next(run_stage(self._stage, self.default_fcls))
            yield
        except StopIteration:
            pass


class WorkflowExecutor: 
    """Class to wrap settings and workflow objects, and manage task submission."""
    def __init__(self, settings: json):
        self.larsoft_opts = settings['larsoft']

        self.run_opts = settings['run']
        self.output_dir = Path(self.run_opts['output'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_futures = self.run_opts['max_futures']

        self.fcl_dir = Path(self.run_opts['fclpath'])
        self.fcls = settings['fcls']

        self.futures = []

        # workflow
        self.workflow_opts = settings['workflow']
        self.workflow = None


    def execute(self):
        """
        Run many copies of a single workflow. Workflow.run() method should
        yield each time a stage is executed for efficient task submission,
        i.e., we'll get the first tasks from each workflow first, instead of
        all the tasks from workflow 0, then all the tasks from workflow 1, etc.
        Use itertools.cycle() to keep looping over all workflows until all
        tasks are submitted.
        """

        nsubruns = self.run_opts['nsubruns']

        # generator madness...
        # we'll cycle over indices until all tasks are submitted, taking one
        # task from each subrun at a time. This ensures we get the parsl
        # futures in the "correct" order: Futures without dependencies first,
        # then dependencies later
        idx_cycle = itertools.cycle(range(nsubruns))
        wfs = [None for _ in range(nsubruns)]
        skip_idx = set()

        while len(skip_idx) < nsubruns:
            idx = next(idx_cycle)
            if idx in skip_idx:
                continue

            wf = wfs[idx]
            if wf is None:
                wf = self.setup_single_workflow(idx)
                wfs[idx] = wf

            # rate-limit the number of concurrent futures to avoid using too
            # much memory on login nodes
            while len(self.futures) > self.max_futures:
                self.get_task_results()
                # still too many?
                if len(self.futures) > self.max_futures:
                    print(f'Waiting: Current futures={len(self.futures)}')
                    time.sleep(10)

            try:
                next(wf.get_next_task())
            except StopIteration:
                skip_idx.add(idx)

                # let garbage collection happen
                wfs[idx] = None
        
        while len(self.futures) > 0:
            self.get_task_results()
            time.sleep(10)


    def get_task_results(self):
        for f in self.futures:
            if not f.done():
                continue
            try:
                print(f'[SUCCESS] task {f.tid} {f.filepath} {f.result()}')
            except Exception as e:
                print(f'[FAILED] task {f.tid} {f.filepath}')
            self.futures.remove(f)


    def setup_single_workflow(self, iteration: int):
        pass


if __name__ == '__main__':
    # TODO demo
    pass
