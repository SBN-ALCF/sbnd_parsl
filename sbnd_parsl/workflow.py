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
        self._parents = []
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
            return None
        return self.stage_order[parent_idx]

    # @property
    # def ancestors(self) -> Dict:
    #     return self._ancestors

    # @ancestors.setter
    # def ancestors(self, ancestors: Dict) -> None:
    #     self._ancestors = ancestors

    @property
    def parents(self) -> List:
        return self._parents

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

        # ideally will delete references to the parents once they are run
        if self._parents:
            print('Warning: Running a stage while it still has references to its parent stage(s).')


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

    # def add_ancestors(self, stages: List) -> None:
    #     """Add a list of known prior stages (ancestors) to this one."""
    #     if self._stage_type == StageType.GEN:
    #         raise StageAncestorException("Tried to add ancestor to a GEN stage")

    #     if not isinstance(stages, list):
    #         stages = [stages]

    #     for s in stages:
    #         try: 
    #             self._ancestors[s.stage_type].append(s)
    #         except KeyError:
    #             self._ancestors[s.stage_type] = [s]

    def add_parents(self, stages: List) -> None:
        """Add a list of known prior stages to this one."""
        if not isinstance(stages, list):
            stages = [stages]

        for s in stages:
            # if s.stage_type != self.parent_type:
            #     raise StageAncestorException(f"Tried to add stage of type {s.stage_type} as a parent to a stage with type {stage.stage_type}")
            try: 
                self._parents.append(s)
            except KeyError:
                self._parents = [s]


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

        self._default_fcls = {}
        if default_fcls is not None:
            for k, v in default_fcls.items():
                if not isinstance(k, StageType):
                    self._default_fcls[StageType.from_str(k)] = v
                else:
                    self._default_fcls[k] = v

        # self._stages = []
        self._run_dir = run_dir
        self._default_runfunc = runfunc
        if self._default_runfunc is None:
            self._default_runfunc = Workflow.default_runfunc
        self._iterators = deque()

    def add_final_stage(self, stage: Stage):
        """Add the final stage to a workflow."""
        self._iterators.append(self.run_stage(stage))

    def get_next_task(self, mode='cycle'):
        """
        Run the workflow by individually running the added stages. Can either
        cycle through end stages (grab one task from each stage at a time) or
        not (grab all tasks from first stage before continuing)
        """
        while self._iterators:
            # remove 
            iterator = self._iterators.popleft()
            try:
                next(iterator)
                # put back if not done. Either at the back of the deque or in place
                if mode == 'cycle':
                    self._iterators.append(iterator)
                else:
                    self._iterators.appendleft(iterator)
                yield
            except StopIteration:
                print(f'no more tasks for workflow stage')
        return

    def run_stage(self, stage: Stage):
        """Run an individual stage, recursing to parent stages as necessary.
        Note that this is a generator: Call next(Workflow.run_stage(stage)) to
        get the next task."""
        # this raises StopIteration
        if stage.complete:
            return

        # fill any un-specified components with workflow defaults
        if stage.fcl is None:
            try:
                stage.fcl = self._default_fcls[stage.stage_type]
            except KeyError:
                raise NoFclFileException(f'Attempt to run stage {stage.stage_type} with no fcl provided and no default')

        if stage.run_dir is None:
            stage.run_dir = self._run_dir
        if stage.runfunc is None:
            stage.runfunc = self._default_runfunc

        # some stage types should not have any parents
        if stage.stage_type == StageType.GEN:
            stage.run()
            yield

        # if we have our inputs already, can run
        if stage.input_files is not None:
            stage.run()
            yield

        # no inputs, let's try to get them
        # Use default order if not already set
        if stage.stage_order is None:
            stage.stage_order = self._stage_order
        
        if not stage.parents:
            # no inputs and no parents, let's create a parent stage
            parent_stage = Stage(stage.parent_type)
            # print(f'creating stage of type {stage.parent_type} from {stage.stage_type}')

            # parent will inherit run dir and runfunc and stage order
            parent_stage.run_dir = stage.run_dir
            parent_stage.runfunc = stage.runfunc
            parent_stage.stage_order = stage.stage_order
            stage.add_parents(parent_stage)

        for parent in stage.parents:
            # if parent was already set but not initialized, initialize it here
            if parent.stage_order is None:
                parent.stage_order = stage.stage_order
            if parent.run_dir is None:
                parent.run_dir = stage.run_dir
            if parent.runfunc is None:
                parent.runfunc = stage.runfunc

            # run parent's generator until we're done
            while not parent.complete:
                next(self.run_stage(parent))
                yield
            
        while stage.parents:
            parent = stage.parents.pop()
            for f in parent.output_files:
                stage.add_input_file(f)

        stage.run()
        yield
        


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

        # parsl
        self.user_opts = create_default_useropts()
        self.user_opts.update(settings['queue'])

        self.user_opts["run_dir"] = str(self.output_dir / 'runinfo')
        print(self.user_opts)

        self.config = create_parsl_config(self.user_opts)
        print(self.config)

        self.futures = []
        parsl.clear()
        parsl.load(self.config)

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
        wfs = []
        skip_idx = set()

        while len(skip_idx) < nsubruns:
            idx = next(idx_cycle)
            if idx in skip_idx:
                continue

            try:
                wf = wfs[idx]
            except IndexError:
                wf = self.setup_single_workflow(idx)
                wfs.append(wf)

            # rate-limit the number of concurrent futures to avoid using too
            # much memory on login nodes
            while len(self.futures) > self.max_futures:
                self.get_task_results()
                print(f'Waiting: Current futures={len(self.futures)}')
                time.sleep(10)

            try:
                next(wf.get_next_task())
            except StopIteration:
                skip_idx.add(idx)
        
        while len(self.futures) > 0:
            self.get_task_results()
            time.sleep(10)


    def get_task_results(self):
        done_futures = [f for f in self.futures if f.done()]
        for f in done_futures:
            try:
                print(f'[SUCCESS] task {f.tid} {f.filepath} {f.result()}')
            except Exception as e:
                print(f'[FAILED] task {f.tid} {f.filepath}')
            self.futures.remove(f)

    def setup_single_workflow(self, iteration: int):
        pass


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




if __name__ == '__main__':
    stage_order = (StageType.SCRUB, StageType.G4, StageType.DETSIM, \
                   StageType.RECO1, StageType.RECO2, StageType.CAF)
    default_fcls = {
        StageType.SCRUB: 'scrub.fcl',
        StageType.G4: 'g4.fcl',
        StageType.DETSIM: 'detsim.fcl',
        StageType.RECO1: 'reco1.fcl',
        StageType.RECO2: 'reco2.fcl',
        StageType.CAF: 'caf.fcl',
    }

    wf = Workflow(stage_order, default_fcls, run_dir='../')

    variations = ['g4_var1.fcl', 'g4_var2.fcl']
    s1 = Stage(StageType.SCRUB)
    s1.add_input_file('reco1_file.root')

    for var_fcl in variations:
        s2 = Stage(StageType.G4)
        s2.fcl = var_fcl
        s2.add_ancestors(s1)

        s3 = Stage(StageType.RECO2)
        s3.add_ancestors(s2)

        wf.add_final_stage(s3)

    wf.run()
