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
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name
from sbnd_parsl.dfk_hacks import apply_hacks


@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, cmd, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1],
        cmd=cmd,
        **larsoft_opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def runfunc(self, fcl, inputs, run_dir, var_name, executor, last_file=None, nevts=-1, nskip=0):
    """Method bound to each Stage object and run during workflow execution."""

    run_dir.mkdir(parents=True, exist_ok=True)

    # only needed for stages that have no inputs
    if self.stage_type != DefaultStageTypes.GEN:
        first_file_name = None
        if not isinstance(inputs[0][0], parsl.app.futures.DataFuture):
            first_file_name = inputs[0][0].name
        else:
            first_file_name = inputs[0][0].filename

    if self.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        output_filename = ''.join([
            str(self.stage_type.name), '-', var_name, '-',
            hash_name(os.path.basename(fcl) + executor.name_salt + str(executor.lar_run_counter)),
            ".root"
        ])
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.flat.caf.root'

    output_dir = executor.output_dir / var_name / self.stage_type.name / f'{executor.lar_run_counter // 1000:06d}' \
            / f'{executor.lar_run_counter // 100:06d}'
    output_file = output_dir / output_filename
    executor.lar_run_counter += 1

    if output_file.is_file():
        # print(f'Skipping {output_file}, already exists')
        return [[output_file], '']

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file_arg_str = ''
    if self.stage_type != DefaultStageTypes.CAF:
        output_file_arg_str = f'--output {str(output_file)}'

    input_file_arg_str = ''
    parent_cmd = ''
    dummy_input = None
    if last_file is not None:
        dummy_input = last_file[0][0]
    input_arg = [str(fcl), dummy_input]

    if inputs is not None:
        input_files = list(itertools.chain.from_iterable(inputs[0::2]))
        parent_cmds = '&&'.join(inputs[1::2])
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' if not isinstance(file, parsl.app.futures.DataFuture) else f'-s {str(file.filepath)}' for file in input_files])
        input_arg += [str(f) if not isinstance(f, parsl.app.futures.DataFuture) else f for f in input_files]

    nevts = executor.larsoft_opts["nevts"]
    # caf stage: don't limit number of events; use all events from all input files
    if self.stage_type == DefaultStageTypes.CAF:
        nevts = -1
    cmd = f'mkdir -p {run_dir} && cd {run_dir} && lar -c {fcl} {input_file_arg_str} {output_file_arg_str} --nevts={nevts}'

    if parent_cmd != '':
        cmd = ' && '.join([parent_cmd, cmd])

    if self.combine:
        # don't submit work, just forward commands to the next task
        return [[output_file], cmd]

    mg_cmd = ''
    if self.stage_type == DefaultStageTypes.GEN:
        executor.unique_run_number += 1
        run_number = 1 + (executor.unique_run_number // 100)
        subrun_number = executor.unique_run_number % 100
        mg_cmd = '\n'.join([mg_cmd,
            f'echo "source.firstRun: {run_number}" >> {os.path.basename(fcl)}',
            f'echo "source.firstSubRun: {subrun_number}" >> {os.path.basename(fcl)}',
            f'''echo "physics.producers.generator.FluxSearchPaths: \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFiles/\\"" >> {os.path.basename(fcl)}''',
            f'''echo "physics.producers.corsika.ShowerInputFiles: [ \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\\" ]" >> {os.path.basename(fcl)}''',
            f'''echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> {os.path.basename(fcl)}''',
        ])
        mg_cmd += '\n'
    mg_cmd += executor.meta[var_name].run_cmd(
        output_filename + '.json', os.path.basename(fcl), check_exists=False)

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = CMD_TEMPLATE_CONTAINER,
        cmd = cmd,
        larsoft_opts = executor.larsoft_opts,
        inputs = input_arg,
        outputs = [File(str(output_file))],
        pre_job_hook = mg_cmd
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return [future.outputs, '']


class Reco2FromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.unique_run_number = 0
        self.lar_run_counter = 0
        self.name_salt = str(settings['run']['seed']) + str(self.output_dir)

        self.default_fcls = self.fcls['cv']
        self.variations = [key for key in self.fcls.keys() if key != 'cv']

        # fill in the default fcls for each variation
        for var in self.variations:
            for key, val in self.default_fcls.items():
                if key in self.fcls[var]:
                    continue
                self.fcls[var][key] = val

        self.meta = {
            key: MetadataGenerator(
                settings['metadata'], fcls, defer_check=True) \
            for key, fcls in self.fcls.items()
        }
        self.subruns_per_caf = settings['workflow']['subruns_per_caf']

        # the default (CV) stage order
        self.stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM, \
                            DefaultStageTypes.RECO1, DefaultStageTypes.RECO2, DefaultStageTypes.CAF]

        # when we construct the scrub stage, this will let the workflow know to
        # treat a detsim stage as the parent of scrub, instead of a fixed input file
        self.scrub_stage_order = [DefaultStageTypes.DETSIM, DefaultStageTypes.SCRUB]

        # the stage order for the variations
        self.var_stage_order = [DefaultStageTypes.SCRUB, DefaultStageTypes.DETSIM, \
                                DefaultStageTypes.RECO1, DefaultStageTypes.RECO2, DefaultStageTypes.CAF]

    def setup_single_workflow(self, iteration: int, input_files=None, last_file=None):
        cv_dir = self.output_dir / 'cv'
        cv_runfunc = functools.partial(runfunc, var_name='cv', executor=self)

        var_dirs = {}
        var_runfuncs = {}
        for var in self.variations:
            var_dirs[var] = self.output_dir / var
            var_runfuncs[var] = functools.partial(runfunc, var_name=var, executor=self)

        workflow = Workflow(self.stage_order, default_fcls=self.default_fcls)
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = get_caf_dir(cv_dir, iteration)
        s.runfunc = cv_runfunc
        workflow.add_final_stage(s)

        # CAF for each variation
        var_caf_stages = {}
        for var in self.variations:
            svar = Stage(DefaultStageTypes.CAF, stage_order=self.var_stage_order)
            svar.run_dir = get_caf_dir(var_dirs[var], iteration)
            svar.runfunc = var_runfuncs[var]
            var_caf_stages[var] = svar
            workflow.add_final_stage(svar)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            scv_reco2 = Stage(DefaultStageTypes.RECO2)
            scv_reco2.run_dir = get_subrun_dir(cv_dir, inst)

            scv_reco1 = Stage(DefaultStageTypes.RECO1)
            scv_detsim = Stage(DefaultStageTypes.DETSIM)

            scv_scrub = Stage(DefaultStageTypes.SCRUB, stage_order=self.scrub_stage_order)
            scv_scrub.run_dir = get_subrun_dir(cv_dir, inst)
            scv_scrub.runfunc = cv_runfunc

            s.add_parents(scv_reco2, workflow.default_fcls)
            scv_reco2.add_parents(scv_reco1, workflow.default_fcls)
            scv_reco1.add_parents(scv_detsim, workflow.default_fcls)
            scv_scrub.add_parents(scv_detsim, workflow.default_fcls)

            for var, svar in var_caf_stages.items():
                svar_reco2 = Stage(DefaultStageTypes.RECO2, stage_order=self.var_stage_order)
                svar_reco2.fcl = self.fcls[var]['reco2']
                svar_reco2.run_dir = get_subrun_dir(var_dirs[var], inst)

                svar_reco1 = Stage(DefaultStageTypes.RECO1, stage_order=self.var_stage_order)
                svar_reco1.fcl = self.fcls[var]['reco1']

                svar_detsim = Stage(DefaultStageTypes.DETSIM, stage_order=self.var_stage_order)
                svar_detsim.fcl = self.fcls[var]['detsim']

                # svar_g4 = Stage(DefaultStageTypes.G4, stage_order=self.var_stage_order)
                # svar_g4.fcl = self.fcls[var]['g4']

                svar.add_parents(svar_reco2, self.fcls[var])
                svar_reco2.add_parents(svar_reco1, self.fcls[var])
                svar_reco1.add_parents(svar_detsim, self.fcls[var])
                svar_detsim.add_parents(scv_scrub, self.fcls[var])
                # svar_g4.add_parents(scv_scrub, self.fcls[var])

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
