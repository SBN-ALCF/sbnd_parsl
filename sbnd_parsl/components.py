import sys, os
import pathlib
import functools
import itertools
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, DefaultStageTypes

from sbnd_parsl.utils import hash_name



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




def mc_runfunc(self, fcl, inputs, run_dir, template, meta, executor, label='', nskip=0, last_file=None):
    """
    Method bound to each Stage object and run during workflow execution. Use
    this runfunc for generating MC events.

    Supported features:
     - Metadata injection
     - Combined stages: Forward command to the next stage, so both stages run in 1 task
     - wait for last_file (even if no dependency)
     - skip events
    """

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
            str(self.stage_type.name), '-', label, '-',
            hash_name(os.path.basename(fcl) + executor.name_salt + str(executor.lar_run_counter)),
            ".root"
        ])
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.flat.caf.root'

    output_dir = executor.output_dir / label / self.stage_type.name / f'{executor.lar_run_counter // 1000:06d}' \
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

    # stages after gen: don't limit number of events; use all events from all input files
    nevts = -1
    if self.stage_type == DefaultStageTypes.GEN:
        nevts = executor.larsoft_opts["nevts"]
    cmd = f'mkdir -p {run_dir} && cd {run_dir} && lar -c {fcl} {input_file_arg_str} {output_file_arg_str} --nevts={nevts} --nskip={nskip}'

    if parent_cmd != '':
        cmd = ' && '.join([parent_cmd, cmd])

    if self.combine:
        # don't submit work, just forward commands to the next task
        return [[output_file], cmd]

    # metadata & fcl manipulation
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
    mg_cmd += meta.run_cmd(
        output_filename + '.json', os.path.basename(fcl), check_exists=False)

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = template,
        cmd = cmd,
        larsoft_opts = executor.larsoft_opts,
        inputs = input_arg,
        outputs = [File(str(output_file))],
        pre_job_hook = mg_cmd
    )

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return [future.outputs, '']
