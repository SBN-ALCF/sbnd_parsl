#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import json
import pathlib
from typing import Dict, List

import parsl
from parsl.app.app import bash_app
from parsl.data_provider.files import File

from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE


@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """ Return formatted bash script which produces each future when executed """
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1],
        **larsoft_opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def generate_mc_sample(workdir: pathlib.Path, larsoft_opts: Dict, fcls: List, mg: MetadataGenerator):
    """
    Create a future for each fcl output required to produce a fully-simulated &
    reconstructed MC file
    """
    input_file = None
    last_future = None
    workdir.mkdir(parents=True, exist_ok=True)

    for i, fcl in enumerate(fcls):
        mg_cmd = f'PATH={larsoft_opts["larsoft_top"]}/sbndutil/{larsoft_opts["version"]}/bin:$PATH {mg.run_cmd(os.path.basename(fcl), check_exists=False)}'
        output = os.path.basename(fcl).replace(".fcl", ".root")
        output_file = workdir / pathlib.Path(output)
        this_future = fcl_future(
            workdir = str(workdir),
            stdout = str(workdir / pathlib.Path(f"larStage{i}.out")),
            stderr = str(workdir / pathlib.Path(f"larStage{i}.err")),
            template = SINGLE_FCL_TEMPLATE,
            larsoft_opts = larsoft_opts,
            inputs=[fcl, input_file],
            outputs=[File(str(output_file))],
            pre_job_hook = mg_cmd
        )
        input_file = this_future.outputs[0]
        last_future = this_future.outputs[0]

    # input file is set to the last future of this job
    return last_future


def generate_caf(workdir: pathlib.Path, larsoft_opts: Dict, fcl, inputs: List, post_job_hook: str, mg: MetadataGenerator):
    """
    Create a future for a caf file. The inputs are the ouputs from the final
    stage of multiple MC futures.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    caf_input_arg = ' '.join([f'-s {fname.filepath}' for fname in inputs])
    output = f"cafmakerjob_sbnd_sce_genie_and_fluxwgt.root"
    output_file = workdir / pathlib.Path(output)
    future_inputs = [fcl, caf_input_arg] + inputs
    mg_cmd = f'PATH={larsoft_opts["larsoft_top"]}/sbndutil/{larsoft_opts["version"]}/bin:$PATH {mg.run_cmd(os.path.basename(fcl), check_exists=False)}'

    opts = larsoft_opts.copy()
    opts['lar_args'] = "--sam-data-tier caf"

    # use post_job_hook to clean up
    this_future = fcl_future(
        workdir = str(workdir),
        stdout = str(workdir / pathlib.Path("cafStage.out")),
        stderr = str(workdir / pathlib.Path("cafStage.err")),
        template = CAF_TEMPLATE,
        larsoft_opts = opts,
        inputs = future_inputs,
        outputs = [File(str(output_file))],
        pre_job_hook = mg_cmd,
        post_job_hook = post_job_hook
    )

    return this_future.outputs[0]


def cleanup_caf_inputs(subruns_per_caf: int, full_keep_frac: float, inputs: List, fcls: List):
    ''' make a hook to delete all but a certain fraction of the inputs '''
    n_remove = int((1.0 - full_keep_frac) * subruns_per_caf)

    # remove all the ROOT files from the first n_remove MC files, except reco1!
    # create an empty file in its place so that subsequent runs don't try to
    # re-make it
    mc_rm_filenames = [fcl.replace('.fcl', '.root') for fcl in fcls if not "reco1" in fcl]
    rm_strs = []
    for iput in inputs[:n_remove]:
        for mc_fname in mc_rm_filenames:
            rm_file = pathlib.Path(iput.filepath).parent / mc_fname
            rm_strs.append(f'rm -f {rm_file}')
            rm_strs.append(f'touch {rm_file}')

    return '\n'.join(rm_strs)


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    return prefix / pathlib.Path(f"{100*(subrun//100):04d}") / pathlib.Path(f"subrun_{subrun:04d}")


def main(settings: json):
    output_dir = pathlib.Path(settings['run']['output'])
    output_dir.mkdir(parents=True, exist_ok=True)

    fcl_dir = pathlib.Path(settings['run']['fclpath'])
    nsubruns = settings['run']['nsubruns']

    user_opts = create_default_useropts()
    user_opts.update(settings['queue'])

    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"
    print(user_opts)

    config = create_parsl_config(user_opts)
    print(config)

    larsoft_opts = settings['larsoft']
    fcls = settings['fcls']
    workflow_opts = settings['workflow']
    mc_meta = MetadataGenerator(settings['metadata'], fcls)

    futures = []
    parsl.clear()
    parsl.load(config)
    
    # unpack the fcl list to remove stage name
    mc_fcls = [fcl for stage, fcl in fcls.items() if stage != 'caf']
    # create futures for MC files
    for i in range(nsubruns):
        this_out_dir = get_subrun_dir(output_dir, i)
        futures.append(
            generate_mc_sample(
                workdir=this_out_dir, 
                larsoft_opts=larsoft_opts,
                fcls=[str(fcl_dir / pathlib.Path(fcl)) for fcl in mc_fcls],
                mg=mc_meta
            )
        )

    subruns_per_caf = workflow_opts['subruns_per_caf']
    batches = [futures[i:i + subruns_per_caf] for i in range(0, len(futures), subruns_per_caf)]

    for b in batches:
        files_str = ''.join([f.filepath for f in b])
        caf_name = hash_name(files_str)

        rm_hook = cleanup_caf_inputs(workflow_opts['subruns_per_caf'], workflow_opts['full_keep_fraction'], b, mc_fcls)
        this_out_dir = pathlib.Path(output_dir, 'caf', caf_name)
        futures.append(
            generate_caf(
                workdir=this_out_dir,
                larsoft_opts=larsoft_opts,
                fcl=str(fcl_dir / pathlib.Path(fcls['caf'])),
                inputs=b,
                post_job_hook=rm_hook,
                mg=mc_meta
            )
        )
        
    print('\n'.join([f.result().filepath for f in futures]))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
