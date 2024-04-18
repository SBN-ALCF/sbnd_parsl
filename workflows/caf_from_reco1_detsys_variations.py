#!/usr/bin/env python

# Variation of detsys workflow to re-process reco1 files

import sys, os
import json
import pathlib
from typing import Dict, List

import numpy as np

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


def generate_from_reco1(workdir: pathlib.Path, reco1_filename: str, larsoft_opts: Dict, fcl_dir: pathlib.Path, fcls: Dict, mg: MetadataGenerator):
    """
    Create an MC sample from a reco1 file by scrub, then do all other fcl
    steps for each g4 variation
    """

    workdir.mkdir(parents=True, exist_ok=True)

    workdir.mkdir(parents=True, exist_ok=True)
    rest_fcls = [fcls[key] for key in ['reco2']] 
    fcl_offset = list(fcls.keys()).index('reco2')

    input_file = reco1_filename

    # use i + 1 so generator is still larStage0, g4 is still larStage1, etc.
    for i, fcl in enumerate(rest_fcls):
        mg_cmd = f'PATH={larsoft_opts["larsoft_top"]}/sbndutil/{larsoft_opts["version"]}/bin:$PATH {mg.run_cmd(os.path.basename(fcl), check_exists=False)}'
        output = os.path.basename(fcl).replace(".fcl", ".root")
        output_file = File(str(workdir / pathlib.Path(output)))
        this_future = fcl_future(
            workdir = str(workdir),
            stdout = str(workdir / pathlib.Path(f"larStage{fcl_offset + i}.out")),
            stderr = str(workdir / pathlib.Path(f"larStage{fcl_offset + i}.err")),
            template = SINGLE_FCL_TEMPLATE,
            larsoft_opts = larsoft_opts,
            inputs=[str(fcl_dir / pathlib.Path(fcl)), input_file],
            outputs=[output_file],
            pre_job_hook = mg_cmd
        )
        input_file = this_future.outputs[0]

    return input_file


def generate_caf(workdir: pathlib.Path, larsoft_opts: Dict, fcl, inputs: List, post_job_hook: str, mg: MetadataGenerator):
    """
    Create a future for a caf file. The inputs are the ouputs from the final
    stage of multiple MC futures.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    caf_input_arg = ' '.join([f'-s {fname.filepath}' for fname in inputs])
    output = f"reco2_sce.flat.caf.root"
    output_file = workdir / pathlib.Path(output)
    future_inputs = [fcl, caf_input_arg] + inputs
    mg_cmd = f'PATH={larsoft_opts["larsoft_top"]}/sbndutil/{larsoft_opts["version"]}/bin:$PATH {mg.run_cmd(os.path.basename(fcl), check_exists=False)}'

    opts = larsoft_opts.copy()
    opts['lar_args'] = "--sam-data-tier caf"

    this_future = fcl_future(
        workdir = str(workdir),
        stdout = str(workdir / pathlib.Path("cafStage.out")),
        stderr = str(workdir / pathlib.Path("cafStage.err")),
        template = CAF_TEMPLATE,
        larsoft_opts = opts,
        inputs = future_inputs,
        outputs = [File(str(output_file))],
        pre_job_hook=mg_cmd,
        post_job_hook=post_job_hook
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


def main(settings: json):
    input_dir = pathlib.Path(settings['workflow']['reco1_dir'])
    if not input_dir.is_dir() or not input_dir.exists():
        print(f"Directory {input_dir} is not a valid directory")
        sys.exit(1)

    output_dir = pathlib.Path(settings['run']['output'])
    output_dir.mkdir(parents=True, exist_ok=True)

    user_opts = create_default_useropts()
    user_opts.update(settings['queue'])
    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"
    print(user_opts)

    config = create_parsl_config(user_opts)
    print(config)

    larsoft_opts = settings['larsoft']
    workflow_opts = settings['workflow']

    parsl.clear()
    parsl.load(config)

    subruns_per_caf = workflow_opts['subruns_per_caf']
    full_keep_fraction = workflow_opts['subruns_per_caf']

    fcl_dir = pathlib.Path(settings['run']['fclpath'])
    fcls = settings['fcls']
    mc_meta = MetadataGenerator(settings['metadata'], fcls, defer_check=True)
    g4_fcls = fcls['g4']

    all_futures = []
    for g4_fcl in g4_fcls:
        futures = []
        subdir = g4_fcl.replace('.fcl', '')

        # create futures for each reco1 file
        reco1_files = sorted(list(input_dir.glob(f"**/{subdir}/reco1*.root")))
        print(f"Found {len(reco1_files)} files for variation {subdir}.")

        var_output_dir = output_dir / subdir

        for i, f in enumerate(reco1_files):
            # remove base path from input file but preserve the structure
            this_out_dir = var_output_dir / f.relative_to(input_dir).parent
            futures.append(
                generate_from_reco1(
                    workdir=this_out_dir,
                    reco1_filename=str(f),
                    larsoft_opts=larsoft_opts,
                    fcl_dir=fcl_dir,
                    fcls=fcls,
                    mg=mc_meta
                )
            )

        batches = [futures[i:i + subruns_per_caf] for i in range(0, len(futures), subruns_per_caf)]

        mc_fcls = [g4_fcl] + [fcl for key, fcl in fcls.items() if not key in ('scrub', 'g4')]
        for b in batches:
            files_str = ''.join([f.filepath for f in b])
            caf_name = hash_name(files_str)
            rm_hook = cleanup_caf_inputs(subruns_per_caf, full_keep_fraction, b, mc_fcls)
            this_out_dir = pathlib.Path(var_output_dir, 'caf', caf_name)

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
            all_futures.append(futures)
    
    all_futures = [ele for l in all_futures for ele in l]
    for f in all_futures:
        try:
            print(f.result())
        except Exception as e:
            print('FAILED {f.filepath}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
