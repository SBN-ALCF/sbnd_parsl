#!/usr/bin/env python

# This workflow "scrubs" reco1 files and then re-simulates one part of the fcl
# chain for the purposes of making matched detector systematic variations.

import sys, os
import pathlib
import hashlib
import argparse 
import tempfile
from typing import Dict, List

import numpy as np

import parsl
from parsl.app.app import bash_app

from parsl.config import Config
from parsl.utils import get_all_checkpoints
from parsl.data_provider.files import File

from sbnd_parsl.utils import create_default_useropts, create_parsl_config, build_parser
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE, JOB_PRE, JOB_POST


# SUBRUNS_PER_CAF = 20
# FULL_KEEP_FRACTION = 0.05
SUBRUNS_PER_CAF = 16
FULL_KEEP_FRACTION = 0.0625

FCLS = {
    'scrub': 'scrub_g4_wcls_detsim_reco1.fcl',
    'g4': [
        'g4_sce_dirt_filter_lite_wc_recomb01.fcl',
        'g4_sce_dirt_filter_lite_wc_recomb0-1.fcl',
        'g4_sce_dirt_filter_lite_wc_recomb10.fcl',
        'g4_sce_dirt_filter_lite_wc_recomb-10.fcl',
        'g4_sce_dirt_filter_lite_wc_recomb-11.fcl',
        'g4_sce_dirt_filter_lite_wc_recomb-1-1.fcl',
        'g4_sce_dirt_filter_lite_wc_recomb1-1.fcl',
        'g4_sce_dirt_filter_lite_wc_recomb11.fcl',
    ],
    'wcsim': 'wirecell_sim_sp_sbnd_detvar.fcl',
    'detsim': 'detsim_sce_lite_wc_detvar.fcl',
    'reco1': 'reco1_sce_lite_wc2d_detvar.fcl',
    'reco2': 'reco2_sce.fcl',
    'caf': 'cafmakerjob_sbnd_sce_genie_and_fluxwgt.fcl'
}


LARSOFT_OPTS = {
    "container": "/lus/grand/projects/neutrinoGPU/software/slf7.sif",
    "software": "sbndcode",
    "larsoft_top": "/lus/grand/projects/neutrinoGPU/software/larsoft", 
    "version": "v09_78_00",
    "qual": "e20:prof",
    "nevts": -1,
}


QUEUE_OPTS = {
    "queue": "prod",
    "walltime": "3:00:00",
    "nodes_per_block": 10
}


@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, larsoft_opts, inputs=[], outputs=[]):
    """ Return formatted bash script which produces each future when executed """
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1],
        **larsoft_opts,
    )


@bash_app(cache=True)
def fcl_future_dummy(workdir, inputs=[], outputs=[]):
    '''
    create a dummy future that replaces the file with a placeholder if  the
    output of the caf stage deleted its inputs already. This allows us to re-
    run the code and have the same future structure even if some outputs were
    deleted
    '''
    return f'''
cd {workdir}
touch {outputs[0]}'''


def generate_from_reco1(workdir: pathlib.Path, reco1_filename: str, larsoft_opts: Dict):
    """
    Create an MC sample from a reco1 file by scrub, then do all other fcl
    steps for each g4 variation
    """

    workdir.mkdir(parents=True, exist_ok=True)
    scrub_filename = os.path.basename(reco1_filename).replace(".root", "_scrub.root")
    scrub_file = File(str(workdir / pathlib.Path(scrub_filename)))

    scrub_future = fcl_future(
        workdir = str(workdir),
        stdout = str(workdir / pathlib.Path(f"scrubStage.out")),
        stderr = str(workdir / pathlib.Path(f"scrubStage.err")),
        template = SINGLE_FCL_TEMPLATE,
        larsoft_opts = LARSOFT_OPTS,
        inputs=[FCLS['scrub'], reco1_filename],
        outputs=[scrub_file],
    )

    last_futures = []

    for g4_fcl in FCLS['g4']:
        this_workdir = pathlib.Path(workdir, g4_fcl.replace(".fcl", ""))
        this_workdir.mkdir(parents=True, exist_ok=True)
        rest_fcls = [g4_fcl] + [FCLS[key] for key in ['wcsim', 'detsim', 'reco1', 'reco2']] 

        # this check skips creating futures for directories where the caf
        # stage removed the inputs already, which might be true when
        # re-running this workflow after a timeout
        caf_complete = False
        output_log_last_stage = pathlib.Path(this_workdir / f"larStage{len(rest_fcls):d}.out")
        if output_log_last_stage.is_file():
            caf_complete = True

        input_file = scrub_future.outputs[0]

        # use i + 1 so generator is still larStage0, g4 is still larStage1, etc.
        for i, fcl in enumerate(rest_fcls):
            output = os.path.basename(fcl).replace(".fcl", ".root")
            output_file = File(str(this_workdir / pathlib.Path(output)))
            if caf_complete:
                this_future = fcl_future_dummy(
                        workdir=str(this_workdir),
                        inputs=[],
                        outputs=[output_file])
            else:
                this_future = fcl_future(
                    workdir = str(this_workdir),
                    stdout = str(this_workdir / pathlib.Path(f"larStage{i + 1}.out")),
                    stderr = str(this_workdir / pathlib.Path(f"larStage{i + 1}.err")),
                    template = SINGLE_FCL_TEMPLATE,
                    larsoft_opts = LARSOFT_OPTS,
                    inputs=[fcl, input_file],
                    outputs=[output_file],
                )
            input_file = this_future.outputs[0]

        last_futures.append(input_file)
    
    return last_futures


def generate_caf(workdir: pathlib.Path, larsoft_opts: Dict, fcl, inputs: List, g4_fcl: str):
    """
    Create a future for a caf file. The inputs are the ouputs from the final
    stage of multiple MC futures.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    caf_input_arg = ' '.join([f'-s {fname.filepath}' for fname in inputs])
    output = f"cafmakerjob_sbnd_sce_genie_and_fluxwgt.root"
    output_file = workdir / pathlib.Path(output)
    future_inputs = [fcl, caf_input_arg] + inputs

    # make a hook to delete all but a certain fraction of the inputs
    n_remove = int((1.0 - FULL_KEEP_FRACTION) * len(inputs))

    # remove all the ROOT files from the first n_remove MC files, except reco1!
    fcl_list = [ g4_fcl, FCLS['wcsim'], FCLS['detsim'], FCLS['reco1'], FCLS['reco2'] ]
    mc_rm_filenames = [fcl.replace('.fcl', '.root') for fcl in fcl_list if not "reco1" in fcl]
    rm_strs = []
    for iput in inputs[:n_remove]:
        for mc_fname in mc_rm_filenames:
            rm_strs.append(f'rm -f {pathlib.Path(iput.filepath).parent}/{mc_fname}')

    rm_hook = '\n'.join(rm_strs)

    opts = LARSOFT_OPTS.copy()
    opts['post_job_hook'] = rm_hook

    this_future = fcl_future(
        workdir = str(workdir),
        stdout = str(workdir / pathlib.Path("cafStage.out")),
        stderr = str(workdir / pathlib.Path("cafStage.err")),
        template = CAF_TEMPLATE,
        larsoft_opts = opts,
        inputs = future_inputs,
        outputs = [File(str(output_file))],
    )

    return this_future.outputs[0]


def hash_name(string: str) -> str:
    ''' create something that looks like abcd-abcd-abcd-abcd from a string '''
    strhash = hashlib.shake_128(bytes(string, encoding='utf8')).hexdigest(16)
    return '-'.join(strhash[i*4:i*4+4] for i in range(4))


def main():
    p = build_parser()
    p.add_argument("--reco1-dir", "-r", type=pathlib.Path,
                   required=True,
                   help="Directory containing reco1 input files")
    args = p.parse_args()

    input_dir = args.reco1_dir
    if not input_dir.is_dir() or not input_dir.exists():
        print(f"Directory {input_dir} is not a valid directory")
        sys.exit(1)

    # create futures for each reco1 file
    reco1_files = sorted(list(input_dir.glob("**/reco1*.root")))
    print(f"Found {len(reco1_files)} files.")

    output_dir = args.output_dir
    fcl_dir = args.fcl_dir
    LARSOFT_OPTS['version'] = args.software_version
    output_dir.mkdir(parents=True, exist_ok=True)

    user_opts = create_default_useropts(allocation="neutrinoGPU")
    user_opts.update(QUEUE_OPTS)
    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"
    print(user_opts)

    config = create_parsl_config(user_opts)
    print(config)

    futures = []
    parsl.clear()
    parsl.load(config)

    for f in reco1_files:
        # remove base path from input file but preserve the structure
        this_out_dir = output_dir / f.relative_to(input_dir).parent
        futures.append(generate_from_reco1(
            workdir = this_out_dir,
            reco1_filename = str(f),
            larsoft_opts = LARSOFT_OPTS)
        )

    # make cafs from the outputs of each fcl variation
    # each list within futures contains a reco2 file from each g4 variation
    # we want to use all the reco2 futures from the same g4 variation as input to each caf
    nvariations = len(FCLS['g4'])
    futures_by_variation = [
        [l[i] for l in futures] for i in range(nvariations)
    ]

    caf_futures = []
    for g4_fcl, fs in zip(FCLS['g4'], futures_by_variation):
        variation_name = g4_fcl.replace(".fcl", "")
        this_out_dir = pathlib.Path(output_dir, "caf", variation_name)
        batches = [fs[i:i + SUBRUNS_PER_CAF] for i in range(0, len(fs), SUBRUNS_PER_CAF)]
        for b in batches:
            # create a unique name for this caf based on its inputs.  remove
            # the g4 variation so that all cafs get the same name regardless of
            # variation. They are still placed in the variation sub-folder
            files_str = ''.join([f.filepath.replace(variation_name, "") for f in b])
            out_name = hash_name(files_str)
            batch_out_dir = pathlib.Path(this_out_dir, out_name)

            caf_futures.append(generate_caf(
                workdir = batch_out_dir,
                larsoft_opts = LARSOFT_OPTS,
                fcl = str(fcl_dir / pathlib.Path(FCLS['caf'])),
                inputs = b,
                g4_fcl=g4_fcl)
            )
    futures.append(caf_futures)

    # flatten the list
    futures = [ele for l in futures for ele in l]
    print(list(f.result() for f in futures))


if __name__ == '__main__':
    main()
