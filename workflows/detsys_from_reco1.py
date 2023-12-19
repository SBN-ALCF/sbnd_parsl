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
from sbnd_parsl.templates import SINGLE_FCL_TEMPLATE, CAF_TEMPLATE


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
    "queue": "debug",
    "walltime": "1:00:00",
    "nodes_per_block": 1
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


def generate_from_reco1(workdir: pathlib.Path, reco1_filename: str, larsoft_opts: Dict):
    """
    Create an MC sample from a reco1 file by scrub, then do all other fcl
    steps for each g4 variation
    """

    workdir.mkdir(parents=True, exist_ok=True)
    scrub_filename = os.path.basename(reco1_filename).replace(".root", "_scrub.root")
    scrub_file = File(str(workdir / pathlib.Path(scrub_filename)))
    print(workdir, scrub_filename)

    scrub_future = fcl_future(
        workdir = str(workdir),
        stdout = str(workdir / pathlib.Path(f"scrubStage.out")),
        stderr = str(workdir / pathlib.Path(f"scrubStage.err")),
        template = SINGLE_FCL_TEMPLATE,
        larsoft_opts = LARSOFT_OPTS,
        inputs=[FCLS['scrub'], reco1_filename],
        outputs=[scrub_file],
    )

    last_futures = [scrub_future.outputs[0]]

    for g4_fcl in FCLS['g4']:
        this_workdir = pathlib.Path(workdir, g4_fcl.replace(".fcl", ""))
        this_workdir.mkdir(parents=True, exist_ok=True)
        rest_fcls = [g4_fcl] + [FCLS[key] for key in ['wcsim', 'detsim', 'reco1', 'reco2']] 
        input_file = scrub_future.outputs[0]

        # use i + 1 so generator is still larStage0, g4 is still larStage1, etc.
        for i, fcl in enumerate(rest_fcls):
            output = os.path.basename(fcl).replace(".fcl", ".root")
            output_file = File(str(this_workdir / pathlib.Path(output)))
            print(this_workdir, fcl, output_file)
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


def generate_caf(workdir: pathlib.Path, larsoft_opts: Dict, fcl, inputs: List):
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
    mc_rm_filenames = [fcl.replace('.fcl', '.root') for fcl in FCLS['mc'] if not "reco1" in fcl]
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

    return this_future


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

    for f in reco1_files[:5]:
        # remove base path from input file but preserve the structure
        this_out_dir = output_dir / f.relative_to(input_dir).parent
        futures.append(generate_from_reco1(
            workdir = this_out_dir,
            reco1_filename = str(f),
            larsoft_opts = LARSOFT_OPTS)
        )

    # flatten the list
    futures = [ele for l in futures for ele in l]
    print(list(f.result() for f in futures))


if __name__ == '__main__':
    main()
