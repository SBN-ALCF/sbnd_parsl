#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

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


NSUBRUNS = 1
NEVENTS_PER_SUBRUN = 50
SUBRUNS_PER_CAF = 20
FULL_KEEP_FRACTION = 0.1

FCLS = {
        'mc': [
            "prodoverlay_corsika_cosmics_proton_genie_rockbox_sce.fcl",
            "g4_sce_dirt_filter_lite_wc.fcl",
            "wirecell_sim_sp_sbnd.fcl",
            "detsim_sce_lite_wc.fcl",
            "reco1_sce_lite_wc2d.fcl",
            "reco2_sce.fcl",
        ],
        'caf': [
            "cafmakerjob_sbnd_sce_genie_and_fluxwgt.fcl",
        ]
}


LARSOFT_OPTS = {
    "container": "/lus/grand/projects/neutrinoGPU/software/slf7.sif",
    "software": "sbndcode",
    "larsoft_top": "/lus/grand/projects/neutrinoGPU/software/larsoft", 
    "version": "v09_78_00",
    "qual": "e20:prof",
    "nevts": NEVENTS_PER_SUBRUN
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


def generate_mc_sample(workdir: pathlib.Path, larsoft_opts: Dict, fcls: List):
    """
    Create a future for each fcl output required to produce a fully-simulated &
    reconstructed MC file
    """
    input_file = None
    last_future = None
    workdir.mkdir(parents=True, exist_ok=True)

    for i, fcl in enumerate(fcls):
        output = os.path.basename(fcl).replace(".fcl", ".root")
        output_file = workdir / pathlib.Path(output)
        this_future = fcl_future(
            workdir = str(workdir),
            stdout = str(workdir / pathlib.Path(f"larStage{i}.out")),
            stderr = str(workdir / pathlib.Path(f"larStage{i}.err")),
            template = SINGLE_FCL_TEMPLATE,
            larsoft_opts = LARSOFT_OPTS,
            inputs=[fcl, input_file],
            outputs=[File(str(output_file))],
        )
        input_file = this_future.outputs[0]
        last_future = this_future.outputs[0]

    # input file is set to the last future of this job
    return last_future


def generate_caf(workdir: pathlib.Path, larsoft_opts: Dict, fcl, inputs: List):
    """
    Create a future for a caf file. The inputs are the ouputs from the final
    stage of multiple MC futures.
    """
    workdir.mkdir(parents=True, exist_ok=True)

    caf_input_arg = ' '.join([f'-s {str(pathlib.Path(fname.filepath, fname.filename))}' for fname in inputs])
    output = f"cafmakerjob_sbnd_sce_genie_and_fluxwgt.root"
    output_file = workdir / pathlib.Path(output)
    future_inputs = [fcl, caf_input_arg] + inputs

    this_future = fcl_future(
        workdir = str(workdir),
        stdout = str(workdir / pathlib.Path("cafStage.out")),
        stderr = str(workdir / pathlib.Path("cafStage.err")),
        template = CAF_TEMPLATE,
        larsoft_opts = LARSOFT_OPTS,
        inputs = future_inputs,
        outputs = [File(str(output_file))],
    )

    return this_future


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    return prefix / pathlib.Path(f"{100*(subrun//100):04d}") / pathlib.Path(f"subrun_{subrun:04d}")


def main():
    p = build_parser()
    args = p.parse_args()
    output_dir = args.output_dir
    fcl_dir = args.fcl_dir
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
    
    # create futures for MC files
    for i in range(NSUBRUNS):
        this_out_dir = get_subrun_dir(output_dir, i)
        futures.append(generate_mc_sample(
            workdir = this_out_dir, 
            larsoft_opts = LARSOFT_OPTS,
            fcls = [str(fcl_dir / pathlib.Path(fcl)) for fcl in FCLS['mc']])
        )

    batches = [futures[i:i + SUBRUNS_PER_CAF] for i in range(0, len(futures), SUBRUNS_PER_CAF)]

    for b in batches:
        files_str = ''.join([str(pathlib.Path(f.filepath, f.filename)) for f in b])
        hash_name = hashlib.shake_128(bytes(files_str, encoding='utf8')).hexdigest(16)

        this_out_dir = pathlib.Path(output_dir, 'caf', hash_name)
        futures.append(generate_caf(
            workdir = this_out_dir,
            larsoft_opts = LARSOFT_OPTS,
            fcl = str(fcl_dir / pathlib.Path(FCLS['caf'][0])),
            inputs = b)
        )
        
    print(list(f.result() for f in futures))


if __name__ == '__main__':
    main()
