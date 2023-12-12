#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import pathlib
import argparse 

import parsl
from parsl.app.app import bash_app

from parsl.config import Config
from parsl.utils import get_all_checkpoints
from parsl.data_provider.files import File

from sbnd_parsl.utils import create_default_useropts, create_parsl_config, build_parser


FULL_KEEP_FRACTION = 0.1
SUBRUNS_PER_CAF = 20

FCLS = {
        'mc': [
            "fcls/prodoverlay_corsika_cosmics_proton_genie_rockbox_sce.fcl",
            "fcls/g4_sce_dirt_filter_lite_wc.fcl",
            "fcls/wirecell_sim_sp_sbnd.fcl",
            "fcls/detsim_sce_lite_wc.fcl",
            "fcls/reco1_sce_lite_wc2d.fcl",
            "fcls/reco2_sce.fcl",
        ],
        'caf': [
            "fcls/cafmaker_job_genie_fluxwgt.fcl",
        ]
}


LARSOFT_OPTS = {
    "container": "/lus/grand/projects/neutrinoGPU/software/slf7.sif",
    "software": "sbndcode",
    "larsoft_top": "/lus/grand/projects/neutrinoGPU/software/larsoft", 
    "version": "v09_78_00",
    "qual": "e20:prof",
    "nevts": 50
}


QUEUE_OPTS = {
    "queue": "prod",
    "walltime": "3:00:00",
    "nodes_per_block": 10
}


@bash_app(cache=True)
def generate_mc_sample():
    pass


@bash_app(cache=True)
def generate_caf():
    pass


def generate_futures():
    pass


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    return prefix / pathlib.Path(f"{100*(subrun//100):04d}") / pathlib.Path(f"subrun_{subrun:04d}")


def main():
    p = build_parser()
    args = p.parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    user_opts = create_default_useropts(allocation="neutrinoGPU")
    user_opts.update(QUEUE_OPTS)
    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"
    print(user_opts)

    config = create_parsl_config(user_opts)
    print(config)
    sys.exit(1)

    parsl.clear()
    parsl.load(config)
    
    # create futures for MC files
    nsubruns = 1000
    futures = []
    for i in range(nsubruns):
        this_out_dir = get_subrun_dir(output_dir, i)
        futures.append(generate_mc_sample(
            output_top = this_out_dir, 
            larsoft_opts = larsoft_opts,
            fcls = FCLS['mc'])
        )

    # create futures for CAF files
    batches = np.array_split(np.arange(nsubruns), SUBRUNS_PER_CAF)
    for b in batches:
        inputs = [get_subrun_dir(output_dir, i) / FCLS['mc'][-1].replace(".fcl", ".root") for i in b]
        futures.append(generate_caf(inputs))
        
    print(list(f.result() for f in futures))


if __name__ == '__main__':
    main()
