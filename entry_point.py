import sys, os
import pathlib
import argparse 

import parsl
from parsl.app.app import python_app, bash_app
# from parsl.configs.local_threads import config


# parsl.set_stream_logger() # <-- log everything to stdout


from parsl.config import Config

# from libsubmit.providers.local.local import Local
from parsl.channels import LocalChannel
from parsl.launchers import MpiExecLauncher, GnuParallelLauncher
from parsl.addresses import address_by_hostname
from parsl.monitoring.monitoring import MonitoringHub
from parsl.utils import get_all_checkpoints
from parsl.data_provider.files import File

from parsl_tools.utils import create_default_useropts


@bash_app(cache=True)
def generate_single_sample(workdir, stdout, stderr, larsoft_opts, inputs=[], outputs=[]):
    """
    Using singularity, generate a few events from the specified fcl file.

    By convention, the inputs are:
    inputs[0] = n_events
    inputs[1] = fcl_file (A File object)
    inputs[2] = small_group_id
    inputs[3] = medium_group_id
    inputs[4] = big_group_id
    inputs[5] = run_id

    outputs[0] = larcv file (File object)

    """

    # Copy the fcl file to the work dir

    # Move to the subdir for this:

    output = "temporary_larsoft_output.root"
    larcv_temp_output = output.replace(".root", "_larcv.h5")

    template = '''
echo "Job starting!"
echo "Move fcl."
cp {fhicl} {workdir}/
cd {workdir}
date
hostname
echo "Current files: "
ls
echo "Load singularity"
module load singularity
echo "Current directory: "
pwd
set -e
singularity run -B /lus/grand/projects/:/lus/grand/projects/:rw /lus/grand/projects/neutrino_osc_ADSP/containers/fnal-wn-sl7.sing <<EOF
    echo "Running in: "
    pwd
    echo "Sourcing products area"
    #setup SBNDCODE:
    source {larsoft_top}/setup
    setup {software} {version} -q {qual}
    echo "Products setup!"
    # get the fcls
    set -e
    lar -c {fhicl} --nevts {nevts} --output {output}
    set +e

    # Clean up temporary files, if they exist:
    rm -f RootOutput-*.root
    rm -f TFileService-*.root
EOF
echo "Job finishing!"
date
echo "\nJob finished in: $(pwd)"
echo "Available files:"
ls
hostname
    '''.format(
        workdir  = workdir,
        software = larsoft_opts["software"],
        version  = larsoft_opts["version"],
        qual     = larsoft_opts["qual"],
        fhicl    = inputs[1],
        nevts    = inputs[0],
        output   = output,
    )
    return template


def generate_small_group_of_files(output_top : pathlib.Path, larsoft_opts : dict, fcls : list):

    # Make sure the output directory exists:
    workdir = output_top
    workdir.mkdir(parents=True, exist_ok=True)

    sample_futures = []
    for mode in ["nueCC", "numuCC", "NC"]:
        # Generate the futures for the three indivudual components:
        mode_file = workdir / pathlib.Path(f"{mode}/{file_base}")
        this_workdir = workdir / pathlib.Path(mode)
        mode_file.parent.mkdir(exist_ok=True, parents=True)
        # print(this_workdir)
        mode_future = generate_single_sample(
            inputs = [
                10,
                fcl_lookup[mode],
                small_group_id,
                medium_group_id,
                big_group_id,
                run_id,
            ],
            outputs = [
                File(str(mode_file))
            ],
            stdout = str(this_workdir) + "/lar.out",
            stderr = str(this_workdir) + "/lar.err",
            workdir = str(this_workdir)
        )
        sample_futures.append(mode_future)
    # print(sample_futures)
    # print([o.outputs[0] for o in sample_futures])
    preprocess_job = preprocess_larcv_files(
        workdir = str(workdir),
        stdout  = str(workdir) + "/preprocess.out",
        stderr  = str(workdir) + "/preprocess.err",
        inputs  = [fcl_lookup["preproc"],] + [o.outputs[0] for o in sample_futures],
        outputs = [File(str(workdir / pathlib.Path(preproces_file)) ) ],
    )

    # # Return the future for this job
    return preprocess_job


def build_parser():

        # Make parser object
    p = argparse.ArgumentParser(description="Main entry script for simulating/reconstructing sbnd data.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    p.add_argument("--sample", "-s", type=lambda x : str(x).lower(),
                   required=True,
                   choices=["beam-simulation"],
                   help="Configuration to run")
    p.add_argument("--events-per-file", "-e", type=int,
                   default=25,
                   help="Number of nexus events per file")

    p.add_argument("--output-dir", "-o", type=pathlib.Path,
                   required=True,
                   help="Top level directory for output")
                

    return p


def create_config(user_opts):
    from parsl_tools.utils import create_provider_by_hostname
    from parsl_tools.utils import create_executor_by_hostname

    checkpoints = get_all_checkpoints(user_opts["run_dir"])
    # print("Found the following checkpoints: ", checkpoints)

    providor = create_provider_by_hostname(user_opts)
    executor = create_executor_by_hostname(user_opts, providor)


    config = Config(
            executors=[executor],
            checkpoint_files = checkpoints,
            run_dir=user_opts["run_dir"],
            checkpoint_mode = 'task_exit',
            strategy=user_opts["strategy"],
            retries=0,
            app_cache=True,
    )
    

    return config



def main():

    p = build_parser()

    args = p.parse_args()

    # Define here the larsoft options and installation information:
    larsoft_opts = {
        "container"   : "/lus/grand/projects/neutrinoGPU/software/slf7.sif",
        "software"    : "sbndcode",
        "larsoft_top" : "/lus/grand/projects/neutrinoGPU/software/larsoft", 
        "version"     : "v09_75_03_02",
        "qual"        : "e20:prof",
    }

    # What fcls to run, and in what order:
    
    fcls = [
        "fcls/prodoverlay_corsika_cosmics_proton_genie_rockbox_sce.fcl",
        "fcls/detsim_sce_lite_wc.fcl",
    ]


    # Where to put the outputs?
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    print(args)
    
    # Next, set up the user options:
    user_opts = create_default_useropts(allocation="datascience")
    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"

    print(user_opts)
    # This creates a parsl config:
    config = create_config(user_opts)

    print(config)
    exit()
    parsl.clear()
    parsl.load(config)


    all_futures = []
    for i_run in range(n_runs):
        sim_future = sim_and_reco_run(
            top_dir       = output_dir,
            run           = i_run,
            n_subruns     = n_subruns,
            start_event   = i_run*events_per_run,
            subrun_offset = subrun_offset,
            n_events      = events_per_file,
            templates     = nexus_input_templates,
            detector      = args.detector,
            sample        = args.sample,
            ic_template_dir = IC_template_dir
        )
        all_futures += sim_future

    print(all_futures[-1].result())


if __name__ == "__main__":
    main()