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
    inputs[2] = optional larsoft input file

    outputs[0] = larsoft file (File object)

    """

    # Copy the fcl file to the work dir

    # Move to the subdir for this:

    absolute_output_file = str(workdir / outputs[0])

    template = '''
echo "Job starting!"
echo "Move fcl."
cp {fhicl} {workdir}/
cd {workdir}
export LOCAL_FCL=$(basename {fhicl})
date
hostname
echo "Current files: "
ls
echo "Load singularity"
module load singularity
echo "Current directory: "
pwd
set -e
singularity run -B /lus/eagle/ -B /lus/grand/ {container} <<EOF
    echo "Running in: "
    pwd
    echo "Sourcing products area"
    #setup SBNDCODE:
    source {larsoft_top}/setup
    setup {software} {version} -q {qual}
    echo "Products setup!"
    # get the fcls
    set -e
    # Add an optional input file:
    export lar_cmd="-c ${{LOCAL_FCL}} --nevts {nevts} --output {output}"
    echo \${{lar_cmd}}
    if [ -f {input} ]; then
        export lar_cmd="\${{lar_cmd}} --input {input} "
    fi
    echo "About to run larsoft"
    echo \${{lar_cmd}}
    lar \${{lar_cmd}}
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
        container = larsoft_opts["container"],
        larsoft_top     = larsoft_opts["larsoft_top"],
        fhicl    = inputs[1],
        nevts    = inputs[0],
        input    = inputs[2],
        output   = absolute_output_file,
    )
    return template


def generate_small_group_of_files(output_top : pathlib.Path, larsoft_opts : dict, fcls : list):

    # Make sure the output directory exists:
    workdir = output_top
    workdir.mkdir(parents=True, exist_ok=True)


    # Loop through the fcl files and generate.  First file assumes no input.
    # Then, capture output as next input.

    input_file = None
    sample_futures = []
    for fcl in fcls:
        # TODO: Could use a better fcl to output naming technique
        output = os.path.basename(fcl)
        output = output.replace(".fcl", ".root")
        print(output)
        # Generate the futures for the three indivudual components:
        # print(this_workdir)
        this_future = generate_single_sample(
            inputs = [
                10,
                fcl,
                input_file,
            ],
            outputs = [
                File(str(output))
            ],
            stdout = str(workdir) + "/lar.out",
            stderr = str(workdir) + "/lar.err",
            larsoft_opts = larsoft_opts,
            workdir = str(workdir)
        )
        sample_futures.append(this_future)

        input_file = this_future.outputs[0]

    # Return the last future for this job
    return sample_futures[-1]


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
    parsl.clear()
    parsl.load(config)
    
    futures = []
    for i in range(10):
        this_out_dir = output_dir / pathlib.Path(f"subrun_{i}")
        futures.append(generate_small_group_of_files(
            output_top   = this_out_dir, 
            larsoft_opts = larsoft_opts,
            fcls = fcls)
        )
        
    print(futures[-1].result())


if __name__ == "__main__":
    main()