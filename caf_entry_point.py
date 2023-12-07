# Modification of entry point to just do caf steps on reco2 files

import sys, os
import pathlib
import argparse 
import hashlib

import parsl
from parsl.app.app import bash_app

from parsl.config import Config

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
    print(inputs)

    template = '''
echo "Job starting!"
pwd
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
    setup sbndata v01_05
    echo "Products setup!"
    # get the fcls
    set -e
    # Add an optional input file:
    export lar_cmd="-c ${{LOCAL_FCL}} --output {output} {input}"
    echo \${{lar_cmd}}
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
        fhicl    = inputs[0],
        input    = inputs[1],
        output   = outputs[0],
    )
    return template


def generate_futures(output_top : pathlib.Path, larsoft_opts : dict, fcls : list, input_filename: str):
    # Make sure the output directory exists:
    workdir = output_top
    workdir.mkdir(parents=True, exist_ok=True)


    # Loop through the fcl files and generate.  First file assumes no input.
    # Then, capture output as next input.

    input_file = File(input_filename)
    sample_futures = []
    for i, fcl in enumerate(fcls):
        output = os.path.basename(fcl)
        absolute_output_file = workdir / pathlib.Path("cafmakerjob_sbnd_sce_genie_and_fluxwgt.root")
        print(f'Generating future for {absolute_output_file}...')

        # Generate the futures for the three indivudual components:
        # print(this_workdir)
        this_future = generate_single_sample(
            inputs = [
                fcl,
                input_file,
            ],
            outputs = [
                File(absolute_output_file)
            ],
            stdout = str(workdir) + f"/larStage{i}.out",
            stderr = str(workdir) + f"/larStage{i}.err",
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
        "version"     : "v09_78_04",
        "qual"        : "e20:prof",
    }

    # What fcls to run, and in what order:
    fcls = [ "fcls/cafmakerjob_sbnd_sce_genie_and_fluxwgt.fcl" ]

    # Make these fcl paths absolute without hardcoding it:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    fcls = [script_dir + "/" + f for f in fcls]

    # Where to put the outputs?
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    print(args)
    
    # Next, set up the user options:
    user_opts = create_default_useropts(allocation="neutrinoGPU")
    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"
    user_opts["queue"] = "debug"
    user_opts["walltime"] = "1:00:00"
    user_opts["nodes_per_block"] = 1
    # to test hypterthreading
    # user_opts["cpus_per_node"] = 64

    print(user_opts)
    # This creates a parsl config:
    config = create_config(user_opts)

    print(config)
    parsl.clear()
    parsl.load(config)

    # TODO a bit fragile!
    # create short hashes for file names so that parsl always looks in the same directory for a given file name
    batch_size = 20
    file_list = sorted(list(pathlib.Path('/lus/eagle/projects/neutrinoGPU/production-v09_78_04-fewernodes').glob("**/reco2*.root")))
    file_batches = [file_list[i:i + batch_size] for i in range(0, len(file_list), batch_size)]
    file_batches_str = [''.join(str(l)) for l in file_batches]
    hash_names = [hashlib.shake_128(bytes(f, encoding='utf8')).hexdigest(16) for f in file_batches_str]

    futures = []
    for h, batch in zip(hash_names, file_batches):
        this_out_dir = output_dir / pathlib.Path(f"subrun_{h}")
        input_filenames = ' '.join([f'-s {str(fname)}' for fname in batch])
        futures.append(generate_futures(
            output_top   = this_out_dir, 
            larsoft_opts = larsoft_opts,
            fcls = fcls,
            input_filename = input_filenames
            )
        )
        
    print(list(f.result() for f in futures))


if __name__ == "__main__":
    main()
