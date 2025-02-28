#!/usr/bin/env python3

JOB_PRE = r'''
echo "START $(date +%s) $(hostname)" 
echo "Job starting!"
pwd
date
hostname
'''


JOB_POST = r'''
echo "Job finishing!"
date
echo "\nJob finished in: $(pwd)"
echo "Available files:"
ls
hostname
echo "END $(date +%s) $(hostname)" 
'''

# pick the CUDA device with the lowest memory utilization by assigning it to
# CUDA_VISIBLE_DEVICES environment variable
# note: ties broken randomly
NVIDIA_BEST_CUDA = r'''
nvidia-smi
BESTCUDA=$(python3 -c 'import gpustat;import numpy as np;stats=gpustat.GPUStatCollection.new_query();memory=np.array([gpu.memory_used for gpu in stats.gpus]);print(np.random.choice(np.flatnonzero(memory == memory.min())))')
export CUDA_VISIBLE_DEVICES=$BESTCUDA
echo GPU Selected
echo $CUDA_VISIBLE_DEVICES
'''


# execute a single fcl file in a container
# note: escape dollar signs for SL 7 container. Don't escape for running on the node
SINGLE_FCL_TEMPLATE = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."
cp {{fhicl}} {{workdir}}/
export LOCAL_FCL=$(basename {{fhicl}})

{{pre_job_hook}}
echo "Load singularity"
module use /soft/spack/gcc/0.6.1/install/modulefiles/Core
module load apptainer
set -e
singularity run -B /lus/eagle/ -B /grand/ {{container}} <<EOF
    echo "Running in: "
    pwd
    echo "Sourcing products area"
    #setup SBNDCODE:
    export EXPERIMENT={{experiment}}
    source {{larsoft_top}}/setup
    setup {{software}} {{version}} -q {{qual}}
    echo "Products setup!"
    # get the fcls
    set -e
    # Add an optional input file:
    export lar_cmd="-c $LOCAL_FCL {{output_cmd}} {{lar_args}}"
    if [ -f {{input}} ]; then
        export lar_cmd="\$lar_cmd --source {{input}} "
    else
        export lar_cmd="\$lar_cmd --nevts {{nevts}} "
    fi
    echo \$lar_cmd
    echo "About to run larsoft"
    lar \$lar_cmd
    set +e

    # move histogram and json files to same directory as output
    # note: may not exist, fail silently
    mv *.json \$(dirname {{output}}) || true
    mv  hist*root "\$(dirname {{output}})/hists_$(basename {{output}})" || true
EOF
{{post_job_hook}}
{JOB_POST}
'''

# execute a single fcl on bare-metal using spack build
SINGLE_FCL_TEMPLATE_SPACK = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."
cp {{fhicl}} {{workdir}}/
export LOCAL_FCL=$(basename {{fhicl}})

{{pre_job_hook}}
# this is now done in worker_init
# echo "Load spack env"
# source {{spack_top}}/share/spack/setup-env.sh

set -e
echo "Running in: "
pwd
echo "Sourcing products area"
#setup SBNDCODE:
{NVIDIA_BEST_CUDA}
export EXPERIMENT={{experiment}}
echo "Products setup!"
# get the fcls
set -e
# Add an optional input file:
export lar_cmd="-c $LOCAL_FCL {{output_cmd}} {{lar_args}}"
if [ -f {{input}} ]; then
    export lar_cmd="$lar_cmd --source {{input}} --nevts {{nevts}} --nskip {{nskip}}"
else
    export lar_cmd="$lar_cmd --nevts {{nevts}} "
fi
echo $lar_cmd
echo "About to run larsoft"
lar $lar_cmd
set +e

echo "moving files"
echo "mv *.json $(dirname {{output}}) || true"
mv *.json $(dirname {{output}}) || true
echo "mv  *hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true"
mv  *hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true
{{post_job_hook}}
{JOB_POST}
'''

# this template additionally loads sbndata and expects "input" in the form of "-s file1 -s file2 ..."
CAF_TEMPLATE = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."
cp {{fhicl}} {{workdir}}/
export LOCAL_FCL=$(basename {{fhicl}})

{{pre_job_hook}}
echo "Load singularity"
module use /soft/spack/gcc/0.6.1/install/modulefiles/Core
module load apptainer
set -e
singularity run -B /lus/eagle/ -B /grand/ {{container}} <<EOF
    echo "Running in: "
    pwd
    echo "Sourcing products area"
    #setup SBNDCODE:
    export EXPERIMENT={{experiment}}
    source {{larsoft_top}}/setup
    setup {{software}} {{version}} -q {{qual}}
    setup sbndata v01_05
    echo "Products setup!"
    # get the fcls
    set -e
    # Add an optional input file:
    export tempfile=inputlist.txt
    echo "{{input}}" | sed 's/\ /\\n/g' > \$tempfile
    export lar_cmd="-c $LOCAL_FCL {{lar_args}} -S \$tempfile"
    echo \$lar_cmd
    echo "About to run larsoft"
    lar \$lar_cmd
    set +e

    # Clean up temporary files, if they exist:
    # note: cleanup can cause art to crash, since art tries to clean up too...
    # rm -f RootOutput-*.root
    # rm -f TFileService-*.root
    first_file=\$(basename \$(head -n 1 \$tempfile) | sed 's/\.root//g')
    outfile_caf=\$first_file.caf.root
    outfile_flat_caf=\$first_file.flat.caf.root


    echo "moving files"
    echo "mv *.json \$(dirname {{output}}) || true"
    mv *.json \$(dirname {{output}}) || true
    echo "mv  *hist*root "\$(dirname {{output}})/hists_\$(basename {{output}})" || true"
    mv  *hist*root "\$(dirname {{output}})/hists_\$(basename {{output}})" || true
    echo "mv \$outfile_caf \$(dirname {{output}}) || true"
    mv \$outfile_caf \$(dirname {{output}}) || true
    echo "mv \$outfile_flat_caf \$(dirname {{output}}) || true"
    mv \$outfile_flat_caf \$(dirname {{output}}) || true
EOF
{{post_job_hook}}
{JOB_POST}
'''

# spack-ified CAF template
CAF_TEMPLATE_SPACK = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
echo "Current directory CAF!!!!: "
pwd
echo "Current files: "
ls
echo "Move fcl."
cp {{fhicl}} {{workdir}}/
export LOCAL_FCL=$(basename {{fhicl}})

{{pre_job_hook}}

set -e
echo "Running in: "
pwd
echo "Sourcing products area"
#setup SBNDCODE:
export EXPERIMENT={{experiment}}
echo "Products setup!"
# get the fcls
set -e
# Add an optional input file:
export tempfile=inputlist.txt
echo "{{input}}" | sed 's/\ /\\n/g' > $tempfile
export lar_cmd="-c $LOCAL_FCL {{lar_args}} -S $tempfile"
echo $lar_cmd
echo "About to run larsoft"
lar $lar_cmd
set +e


first_file=$(basename $(head -n 1 $tempfile) | sed 's/\.root//g')
outfile_caf=$first_file.caf.root
outfile_flat_caf=$first_file.flat.caf.root

mv *.json $(dirname {{output}}) || true
mv  *hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true
mv $outfile_caf $(dirname {{output}}) || true
mv $outfile_flat_caf $(dirname {{output}}) || true
{{post_job_hook}}
{JOB_POST}
'''

# this template additionally loads sbndata and expects "input" in the form of "-s file1 -s file2 ..."
SPINE_TEMPLATE = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls

{{pre_job_hook}}
echo "Load singularity"
module use /soft/spack/gcc/0.6.1/install/modulefiles/Core
module load apptainer
set -e

# these lines replace hard-coded path in SPINE cfg files from github
# put the changes in a temporary copy
TMP_CFG=$(mktemp)
cp {{config}} $TMP_CFG
sed -i "s|\(.*num_workers:\).*|\\1 {{cores_per_worker}}|g" $TMP_CFG
sed -i "s|\(.*weight_path:\).*|\\1 {{weights}}|g" $TMP_CFG
sed -i "s|\(.*cfg:\).*\(flashmatch.*\.cfg\).*|\\1 $(dirname {{config}})/\\2|g" $TMP_CFG

# use GPUs from environment, so remove this 
sed -i "s|\(.*gpus:\).*||g" $TMP_CFG
echo "Config: $TMP_CFG"

{NVIDIA_BEST_CUDA}

singularity run -B /lus/eagle/ -B /lus/grand/ --nv {{container}} <<EOL
    echo "Running in: "
    pwd
    export CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES
    echo "CUDA_VISIBLE_DEVICES=\$CUDA_VISIBLE_DEVICES"
    if [ "{{opt0finder}}x" != "x" ]; then
        source {{opt0finder}}/configure.sh
        echo "using custom OpT0Finder"
        echo \$FMATCH_BASEDIR
        echo \$FMATCH_LIBDIR
    fi
    python {{exe}} -c $TMP_CFG -S {{input}}

    echo "moving files"
    mv *.h5 $(dirname {{output}}) || true
EOL
{{post_job_hook}}
{JOB_POST}
'''


# execute a custom command
CMD_TEMPLATE_SPACK = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."
cp {{fhicl}} {{workdir}}/
export LOCAL_FCL=$(basename {{fhicl}})

{{pre_job_hook}}
set -e
echo "Running in: "
pwd
echo "Sourcing products area"
#setup SBNDCODE:
{NVIDIA_BEST_CUDA}
export EXPERIMENT={{experiment}}
echo "Products setup!"
# get the fcls
set -e
# Add an optional input file:
export lar_cmd="{{cmd}}"
echo $lar_cmd
echo "About to run larsoft"
eval $lar_cmd
set +e

echo "moving files"
echo "mv *.json $(dirname {{output}}) || true"
mv *.json $(dirname {{output}}) || true
echo "mv  *hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true"
mv  *hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true
{{post_job_hook}}
{JOB_POST}
'''
