#!/usr/bin/env python3

JOB_PRE = r'''
echo "Job starting!"
pwd
date
hostname'''


JOB_POST = r'''
echo "Job finishing!"
date
echo "\nJob finished in: $(pwd)"
echo "Available files:"
ls
hostname'''


# execute a single fcl file in a container
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
    export lar_cmd="-c $LOCAL_FCL --output {{output}} {{lar_args}}"
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
    mv *.json $(dirname {{output}}) || true
    mv  hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true
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
echo "Load spack env"
source {{spack_top}}/share/spack/setup-env.sh
spack env activate sbndcode-{{version}}_env
spack load sbndcode

export GENIE_XSEC_GENLIST=Default
export GENIE_XSEC_EMAX=1000.0
export GENIE_XSEC_DIR=/grand/neutrinoGPU/software/larsoft/genie_xsec/v3_04_00/NULL/AR2320i00000-k250-e1000
export GENIE_XSEC_KNOTS=250
export GENIE_XSEC_TUNE=AR23_20i_00_000

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
export lar_cmd="-c $LOCAL_FCL --nevts {{nevts}} --output {{output}} {{lar_args}}"
if [ -f {{input}} ]; then
    export lar_cmd="\$lar_cmd --source {{input}} "
fi
echo $lar_cmd
echo "About to run larsoft"
lar $lar_cmd
set +e

# Clean up temporary files, if they exist:
rm -f RootOutput-*.root
rm -f TFileService-*.root
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
    export lar_cmd="-c $LOCAL_FCL {{lar_args}} {{input}}"
    echo \$lar_cmd
    echo "About to run larsoft"
    lar \$lar_cmd
    set +e

    # Clean up temporary files, if they exist:
    # note: cleanup can cause art to crash, since art tries to clean up too...
    # rm -f RootOutput-*.root
    # rm -f TFileService-*.root
EOF
{{post_job_hook}}
{JOB_POST}
'''
