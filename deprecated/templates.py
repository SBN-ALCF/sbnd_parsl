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

{FIND_FCL}
fhicl_from_env=$(find_fcl {{fhicl}})
echo "fhicl_from_env=$fhicl_from_env"
if [ ! -z $fhicl_from_env ]; then
    echo "got fhicl from environment variable: $fhicl_from_env"
    cp $fhicl_from_env {{workdir}}/
else
    echo "did not find fhicl from environment variable. Trying local."
    cp {{fhicl}} {{workdir}}/
fi
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
# {NVIDIA_BEST_CUDA}
echo "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"
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
