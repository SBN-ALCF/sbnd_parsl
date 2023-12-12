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

echo "Load singularity"
module load singularity
set -e
singularity run -B /lus/eagle/ -B /lus/grand/ {{container}} <<EOF
    echo "Running in: "
    pwd
    echo "Sourcing products area"
    #setup SBNDCODE:
    source {{larsoft_top}}/setup
    setup {{software}} {{version}} -q {{qual}}
    echo "Products setup!"
    # get the fcls
    set -e
    # Add an optional input file:
    export lar_cmd="-c $LOCAL_FCL --nevts {{nevts}} --output {{output}}"
    echo \$lar_cmd
    if [ -f {{input}} ]; then
        export lar_cmd="\$lar_cmd --source {{input}} "
    fi
    echo "About to run larsoft"
    lar \$lar_cmd
    set +e

    # Clean up temporary files, if they exist:
    rm -f RootOutput-*.root
    rm -f TFileService-*.root
EOF
{JOB_POST}
'''
