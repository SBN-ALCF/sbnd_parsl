#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# then deletes the intermediate output files while saving a certain fraction

import sys, os
import json
import pathlib
from typing import Dict, List

import parsl
from parsl.app.app import bash_app
from parsl.data_provider.files import File

from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name
from sbnd_parsl.templates import SPINE_TEMPLATE

@bash_app(cache=True)
def spine_future(workdir, stdout, stderr, template, opts, inputs=[], outputs=[], pre_job_hook="", post_job_hook=""): 
    """ Return formatted bash script which produces each future when executed """
    return template.format(
        workdir=workdir,
        output=outputs[0],
        input=inputs[0],
        **opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )

def generate_spine(workdir: pathlib.Path, config: str, inputs: str, container: str, pre_job_hook: str, post_job_hook: str):
    """
    Create a future for a caf file. The inputs are the ouputs from the final
    stage of multiple MC futures.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    output = "dummy.h5"
    output_file = workdir / pathlib.Path(output)

    opts = {}
    opts["config"] = config
    opts["container"] = container

    # use post_job_hook to clean up
    this_future = spine_future(
        workdir = str(workdir),
        stdout = str(workdir / pathlib.Path("spine.out")),
        stderr = str(workdir / pathlib.Path("spine.err")),
        template = SPINE_TEMPLATE,
        opts = opts,
        inputs = [inputs],
        outputs = [File(str(output_file))],
        pre_job_hook = pre_job_hook,
        post_job_hook = post_job_hook
    )

    return this_future.outputs[0]

def main(settings: json):
    output_dir = pathlib.Path(settings['run']['output'])
    output_dir.mkdir(parents=True, exist_ok=True)

    user_opts = create_default_useropts()
    user_opts.update(settings['queue'])

    user_opts["run_dir"] = f"{str(output_dir)}/runinfo"
    print(user_opts)

    config = create_parsl_config(user_opts)
    print(config)

    futures = []
    parsl.clear()
    parsl.load(config)

    this_out_dir = pathlib.Path(output_dir, 'spine')
    futures.append(
        generate_spine(
            workdir=this_out_dir,
            config=settings["config"],
            inputs=settings["inputs"],
            container=settings["container"],
            pre_job_hook="",
            post_job_hook=""
        )
    )
    
    for f in futures:
        try:
            print(f.result())
        except Exception as e:
            print('FAILED {f.filepath}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)
    
    main(settings)
