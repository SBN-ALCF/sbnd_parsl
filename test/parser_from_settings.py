#!/usr/bin/env python

import sys
import json
import argparse

from sbnd_parsl.metadata import MetadataGenerator

def main(settings_filename: str):
    with open(settings_filename, 'r') as f:
        settings = json.load(f)

    lar_settings = settings['larsoft']
    queue_settings = settings['queue']
    fcl_settings = settings['fcls']
    metadata_settings = settings['metadata']
    print(metadata_settings)
    mg = MetadataGenerator(settings, fcl_settings)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(1)

    main(sys.argv[1])

