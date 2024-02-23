#!/usr/bin/env python3
# Class for generating metadata directives for fcl files

import shutil
import pathlib
import subprocess
from typing import List, Dict

POMS_EXE="sbndpoms_metadata_injector.sh"

class MetadataGenerator:
    defaults = {
        "inputfclname": "dummy.fcl",
        "mdfclname": "dummy.fcl",
        "mdprojectname": "dummy",
        "mdprojectstage": "gen",
        "mdprojectversion": "v09_78_04",
        "mdprojectsoftware": "sbndcode",
        "mdproductionname": "MCP2023Blike",
        "mdproductiontype": "polaris",
        "mdappversion": "",
        "mdfiletype": "mc",
        "mdappfamily": "art",
        "mdruntype": "physics",
        "mdgroupname": "sbnd",
        "tfilemdjsonname": "",
        "cafname": "caf",
    }

    def __init__(self, settings: Dict, fclnames: Dict, exe=POMS_EXE):
        path = shutil.which(exe)
        if path is None:
            raise RuntimeError(f"Could not find {exe} in $PATH.")

        if len(fclnames) == 0:
            raise ValueError("Need to provide at least one stagename and corresponding fcl filename")
        self.metadata = MetadataGenerator.defaults.copy()
        self.metadata.update(settings.get('metadata', {}))
        self.fclnames = fclnames

        # project name is the first fcl in the chain
        self.metadata["mdprojectname"] = list(fclnames.values())[0].replace('.fcl', '')

        # currently no difference between app and project versions
        self.metadata["mdappversion"] = self.metadata["mdprojectversion"]

    def metadata_stage(self, stagename):
        """ Return metadata for a specific stage """
        this_metadata = self.metadata.copy()
        this_metadata["inputfclname"] = self.fclnames[stagename]
        this_metadata["mdfclname"] = self.fclnames[stagename]
        this_metadata["mdprojectstage"] = stagename
        this_metadata["tfilemdjsonname"] = self.fclnames[stagename].replace('.fcl', '.root.json')
        if stagename != "caf":
            del this_metadata["cafname"]

        return this_metadata

    def metadata_fcl(self, fclname):
        """ Return metadata for a specific fcl """
        # reverse dict lookup
        stagename = list(self.fclnames.keys())[
            list(self.fclnames.values()).index(fclname)
        ]
        return self.metadata_stage(stagename)

    def run(self, fcl='', stage='', check_exists=True):
        parts = self.run_cmd_parts(fcl, stage, check_exists)
        subprocess.run(parts)

    def run_cmd(self, fcl='', stage='', check_exists=True):
        parts = self.run_cmd_parts(fcl, stage, check_exists)
        return ' '.join(parts)

    def run_cmd_parts(self, fcl='', stage='', check_exists=True):
        """ Generate command to run POMS utility with metadata """
        if fcl == '' and stage == '':
            raise RuntimeError("run method requires either a fcl or stage key word argument")

        if stage != '':
            fcl = self.fclnames[stage]

        if fcl not in self.fclnames.values():
            raise ValueError(f"Attempt to run metadata generation with {fclname} which has no corresponding stage.")

        fclfilepath = pathlib.Path(fcl)
        if check_exists and not fclfilepath.is_file():
            raise ValueError(f"{fcl} does not exist.")

        m = self.metadata_fcl(fcl)
        args = []
        for key, value in m.items():
            args.append(f'--{key}')
            args.append(f'{value}')
        args.insert(0, POMS_EXE)
        return args


if __name__ == '__main__':
    # test
    settings = {}
    fcls = {'gen': 'myfile.fcl'}
    m = MetadataGenerator(settings, fcls)
    print(m.metadata_stage('gen'))
    print(m.metadata_fcl('myfile.fcl'))
    print(m.run_cmd('myfile.fcl', check_exists=False))
