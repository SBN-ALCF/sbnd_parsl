#!/usr/bin/env python3

import json
from datetime import datetime

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator, MultipleLocator, MaxNLocator
from matplotlib.dates import AutoDateLocator


# XeTeX backend. Only use if needed, otherwise it's slow
# matplotlib.use('pgf')

THEME = {
    'font': {'weight': 'normal', 'size': 8, 'family': 'DejaVu Sans'},
    'xtick': {'direction': 'in' , 'top': 'on'},
    'ytick': {'direction': 'in' , 'right': 'on'},
    'grid': {'color': 'lightgray', 'linestyle': 'dotted'},
    'axes': {'axisbelow': True},
    'legend': {'frameon': False},
    'pgf': {'preamble': '\n'.join(
        [
            r'\usepackage{siunitx}',
            r'\sisetup{mode = text, per-mode = symbol}',
            r'\DeclareSIUnit\ev{\electronvolt}',
            r'\usepackage{mathspec}',
            r'\setmathfont(Digits,Latin,Greek){Helvetica}'
        ]
    )},
}

[matplotlib.rc(key, **val) for key, val in THEME.items()]


FCL_LIST = {
    'prodoverlay_corsika_cosmics_proton_genie_rockbox_sbnd.fcl': 'GEN',
    'g4_dirt_filter_lite.fcl': 'G4',
    'standard_detsim_sbnd.fcl': 'DETSIM',
    'standard_reco1_sbnd.fcl': 'RECO1',
    'standard_reco2_sbnd.fcl': 'RECO2',
    'cafmakerjob_sbnd_systtools_and_fluxwgt.fcl': 'CAF',
}


CM = 1/2.54
LOG_NAME = 'log.json'


def main():
    with open(LOG_NAME, 'r') as f:
        json_log = json.loads(f.read())

    nrecords = len(json_log)
    times = []
    lar_data = {}
    cpu_data = {}
    gpu_data = {}
    mem_data = {}

    for i, ts_record in enumerate(json_log.items()):
        ts, record = ts_record

        timestamp = datetime.fromtimestamp(int(ts))
        lar = record['lar']
        cpu_info_list = record['sysstat']['hosts'][0]['statistics'][0]['cpu-load']
        gpu_info = record['gpu']
        mem_info = record['mem']

        times.append(timestamp)

        for _, info in lar.items():
            fcl = info['fcl']

            if fcl not in lar_data:
                lar_data[fcl] = np.zeros(nrecords)
            lar_data[fcl][i] += 1

        for cpu_info in cpu_info_list:
            # ignore the global average reported by mpstat
            cpu = cpu_info['cpu']
            if cpu == "-1":
                continue
            if cpu not in cpu_data:
                cpu_data[cpu] = np.zeros(nrecords)
            cpu_data[cpu][i] = cpu_info['usr'] / 100

        for gpu, info in gpu_info.items():
            if gpu not in gpu_data:
                gpu_data[gpu] = np.zeros(nrecords)
            gpu_data[gpu][i] = info['gpu']

        for key, val in mem_info.items():
            if key not in mem_data:
                mem_data[key] = np.zeros(nrecords)
            mem_data[key][i] = val
        

    nrows = 4
    fig, ax = plt.subplots(nrows, 1, figsize=(16 * CM, 20 * CM))
    ax = ax.flatten()

    date_formatter = matplotlib.dates.DateFormatter('%b %d\n%H:%M')
    ax[0].xaxis.set_major_locator(AutoDateLocator(maxticks=6))
    ax[0].xaxis.set_major_formatter(date_formatter)
    ax[0].xaxis.set_minor_locator(AutoMinorLocator())
    for i, a in enumerate(ax):
        a.sharex(ax[0])
        a.grid()
        a.margins(0)
        a.yaxis.set_minor_locator(AutoMinorLocator())
        if i < nrows - 1:
            a.tick_params(labelbottom=False)
    # fig.subplots_adjust(hspace=0)
    
    totals = np.zeros(nrecords)

    # processes plot
    ax[0].axhline(32, color='gray', linestyle='--')
    for fcl, label in FCL_LIST.items():
        try:
            ax[0].plot(times, lar_data[fcl], label=label)
            totals += lar_data[fcl]
        except KeyError:
            continue
    ax[0].plot(times, totals, linestyle='-.', color='k', label='Total')
    ax[0].legend()
    ax[0].set_ylabel('Number of Processes')
    ax[0].set_ylim(0, 36)

    for i, cpu_vals in enumerate(reversed(cpu_data.items())):
        cpu, vals = cpu_vals
        ax[1].plot(times, vals + i * 0.1, label=cpu)
    ax[1].set_ylabel('Per-core CPU Usage (A.U.)')
    ax[1].set_ylim(0, len(cpu_data) * 0.1 + 1.5)

    for gpu, vals in gpu_data.items():
        ax[2].plot(times, vals, label=gpu)
    ax[2].set_ylim(0, 110)
    ax[2].legend()
    ax[2].set_ylabel('GPU Usage (%)')

    max_mem = mem_data['total'][0] / (1024**2)
    ax[3].plot(times, mem_data['used'] / (1024**2))
    ax[3].axhline(max_mem, color='gray', linestyle='--')
    ax[3].set_ylabel('Memory Usage (GB)')
    ax[3].set_ylim(0, max_mem * 1.1)

    plt.tight_layout()
    plt.savefig('cpu_run_demo.pdf')

            


if __name__ == '__main__':
    main()

