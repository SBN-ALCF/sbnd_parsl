#!/usr/bin/env python3

import json
from datetime import datetime

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import argparse

def main():
    parser = argparse.ArgumentParser(description='Plot SPINE node log')
    parser.add_argument('-l', '--log', type=str, required=True, help='Path to the log file')
    parser.add_argument('-o', '--output', default='~/cpu_run_demo', type=str, help='Path to the output plot')
    args = parser.parse_args()

    with open(args.log, 'r') as f:
        json_log = json.loads(f.read())

    nrecords = len(json_log)
    times = []
    pid_data = {}
    cpu_data = {}
    gpu_data = {}
    mem_data = {}

    for i, ts_record in enumerate(json_log.items()):
        ts, record = ts_record

        timestamp = datetime.fromtimestamp(int(ts))
        PID = record['spine']
        cpu_info_list = record['sysstat']['hosts'][0]['statistics'][0]['cpu-load']
        gpu_info = record['gpu']
        mem_info = record['mem']

        times.append(timestamp)

        for _, info in PID.items():
            pid = info['PID']
            if pid not in pid_data:
                pid_data[pid] = np.zeros(nrecords)
            pid_data[pid][i] += 1

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

    # Create subplots with 4 rows
    fig = make_subplots(rows=4, cols=1, 
                        subplot_titles=("Number of Processes", 
                                        "Per-core CPU Usage (A.U.)", 
                                        "GPU Usage (%)", 
                                        "Memory Usage (GB)"),
                        shared_xaxes=True,
                        vertical_spacing=0.1)

    # Plot 1: Processes
    totals = np.zeros(nrecords)
    for pid, data in pid_data.items():
        fig.add_trace(
            go.Scatter(x=times, y=data, name=f"PID {pid}", mode="lines"),
            row=1, col=1
        )
        totals += data
    
    fig.add_trace(
        go.Scatter(x=times, y=totals, name="Total", mode="lines", 
                  line=dict(color="black", dash="dashdot")),
        row=1, col=1
    )
    fig.update_yaxes(range=[0, 36], row=1, col=1)

    # Plot 2: CPU Usage
    for i, cpu_vals in enumerate(reversed(cpu_data.items())):
        cpu, vals = cpu_vals
        fig.add_trace(
            go.Scatter(x=times, y=vals + i * 0.1, name=f"CPU {cpu}", mode="lines"),
            row=2, col=1
        )
    fig.update_yaxes(range=[0, len(cpu_data) * 0.1 + 1.5], row=2, col=1)

    # Plot 3: GPU Usage
    for gpu, vals in gpu_data.items():
        fig.add_trace(
            go.Scatter(x=times, y=vals, name=f"GPU {gpu}", mode="lines"),
            row=3, col=1
        )
    fig.update_yaxes(range=[0, 110], row=3, col=1)

    # Plot 4: Memory Usage
    max_mem = mem_data['total'][0] / (1024**2)
    fig.add_trace(
        go.Scatter(x=times, y=mem_data['used'] / (1024**2), name="Used Memory", mode="lines"),
        row=4, col=1
    )
    fig.add_trace(
        go.Scatter(x=[times[0], times[-1]], y=[max_mem, max_mem], 
                  name="Total Memory", mode="lines", 
                  line=dict(color="gray", dash="dash")),
        row=4, col=1
    )
    fig.update_yaxes(range=[0, max_mem * 1.1], row=4, col=1)

    # Update layout
    fig.update_layout(
        height=800,
        width=1000,
        title_text="System Monitoring",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=0.,
            xanchor="center",
            x=1.5
        ),
        template="plotly_white"
    )

    # Add grid to all subplots
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')

    # Save
    fig.write_html(f'{args.output}.html')
    fig.write_image(f'{args.output}.png')

if __name__ == '__main__':
    main()
