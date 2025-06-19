import socket
import pathlib
import hashlib
import itertools

from parsl.config import Config

from parsl.addresses import address_by_interface
from parsl.utils import get_all_checkpoints
from parsl.providers import PBSProProvider, LocalProvider
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.launchers import MpiExecLauncher, GnuParallelLauncher
from parsl.monitoring.monitoring import MonitoringHub


POLARIS_OPTS = {
    'hostname': 'polaris',
    'ncpus': 32,
    'scheduler': '#PBS -l filesystems=home:grand:eagle\n#PBS -l place=scatter',
    'launcher': '--depth=64 --ppn 1',
    'cpu_affinity': 'alternating',
    'available_accelerators': 32
}


AURORA_OPTS = {
    'hostname': 'aurora',
    'ncpus': 208,
    'scheduler': '#PBS -l filesystems=home:flare',
    'launcher': '--ppn 1',
    'cpu_affinity': 'list:1-2,105-106:3-4,107-108:5-6,109-110:7-8,111-112:9-10,113-114:11-12,115-116:13-14,117-118:15-16,119-120:17-18,121-122:19-20,123-124:21-22,125-126:23-24,127-128:25-26,129-130:27-28,131-132:29-30,133-134:31-32,135-136:33-34,137-138:35-36,139-140:37-38,141-142:39-40,143-144:41-42,145-146:43-44,147-148:45-46,149-150:47-48,151-152:53-54,157-158:55-56,159-160:57-58,161-162:59-60,163-164:61-62,165-166:63-64,167-168:65-66,169-170:67-68,171-172:69-70,173-174:71-72,175-176:73-74,177-178:75-76,179-180:77-78,181-182:79-80,183-184:81-82,185-186:83-84,187-188:85-86,189-190:87-88,191-192:89-90,193-194:91-92,195-196:93-94,197-198:95-96,199-200:97-98,201-202:99-100,203-204',
    'available_accelerators': list(itertools.chain.from_iterable([[f'{gid}.{tid}'] * 4 for gid in range(6) for tid in range(2)]))
}


def _worker_init(spack_top=None, spack_version='', software='sbndcode', mps: bool=True, venv_name=None):
    """Return list of worker init commands based on the options."""
    cmds = []
    if spack_top is not None:
        cmds += [
            f'source {pathlib.Path(spack_top, "share/spack/setup-env.sh")}',
            f'spack env activate {software}-{spack_version}_env',
            f'spack load {software}'
        ]
    if venv_name:
        hostname = socket.gethostname()
        if 'polaris' in hostname:
            # use conda
            cmds += [
                'module use /soft/modulefiles',
                'module load conda',
                f'conda activate {venv_name}'
            ]
        elif 'aurora' in hostname:
            # use pip with frameworks
            cmds += [
                'module load frameworks',
                f'source ~/.venv/{venv_name}/bin/activate',
                'export ZEX_NUMBER_OF_CCS=0:4,1:4,2:4,3:4,4:4,5:4,6:4,7:4,8:4,9:4,10:4,11:4'
            ]
        else:
            raise RuntimeError(f"Don't know how to load virtual environments on machine {hostname}")

    if mps:
        cmds += [
            'export CUDA_MPS_PIPE_DIRECTORY=/tmp/nvidia-mps',
            'export CUDA_MPS_LOG_DIRECTORY=/tmp/nvidia-log',
            'CUDA_VISIBLE_DEVICES=0,1,2,3 nvidia-cuda-mps-control -d',
            'echo \"start_server -uid $( id -u )\" | nvidia-cuda-mps-control'
        ]

    return '&&'.join(cmds)


def create_provider_by_hostname(user_opts, system_opts, spack_opts):
    hostname = socket.gethostname()
    if len(spack_opts) >= 2:
        spack_top = spack_opts[0]
        version = spack_opts[1]
        software = spack_opts[2]
        worker_init = _worker_init(spack_top=spack_top, spack_version=version, software=software, mps=mps, venv_name=user_opts.get("worker_venv_name", "sbn"))
    else:
        mps = 'polaris' in system_opts['hostname']
        worker_init = _worker_init(mps=mps, venv_name='sbn')

    return PBSProProvider(
        account         = user_opts["allocation"],
        queue           = user_opts.get("queue", "debug"),
        nodes_per_block = user_opts.get("nodes_per_block", 1),
        cpus_per_node   = user_opts.get("cpus_per_node", system_opts['ncpus']),
        init_blocks     = user_opts.get("init_blocks", 1),
        max_blocks      = user_opts.get("max_blocks", 1),
        walltime        = user_opts.get("walltime", "1:00:00"),
        cmd_timeout     = 240,
        scheduler_options = system_opts['scheduler'],
        launcher        = MpiExecLauncher(bind_cmd="--cpu-bind", overrides=system_opts['launcher']),
        worker_init     = worker_init + '&&export PATH=/opt/cray/pals/1.4/bin:${PATH}'
    )

def create_executor_by_hostname(user_opts, system_opts, provider):
    from parsl import HighThroughputExecutor
    return HighThroughputExecutor(
        label="htex",
        heartbeat_period=15,
        heartbeat_threshold=120,
        worker_debug=True,
        max_workers_per_node=user_opts["cpus_per_node"],
        cores_per_worker=user_opts["cores_per_worker"],
        available_accelerators=system_opts['available_accelerators'],
        address=address_by_interface("bond0"),
        address_probe_timeout=120,
        cpu_affinity=system_opts['cpu_affinity'],
        prefetch_capacity=0,
        provider=provider,
        block_error_handler=False,
        working_dir=str(pathlib.Path(user_opts["run_dir"]) / 'cmd')
    )
    

def create_default_useropts(**kwargs):
    hostname = socket.gethostname()

    if 'polaris' in hostname or 'aurora' in hostname:
        # Then, we're likely on the login node of polaris and want to submit via parsl:
        user_opts = {
            # Node setup: activate necessary conda environment and such.
            'worker_init': '',
            'scheduler_options': '',
            'allocation': 'neutrinoGPU',
            'queue': 'debug',
            'walltime': '1:00:00',
            'nodes_per_block' : 1,
            'cpus_per_node' : 32,
            'strategy' : 'none',
        }
    else:
        # We're likely running locally
        user_opts = {
            'cpus_per_node' : 32,
            'strategy' : 'simple',
        }

    user_opts.update(**kwargs)

    return user_opts


def create_parsl_config(user_opts, spack_opts=[]):
    hostname = socket.gethostname()
    system_opts = None
    if 'polaris' in hostname:
        system_opts = POLARIS_OPTS
    elif 'aurora' in hostname:
        system_opts = AURORA_OPTS

    provider = create_provider_by_hostname(user_opts, system_opts, spack_opts)
    executor = create_executor_by_hostname(user_opts, system_opts, provider)
    checkpoints = get_all_checkpoints(user_opts["run_dir"])
    config = Config(
            checkpoint_mode='task_exit',
            executors=[executor],
            checkpoint_files=checkpoints,
            run_dir=user_opts["run_dir"],
            strategy=user_opts.get("strategy", "none"),
            retries=user_opts.get("retries", 5),
            app_cache=True,
            monitoring=MonitoringHub(
                hub_address=address_by_interface('bond0'),
                monitoring_debug=False,
                resource_monitoring_interval=10,
            ),
    )

    return config


def hash_name(string: str) -> str:
    """Create something that looks like abcd-abcd-abcd-abcd from a string."""
    strhash = hashlib.shake_128(bytes(string, encoding='utf8')).hexdigest(16)
    return '-'.join(strhash[i*4:i*4+4] for i in range(4))


def subrun_dir(prefix: pathlib.Path, subrun: int, step: int=2, depth: int=2, width: int=6):
    """Returns a path with directory structure like XXXX00/XXXXXX.
    Number of 0s set by depth. Left padding set by width."""
    width = max(width, len(str(subrun)))
    result = prefix
    if depth < 1:
        raise RuntimeError("Must set depth >= 1")

    for i in reversed(range(0, depth)):
        q = 10**(step * i)
        path_element = q * (subrun // q)
        result /= f'{path_element:0{width}d}'

    return result
