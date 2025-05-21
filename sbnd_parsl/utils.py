import socket
import pathlib
import hashlib

from parsl.config import Config

from parsl.addresses import address_by_interface
from parsl.utils import get_all_checkpoints
from parsl.providers import PBSProProvider, LocalProvider
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.launchers import MpiExecLauncher, GnuParallelLauncher
from parsl.monitoring.monitoring import MonitoringHub


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
                f'source ~/.venv/{venv_name}/bin/activate'
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


def create_provider_by_hostname(user_opts, spack_opts):
    hostname = socket.gethostname()

    if len(spack_opts) >= 2:
        spack_top = spack_opts[0]
        version = spack_opts[1]
        software = spack_opts[2]
        worker_init = _worker_init(spack_top=spack_top, spack_version=version, software=software, mps=True, venv_name=user_opts.get("worker_venv_name", "sbn"))
    else:
        mps = 'polaris' in hostname
        worker_init = _worker_init(mps=mps, venv_name='sbn')

    if 'polaris' in hostname:
        return PBSProProvider(
            account         = user_opts["allocation"],
            queue           = user_opts.get("queue", "debug"),
            nodes_per_block = user_opts.get("nodes_per_block", 1),
            cpus_per_node   = user_opts.get("cpus_per_node", 32),
            init_blocks     = user_opts.get("init_blocks", 1),
            max_blocks      = user_opts.get("max_blocks", 1),
            walltime        = user_opts.get("walltime", "1:00:00"),
            cmd_timeout     = 240,
            scheduler_options = '#PBS -l filesystems=home:grand:eagle\n#PBS -l place=scatter',
            launcher        = MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"),
            worker_init     = worker_init
        )
    elif 'aurora' in hostname:
        return PBSProProvider(
            account         = user_opts["allocation"],
            queue           = user_opts.get("queue", "debug"),
            nodes_per_block = user_opts.get("nodes_per_block", 1),
            cpus_per_node   = user_opts.get("cpus_per_node", 208),
            init_blocks     = user_opts.get("init_blocks", 1),
            max_blocks      = user_opts.get("max_blocks", 1),
            walltime        = user_opts.get("walltime", "1:00:00"),
            cmd_timeout     = 240,
            scheduler_options = '#PBS -l filesystems=home:flare',
            launcher        = MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--ppn 1"),
            worker_init     = worker_init
        )

def create_executor_by_hostname(user_opts, provider):
    hostname = socket.gethostname()
    if 'polaris' in hostname:
        from parsl import HighThroughputExecutor
        return HighThroughputExecutor(
            label="htex",
            heartbeat_period=15,
            heartbeat_threshold=120,
            worker_debug=True,
            max_workers_per_node=user_opts["cpus_per_node"],
            cores_per_worker=user_opts["cores_per_worker"],
            available_accelerators=ngpus,
            address=address_by_interface("bond0"),
            address_probe_timeout=120,
            cpu_affinity="alternating",
            prefetch_capacity=0,
            provider=provider,
            block_error_handler=False,
            working_dir=str(pathlib.Path(user_opts["run_dir"]) / 'cmd')
        )
    elif 'aurora' in hostname:
        # see: https://docs.alcf.anl.gov/aurora/workflows/parsl/
        from parsl import HighThroughputExecutor
        tile_names = [f'{gid}.{tid}' for gid in range(6) for tid in range(2)]
        return HighThroughputExecutor(
            label="htex",
            heartbeat_period=15,
            heartbeat_threshold=120,
            worker_debug=True,
            max_workers_per_node=user_opts["cpus_per_node"],
            available_accelerators=tile_names,
            address_probe_timeout=120,
            cpu_affinity="list:1-8,105-112:9-16,113-120:17-24,121-128:25-32,129-136:33-40,137-144:41-48,145-152:53-60,157-164:61-68,165-172:69-76,173-180:77-84,181-188:85-92,189-196:93-100,197-204",
            prefetch_capacity=0,
            provider=provider,
            block_error_handler=False,
            working_dir=str(pathlib.Path(user_opts["run_dir"]) / 'cmd')
        )
    else:
        # default: 
        from parsl import ThreadPoolExecutor
        return ThreadPoolExecutor(
            label="threads",
            max_threads=user_opts["cpus_per_node"]
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
    checkpoints = get_all_checkpoints(user_opts["run_dir"])

    provider = create_provider_by_hostname(user_opts, spack_opts)
    executor = create_executor_by_hostname(user_opts, provider)
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
