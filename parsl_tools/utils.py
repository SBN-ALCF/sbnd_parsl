import socket

from parsl.addresses import address_by_interface

from parsl.providers import PBSProProvider, LocalProvider
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.launchers import MpiExecLauncher, GnuParallelLauncher


def create_provider_by_hostname(user_opts):

    hostname = socket.gethostname()
    if 'polaris' in hostname:
        # TODO: The worker init should be somewhere outside Corey's homedir
        provider = PBSProProvider(
            account         = user_opts["allocation"],
            queue           = user_opts["queue"],
            nodes_per_block = user_opts["nodes_per_block"],
            cpus_per_node   = user_opts["cpus_per_node"],
            init_blocks     = 1,
            max_blocks      = 1,
            walltime        = user_opts["walltime"],
            scheduler_options = '#PBS -l filesystems=home:grand:eagle\n#PBS -l place=scatter',
            launcher        = MpiExecLauncher(bind_cmd="--cpu-bind"),
            worker_init     = "module load conda/2023-10-04.lua; conda activate; source /lus/grand/projects/neutrinoGPU/software/parsl-conda-2023-10-04/bin/activate",
        )
        return provider
    else:
        return LocalProvider()

def create_executor_by_hostname(user_opts, provider):

    hostname = socket.gethostname()

    if 'polaris' in hostname:
        from parsl import HighThroughputExecutor
        return HighThroughputExecutor(
                    label="htex",
                    heartbeat_period=15,
                    heartbeat_threshold=120,
                    worker_debug=True,
                    max_workers=user_opts["cpus_per_node"],
                    cores_per_worker=1,
                    address=address_by_interface("bond0"),
                    address_probe_timeout=120,
                    cpu_affinity="alternating",
                    prefetch_capacity=0,
                    provider=provider,
                    block_error_handler=False
                )
    
    else:
        # default: 
        from parsl import ThreadPoolExecutor
        return ThreadPoolExecutor(
            label="threads",
            max_threads=user_opts["cpus_per_node"]
        )
    

def create_default_useropts(allocation="datascience"):


    hostname = socket.gethostname()

    if 'polaris' in hostname:
        # Then, we're likely on the login node of polaris and want to submit via parsl:
        user_opts = {
            # Node setup: activate necessary conda environment and such.
            'worker_init': '',
            'scheduler_options': '',
            'allocation': allocation,
            'queue': 'debug',
            'walltime': '1:00:00',
            'nodes_per_block' : 1,
            'cpus_per_node' : 32,
            'strategy' : 'simple',
        }
    
    else:
        # We're likely running locally
        user_opts = {
            'cpus_per_node' : 32,
            'strategy' : 'simple',
        }


    return user_opts
