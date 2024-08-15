# sbnd_parsl

Classes and functions for running SBND workflows on Polaris at ALCF.

## Installation

To install alongside an existing conda environment, use

```
pip install --no-build-isolation --no-deps .
```

For developing, include the optional `-e` flag before the `.` in the above line.

## Usage

`sbnd_parsl` provides a three-level class structure for structuring job submission:

- `Stage` objects for defining individual tasks, e.g., running a single FCL file.
- `Workflow` objects for composing multiple stages together with dependencies.
- `WorkflowExecutor` objects for configuring workflows from user settings.

### Basic Example

```python
#!/usr/bin/env python3

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor


class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_workflow(self):
        stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]

        self.workflow = Workflow(stage_order, self.fcls)
        s = Stage(StageType.DETSIM)
        s.run_dir = self.run_opts['output']

        # workflow will automatically fill in g4 and gen stages, with
        # run_dir inherited from detsim stage
        self.workflow.add_final_stage(s)

def main(settings):
    wfe = SimpleWorkflowExecutor(settings)
    wfe.execute()

if __name__ == '__main__':
    settings = {
        'run': {
            'output': 'output',
            'fclpath': '.'
        },
        'larsoft': {},
        'fcls': {
            'gen': 'gen.fcl',
            'g4': 'g4.fcl',
            'detsim': 'detsim.fcl'
        },
        'queue': {},
        'workflow': {}
    }
    # or load from a JSON file

    main(settings)
```

Running the above code will print some dummy output showing the sequence of
commands required to generate the `DETSIM` stage:
```
lar -c gen.fcl  --output output/gen.root
lar -c g4.fcl -s output/gen.root --output output/g4.root
lar -c detsim.fcl -s output/g4.root --output output/detsim.root
```

### Adding Parsl

To actually submit work with Parsl, you need to define a `runfunc` for at least
the final `Stage` added to the `Workflow`. The `runfunc` must have the minimal
function signature in the extended example below, and should return a list of
Parsl `File` objects. 

```python
...
from parsl.data_provider.files import File
from parsl.app.app import bash_app

@bash_app(cache=True)
def fcl_future(inputs=[], outputs=[]):
    """Return a shell script containing the commands to run LArSoft and produce
    the output file from the arguments."""
    my_bash_script = ...
    return my_bash_script

def runfunc(self, fcl, input_files, output_dir):
    output_file = File(...) # user specifies the filename
    future = fcl_future(outputs=[output_file])
    return future.outputs

...
class SimpleWorkflowExecutor(WorkflowExecutor):
    def __init__(self, settings):
        super().__init__(settings)

    def setup_workflow(self):
        stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]

        self.workflow = Workflow(stage_order, self.fcls)
        s = Stage(StageType.DETSIM)
        s.run_dir = self.run_opts['output']
>>>     s.runfunc = runfunc
...
```

Note that `runfunc` function takes `self` as the first argument, despite being
defined in the global scope. `sbnd_parsl` sets up the appropriate bindings at
runtime so that `runfunc` can access the member variables and functions of the
`Stage` it is bound to. This is useful to add logic based on the stage's type
or `.fcl` file.

### Passing extra arguments to `runfunc`

You can extend `runfunc` with arbitrary arguments while preserving the function
signature using `functools.partial`. Below, we modify `runfunc` from the
original example to pass the `WorkflowExecutor` object as an extra argument.
This allows us to access the user settings within `runfunc`:

```Python
import functools

def my_runfunc(self, fcl, input_files, output_dir, executor):
    output_filepath = executor.output_dir
    ...

class SimpleWorkflowExecutor(WorkflowExecutor):
    ...

    def setup_workflow(self):
        ...
>>>     s.runfunc = functools.partial(my_runfunc, executor=self) 
```
