import pytest

from sbnd_parsl.workflow import Stage, StageType, run_stage
from sbnd_parsl.workflow import NoStageOrderException, NoFclFileException


def test_stage_init():
    s = Stage(StageType.GEN)
    # should warn, but proceed
    s = Stage('MyType')

def test_stage_add_parent_no_order():
    # raises: can't add a parent if stage order is not specified
    stage_order = [StageType.GEN, StageType.G4]
    s1 = Stage(StageType.G4)
    s2 = Stage(StageType.GEN)
    with pytest.raises(NoStageOrderException):
        s1.add_parents(s2)

def test_stage_add_parent_last_stage():
    stage_order = [StageType.GEN, StageType.G4]
    s1 = Stage(StageType.G4, stage_order=stage_order)
    s2 = Stage(StageType.GEN)
    
    # OK: s2 is the first stage in the order, so we don't need to know
    # the fcl files for its parents
    s1.add_parents(s2)

def test_stage_add_parent_not_last_stage():
    stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]
    s1 = Stage(StageType.DETSIM, stage_order=stage_order)
    s2 = Stage(StageType.G4)
    
    # Bad: s2 is not the first stage in the order, so we might have to generate
    # its parent. Fcl dict argument is required here
    with pytest.raises(NoFclFileException):
        s1.add_parents(s2)

def test_stage_add_parent_not_last_stage2():
    stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]
    s1 = Stage(StageType.DETSIM, stage_order=stage_order)
    s2 = Stage(StageType.G4)
    fcls = {StageType.GEN: 'gen.fcl', StageType.G4: 'g4.fcl', StageType.DETSIM: 'detsim.fcl'}
    
    # OK: s2 is not the first stage in the order, but we provide fcl dict
    s1.add_parents(s2, fcls)

def test_run_stage_no_fcl():
    stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]
    s1 = Stage(StageType.DETSIM, stage_order=stage_order)
    with pytest.raises(NoFclFileException):
        next(run_stage(s1))

def test_run_stage():
    stage_order = [StageType.GEN, StageType.G4, StageType.DETSIM]
    fcls = {StageType.GEN: 'gen.fcl', StageType.G4: 'g4.fcl', StageType.DETSIM: 'detsim.fcl'}
    s1 = Stage(StageType.DETSIM, stage_order=stage_order)
    while True:
        try:
            next(run_stage(s1, fcls))
        except StopIteration:
            break
