import pytest

from sbnd_parsl.workflow import Stage, StageType, GEN, G4, DETSIM, run_stage
from sbnd_parsl.workflow import NoStageOrderException, NoFclFileException


def test_stage_init():
    s = Stage(GEN)
    # should warn, but proceed
    s = Stage('MyType')

def test_stage_add_parent_no_order():
    # raises: can't add a parent if stage order is not specified
    stage_order = [GEN, G4]
    s1 = Stage(G4)
    s2 = Stage(GEN)
    with pytest.raises(NoStageOrderException):
        s1.add_parents(s2)

def test_stage_add_parent_last_stage():
    stage_order = [GEN, G4]
    s1 = Stage(G4, stage_order=stage_order)
    s2 = Stage(GEN)
    print(s1.stage_type, stage_order)
    
    # OK: s2 is the first stage in the order, so we don't need to know
    # the fcl files for its parents
    s1.add_parents(s2)

def test_stage_add_parent_not_last_stage():
    stage_order = [GEN, G4, DETSIM]
    s1 = Stage(DETSIM, stage_order=stage_order)
    s2 = Stage(G4)
    
    # Bad: s2 is not the first stage in the order, so we might have to generate
    # its parent. Fcl dict argument is required here
    with pytest.raises(NoFclFileException):
        s1.add_parents(s2)

def test_stage_add_parent_not_last_stage2():
    stage_order = [GEN, G4, DETSIM]
    s1 = Stage(DETSIM, stage_order=stage_order)
    s2 = Stage(G4)
    fcls = {GEN: 'gen.fcl', G4: 'g4.fcl', DETSIM: 'detsim.fcl'}
    
    # OK: s2 is not the first stage in the order, but we provide fcl dict
    s1.add_parents(s2, fcls)

def test_run_stage_no_fcl():
    stage_order = [GEN, G4, DETSIM]
    s1 = Stage(DETSIM, stage_order=stage_order)
    with pytest.raises(NoFclFileException):
        next(run_stage(s1))

def test_run_stage():
    stage_order = [GEN, G4, DETSIM]
    fcls = {GEN: 'gen.fcl', G4: 'g4.fcl', DETSIM: 'detsim.fcl'}
    s1 = Stage(DETSIM, stage_order=stage_order)
    while True:
        try:
            next(run_stage(s1, fcls))
        except StopIteration:
            break

def test_combine():
    stage_order = [GEN, G4, DETSIM]
    fcls = {GEN: 'gen.fcl', G4: 'g4.fcl', DETSIM: 'detsim.fcl'}
    s1 = Stage(DETSIM, stage_order=stage_order)
    s2 = Stage(G4, stage_order=stage_order)
    s3 = Stage(GEN, stage_order=stage_order)

    # combine: when we call next() below, we should get all stages executed
    # instead of 1 per next() call since all stages are marked as combine
    s1.combine = True
    s2.combine = True
    s3.combine = True

    s2.add_parents(s3, fcls)
    s1.add_parents(s2, fcls)
    runs = 0
    while True:
        try:
            next(run_stage(s1, fcls))
            runs += 1
        except StopIteration:
            break

    assert runs == 1
