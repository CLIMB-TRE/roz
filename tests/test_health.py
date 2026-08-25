import json
import time

import pytest

from roz_scripts.utils.health import HealthState, JobHeartbeat, evaluate


@pytest.fixture
def state_dir(tmp_path):
    return tmp_path / "health"


def test_fresh_state_is_healthy(state_dir):
    HealthState(state_dir)
    ok, reason = evaluate(state_dir)
    assert ok
    assert reason == "ok"


def test_stale_main_loop_fails(state_dir):
    health = HealthState(state_dir)
    health.heartbeat()

    state_path = state_dir / "state.json"
    state = json.loads(state_path.read_text())
    state["last_loop_ns"] = time.time_ns() - 700 * 1_000_000_000
    state_path.write_text(json.dumps(state))

    ok, reason = evaluate(state_dir, main_loop_stale_s=600)
    assert not ok
    assert "main loop" in reason


def test_fatal_marker_fails(state_dir):
    health = HealthState(state_dir)
    health.mark_fatal("worker wedged")

    ok, reason = evaluate(state_dir)
    assert not ok
    assert "worker wedged" in reason


def test_fresh_job_heartbeat_is_healthy(state_dir):
    HealthState(state_dir)
    job = JobHeartbeat(state_dir, uuid="abc", budget_s=3600)
    job.beat(stage="running_pipeline")

    ok, reason = evaluate(state_dir)
    assert ok


def test_stale_job_heartbeat_fails(state_dir):
    HealthState(state_dir)
    job = JobHeartbeat(state_dir, uuid="abc", budget_s=3600)
    job.beat(stage="running_pipeline")

    job_path = state_dir / "jobs" / "abc.json"
    record = json.loads(job_path.read_text())
    record["stage_ns"] = time.time_ns() - 1000 * 1_000_000_000
    job_path.write_text(json.dumps(record))

    ok, reason = evaluate(state_dir, job_heartbeat_stale_s=900)
    assert not ok
    assert "heartbeat stale" in reason


def test_long_legitimate_job_stays_healthy_via_fresh_heartbeats(state_dir):
    """A job with a huge overall budget (e.g. a 24h+ nextflow run) must not
    be flagged just because it has been running a long time - only a stale
    heartbeat or an exceeded per-stage deadline should fail it."""

    HealthState(state_dir)
    job = JobHeartbeat(state_dir, uuid="abc", budget_s=100_000)

    job_path = state_dir / "jobs" / "abc.json"
    job.beat(stage="running_pipeline")
    record = json.loads(job_path.read_text())
    # Simulate a stage that started long ago but is still within its budget,
    # with a heartbeat that just landed.
    record["stage_start_ns"] = time.time_ns() - 90_000 * 1_000_000_000
    job_path.write_text(json.dumps(record))

    ok, reason = evaluate(state_dir)
    assert ok


def test_job_exceeding_stage_deadline_fails(state_dir):
    HealthState(state_dir)
    job = JobHeartbeat(state_dir, uuid="abc", budget_s=100)
    job.beat(stage="running_pipeline")

    job_path = state_dir / "jobs" / "abc.json"
    record = json.loads(job_path.read_text())
    # Still heartbeating (stage_ns fresh) but well past 1.1x its 100s budget.
    record["stage_start_ns"] = time.time_ns() - 200 * 1_000_000_000
    job_path.write_text(json.dumps(record))

    ok, reason = evaluate(state_dir, deadline_multiplier=1.1)
    assert not ok
    assert "deadline" in reason


def test_stage_reset_extends_deadline_after_pipeline_timeout(state_dir):
    """The mitigation for the tight 1.1x multiplier: resetting budget_s on a
    stage transition (e.g. into post-pipeline teardown) gives that stage its
    own fresh deadline, rather than inheriting the original pipeline budget."""

    HealthState(state_dir)
    job = JobHeartbeat(state_dir, uuid="abc", budget_s=100)
    job.beat(stage="running_pipeline")

    # Pipeline stage ran right up to its deadline...
    job_path = state_dir / "jobs" / "abc.json"
    record = json.loads(job_path.read_text())
    record["stage_start_ns"] = time.time_ns() - 109 * 1_000_000_000
    job_path.write_text(json.dumps(record))

    # ...then transitions into a new stage with its own budget.
    job.beat(stage="uploading_results", budget_s=600)

    ok, reason = evaluate(state_dir, deadline_multiplier=1.1)
    assert ok


def test_restart_clears_previous_process_leftovers(state_dir):
    health = HealthState(state_dir)
    job = JobHeartbeat(state_dir, uuid="abc", budget_s=100)
    job.beat(stage="running_pipeline")
    health.mark_fatal("simulated crash")

    assert (state_dir / "fatal").exists()
    assert (state_dir / "jobs" / "abc.json").exists()

    # Simulate the pod restarting: a fresh HealthState on the same shared dir.
    HealthState(state_dir)

    assert not (state_dir / "fatal").exists()
    assert not (state_dir / "jobs" / "abc.json").exists()

    ok, reason = evaluate(state_dir)
    assert ok
