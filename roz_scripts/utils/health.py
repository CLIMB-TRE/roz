"""
Versatile k8s health signalling for roz daemons.

Deliberately stdlib-only: this module is imported both by the daemons
(mscape/pathsafe ingest, chimera_runner, etc, which pull in boto3/kubernetes/
onyx/varys) and by the `roz_healthcheck` probe entrypoint, which runs on
every liveness/readiness tick and must not pay the cost - or the failure
risk - of importing any of that.

Two distinct signals are tracked, on a shared-storage state directory so an
external probe process can read them without touching the daemon:

- The main process's own receive-loop progress (`HealthState.heartbeat()`),
  which only proves the daemon's control loop is iterating.
- Per-job worker progress (`JobHeartbeat.beat()`), written from inside the
  code path that actually does the work (pipeline polling, S3/Onyx calls),
  so a wedged worker can be detected even though the main loop is fine.

`evaluate()` is the pure function both the probe CLI and unit tests use to
turn the state directory into a liveness verdict.
"""

import json
import os
import sys
import threading
import time
from pathlib import Path

SCHEMA_VERSION = 1

MAIN_LOOP_STALE_S = 600
JOB_HEARTBEAT_STALE_S = 900
DEADLINE_MULTIPLIER = 1.1

DEFAULT_STATE_DIR = "/tmp/roz-health"


def get_health_dir() -> str:
    """
    Resolve the health state directory. Production sets $ROZ_HEALTH_DIR to a
    path on shared storage (e.g. /shared/team/health/<deployment-name>); the
    default here is only a fallback for local runs/tests.
    """
    return os.environ.get("ROZ_HEALTH_DIR", DEFAULT_STATE_DIR)


def _now_ns() -> int:
    return time.time_ns()


def _atomic_write(path: Path, content: str) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    with open(tmp_path, "w") as fh:
        fh.write(content)
    os.replace(tmp_path, path)


def _read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


class HealthState:
    """
    Main-process handle for a daemon's own health state. One instance per
    process, constructed once at startup.
    """

    def __init__(self, state_dir):
        self.state_dir = Path(state_dir)
        self.jobs_dir = self.state_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self.pid = os.getpid()
        self.started_ns = _now_ns()
        self._deps = {}

        # State on this path may outlive the process that wrote it (it lives
        # on shared storage so it survives a restart for post-mortem). A
        # fresh process has no legitimate in-flight jobs yet and must not be
        # judged fatal because of its dead predecessor's leftovers.
        (self.state_dir / "fatal").unlink(missing_ok=True)
        for job_file in self.jobs_dir.glob("*.json"):
            job_file.unlink(missing_ok=True)

        self._write_state()

    def _write_state(self) -> None:
        state = {
            "schema": SCHEMA_VERSION,
            "pid": self.pid,
            "started_ns": self.started_ns,
            "last_loop_ns": _now_ns(),
            "deps": self._deps,
        }
        _atomic_write(self.state_dir / "state.json", json.dumps(state))

    def heartbeat(self) -> None:
        """Call once per iteration of the main receive loop."""
        with self._lock:
            self._write_state()

    def record_dep_ok(self, name: str) -> None:
        """Record that `name` (e.g. "onyx", "s3", "varys") was just reached successfully."""
        with self._lock:
            self._deps[name] = _now_ns()
            self._write_state()

    def mark_fatal(self, reason: str, alert_fn=None) -> None:
        """
        Mark this process as unrecoverable so the liveness probe fails and
        k8s restarts the pod. Idempotent - safe to call more than once.

        alert_fn, if given, is called with `reason` before the marker is
        written, so callers can send an admin alert without this module
        needing to know how to do that itself.
        """
        if alert_fn is not None:
            try:
                alert_fn(reason)
            except Exception:
                pass

        with self._lock:
            _atomic_write(
                self.state_dir / "fatal",
                json.dumps({"reason": reason, "ts_ns": _now_ns(), "pid": self.pid}),
            )

    def clear_job(self, uuid: str) -> None:
        """
        Remove a job's heartbeat file once the main process has observed it
        finish (success, failure, or requeue) - called from the main
        process, since it doesn't hold the worker's JobHeartbeat instance.
        """
        (self.jobs_dir / f"{uuid}.json").unlink(missing_ok=True)


class JobHeartbeat:
    """
    Worker-side handle for a single in-flight job. `budget_s` is the
    dynamically-computed budget for the current stage (e.g. the pipeline
    timeout) - call beat() with a new `budget_s` whenever the job moves into
    a new stage with a different natural budget (e.g. from "running the
    pipeline" to "uploading results"), so the deadline check tracks the
    current stage rather than the job's original, possibly very long,
    overall budget.
    """

    def __init__(self, state_dir, uuid: str, budget_s: float, pid: int | None = None):
        self.state_dir = Path(state_dir)
        self.jobs_dir = self.state_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

        self.uuid = uuid
        self.pid = pid if pid is not None else os.getpid()
        self.stage_start_ns = None
        self.budget_s = budget_s

    def __enter__(self):
        self.beat(stage="started")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.clear()
        return False

    def beat(self, stage: str, budget_s: float | None = None) -> None:
        now_ns = _now_ns()

        if budget_s is not None:
            self.budget_s = budget_s
            self.stage_start_ns = now_ns
        elif self.stage_start_ns is None:
            self.stage_start_ns = now_ns

        record = {
            "uuid": self.uuid,
            "pid": self.pid,
            "stage": stage,
            "stage_ns": now_ns,
            "stage_start_ns": self.stage_start_ns,
            "stage_budget_s": self.budget_s,
        }
        _atomic_write(self.jobs_dir / f"{self.uuid}.json", json.dumps(record))

    def clear(self) -> None:
        (self.jobs_dir / f"{self.uuid}.json").unlink(missing_ok=True)


def evaluate(
    state_dir,
    main_loop_stale_s: float = MAIN_LOOP_STALE_S,
    job_heartbeat_stale_s: float = JOB_HEARTBEAT_STALE_S,
    deadline_multiplier: float = DEADLINE_MULTIPLIER,
) -> tuple[bool, str]:
    """
    Inspect the state directory and return (ok, reason). Pure and
    side-effect-free - used directly by unit tests and by the
    `roz_healthcheck` CLI.
    """

    state_dir = Path(state_dir)
    now_ns = time.time_ns()

    fatal = _read_json(state_dir / "fatal")
    if fatal is not None:
        return False, f"fatal: {fatal.get('reason', 'unknown')}"

    state = _read_json(state_dir / "state.json")
    if state is None:
        return False, "no state file found"

    last_loop_ns = state.get("last_loop_ns", 0)
    if (now_ns - last_loop_ns) / 1e9 > main_loop_stale_s:
        return False, "main loop heartbeat stale"

    jobs_dir = state_dir / "jobs"
    if jobs_dir.exists():
        for job_file in sorted(jobs_dir.glob("*.json")):
            job = _read_json(job_file)
            if job is None:
                continue

            stage_ns = job.get("stage_ns", 0)
            if (now_ns - stage_ns) / 1e9 > job_heartbeat_stale_s:
                return False, f"job {job.get('uuid')} heartbeat stale"

            stage_start_ns = job.get("stage_start_ns")
            stage_budget_s = job.get("stage_budget_s")
            if stage_start_ns and stage_budget_s:
                hard_deadline_ns = stage_start_ns + int(
                    stage_budget_s * deadline_multiplier * 1e9
                )
                if now_ns > hard_deadline_ns:
                    return False, f"job {job.get('uuid')} exceeded stage deadline"

    return True, "ok"


def cli() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="roz k8s health probe")
    parser.add_argument(
        "--mode", choices=["liveness", "readiness", "startup"], default="liveness"
    )
    parser.add_argument("--state-dir", default=os.environ.get("ROZ_HEALTH_DIR"))
    args = parser.parse_args()

    if not args.state_dir:
        print(
            "ROZ_HEALTH_DIR not set and --state-dir not provided", file=sys.stderr
        )
        return 1

    if args.mode == "startup":
        ok = (Path(args.state_dir) / "state.json").exists()
        print("ok" if ok else "no state file yet")
        return 0 if ok else 1

    if args.mode == "readiness":
        state = _read_json(Path(args.state_dir) / "state.json")
        if state is None:
            print("no state file found")
            return 1
        deps = state.get("deps", {})
        if not deps:
            # No dependency contact recorded yet is not the same as failure -
            # readiness is observability-only here, so default to healthy.
            print("no dependency contact recorded yet")
            return 0
        print("ok")
        return 0

    ok, reason = evaluate(
        args.state_dir,
        main_loop_stale_s=float(
            os.environ.get("ROZ_MAIN_LOOP_STALE_S", MAIN_LOOP_STALE_S)
        ),
        job_heartbeat_stale_s=float(
            os.environ.get("ROZ_JOB_HEARTBEAT_STALE_S", JOB_HEARTBEAT_STALE_S)
        ),
        deadline_multiplier=float(
            os.environ.get("ROZ_DEADLINE_MULTIPLIER", DEADLINE_MULTIPLIER)
        ),
    )
    print(reason)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(cli())
