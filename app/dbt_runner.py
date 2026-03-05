# app/dbt_runner.py
import os
import subprocess
from typing import Optional, Tuple

def run_dbt_capture(project_dir: str, cmd: str, target: Optional[str] = None, profiles_dir: Optional[str] = None) -> Tuple[int, str]:
    """
    Run a dbt command and capture combined stdout/stderr as text.

    Args:
        project_dir: folder containing dbt_project.yml
        cmd: 'run' | 'test' | 'build' | 'compile' | 'docs generate' etc
        target: profiles target name (e.g., 'dev')
        profiles_dir: optional profiles directory (defaults to ~/.dbt)
    Returns:
        (return_code, output_text)
    """
    project_dir = os.path.abspath(project_dir)
    if not os.path.isdir(project_dir):
        raise FileNotFoundError(f"dbt project_dir not found: {project_dir}")

    args = ["dbt"] + cmd.split()
    args += ["--project-dir", project_dir]
    if target:
        args += ["--target", target]
    if profiles_dir:
        args += ["--profiles-dir", profiles_dir]

    env = os.environ.copy()

    # Windows: ensure we can resolve 'dbt' from venv Scripts if present
    # If running inside the same venv as Sidecar, PATH should already be good.
    proc = subprocess.Popen(
        args,
        cwd=project_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        shell=False,
    )
    out_lines = []
    if proc.stdout:
        for line in proc.stdout:
            out_lines.append(line.rstrip("\n"))
    rc = proc.wait()
    return rc, "\n".join(out_lines)
