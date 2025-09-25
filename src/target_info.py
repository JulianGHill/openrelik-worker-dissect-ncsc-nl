"""Celery task dedicated to the Dissect target-info recipe."""

from __future__ import annotations

from celery import signals
from .app import celery
from .tasks import DEFAULT_QUERY, _run_query, log_root as shared_log_root

TASK_NAME = "openrelik-worker-dissect-ncsc-nl.tasks.run_target_info"
TASK_METADATA = {
    "display_name": "Dissect target-info",
    "description": "Run the Dissect target-info recipe against forensic disk images and capture the textual result.",
    "task_config": [],
}

log_root = shared_log_root


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    if sender.name != TASK_NAME:
        return

    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def run_target_info(
    self,
    pipe_result: str | None = None,
    input_files: list | None = None,
    output_path: str | None = None,
    workflow_id: str | None = None,
    task_config: dict | None = None,
) -> str:
    """Run the Dissect target-info recipe with no additional configuration."""

    return _run_query(
        self,
        pipe_result=pipe_result,
        input_files=input_files,
        output_path=output_path,
        workflow_id=workflow_id,
        task_config={"query": DEFAULT_QUERY, "arguments": ""},
    )
