"""Celery task that runs a predefined set of Dissect target-query commands."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from celery import signals
from celery.utils.log import get_task_logger

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.logging import Logger
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery
from .tasks import invoke_console_script, quote_command, send_progress

TASK_NAME = "openrelik-worker-dissect-ncsc-nl.tasks.run_target_query_bundle"

TASK_METADATA = {
    "display_name": "Dissect target-query bundle",
    "description": "Run a curated list of target-query functions and convert results to CSV using rdump.",
    "task_config": [],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))

RDUMP_ARGS = ["-C", "--multi-timestamp"]

TARGET_QUERY_BUNDLE = [
    # {
    #     "name": "MFT timeline",
    #     "arguments": ["-f", "mft_timeline"],
    #     "output_suffix": "mft_timeline",
    # },
    {
        "name": "EVTX",
        "arguments": ["-f", "evtx"],
        "output_suffix": "evtx",
    },
    {
        "name": "Shimcache",
        "arguments": ["-f", "shimcache"],
        "output_suffix": "shimcache",
    },
    {
        "name": "Amcache.hve",
        "arguments": ["-f", "amcache"],
        "output_suffix": "amcache",
    },
    {
        "name": "Jump Lists",
        "arguments": ["-f", "jumplist"],
        "output_suffix": "jumplist",
    },
]


def _bundle_command_display(source_path: str, arguments: Iterable[str]) -> str:
    base = ["target-query", source_path, *arguments]
    return quote_command(base)


def _run_bundle(
    task,
    *,
    pipe_result: str | None,
    input_files: list | None,
    output_path: str | None,
    workflow_id: str | None,
) -> str:
    if not output_path:
        raise RuntimeError("Output path is required for Dissect results")

    log_root.bind(workflow_id=workflow_id)
    logger.info("Starting Dissect target-query bundle", extra={"workflow_id": workflow_id})

    resolved_inputs = get_input_files(pipe_result, input_files or [])
    if not resolved_inputs:
        raise RuntimeError("No input files available for Dissect")

    output_files = []
    meta_entries = []

    for entry in resolved_inputs:
        source_path = entry.get("path")
        if not source_path:
            logger.warning("Skipping entry without a path", extra={"entry": entry})
            continue

        display_name = entry.get("display_name") or Path(source_path).name
        base_name = Path(display_name).stem

        for preset in TARGET_QUERY_BUNDLE:
            command_args = [source_path, *preset["arguments"]]
            command_display = _bundle_command_display(source_path, preset["arguments"])
            logger.info("Running target-query", extra={"command": command_display})

            exit_code, query_stdout, query_stderr = invoke_console_script(
                "target-query", command_args, decode_stdout=False
            )

            send_progress(task)

            if exit_code != 0:
                logger.error(
                    "target-query preset failed",
                    extra={
                        "command": command_display,
                        "exit_code": exit_code,
                        "stderr": query_stderr,
                    },
                )
                stderr_text = (
                    query_stderr.decode("utf-8", errors="replace")
                    if isinstance(query_stderr, bytes)
                    else query_stderr
                )
                raise RuntimeError(
                    stderr_text.strip()
                    or f"target-query preset '{preset['name']}' failed for {display_name}"
                )

            rdump_exit, rdump_stdout, rdump_stderr = invoke_console_script(
                "rdump",
                RDUMP_ARGS,
                stdin_data=query_stdout,
            )

            if rdump_exit != 0:
                logger.error(
                    "rdump failed",
                    extra={
                        "command": "rdump " + " ".join(RDUMP_ARGS),
                        "stderr": rdump_stderr,
                    },
                )
                raise RuntimeError(
                    rdump_stderr.strip() or f"rdump failed for preset '{preset['name']}'"
                )

            output_file = create_output_file(
                output_path,
                display_name=f"{base_name}-{preset['output_suffix']}",
                extension="csv",
                data_type="openrelik:dissect:target-query:csv",
            )

            with open(output_file.path, "w", encoding="utf-8") as handle:
                handle.write(rdump_stdout)

            output_files.append(output_file.to_dict())
            meta_entries.append(
                {
                    "input": display_name,
                    "preset": preset["name"],
                    "target_query_command": command_display,
                    "rdump_command": quote_command(["rdump", *RDUMP_ARGS]),
                    "target_query_stderr": (
                        query_stderr.decode("utf-8", errors="replace")
                        if isinstance(query_stderr, bytes)
                        else query_stderr
                    ).strip(),
                    "rdump_stderr": rdump_stderr.strip(),
                }
            )

    if not output_files:
        raise RuntimeError("No target-query bundle outputs were generated")

    preset_names = [preset["name"] for preset in TARGET_QUERY_BUNDLE]

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=f"target-query presets: {', '.join(preset_names)}",
        meta={
            "presets": preset_names,
            "results": meta_entries,
        },
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def run_target_query_bundle(
    self,
    pipe_result: str | None = None,
    input_files: list | None = None,
    output_path: str | None = None,
    workflow_id: str | None = None,
    task_config: dict | None = None,
) -> str:
    """Run the predefined target-query bundle against every provided input file."""

    return _run_bundle(
        self,
        pipe_result=pipe_result,
        input_files=input_files,
        output_path=output_path,
        workflow_id=workflow_id,
    )


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    if sender.name != TASK_NAME:
        return

    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )
