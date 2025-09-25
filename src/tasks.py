"""Celery tasks for running Dissect queries on disk images."""

from __future__ import annotations

import contextlib
import io
import shlex
import sys
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path
from typing import Iterable, Sequence

from celery import signals
from celery.utils.log import get_task_logger

# API docs - https://openrelik.github.io/openrelik-worker-common/openrelik_worker_common/index.html
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.logging import Logger
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-dissect-ncsc-nl.tasks.run_query"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Dissect query runner",
    "description": "Execute any Dissect console script (for example `target-info`) against forensic disk images and capture the textual result.",
    "task_config": [
        {
            "name": "query",
            "label": "Dissect tool",
            "description": "Name of the Dissect console script to execute (required, e.g. target-info, target-query, target-dd).",
            "type": "text",
            "required": True,
        },
        {
            "name": "arguments",
            "label": "Additional CLI arguments",
            "description": "Optional extra arguments passed verbatim to the Dissect tool (one per line or shell-style string).",
            "type": "textarea",
            "required": False,
        },
    ],
}

DEFAULT_QUERY = "target-info"

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))


def _parse_argument_string(argument_string: str | None) -> list[str]:
    """Parse user supplied argument string into a list of tokens."""

    if not argument_string:
        return []

    stripped = argument_string.strip()
    if not stripped:
        return []

    try:
        return shlex.split(stripped, comments=False, posix=True)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Unable to parse Dissect arguments: {exc}") from exc


def quote_command(command: Iterable[str]) -> str:
    """Return a shell-escaped command string for logging/metadata."""

    return " ".join(shlex.quote(part) for part in command)


def _resolve_console_script(script: str) -> tuple[str, str]:
    """Return module and callable that implement a console script."""

    eps = entry_points()

    try:
        matches = list(eps.select(group="console_scripts", name=script))
    except AttributeError:  # pragma: no cover - compatibility for <3.10
        matches = [ep for ep in eps.get("console_scripts", []) if ep.name == script]

    if not matches:
        raise RuntimeError(f"Unable to locate Dissect console script '{script}'")

    target = matches[0].value
    module_path, _, attr = target.partition(":")
    return module_path, attr or "main"


def invoke_console_script(
    script: str,
    args: Sequence[str],
    *,
    stdin_data: bytes | str | None = None,
    decode_stdout: bool = True,
) -> tuple[int, str | bytes, str | bytes]:
    """Execute a console script exposed by Dissect and capture stdio."""

    module_path, attr = _resolve_console_script(script)
    module = import_module(module_path)
    function = getattr(module, attr)

    stdout_bytes = io.BytesIO()
    stderr_bytes = io.BytesIO()
    stdout_capture = io.TextIOWrapper(stdout_bytes, encoding="utf-8")
    stderr_capture = io.TextIOWrapper(stderr_bytes, encoding="utf-8")

    stdin_buffer = None
    if stdin_data is not None:
        if isinstance(stdin_data, str):
            stdin_bytes = stdin_data.encode("utf-8")
        else:
            stdin_bytes = stdin_data
        stdin_buffer = io.BytesIO(stdin_bytes)
        stdin_buffer.seek(0)

    original_argv = sys.argv
    sys.argv = [script, *args]
    try:
        with contextlib.redirect_stdout(stdout_capture), contextlib.redirect_stderr(stderr_capture):
            if stdin_buffer is not None:
                sys.stdin = io.TextIOWrapper(stdin_buffer, encoding="utf-8")
            try:
                result = function()
            except SystemExit as exit_exc:  # pragma: no cover - defensive guard
                result = exit_exc.code
    finally:
        sys.argv = original_argv
        sys.stdin = sys.__stdin__
        stdout_capture.flush()
        stderr_capture.flush()
        if stdin_buffer is not None:
            stdin_buffer.close()

    if isinstance(result, int):
        exit_code = result
    elif result in (None, ""):
        exit_code = 0
    else:
        exit_code = 1

    stdout_raw = stdout_bytes.getvalue()
    stderr_raw = stderr_bytes.getvalue()

    if decode_stdout:
        stdout_result: str | bytes = stdout_raw.decode("utf-8", errors="replace")
        stderr_result: str | bytes = stderr_raw.decode("utf-8", errors="replace")
    else:
        stdout_result = stdout_raw
        stderr_result = stderr_raw

    return exit_code, stdout_result, stderr_result


def _invoke_query(script: str, args: list[str]) -> tuple[int, str, str]:
    """Execute a Dissect console script inside the current interpreter."""

    return invoke_console_script(script, args)


def send_progress(task) -> None:
    """Emit a noop progress event when the worker supports it."""

    send_event = getattr(task, "send_event", None)
    if callable(send_event):
        send_event("task-progress", data=None)


def _run_query(
    task,
    *,
    pipe_result: str | None,
    input_files: list | None,
    output_path: str | None,
    workflow_id: str | None,
    task_config: dict | None,
) -> str:
    log_root.bind(workflow_id=workflow_id)
    logger.info("Starting Dissect query execution", extra={"workflow_id": workflow_id})

    if not output_path:
        raise RuntimeError("Output path is required for Dissect results")

    config = task_config or {}
    query_name = (config.get("query") or "").strip()
    if not query_name:
        raise RuntimeError("No Dissect console script provided. Please specify a query to run.")
    argument_tokens = _parse_argument_string(config.get("arguments"))

    logger.debug(
        "Resolved configuration",
        extra={
            "query": query_name,
            "arguments": argument_tokens,
        },
    )

    resolved_inputs = get_input_files(pipe_result, input_files or [])
    if not resolved_inputs:
        raise RuntimeError("No input files available for Dissect")

    output_files = []
    per_file_meta = []

    for entry in resolved_inputs:
        source_path = entry.get("path")
        if not source_path:
            logger.warning("Skipping entry without a path", extra={"entry": entry})
            continue

        display_name = entry.get("display_name") or Path(source_path).name
        output_display_name = f"{Path(display_name).stem}-{query_name}"
        output_file = create_output_file(
            output_path,
            display_name=output_display_name,
            extension="txt",
            data_type="openrelik:dissect:query-result",
        )

        command_tokens = [query_name, *argument_tokens, source_path]
        command_str = quote_command(command_tokens)
        logger.info(
            "Executing Dissect tool",
            extra={"command": command_str, "source": source_path, "output": output_file.path},
        )

        exit_code, stdout, stderr = _invoke_query(query_name, argument_tokens + [source_path])

        send_progress(task)

        if exit_code != 0:
            logger.error(
                "Dissect tool failed",
                extra={
                    "command": command_str,
                    "exit_code": exit_code,
                    "stderr": stderr,
                },
            )
            raise RuntimeError(
                stderr.strip() or f"Dissect tool '{query_name}' failed for {display_name} with exit code {exit_code}"
            )

        with open(output_file.path, "w", encoding="utf-8") as handle:
            handle.write(stdout)

        if stderr.strip():
            logger.warning(
                "Dissect tool reported warnings",
                extra={"command": command_str, "stderr": stderr},
            )

        output_files.append(output_file.to_dict())
        per_file_meta.append(
            {
                "input": display_name,
                "output": output_file.display_name if hasattr(output_file, "display_name") else output_display_name,
                "command": command_str,
                "stderr": stderr.strip(),
            }
        )

    if not output_files:
        raise RuntimeError("Dissect did not generate any output")

    base_command = quote_command([query_name, *argument_tokens])
    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=base_command,
        meta={
            "query": query_name,
            "arguments": argument_tokens,
            "results": per_file_meta,
        },
    )


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def run_query(
    self,
    pipe_result: str | None = None,
    input_files: list | None = None,
    output_path: str | None = None,
    workflow_id: str | None = None,
    task_config: dict | None = None,
) -> str:
    """Celery task entrypoint that wraps :func:`_run_query`."""

    return _run_query(
        self,
        pipe_result=pipe_result,
        input_files=input_files,
        output_path=output_path,
        workflow_id=workflow_id,
        task_config=task_config,
    )
