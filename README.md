# OpenRelik worker for Dissect (NCSC-NL)

This worker wraps the [Fox-IT Dissect](https://github.com/fox-it/dissect) tooling that powers
some IR playbooks. It loads disk images provided by OpenRelik,
executes the requested Dissect recipes, and writes the resulting artefacts back into the
workflow for download or further automation.

## What you can run from the OpenRelik UI

- **Dissect target-info (`run_target_info`)** – single-click runs the standard `target-info`
  recipe with no additional configuration.
- **Dissect query (`run_query`)** – choose any Dissect console script (for example
  `target-query`, `target-dd`, `target-shell`) and provide optional CLI arguments. The worker
  captures stdout/stderr, raises clear errors when the tool exits
  unexpectedly, and stores the textual output alongside workflow metadata.
- **Target-query bundle (`run_target_query_bundle`)** – one click runs a curated series of
  `target-query` presets (`mft_timeline`, `evtx`, `shimcache`, `amcache`, `jumplist`). Each
  preset is piped through `rdump -C --multi-timestamp` so you receive CSV files automatically.

Both options appear as separate tasks inside OpenRelik, so analysts can choose simple
ad-hoc queries or a richer triage bundle without leaving the UI.

## Installation instructions

Add the worker to your OpenRelik `docker-compose` stack:

```
  openrelik-worker-dissect-ncsc-nl:
    container_name: openrelik-worker-dissect-ncsc-nl
    image: ghcr.io/julianghill/openrelik-worker-dissect-ncsc-nl:latest
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
      - OPENRELIK_PYDEBUG=0
    volumes:
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-dissect-ncsc-nl"
```

## Local development

```
uv sync --group test
uv run pytest -s --cov=.
```

To run the worker locally with Redis available:

```
REDIS_URL=redis://localhost:6379/0 \
uv run celery --app=src.app worker --task-events --concurrency=1 --loglevel=INFO
```

## Notes

- Dissect and its plugin ecosystem are installed from PyPI when the worker image is built.
- Ensure the host provides the filesystem libraries Dissect needs to mount your evidence
  (for example `libewf`, `libguestfs-tools`) if you operate outside Docker.
- The preset list for the bundle lives in `src/target_query_bundle.py` (`TARGET_QUERY_BUNDLE`).
  Tweak that list to add new `target-query -f` arguments or rename the generated CSV files.

##### Obligatory Fine Print
This is not an official product of Fox-IT, NCSC-NL, or any commercial entity. It is
community code packaged for OpenRelik.
