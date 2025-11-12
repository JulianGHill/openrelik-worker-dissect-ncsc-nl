# OpenRelik worker for Dissect (NCSC-NL)

This worker wraps the [Fox-IT Dissect](https://github.com/fox-it/dissect) tooling that powers
some IR playbooks. It loads disk images provided by OpenRelik,
executes the requested Dissect recipes, and writes the resulting artefacts back into the
workflow for download or further automation.

## What you can run from the OpenRelik UI

- **Dissect target-info (`run_target_info`)** – single-click runs the standard `target-info`
  recipe with no additional configuration. Optional ELK exports are available by pasting a Dissect
  writer URI (for example `elastic+http://elastic:9200?index=dissect-target-info&verify_certs=false`)
  into the new **Elastic writer URI** field. The worker transparently replays `target-info --record`
  for each evidence file and streams the records through `rdump -w` so your Elasticsearch cluster
  receives the same structured output the CLI would have produced.
- **Dissect query (`run_query`)** – choose any Dissect console script (for example
  `target-query`, `target-dd`, `target-shell`) and provide optional CLI arguments. The worker
  captures stdout/stderr, raises clear errors when the tool exits
  unexpectedly, and stores the textual output alongside workflow metadata.
- **Target-query bundle (`run_target_query_bundle`)** – one click runs a curated series of
  `target-query` presets (`evtx`, `shimcache`, `Task Bar Feature Usage`, `amcache`, `jumplist`,
  `Open/Save MRU`, `Recent Files (MRU)`, `Shortcut (LNK) Files`, `Shell Bags`, `Office Recent Files`,
  `Office Trust Records`, `Last Visited MRU`, `RunMRU`, `Windows 10 Timeline (ActivitiesCache.db)`,
  `BAM/DAM`, `SRUM`, `Prefetch`, `CapabilityAccessManager`, `UserAssist`, `Installed Services`,
  `Recycle Bin`, `Thumbcache`, `Internet Explorer file:/// History`, `Search - WordWheelQuery`,
  `USB history (registry)`, `Removable device activity`,
  `Browser (all below)`, `Browser Cookies`, `Browser Downloads`, `Browser Extensions`,
  `Browser History`, `Browser Passwords`). Each
  preset is piped through `rdump -C --multi-timestamp` so you receive CSV files automatically.
  The UI now exposes an autocomplete picker so you can run any combination of scopes including
  "all_event_logs", "mft_timeline", "application_execution", "file_folder_opening",
  "deleted_items_file_existence", "browser_activity", "external_device_usage", or simply leave the
  field on "Everything" (the default) to run the whole bundle.

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
      - DISSECT_ELASTIC_WRITER_URI=elastic+https://elastic.example:9200?index=dissect-records&api_key=${ELASTIC_API_KEY}
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
- If Dissect misses a requested preset (for example when a plugin is not shipped in your
  version), the worker logs the skip and continues with the remaining bundle items.

### Preset groups & Yara usage

- The optional `bundle_scopes` autocomplete lets you pick one or many preset groups. Leave it set to
  `Everything` to mimic the historical behaviour or add any combination of `all_event_logs`,
  `mft_timeline`, `application_execution`, `file_folder_opening`, `deleted_items_file_existence`,
  `browser_activity`, or `external_device_usage` for focused runs.
- A new **User information** scope captures account-centric artefacts (`target-query -f users`,
  `target-reg …Users\\Names\\`, etc.) so you can pull local user inventories and SAM details in one click.
- The worker now bundles `yara-python`. Rebuild and redeploy the container after pulling this change,
  and re-run `uv sync --group test` locally so your venv picks up the dependency before running tests.
- Provide a custom rule in the `yara_rule` textarea to run `target-query -f yara` alongside the
  selected presets. Leaving the field blank skips the scan. When populated, the bundle appends a
  `*-yara.csv` file for each matching target input.
- Prefer to reuse existing rule files? Supply a comma or newline separated list of rule paths or
  directories in the `yara_rule_paths` field. You can combine both inputs—the inline rule is written
  to a temporary file and passed along with any directories you provide.

## TODO

- Investigate a structured export path for registry artefacts (for example `target-reg --record` or
  a custom helper) so presets like the SAM user list can stream JSON to Elastic without ad-hoc parsers.

### Exporting to Elastic

- The **target-info**, **generic run_query** (including `target-reg`), and **target-query bundle** tasks all expose an
  **Elastic/Record writer URI** field. Paste any valid Dissect writer string (for example
  `elastic+http://elasticsearch:9200?index=dissect-target-info&verify_certs=false`) to stream
  record-formatted output directly to Elasticsearch using `rdump -w`.
- You can preconfigure credentials inside Docker by setting `DISSECT_ELASTIC_WRITER_URI`
  (or `DISSECT_RECORD_WRITER_URI`) on the worker service. When the env var is present the UI
  auto-fills the URI and you only need to flip the export toggle.
- Use the **Export to record writer** checkbox to control whether a run streams to Elastic. It is
  off by default—even if an env var is set—so you can continue saving only local files unless you
  explicitly opt in.
- For `target-info`, the worker automatically reruns the recipe with `--record` to generate the
  structured stream before handing it to `rdump`. For `target-query` and the bundle, the raw record
  stream produced during CSV conversion is reused, so exporting adds minimal overhead. When you run the
  bundle, every preset (including optional Yara scans) is streamed to the writer URI in addition to
  the CSV files saved back into OpenRelik.
- Refer to the
  [Dissect Elastic adapter docs](https://docs.dissect.tools/en/stable/api/flow/record/adapter/elastic/index.html)
  for all available URI parameters (`api_key`, `username`/`password`, `verify_certs`, `request_timeout`,
  `max_retries`, etc.). Whatever you append to the URI is passed straight through to `rdump -w`.

##### Obligatory Fine Print
This is not an official product of Fox-IT, NCSC-NL, or any commercial entity. It is
community code packaged for OpenRelik.
