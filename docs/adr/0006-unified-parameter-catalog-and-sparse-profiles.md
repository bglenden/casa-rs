# ADR-0006: One parameter model and current sparse profiles

Status: accepted
Date: 2026-11-12
Truth class: normative

The CLI, Python, and task providers share Rust definitions for parameter
names, types, defaults, normalization, and validation. Share concepts by
meaning; each task supplies its own bindings and defaults. Runtime controls
such as executable selection and telemetry stay out of saved parameters.

Profiles are sparse TOML. They store required values and explicit differences
from defaults. Provider JSON is a machine transport. The common task runtime
owns resolution and any managed Last state; ordinary Python task calls leave
Last alone.

A profile must name the current task and use the current file format and task
profile version. Refuse stale files and tell the user to recreate them. Bump
the marker only when an existing profile can no longer be interpreted as
intended. An added optional parameter can use its default. The former
migration and history policy is superseded by
[ADR-0020](0020-single-investigator-development.md).

See [Task Parameter Profiles](../task-parameters.md) for the format and
[Provider Contracts](../provider-contracts.md#parameter-catalog-and-surface-definitions)
for the current implementation.
