# ADR-0006: One parameter model and current sparse profiles

Status: accepted
Date: 2026-11-12
Amended: 2026-11-25, clean-sheet module reorganization: retire
`ProviderContractEnvelope`, `build.rs` JSON-fingerprinting, and the mandatory
24-binary `ApplicationCatalog` launcher once Python completes in-process cutover.
Truth class: normative

The CLI (`casars-cli`), Python (`casars.tasks`), and task adapters share one
Rust parameter model (`ParameterCatalog`, typed `SurfaceDefinition`) for
parameter names, types, defaults, normalization, and validation. Share concepts
by meaning; each task supplies its own bindings and defaults. Runtime controls
such as telemetry stay out of saved parameters.

Profiles are sparse TOML. They store required values and explicit differences
from defaults. Provider JSON is a machine transport. `surfaces/casa-tasks`
owns resolution and any managed Last state; ordinary Python task calls isolate
Last state from CLI runs.

`ProviderContractEnvelope`, `build.rs` JSON-fingerprinting probes (such as
`IMAGE_PROVIDER_PROBES`), and the mandatory 24-binary `ApplicationCatalog`
subprocess launcher are retired (the executable resolver is deleted once
`casars.tasks` cuts over to in-process execution per ADR-0017).

A profile must name the current task and use the current file format and task
profile version. Refuse stale files and tell the user to recreate them. Bump
the marker only when an existing profile can no longer be interpreted as
intended. An added optional parameter can use its default. The former
migration and history policy is superseded by
[ADR-0020](0020-single-investigator-development.md).

See [Task Parameter Profiles](../task-parameters.md) for the format and
[Provider Contracts](../provider-contracts.md#parameter-catalog-and-surface-definitions)
for the current implementation.
