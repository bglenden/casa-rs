# Portable Tutorial Templates

Truth class: current descriptive
Last reality check: 2026-07-12
Verification: just docs-check

Tutorials are immutable portable template folders that fork into ordinary editable scientific notebooks. The Rust `casa-notebook` crate owns the v1 manifest, learner fork, managed lock, acquisition state, integrity checks, and safe materialization. Swift and other frontends consume projections of that contract rather than interpreting a second manifest.

## Template shape

```text
<template>/
  tutorial.md
  tutorial.toml
  assets/                 # optional portable prose assets
  regression/             # optional maintainer evidence overlay
```

`tutorial.md` is ordinary Markdown and may contain the same typed task cells as any scientific notebook. `tutorial.toml` declares the tutorial identity, ordered sections, datasets, source URIs, project-local destinations, optional size and SHA-256, bounded unpack plan, and optional checks. The checked-in TW Hya walkthrough is [`resources/tutorials/tw-hya-first-look`](https://github.com/bglenden/casa-rs/blob/main/resources/tutorials/tw-hya-first-look/tutorial.md).

Forking copies the Markdown and assets into the project while leaving the source template unchanged. The learner copy is stored under `notebooks/`; Rust-managed state is stored under `.casa-rs/tutorials/<notebook-id>/lock.toml`.

## Dataset acquisition

Opening or selecting a tutorial never downloads or executes anything. Acquisition requires an explicit approval bound to the exact requested and resolved URI, redirects, size, project destination, digest, disk requirement, extraction bounds, and optional-check choices.

The v1 registry ships `file`, `http`, and `https` handlers. Applications may register other schemes, but unknown schemes stay inert and handlers never delegate to a shell or system opener. User-supplied source URIs pass through the same registry, approval, integrity, and materialization policy as catalog sources.

Downloads remain under `.casa-rs/tutorials/<notebook-id>/staging/` until size and SHA-256 verification, bounded extraction, and optional checks finish. Missing source digests require explicit approval; the computed SHA-256 is then pinned in managed state. Archive extraction rejects traversal, escaping links, special files, entry/expanded-size limits, and destination collisions. Only a final atomic publish changes the declared project destination.

## Legacy v0 migration

`tutorial-pack.v0` is not a runtime compatibility format. The explicit one-shot Rust migrator reads a legacy `pack.json`, produces a new immutable v1 folder, converts native GUI steps to typed notebook task cells, and copies the useful regression evidence overlay. Swift has no v0 parser or v0 GUI state. Once the v1 folder exists, normal opens use only `tutorial.md` and `tutorial.toml`.

Historical regression scripts may still produce v0 input solely for exercising that one-shot migration or preserving old evidence. They are not a peer tutorial truth source.

## Regression overlay

The optional `regression/` directory is maintainer evidence, not the learner notebook authority. It may contain native/oracle run records, comparisons, tolerances, timings, provenance, and review records. Learner prose should summarize useful conclusions without exposing routine diagnostic bulk by default.
