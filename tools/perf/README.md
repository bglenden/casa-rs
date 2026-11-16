# Performance Tooling

Performance-analysis tools, one directory per feature:

- `imager/`: CASA `tclean` versus `casars-imager` benchmarks, workload
  manifests, evidence and the performance ledger (`imager/README.md`).
- `calibrate/`: `calibrate` apply-path tracing and its report script
  (`calibrate/README.md`).

Runtime code writes structured JSONL traces, which are the source of truth for
analysis; `.log` files are convenience summaries. Profilers such as
Instruments or `sample` complement these traces but cannot say what a run
was doing.
