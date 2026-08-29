# Issue #540 frozen CASA cycle forensics

Truth class: recovered artifact evidence with bounded absence findings  
Recorded: 2026-08-28  
Work issue: #540  
CASA rerun: **no**

## Outcome

A run-bound CASA cycle trace was recovered. It is not in the output-image
`logtable`; it is in the global CASA logger file
`/Users/brianglendenning/.codex/worktrees/5d43/casa-rs/casa-20260827-185909.log`.
That file names the exact
`20260827T185907Z-wave3-standard-mfs-single-term-heavy-wave2-075607db`
product prefix and records all 11 logged major-cycle ordinals, ten 50-iteration
minor cycles, their thresholds/model/peak transitions, 500 cumulative
iterations, and the terminal reason `iteration limit`.

The same retained run's benchmark stream reports `688.996833` seconds, not
`733.660` seconds. The latter is a distinct historical Wave 2 observation first
recorded in git commit `ab747554fa34b1bf2dc2e5a063fc9acc3563d07b`, whose
cited directory was
`target/imperformance-wave2/medium-divergence-20260525/niter_500`. No surviving
log, receipt, product prefix, or workload hash for that historical observation
was found in the finite retained universe below. The two timings can be
same-workload repeats; this receipt does not invalidate either or silently
replace the accepted performance anchor.

## Run binding and retained identities

| Artifact | Binding/result | Digest |
| --- | --- | --- |
| Global CASA logger | 99 lines; exact `vis` and `imagename`; CASA `6.7.6.14`; task completed | SHA-256 `c0e8c85d39cdd713848596b7526c0ff6b8733a75d255337009ea54ae420c1fbb` |
| Retained benchmark stream | `run=1 real=688.996833`, `median=688.996833`, exact kept prefix | SHA-256 `42a0cf1880d23195d48d4b4febf76697c796db4e54548edc018a81b4674d4ed8` |
| Comparator request | References the same exact CASA prefix | SHA-256 `38f1ade7ad30ca4f535380e3c7c191e25cadfa309a742cac9da8a4b5e985522e` |
| Comparator result | Inventories the exact CASA products; Rust side was absent | SHA-256 `76e06c8ab6dab446242525e74ca0046b4e2365f88981a744d403771b12a4f2c5` |
| Comparator log | Empty, zero bytes | SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| Seven product `logtable` message streams | 94 rows each; identical task/version/parameter messages | Canonical newline-joined message SHA-256 `a3f0bfc086bad377c1c233dcb4426b6d650967579a689aeb0b109c7ac194d042` |
| Historical Wave 2 report at `ab747554f` | First retained textual owner of `733.660` and the old target directory | Git blob `ab38b11ac4ddae6443eb5ed21439b89989575f57` |

The global logger and benchmark stream both have filesystem modification time
`2026-08-27T13:10:39-0600`. The logger is the only retained `casa-*.log` in the
searched roots that contains the exact run identifier.

## Exact recovered invocation fields

The global logger's `tclean(...)` record and selection records supply these
values directly:

| Field | Value |
| --- | --- |
| Python / CASA | Python `3.12.4`; CASA `CASALITH 6.7.6.14` |
| MeasurementSet | `/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms` |
| Selected rows | `4,094,064` |
| Field / SPW | field `0`; SPW/channels `0:0~63` |
| Data / image | `datacolumn='data'`; `imsize=1024`; `cell='0.25arcsec'`; Stokes `I`; `specmode='mfs'` |
| Grid / weights | `gridder='standard'`; `weighting='briggs'`; `robust=0.5`; `perchanweightdensity=False` |
| Deconvolution | `hogbom`; `nterms=1`; `niter=500`; `gain=0.1`; `threshold='0.0Jy'`; `nsigma=0.0` |
| Cycle controls | `cycleniter=50`; `cyclefactor=1.0`; `minpsffraction=0.05`; `maxpsffraction=0.8`; `nmajor=-1` |
| Summary / execution | `fullsummary=False`; `parallel=False`; `interactive=False` |
| Products | restoration, residual, and PSF enabled; PB correction disabled; seven CASA products retained |
| Product prefix | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/products/20260827T185907Z-wave3-standard-mfs-single-term-heavy-wave2-075607db/casa/casa` |

## Recovered cycle trajectory

Values below are transcribed from the global logger, not reconstructed from
image arrays. `Cum. iter` is the logger's `Completed N iterations` record.

| Major | Logged start | Cycle threshold | Model start -> end | Peak residual start -> end | Cum. iter | Minor stop |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | 19:00:32 | 6.082975387573242 | 0 -> 328.683 | 91.7745 -> 51.8045 | 50 | cycleniter |
| 2 | 19:01:19 | 3.4336907863616943 | 328.683 -> 560.461 | 51.8045 -> 39.998 | 100 | cycleniter |
| 3 | 19:02:14 | 2.651136875152588 | 560.461 -> 746.036 | 39.998 -> 32.8662 | 150 | cycleniter |
| 4 | 19:03:10 | 2.1784305572509766 | 746.036 -> 901.493 | 32.8662 -> 28.1089 | 200 | cycleniter |
| 5 | 19:04:04 | 1.8631064891815186 | 901.492 -> 1035.29 | 28.1089 -> 24.3178 | 250 | cycleniter |
| 6 | 19:05:03 | 1.6118278503417969 | 1035.29 -> 1152.06 | 24.3178 -> 21.5334 | 300 | cycleniter |
| 7 | 19:05:58 | 1.427268147468567 | 1152.06 -> 1255.48 | 21.5334 -> 19.1686 | 350 | cycleniter |
| 8 | 19:06:53 | 1.270528793334961 | 1255.48 -> 1348.17 | 19.1686 -> 17.2254 | 400 | cycleniter |
| 9 | 19:07:57 | 1.1417288780212402 | 1348.17 -> 1431.87 | 17.2254 -> 15.5905 | 450 | cycleniter |
| 10 | 19:08:50 | 1.033364176750183 | 1431.87 -> 1508.1 | 15.5905 -> 14.3108 | 500 | cycleniter |
| 11 | 19:09:44 | n/a | no minor cycle | initialized peak 14.3108 | 500 | global `iteration limit` at 19:10:40 |

The `901.493` / `901.492` boundary is preserved exactly as printed; no rounding
repair was applied. Each per-cycle `iters=0->50 [50]` field resets locally, so
only the separate cumulative records are reported as total iteration progress.

## Terminal and timing fields

| Requested field | Forensic result |
| --- | --- |
| `summaryminor` | No literal field or serialized return value survived. The global logger does retain the exact per-cycle controls and transitions above. |
| `summarymajor` | No literal field or serialized return value survived. |
| `iterdone` | No literal task-return field survived. The logger explicitly reaches `Completed 500 iterations.` |
| `nmajordone` | No literal task-return field survived. There are exactly 11 `Run Major Cycle N` records, ordinals 1 through 11. |
| `stopcode` | Not present; this receipt does **not** infer a numeric code from the textual reason. |
| stop reason | Exact logger text: `Reached global stopping criterion : iteration limit`. |
| task return record | Not retained. The run-era legacy benchmark script calls `tclean(**kwargs)` without binding or serializing its return value. |
| benchmark wall | Exact wrapper record: `688.996833` seconds. |
| CASA task wall | Logger start `2026-08-27 12:59:10.671835`, end `2026-08-27 13:10:39.658573`; arithmetic difference `688.986738` seconds. |
| stage timing report | None retained. One local timer survives: `Time to fit Gaussian to PSF 0.008453`. Event timestamps give chronology but are not promoted to exact stage durations. |

## The `733.660` historical observation versus this retained repeat

The exact-value search found `733.660` only in checked-in documentation and
later issue-540 ledgers, not in any retained external log or receipt. Commit
`ab747554f` records the historical run as a paired Wave 2 audit at
`target/imperformance-wave2/medium-divergence-20260525/niter_500`.

The historical prose and the 2026-08-27 logger agree on the following intended
workload fields: medium VLA MeasurementSet, 64 channels, 1024-pixel image,
0.25-arcsec cell, Briggs weighting, and `niter=500`. The historical record does
not retain a manifest hash, MeasurementSet hash, complete CASA argument vector,
CASA version, task-return record, or exact product prefix. Therefore:

- the two observations are consistent with repeats of the same intended
  workload;
- exact workload-hash identity cannot be proved;
- the 2026-08-27 products are directly bound to the `688.996833`-second run;
- the later issue-540 ledger's association of the accepted `733.660` anchor
  with that product prefix is secondary documentation, not a surviving source
  receipt for the May timing.

## Finite search universe and results

| Retained root | What was inspected | Result |
| --- | --- | --- |
| `/Users/brianglendenning/.codex/worktrees/5d43/casa-rs` | Run-ID/date searches and `casa-*.log` contents | Recovered the exact global logger `casa-20260827-185909.log`. |
| `/Volumes/GLENDENNING/casa-rs-imperformance/baselines/2026-08-27-current-main-casa-only` | Every file; exact run ID; timing and terminal keywords | Four files only: benchmark log, comparator request/result, and empty comparator log. No workload result JSON. |
| `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/products/20260827T185907Z-wave3-standard-mfs-single-term-heavy-wave2-075607db` | Full path inventory; stdout/stderr/result/summary/return/timing/trace filenames; all seven product log tables | Products and embedded parameter histories only; no separate text trace or task return. |
| Matching `comparisons/...075607db` | Full inventory | Empty `panels` directory only. |
| Shared `imperformance-artifacts/tmp` | Full inventory | Empty; no run-owned temporary script/result survived. |
| Known retained ImPerformance result roots: `baselines`, `imperformance-artifacts`, `runs`, `wave3/runs`, `w4-07-runs`, and `wave1/native-generation-logs` | Extension-bounded text search for exact `733.660`, date/run IDs, old target path, cycle/return terms | No external source record for `733.660`; exact 2026-08-27 references resolve to the product/baseline records above and later Rust comparisons. |
| Current worktree, `/Users/brianglendenning/SoftwareProjects/casa-rs`, `/Volumes/GLENDENNING/casa-rs`, and `/Volumes/GLENDENNING/casa-rs-imperformance` | Exact old target-directory search | No surviving `target/imperformance-wave2/medium-divergence-20260525/niter_500`. May 2026 CASA logs that survive in the primary checkout do not contain the matched `niter=500`/1024-pixel cycle trace. |
| Archives under `/Volumes/GLENDENNING/casa-rs-imperformance` | Archive filename inventory and run-ID match | No archive names or contents were identified as belonging to this run; the visible archives are unrelated source/test fixtures. |
| Git history through `fff9c2d553eace4b6a57b1df9ded4773f2263ceb` and introducing commit `ab747554f` | Exact timing/path search and benchmark-script inspection | Recovered the historical prose owner and confirmed that the legacy script discarded the `tclean` return value. |

## Commands

The following are the material read-only searches, normalized only by assigning
short shell variables to the literal paths:

```bash
run_id='20260827T185907Z-wave3-standard-mfs-single-term-heavy-wave2-075607db'
imperf='/Volumes/GLENDENNING/casa-rs-imperformance'
product_root="$imperf/_tmp_safe_to_delete/imperformance-artifacts/products/$run_id"
baseline_root="$imperf/baselines/2026-08-27-current-main-casa-only"

find "$imperf" -type d -name "*$run_id*" -print
find "$imperf" -type f -name "*$run_id*" -print
find "$product_root" -maxdepth 4 -print
find "$product_root" -type f \( -iname '*.log' -o -iname '*stdout*' -o -iname '*stderr*' -o -iname '*result*' -o -iname '*summary*' -o -iname '*return*' -o -iname '*timing*' -o -iname '*trace*' \) -print

rg -a -n '(^|[^0-9])733\.660([^0-9]|$)' \
  "$imperf/baselines" \
  "$imperf/_tmp_safe_to_delete/imperformance-artifacts" \
  "$imperf/runs" "$imperf/wave3/runs" "$imperf/w4-07-runs" \
  "$imperf/wave1/native-generation-logs" \
  -g '*.json' -g '*.log' -g '*.md' -g '*.txt' -g '*.csv'

rg -l --glob 'casa-*.log' "$run_id" \
  /Users/brianglendenning/.codex/worktrees/5d43/casa-rs \
  /Users/brianglendenning/SoftwareProjects/casa-rs \
  /Volumes/GLENDENNING/casa-rs "$imperf"

rg -n -i 'summaryminor|summarymajor|iterdone|nmajordone|stopcode|stop.?reason|Reached global stopping criterion|Completed [0-9]+ iterations|Run Major Cycle|Time to |Task tclean complete' \
  /Users/brianglendenning/.codex/worktrees/5d43/casa-rs/casa-20260827-185909.log

find /Users/brianglendenning/.codex/worktrees/5d43/casa-rs \
     /Users/brianglendenning/SoftwareProjects/casa-rs \
     /Volumes/GLENDENNING/casa-rs "$imperf" \
  -type d -path '*target/imperformance-wave2/medium-divergence-20260525/niter_500' -print

git grep -n -e '733.660' -e 'medium-divergence-20260525' \
  fff9c2d553eace4b6a57b1df9ded4773f2263ceb --
git show ab747554fa34b1bf2dc2e5a063fc9acc3563d07b:scripts/bench-imager-vs-casa.sh

shasum -a 256 \
  /Users/brianglendenning/.codex/worktrees/5d43/casa-rs/casa-20260827-185909.log \
  "$baseline_root/$run_id.log" \
  "$baseline_root/$run_id.comparison-input.json" \
  "$baseline_root/$run_id.comparison.json" \
  "$baseline_root/$run_id.comparison.log"
```

Each CASA image `logtable` was opened read-only with CASA's table tool
(`open(path, nomodify=True)`), and columns `TIME`, `PRIORITY`, `MESSAGE`,
`LOCATION`, and `OBJECT_ID` were inspected. Searches covered
`summaryminor`, `summarymajor`, `iterdone`, `nmajordone`, `stopcode`, stop
reason, major/minor cycle, elapsed/timing/wall, and the explicit cycle-control
parameters. All seven tables had 94 rows and zero trace-or-timing keyword hits.

## Limitations

- The global CASA log is positive cycle evidence for the exact 2026-08-27
  product prefix, not source evidence for the separate `733.660`-second May
  observation.
- The old Wave 2 target directory and its workload/product hashes are absent
  from the finite retained roots. No claim of byte-identical workload identity
  is made.
- No task-return dictionary survived, so literal `iterdone`, `nmajordone`,
  `stopcode`, `summaryminor`, and `summarymajor` values remain unavailable.
  Logged events are reported under their own names and are not recast as those
  task-return fields.
- The logger timestamps are suitable for ordering and coarse elapsed bounds,
  not unreported phase attribution. No stage durations are inferred from image
  arrays or rounded event timestamps.
- The search was bounded to known retained casa-rs/ImPerformance roots, git
  history, and the exact run's repo-root CASA logs. It did not inspect unrelated
  user storage or Time Machine backups.
