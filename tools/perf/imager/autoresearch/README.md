# VLASS five-minute autoresearch adapter

`vlass_5m.py` owns a bounded, fail-closed proxy for the production VLASS
AWProject residual-refresh bottleneck. It preserves 4096-square geometry, all
63 fields and POINTING behavior, SPWs 2/7/12/17, 32 W planes, A/WB/conjugate
beams, Briggs weighting, MT-MFS `nterms=2`, and the exact 19-product surface.
Only the source-channel count is reduced, to 24 channels per SPW.

The metric is the final production
`mosaic_mtmfs_stream_replay pass=ResidualRefresh` duration. The process also
performs the real initial AWProject pass, loads a frozen nonzero MT-MFS model,
executes exactly one residual refresh, performs the FFT/readback, and writes
the products. This makes the roughly five-minute row representative of the
miss-heavy compact replay path without running another complete 2000-iteration
clean.

Performance evidence is release-only:

- `cargo build --locked --release` completes before the timed process starts;
- the timed executable SHA-256 is recorded;
- `timed_build_seconds` must be zero;
- the read-only guard rejects debug or unbound executables.

The receipt captures application-cache loads, hits, evictions, residency,
compact program bytes/builds, source-order window timings, Metal dispatch,
FFT readback, process/host memory, compression, swap and disk I/O, exact
selection accounting, and output identities.

The frozen selection-accounting v2 receipt has SHA-256
`2f81d69801d37c528ce7b3a747ef0676c6e57f870359d9661f43d2bf7f34438c`.
It proves 3,400,608 attempted and 2,204,617 accepted Stokes-I samples, with
nonempty accounting for every one of the 63 fields in every selected SPW.

## Qualification sequence

The contract was introduced with `baseline.status=qualification`. The guarded
qualification completed on 2026-08-04 and is now frozen in the contract.

1. Generate the one-time read-only MS selection receipt:

   ```sh
   /path/to/casa-python \
     tools/perf/imager/autoresearch/vlass_5m.py freeze-selection
   ```

2. Record the printed SHA-256 in
   `dataset.selection_accounting_sha256`. Selection schema v2 records exact
   attempted and accepted Stokes-I sample counts for every field and SPW, as
   well as the correlation basis and CASA parallel-hand pair.
3. Run the first release qualification:

   ```sh
   python3 tools/perf/imager/autoresearch/vlass_5m.py measure
   python3 tools/perf/imager/autoresearch/vlass_5m.py guard
   ```

4. Inspect fidelity, duration, cache pressure, memory, and products. Freeze the
   accepted receipt and output prefix in `baseline`, then measure enough
   unchanged release runs to establish variance.

The frozen qualification receipt is
`20260804T184725Z-dd0356542bc0/receipt.json`, SHA-256
`965dc24a6e6fe5be7adb5b2b4c11fb3e66e4c755c5db054d206c75b5e4fe80fa`.
It used release executable SHA-256
`96b867fff8f7232eea43cab4ee539a5bf1fcb770ef1332805beb759c6d4f191d`;
the residual-refresh metric was 86.729 seconds and the whole process was
182.997 seconds. The application cache reported a 9.09% hit rate and 99.59%
eviction/load ratio, all 2,204,617 samples were accepted, and the read-only
guard passed.

After the baseline is frozen, every measurement is compared against it with a
chunked full-array pass over the seven sensitive proxy products, a normalized
RMS ceiling of `1e-3`, exact finite/mask topology, and metadata parity. This
proxy comparison is an experiment guard; final promoted code still owes the
approved CASA scientific and 19-product acceptance contract.

The guard writes nothing. It verifies the latest pointer and receipt hashes,
source-state and release-binary binding, workload shape, cache pressure,
the exact per-field/SPW sample census, no fallback, memory/swap bounds, product
inventory, and proxy numerical parity.

The first unchanged repeat measured 77.400 seconds with the same release
binary. Its initial sampled comparison measured normalized RMS `0.0` for all
seven products but was correctly rejected because sampled mode could not prove
topology. A bounded full-array diagnostic on those existing products passed
all normalized-RMS, topology, and metadata checks in about 21 seconds; its
comparison receipt SHA-256 is
`59e8351924495d49ec47c758f6802045013d7fa27f5659bf17e487e461cbfcf6`.
The production proxy guard therefore uses that chunked full-array mode.

The focused full-array repeat then passed end to end at 75.855 seconds; receipt
SHA-256
`353abdf38f28547289cbaafac6bd5d0f2a8958917b3e8270cf50fd97da7420ac`.
The two warm release metrics, 77.400 and 75.855 seconds, have a 76.627-second
mean and a 2.02% range. Autoresearch therefore needs at least a 5% improvement
over the warm mean (72.796 seconds or lower) before a single trial can become a
promotion candidate. Promoted trials still require an unchanged rerun to
protect against timing noise.

The controller guard enforces that minimum relative to its current retained
metric:

```sh
python3 tools/perf/imager/autoresearch/vlass_5m.py \
  guard --minimum-improvement-fraction 0.05
```

During controller initialization there is no incumbent state, so this option
acts as the ordinary baseline guard. During an experiment it rejects and
therefore reverts an apparent improvement smaller than 5%.

## Full-16-SPW exact replay campaign

`vlass_full16_replay.py` is the finite scaling campaign that supersedes the
four-SPW proxy for replay-architecture work. Its external fixtures preserve
all 63 fields, POINTING, all 16 SPWs, the production raw-CF/phase/tap and
conjugation representation, exact source order, and the ten-segment boundary
observed by the completed v36 row. A second four-SPW fixture provides the
required no-more-than-5% regression guard.

Fixture capture is a one-time release-mode workload. It stops deliberately
after the first exact residual replay and publishes private-layout payloads
under `/Volumes/GLENDENNING`; those payloads are experimental evidence, not a
production persisted-cache format:

```sh
PYTHONPATH=tools/perf/imager \
  python3 tools/perf/imager/autoresearch/vlass_full16_replay.py capture
```

The captured model grids retain casacore's first-axis-contiguous storage.
Before timing, seal legacy manifests once so model and residual sections have
nonzero SHA-256 identities while the original manifests remain preserved in
the seal receipt:

```sh
PYTHONPATH=tools/perf/imager \
  python3 tools/perf/imager/autoresearch/vlass_full16_replay.py seal-fixtures
```

The AOT grouped-tile experiment compiles a separate `1e-6`
squared-L2-support sidecar for each frozen raw fixture. `measure` creates each
sidecar once, outside the timed region and with the exact release test
executable, starting with the four-SPW control. The sidecar stores cropped
prediction plans, source-order sample/role-to-group mappings, incumbent
grouped plans, and the incumbent grouped tile route. It references unchanged
kernel, phase, Taylor-weight, and source-index sections from the raw fixture.
The compact production capture canonically omits the optional diagnostic
source-index vector; its empty section is valid only with zero bytes and the
empty SHA-256. Exact replay order instead comes from the ordered prediction
records and the one-to-one source-role map. A diagnostic capture may retain
one source index per prediction record, but partial nonzero cardinalities fail
closed.
The raw prediction, tile-sample, and ungrouped-route sections are replaced and
are neither retained nor read by replay. The manifest binds the raw manifest
and payload hashes, the exact release test executable SHA-256, private Rust
layout, threshold bits, differential grouped-plan/route hashes, and explicit
replacement/lifetime byte equations. Its compiler-memory receipt labels the
HashMap components as estimates and adds a separate four-times, at-least-64-MiB
uncertainty reserve to a 32-GiB admission formula. Subsequent runs reuse the
sealed sidecar only when every bound identity, including the executable hash,
still matches. This private sidecar remains experimental evidence, not a
production persisted-cache contract.

The topology-guard repair changes the exact release executable identity, so
the immutable v2 sidecars remain historical evidence but are intentionally
ineligible for the repaired binary. New compilations use v3 artifact paths;
the controller never overwrites or silently rekeys an existing sidecar.

Each campaign iteration then builds the release `casa-imaging` test executable
before timing and rejects sidecars built by any other executable. Manifest and
key validation remains outside timing without reading the full sidecar
payload. The first mandatory full sidecar SHA-256 scan occurs after the replay
timer starts and is receipted in timed bytes and seconds; the later guard
rehashes it independently. The campaign replays the frozen full-band fixture
once, replays the four-SPW control once, and records the executable hash,
raw/sidecar byte ledger, per-segment reload/Metal timing, NRMSE, process
footprint, swap and disk telemetry. The guard additionally requires one AOT
use per segment and zero *instrumented canonical-seam deltas* for runtime
grouping, sorting, or route construction:

```sh
PYTHONPATH=tools/perf/imager \
  python3 tools/perf/imager/autoresearch/vlass_full16_replay.py measure
PYTHONPATH=tools/perf/imager \
  python3 tools/perf/imager/autoresearch/vlass_full16_replay.py guard
```

The first accepted measurement is frozen externally with `freeze-baseline`.
The primary JSON metric key is `seconds`, lower is better, with a target of
35 seconds and a maximum of 12 autoresearch iterations. A failed guard exits
nonzero and emits no retainable `seconds` metric. Promotion has hard upper
bounds of `63.148921725` seconds for full-16 and `8.256052394` seconds for the
four-SPW control in addition to the frozen relative control guard. The guard
also requires exact sample/topology cardinality, zero rejected samples, NRMSE
at most `1e-3`, a complete byte/timing ledger, and a process
physical-footprint peak no greater than 32 GiB.

The frozen corrected baseline is `252.789194042` seconds for full-16 and
`14.004195833` seconds for four-SPW, with zero NRMSE on both Taylor terms.
Its measurement receipt SHA-256 is
`5f76492d46ed968db4fbc2148c523653eed733f54ce785d8b55b4307d390e212`;
the baseline JSON SHA-256 is
`acfe3e32c12a9dc41d3003d240e1b76d2dc0a8c3088c1c509d9ab26e861dbe04`.
