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

## End-to-end 4096-square recovery controller

`vlass_4096_recovery.py` is the end-to-end CLEAN controller. Its phase-one
guard keeps the accepted single-field four-SPW result at 100x CASA while its
phase-two metric is the matched all-63-field run. The all-field target is the
complete process wall time, including initial gridding, CLEAN refreshes, and
product writes; residual-replay timings are diagnostics only.

The bounded source-major architecture experiment is selected only by
`CASA_RS_VLASS_SOURCE_MAJOR_GROUPED_INITIAL=1`. For each source block it:

1. classifies and materializes exact-support CF taps once;
2. compiles an exact residual replay program and an eight-plane grouped
   initial-grid program from the same source-owned materialization;
3. dispatches the initial grouped program into compensated Metal grids; and
4. releases temporary initial/source state while retaining the complete
   residual program through the final residual transform.

The guard requires one source-major window for every selected DDID, exact
support, complete resident AOT residual replay, zero refresh spill reads or
runtime topology rebuilds, and an owned-memory peak no larger than the
planner's grouped compile admission. Scientific promotion still requires the
matched CASA comparison; resource or correctness failure makes the trial
non-retainable and the controller reverts it.
