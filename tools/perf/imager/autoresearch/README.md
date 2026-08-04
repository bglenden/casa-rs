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

## Qualification sequence

The contract intentionally begins with `baseline.status=qualification`.

1. Generate the one-time read-only MS selection receipt:

   ```sh
   python3 tools/perf/imager/autoresearch/vlass_5m.py freeze-selection
   ```

2. Record the printed SHA-256 in
   `dataset.selection_accounting_sha256`.
3. Run the first release qualification:

   ```sh
   python3 tools/perf/imager/autoresearch/vlass_5m.py measure
   python3 tools/perf/imager/autoresearch/vlass_5m.py guard
   ```

4. Inspect fidelity, duration, cache pressure, memory, and products. Freeze the
   accepted receipt and output prefix in `baseline`, then measure enough
   unchanged release runs to establish variance.

After the baseline is frozen, every measurement is sampled against it with a
normalized RMS ceiling of `1e-3`, topology parity, and metadata parity. This
proxy comparison is an experiment guard; final promoted code still owes the
approved CASA scientific and 19-product acceptance contract.

The guard writes nothing. It verifies the latest pointer and receipt hashes,
source-state and release-binary binding, workload shape, cache pressure,
sample acceptance, no fallback, memory/swap bounds, product inventory, and
proxy numerical parity.
