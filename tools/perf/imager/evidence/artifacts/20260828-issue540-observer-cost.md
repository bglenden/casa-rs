# Issue #540 observer-cost gate

Truth class: recorded single-triplet observation  
Recorded: 2026-08-28  
Work issue: #540

## Outcome

The first production-path observer triplet failed its predeclared overhead
gate. The tested instrumentation added one wall clock around each bounded
`process_block`, pass-finalization/FFT/reconciliation/minor-cycle clocks, and
cycle/stage trace output. The workload was
`wave3-standard-mfs-single-term-turnaround`: the mounted medium VLA
MeasurementSet, complete ordered row traversal, one channel, a 1024-pixel
Briggs image, 50 Hogbom iterations, RustFFT, CPU, and one worker.

| Order | Trace state | Rust wall (s) |
| ---: | --- | ---: |
| 1 | OFF | 86.682149 |
| 2 | ON | 91.733128 |
| 3 | OFF | 88.519991 |

The OFF observations differ by 2.10 percent, within the declared 3-percent
stability bound. Their mean is 87.601070 seconds. ON is 4.72 percent above
that mean, exceeding the declared 2-percent observer-cost ceiling. Because the
OFF pair was stable, the plan did not authorize a repeat triplet.

The intrusive clocks were therefore removed. Retained instrumentation uses
already-existing coarse timers, classifies the already-measured consumer wait
as source-starved versus terminal wait, and emits algorithmically inert cycle
state. Finer fused-kernel attribution must use external sampling and work
counters; this failed gate does not authorize an optimization.

All three runs retained the same workload shape: one density pass, one initial
weighted replay, seven later weighted replays, 42 blocks per pass, 4,094,064
rows per pass, 8,188,128 selected samples per pass, one worker, two source
slots, and 57,297,318 bytes peak live source capacity. The harness returned
`failed_comparison` after successful imaging because CASA was intentionally
skipped and metadata parity was therefore absent; this is the already-recorded
harness reporting defect, not an imaging failure.

## Retained source artifacts

| Observation | Result SHA-256 | Log SHA-256 |
| --- | --- | --- |
| OFF 1 | `e9a94dc911e7648378d6bb8a3d6e1b66ff19c19e6e0eea141c949537ce99e0d6` | `8fd98fe942639b33b34e906a43f171b81c6e3d7d47a16d86f0383240ced0d5f2` |
| ON | `50813d90b7d3c0af1deed6dee53b7e43dce189bd295cc3398edc1f74ab934168` | `e2bac5659622455f0c1ef4c466b7c14bf2f04ce2ca96cbfcc31930e86314d2c9` |
| OFF 2 | `cccfc26ebc03f071e94e0eeb1aac055e462932a6691d710eee03841496968c53` | `41036f42af3dffc42086ee1f1a7618a04ea7d8def8b7837d240de6da26fadac2` |

The result and log files live below the corresponding
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/issue540-observer-{off1,on,off2}/results`
directories. Generated products are temporary and are not the durable source
of the timings above.
