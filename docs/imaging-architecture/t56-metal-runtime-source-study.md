# T56 planner-owned Metal runtime source study

Truth class: historical implementation evidence
Last reality check: 2026-09-01
Verification: `CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime metal_runtime --lib`; `CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime --test compile_plan_run receipt_compares_plan_predictions_with_actual_stage_resource_and_fence_use -- --exact`; `just arch-check`

T56 implements only the physical Metal runtime named by ADR-0010. It does not
select an imaging implementation or define a kernel, precision, reduction,
normalization, mode, or product rule. T57 remains responsible for scientific
Metal implementations behind the existing registry and Numerics Contract.

## Superseding runtime direction — 2026-09-29

The owner explicitly authorized replacing inadequate Metal runtime APIs without
backwards compatibility. The T57 cube integration removes the no-encoder smoke
operation and one-submission-per-node restriction. Observation I/O retains its
source authority and I/O admission while declaring accelerator/queue claims and
both I/O/device completion fences. Only planned MetalShared allocations become
GPU buffers; host-only source and FFT/CLEAN buffers remain host allocations.
Actual grid/degrid batches drain before the borrowed input callback returns;
batch waits do not complete the stage. Slots materialize on admitted use and
retire at the existing allocation ledger's release events, not runtime startup
and shutdown. Pipeline compilation is charged to the planned JIT envelope.
The old eager-residency and smoke-only descriptions below are historical,
non-normative T56 evidence, not constraints on T57. Scientific application
integration and acceptance are separate from these runtime lifecycle checks.

## Historical old-to-current map (T56)

| Evidence lineage | Proven mechanism | Current owner and disposition |
| --- | --- | --- |
| Pre-cutover casa-rs `fff9c2d553eace4b6a57b1df9ded4773f2263ceb`, especially `crates/casa-imaging/src/lib.rs` | `objc2-metal` device and queue creation, shared buffers retained through command completion, explicit command status checks, and grouped resident inputs. The archived experiments also show that global atomics and repeated whole-grid transfers are not generally viable. | The scheduler-owned `MetalExecutionState` retains one device, queue, and unique physical-slot ledger for the complete admitted execution. Mode-specific dispatch, frontend probes, runtime shader compilation, per-node residency recreation, and the displaced monolithic owner are rejected. |
| CASA/casacore synthesis and lattice paths | CASA preserves visibility-buffer iteration, cache limits, product semantics, and explicit stage separation; the inspected production path does not provide the Apple Metal runtime seam. | CASA remains the scientific/product oracle. No C++ ownership shape or implicit host fallback is copied into the Metal runtime. |
| LibRA `AWVisResamplerHPG.cc` and `AWProjectFT.cc` | HPG execution explicitly selects a device, bounds concurrent streams, copies only at declared boundaries, and fences before host consumption. | The device/fence lifetime is adapted into plan-owned Metal commands and unified residency. LibRA's backend-specific science owner and stream policy are rejected. |

## Resulting interface

`MetalExecutionDecision::bind` accepts only an already validated immutable
`ExecutionDag` and `ResourceTopology`. It resolves one inventoried Metal
accelerator and queue, rejects discrete device memory, requires a device fence
for every Metal node, verifies that every visible allocation uses the selected
unified-memory view, and charges residency once per physical slot. Transfer
links, staging buffers, resident cache, and driver/JIT/command-buffer envelopes
remain the exact plan values already serialized by `ExecutionReceipt`.

`ExecutionScheduler` creates one `MetalExecutionState` after Resource Authority
admission and closes it before releasing or quarantining the execution lease.
The state binds the first submission to the exact execution attempt and lease
epoch, accepts every selected node once, allocates every unique physical slot
once for the full execution, and drains a committed command before closing.
Producer and consumer nodes that name the same physical slot therefore observe
the same retained Metal buffer across the producer fence.

The execution state and its encoding operations are crate-private. No public
device, queue, command-buffer, or Metal-buffer protocol object is exposed to a
work implementation, so an implementation cannot recover allocation authority
or create unplanned buffers. T56 provides only a constrained no-encoder host
smoke operation; T57 owns the typed compute-encoding operations that will use
this residency and fence seam.

`WorkImplementation::wait_for_fence` returns terminal `WorkMeasurements`.
Launch evidence may be partial for asynchronous work; each fence contribution
is validated, accumulated with launch evidence, and checked for complete plan
coverage at the final fence. The terminal contribution is recorded before the
canonical receipt marks the fence complete. This lets Metal implementations
report owner-observed accelerator, queue, transfer, and artifact outcomes only
after native command completion without bypassing receipt validation.

There is no CPU or legacy retry in this module. Missing device access,
non-unified memory, queue failure, over-large residency, allocation failure,
unknown work, duplicate submission, and native command failure are distinct
typed errors.
