# T56 planner-owned Metal runtime source study

Truth class: implementation evidence
Last reality check: 2026-09-01
Verification: `CARGO_INCREMENTAL=0 cargo test -p casa-imaging-runtime metal_runtime --lib`; `just arch-check`

T56 implements only the physical Metal runtime named by ADR-0010. It does not
select an imaging implementation or define a kernel, precision, reduction,
normalization, mode, or product rule. T57 remains responsible for scientific
Metal implementations behind the existing registry and Numerics Contract.

## Old-to-current map

| Evidence lineage | Proven mechanism | Current owner and disposition |
| --- | --- | --- |
| Pre-cutover casa-rs `fff9c2d553eace4b6a57b1df9ded4773f2263ceb`, especially `crates/casa-imaging/src/lib.rs` | `objc2-metal` device and queue creation, shared buffers retained through command completion, explicit command status checks, and grouped resident inputs. The archived experiments also show that global atomics and repeated whole-grid transfers are not generally viable. | `casa-imaging-runtime::MetalRuntime` retains the device/queue/buffer/fence mechanics through the current Execution DAG and Resource Authority. Mode-specific dispatch, frontend probes, runtime shader compilation, and the displaced monolithic owner are rejected. |
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

`MetalRuntime` then opens the selected device and queue, allocates the closed
physical-slot ledger with shared storage, and submits one selected node at a
time. The borrowed `MetalEncodingContext` exposes only that command buffer and
the node's plan-owned buffers to the implementation owner. A committed
`MetalCommandFence` must be waited or cancellation-drained; dropping it also
drains synchronously so device work cannot outlive its retained buffers.

There is no CPU or legacy retry in this module. Missing device access,
non-unified memory, queue failure, over-large residency, allocation failure,
unknown work, encoding failure, and native command failure are distinct typed
errors.
