# VLASS Imaging Merge-Recovery Contract

Truth class: approved execution contract

Approved: 2026-08-02 by Brian Glendenning; scientific-equivalence amendment
approved 2026-08-03

Verification: `just docs-check` plus the focused recovery-contract and imaging
harness tests

This contract turns the existing VLASS imaging wave into a finite,
merge-oriented recovery. It does not reduce the scientific scope, the
full-geometry rows, the 19-product clean contract, the 32 GiB laptop
acceptance host, or the independent 10x performance requirement.

The machine-readable controlling record is
`tools/perf/imager/vlass_recovery_contract.json`. Launches are recorded in
`tools/perf/imager/vlass_recovery_launch_ledger.json`; the bounded audit of
already-created work is recorded in
`tools/perf/imager/vlass_recovery_salvage_catalog.json` and summarized in
`vlass-imaging-recovery-salvage-audit.md`. The longer historical
optimization plan remains available at archive commit `4c3cf8cc9`; current
`main` intentionally removed that 10,000-line experimental ledger, and this
PR does not resurrect it. Where the archived plan's older scheduling language
conflicts with this document, this recovery contract controls scheduling,
launch budgets, and closeout.

## Recovery outcome

The wave terminates in exactly one of two states:

1. A reviewable four-PR train whose selected production code passes the
   applicable repository gates, full CASA science comparisons, bounded-memory
   laptop execution, and the independent 10x rows.
2. A documented blocker after the bounded attempts are exhausted, with the
   exact failed gate, retained evidence, and one specific decision requested
   from Brian.

“Continue optimizing” is not a terminal state. A likely 100x deconvolution
speedup remains interesting but is not a fixed acceptance target. Once the
scientific floor and 10x requirement make the train merge-ready, stop and ask
Brian whether to merge or spend a separately agreed budget pursuing more
performance.

## Preservation boundary

The unique dirty worktree was preserved before cleanup or formatting:

| Item | Identity |
| --- | --- |
| Trusted pre-experiment checkpoint | `aea444b5e40fde5486e2ea421e5f8e2cf32d6174` |
| Archive branch | `codex/vlass-recovery-archive-20260802` |
| Archive commit | `4c3cf8cc916656f1ece6efc9bbd3fb674e401132` |
| Archive tree | `d46765f2795d3b4c31edef891f523ddae2433b7b` |
| Verified Git bundle SHA-256 | `4ff641f50a5d0248288ba480255765a8870b7bbab6ef9842b3cacc14e75dc519` |
| Binary patch SHA-256 | `55286ec6390dde0552df38abe80e1de2ea84891906d6549675ab0133a57b1aad` |

The bundle and patch are under
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-archive/2026-08-02/`.
The remote archive SHA matches the local commit. The archive commit is
deliberately non-promotable: its normal commit hook found a missing SPDX
header in one experimental file, and the exact evidence was committed with
hooks bypassed rather than edited before preservation.

Draft PR #451 remains an evidence and handoff record. It is not merged,
undrafted, force-pushed, or treated as the final review unit. Audited pieces
move into the recovery PR train; superseded experiments remain reachable from
the archive.

## CASA reference rows

The old `8,183.264 s` number is the connected-63-field **dirty** CASA time. It
is not a matched clean baseline and must never be used to claim a clean
speedup.

Two clean references are now required:

| ID | Selection and cap | Manifest | Wall ceiling |
| --- | --- | --- | ---: |
| `CASA-B-FRAGMENT63-CLEAN-CAP20000-v1` | All 63 fields, SPWs 2–17, `niter=20000` | `vlass-fragment-all-fields-clean-cap20000-casa.json` | 7 days |
| `CASA-B-FRAGMENT63-CLEAN-CAP20000-v2` | All 63 fields, SPWs 2–17, corrected source mask, `niter=20000` | `vlass-fragment-all-fields-clean-cap20000-casa-v2.json` | 7 days |
| `CASA-A-SINGLE-CLEAN-N2000-v1` | Field 1525, SPWs 2–17, `niter=2000` | `vlass-fragment-single-field-clean-casa.json` | 72 hours |

The 63-field CAP20000 row changes only the scientific `niter` ceiling from
the checksum-bound `niter=2000` all-fields manifest. Geometry, data selection,
mask and mask hash, `nsigma`, gain, major/minor-cycle controls, AWProject,
POINTING, W planes, A/WB/conjugate beams, Briggs weighting, MT-MFS, scales,
restoration, and all 19 products remain unchanged. Identity, description, run
label, and evidence-role strings distinguish the new reference.

`niter=20000` is a ceiling, not a claim that CASA will select exactly 20,000
components. Each CASA and casa-rs receipt records the configured cap, actual
iterations/components, cycle boundaries, and stop reason.

The reference launch order is B then A. There is one normal launch for each
row and one shared retry only for an external invalidation such as power,
mount, storage, or host failure. CASA parameter changes, scientific failure,
timeout, or an avoidable harness defect do not silently earn more retries.
No unchanged dirty or reduced CASA reference is rerun.

### CASA-B v1 negative evidence

`CASA-B-FRAGMENT63-CLEAN-CAP20000-v1` completed its CASA imaging call on
2026-08-02 in `6,902.788125 s`, but it is not a valid clean reference. The
configured mask covered pixels `[6243, 6003]` through `[6306, 6066]`, while
the measured `0.431877822 Jy` residual peak was at `[4633, 6183]`. The
largest residual inside the mask was only `0.000659208 Jy`, below CASA's
`0.00169344 Jy` `nsigma` threshold. CASA therefore selected zero minor
iterations, produced an empty model, and performed only the dirty and
restoration work. The elapsed time must not be reported as an all-fields
clean baseline.

The v1 receipt is
`b62daf1de4549d4f5ca186c1b5c02ce501089d97ba72cfc61cefe9fb35084bb5`;
its CASA log is
`535248c9c1910d94666df59a5d45dd5b66e25b759d1587acfa862e1f064671c0`.
The comparator correctly rejected the empty model, but the harness then
decorated the operational failure with a numerical tolerance result and
obscured it as a schema error. The regression is covered by the focused
comparison-protocol test.

The launch ledger records a proposed v2 amendment using the already existing
source mask at pixels `[4602, 6152]` through `[4665, 6215]`, whose stable tree
SHA-256 is
`fabf361e6609a4d66c251458c2ed31bc80978d936e78a39a8f449bd1a63dc322`.
The proposed base and CAP20000 manifest SHA-256 values are respectively
`0cabbe5fdc2f687a10fce3653d018e09db7349a0a1299d16a4f6772557f9f5d9`
and
`63948fe140d5c06c00b924eea407e5afe8ccb2f99e2c927290d9de4644002053`.
The original v1 manifests remain unchanged. Because this was an avoidable
configuration defect rather than an external invalidation, the corrected v2
launch required separate approval. Brian explicitly approved it on
2026-08-02. Its fail-closed dry-run receipt is
`ab7b6c3fa142d0cb3d0f54236b142b08b0aa837f120ffbf4314742723be04b27`.

### CASA-B v2 accepted clean reference

The separately approved corrected run completed successfully on 2026-08-03.
It is the frozen CASA-B clean reference. CASA selected 444 cumulative MT-MFS
minor iterations across eight minor cycles and then stopped on the configured
5-sigma criterion: the corrected-mask residual fell from `0.431878 Jy/beam`
to `0.00105339 Jy/beam`, below the final `0.00106779 Jy/beam` n-sigma
threshold. The final full-image peak was `0.0110944 Jy/beam`; it lies outside
the deliberately bounded source mask and did not prevent the valid masked
stopping condition.

The measured `tclean` wall time is `82,351.832814 s` (22 h 52 m 31.833 s).
The completed receipt is
`30aaf60c4c29595eb9789bcfe1fdab5723bb761295d4e647e4632b8eb6c31be6`;
the manifest is
`63948fe140d5c06c00b924eea407e5afe8ccb2f99e2c927290d9de4644002053`.
The retained request, result, combined output, CASA log, and host-telemetry
SHA-256 values are respectively
`dd0c86ce305e07e5d82431bdc69c1c26dd8937f25ea3ae2044220d5195968a01`,
`8d2b5edb9835ed2f29b4e9e9591647838a45a30a824e0e362f1c2bb842d9a762`,
`1132bff61802078b545fcbcf3e07273c3f473378232a6189911d72e7d99c94f4`,
`810406c782347a742f4f88aa93b938e5da9c1f15ea27f4afe7fdc603fb57abe7`,
and
`7ceae0fc8641c08d275ff827e3ecfb08688219eea4221d2b599aebbef9f06bc3`.
All retained hashes were independently re-read after bundle promotion.

The receipt binds all 63 requested fields, all 16 SPWs, 655,200 selected MS
rows, 12,150-square geometry, the corrected mask hash
`fabf361e6609a4d66c251458c2ed31bc80978d936e78a39a8f449bd1a63dc322`,
and the frozen AWProject, 32-W-plane, POINTING, A-term, WB A-projection,
conjugate-beam, Briggs, MT-MFS `nterms=2`, and scales `[0,5,12]` controls.
Bundle validation passed for the exact 19-product inventory. The product trees
total 9,579,098,756 logical bytes; their individual stable hashes are recorded
in the launch ledger.

The CASA process peaked at 12,818,137,088 bytes RSS. Host telemetry recorded a
minimum 33% free memory, zero throttled pages, 410,894,925,824 bytes of
swap-in, and 462,597,226,496 bytes of swap-out. Despite the large cumulative
swap traffic, field/SPW progress remained steady and no throttling or terminal
memory-pressure stall occurred, so the observed swapping was expensive but
not destructive. Process I/O was 556,159,590,400 bytes read and
126,701,232,128 bytes written.

Two log anomalies do not invalidate the reference. CASA emitted its known
startup `measures_update` SEVERE because the local measures tree is not managed
by `casaconfig`; the receipt binds the runtime and data trees actually used.
One major-cycle diagnostic printed `1.84467e+17` seconds for “time to massage
data”; surrounding timestamps and the independent stage receipt identify it
as a timer overflow. No imaging exception, traceback, divergence, or failed
product was present.

This evidence accepts CASA-B as the matched reference to which the eventual
casa-rs all-fields row will be compared. It makes no claim yet about casa-rs
full-geometry correctness or speedup.

### CASA-A accepted clean reference

The approved single-field run completed successfully on 2026-08-03 and is
the frozen CASA-A clean reference. CASA selected 1,103 cumulative MT-MFS minor
iterations across eight minor cycles. The last minor cycle reduced the masked
peak to `0.00235388 Jy/beam`, below that cycle's `0.00235472 Jy/beam` n-sigma
threshold. After the ninth and final major cycle CASA measured
`0.00234316 Jy/beam` in the mask and `0.0283284 Jy/beam` over the full image,
reported its global stopping criterion, and completed normally. The full-image
peak lies outside the deliberately bounded source mask.

The measured `tclean` wall time is `3,798.068812 s` (1 h 3 m 18.069 s), and
the checked protocol total is `3,846.651839 s`. The completed receipt is
`f9216878e3372ecb4a81f565e33e6b5b2729abf20d0c1d7313892ac4db6a680d`;
the manifest is
`5da8ce24c92b2d47e53784e8600976bf37708086309820cb1b61af6f8982bd9e`.
The retained request, result, combined output, CASA log, and host-telemetry
SHA-256 values are respectively
`dc9ee1cb0a985a08a2cfd275905cece050c692636ac45be39d7094c803c6eaad`,
`fe5f22cfe2fc7b7b1b3441916114ecb8812df9ac73c13b43a8a5551434fb7936`,
`b850a0f44df8a0b4437b0ccaa679de62fb3c113de07b1181724dec69244889f8`,
`573dce4f9675a8f1053ad4f8779a1993f6d28eb67bedafa026f0de4e2cfa291c`,
and
`ee59ea9efbd61139194693b57347436e988ad1a1d7288b36455de2a6c158cf38`.
All retained hashes were independently re-read after bundle promotion.

The receipt binds field 1525, all 16 SPWs, 10,400 selected MS rows,
12,150-square geometry, `niter=2000`, corrected mask hash
`fabf361e6609a4d66c251458c2ed31bc80978d936e78a39a8f449bd1a63dc322`,
and the frozen AWProject, 32-W-plane, POINTING, A-term, WB A-projection,
conjugate-beam, Briggs, MT-MFS `nterms=2`, and scales `[0,5,12]` controls.
Bundle validation passed for the exact 19-product inventory. The product trees
total 9,579,098,756 logical bytes; their individual stable hashes are recorded
in the launch ledger.

The CASA process peaked at 13,784,383,488 bytes RSS. Host telemetry recorded a
minimum 33% free memory, zero throttled pages, 361,460,219,904 bytes of
swap-in, and 405,832,564,736 bytes of swap-out. The substantial cumulative
swap traffic did not produce throttling, convergence loss, or a terminal
memory-pressure stall. Process I/O was 505,119,948,800 bytes read and
123,118,936,064 bytes written, with 9,531 major page faults.

The known startup `measures_update` SEVERE reports an unmanaged local measures
tree; it does not invalidate the bound runtime, data, successful imaging call,
or retained products. This evidence accepts CASA-A as the matched reference to
which the eventual casa-rs single-field row will be compared. It makes no
claim yet about casa-rs full-geometry correctness or speedup.

### First reduced all-fields pair

The first missing all-63-field ladder row has been executed once and is
frozen. It retains all 63 fields and POINTING rows, SPWs `2,7,12,17`,
`4096`-square geometry, AWProject with 32 W planes, A/WB/conjugate beams,
Briggs weighting, MT-MFS `nterms=2`, and the dirty `niter=0` 18-product
contract. No larger row was launched.

The CASA run
`20260803T203820Z-vlass-fragment-all-fields-dirty-4096-four-spw-casa-ab87a6f1`
took `541.352405 s` in `tclean`. Its receipt SHA-256 is
`24c36370670d8c88fcc8849061a34a84a2e25477050bafaeab2cc5317b4fef99`.
The matching release casa-rs executable was built from
`8667b5760d88948548da3e06aa402cd10e11378b`; its SHA-256 is
`3a8d671a9935f85379dd1d4418153f1236913b3554fc87e36b9875a4ad372648`.
It took `225.10 s` wall, yielding a matched CASA/casa-rs speedup of
`2.405x`. This is an early performance warning, not acceptance: it is below
the required `10x`. The casa-rs core took `220.513 s`, of which
`217.063 s` was the initial PSF grid. Peak RSS was `10,045,800,448` bytes,
peak physical footprint was `13,899,198,224` bytes, and the process recorded
zero swaps.

This candidate fixed the two immediately preceding parity defects. Dirty
imaging no longer emits the clean mask, so the exact 18-product inventory now
matches CASA. Shape, unit, masks, coordinate topology, WCS operation grouping,
and all coordinate values also match for every product. The ordinary
full-array numerical amplitudes remain small: representative RMS ratios are
about `38.2 ppm` for `.image.tt0`, `42.0 ppm` for `.psf.tt0`, `53.2 ppm`
for `.residual.tt1`, `4.43 ppm` for `.alpha`, and `2.10 ppm` for
`.weight.tt1`; both model terms are exactly zero.

The row initially failed promotion under the v1 exact-metadata/heuristic
contract. Five
beam-bearing products differ in restoring-beam metadata because the
independently fitted PSFs differ slightly. The casa-rs and CASA major axes are
respectively `2.9585349559783936` and `2.9585354328155518` arcsec; their
position angles are `69.54161071777344` and `69.54159545898438` degrees.
Feeding the frozen CASA and casa-rs PSFs separately through the shared beam
fitter reproduces those values, localizing the remaining beam difference to
the PSF arrays rather than the fitter. The structured-difference review also
requires investigation for `.psf.tt1`, `.psf.tt2`, and `.weight.tt1`.
At that point no tolerance had been changed, so the ladder correctly stopped
at the first unresolved semantic boundary.

The comparison SHA-256 is
`2cc4cb2636c84551c4bb30f5e81649f746f4dab819757549257416a799903bee`;
the bound input and run-log SHA-256 values are
`2fa493f2557fc69cb84c86579c38227710074f3cb1aa1753a71f9838bdc25568`
and
`6eb766d65cf9c5f66262d14f3342859c2a9b157ac89098bad93584ff4ba94c09`.
The comparison validator was corrected after this row exposed an invalid
attempt to reconstruct offset-inclusive regression R-squared from raw RMS.
That historical v1 failure and its artifacts remain recorded.

Brian subsequently approved
`tools/perf/imager/contracts/vlass-scientific-equivalence-v2.json`, SHA-256
`daf1692d23a627d513285cd4c5fc5c81c8e5dd361e6bf2815c74c8897fbc0537`.
It keeps selection, inventory, shape, coordinate, unit, mask, and topology
semantics exact, while bounding full-array NRMSE at `1e-3`, peak-normalized
maximum error at `5e-3`, source flux at `1e-3`, centroid separation at `0.01`
beams, Gaussian restoring-beam kernel and area differences at `1e-3`, and
large-scale coherent error at `1e-4` of CASA RMS. Raw beam parameters,
structure classifications, component order, and cycle count are diagnostics
when their bound scientific checks, stable convergence, stopping, and
no-divergence requirements pass.

A comparison-only re-evaluation reused the frozen product trees and launched
no imaging. All 18 products passed with no failed or incomplete check.
`.image.tt0` NRMSE is `3.8243114447519807e-5`; source flux error is
`1.2287444501503239e-6`; centroid separation is
`1.2874159490437483e-6` beams; beam-kernel NRMSE is
`1.6982385191835777e-7`; beam-area error is
`1.611733808637439e-7`; and the worst coherent block error is
`1.524262302280491e-5`. The portable receipt SHA-256 is
`187dd20c3c7dc70cbb181c622e7330fdb0c0b09d43947c369e183504cc6af80d`.
Correctness is promoted for this row. The unchanged `2.405x` release ratio
remains below the final `10x` requirement and is explicitly retained as a
performance warning.

### Reduced all-fields clean mask preflight

The first all-63-field `4096`-square, four-SPW clean attempt stopped before
minor-cycle iteration because its inherited two-dimensional CASA image mask
had no Spectral coordinate. CASA run
`20260803T221900Z-vlass-fragment-all-fields-clean-4096-four-spw-casa-186ce59e`
took `286.262243 s` in `tclean` before rejecting that mask. It produced no
matched casa-rs row and therefore no runtime ratio or correctness claim. The
failure receipt SHA-256 is
`095d214f5a8b453fd91cd26083b5c63575bf8ea088dffe09f358cfb520eaf0e5`.

The correction reuses the existing deterministic source box while copying the
full coordinate system from the accepted all-fields dirty `.image.tt0`. The
new mask has shape `[4096,4096,1,1]`, Direction, Stokes, and Spectral
coordinates, BLC `[575,2125]`, TRC `[638,2188]`, and exactly 4,096 selected
pixels. Its portable tree SHA-256 is
`8490acb911cbbba78f7a20ba4a1d379e227c3a42dfc7eefcc9b7fd5f4139572f`.
The corrected manifest SHA-256 is
`05994b8ed3566a8a333e8761aa4cc05b8d0534daccabc57f29100f4cd6f8534c`,
and its dry-run preflight passed. The ledger retains the failed attempt as
negative evidence.

### Reduced all-fields clean pair

The corrected CASA retry and one matching release casa-rs candidate are
complete and frozen. Both use all 63 fields and POINTING, SPWs `2,7,12,17`,
`4096`-square geometry, the corrected mask, AWProject with 32 W planes,
A/WB/conjugate beams, Briggs weighting, MT-MFS `nterms=2`, scales
`[0,5,12]`, `niter=2000`, and the exact 19-product contract.

CASA run
`20260803T223749Z-vlass-fragment-all-fields-clean-4096-four-spw-casa-9b96c6ad`
took `3470.197045 s` in `tclean`, selected 193 cumulative components across
11 major cycles, and stopped below its n-sigma threshold. Its receipt SHA-256
is
`9ff187a87357424cc1509e2f79d6e5c929472d23780f536d589c7289bb076beb`.
This is the one frozen reduced clean oracle; it is not rerun.

The matching bounded exact-source-order windowed candidate is commit
`388161fd29cf4474100458dc2fa7c4f4768378a3`. Its frozen release executable
SHA-256 is
`2f67dd816714c4c674742f45313df9aa65d7f6d592cfc73edb7cfb9ca3e2bbbe`.
It took `2674.88 s`, producing an exact matched CASA/casa-rs ratio of
`1.29733x`. This is below the required `10x`. Peak RSS was
`12,230,852,608` bytes, peak physical footprint was `14,089,727,880` bytes,
and the process recorded zero swaps. The run completed 187 minor iterations
across 11 major cycles and stopped on the n-sigma criterion without
divergence. Residual degridding and gridding dominated at `2430.742 s`;
minor cycles took only `8.661 s`.

The candidate matches the exact inventory and passes the mask, PB, PSF,
sum-weight, weight, coordinate, restoring-beam kernel, and restoring-beam area
checks, but it fails the v2 clean science floor. `.image.tt0` and
`.image.tt1` NRMSE are `0.00982925` and `0.0113314`; `.residual.tt0` and
`.residual.tt1` NRMSE are `0.0108125` and `0.0109003`; and `.model.tt1`
NRMSE is `0.0260674`. These exceed the `1e-3` ceiling. Alpha and alpha-error
also retain topology mismatches. The comparison input, raw output, and log
SHA-256 values are
`9a8771cca77c19307a48c33f6127737cc8e910da98a50cd903939ed3f2ca0d77`,
`da18a998ae09699bb8e0ffd1ca98e8542c10fd9c88626ef35f12b472bf22119f`,
and
`f154ec4b8a4f7f08314097e81613111a8b3121211da5e24fc94b5bbd6264706e`.

This is the current finite-recovery stop boundary. No full-16-SPW or
full-geometry casa-rs clean row is launched until the reduced clean
model/residual semantic defect is fixed and this row passes. The windowed
architecture's bounded-memory result is retained as positive evidence; its
unchanged scientific failure and `1.29733x` ratio are retained as negative
evidence.

## Candidate budget and promotion

The salvage audit receives at most eight engineer-hours. It may select one
primary candidate and one reserve from code and evidence that already existed
at archive commit `4c3cf8cc9`. It does not invent another architecture.
Retired families stay retired. Positive diagnostic-only work is not promoted
without an already executable, bounded, scientifically testable route.

At most two immutable casa-rs candidate freezes may enter the promotion
ladder. Their full-size budget is four planned rows—A and B for each
freeze—plus one shared external-invalidation retry. A candidate reaches full
geometry only after:

1. The real 4096-square, four-SPW clean row passes.
2. The real 4096-square, full-16-SPW clean row passes.
3. Its memory plan and stage projection are credible for the 32 GiB laptop.

Reduced rows are development gates, never final evidence. Smaller diagnostics
are permitted only for an isolated semantic regression in the selected
candidate; they cannot reopen an architecture tournament.

## Correctness and performance

CASA is the scientific reference, not an implementation transcript. Bitwise
identity, identical component order, and identical major-cycle history remain
diagnostics rather than gates. The controlling numerical contract is
`tools/perf/imager/contracts/vlass-scientific-equivalence-v2.json`.
Promotion still requires:

- the exact data, field, SPW, POINTING, AWProject, weighting, MT-MFS, mask,
  scale, and restoration semantics;
- the exact 19-product inventory with correct coordinates and metadata;
- all frozen numerical, topology, beam-aware, source, morphology, flux,
  residual/noise, dynamic-range, and stable-domain alpha gates;
- stable convergence without divergence or conspicuous coherent artifacts;
- an actual-iteration, component-count, cycle, and stopping receipt; and
- bounded memory, compression, swap, I/O, progress, and end-to-end stage
  evidence on the 32 GiB laptop.

Each final row independently requires at least 10x against its corresponding
matched CASA reference. No speedup is inferred across dirty versus clean,
single-field versus 63-field, reduced versus full geometry, or different
product sets.

For every newly completed CASA and release casa-rs pair with identical
processing, report `CASA wall / casa-rs wall` immediately. A failing
correctness pair still reports the ratio as diagnostic evidence, clearly
labelled non-promotable. This early-warning rule does not turn unmatched or
debug-build timings into performance evidence.

## Performance-preservation amendment

Brian approved this amendment on 2026-08-04 after review showed that the
headline performance had not been lost through a same-workload regression.
The `28.65`--`29.43` second four-SPW result and the `101.646` second full-16-SPW
result survived through the single-field ladder. Performance collapsed only
when execution moved to the connected 63-field clean row, where the
single-position image-response cache was not admitted and `2,430.742` of
`2,674.88` seconds was spent repeating exact residual degridding/gridding.

The later five-minute autoresearch row did not cause that loss. It is an
explicit frozen-model residual-operator proxy. It executes no clean-from-zero
minor cycle or model update and can never satisfy an end-to-end CLEAN
performance gate.

The machine-readable preservation contract is the
`performance_preservation` member of
`tools/perf/imager/vlass_recovery_contract.json`. It freezes two release-mode
single-field landmarks:

1. `4,096` square, SPWs `2,7,12,17`, `niter=2000`: historical casa-rs
   `28.65` seconds against CASA `3,631.809729` seconds, nominally
   `126.7647x`.
2. `4,096` square, all 16 SPWs, `niter=2000`: historical casa-rs
   `101.646` seconds.

Every selected production candidate must preserve those workloads before
all-field promotion. The receipt must bind one release executable and prove
real CLEAN from zero, real minor-cycle records and model changes, response
calibration and synthesis, sparse MT-MFS state, radix statistics, and the
exact final residual refresh. A frozen model, zero-work minor cycle, debug
binary, isolated stage metric, or proxy wall time cannot substitute.

A regression of more than ten percent on either landmark blocks silent
promotion and requires diagnosis plus Brian's direction. A demonstrated
landmark capability remains in the candidate lineage until it is generalized,
superseded by a faster same-landmark implementation, or explicitly retired by
Brian. Branch restructuring, recovery selection, and experimental cleanup are
not retirement decisions.

The finite implementation sequence is:

1. revalidate the two landmark rows with one frozen release executable;
2. run the already-available model-delta census on the real `4,096`-square,
   four-SPW, all-63-field CLEAN trajectory;
3. generalize the existing frozen-base response cache from one position to a
   bounded deterministic position set, retaining exact shadow checks,
   decision-margin admission, invalidation, and exact fallback;
4. promote the matched all-field four-SPW and full-16-SPW rows; and
5. return to the required `12,150`-square single-field and 63-field acceptance
   rows.

This is not a reopened architecture tournament. It integrates and generalizes
the already-demonstrated response mechanism while preserving the selected
exact AW operator, sparse RHS, radix statistics, scientific-equivalence
contract, memory contract, and finite merge decision.

## Finite delivery train

The merge train has four review boundaries:

1. Contract, evidence harness, CAP20000 manifest, launch ledger, and CASA
   references.
2. Audited shared execution substrate: multi-SPW, POINTING, AWProject,
   MT-MFS, product parity, resource planner/telemetry, and canonical UI/task
   controls only.
3. One selected performance candidate, with the reserve used only after a
   declared primary failure.
4. Final acceptance receipts, bounded refactor, issue #454 experimental
   cleanup, sidecar reviews, and closeout.

The recovery budget is eight engineer-hours for salvage and 48 engineer-hours
total inside a 72-active-hour window. Valid long-running CASA reference wall
time is excluded from those active-work clocks. CI is run at coherent PR
boundaries and once for final verification, not after every scientific
measurement. Existing green evidence is reused whenever intervening changes
cannot affect it.

No PR is merged automatically. When the train is merge-ready—or when the
bounded attempts end in a blocker—the agent presents the evidence and waits
for Brian’s decision.

## 2026-08-05 promoted reduced all-field candidate

The selected exact-source-order candidate now represents replay phase screens
as separable X and Y complex axes and reconstructs their product in the Metal
kernel. A fully fused kernel/phase experiment was rejected before execution
because its projected `35.459 GB` compile peak exceeded the bounded
`25.770 GB` budget. The separable design preserves exact A/W kernels, source
order, POINTING semantics, and the expanded fallback while reducing the
resident global replay program from `17.688 GB` to `5.225 GB`.
It is now the production Metal replay representation; no hidden environment
flag or new scientific/UI control is required.

The exact matched reduced row is complete:

- CASA: `3470.197045 s`;
- release casa-rs: `257.40 s`;
- CASA/casa-rs: `13.481729x`;
- release executable SHA-256:
  `82b01c3950eac4187c9e88e5282d606fc0743a5398f2da4b22872ad70c95c16b`;
- all 63 fields and POINTING, SPWs `2,7,12,17`, `4096` square,
  AWProject/32 W planes, Briggs, MT-MFS `nterms=2`, scales `[0,5,12]`,
  deterministic mask, and `niter=2000`;
- 193 minor components, 12 recorded major cycles, stable n-sigma stop;
- peak RSS `12,634,636,288` bytes, peak process footprint
  `11,765,475,792` bytes, and no destructive runtime swapping; and
- exact 19-product inventory and complete numerical, topology, metadata,
  source, and restoring-beam contract pass.

Image tt0 and tt1 NRMSE are `7.37654e-5` and `9.74054e-5`. Alpha and
alpha-error retain only 37 cutoff-edge pixels (`2.20537 ppm`) and
`4.60017e-4` coherent block RMS. Brian's approved optimization-safe contract
uses `1e-3` normalized/coherent error and a `10 ppm` cutoff-mask bound for
these derived products; it does not require implementation-identity at a
threshold edge. The contract SHA-256 is
`58cece2f388f6098058598e19e00d4998a8c321f238d062ca8d567cafd29143a`.

The immutable reassessment receipt SHA-256 is
`5defc479822415b1c7cec24ac955a442b0015228f76a5a12b718414156bd8918`;
the source comparison output SHA-256 is
`35d1defac1e026025bdca32a12db61d1a9776b4cc8927382ea42185d72d69c42`.
The production checkpoint is commit
`4d787cb1fb8047d5e3087307db8f03bd37b8b6a4`.
This supersedes the previous reduced-row correctness and performance stop
boundary. It does not satisfy or weaken the required full-16-SPW and
`12,150`-square acceptance rows. No unchanged CASA reference was rerun.
