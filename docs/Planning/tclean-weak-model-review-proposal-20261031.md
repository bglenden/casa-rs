# Proposal: review one weak MT-MFS model coefficient

Truth class: accepted identity-bound owner decision, 2026-11-01; raw failure retained.
Work issues: #991 and #980.

The owner accepted this specific reviewed output on 2026-11-01. The
[decision record](../../resources/casa-compat/evidence/tclean-owner-decisions-20261101.json)
binds the original raw receipt, measurement, binary, input, parameters and model
payload identities. Its disposition is `accepted_by_identity_bound_owner_review`;
the original raw `.model.tt1` failure remains unchanged. Future outputs do not
inherit this approval.

## Accepted decision

Accept **this exact CPU/f32 `clean8` output** as published-model agreement
with CASA, using the criterion below. Retain its raw `.model.tt1` NRMS
failure as the review trigger. This changes no algorithm, CASA parameter,
default, output value or provider bundle. It grants no general exception
for other weak coefficients, outputs or future runs.

The bound inputs and products are in the [current T2 receipt](../../resources/casa-compat/evidence/tclean-standard-mtmfs-t2-20261031.json) and its diagnostic
`model-review-measurement.json`: native revision
`b6a87fa91271e5bf809b86c68e40a979974c1368`, CPU/f32, standard two-term MFS,
512 square, four SPWs, 1,336,320 nominal correlation-channel samples,
eight iterations in cycles of two. The retained input tree SHA-256 is
`e1f1e030a1d1968b627863b3b5386e03fc7ed5770760698a1f6b603f4e885548`.
Excluding 125 AppleDouble filesystem sidecars, all 90 MeasurementSet files
also match the original #607 source hash exactly; the data have not changed.
CASA is casatasks **6.7.6.14** / casatools **6.7.6-14**. The measurement binds
the uninstrumented binary, parameters and numeric product fingerprints.

## Evidence and proposed criterion

Both models have exactly one nonzero pixel, at `(256,256)`, in both terms.
The CASA coefficients are 0.5695148706 Jy and -0.0002664566 Jy;
the native TT1 coefficient is -0.0002667966 Jy. Its raw TT1 NRMS is
**0.0012759712**, exceeding the unchanged **0.001** limit. All other
18 product checks pass, including their required masks, finite values,
recorded WCS, beams and independent intensity/flux checks. CPU/f64 and
Metal/f32 pass this CLEAN case; all three paths pass the dirty case.
The existing #607 alpha/alpha.error rule requires exact validity within
CASA's PB support; each CLEAN alpha product has one differing pixel outside
that support, counted in the receipt. Every other product's validity is exact.

For this output, require those other gates unchanged, identical spatial
support, finite coefficients, compatible coordinates and a common spectral
frame. Compare the **ideal published Taylor models**:

`M(nu) = M0 + ((nu - nu0) / nu0) * M1`

Use each model's published reference frequency. Require
`max_nu abs(native_M(nu) - CASA_M(nu)) / abs(CASA_M(nu)) <= 1e-6`
over the CASA image's published MFS interval: **84.3055550304 to
100.1806045262 GHz**, in their common spectral frame. CASA's intensity
stays positive. For two affine spectra with a denominator of constant sign,
the maximum absolute relative difference occurs at an endpoint, so this
checks the intervening band as well as the sampled frequencies.

The measured maximum is **5.7468675e-7**, or **0.3273 microJy** absolute.
The spectral-index ratio `M1/M0` differs by **5.972e-7** absolute. Deleting
CASA's TT1 term entirely would give **4.026e-5** relative in-band error
and fail the proposed criterion by a factor of 40.

This accepts a roughly **0.128% relative difference in a very weak slope
coefficient**, because its effect on the represented intensity stays below
one part per million. The `1e-6` limit is a proposed engineering criterion
for this identity-bound output, not a universal numerical-error bound.

## Diagnosis and limits

One instrumented retry captured the actual Hessians, RHS values and all
accepted components. Both published model terms were bitwise unchanged
from the original outputs. Each native TT1 update agrees with a 60-digit
closed-form solve to better than 7.4e-17 Jy. Small upstream differences
are amplified by cancellation in the weak coefficient; no incorrect
native solve or different component selection was demonstrated.

This supports agreement consistent with floating-point sensitivity. It
does not prove rounding is the sole cause or that every upstream operation
matches CASA. The ideal model evaluation also differs from the production
basis's Float narrowing; it is **not runtime visibility-prediction
verification**. That remains a separate task obligation.

An independent separate-agent review found this specific proposal suitable
for an owner decision. Oracle Pro is usage-limited and supplied no review.
The measured [comparison figure](/Volumes/GLENDENNING/casa-rs-evidence/tclean-20261031/t2-standard-mtmfs-current/cpu32-model-tt1-failure.png)
and raw failed verdict are retained.

The approved disposition is bound to these unchanged artifacts. Every other
test and unfinished tclean obligation remains. The separate weak **weight**
rule grants no authority for other **model** results. No algorithm, CASA
parameter/default or output value changed when this review was finalized.
