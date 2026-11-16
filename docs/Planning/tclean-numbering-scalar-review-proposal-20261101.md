# Proposal: review one Metal32 final cycle threshold

Status: accepted for this exact output, owner2026-11-01: “Accept this specific output”. Work issues: #980 and #991.

The owner accepted only the unchanged `cli-gpu32/explicit_startmodel` output in the
[activation receipt](../../resources/casa-compat/evidence/tclean-numbering-activation-20261101.json).
Its final `cyclethreshold` is 0.07878153771162033 Jy/beam versus CASA's
0.07878141850233078: 0.119209 microJy/beam, or 1.5132 parts per million.
The difference is 16 Float ULPs. It exceeds the original bound
8.87814185e-8 Jy/beam; the original failed verdict remains unchanged.

For this exact output only, the approved reviewed bound is
`abs(native - CASA) <= 2e-6 * abs(CASA) + 1e-8 Jy/beam`, or
1.67562837e-7 Jy/beam. All original product, mask, recorded metadata/beam,
sumwt, exact count, other scalar and preservation checks still apply.
They pass. CPU64, CPU32 and public Python pass the original scalar limits.
This changes no algorithm, threshold value, CASA parameter/default or contract.
It grants no automatic acceptance to a future output.

A single bounded diagnostic reproduces this native output and leaves the
model pixels exactly unchanged. The final native peak is
0.09847691685766632; the measured sidelobe is 0.9974843263626099.
Both implementations select the Float 0.8 cap. Narrowing the native peak
to Float and multiplying by Float 0.8 reconstructs the reported threshold
exactly. The report formula is correct for native state; the upstream
residual-state cause is unproved. Rounding alone is not established.

The proposed allowance is an engineering acceptance choice, not a universal
floating-point error bound. Its user impact is accepting this tiny difference
in a returned automatic minor-cycle threshold while all recorded scientific
products pass. The unchanged raw failure and trace are retained. A separate
agent independently reviewed the formula and supports this specific request.
No unchanged retry or blind solver change is planned.

The measurement is `metal32-cyclethreshold-review.json` in the raw evidence
root recorded by the activation receipt. It pins revision
`99b1cfad559bf92de9ac070a387b4d2c9165ec97`, binary SHA256
`dd3d846c36713de75347e204e4cd6d9163d72e9add93cd712da11d4bd227aad4`,
raw receipt SHA256
`255917f110351bcf9dbd62ba61daebac68f556d0ad8905ea4917a685a1d1ed57`,
and the shared model payload/mask/coordinates. Oracle: casatasks6.7.6.14 /
casatools6.7.6-14. This approval binds those identities only.

[AGENTS.md](../../AGENTS.md#ask-first) requires asking before reducing
acceptance checks. This is the specific proposed replacement for one failed
scalar comparison; every other task obligation remains.
