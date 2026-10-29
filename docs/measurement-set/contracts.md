# Intermediate observation contracts

Status: revision 7; the read contract the M0/M1 trial realizes, frozen
narrowly for M2 by the owner on 2026-10-25
Truth class: design proposal
Date: 2026-10-25

This is the interface between persistent observations and numerical consumers.
The governing principle is that MSv4 is the future: the interface takes MSv4's
native shape, and the MSv2 adapter assembles MSv2 data into it.

Sections 1 to 6 and the creation part of section 7 are the **current
contract**: what the trial's `casa-observation` crate and its `casa-ms`
adapters implement, stated directly, with the profile's refusals. The owner
froze them, narrowly, for M2 on 2026-10-25 (#898): M2 builds on them, and a
change to them follows the implementation plan's change control (its section
10). The freeze is narrower than the M1 gate. The M1 rows the trial did not
complete (trial README, "M1 coverage") stay open, and porting the applications
is expected to show where the design is not sufficient; such a finding
reopens the section concerned through the same change control. Section 8 holds
the **provisional capabilities**: designed but not realized or not proven,
each with the acceptance obligation it must meet before it joins the contract.
Updates (section 7) are provisional too. How the design got here (decisions D1
to D25 and the trial's revisions) is recorded in the
[trial's design record](trial/revision-2-design.md) and the
[trial README](trial/README.md), not here.

The bounded intermediate is `VisibilityBatch`. Chunk and tile mean physical
storage; compute window means a kernel's view. The name does not imply reading
visibility payloads for a flags-only request.

## 1. Contract rules

| Rule | Required behavior |
| --- | --- |
| C1: source independence | No physical row IDs, Zarr keys, storage managers, or MS-version branches in numerical processing loops. |
| C2: faithful values | Coordinates and references are native casa-rs Measures types/views, directly accepted by its conversion APIs. Canonical coordinate units; explicit frames and precision, native flag and weight semantics, full data-group association, ordered baselines. No silent scientific transform: what the profile cannot represent is refused. |
| C3: layout independence | Separate sample topology, memory layout, scientific grouping, and storage chunking. |
| C4: bounded ownership | Every live payload, index, decode buffer, cache, packed copy and queue is admitted; borrowed views cannot outlive/refill their storage. Independent work can retire separately; shared storage stays live until its last use completes. |
| C5: invertible updates | Sample-preserving reorder/selection retains a checked inverse source map; reduction does not. Shared variables have explicit mutation scope. |
| C6: honest guarantees | Unsupported profiles, layouts and native features, and non-atomic writes, are explicit typed refusals. |
| C7: bulk execution | Bulk conversion is a requirement across Measures and quanta, including future measure families. Bind numeric kind, layout and invariant context outside scalar loops; use native array views and family-specific kernels. No required per-value object allocation, virtual dispatch, coordinate reconstruction or repeated invariant setup/provider lookup. |

## 2. Topology and identity

### The catalogue

An opened observation supplies a `Catalogue`: antennas, fields, scans,
observing sessions and partitions, and for each partition everything a batch
needs that does not vary by sample (channel frequencies and widths and their
frame, correlation products, the baseline table, the time scale and origin,
the data groups with their units and UVW frame). A batch links to its
partition's entry and copies none of it. IDs are opaque indices scoped to that
opening. They are not casacore subtable row numbers, globally unique names, or
serialized identities; backends keep their own mapping. Large pointing,
ephemeris and time-dependent metadata are not part of the catalogue and are
queried in bounded windows; opening does not build a sample-sized
metadata snapshot beyond the partition grid of section 2. Pointing is such a
query (`Observation::pointing`, below); ephemerides are provisional
(section 8, #942).

A **partition** is one store unit (an MSv4 dataset; the MSv2 rows that one such
dataset would hold), defined by its content: spectral window (frequencies,
widths and frame), correlation products, intents, observing session, and, in
MSv2 only, the feed pair. The MSv2 adapter partitions rows as XRADIO's
converter does by default (`DATA_DESC_ID` × `OBS_MODE` × `OBSERVATION_ID` ×
`EPHEMERIS_ID`, and the feed pair), so one observation opens as the same
partition set from either store. Each partition also records the fields and
scans its samples visit, and its antennas' receptors (polarization types and
angles): an MSv4 dataset's `antenna_xds` holds them, and an MSv2's `FEED`
rows as casacore's `MSIter` and XRADIO's converter select them, the
partition's window's own rows once any row names a window, else the rows
for every window (-1). Of `FEED` rows of one antenna, feed and window that
differ (receptors that vary in time), the last in table order is read, as
casacore's `MSIter` reads it (with a warning); XRADIO's converter refuses
them, and casa-rs's reports them as a loss (owner decision on #946,
2026-10-28).

A field's phase centre is stored in one frame for the whole catalogue (a
data group has one UVW frame, MSv4 one phase-centre frame). The MSv2 adapter
resolves `PHASE_DIR`'s frame per row where the column's reference varies
(`PhaseDir_Ref`) and refuses fields in different frames. A field read from an
MSv2 carries `FIELD.TIME` as its reference epoch, at which a direction in a
frame that moves against the sky is converted; an MSv4 field has none, its
frame being celestial.

**Pointing** is read per partition, never held in the catalogue:
`Observation::pointing` takes a partition, antennas, a span of times in the
partition's time system, the direction (the beam's, or MSv2's tracking
target, which MSv4 does not hold) and a byte budget, and returns a pointing
series: per antenna, the samples within the span and its nearest each
side, each a polynomial in time about its own origin, in the store's frame.
A pointing bracket evaluates them at one time: the sample whose interval
covers it (the nearest), the last at or before it and the first at or after
it. MSv2 reads its `POINTING` rows through today's imaging reader, so its
brackets are that reader's bit for bit, except that of two rows equally near
a time it covers with the earlier where that reader takes the lower row
(the same wherever an antenna's rows are in time order). That reader is
casa-rs's, not casacore's: `MSPointingColumns::pointingIndex` covers with
the first row in table order, gives a row of `INTERVAL` 0 half the gap to
the antenna's next row and a negative interval every time (#948). The
cutover keeps the boundary's tie rule (owner decision on #946, 2026-10-28).
MSv4 reads `POINTING_BEAM`, whose samples state no interval, so nothing
covers and a consumer's law chooses between the samples either side; XRADIO keeps
one polynomial term and defines no origin for more, so more are refused.

An **observing session** carries the telescope, project and observers, and
MSv4's archive identifiers (`execution_block_UID`, `session_reference_UID`,
`scheduling_block_UID`). An MSv2 holds those only where an importer recorded
them; the adapter reads the `ExecBlock` and `SchedulingBlock` UIDs of
`OBSERVATION::SCHEDULE` as XRADIO's converter does, and not `ASDM_EXECBLOCK`.

### Binding two catalogues

`Catalogue::bind_to` maps one opening's entities onto another's by content,
never by list position or display name (converters rename antennas and
fields):

| Entity | Matched by |
| --- | --- |
| Antenna | station and position, within a stated tolerance |
| Field | phase centre: frame, and angles within the tolerance |
| Scan | casacore number, or name when a scan has none |
| Session | telescope, project and observers, and each archive identifier both sessions state |
| Partition | the spectral and product content (frequencies and widths bit for bit, frame, products in order, intents), the bound session, the bound set of fields, the bound set of scans when both stores name scans, and the feed pair when both stores have one |

A key one store lacks (MSv4 has no feed axis; an MSv2 may hold no archive
identifiers) does not distinguish, but the binding is **one to one**: two
entities that one catalogue keeps apart never bind to the same entity of the
other. An entity that matches nothing, more than one, or the same counterpart
as another is a typed refusal; an ambiguity is never resolved by position.
Ephemeris is not yet a partition key.

### Binding a calibration table

A calibration table numbers antennas, fields and spectral windows by the rows
of its own `ANTENNA`, `FIELD` and `SPECTRAL_WINDOW` subtables. Catalogue
identities are scoped to one opening, so a table never holds them: a store's
**persisted namespace** (`casa_ms::observation_io::persisted_namespace`) gives
the subtables a table solved on it carries and the row of each catalogue
entity there, for the store opened with given options (partitioning by field
or scan changes the partitions, so the namespace keeps the catalogue it
describes and the writer refuses a solve from another opening). An MSv2's
namespace is its own subtables, its catalogue's antennas, fields and
sessions being their rows and each partition naming its data description's
window, so a table written through it has the rows CASA's `gaincal` copies
and CASA applies it to that MeasurementSet by its own association. An MSv4 has no such rows; its
namespace is the subtables its MSv2 conversion holds, in catalogue order, so
a table written through it applies by CASA's association only to that
conversion, and is bound to any other store. The writer translates a solve's
catalogue identities to those rows, copies the subtables and records the
store's path as `MSName`; it never writes a catalogue identity.

Binding a table to a store (`CalibrationBinding`, owned by calibration)
compares the table's rows with the store's namespace rows, in this order:

1. **Stated associations**, as CASA's `spwmap` states them: a partition's
   window (several may share one row, including the row an unstated window
   binds to below, as `spwmap` leaves a window unlisted there on its own
   row) and an antenna (one to one).
2. **CASA's own association**, where the caller states that the table was
   written through this store's namespace, as CASA assumes of a table applied
   to the MeasurementSet it was solved on: each row binds to its own index,
   even where two rows hold the same content, once the rows are checked
   consistent (as many of each kind, antenna names agreeing as below; a
   window may hold a solve over part of its channels). Names, row counts and a gain table's collapsed channels do
   not establish it: two windows of different channels can collapse to one
   row alike.
3. **Content** otherwise: an antenna by station and position, a field by
   phase centre, a window by frame and channels, or the one channel a gain
   table collapses them to, of the summed width (in magnitude: CASA's solve
   holds widths positive) at the channels' arithmetic mean frequency (CASA's
   `gaincal`, whose `ChannelAverageTVI` averages frequencies unweighted
   before a G or T solve, whatever the widths, and casa-rs's writer since
   #917). Names never bind, since converters rewrite them, but an antenna
   whose name disagrees beyond a converter's `_STATION` suffixes is refused:
   another antenna on the same pad does not take its gains. An entity the
   other side lacks binds to nothing, so an antenna subset applies as CASA
   applies a table missing an antenna (`calflag` flags its samples); a field
   the table lacks likewise has no solutions until a `gainfield` policy is
   built. A window that a table window bound to no other window overlaps
   in frequency is refused (a solve over part of its channels), not left
   unbound, in any frame: a table solved in one frame and applied to the
   store regridded to another solves none of its windows, so it must be
   stated rather than flag every sample. The applier must have opened the store as
   the namespace's opening did, or binding is refused. Two candidates, or two entities
   claiming one row, are refused: equal grids or positions alone do not
   prove two rows are one entity. Directions compare within a stated
   tolerance: the catalogue holds them as direction cosines, which move an
   angle by a few units in the last place at each store crossing (#918).

### One topology: the time × baseline grid

A batch is whole times of its partition's time × baseline grid, MSv4's own:
`sample = time_index × baseline_count + baseline_index`. Field and scan are
per time; timestamps and baseline pairs are held once (O(T + B) entries, not
O(T × B)). There is no irregular topology. The MSv2 adapter places rows on the
grid when the MeasurementSet is opened, reading MAIN's key columns in bounded
row chunks. Two rows claiming one cell are distinct acquisitions the partition
keys failed to separate: opening refuses them, naming both rows, rather than
discarding or averaging either.

A cell no acquisition fills is **padding**. Every store delivers it flagged,
with a NaN weight: the MSv2 adapter pads with flag true and values, weights,
UVW and time auxiliaries NaN; an MSv4 store holds its own padding (XRADIO's
converter pads with NaN values and weights and leaves `FLAG` false; casa-rs's
writer pads the same way with `FLAG` true), and the MSv4 adapter folds the
non-finite stored weight into the delivered flag. A NaN weight is padding's
one mark in both stores; the MSv2 adapter padded with weight 0 until the
write-back trial found that an MSv4 written from it could not tell padding
from a flagged acquisition. The estimate
reports the fraction of cells an adapter pads before anything is read; it is 0
for MSv4, whose store holds its grid whole, and that says nothing of the cells
the store itself holds as padding.

Keep three facts separate:

- **Selection:** which logical elements the request includes.
- **Acquisition occupancy:** the batch carries absence only as padding, which
  the delivered flags mark. It does not record whether an MSv4 store's padding
  was once an MSv2 row; recovering original row existence is a conversion
  policy (section 8).
- **Validity and flags:** numerical validity of requested values and native
  scientific flag state, varying by channel and correlation.

```rust
// The current shape; casa-observation has it.
pub struct VisibilityBatch<'a> {
    pub catalogue: &'a Arc<Catalogue>, // per-partition constants live there once
    pub partition: PartitionId,
    pub time: EpochView<'a>,              // whole times of the partition's grid
    pub baselines: &'a [OrderedBaseline], // the partition's table, from the catalogue
    pub field: ArrayView1<'a, FieldId>,   // per time
    pub scan: Option<ArrayView1<'a, ScanId>>, // per time
    pub timing: SampleTiming<'a>,         // time centroid, interval, exposure
    pub first_channel: usize,
    pub frequency: FrequencyView<'a>,     // a window of the catalogue's
    pub correlations: &'a [Correlation],  // the delivered products, shared, never copied
    pub group: DataGroupView<'a>,         // [sample, channel, correlation] payload, UVW
    pub fragments: &'a [GroupFragment],   // scientific groups (section 4)
}
```

### Fields along time

For native MSv4, distinguish a dimension from an auxiliary coordinate:
`VISIBILITY[time, baseline, frequency, polarization]` carries
`field_name[time]`. A sequential mosaic may therefore have fields
`[A, A, B, B, A]` along its time axis without a field axis or any averaging
across fields. Optional field-based partitioning produces separate datasets
instead; the API preserves the sample-to-field association in either case,
including selection and slicing. Per-antenna pointing is a separate
association. This does not establish a representation for simultaneous beams
or phase centres, or for per-baseline field changes.

Each data group of an MSv4 dataset has its own `field_and_source` context and
UVW frame, and a traversal of a group delivers that group's fields. Fields
from different contexts are distinct unless name, phase centre and source all
agree. Before writing, the writer validates that every sample of a time agrees
on the field, against the declared grid, and refuses otherwise; it does not
copy XRADIO's maximum-ID reduction. Distinct acquisitions never overwrite one
another in one time/baseline cell. The
[converter evidence](architecture.md#converter-evidence-for-mosaics)
distinguishes observed upstream behavior from this preservation contract.

## 3. Numeric arrays, metadata, and data groups

### Units and measure references

The processing interface guarantees canonical coordinate units, so a kernel
never inspects a string to discover whether a frequency is in Hz:

| Quantity | Processing representation |
| --- | --- |
| Frequency and channel width | Hz |
| UVW and Cartesian positions/distances | m |
| Angular coordinates | rad, with coordinate representation and reference retained |
| Durations, integration intervals and exposure | s |
| Epochs | A split-precision epoch view: time scale, an `MjdHighPrec` origin and f64 offsets in seconds; never an unlabelled f64 Unix timestamp |

The adapters check every stored unit and reference once, at opening, and
refuse what the profile does not cover rather than rescale or reinterpret it.
MSv2 `TIME` and `TIME_CENTROID` take their fixed `MEASINFO` reference
(UTC by default, per the MeasurementSet definition); a variable reference
column, or two columns that disagree, is refused. MSv4 times are read only as
`format: unix` with `scale: utc`, following astropy, whose Unix epoch is UTC
in every scale; any other scale is refused. Requesting other units or a
conversion at binding is provisional (section 8).

Each measure view associates values with quantity kind, unit, reference/frame
and representation. References shared by an axis or batch are stored once.
There is no per-visibility measure object. A data group's UVW view carries its
**effective** frame: for MSv2, CASA's convention that `UVW` is computed in the
phase centre's frame whatever its `MEASINFO` says (simulators and fillers
commonly label it `ITRF`), resolved once at opening; MSv4 states it, and a
group whose UVW frame is not its phase centres' frame is refused. The stored
label stays in the catalogue for faithful writing.

Unit normalization never performs a reference-frame or time-scale
conversion: Hz in TOPO and Hz in LSRK remain distinct, and UTC does not become
TAI by expressing time in seconds. Explicit scientific transforms use casa-rs
Measures and its declared context and provider. Reading a dataset never
rewrites its units or rounds untouched persistent values.

### Shared Measures types and bulk conversion

**System-wide requirement for M2 consumers:** all supported Measures families
must offer natural, efficient bulk operations. The trial's views
(`EpochView`, `FrequencyView`, `UvwView`) are the first step; the bulk
acceptance below remains open. LSRK↔TOPO is one non-exhaustive example.
Frequency conversion plus one other family is not sufficient acceptance. The
same contract applies to present and future scalar/vector measures and
compatible quanta conversions, each with its own mathematics and accuracy
requirements. This does not assert support for currently unsupported
reference-frame routes.

| Family / illustrative operation | Reusable work and value-dependent work |
| --- | --- |
| Directions: celestial, terrestrial or horizon references | Prepare common epoch/site/model terms; transform direction arrays, retaining required per-direction nonlinear aberration, deflection or refraction work. |
| Epochs and time scales | Reuse route/provider state and operate on split-precision arrays; evaluate varying EOP/relativistic terms and leap-second boundaries at their required scope. A whole array need not share one offset. |
| Positions and spatial vectors | Bind units, reference and ellipsoid/model; apply Cartesian/geodetic and vector operations in bulk, retaining necessary per-position nonlinear work. |
| Baselines and UVW | Reuse epoch/phase-centre geometry for many vectors; preserve ordered-baseline and tangent-basis conventions and changing contexts. |
| Frequencies, radial velocities and Doppler conventions | Reuse valid context-dependent terms; apply spectral reference and convention transforms, including nonlinear relations and explicit rest-frequency/source-velocity context. LSRK↔TOPO is one route. |
| Earth-magnetic and other supported vector measures | Reuse relevant frame/model state for bulk vector conversions; supply genuinely position/time-dependent inputs at their proper scope. |
| Quanta/unit conversions | Bind dimensional compatibility and scaling once, then apply to numeric arrays; retain any additional semantics of the quantity. |

**One semantic type system:** the batch's frequencies, directions, positions,
epochs, durations, UVW references, units and frame identifiers are owned by
casa-rs Measures and quanta (`casa-types`, a definitional crate every imaging
crate may depend on under ADR-0016). `FrequencyView`, `EpochView`, `UvwView`
and analogous names denote native Measures views, not observation-specific
wrappers. `MFrequency`/`FrequencyRef`, `MEpoch`/`EpochRef`/`MjdHighPrec`,
`MDirection`/`DirectionRef`, `MPosition`/`PositionRef` and `MeasFrame` are the
starting points; their existing shape is not a compatibility constraint.
Observation-specific entity IDs and axis associations remain in the
catalogue. The I/O boundary decodes MSv2 `MEASINFO`/`QuantumUnits` or MSv4
measure attributes into these references once; there is no later MS-measure →
computation-measure translation. Avoid a `Vec<MFrequency>` or a populated
`MeasFrame` per element: numeric buffers plus checked shared metadata.
Split-precision epoch arrays stay directly usable without collapsing them into
an f64 MJD.

**Prepare, then apply:** Measures owns the conversion route and its bulk
execution:

| Scope | Work |
| --- | --- |
| Bind once for a conversion request | Validate source/destination references and units, choose the route and scientific/interpolation policy, bind the prepared Measures provider and output representation. |
| Prepare each required scientific context | Resolve relevant source/destination context and model data, in bounded batches when they vary; derive reusable factors, matrices, interpolation data or other family-specific state. |
| Apply to a numeric slice | Run the family's typed arithmetic into a supplied reusable output buffer, or an explicitly supported in-place view. Avoid repeated invariant route/frame/provider setup, per-value allocation and locking. Retain required per-value mathematics and validity checks. |

Varying input coordinates can themselves require different context
evaluations; bulk preparation supports that without pretending one matrix,
offset or factor fits every value. The protocol permits specialized typed
kernels, nonlinear and iterative operations, and per-element error and
validity policies; it does not require a universal matrix transform.

```rust
// Proposed protocol, not existing method names or one universal transform type.
for group in bounded_context_groups {
    let prepared = transform.prepare(group.source_frame(), group.target_frame())?;
    let converted = prepared.apply_into(group.measure_view(), &mut output)?;
    consume(converted); // carries the resulting Measures reference and units
}
```

Source and destination contexts are distinct inputs where required.
Reference-enum equality alone does not establish an identity transform when the
physical contexts differ. Missing context or unsupported frames fail binding or
preparation; nonfinite values, range errors and partial output follow an
explicit policy. A prepared transform is tied to every input that affects it,
and context changes invalidate or advance it; approximate reuse over an
interval requires an explicit science tolerance. Prepared coefficients are
immutable and shareable; output and mutable scratch follow the admitted work
unit. CASA-compatible ordered interpolation keeps its declared sequence across
batch boundaries. The existing MS-specific prepared-factor code moves into the
Measures engine; storage and imaging do not maintain competing routes, and an
adapter cannot perform an undeclared frame change on open.

Current mechanisms and duplication are pinned in the
[Measures source map](README.md#source-map-for-external-reviewers). M1's
`measures-bulk` packet must prove direct use by consumers, scalar/bulk/CASA
agreement, bounded independent work and measured performance against both the
current scalar and already-prepared production paths for the affected
families.

### Layouts and scalar precision

Every payload view is `[sample, channel, correlation]`, correlation fastest,
the order both stores hold, so no adapter transposes. A consumer that wants
channel-major or a calibration layout takes a permuted or sliced `ndarray`
view of the batch; there is no layout choice in the request. A fixed channel
axis within a batch does not imply uniform channel spacing or ascending order.

The read profile is fixed:

| Role | Element | Refused |
| --- | --- | --- |
| Visibilities | `Complex<f32>` | complex128 and real-valued data (section 8) |
| Flags | `bool` | bitmask flags (`uint8`) and flag categories |
| Weights | `f32` | |
| UVW | `f64`, metres | |
| Time centroid, exposure | `f64` seconds per sample (MSv4 `[time, baseline_id]`) | `TIME_CENTROID_EXTRA_PRECISION` when centroids are requested; four-dimensional `EFFECTIVE_INTEGRATION_TIME` |

Opening checks every MSv4 variable's exact Zarr data type, dimension names and
order, and shape before any decode, and refuses a variable of another type even
of the same width. A partition's correlations are one list for every baseline;
an MSv4 dataset with `polarization_mixed` (a basis per baseline under the
generic labels `PP`, `PQ`, `QP`, `QQ`) is refused at opening.

Timestamps and baseline pairs cost O(T + B) entries per batch, not O(T × B);
UVW and genuinely sample-dependent metadata are per sample. Compute windows
are views: `channel_window` and `sample_window` (whole times) slice a batch
without copying. Arbitrary sparse gathers are represented by explicit index
views or packed by the backend, never disguised as a contiguous slice; the
trial's one gather is a correlation subset, decoded whole and gathered by the
adapter (section 4).

```rust
pub struct DataGroupView<'a> {
    pub entry: &'a DataGroupEntry,                 // name, units, UVW frame
    pub values: Option<ArrayView3<'a, Complex32>>, // [sample, channel, correlation]
    pub flags: Option<ArrayView3<'a, bool>>,       // absent acquisitions folded in
    pub weights: Option<ArrayView3<'a, f32>>,      // channel stride 0 when per correlation
    pub uvw: Option<UvwView<'a>>,                  // [sample, 3], effective frame
}
```

### Buffer ownership and array views

The batch carries the **actual numerical data**, accessed through typed array
views. A view is a borrowed reference into an existing numeric buffer, with
shape and element strides; making it copies nothing, and indexing it reads the
stored value directly. Before delivery the cursor completes every
read/decode/placement the batch needs: reading an element through a delivered
view performs no I/O or decode. The buffers belong to the caller or a
`BufferPool` (section 5); the borrowed `VisibilityBatch<'a>` keeps them valid
and prevents refill while the borrow is live. Dispatch on store happens once
per bind and once per batch, never per element.

For independent workers, transfer the buffer owner (`FilledBatch`) or share
read-only storage under the lifetime rules of section 5, then borrow local
views. A small view keeps a much larger allocation live until its last use.
Writable views require exclusive access or proven disjoint regions; a
broadcast view is not a set of independent writable cells.

### Using ndarray

**Owner-agreed:** numerical arrays and borrowed views are `ndarray`'s, with
thin domain types carrying axis meaning, native Measures metadata and flag and
weight meaning. There is no reason to build a competing array library. The
workspace declares `ndarray = "0.17"` and locks 0.17.2;
[`casa-values::ArrayValue`](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-values/src/lib.rs#L336)
already holds typed `ArrayD` payloads.

Slicing a view returns another borrowed view. On the `[2, 2, 4]` sample-major
array below, `values.slice(s![.., 1..2, ..])` selects channel 1 with shape
`[2, 1, 4]` without copying; the view keeps strides `[8, 4, 1]` and is not
contiguous across samples. `as_slice()` requires contiguous standard order;
neither accessor silently packs a noncontiguous view. `ndarray` does not supply
scientific axis meanings, Measures references, resource admission or storage
scheduling; those stay in the domain types. Do not introduce implicit owned
copies, packing or broadcast expansion into a path that promises borrowed
access; charge required copies explicitly. The library's
[ownership and layout documentation](https://docs.rs/ndarray/0.17.2/ndarray/struct.ArrayBase.html)
is part of the review basis.

### Concrete batch example

Illustrative values, not an observed dataset: one time, two ordered
baselines, two channels, four correlations. The sample-major values and
boolean flags each have shape `[2, 2, 4]` and element strides `[8, 4, 1]`;
flattening uses `i = s*8 + c*4 + p`. `Complex32` means `Complex<f32>` (64 bits
total).

```text
time = MJD 60000 TAI + 0 s (split epoch representation)
baselines = [(antenna 0, antenna 0), (antenna 0, antenna 1)]
frequency_hz = [1_000_000_000, 1_001_000_000]  // TOPO
correlations = [XX, XY, YX, YY]
field(time) = A; phase_centre(A) = (1.0, 0.5) rad, ICRS
uvw_m[sample, uvw] = [[0, 0, 0], [100, -40, 20]]
// UVW is referred to that phase centre with the declared baseline convention.

group.values[sample, channel, correlation] = Complex<f32>, Jy
  sample 0, channel 0: [10+0i,  1+2i,       1-2i,       8+0i]
  sample 0, channel 1: [12+0i,  0.5+1i,     0.5-1i,     9+0i]
  sample 1, channel 0: [ 3+4i,  0.2-0.1i,  -0.3+0.4i,   2-1i]
  sample 1, channel 1: [ 4+3i,  0.1+0.2i,   0.5-0.2i,   1+2i]

group.flags[sample, channel, correlation] = bool
  sample 0, channel 0: [false, false, false, false]
  sample 0, channel 1: [false, false, false, false]
  sample 1, channel 0: [false, false, false, false]
  sample 1, channel 1: [ true, false, false, false]

group.weights[sample, channel, correlation] = f32, channel stride 0
  sample 0: [ 4, 2, 2,  4]
  sample 1: [16, 8, 8, 16]
// Inverse-variance weights in Jy^-2: one per correlation (MSv2 WEIGHT),
// seen through every channel by a zero channel stride, without expanding
// the eight stored numbers.
```

Both acquisitions are present and every value is finite. At
`(sample=1, channel=0, correlation=XX)` a consumer reads `3+4i`, flag false,
weight 16, frequency 1 GHz and UVW (100, -40, 20) m. The XX value at channel 1
remains `4+3i` in the buffer but the validity rule excludes it by its flag.
Data are not replaced by zero to express exclusion. An MSv2 `FLAG_ROW` would
have been folded into every flag of its row when the batch was filled.

### Role presence

A request names the roles it reads (values, flags, weights, UVW, time
centroid, interval, exposure, scan). A role left out is not read at all: a
flags-and-weights traversal decodes no visibilities. A role the store does not
hold (an MSv4 dataset without `TIME_CENTROID`, `EFFECTIVE_INTEGRATION_TIME`,
`time.integration_time` or `scan_name`) fails binding, never a later read or a
made-up value. Data-group associations bind semantic roles to stored
variables; the MSv2 adapter names its `DATA` association `base`, MSv4's own
group name.

### Autocorrelations and real-valued data

For the same antenna, feed and receptor, the ideal self-product
`XX = <E_X conjugate(E_X)> = <|E_X|^2>` is real and nonnegative; likewise YY
(or RR/LL). But an antenna's full polarization correlation matrix also includes
`XY = <E_X conjugate(E_Y)>`, which can be complex. Ideally `YX = conjugate(XY)`.
Products of different feeds/signals likewise need not be real merely because
the antenna IDs match. Stored or processed data are preserved as supplied;
opening does not enforce ideal reality or Hermitian symmetry by changing
values.

MSv2 [`DATA` is complex and `FLOAT_DATA` is real](https://github.com/casacore/casacore/blob/a1399b6b99907bd14021920a089403a93c10aba1/ms/MeasurementSets/MSMainEnums.h#L176).
Autocorrelations stored in `DATA` keep its complex element type, including the
real-valued parallel hands. In the pinned MSv4 schema,
[`VISIBILITY` is complex64/complex128](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1020),
whereas [`SPECTRUM` is real](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1001)
with `[time, antenna_name, frequency, polarization]` dimensions, a separate
single-dish representation outside the VisibilityXds profile. Do not infer a
real dtype from `antenna1 == antenna2` or introduce a per-element real/complex
variant: mixed auto and cross batches use one homogeneous complex array and
one typed loop.

### Coordinate dependencies

Metadata has declared axis dependencies and checked broadcast views.
Constants, per-time, per-baseline and per-sample dependence never become
duplicated per-element objects. Preserved:

- Ordered antenna pairs and autocorrelations; the effective UVW frame and its
  stored label. Swapping antennas conjugates the visibility, negates UVW and
  swaps the cross hands, so no layer reorders a baseline in passing. Feeds are
  a partition key, not part of the baseline.
- Time, time centroid, interval and exposure, with the source's time scale and
  split-precision origin. Absent metadata is a binding error, not a default.
- Channel frequencies and signed widths and their frame; correlation labels in
  stored order.
- Data-group-specific field, source and phase-centre context. Phase centre is
  not pointing; pointing is read per partition and time span (section 2),
  and ephemeris is provisional (section 8).

Measures owns the reference-frame mathematics and reusable UVW transforms;
the scientific geometry owner chooses their context and projection. Neither
storage adapter reproduces that engine.

### Flags and weights

**Flags.** A delivered flag is the stored flag with every absent acquisition
folded in: MSv2 `FLAG_ROW` (every flag of the row), the cells the MSv2 adapter
pads, and, in MSv4, a cell whose stored weight is not finite (XRADIO's
padding). There is no separate row-flag view. A correlation subset delivers
either the delivered products' own flags or, per `FlagsFrom::All`, the OR over
every product of the cell. That policy is casa-rs's imaging operator's choice
for Stokes I from `XX` and `YY`, not CASA's general rule: CASA collapses flags
over polarization only on its frequency-interpolation path
(`FTMachine::interpolateFrequencyTogrid`), while `GridFT::put` tests each
hand's own flag. CASA's solver and gridder both exclude `FLAG_ROW` rows
(`SolveDataBuffer`, `GridFT::put`), so folding `FLAG_ROW` matches CASA.

**Validity.** One rule, the same for every store and every role set: a value is
excluded when its delivered flag is set or, among the roles read, its
visibility or weight is not finite. `VisibilityBatch::validity` evaluates it
per element or, with `mask_into`, once over a batch. A flags-only read gives the
mask a read of every role gives.

**Weights.** One broadcast view: per channel when the store holds spectral
weights (MSv4 `WEIGHT`), or one per correlation seen through every channel by
a zero channel stride (MSv2 `WEIGHT`), without expansion. Padding weighs 0 in
MSv2 and NaN in an XRADIO store; both are excluded. No unit weights are
invented. The request chooses what the view holds: the stored weights, or
weights made from the stored sigmas (section 4).

## 4. Access requests and scientific groups

The algorithm's planner builds a request from the operation's requirements,
estimates it, and binds it. The kernel then consumes bound views; it never
issues I/O or describes its access again per value.

```text
planning:  planner -- AccessRequest --> Observation::estimate --> Estimate
binding:   planner -- AccessRequest --> Observation::bind_read --> Traversal (cursors)
execution: algorithm <-- VisibilityBatch / GroupEnd ---- cursor
```

```rust
pub trait Observation: Sync {
    fn catalogue(&self) -> &Arc<Catalogue>;
    fn estimate(&self, request: &AccessRequest) -> Result<Estimate, ObservationError>;
    fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError>;
    fn grid(&self, id: PartitionId, data_group: &str) -> Result<TimeBaselineGrid, ObservationError>;
}

pub struct AccessRequest {
    pub partitions: Vec<PartitionId>,   // chosen by content from the catalogue
    pub data_group: String,             // one group per traversal
    pub roles: Roles,                   // what is read at all
    pub channels: Range<usize>,         // native channel window
    pub times: TimeSelection,           // every grid time, or those in one epoch range
    pub fields: FieldSelection,         // every field, or the times phased to some
    pub weights: WeightRequest,         // the stored weights, or 1/σ² of the sigmas
    pub correlations: CorrelationRequest, // products delivered and whose flags
    pub grouping: Grouping,             // scientific groups, as gaincal keys them
    pub batch_bytes: usize,             // budget per batch, scratch included
}
```

The request is resolved against each partition once, at binding: the data
group, the channel window, the time and field selections, and the
correlation axis,
which every batch of the traversal and every piece packed from one then
shares without copying (all products borrow the catalogue's list).

A **time selection** is every grid time, or those within one half-open range
of epochs, `[start, end)` seconds from an origin on a time scale, either end
possibly infinite. It is stated in the partitions' own origin and scale
(MJD 0 for MSv2, the Unix epoch for MSv4; every partition of a store shares
them), so selecting a grid time compares two offsets from one origin and
rounds nothing; a range in another origin or scale, a NaN end, or a start
after the end is refused at binding, and a range holding no grid time
(between two, or past either end) is an empty traversal. One range serves
every partition of a request, so a calibration solution interval selects the
same epochs in each spectral window. A planner that splits a partition by
time cuts it at grid indices, through `Observation::grid`, which reads no
payload, and states each slice with `TimeRange::of_grid`, which uses the
grid's own values; without a field selection, slices aligned to the
estimate's natural time blocks decode no unit twice. The MSv2 adapter reads the selected rows from its
opening index; the MSv4 adapter reads only the chunks the selection
touches. Updaters address samples by the batch's own times, so a sliced
traversal writes back, passing partitions whose selection holds no time; an
updater bound to a slice refuses a batch of times outside it; an updater
takes its traversal's batches in any order and any of them, the times it
is not given keeping what they hold. Several updaters of one session write
the slices of a split at once, each claiming its times (section 7).

**Field selection.** A request reads every field, or the times phased to
some fields (`FieldSelection::Only`), in its data group. A field is per
time, so the selection takes whole times. With the time range, it resolves
at binding to the partition's selected times as runs of consecutive grid
times (`TimeRuns`, the estimate's `time_runs`). A batch holds whole times of
one run, so its times are consecutive on the grid.

What the adapters decode, as the estimate models it:
- The MSv2 adapter reads only the selected rows of its opening index.
- The MSv4 adapter reads the chunks each batch's times touch. A chunk row
  also holds the gap times beside a run, and a chunk row that holds several
  runs is decoded once for each batch that reads it. Each batch decodes
  every chunk row its times touch: one for a run inside a chunk row, two
  for a run that crosses a chunk-row boundary (#935 would hold a decoded
  chunk row across batches).

Group keys are taken over the selected fields' times, as production
`gaincal` bins only the rows it selects. A group keyed by field is held
whole or not at all. A group that combines fields holds the selected
fields' times alone, with its bins counted from the earliest of them;
production `gaincal` counts from the lowest-numbered field's first time,
which differs when that field is not observed first (#936). So a field
selection cuts no group, and `clipped` stays for time ranges. Time splits and
updaters take a field selection as they take a time range: slices tile the
selected times, and an updater claims and writes its runs.

Intents are not selected on the request. A partition's samples share their
intents (the adapters partition by `OBS_MODE`, as XRADIO's converter does),
so a consumer selects intents by the partitions it requests. Two
differences from CASA's selection follow:
- The catalogue keeps a partition's intents split at commas, sorted and
  deduplicated. CASA matches an intent pattern against the whole
  `OBS_MODE` string, so a pattern that depends on an intent's place in
  that string (`CALIBRATE_BANDPASS*` rather than `*BANDPASS*`) can select
  otherwise when matched intent by intent.
- Selection by `STATE_ID` (CASA's intent selection by number) has no
  counterpart. States with one `OBS_MODE` share a partition, so a
  consumer cannot select one of them.

**Time splits.** Workers that split a request by time plan the slices
together: `TimeSplit::plan` cuts the request at consecutive epoch ranges
that must tile its selected times in every partition (a gap, an overlap,
or a time left outside every slice is refused), and counts, from the store's group
index (`Observation::group_keys`, reading no payload), the scientific
groups the cuts divide and which slices hold part of each. Each slice's
traversal delivers its part of a divided group, positioned and closed
within the slice and marked `clipped`; the split's `SplitCompletion`, shared
by the slices (each through its own `SliceCompletion`), makes such a group
ready once, after every part's input has completed in its slice and the
work begun on any part has finished, however the slices interleave. So a
consumer writes the same code for one traversal and for a split. A
standalone time range is not a split: a group it cuts is delivered as its
part, marked `clipped`, and the rest never arrives. A batch holds whole times; the budget
counts each time's samples, its per-time arrays and the cursor's decode
scratch for it, and binding fails when one time does not fit.

**Weights from sigmas.** The weight role reads the stored weights
(`WeightRequest::Stored`) or the weights CASA's solvers use, 1/σ² of the
stored sigmas (`WeightRequest::InverseSigmaSquared`), 0 where σ is not
positive or is NaN (`VisBufferImpl2::resetWeightsUsingSigma`). An MSv2 reads
`SIGMA_SPECTRUM` when MAIN's `WEIGHT_SPECTRUM` and `SIGMA_SPECTRUM` both hold
content, else `SIGMA` broadcast over channels, as `VisBufferImpl2` does by
casacore's `hasContent` (the first cell defined, no axis empty: a column added
and never filled holds none, and the stored weights are then `WEIGHT`); the
value is made a weight
before padding is spread, so padding stays NaN. An MSv4 holds no sigmas, so
the request names its fallback: refuse a traversal that reads weights
(`SigmaFallback::Refuse`), or read the stored weights
(`SigmaFallback::StoredWeights`). Which rule and which column or variable a
partition's weights come from is recorded in its estimate
(`PartitionEstimate::weights`, `None` when weights are not read), so a
fallback is never silent. Each batch states its rule
(`DataGroupView::weight_rule`), and a weight patch from a batch of
sigma-derived weights is refused: patches computed from 1/σ² would otherwise
write sigmas back as weights. An updater writing weights is also refused at
binding beside a request that makes them (an MSv4's fallback reads stored
weights, so it binds). `casa_observation::inverse_sigma_squared`
states the rule; production `gaincal`, which reads the MeasurementSet
directly until calibration moves onto batches, applies the same rule, and a
law holds the two solves equal.

Opening chose the
store and its options (`casa_ms::observation_io::open`); after that an
algorithm handles only the catalogue, requests, the traversal and batch views,
and imports no store type.

`Observation::estimate` costs a request from the catalogue and the store's
geometry without reading payload, and binding returns the same estimate. Per
partition it gives the grid's times and baselines, bytes per sample and per
time, decode scratch per sample, times per batch and batches, the bytes a full
batch's buffer holds (`buffer_bytes`, exactly what a fill reserves), the store's
natural blocks (Zarr chunk, casacore tile) along time and frequency, the padded
fraction, and the decode units and elements the traversal touches.
`decode_units` are **modelled** touched chunks: the estimate and the cursor's
counters apply the same model to the selections, so they agree by
construction; they are not measured decodes inside the storage library. A
correlation subset is decoded whole and gathered; the estimate counts that as
decoded and the subset as selected, and its scratch is in the budget.

Scientific groups come from declared keys: production `gaincal`'s rules, keyed
by partition, field unless combined, scan unless combined, and the time bin
counted from the first time of the (field, scan) run, over the grid's
field-selected times whatever the time selection, so traversals of
different slices agree on every time's group. A grouping keyed by scan
on a store that names none is refused at binding. Groups can span batches and
storage chunks; the backend executes the requested grouping and does not decide
which fields a solver may combine. A delivered batch lists its fragments: the
sample ranges of each group, whole times, with `First`, `Middle`, `Last` or
`Whole`. Completion is signalled exactly once: `Last` or `Whole` when proven at
delivery, otherwise a later `GroupEnd`. Under a time selection the positions
and the completion describe the part of each group the selection holds, and
a fragment says whether the selection cut its group (`clipped`). The slices
of a time split share one completion that joins a divided group's parts
(section 4, "Time splits"): it counts each group's parts from the split, so
a later slice closing its part never releases the group before an earlier
slice's work on it has begun. Compute windows (`channel_window`,
`sample_window`) carry no fragments, so a closing position is seen once however
many work units share the batch.

`GroupEnd` establishes that no more input will arrive, not that work on the
group is done. `GroupCompletion` joins the two: each unit of work begins with a
move-only `WorkToken` and ends by finishing it; the call that makes a group
ready returns `true` once. A token dropped unfinished (its unit panicked,
failed or was cancelled) abandons its group, and the join reports
`WorkAbandoned` instead of waiting for work that will not come. One completion
serves one traversal, or the slices of one split; since a source completes
each (partition, field, scan) series' bins in order within a traversal, it
keeps only each series' latest closed bin per traversal, and the divided
groups' outstanding parts, so its memory is the series, the cuts and the
groups in flight, not the traversal's length.

A solver with splittable accumulation consumes fragments and keeps only its
sufficient state. An algorithm that needs all samples of a group at once
declares it and fails admission when they cannot fit; the backend never
materializes it secretly. Revisiting is a new traversal: binding again is
cheap, and nothing is cached between traversals. Imaging need not request
calibration grouping.

## 5. Ownership, reuse, and resource accounting

```rust
let mut traversal = observation.bind_read(&request)?;
let mut buffers = BatchBuffers::default();
while let Some(event) = traversal.next(&mut buffers)? {
    match event {
        ReadEvent::Batch(batch) => process(batch.channel_window(window)?),
        ReadEvent::GroupEnd(key) => finish_group(key)?,
    }
} // each borrow ends before refill
```

Buffers are caller or pool owned with reusable capacity. A cursor fills them
through one checked path: `BatchBuffers::begin` checks the batch's header
against the catalogue (partition, data group, channel window, correlation axis)
and reserves exactly the arrays the batch needs, and `BatchFill::finish`
checks every array's length, every identity against the catalogue and the
fragments' tiling, once, outside any element loop. The batch is then built
from the header, the arrays and the catalogue entry alone, so no second
description of the same data can drift. A batch's buffers keep a shared
reference to that catalogue, so an owned batch is self-contained.

### The buffer pool and admission

Owned batches are filled into a `BufferPool` of `n` buffers, each with a byte
**envelope**. A buffer never holds more than its envelope, lent or free,
whatever sequence of batch shapes and roles it serves: a fill whose batch
needs more is refused before anything is allocated; a fill that would keep
more than the envelope from earlier fills frees the oversized arrays first;
and a returned buffer over it is emptied. So the pool's memory is at most
`n × envelope`, and its counting law fills one buffer with values-heavy and
weights-heavy batches in turn and checks that bound, lent and free. For a
traversal, the envelope is its estimate's `peak_buffer_bytes`, which every
piece packed from its batches also fits.

A buffer is taken only for a batch. A take waits for a free buffer, which is
the backpressure a slow consumer applies; a handle made `waiting_at_most` a
duration fails with `PoolExhausted` instead, and a refused `next_owned` has
consumed nothing, so a cancellable runtime asks again. A `FilledBatch` returns
its buffer when dropped, or when its last `Arc` is.

What a traversal holds, and what admission must reserve:

| Holder | Bound | Lifetime |
| --- | --- | --- |
| Pooled batch buffers | `capacity × envelope` | the pool |
| A cursor's decode scratch | `scratch_bytes_per_sample × samples_per_batch` (row numbers, `FLAG_ROW`, time auxiliaries; the stored axis for a correlation subset; MSv4 flag bytes and the weights its flags read) | the cursor |
| The MSv2 opening index | the partition grids: four bytes per cell, plus each grid's times and baselines | the opened observation |
| Work units' own state and scratch | the algorithm's | the work unit |
| `zarrs` codec scratch | not counted yet (section 8) | each decode |

Connecting the pool and these bounds to the runtime's `HostResources`
admission is M2 work (section 8).

### Independent work-unit lifetimes

A **work unit** executes one or more compute windows on one worker or a
cooperating subset of the existing admitted team; it is distinct from a
scientific group such as a calibration solution bin. Ownership follows the
work, without permanent thread affinity or a new pool or scheduler. Batches
move between work units as `FilledBatch` or are shared read-only through an
`Arc`; pieces with storage of their own are packed explicitly into a pool
(`BufferPool::pack`), counted as copied elements. Mutable compute arrays have
one owner or proven disjoint regions; disjoint logical windows alone do not
establish disjoint persistent updates.

Independent work may finish out of delivery order when the request's
semantics permit it. There is no global barrier: joins come from scientific
dependencies, declared ordering or conflicting writes. Reuse waits for actual
last use. Group-input completion stays separate from the completion of the
work consuming it (section 4). A slow window borrowing one large allocation
keeps the **entire allocation** live; realizations can pack independently
owned pieces when prolonged sharing would pin too much memory, and the pool
bounds both. The trial measures the two strategies (one shared batch held back
by a slow unit; packed pieces that let fast units finish) in its imaging
example.

For cube CLEAN, visibility batches belong to gridding and prediction passes.
The model, residual, mask and solver state belong to the imaging owners; a
channel's minor cycle must not retain an input batch because that channel
needs more iterations.

### Accounting and backend realization

Backends fill directly into the batch's arrays where layout permits (both
adapters do for every product of an unpadded grid, and the MSv2 adapter spreads
padded rows in place) and disclose every copy (`IoCounters::copied_elements`).
No universal zero-copy promise is made for compression, type conversion,
sparse gathers or GPU upload. Counters are kept at batch granularity: read
calls, modelled decode units, delivered bytes, copied elements and buffer
growths.

Decoding runs on the calling thread: the MSv4 adapter fixes zarrs'
concurrency target at one rather than its default of the global rayon pool's
size. That is not yet a proof of serial execution (section 8). A larger batch
never increases the number of per-sample adapter operations.

## 6. Source and sink lifecycle

The lifecycle is `open/create → inspect → estimate → bind → read/write →
finish`. `casa_ms::observation_io::open` chooses the backend once (MSv2 or
MSv4) and returns a `Box<dyn Observation>`; each adapter dispatches statically
inside, so the boundary costs one virtual call per bind and per batch.
Separate session types keep invalid transitions unrepresentable: an opened
observation reads, and an `Msv4Writer` creates.

Partitions can be read in parallel: an `Observation` is `Sync` and its cursors
are `Send`, so threads share the observation by reference and each takes
cursors. The trial's probe reads all 64 partitions of a real MeasurementSet
on eight workers and checks every partition's samples and checksum against a
sequential read.

The trial's adapters open stores read-only and hold no locks beyond the
storage libraries' own. An **update session** (`open_update`, section 7) is
the one mixed owner: it takes the process's write intent on the store, so a
second update session is refused, lends the same observation for reading and
updaters for writing, and lets independent read-only observations open
beside it. Several updaters may be live at once, each claiming the times of
each variable and partition it writes (on MSv4 widened to whole chunk rows);
one that meets a live claim is refused. An MSv2 session holds casacore's
write lock on MAIN from its first live updater to its last, so another
process waits for a pass rather than at every batch, and reopens its handle
whenever it takes the lock, so what it merges is never older than the lock;
its updaters write one batch at a time. MSv4 updaters replace disjoint
chunks at once, and MSv4 has no cross-process lock protocol, which the
session states (`UpdateGuarantees`).

## 7. Updates and creation

### Creation from batches

A producer declares a dataset with the common catalogue (antennas, fields,
scans, the partition and its data group) and a `TimeBaselineGrid` (baselines,
times, the field and scan of each time, and the integration interval).
`Observation::grid` gives an opened partition's grid from the store's index,
reading no payload. Chunks, codecs, paths and provenance are backend storage
options (`Msv4StorageOptions`), not part of the declaration.

The `Msv4Writer` keeps the declaration and checks every batch against it
before writing anything: partition, frequencies (bit for bit) and frame,
correlation products, units, UVW frame and time scale; then each sample's
time, baseline, field and scan against the grid, and its interval where it has
an acquisition (padding has none). It refuses what it cannot represent: a TAI
grid (it writes `unix`/`utc` only), a true row flag (fold row flags into the
element flags first), a channel window. A padded cell is written as padding:
flagged, NaN values, weights, UVW and time auxiliaries. Every coordinate and
sub-dataset is written into a staging directory of the writer's own; the
dataset is published by a rename that refuses to replace an existing one, and
a dropped or failed writer removes its staging.

Batches in time order encode every chunk once, whatever their size. The
writer stores the chunks a batch covers whole straight from its views, and
holds (copies) the batch's times in a chunk it covers only partly until the
chunk's other times arrive, then stores that chunk whole. It holds at most one
chunk of times of every variable (`buffer_bytes`), which a producer budgets
beside its batches; batches of a multiple of `chunk_times` times, from a chunk
boundary, make it hold nothing. Held times the next batch does not continue
(batches out of time order) are stored as they are, and zarrs decodes, merges
and re-encodes that chunk when its other times come (`partial_chunks`).

An MSv2 creator from batches is built narrowly, inside the trial's converter
(trial README, "Converter"): subtables from the common catalogue, then MAIN's
rows through production's variable-shape creation writers, in time order per
partition. As the declaration was designed to serve one (revision 2 design,
D23), cells in time order become rows, and a padded cell a row flagged
throughout with NaN data and weights; its interval, exposure (0) and time
centroid stay valid so the MeasurementSet reopens. A conversion that must
reproduce an original sparse row set declares it, and padding then becomes
no row: since a batch carries no presence mask and an MSv2's row count is
fixed before its rows are written, that policy first reads the weights to
count the acquisitions. `SIGMA` is derived from the weights, a loss the
caller accepts. Publication semantics beyond staging and rename, and
loss-policy coverage beyond the converter's inventory, are provisional
(section 8).

### Updates (provisional)

The write-back trial (trial README, "Write-back") realizes a narrow part of
this subsection: update sessions with declared targets (data group, role,
alias scope, creation), one updater per traversal (several per session,
claiming disjoint times, for the slices of a split) writing visibilities,
flag set/clear changes and weights for the batches the session delivered,
the broadcast-weight rule below (Pro R1), data-group aliases accepted
explicitly (a group the session creates included), one binding per
variable, writes against a delivered batch or its owned address
(`BatchAddress`, which outlives the batch's buffers; a flag cleared or a
weight written needs the batch, whose weights mark MSv4 padding), a
visibility patch's mask of the elements it writes (a sample it leaves
wholly is neither read nor written), batches in any order and any of them,
row flags and padding preserved (a padded MSv2 cell has no row;
an MSv4 updater refuses to clear a flag or write a finite weight where the
store holds a NaN weight), whole-unit writes that read nothing, MSv4 chunks
replaced atomically through a temporary object with the changed directories
synchronized at finish, and new `CORRECTED_DATA` and `MODEL_DATA`
initialized as CASA initializes them (from `DATA`; 1, and 0 in the cross
hands). What CASA clears is cleared first (`casa_ms::scratch_columns`,
#912), so that CASA reads the column written: the cached sort keywords
before a column is added, and every field's virtual model before any
`MODEL_DATA` write, created or not.
A session over an MSv2 refuses, when it opens and again when its first
updater takes MAIN's lock, before anything is written, a target its
updaters cannot write in place within `UpdateSession::write_bytes`: a
column of any role outside `TiledShapeStMan` and `TiledColumnStMan`, whose
save rewrites every row of its storage group, or a column it creates whose
clone of `DATA`'s manager would be one or which `DATA` shares
(`casa_ms::scratch_columns::unbounded_destination`, the rule the
selected-row visibility writer applies too).
A batch's own times and baselines address its samples in
both stores, so no update map is added to the read side. The rest of this
subsection, and its acceptance obligations, stand for M1/M2.

An `UpdateMapRef` refers to a session-owned inverse map from selected logical
elements to a named writable role's native address domain (cells, scalars or
chunk coordinates). That domain need not equal the visibility array's domain.
A permutation or selection composes that mapping. A source may use implicit
rectangular strides or compact row maps; it need not allocate an address for
each visibility. M1 must prove borrowed-map and bounded queued-write lifetimes
without retaining read buffers until the whole pass finishes.

`RoleUpdateMaps` is a scoped accessor to that provenance, not one visibility
map reused for every group and role. It yields role-specific maps for
visibility, row weights, spectral weights, flags and other writable roles with
their own native domains. A patch's role must match the role bound into its
map; shared underlying destinations are still detected as aliases/conflicts.

```rust
// Proposed shape, not realized.
enum PatchValues<'a> {
    ReplaceVisibility(ArrayView3<'a, Complex32>),
    ReplaceWeights(ArrayView3<'a, f32>),
    ChangeFlagBits(FlagBitChanges<'a>),
    SetRowFlags(RowFlagChanges<'a>),
}

struct ObservationPatch<'a> {
    destination: UpdateMapRef<'a>,
    role: WritableRole,
    scope: MutationScope, // includes group and broadcast aliases in native domain
    changed_region: LogicalRegion<'a>,
    values: PatchValues<'a>,
}
```

Patches identify actual changed regions and roles, not a requirement to
rewrite all fields returned by a read. Bit operations preserve unselected and
unknown bits. Changing an effective boolean flag must not silently clear
`FLAG_ROW` or discard `FLAG_CATEGORY`; setting or clearing row flags is a
separate operation. Patches outside selected extents, with incompatible shape
or precision, from another session, or with ambiguous destinations fail
validation before submission.

Resolve conflicts and the full affected logical extent **after** mapping to
the native role and expanding both data-group aliases and broadcast aliases.
For example, one v2 `WEIGHT` scalar of 2 supplies channels 0 and 1. A request
to set only channel 0's effective weight to 8 cannot silently modify that
scalar and therefore channel 1: require an explicit whole-scalar scope, a
supported plan that creates a channel-resolved destination under bounded
memory, or rejection. Calibration owns any reduction from channel weights into
row summary `WEIGHT` and submits both native-role patches; storage never
invents that inverse. Overlapping queued writes require an explicitly
supported composition/order rule or rejection; there is no default
last-writer-wins. Averaging, resampling and reductions lose their source
update map; their results go to an explicit output.

Groups may share flags, weights, UVW, or field/source variables. A mutation
either explicitly affects that shared variable and its aliases or first creates
an independent variable and rebinds the selected group under a supported
structural-write plan. Neither unintended cross-group mutation nor hidden
whole-variable copying is allowed.

The sink coalesces bounded patches for each physical update unit (for Zarr
possibly a shard). One admitted owner serializes each unit; within-process
exclusion does not promise safety against another process. Selective writes
preserve other chunks and cells, codecs, tiling, unrelated columns, extension
metadata and keywords. When the sink has the complete replacement for every
preserved byte of an independently writable unit, it reads zero old payload
bytes; partial writes coalesce within the admitted update window, reading a
covering unit at most once per flush, without an unbounded dirty-chunk cache.

A **structural preflight** precedes mutation: required roles and managers,
representability, alias scope, locks, buffer budget and output metadata
support, without scanning or hashing payload. In-place failure may leave
partial completion across units; v2 keeps its CASA-compatible failure
semantics.

**Proposed local v4 guarantee:** replace each encoded object (chunk or shard,
and separately each metadata object) through bounded temporary storage and an
atomic same-filesystem replacement, rather than truncating the live object. On
a supported filesystem, I/O failure or process termination during replacement
leaves the previous published state (including an absent key representing
fill) or the new complete object at its path. Reject selected mutation where
that capability cannot be established. M1 injects short writes and ENOSPC and
terminates during replacement, then independently reopens and checks selected
and unselected sentinels, including failed first materialization of an absent
chunk. Power-loss durability additionally depends on synchronization and
filesystem behavior; declare and test the chosen finish boundary. There is no
whole-store transaction or rollback, and dropping an unfinished session is not
success.

## 8. Provisional capabilities

Designed, not realized or not proven by the trial. Each joins the contract only
when its acceptance obligation is met.

| Capability | Status in the trial | Acceptance obligation |
| --- | --- | --- |
| Requested units and conversions at binding | Canonical units only; others refused | A requested unit holds for the view's bound scope; conversions fused with fill, identity conversions copy nothing, weights transform as `w / \|a\|²` under a declared amplitude scale `a`; unsupported conversions are typed errors |
| Requested precision | `Complex<f32>`, `bool`, `f32`, `f64` only; complex128 refused | Native precision, lossless widening or explicitly approved narrowing per role, charged and fused with decode; the writer checks representability; the existing science tolerances hold |
| Real-valued data (`FLOAT_DATA`, `SPECTRUM`) | Refused | Its own typed variant under its profile, not reinterpreted as frequency visibilities |
| More than one data group per traversal; role aliases | One group per traversal; each group keeps its own field and UVW context | Each group keeps its UVW and field/source context and explicit alias relationships to the others |
| Occupancy and conversion to rows | Absence only as flagged padding with a NaN weight in both stores; the batch carries no presence mask, so an update infers padding (a missing MSv2 row, a non-finite MSv4 weight, which a traversal must read weights to see) and refuses to clear its flags or give it a finite weight. The converter writes padding as D23's flagged NaN rows, or, declared, as no row (a sample whose every weight is non-finite is padding), counting the acquisitions by a first pass over the weights | Unknown occupancy representable; conversion to a row format resolves it by declared policy or reports the ambiguity, never recovering an imaginary MSv2 row; writing new measurements into holes needs an explicit fill policy |
| Native flags: bitmasks, reason bits, categories, row flags as their own view | `bool` only; `uint8` refused; row flags folded on read; a flag change sets and clears element flags and never writes `FLAG_ROW` or `FLAG_CATEGORY` | Native dtype and reason bits preserved for lossless writes; `UNSPECIFIED_BIT` for generic flagging, bound to its actual bit; clearing an effective flag never clears unknown reasons |
| Weight roles and fallback; `SIGMA` | The request reads stored weights or 1/σ² of the sigmas by CASA's column rule; an MSv4, holding no sigmas, refuses or falls back to its stored weights as the request names; the estimate records the rule and column per partition; a weight write refuses sigma-derived weights (section 4). A batch-fed gain solve weighted from the sigmas equals production `gaincal` where `WEIGHT` is not 1/σ² (trial README, example B) | A fallback per cell where a sigma cell is undefined, recorded per cell; today an undefined cell is a read error, as for every role |
| Revisit and compute-window requirements as request fields | A new traversal per pass; windows are views | Binding states what depends on an estimated pass count; a revisit does not imply cached values |
| Updates | The write-back trial's update sessions (section 7) | Section 7's remaining obligations: bit flags and flag categories, conflicting queued patches, structural writes (a new MSv4 data group), per-batch MSv2 planning, a killed process, stray temporary files and the synchronization cost at scale |
| MSv2 creation from batches; general creation and loss policy | The MSv4 writer (one dataset or a processing set of many), and the converter's MSv2 creator: padded cells flagged NaN rows, or no row by declared policy (the occupancy row above), losses inventoried and refused unless accepted before anything is written, `SIGMA` derived from the weights as an accepted loss | Cells in time order become rows, padded cells flagged NaN rows; cross-format creation preflights metadata, flag and occupancy mappings and rejects unapproved loss |
| Mixed read/update sessions and locking | One update session per store in a process, its updaters claiming disjoint times (whole chunk rows on MSv4), read-only opens beside them; casacore's MSv2 lock held while an updater is live | Plan read and write access before locking, no implicit upgrade; original-input semantics need disjoint read and write regions after resolving aliases, or a proven read-before-write order; casacore cross-process locks stay authoritative for v2; the v4 profile reports that it has no cross-process lock protocol |
| Calibration binding across stores | Section 2's persisted namespace and calibration binding, in the calibration trial (test code until the calibration port): a batch solve persisted through an MSv2's namespace is production `gaincal`'s table; one solved on an MSv4 lacking an antenna, rebound to the full MSv2 that numbers its antennas otherwise and applied from batches, equals production `gaincal` and `applycal`, and CASA's `applycal` reads the MSv4-namespace table as casa-rs does; reordered antennas and windows, renamed entities, subsets, equal content refused unless the namespace is the table's own or the association is stated (trial README, "Caltable round trip") | The binding in production, with `gainfield` and nearest-field selection through bound fields; bandpass and other per-channel tables, whose windows keep their channels; tables solved over combined windows; `FEED` and receptor association; a field binding tolerant of ephemeris fields |
| Ephemeris and time-dependent metadata | Ephemeris keys MSv2 partitions as XRADIO's converter does but is not read, and the converter refuses fields that follow one (#942). Pointing is read (section 2) and converted both ways, what is not carried found before anything is written: into an MSv4 each row's direction at its own time (further polynomial terms, `INTERVAL`, `TARGET`, `NAME`, `TRACKING`, repeated rows and rows beyond the partitions' times reported as losses, pointing in a frame MSv4 does not name lost whole); into an MSv2 every row of the partitions' antennas, an MSv4's samples as rows of `INTERVAL` 0 | Ephemerides queried in bounded windows on both stores and carried by the converter; whether pointing samples without intervals image as the MSv2 they came from (imaging port step 3b) |
| Baseline-dependent polarization (`polarization_mixed`) | Refused at opening | A checked per-baseline basis mapping in the common metadata |
| Centroid extra precision (`TIME_CENTROID_EXTRA_PRECISION`) | Refused when centroids are requested | The residual carried through the epoch view, slicing, packing and writing; adding it to a large f64 is not a substitute |
| Four-dimensional `EFFECTIVE_INTEGRATION_TIME` | Refused | Per-channel exposure in the batch's timing |
| Resource admission | Pool envelope, estimate and writer bounds; not connected. `zarrs_filesystem` pinned; its per-key lock registry grows with distinct keys visited and plateaued over 30 passes of `pilot.ms` (trial README, converter finding 3) | Pool allocations, cursor scratch, the opening index, packed pieces, the writer's held chunk and queues admitted through `HostResources` (`Reservation`/`Admitted<T>`), with cancellation-compatible backpressure; zarrs codec scratch discovered or conservatively bounded before work, failing with the minimum requirement; caches bounded by bytes and plateauing over increasing prefixes of present and missing chunk keys (`zarrs_filesystem` and codecs audited, not just `zarrs`) |
| Decode on the admitted executor | zarrs' concurrency target and its chunk concurrency minimum (default 4, which overrode the target) set to one, so codecs run on the calling thread; rayon's global pool is still started, idle (trial README, converter finding 2) | Executing threads and simultaneously live codec buffers counted on the real decode path; no fall-through to a global pool |
| Matched performance | Gross-cost checks on real data, a 120-sample like-for-like | Production-versus-batch timings on representative workloads, including sparse grids and restricted channel waves, before any no-loss claim |

## 9. Worked counterexamples for contract review

1. **Sparse v2, dense v4:** two integrations, three baselines, one absent
   acquisition, one real NaN measurement and one duplicate acquisition. The
   MSv2 adapter pads the absent cell and refuses the duplicate; numerical
   validity is not proof of original occupancy, and an arbitrary external v4
   store cannot recover the original row set. Conversion to rows requires a
   declared policy or rejection.
2. **Mosaic plus two SPWs:** the field varies over time; the SPWs have
   different channel counts and frames. Batches stay homogeneous while imaging
   visits both partitions and a solve groups their fragments. Do not pad them
   into one invented frequency axis or equate a field with antenna pointing.
   Compare compatible fields combined along time with the same data
   partitioned by field, keeping repeated visits and sample-to-field
   associations.
3. **Calibrate then write:** gather channels in requested order, solve from
   fragments, apply gains, patch corrected data and selected flag bits.
   Preserve untouched channels, native weights and flag categories, group
   aliases and actual storage bindings. Reject writing averaged solve inputs
   back as if invertible.
4. **Large v4 batch:** decode several chunks into one batch; borrow many
   smaller compute windows. Peak memory counts decoded and packed storage
   where they overlap. Show fewer I/O and decode calls or reduced elapsed time
   without inflating kernel scratch.
5. **Failure:** cancel between two chunk or tile flushes. Release ownership,
   report partial completion and do not claim rollback or group completion.
   Abandoned work fails the join rather than waiting. Fail inside v4 object
   replacement and prove the old-or-new object rule, including unselected
   regions. New staged output is never published as complete.

These become executable contract laws as each capability lands. They are not
substitutes for the representative science, performance and independent
interoperability gates.
