// SPDX-License-Identifier: LGPL-3.0-or-later
//! The catalogue's descriptive metadata (#937) as each trial store states
//! it: the MSv2's every `SPECTRAL_WINDOW`, `STATE` and `PROCESSOR` row and
//! `OBSERVATION`'s extent, flags and release date; the MSv4 XRADIO wrote
//! from it, which states a window's reference frequency, a processor's type
//! and subtype and a release date, and leaves the rest unstated. The two
//! still bind.

use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{BindingTolerance, PartitionId, ProcessorId, StateId, WindowId};
use casa_types::measures::frequency::FrequencyRef;

use super::fixture;

#[test]
fn each_store_states_its_windows_states_processors_and_observations() {
    let root = tempfile::tempdir().expect("root");
    let msv2 = open(fixture(root.path(), "ms_trial.ms"), &OpenOptions::default()).expect("msv2");
    let msv4 = open(
        fixture(root.path(), "ms_trial.ps.zarr"),
        &OpenOptions::default(),
    )
    .expect("msv4");
    let (ours, theirs) = (msv2.catalogue(), msv4.catalogue());

    // The MSv2's one SPECTRAL_WINDOW row, every member stated.
    assert_eq!(ours.windows.len(), 1);
    let window = ours.window(ours.partitions[0].window);
    let reference = window.reference_frequency.as_ref().expect("REF_FREQUENCY");
    assert_eq!(
        (reference.hz(), reference.refer()),
        (44.512e9, FrequencyRef::LSRK)
    );
    assert_eq!(window.total_bandwidth_hz, Some(1.024e9));
    assert_eq!((window.if_chain, window.net_sideband), (Some(0), Some(1)));
    assert_eq!(window.frequency_group_name.as_deref(), Some(""));
    assert_eq!(
        window.effective_bandwidths_hz.as_ref().map(Vec::len),
        Some(8)
    );
    assert_eq!(window.flagged, Some(false));
    // Its one STATE row, which every partition's rows use; an empty
    // PROCESSOR whose MAIN rows name PROCESSOR_ID 0 gives no processor.
    assert_eq!(ours.states.len(), 1);
    let state = &ours.states[0];
    assert_eq!(state.obs_mode, "TARGET.ON_SOURCE");
    assert_eq!(
        (
            state.cal_k,
            state.load_k,
            state.signal,
            state.reference,
            state.sub_scan
        ),
        (Some(0.0), Some(0.0), Some(true), Some(false), Some(0))
    );
    assert!(ours.processors.is_empty());
    for partition in &ours.partitions {
        assert_eq!(partition.window, WindowId(0));
        assert_eq!(partition.states, [StateId(0)]);
        assert_eq!(partition.processor, None);
    }
    let observation = &ours.observations[0];
    let range = observation.time_range.as_ref().expect("TIME_RANGE");
    assert!(range[0].value().as_mjd() <= range[1].value().as_mjd());
    assert_eq!(
        observation
            .release_date
            .as_ref()
            .map(|date| date.value().as_mjd()),
        Some(0.0)
    );
    assert_eq!(observation.flagged, Some(false));
    // Without a SOURCE table there are no sources.
    assert!(ours.sources.is_empty());
    assert!(ours.fields.iter().all(|field| field.source.is_none()));

    // XRADIO's conversion: the reference frequency, a processor of empty
    // type and subtype, the release date; no states, no extent, no flags.
    assert_eq!(theirs.windows.len(), 1);
    let window = theirs.window(theirs.partitions[0].window);
    let reference = window
        .reference_frequency
        .as_ref()
        .expect("reference_frequency");
    assert_eq!(
        (reference.hz(), reference.refer()),
        (44.512e9, FrequencyRef::LSRK)
    );
    assert_eq!(
        (window.total_bandwidth_hz, window.if_chain, window.flagged),
        (None, None, None)
    );
    assert_eq!(theirs.processors.len(), 1);
    assert_eq!(
        (
            theirs.processors[0].kind.as_deref(),
            theirs.processors[0].sub_type.as_deref()
        ),
        (Some(""), Some(""))
    );
    assert_eq!(theirs.partitions[0].processor, Some(ProcessorId(0)));
    assert!(theirs.states.is_empty() && theirs.partitions[0].states.is_empty());
    let observation = &theirs.observations[0];
    assert!(
        observation.time_range.is_none(),
        "never derived from the samples"
    );
    assert_eq!(
        observation
            .release_date
            .as_ref()
            .map(|date| date.value().as_mjd()),
        Some(0.0)
    );

    // What one store leaves unstated does not keep them from binding.
    let binding = ours
        .bind_to(theirs, &BindingTolerance::EXACT)
        .expect("the MSv2 binds to its conversion");
    assert_eq!(binding.partitions, [PartitionId(0)]);
    // Intent selection reads the MSv2's one unflagged state.
    assert_eq!(
        ours.partitions_with_intent("TARGET.ON_SOURCE")
            .expect("selection"),
        [PartitionId(0)]
    );
}
