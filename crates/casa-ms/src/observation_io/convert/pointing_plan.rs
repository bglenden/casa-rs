// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a conversion carries of the source's pointing, decided with the
//! losses that implies before anything is written ([`plan`]), as the reads
//! through the boundary ([`Observation::pointing`]) that carry it:
//! - into an MSv4, one per partition over its own times, as XRADIO's
//!   converter takes the `POINTING` rows within each dataset's times (the
//!   boundary adds each antenna's nearest sample either side);
//! - into an MSv2, reads that gather every sample once: an MSv2 source's
//!   rows of every named antenna, its antennas grouped so that each read
//!   fits the budget, or an MSv4 source's datasets whole.
//!
//! An MSv2 source's `POINTING` is scanned once ([`pointing_scan`]) for what
//! those reads cannot carry: rows nothing reads, rows a partition reads at
//! the antenna and time of another (an MSv4 holds one sample there),
//! polynomial terms beyond the direction (an MSv4 holds one term), and the
//! columns neither destination holds. Pointing a read would refuse or the
//! destination cannot hold at all (directions in several frames, a frame
//! MSv4 has no name for, rows beyond the reader's bound) is lost whole, as
//! a reported loss, rather than refused part way through writing. Each read
//! is checked against the budget with the catalog's own bound, and an MSv2
//! destination's gathered rows with the writer's; beyond it the conversion
//! is refused.

use std::path::Path;

use casa_observation::{
    AntennaId, Observation, ObservationError, PartitionId, PointingDirection, PointingRequest,
};
use casa_tables::Table;

use super::Loss;
use super::losses::Destination;
use super::msv2_pointing::gathered_bytes;
use super::pointing_scan::{self, Partition, Scan};
use crate::observation_io::msv2::{POLYNOMIAL_TERMS, scan_rows};
use crate::observation_io::msv4::POINTING_FRAMES;
use crate::observation_io::update::{request_error, storage};
use crate::schema::SubtableId;
use crate::selected_pointing::catalog_peak_bytes;

/// What a conversion carries of the source's pointing: the reads that
/// carry it, each in the time system of its partition.
#[derive(Debug, Default)]
pub(super) struct PointingPlan {
    reads: Vec<PointingRequest>,
}

impl PointingPlan {
    /// An MSv4 destination's read for `partition`'s dataset, if its
    /// pointing is carried.
    pub(super) fn read_for(&self, partition: PartitionId) -> Option<&PointingRequest> {
        self.reads.iter().find(|read| read.partition == partition)
    }

    /// An MSv2 destination's reads, which gather every carried sample.
    pub(super) fn reads(&self) -> &[PointingRequest] {
        &self.reads
    }
}

/// What converting `observation` (at `source`) into `destination` carries
/// of its pointing, each read within `budget` bytes, and the losses that
/// implies.
///
/// # Errors
///
/// [`ObservationError::InvalidRequest`] when a read, or an MSv2
/// destination's gathered rows, would hold more than `budget`; any error
/// reading the source.
pub(super) fn plan(
    source: &Path,
    observation: &dyn Observation,
    destination: Destination,
    budget: usize,
) -> Result<(PointingPlan, Vec<Loss>), ObservationError> {
    let partitions = partitions(observation)?;
    if source.join("zarr.json").is_file() {
        return msv4(observation, &partitions, destination, budget);
    }
    let ms = crate::MeasurementSet::open_retained_read(source).map_err(storage)?;
    let Some(table) = ms.subtable(SubtableId::Pointing) else {
        return Ok((PointingPlan::default(), Vec::new()));
    };
    if table.row_count() == 0 {
        return Ok((PointingPlan::default(), Vec::new()));
    }
    msv2(table, &partitions, destination, budget)
}

/// Each partition's antennas (those its baselines name) and times.
fn partitions(observation: &dyn Observation) -> Result<Vec<Partition>, ObservationError> {
    let catalogue = observation.catalogue();
    catalogue
        .partition_ids()
        .into_iter()
        .map(|id| {
            let Some(group) = catalogue.partition(id).data_groups.first() else {
                return Ok(Partition {
                    antennas: Vec::new(),
                    times: None,
                });
            };
            let grid = observation.grid(id, &group.name)?;
            let mut antennas: Vec<AntennaId> = grid
                .baselines
                .iter()
                .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
                .collect();
            antennas.sort_unstable();
            antennas.dedup();
            let times = grid
                .times_s
                .first()
                .zip(grid.times_s.last())
                .map(|(first, last)| *first..=*last);
            Ok(Partition { antennas, times })
        })
        .collect()
}

/// The plan that carries nothing, losing the pointing whole for `reason`.
fn lost_whole(table: &str, reason: &str) -> (PointingPlan, Vec<Loss>) {
    let loss = Loss::Table(format!("{table} ({reason})"));
    (PointingPlan::default(), vec![loss])
}

/// Refuse gathering `samples` samples of `terms` terms into an MSv2 beyond
/// `budget`.
fn check_gathered(samples: usize, terms: usize, budget: usize) -> Result<(), ObservationError> {
    let bytes = samples.saturating_mul(gathered_bytes(terms));
    if bytes > budget {
        return Err(request_error(format!(
            "the pointing gathered for the MSv2 holds about {bytes} bytes, beyond the batch \
             budget of {budget}"
        )));
    }
    Ok(())
}

/// An MSv4 source: each dataset's pointing whole, read once here to find
/// what the destination cannot hold.
fn msv4(
    observation: &dyn Observation,
    partitions: &[Partition],
    destination: Destination,
    budget: usize,
) -> Result<(PointingPlan, Vec<Loss>), ObservationError> {
    let mut reads = Vec::new();
    let mut frames = Vec::new();
    let mut samples = 0_usize;
    for (index, partition) in partitions.iter().enumerate() {
        let request = PointingRequest {
            partition: PartitionId(index as u32),
            direction: PointingDirection::Beam,
            antennas: partition.antennas.clone(),
            times: f64::MIN..=f64::MAX,
            budget_bytes: budget,
        };
        let series = match observation.pointing(&request) {
            Ok(series) => series,
            Err(ObservationError::Unsupported { what }) => {
                return Ok(lost_whole("pointing_xds", what));
            }
            Err(error) => return Err(error),
        };
        if series.antennas.is_empty() {
            continue;
        }
        if !frames.contains(&series.frame) {
            frames.push(series.frame);
        }
        samples += series
            .antennas
            .values()
            .map(|own| own.times_s().len())
            .sum::<usize>();
        reads.push(request);
    }
    // An MSv4 holds a frame per dataset, an MSv2 one for its `POINTING`.
    if destination == Destination::Msv2 {
        if frames.len() > 1 {
            return Ok(lost_whole("pointing_xds", "directions in several frames"));
        }
        check_gathered(samples, 1, budget)?;
    }
    Ok((PointingPlan { reads }, Vec::new()))
}

/// An MSv2 source: scan `POINTING` once, then plan its reads.
fn msv2(
    table: &Table,
    partitions: &[Partition],
    destination: Destination,
    budget: usize,
) -> Result<(PointingPlan, Vec<Loss>), ObservationError> {
    let scan = pointing_scan::scan(table, partitions, destination)?;
    if let Some(reason) = scan.refused {
        return Ok(lost_whole("POINTING", reason));
    }
    if scan.frames.iter().any(|frames| frames.len() > 1) {
        return Ok(lost_whole("POINTING", "directions in several frames"));
    }
    let mut frames: Vec<_> = scan.frames.iter().flatten().copied().collect();
    frames.sort_by_key(|frame| frame.as_str());
    frames.dedup();
    if destination == Destination::Msv2 && frames.len() > 1 {
        return Ok(lost_whole("POINTING", "directions in several frames"));
    }
    if let Some(frame) = frames
        .iter()
        .find(|frame| !POINTING_FRAMES.iter().any(|(_, known)| known == *frame))
        .filter(|_| destination == Destination::Msv4)
    {
        let reason = format!("directions in {}, which MSv4 does not name", frame.as_str());
        return Ok(lost_whole("POINTING", &reason));
    }
    let losses = losses(table, &scan, destination);
    let reads = match destination {
        Destination::Msv4 => partition_reads(&scan, partitions, budget)?,
        Destination::Msv2 => {
            check_gathered(scan.antenna_rows.values().sum(), scan.most_terms, budget)?;
            antenna_reads(&scan, partitions, budget)?
        }
    };
    Ok((PointingPlan { reads }, losses))
}

/// What the reads of an MSv2's `POINTING` do not carry into `destination`.
fn losses(table: &Table, scan: &Scan, destination: Destination) -> Vec<Loss> {
    let mut losses = lost_columns(table, destination);
    if destination == Destination::Msv4 && scan.most_terms > 1 {
        losses.push(Loss::Table(
            "POINTING polynomial terms (each row's direction at its own time is kept)".to_string(),
        ));
    }
    if destination == Destination::Msv4 && scan.repeated > 0 {
        losses.push(Loss::Table(format!(
            "POINTING rows at the antenna and time of an earlier one ({})",
            scan.repeated
        )));
    }
    if scan.outside > 0 {
        losses.push(Loss::Table(match destination {
            Destination::Msv4 => format!(
                "POINTING rows beyond every partition's times and each antenna's nearest either \
                 side ({})",
                scan.outside
            ),
            Destination::Msv2 => format!(
                "POINTING rows of antennas no partition names ({})",
                scan.outside
            ),
        }));
    }
    losses
}

/// An MSv4 destination's reads: each partition's antennas over its times,
/// refused when the catalog the read prepares would exceed `budget`.
fn partition_reads(
    scan: &Scan,
    partitions: &[Partition],
    budget: usize,
) -> Result<Vec<PointingRequest>, ObservationError> {
    let mut reads = Vec::new();
    for (index, partition) in partitions.iter().enumerate() {
        let Some(times) = partition.times.clone() else {
            continue;
        };
        if partition.antennas.is_empty() {
            continue;
        }
        let bytes = catalog_peak_bytes(
            scan.within[index],
            partition.antennas.len(),
            scan_rows(budget),
            POLYNOMIAL_TERMS,
        )
        .unwrap_or(usize::MAX);
        if bytes > budget {
            return Err(request_error(format!(
                "reading the pointing of partition {index} needs up to {bytes} bytes, beyond the \
                 batch budget of {budget}"
            )));
        }
        reads.push(PointingRequest {
            partition: PartitionId(index as u32),
            direction: PointingDirection::Beam,
            antennas: partition.antennas.clone(),
            times,
            budget_bytes: budget,
        });
    }
    Ok(reads)
}

/// An MSv2 destination's reads from an MSv2 source: each named antenna's
/// rows once, over every row's time, the antennas in ascending groups each
/// read within `budget`. An MSv2's partitions share its `POINTING` and its
/// time system, so a group's read is in any partition naming an antenna.
fn antenna_reads(
    scan: &Scan,
    partitions: &[Partition],
    budget: usize,
) -> Result<Vec<PointingRequest>, ObservationError> {
    let Some((first, last)) = scan.times else {
        return Ok(Vec::new());
    };
    let bytes = |rows: usize, antennas: usize| {
        catalog_peak_bytes(rows, antennas, scan_rows(budget), POLYNOMIAL_TERMS)
            .unwrap_or(usize::MAX)
    };
    let mut reads: Vec<PointingRequest> = Vec::new();
    let mut held = 0;
    for (antenna, rows) in &scan.antenna_rows {
        let grows = reads
            .last()
            .is_some_and(|read| bytes(held + rows, read.antennas.len() + 1) <= budget);
        if grows {
            held += rows;
            reads.last_mut().expect("a read").antennas.push(*antenna);
            continue;
        }
        if bytes(*rows, 1) > budget {
            return Err(request_error(format!(
                "reading the pointing of {antenna:?} needs up to {} bytes, beyond the batch \
                 budget of {budget}",
                bytes(*rows, 1)
            )));
        }
        let partition = partitions
            .iter()
            .position(|partition| partition.antennas.binary_search(antenna).is_ok())
            .expect("a scanned antenna is named");
        held = *rows;
        reads.push(PointingRequest {
            partition: PartitionId(partition as u32),
            direction: PointingDirection::Beam,
            antennas: vec![*antenna],
            times: first..=last,
            budget_bytes: budget,
        });
    }
    Ok(reads)
}

/// `POINTING`'s columns the destination does not hold, as one table loss:
/// both hold each row's antenna, time and direction (its polynomial in
/// time and the origin of it), and an MSv2 its interval; MSv4 states no
/// interval and no target.
fn lost_columns(table: &Table, destination: Destination) -> Vec<Loss> {
    let mut carried = vec!["ANTENNA_ID", "TIME", "DIRECTION", "NUM_POLY", "TIME_ORIGIN"];
    if destination == Destination::Msv2 {
        carried.push("INTERVAL");
    }
    let lost: Vec<String> = table
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .filter(|name| !carried.contains(&name.as_str()))
                .collect()
        })
        .unwrap_or_default();
    if lost.is_empty() {
        Vec::new()
    } else {
        vec![Loss::Table(format!("POINTING {}", lost.join(", ")))]
    }
}

#[cfg(test)]
mod tests {
    use casa_observation::AntennaId;

    use super::{Partition, Scan, antenna_reads};
    use crate::observation_io::msv2::{POLYNOMIAL_TERMS, scan_rows};
    use crate::selected_pointing::catalog_peak_bytes;

    /// An MSv2 destination reads each antenna once, its antennas grouped in
    /// ascending order so that each read's catalog fits the budget, the
    /// group's read in a partition naming its first antenna; an antenna
    /// alone beyond the budget refuses the conversion.
    #[test]
    fn antenna_reads_group_antennas_within_the_budget() {
        let partitions = [
            Partition {
                antennas: vec![AntennaId(0), AntennaId(1)],
                times: Some(0.0..=10.0),
            },
            Partition {
                antennas: vec![AntennaId(1), AntennaId(2)],
                times: Some(0.0..=10.0),
            },
        ];
        let mut scan = Scan::default();
        scan.times = Some((-5.0, 15.0));
        for antenna in 0..3 {
            scan.antenna_rows.insert(AntennaId(antenna), 100);
        }
        let fits = |rows: usize, antennas: usize, budget: usize| {
            catalog_peak_bytes(rows, antennas, scan_rows(budget), POLYNOMIAL_TERMS)
                .is_some_and(|bytes| bytes <= budget)
        };
        let budget = 1 << 20;
        assert!(fits(300, 3, budget));
        let one_read = antenna_reads(&scan, &partitions, budget).unwrap();
        assert_eq!(one_read.len(), 1);
        assert_eq!(
            one_read[0].antennas,
            [AntennaId(0), AntennaId(1), AntennaId(2)]
        );
        assert_eq!(one_read[0].times, -5.0..=15.0);

        // Room for two antennas' rows, not three.
        let tight = (1024..budget)
            .step_by(64)
            .find(|budget| fits(200, 2, *budget) && !fits(300, 3, *budget))
            .expect("a budget for two antennas");
        let reads = antenna_reads(&scan, &partitions, tight).unwrap();
        let groups: Vec<_> = reads
            .iter()
            .map(|read| (read.partition.0, read.antennas.clone()))
            .collect();
        assert_eq!(
            groups,
            [
                (0, vec![AntennaId(0), AntennaId(1)]),
                (1, vec![AntennaId(2)])
            ]
        );
        assert!(antenna_reads(&scan, &partitions, 1024).is_err());
    }
}
