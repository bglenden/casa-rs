// SPDX-License-Identifier: LGPL-3.0-or-later
//! The saved additive field of `setnoise(OTF=False)`: reused on every corrupt.
use casa_calibration::SimulatedNoiseSolution;
use casa_imaging_runtime::Reservation;
use casa_numerics::cell_noise::CellNoise;
use num_complex::Complex32;

#[derive(Debug)]
struct Chunk {
    time: f64,
    field: i32,
    offset: usize,
}
#[derive(Debug)]
struct Window {
    channels: usize,
    chunks: Vec<Chunk>,
}

/// A complete A Noise field, with its storage admitted before construction.
#[derive(Debug)]
pub(crate) struct OfflineNoise {
    pub(crate) antennas: usize,
    pub(crate) scans: usize,
    windows: Vec<Window>,
    values: Vec<Complex32>,
    chunks: usize,
    _reservation: Reservation,
}

impl OfflineNoise {
    pub(crate) fn bytes(antennas: usize, counts: &[usize], channels: &[usize]) -> u64 {
        let baselines = antennas * (antennas + 1) / 2;
        let values = counts
            .iter()
            .zip(channels)
            .map(|(n, c)| n * baselines * c * 4)
            .sum::<usize>();
        (counts.len() * size_of::<Window>()
            + counts.iter().sum::<usize>() * size_of::<Chunk>()
            + values * size_of::<Complex32>()
            + 1024) as u64
    }

    pub(crate) fn new(
        antennas: usize,
        scans: usize,
        counts: &[usize],
        channels: &[usize],
        reservation: Reservation,
    ) -> Self {
        let baselines = antennas * (antennas + 1) / 2;
        let values = counts
            .iter()
            .zip(channels)
            .map(|(n, c)| n * baselines * c * 4)
            .sum();
        Self {
            antennas,
            scans,
            windows: counts
                .iter()
                .zip(channels)
                .map(|(&count, &channels)| Window {
                    channels,
                    chunks: Vec::with_capacity(count),
                })
                .collect(),
            values: Vec::with_capacity(values),
            chunks: 0,
            _reservation: reservation,
        }
    }

    pub(crate) fn push(&mut self, window: usize, time: f64, field: i32, seed: i32, sigma: f32) {
        let channels = self.windows[window].channels;
        let baselines = self.antennas * (self.antennas + 1) / 2;
        let offset = self.values.len();
        self.values
            .resize(offset + baselines * channels * 4, Complex32::new(0., 0.));
        let noise = CellNoise::new([seed as u32, super::PURPOSE_THERMAL_NOISE << 24], sigma);
        for p in 0..self.antennas {
            for q in p..self.antennas {
                let baseline = self.baseline(p, q);
                let begin = offset + baseline * channels * 4;
                noise.add_to_row(
                    (self.chunks * baselines + baseline) as u64,
                    p == q,
                    4,
                    &mut self.values[begin..begin + channels * 4],
                );
            }
        }
        self.windows[window].chunks.push(Chunk {
            time,
            field,
            offset,
        });
        self.chunks += 1;
    }

    pub(crate) fn sorted(mut self) -> Self {
        for window in &mut self.windows {
            window
                .chunks
                .sort_unstable_by(|a, b| a.time.total_cmp(&b.time).then(a.offset.cmp(&b.offset)));
        }
        self
    }

    pub(crate) fn covers(&self, window: usize) -> bool {
        self.windows
            .get(window)
            .is_some_and(|w| !w.chunks.is_empty())
    }

    fn baseline(&self, p: usize, q: usize) -> usize {
        p * self.antennas - p * (p + 1) / 2 + q
    }

    pub(crate) fn add_to_cell(
        &self,
        window: usize,
        time: f64,
        antennas: (usize, usize),
        receptors: &[[usize; 2]],
        cell: &mut [Complex32],
    ) {
        let Some(window) = self.windows.get(window).filter(|w| !w.chunks.is_empty()) else {
            return;
        };
        let after = window.chunks.partition_point(|c| c.time < time);
        let index = match (after.checked_sub(1), window.chunks.get(after)) {
            (Some(before), Some(later))
                if later.time - time < time - window.chunks[before].time =>
            {
                after
            }
            (Some(before), _) => before,
            _ => 0,
        };
        let (p, q) = antennas;
        if p >= self.antennas || q >= self.antennas {
            return;
        }
        let reverse = p > q;
        let (p, q) = if reverse { (q, p) } else { (p, q) };
        let offset = window.chunks[index].offset + self.baseline(p, q) * window.channels * 4;
        for (index, value) in cell.iter_mut().enumerate() {
            let [r1, r2] = receptors[index % receptors.len()];
            let parameter = if reverse { 2 * r2 + r1 } else { 2 * r1 + r2 };
            let noise = self.values[offset + index / receptors.len() * 4 + parameter];
            *value += if reverse { noise.conj() } else { noise };
        }
    }

    pub(crate) fn table_rows(&self) -> u64 {
        (self.chunks * self.antennas * (self.antennas + 1) / 2) as u64
    }
    pub(crate) fn table_values(&self) -> u64 {
        self.values.len() as u64
    }

    pub(crate) fn solutions(&self) -> impl Iterator<Item = SimulatedNoiseSolution> + '_ {
        self.windows
            .iter()
            .enumerate()
            .flat_map(move |(id, window)| {
                window.chunks.iter().flat_map(move |chunk| {
                    (0..self.antennas).flat_map(move |p| {
                        (p..self.antennas).map(move |q| {
                            let offset = chunk.offset + self.baseline(p, q) * window.channels * 4;
                            SimulatedNoiseSolution {
                                time_s: chunk.time,
                                field_id: chunk.field,
                                spectral_window_id: id as i32,
                                antennas: [p as i32, q as i32],
                                values: self.values[offset..offset + window.channels * 4]
                                    .chunks_exact(4)
                                    .map(|v| v.try_into().expect("four parameters"))
                                    .collect(),
                            }
                        })
                    })
                })
            })
    }
}
