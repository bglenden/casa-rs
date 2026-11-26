// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use casa_lattices::{Lattice, LatticeMut, ScratchSpace, TempLattice, TempStoragePolicy};
use ndarray::IxDyn;

use casa_images::ImageError;

use super::{CollapseFunction, CollapsePixel, MAX_TILE_SAMPLES};

const PAGE_SAMPLES: usize = 8192;

pub(super) fn reduce<T: CollapsePixel>(
    function: CollapseFunction,
    mut samples: Vec<T>,
) -> Option<T> {
    let count = samples.len();
    let center = median(samples.as_mut_slice(), count)
        .expect("in-memory median lookups stay within the sample slice")?;
    let Some(scale) = function.deviation_scale() else {
        return Some(center);
    };
    for sample in &mut samples {
        *sample = deviation(*sample, center);
    }
    median(samples.as_mut_slice(), count)
        .expect("in-memory deviation lookups stay within the sample slice")
        .map(|mad| T::narrow(mad.wide() * scale))
}

pub(super) fn reduce_large<T: CollapsePixel>(
    function: CollapseFunction,
    mut visit: impl FnMut(&mut dyn FnMut(T) -> Result<(), ImageError>) -> Result<(), ImageError>,
) -> Result<(Option<T>, usize), ImageError> {
    let mut count = 0;
    let mut samples = Vec::new();
    visit(&mut |sample| {
        count += 1;
        if samples.len() < MAX_TILE_SAMPLES {
            samples.push(sample);
        }
        Ok(())
    })?;
    if count <= MAX_TILE_SAMPLES {
        return Ok((reduce(function, samples), count));
    }
    drop(samples);
    let mut samples = PagedSamples::<T>::new(count)?;
    let mut page = Vec::with_capacity(PAGE_SAMPLES);
    let mut written = 0;
    visit(&mut |sample| {
        page.push(sample);
        if page.len() == PAGE_SAMPLES {
            samples.write_page(written, std::mem::take(&mut page))?;
            written += PAGE_SAMPLES;
        }
        Ok(())
    })?;
    if !page.is_empty() {
        samples.write_page(written, page)?;
    }
    let center = median(&mut samples, count)?.expect("nonempty median");
    let Some(scale) = function.deviation_scale() else {
        return Ok((Some(center), count));
    };
    samples.map(|value| deviation(value, center))?;
    Ok((
        median(&mut samples, count)?.map(|mad| T::narrow(mad.wide() * scale)),
        count,
    ))
}

fn deviation<T: CollapsePixel>(sample: T, center: T) -> T {
    T::narrow((sample.wide() - center.wide()).norm().into())
}

trait RankSamples<T> {
    fn sample(&mut self, index: usize) -> Result<T, ImageError>;
    fn exchange(&mut self, left: usize, right: usize) -> Result<(), ImageError>;
}

impl<T: CollapsePixel> RankSamples<T> for [T] {
    fn sample(&mut self, index: usize) -> Result<T, ImageError> {
        Ok(self[index])
    }

    fn exchange(&mut self, left: usize, right: usize) -> Result<(), ImageError> {
        self.swap(left, right);
        Ok(())
    }
}

fn median<T: CollapsePixel>(
    samples: &mut (impl RankSamples<T> + ?Sized),
    count: usize,
) -> Result<Option<T>, ImageError> {
    if count == 0 {
        return Ok(None);
    }
    let upper = count / 2;
    let lower = (count - 1) / 2;
    let a = select(samples, 0, count - 1, lower)?;
    if lower == upper {
        return Ok(Some(a));
    }
    let b = select(samples, lower, count - 1, upper)?;
    Ok(Some(T::narrow((a.wide() + b.wide()) / 2.0)))
}

// Keep the median-of-three partition order, which determines which complex
// sample is returned when different phases have equal magnitude.
fn select<T: CollapsePixel>(
    values: &mut (impl RankSamples<T> + ?Sized),
    mut low: usize,
    mut high: usize,
    rank: usize,
) -> Result<T, ImageError> {
    let less = |first: T, second: T| first.ordering_value() < second.ordering_value();
    while high > low + 1 {
        let middle = low.midpoint(high);
        if less(values.sample(middle)?, values.sample(low)?) {
            values.exchange(middle, low)?;
        }
        if less(values.sample(high)?, values.sample(low)?) {
            values.exchange(high, low)?;
        }
        if less(values.sample(middle)?, values.sample(high)?) {
            values.exchange(middle, high)?;
        }
        let pivot = values.sample(high)?;
        let mut left = low;
        let mut right = high;
        loop {
            left += 1;
            while less(values.sample(left)?, pivot) {
                left += 1;
            }
            right -= 1;
            while less(pivot, values.sample(right)?) {
                right -= 1;
            }
            if left >= right {
                break;
            }
            values.exchange(left, right)?;
        }
        values.exchange(left, high)?;
        if left <= rank {
            low = left;
        }
        if left >= rank {
            high = left;
        }
    }
    if high == low + 1 && less(values.sample(high)?, values.sample(low)?) {
        values.exchange(low, high)?;
    }
    values.sample(rank)
}

// Keep the partition cursors and pivot in a handful of pages while applying
// the same ordering to groups that do not fit in memory.
struct PagedSamples<T: CollapsePixel> {
    lattice: TempLattice<T>,
    count: usize,
    cache: [Option<Page<T>>; 4],
    next: usize,
}

struct Page<T> {
    start: usize,
    values: Vec<T>,
    dirty: bool,
}

impl<T: CollapsePixel> PagedSamples<T> {
    fn new(count: usize) -> Result<Self, ImageError> {
        let mut lattice = TempLattice::new(
            vec![count],
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
        )?;
        lattice.set_cache_size_in_tiles(4)?;
        Ok(Self {
            lattice,
            count,
            cache: [None, None, None, None],
            next: 0,
        })
    }

    fn page(&mut self, index: usize) -> Result<&mut Page<T>, ImageError> {
        let start = index / PAGE_SAMPLES * PAGE_SAMPLES;
        let slot = match self
            .cache
            .iter()
            .position(|entry| entry.as_ref().is_some_and(|page| page.start == start))
        {
            Some(slot) => slot,
            None => {
                let slot = self.next;
                self.next = (slot + 1) % self.cache.len();
                self.flush(slot)?;
                self.cache[slot] = Some(Page {
                    start,
                    values: self.read_page(start)?,
                    dirty: false,
                });
                slot
            }
        };
        Ok(self.cache[slot].as_mut().expect("requested sample page"))
    }

    fn flush(&mut self, slot: usize) -> Result<(), ImageError> {
        if let Some(page) = self.cache[slot].take()
            && page.dirty
        {
            self.write_page(page.start, page.values)?;
        }
        Ok(())
    }

    fn read_page(&self, start: usize) -> Result<Vec<T>, ImageError> {
        let count = PAGE_SAMPLES.min(self.count - start);
        Ok(self
            .lattice
            .get_slice(&[start], &[count], &[1])?
            .into_iter()
            .collect())
    }

    fn write_page(&mut self, start: usize, values: Vec<T>) -> Result<(), ImageError> {
        let data = ArrayD::from_shape_vec(IxDyn(&[values.len()]), values).expect("one sample page");
        self.lattice.put_slice(&data, &[start])?;
        Ok(())
    }

    fn map(&mut self, convert: impl Fn(T) -> T) -> Result<(), ImageError> {
        for slot in 0..self.cache.len() {
            self.flush(slot)?;
        }
        for start in (0..self.count).step_by(PAGE_SAMPLES) {
            let mut values = self.read_page(start)?;
            for value in &mut values {
                *value = convert(*value);
            }
            self.write_page(start, values)?;
        }
        Ok(())
    }

    fn set(&mut self, index: usize, value: T) -> Result<(), ImageError> {
        let page = self.page(index)?;
        page.values[index - page.start] = value;
        page.dirty = true;
        Ok(())
    }
}

impl<T: CollapsePixel> RankSamples<T> for PagedSamples<T> {
    fn sample(&mut self, index: usize) -> Result<T, ImageError> {
        let page = self.page(index)?;
        Ok(page.values[index - page.start])
    }

    fn exchange(&mut self, left: usize, right: usize) -> Result<(), ImageError> {
        let a = self.sample(left)?;
        let b = self.sample(right)?;
        if a != b {
            self.set(left, b)?;
            self.set(right, a)?;
        }
        Ok(())
    }
}
