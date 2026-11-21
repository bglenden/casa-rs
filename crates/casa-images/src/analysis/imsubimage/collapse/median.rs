// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::ImageError;

use super::{CollapseFunction, CollapsePixel};

pub(super) fn reduce<T: CollapsePixel>(
    function: CollapseFunction,
    mut samples: Vec<T>,
) -> Option<T> {
    let center = median(&mut samples)?;
    let Some(scale) = function.deviation_scale() else {
        return Some(center);
    };
    for sample in &mut samples {
        *sample = deviation(*sample, center);
    }
    median(&mut samples).map(|mad| T::narrow(mad.wide() * scale))
}

pub(super) fn reduce_large<T: CollapsePixel>(
    function: CollapseFunction,
    mut visit: impl FnMut(&mut dyn FnMut(T)) -> Result<(), ImageError>,
) -> Result<Option<T>, ImageError> {
    let Some(center) = median_large(&mut visit)? else {
        return Ok(None);
    };
    let Some(scale) = function.deviation_scale() else {
        return Ok(Some(center));
    };
    let mad = median_large(|target| visit(&mut |sample| target(deviation(sample, center))))?;
    Ok(mad.map(|mad| T::narrow(mad.wide() * scale)))
}

fn deviation<T: CollapsePixel>(sample: T, center: T) -> T {
    T::narrow((sample.wide() - center.wide()).norm().into())
}

fn median<T: CollapsePixel>(samples: &mut [T]) -> Option<T> {
    if samples.is_empty() {
        return None;
    }
    let upper = samples.len() / 2;
    if samples.len() % 2 == 1 {
        return Some(select(samples, upper));
    }
    let lower = upper - 1;
    let a = select(samples, lower);
    let b = select(&mut samples[lower..], 1);
    Some(T::narrow((a.wide() + b.wide()) / 2.0))
}

// Keep the median-of-three partition order, which determines which complex
// sample is returned when different phases have equal magnitude.
fn select<T: CollapsePixel>(values: &mut [T], rank: usize) -> T {
    let less = |first: T, second: T| first.ordering_value() < second.ordering_value();
    let mut low = 0;
    let mut high = values.len() - 1;
    while high > low + 1 {
        let middle = low.midpoint(high);
        if less(values[middle], values[low]) {
            values.swap(middle, low);
        }
        if less(values[high], values[low]) {
            values.swap(high, low);
        }
        if less(values[middle], values[high]) {
            values.swap(middle, high);
        }
        let pivot = values[high];
        let mut left = low;
        let mut right = high;
        loop {
            left += 1;
            while less(values[left], pivot) {
                left += 1;
            }
            right -= 1;
            while less(pivot, values[right]) {
                right -= 1;
            }
            if left >= right {
                break;
            }
            values.swap(left, right);
        }
        values.swap(left, high);
        if left <= rank {
            low = left;
        }
        if left >= rank {
            high = left;
        }
    }
    if high == low + 1 && less(values[high], values[low]) {
        values.swap(low, high);
    }
    values[rank]
}

// Radix selection bounds storage even if the whole image collapses to one
// cell. Revisit the selected source only for the two median order statistics.
fn median_large<T: CollapsePixel>(
    mut visit: impl FnMut(&mut dyn FnMut(T)) -> Result<(), ImageError>,
) -> Result<Option<T>, ImageError> {
    let mut count = 0;
    visit(&mut |_| count += 1)?;
    if count == 0 {
        return Ok(None);
    }
    let mut ranks = [(count - 1) / 2, count / 2];
    let mut prefix = [0, 0];
    for shift in [24, 16, 8, 0] {
        let mut histograms = [[0usize; 256]; 2];
        visit(&mut |sample| {
            let key = order_key(sample);
            let parent = (key >> shift) >> 8;
            let digit = ((key >> shift) & 255) as usize;
            for (target, histogram) in histograms.iter_mut().enumerate() {
                if prefix[target] == parent {
                    histogram[digit] += 1;
                }
            }
        })?;
        for (target, histogram) in histograms.iter().enumerate() {
            for (digit, &count) in histogram.iter().enumerate() {
                if ranks[target] < count {
                    prefix[target] = (prefix[target] << 8) | digit as u32;
                    break;
                }
                ranks[target] -= count;
            }
        }
    }
    let mut values = [None, None];
    visit(&mut |sample| {
        let key = order_key(sample);
        for target in 0..2 {
            if key == prefix[target] && values[target].is_none() {
                values[target] = Some(sample);
            }
        }
    })?;
    let [a, b] = values.map(|value| value.expect("nonempty rank"));
    Ok(Some(T::narrow((a.wide() + b.wide()) / 2.0)))
}

fn order_key(sample: impl CollapsePixel) -> u32 {
    let bits = sample.ordering_value().to_bits();
    if bits & (1 << 31) == 0 {
        bits ^ (1 << 31)
    } else {
        !bits
    }
}
