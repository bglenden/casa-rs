// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of `SubImage` and `SubImageMut`.

use casacore_coordinates::CoordinateSystem;
use casacore_images::{SubImage, TempImage};
use casacore_lattices::{Lattice, LatticeMut};

fn main() {
    let mut image = TempImage::<f32>::new(vec![6, 6], CoordinateSystem::new()).unwrap();
    image.set(0.0).unwrap();

    {
        let mut sub = image.sub_image_mut(vec![2, 2], vec![2, 3]).unwrap();
        sub.set(5.0).unwrap();
    }

    let sub = SubImage::with_stride(&image, vec![0, 0], vec![3, 3], vec![2, 2]).unwrap();
    println!("SubImage shape: {:?}", sub.shape());
    println!("Parent pixel [2,2] = {}", image.get_at(&[2, 2]).unwrap());
    println!("Strided pixel [1,1] = {}", sub.get_at(&[1, 1]).unwrap());
}
