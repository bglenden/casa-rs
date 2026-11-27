// SPDX-License-Identifier: LGPL-3.0-or-later
//! Inputs shared with the independent CASA simulation graphics oracle.
use casa_components::{
    ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
};
use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_images::PagedImage;
use ndarray::{ArrayD, IxDyn};
use std::path::{Path, PathBuf};
const CENTRE: [f64; 2] = [2.617_993_877_991_494_4, -0.523_598_775_598_298_8];

pub fn inputs(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let image = root.join("model.image");
    if image.exists() {
        return (image, root.join("point.cl"), root.join("small.cfg"));
    }
    let mut cs = CoordinateSystem::new();
    cs.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        CENTRE,
        [-2e-6, 2e-6],
        [32.0, 32.0],
    ));
    cs.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    cs.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        230e9,
        1e9,
        0.0,
        230e9,
    ));
    let mut model = PagedImage::<f32>::create(vec![64, 64, 1, 1], cs, &image).expect("image");
    let mut pixels = ArrayD::zeros(IxDyn(&[64, 64, 1, 1]));
    pixels[IxDyn(&[32, 32, 0, 0])] = 1.25;
    pixels[IxDyn(&[41, 28, 0, 0])] = 0.75;
    model.put_slice(&pixels, &[0; 4]).expect("pixels");
    model.set_units("Jy/pixel").expect("units");
    model.save().expect("save");
    let component = root.join("point.cl");
    let mut list = ComponentList::new();
    list.push(SkyComponent::new(
        Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
        ComponentShape::point(MDirection::from_angles(
            CENTRE[0],
            CENTRE[1],
            DirectionRef::J2000,
        )),
        SpectralModel::new(
            MFrequency::new(230e9, FrequencyRef::LSRK),
            SpectralKind::Constant,
        ),
    ));
    list.write_table(&component).expect("component");
    let pads = [
        (-40, -25),
        (30, -40),
        (60, 10),
        (-50, 45),
        (10, 60),
        (0, 0),
        (-80, -70),
        (90, 80),
    ];
    let config = root.join("small.cfg");
    let mut text = String::from("# observatory=ALMA\n# coordsys=XYZ\n");
    for (i, (x, y)) in pads.into_iter().enumerate() {
        text.push_str(&format!(
            "{:.6} {:.6} -2481681.150540 12.0 A{i:02}\n",
            2_225_061.163_6 + f64::from(x),
            -5_440_057.369_94 + f64::from(y)
        ));
    }
    std::fs::write(&config, text).expect("config");
    (image, component, config)
}
