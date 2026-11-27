// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded tiled MS MAIN creation for the FITS visibility readers.

use std::path::Path;

use casa_core::{Complex32, ScalarValue};
use casa_tables::streamed_tiled::{
    StreamedTiledPrimitiveType, StreamedTiledShapeValueType, install_undefined_tiled_shape_column,
};
use casa_tables::{ColumnOverrides, GeneratedScalarColumn};
use ndarray::{Array1, Array2};

use super::invalid;
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetWritePlan, MeasurementSetWriteResources, MeasurementSetWriteSession,
    standard_main_scalar_column_plans,
};
use crate::{MeasurementSet, MsResult};

#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum Spectra {
    None,
    Weight,
    WeightAndSigma,
}

impl Spectra {
    fn columns(self) -> impl Iterator<Item = &'static str> {
        let names: &[&str] = match self {
            Self::None => &[],
            Self::Weight => &["WEIGHT_SPECTRUM"],
            Self::WeightAndSigma => &["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"],
        };
        names.iter().copied()
    }
}

#[doc(hidden)]
pub struct ImportColumns {
    pub(super) writer: MeasurementSetWriteSession,
    rows: usize,
    category_tile: [usize; 4],
    flag_category: Option<Vec<bool>>,
    spectra: Spectra,
}

impl ImportColumns {
    pub fn start(
        path: &Path,
        rows: usize,
        visibility: [usize; 2],
        tile: Vec<usize>,
        categories: bool,
        spectra: Spectra,
    ) -> MsResult<Self> {
        use MeasurementSetColumnStorage::{TiledColumn, TiledShape};
        use StreamedTiledShapeValueType::{Bool, Complex32, Float32, Float64};

        let category_tile = [tile[0], tile[1], 1, tile[2]];
        let weight = [visibility[0]];
        let shape = |name: &str, value_type, cell: &[usize], tile_shape, storage_manager| {
            MeasurementSetArrayColumnPlan {
                name: name.into(),
                value_type,
                shapes: vec![MeasurementSetArrayShapePlan {
                    cell_shape: cell.to_vec(),
                    row_count: rows,
                    tile_shape,
                }],
                storage_manager,
            }
        };
        let weight_tile = crate::ms::casa_weight_tile_shape(&tile);
        let mut arrays = vec![
            shape("DATA", Complex32, &visibility, tile.clone(), TiledShape),
            shape("FLAG", Bool, &visibility, tile.clone(), TiledShape),
            shape("WEIGHT", Float32, &weight, weight_tile.clone(), TiledShape),
            shape("SIGMA", Float32, &weight, weight_tile, TiledShape),
            shape(
                "UVW",
                Float64,
                &[3],
                crate::ms::casa_uvw_tile_shape(&tile),
                TiledColumn,
            ),
        ];
        for name in spectra.columns() {
            arrays.push(shape(name, Float32, &visibility, tile.clone(), TiledShape));
        }
        let flag_category = categories.then(Vec::new);
        if flag_category.is_some() {
            arrays.push(shape(
                "FLAG_CATEGORY",
                Bool,
                &[visibility[0], visibility[1], 3],
                category_tile.to_vec(),
                TiledShape,
            ));
        }
        let mut scalars = standard_main_scalar_column_plans();
        scalars.retain(|column| {
            !CONSTANT_COLUMNS
                .iter()
                .any(|(name, _)| *name == column.name)
        });
        let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(invalid)?;
        let plan =
            MeasurementSetWritePlan::variable_array_creation(rows, arrays, scalars, resources)
                .map_err(invalid)?;
        let writer = MeasurementSetWriteSession::start(path, plan).map_err(invalid)?;
        Ok(Self {
            writer,
            rows,
            category_tile,
            flag_category,
            spectra,
        })
    }

    pub fn append(&mut self, row: MainRow, samples: MainSamples<'_>) -> MsResult<()> {
        use ScalarValue::{Bool, Float64, Int32};
        let flags = samples
            .flags
            .as_slice_memory_order()
            .expect("contiguous FITS flags");
        for (name, value) in [
            ("ANTENNA1", Int32(row.antennas[0])),
            ("ANTENNA2", Int32(row.antennas[1])),
            ("ARRAY_ID", Int32(row.array)),
            ("DATA_DESC_ID", Int32(row.window)),
            ("FIELD_ID", Int32(row.field)),
            ("SCAN_NUMBER", Int32(row.scan)),
            ("FLAG_ROW", Bool(flags.iter().all(|&value| value))),
            ("TIME", Float64(row.time)),
            ("TIME_CENTROID", Float64(row.centroid)),
        ] {
            self.writer
                .push_scalar_row(name, Some(value))
                .map_err(invalid)?;
        }
        if let Some(interval) = row.interval {
            for name in ["INTERVAL", "EXPOSURE"] {
                self.writer
                    .push_scalar_row(name, Some(Float64(interval)))
                    .map_err(invalid)?;
            }
        }
        self.writer
            .push_f64_row("UVW", &[3], &row.uvw)
            .map_err(invalid)?;
        self.append_samples(samples, flags)
    }

    fn append_samples(&mut self, samples: MainSamples<'_>, flags: &[bool]) -> MsResult<()> {
        let data = samples.data;
        let shape = data.shape();
        self.writer
            .push_complex32_row(
                "DATA",
                shape,
                data.as_slice_memory_order().expect("contiguous FITS data"),
            )
            .map_err(invalid)?;
        self.writer
            .push_bool_row("FLAG", shape, flags)
            .map_err(invalid)?;
        if let Some(category) = &mut self.flag_category {
            category.clear();
            category.extend_from_slice(flags);
            category.resize(3 * flags.len(), false);
            self.writer
                .push_bool_row("FLAG_CATEGORY", &[shape[0], shape[1], 3], category)
                .map_err(invalid)?;
        }
        for (name, values) in [("WEIGHT", samples.weight), ("SIGMA", samples.sigma)] {
            self.writer
                .push_f32_row(
                    name,
                    values.shape(),
                    values.as_slice().expect("contiguous correlation vector"),
                )
                .map_err(invalid)?;
        }
        for (name, values) in [
            ("WEIGHT_SPECTRUM", samples.weight_spectrum),
            ("SIGMA_SPECTRUM", samples.sigma_spectrum),
        ] {
            if let Some(values) = values {
                self.writer
                    .push_f32_row(
                        name,
                        shape,
                        values
                            .as_slice_memory_order()
                            .expect("contiguous FITS spectrum"),
                    )
                    .map_err(invalid)?;
            }
        }
        Ok(())
    }

    pub fn finish(self, ms: &mut MeasurementSet, path: &Path) -> MsResult<()> {
        let mut overrides = ColumnOverrides::for_row_count(self.rows);
        for name in ["DATA", "FLAG", "WEIGHT", "SIGMA", "UVW"]
            .into_iter()
            .chain(self.spectra.columns())
        {
            overrides.insert_deferred(name);
        }
        overrides.insert_deferred("FLAG_CATEGORY");
        for (name, value) in CONSTANT_COLUMNS {
            overrides.insert_generated_scalar(
                name,
                GeneratedScalarColumn::constant(self.rows, Some(ScalarValue::Int32(value))),
            );
        }
        self.writer
            .save_and_finish(ms, &overrides)
            .map_err(invalid)?;
        if self.flag_category.is_some() {
            return Ok(());
        }
        let seq =
            crate::ms::measurement_set_main_data_manager_sequence(ms.main_table(), "FLAG_CATEGORY")
                .ok_or_else(|| invalid("Missing FLAG_CATEGORY storage manager"))?;
        install_undefined_tiled_shape_column(
            path,
            seq,
            "FLAG_CATEGORY",
            self.rows,
            StreamedTiledPrimitiveType::Bool,
            3,
            &self.category_tile,
        )
        .map_err(invalid)
    }
}

#[doc(hidden)]
pub struct MainRow {
    pub antennas: [i32; 2],
    pub uvw: [f64; 3],
    pub time: f64,
    pub centroid: f64,
    pub interval: Option<f64>,
    pub array: i32,
    pub field: i32,
    pub window: i32,
    pub scan: i32,
}

#[doc(hidden)]
pub struct MainSamples<'a> {
    pub data: &'a Array2<Complex32>,
    pub flags: &'a Array2<bool>,
    pub weight: &'a Array1<f32>,
    pub sigma: &'a Array1<f32>,
    pub weight_spectrum: Option<&'a Array2<f32>>,
    pub sigma_spectrum: Option<&'a Array2<f32>>,
}

const CONSTANT_COLUMNS: [(&str, i32); 5] = [
    ("FEED1", 0),
    ("FEED2", 0),
    ("OBSERVATION_ID", 0),
    ("PROCESSOR_ID", -1),
    ("STATE_ID", -1),
];
