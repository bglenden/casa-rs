// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// casacore's write locks on the tables of a MeasurementSet that a save
/// rewrites in place, taken by `MeasurementSet::lock_tables_for_in_place_save`.
///
/// A table being created has no preexisting lock.
pub(super) struct InPlaceSaveLocks {
    pub(super) locks: Vec<(Option<SubtableId>, TableWriteLock)>,
}

impl InPlaceSaveLocks {
    pub(super) fn for_table(&self, id: Option<SubtableId>) -> Option<&TableWriteLock> {
        self.locks
            .iter()
            .find(|(locked, _)| *locked == id)
            .map(|(_, lock)| lock)
    }

    /// Release the locks after the saves have published their changes.
    pub(super) fn release(self) -> MsResult<()> {
        for (_, lock) in self.locks.into_iter().rev() {
            lock.release()?;
        }
        Ok(())
    }
}

/// Save MAIN with CASA's mixed storage managers
/// ([`measurement_set_main_table_bindings`]).
pub(super) fn save_main_table_with_policy(
    main: &mut Table,
    path: &Path,
    lock: Option<&TableWriteLock>,
) -> MsResult<()> {
    let options = measurement_set_table_options(path);
    let bindings = measurement_set_main_table_bindings(main);
    main.prepare_write()
        .using_write_lock(lock)
        .save_with_bindings(options, &bindings)?;
    Ok(())
}

pub(super) fn save_main_table_with_policy_and_column_overrides(
    main: &mut Table,
    path: &Path,
    column_overrides: &ColumnOverrides,
    lock: Option<&TableWriteLock>,
) -> MsResult<()> {
    let options = measurement_set_table_options(path);
    let bindings = measurement_set_main_table_bindings(main);
    main.prepare_write()
        .using_write_lock(lock)
        .save_with_column_overrides(options, &bindings, column_overrides)?;
    Ok(())
}

/// Build MAIN storage-manager bindings for the columns present in the supplied table.
pub fn measurement_set_main_table_bindings(main: &Table) -> HashMap<String, ColumnBinding> {
    let column_names: HashSet<_> = main
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name())
                .collect()
        })
        .unwrap_or_default();
    let mut bindings = HashMap::new();
    let mut bind = |name: &str, data_manager: DataManagerKind, tile_shape: Option<Vec<usize>>| {
        if column_names.contains(name) {
            bindings.insert(
                name.to_string(),
                ColumnBinding {
                    data_manager,
                    tile_shape,
                },
            );
        }
    };

    let (num_corr, num_chan) = main_visibility_cell_shape(main).unwrap_or((4, 128));
    let visibility_tile = casa_visibility_tile_shape(num_corr, num_chan, "");
    let visibility_tile_shape = Some(visibility_tile.clone());
    let flag_tile_shape = Some(visibility_tile.clone());
    let flag_category_tile_shape = Some(vec![
        visibility_tile[0],
        visibility_tile[1],
        1,
        visibility_tile[2],
    ]);
    let weight_tile_shape = Some(casa_weight_tile_shape(&visibility_tile));
    let uvw_tile_shape = Some(casa_uvw_tile_shape(&visibility_tile));

    for name in [
        "ARRAY_ID",
        "EXPOSURE",
        "FEED1",
        "FEED2",
        "FIELD_ID",
        "INTERVAL",
        "OBSERVATION_ID",
        "PROCESSOR_ID",
        "SCAN_NUMBER",
        "STATE_ID",
        "TIME",
        "TIME_CENTROID",
    ] {
        bind(name, DataManagerKind::IncrementalStMan, None);
    }
    for name in ["ANTENNA1", "ANTENNA2", "DATA_DESC_ID", "FLAG_ROW"] {
        bind(name, DataManagerKind::StandardStMan, None);
    }
    for name in [
        "DATA",
        "MODEL_DATA",
        "CORRECTED_DATA",
        "FLOAT_DATA",
        "LAG_DATA",
    ] {
        bind(
            name,
            DataManagerKind::TiledShapeStMan,
            visibility_tile_shape.clone(),
        );
    }
    bind("FLAG", DataManagerKind::TiledShapeStMan, flag_tile_shape);
    bind(
        "FLAG_CATEGORY",
        DataManagerKind::TiledShapeStMan,
        flag_category_tile_shape,
    );
    for name in ["SIGMA", "WEIGHT"] {
        bind(
            name,
            DataManagerKind::TiledShapeStMan,
            weight_tile_shape.clone(),
        );
    }
    for name in [
        "SIGMA_SPECTRUM",
        "WEIGHT_SPECTRUM",
        "CORRECTED_WEIGHT_SPECTRUM",
    ] {
        bind(
            name,
            DataManagerKind::TiledShapeStMan,
            visibility_tile_shape.clone(),
        );
    }
    bind("UVW", DataManagerKind::TiledColumnStMan, uvw_tile_shape);

    bindings
}

pub(super) fn main_visibility_cell_shape(main: &Table) -> Option<(usize, usize)> {
    if main.row_count() == 0 {
        return None;
    }
    for column in ["DATA", "MODEL_DATA", "CORRECTED_DATA"] {
        let Ok(column) = main.column_accessor(column) else {
            continue;
        };
        let Ok(cells) = column.array_cells_owned_uncached(&[0]) else {
            continue;
        };
        let Some(Some(array)) = cells.first() else {
            continue;
        };
        let shape = array.shape();
        if shape.len() >= 2 && shape[0] > 0 && shape[1] > 0 {
            return Some((shape[0], shape[1]));
        }
    }
    None
}

/// Choose the CASA visibility tile extents in correlation, channel and row order.
pub fn casa_visibility_tile_shape(
    num_corr: usize,
    num_chan: usize,
    telescope_name: &str,
) -> Vec<usize> {
    let corr_tile = num_corr.max(1);
    let mut chan_tile = num_chan.max(1);
    let n_ifr = casa_interferometer_count(telescope_name);
    let io_block_size = 131_072usize;
    let mut element_io_block_size = io_block_size;
    if chan_tile < 100 {
        chan_tile = (io_block_size / corr_tile / n_ifr).max(1);
    } else if chan_tile < 10_000 {
        chan_tile = ((chan_tile as f32 / 99.9).sqrt().floor() as usize).max(1) * 10;
    } else {
        chan_tile = 100;
    }
    while (io_block_size / corr_tile / chan_tile) > 10 * n_ifr && chan_tile < num_chan {
        chan_tile += 2;
    }
    if (io_block_size / corr_tile / chan_tile) > 10 * n_ifr {
        element_io_block_size = 10 * n_ifr * corr_tile * chan_tile;
    }
    chan_tile = chan_tile.min(num_chan).max(1);
    let row_tile = (element_io_block_size / corr_tile / chan_tile).max(1);
    vec![corr_tile, chan_tile, row_tile]
}

/// Return CASA’s telescope-specific baseline count used to size visibility tiles.
pub fn casa_interferometer_count(telescope_name: &str) -> usize {
    match telescope_name {
        "ATCA" => 15usize,
        "VLA" => 351usize,
        "WSRT" => 91usize,
        "BIMA" => 36usize,
        "DRAO" => 21usize,
        "SMA" => 28usize,
        _ => 200usize,
    }
}

/// Project a visibility tile into the correlation-by-row shape of a WEIGHT tile.
pub fn casa_weight_tile_shape(visibility_tile_shape: &[usize]) -> Vec<usize> {
    vec![
        visibility_tile_shape[0],
        visibility_tile_shape[1] * visibility_tile_shape[2],
    ]
}

/// Choose the three-coordinate UVW tile with the row capacity of the visibility tile.
pub fn casa_uvw_tile_shape(visibility_tile_shape: &[usize]) -> Vec<usize> {
    vec![
        3,
        (visibility_tile_shape[0] * visibility_tile_shape[1] * visibility_tile_shape[2] / 3).max(1),
    ]
}

pub(crate) fn measurement_set_main_data_manager_sequence(
    main: &Table,
    target_column: &str,
) -> Option<u32> {
    let schema = main.schema()?;
    let bindings = measurement_set_main_table_bindings(main);
    let mut binding_seq_map: HashMap<String, u32> = HashMap::new();
    let mut next_seq = 1u32;

    for column in schema.columns() {
        let column_name = column.name();
        let Some(binding) = bindings.get(column_name) else {
            if column_name == target_column {
                return Some(0);
            }
            continue;
        };
        let group_key = if matches!(
            binding.data_manager,
            DataManagerKind::TiledColumnStMan
                | DataManagerKind::TiledShapeStMan
                | DataManagerKind::TiledCellStMan
                | DataManagerKind::TiledDataStMan
        ) {
            format!("{:?}:{column_name}", binding.data_manager)
        } else {
            format!("{:?}", binding.data_manager)
        };
        let seq = if let Some(seq) = binding_seq_map.get(&group_key) {
            *seq
        } else {
            let seq = next_seq;
            binding_seq_map.insert(group_key, seq);
            next_seq += 1;
            seq
        };
        if column_name == target_column {
            return Some(seq);
        }
    }

    None
}
