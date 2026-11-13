// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{num::NonZeroUsize, path::PathBuf};

use casa_imaging_model::ProductNormalization;
use serde::Deserialize;

use super::{AwProjection, normalization, optional};

/// Convolution-function set (`gridder`) and the parameters only it reads.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "gridder", rename_all = "lowercase")]
pub enum Gridder {
    /// The prolate-spheroidal kernel.
    Standard {
        /// Facets along each axis of the main image (CASA's `facets`): each
        /// is gridded about its own centre.
        facets: NonZeroUsize,
    },
    /// W-projection.
    Wproject {
        /// Facets along each image axis, each with its own W kernel set.
        facets: NonZeroUsize,
        /// W planes; `None` derives the count from the selected W range.
        #[serde(deserialize_with = "optional")]
        wprojplanes: Option<NonZeroUsize>,
    },
    /// The heterogeneous-array mosaic beams.
    Mosaic {
        /// Point each field's beam along the `POINTING` table.
        usepointing: bool,
        /// Flat-noise or flat-sky products.
        #[serde(deserialize_with = "normalization")]
        normtype: ProductNormalization,
        /// Grid with each beam at its conjugate frequency about the image
        /// frequency, on an image of one plane (tclean `conjbeams`).
        conjbeams: bool,
        /// Weigh each field by its own visibility density (tclean
        /// `mosweight`, which `task_tclean.py` keeps only for the gridder
        /// named `mosaic`); density weighting only.
        mosweight: bool,
    },
    /// EVLA A-projection with W-projection.
    Awproject(AwProjection),
    /// AWPLPG with an unsquinted VLA or EVLA L-band beam.
    Awp2 {
        /// Number of positive-|w| sampling planes, including zero.
        #[serde(deserialize_with = "optional")]
        wprojplanes: Option<NonZeroUsize>,
        /// Optional explicit VLA radial dish surface in the CASA data format.
        #[serde(deserialize_with = "optional")]
        evla_surface: Option<PathBuf>,
        /// Direction-dependent product normalization.
        #[serde(deserialize_with = "normalization")]
        normtype: ProductNormalization,
    },
}
