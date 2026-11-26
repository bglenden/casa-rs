// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "../../../libs/casa/casa-components/tests/common/mod.rs"]
mod common;
const FIXTURE_ROOT: &str = "../../libs/casa/casa-components/tests/data";
#[path = "cases/casa_axis_maps.rs"]
mod casa_axis_maps;
#[path = "cases/component_oracle.rs"]
mod component_oracle;
#[path = "cases/image_analysis_casa.rs"]
mod image_analysis_casa;
#[path = "cases/imsubimage_casa.rs"]
mod imsubimage_casa;
#[path = "cases/legacy_simulation_fits.rs"]
mod legacy_simulation_fits;
