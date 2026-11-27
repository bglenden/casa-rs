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
#[path = "../../../libs/imaging/casa-imaging/tests/common/imaging.rs"]
#[allow(dead_code)]
mod imaging;
#[path = "cases/imsubimage_casa.rs"]
mod imsubimage_casa;
#[path = "cases/legacy_simulation_fits.rs"]
mod legacy_simulation_fits;
#[path = "cases/mtmfs_publication_oracle.rs"]
mod mtmfs_publication_oracle;
#[path = "cases/t31_multidomain_casa_oracle.rs"]
mod t31_multidomain_casa_oracle;
#[path = "cases/t34_full_stokes_casa_oracle.rs"]
mod t34_full_stokes_casa_oracle;
#[path = "cases/t41_moving_source_casa_oracle.rs"]
mod t41_moving_source_casa_oracle;

#[path = "cases/simalma_casa.rs"]
mod simalma_casa;
#[path = "cases/simanalyze_multi_casa.rs"]
mod simanalyze_multi_casa;
#[path = "cases/simanalyze_task_casa.rs"]
mod simanalyze_task_casa;
