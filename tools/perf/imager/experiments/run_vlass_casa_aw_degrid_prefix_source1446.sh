#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

experiment_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export CASA_RS_VLASS_CASA_DEGRID_PREFIX_LABEL="source1446-v1"
export CASA_RS_VLASS_CASA_DEGRID_PREFIX_ROW=35
export CASA_RS_VLASS_CASA_DEGRID_PREFIX_CHANNEL=19
export CASA_RS_VLASS_CASA_DEGRID_PREFIX_POLARIZATION=0
export CASA_RS_VLASS_CASA_DEGRID_PREFIX_MODEL_COLUMN=0

exec bash "${experiment_dir}/run_vlass_casa_aw_degrid_prefix_oracle.sh"
