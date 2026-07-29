// Read-only CASA VI2 probe for the reduced VLASS Briggs-density investigation.
//
// This is deliberately a standalone diagnostic, not production casa-rs code.
// It exposes the exact row-zero LSRK channel vector that
// VisImagingWeight::VisImagingWeight consumes through vb.frequency().

#include <casacore/casa/Arrays/ArrayMath.h>
#include <casacore/casa/System/Aipsrc.h>
#include <casacore/casa/Quanta/Quantum.h>
#include <casacore/measures/Measures/MFrequency.h>
#include <casacore/ms/MSSel/MSSelection.h>
#include <casacore/ms/MeasurementSets/MSMainEnums.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/TaQL/ExprNode.h>
#include <msvis/MSVis/VisBuffer2.h>
#include <msvis/MSVis/VisImagingWeight.h>
#include <msvis/MSVis/VisibilityIterator2.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "usage: vlass_vi2_frequency_probe MS CASA_MEASURES_DIRECTORY\n";
    return 2;
  }

  const auto measures_key =
      casacore::Aipsrc::registerRC("measures.directory", argv[2]);
  casacore::Aipsrc::set(measures_key, argv[2]);
  casacore::MeasurementSet ms(argv[1]);
  casacore::Block<casacore::Int> columns(4);
  columns[0] = casacore::MS::ARRAY_ID;
  columns[1] = casacore::MS::DATA_DESC_ID;
  columns[2] = casacore::MS::FIELD_ID;
  columns[3] = casacore::MS::TIME;
  casa::vi::SortColumns sort_columns(columns, false);
  casa::vi::VisibilityIterator2 vi(ms, sort_columns, false);
  vi.setReportingFrameOfReference(casacore::MFrequency::LSRK);
  casa::vi::VisBuffer2 *vb = vi.getVisBuffer();

  const std::set<int> selected_spws{2, 7, 12, 17};
  const std::map<casacore::rownr_t, std::vector<std::pair<int, int>>>
      boundary_samples{
          {354024, {{20, -1}}},
          {360128, {{30, -1}}},
          {360135, {{47, -1}}},
          {360225, {{19, -1}}},
          {360390, {{21, 1}}},
          {360405, {{41, -1}}},
          {360455, {{48, 1}}},
          {360543, {{32, 1}}},
          {360569, {{33, -1}}},
          {360731, {{41, 1}}},
      };
  constexpr double scale = 0.011914781026947955;
  constexpr double origin = 2048.0;
  std::cout << std::setprecision(17);
  for (vi.originChunks(); vi.moreChunks(); vi.nextChunk()) {
    for (vi.origin(); vi.more(); vi.next()) {
      const int field = vb->fieldId()(0);
      const int spw = vb->spectralWindows()(0);
      if (field != 1525 || !selected_spws.contains(spw)) {
        continue;
      }
      const auto &times = vb->time();
      const auto &frequencies = vb->getFrequencies(0);
      std::cout << "field=" << field << " spw=" << spw
                << " rows=" << vb->nRows() << " time0=" << times(0)
                << " time_min=" << casacore::min(times)
                << " time_max=" << casacore::max(times)
                << " f0=" << frequencies(0)
                << " f33=" << frequencies(33)
                << " f63=" << frequencies(63) << '\n';
      const auto &row_ids = vb->rowIds();
      const auto &uvw = vb->uvw();
      for (casacore::rownr_t local_row = 0; local_row < vb->nRows();
           ++local_row) {
        const auto found = boundary_samples.find(row_ids(local_row));
        if (found == boundary_samples.end()) {
          continue;
        }
        for (const auto &[channel, sign] : found->second) {
          const float f = static_cast<float>(frequencies(channel) / 299792458.0);
          const float u = static_cast<float>(uvw(0, local_row) * f);
          const float v = static_cast<float>(uvw(1, local_row) * f);
          const double continuous_u = scale * (sign * u) + origin;
          const double continuous_v = scale * (sign * v) + origin;
          std::cout << "sample row=" << row_ids(local_row)
                    << " local_row=" << local_row << " channel=" << channel
                    << " sign=" << sign << " uvw=(" << uvw(0, local_row)
                    << ',' << uvw(1, local_row) << ',' << uvw(2, local_row)
                    << ") f=" << f << " u=" << u << " v=" << v
                    << " continuous=(" << continuous_u << ',' << continuous_v
                    << ") cell=(" << static_cast<int>(continuous_u) << ','
                    << static_cast<int>(continuous_v) << ")\n";
        }
      }
    }
  }

  casacore::MSSelection selection;
  selection.setFieldExpr("1525");
  selection.setSpwExpr("2,7,12,17");
  casacore::TableExprNode selection_node = selection.toTableExprNode(&ms);
  casacore::MeasurementSet selected_ms(ms(selection_node));
  std::cout << "selected_rows=" << selected_ms.nrow() << '\n';

  casa::vi::VisibilityIterator2 selected_vi(selected_ms, sort_columns, false);
  selected_vi.setReportingFrameOfReference(casacore::MFrequency::LSRK);
  constexpr double cell_rad = 2.9088820866572157e-06;
  casa::VisImagingWeight imaging_weight(
      selected_vi, "norm", casacore::Quantity(0.0, "Jy"), 1.0, 4096, 4096,
      casacore::Quantity(-cell_rad, "rad"),
      casacore::Quantity(cell_rad, "rad"), 0, 0, false);
  casacore::Block<casacore::Matrix<casacore::Float>> densities;
  if (!imaging_weight.getWeightDensity(densities) || densities.nelements() != 1) {
    std::cerr << "failed to retrieve one density matrix\n";
    return 3;
  }
  const auto &density = densities[0];
  double density_sum = 0.0;
  double density_sum_squares = 0.0;
  std::size_t density_nonzero = 0;
  for (casacore::Int y = 0; y < density.ncolumn(); ++y) {
    for (casacore::Int x = 0; x < density.nrow(); ++x) {
      const float value = density(x, y);
      density_sum += value;
      density_sum_squares += static_cast<double>(value) * value;
      density_nonzero += value != 0.0F;
    }
  }
  std::cout << "density shape=(" << density.nrow() << ',' << density.ncolumn()
            << ") nonzero=" << density_nonzero << " sum=" << density_sum
            << " sumsq=" << density_sum_squares << '\n';
  const std::vector<std::pair<int, int>> density_cells{
      {1888, 1340}, {1888, 1341}, {2334, 1490}, {2335, 1490},
      {2292, 1986}, {2293, 1986}, {2071, 2056}, {2072, 2056},
  };
  for (const auto &[x, y] : density_cells) {
    std::cout << "density_cell x=" << x << " y=" << y
              << " value=" << density(x, y) << '\n';
  }
}
