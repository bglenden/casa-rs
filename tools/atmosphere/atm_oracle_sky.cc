// SPDX-License-Identifier: LGPL-3.0-or-later
// The 4b part of the ATM oracle: RefractiveIndexProfile and SkyStatus
// (forward radiative transfer) over multi-window grids. Linked into
// atm_oracle by tools/atmosphere/build-atm-oracle.sh. It writes
//
//   absorption.json   for three profiles over a 12-window grid: every
//                     per-channel opacity, phase delay and path length the
//                     RefractiveIndexProfile and SkyStatus give, window
//                     averages, and per-layer absorption coefficients
//   sky.json          Tebb and Trj over airmass, water column, coupling and
//                     spillover, and a SkyStatus call sequence (state set by
//                     setters, then read by the defaulted overloads)

#include <chrono>
#include <cmath>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#define private public
#define protected public
#include "ATMProfile.h"
#include "ATMRefractiveIndexProfile.h"
#include "ATMSkyStatus.h"
#include "ATMSpectralGrid.h"
#undef private
#undef protected

using namespace atm;

namespace {

struct SkyProfile {
  const char* name;
  double altitude_m, pressure_hpa, temperature_k, lapse_rate, humidity_percent;
  double scale_height_km, step_hpa, step_factor, top_km;
  unsigned int type;
};

const SkyProfile kProfiles[] = {
    // CASA's simulator (CalCorruptor::initAtm) and the `at` tool defaults.
    {"calcorruptor", 5000.0, 560.0, 270.0, -5.6, 20.0, 2.0, 10.0, 1.2, 48.0, 1},
    {"sea level midlatitude summer", 0.0, 1013.0, 293.0, -6.5, 60.0, 2.0, 10.0, 1.2, 48.0, 2},
    {"vla midlatitude winter", 2124.0, 790.0, 265.0, -6.5, 40.0, 2.5, 12.5, 1.2, 45.0, 3},
};

struct Window {
  double centre_ghz, width_ghz;
  unsigned int channels;
  bool reversed;
};

// Line centres, windows of each parity, a one-channel window and one with
// a negative separation (as a lower sideband is).
const Window kWindows[] = {
    {22.235, 0.5, 5, false},   {60.3, 1.0, 4, false},   {90.0, 2.0, 1, false},    {118.75, 0.25, 8, false},
    {183.31, 2.0, 7, false},   {230.538, 1.875, 15, false}, {345.796, 2.0, 6, true}, {380.2, 1.0, 5, false},
    {557.0, 4.0, 4, false},    {690.0, 7.5, 3, false},  {850.0, 2.0, 5, false},   {987.9, 2.0, 4, false},
};

AtmProfile make_profile(const SkyProfile& k) {
  return AtmProfile(Length(k.altitude_m, Length::UnitMeter), Pressure(k.pressure_hpa, Pressure::UnitMilliBar),
                    Temperature(k.temperature_k, Temperature::UnitKelvin), k.lapse_rate,
                    Humidity(k.humidity_percent, Percent::UnitPercent),
                    Length(k.scale_height_km, Length::UnitKiloMeter), Pressure(k.step_hpa, Pressure::UnitMilliBar),
                    k.step_factor, Length(k.top_km, Length::UnitKiloMeter), k.type);
}

// The `at` tool's window convention: reference channel (n-1)/2, and for an
// even count the reference frequency half a channel below the centre.
struct WindowSpec {
  unsigned int channels, ref_chan;
  double ref_hz, sep_hz;
};

std::vector<WindowSpec> window_specs() {
  std::vector<WindowSpec> specs;
  for (const Window& w : kWindows) {
    double sep = w.width_ghz * 1e9 / w.channels;
    if (w.reversed) sep = -sep;
    double ref = w.centre_ghz * 1e9;
    if (w.channels % 2 == 0) ref = ref - sep * 0.5;
    specs.push_back({w.channels, (w.channels - 1) / 2, ref, sep});
  }
  return specs;
}

SpectralGrid* make_grid() {
  SpectralGrid* grid = nullptr;
  for (const WindowSpec& s : window_specs()) {
    Frequency ref(s.ref_hz, Frequency::UnitHertz), sep(s.sep_hz, Frequency::UnitHertz);
    if (!grid) grid = new SpectralGrid(s.channels, s.ref_chan, ref, sep);
    else grid->add(s.channels, s.ref_chan, ref, sep);
  }
  return grid;
}

class Json {
 public:
  explicit Json(FILE* out) : out_(out) {}
  void key(const char* k) {
    std::fprintf(out_, "%s\"%s\":", first_ ? "" : ",", k);
    first_ = false;
  }
  void number(const char* k, double v) {
    key(k);
    std::fprintf(out_, "%.17g", v);
  }
  void array(const char* k, const std::vector<double>& v) {
    key(k);
    std::fprintf(out_, "[");
    for (size_t i = 0; i < v.size(); i++) std::fprintf(out_, "%s%.17g", i ? "," : "", v[i]);
    std::fprintf(out_, "]");
  }
  void open(const char* k) {
    key(k);
    std::fprintf(out_, "{");
    first_ = true;
  }
  void open_object() {
    std::fprintf(out_, "%s{", first_ ? "" : ",");
    first_ = true;
  }
  void close() {
    std::fprintf(out_, "}");
    first_ = false;
  }
  void open_list(const char* k) {
    key(k);
    std::fprintf(out_, "[");
    first_ = true;
  }
  void close_list() {
    std::fprintf(out_, "]");
    first_ = false;
  }
  FILE* out_;
  bool first_ = true;
};

using PerChannel = std::function<double(unsigned int, unsigned int)>;

void channels(Json& j, SpectralGrid& grid, const char* name, const PerChannel& f) {
  std::vector<double> v;
  for (unsigned int s = 0; s < grid.getNumSpectralWindow(); s++)
    for (unsigned int c = 0; c < grid.getNumChan(s); c++) v.push_back(f(s, c));
  j.array(name, v);
}

void windows(Json& j, SpectralGrid& grid, const char* name, const std::function<double(unsigned int)>& f) {
  std::vector<double> v;
  for (unsigned int s = 0; s < grid.getNumSpectralWindow(); s++) v.push_back(f(s));
  j.array(name, v);
}

const double kWater = 1.3e-3;  // a water column other than the profile's, m

void write_rip_channels(Json& j, RefractiveIndexProfile& rip, SpectralGrid& grid) {
  Length g = rip.getGroundWH2O(), w(kWater, Length::UnitMeter);
  auto op = [](Opacity o) { return o.get(Opacity::UnitNeper); };
  auto deg = [](Angle a) { return a.get(Angle::UnitDegree); };
  auto m = [](Length l) { return l.get(Length::UnitMeter); };
  channels(j, grid, "dry", [&](unsigned s, unsigned c) { return op(rip.getDryOpacity(s, c)); });
  channels(j, grid, "dry_continuum", [&](unsigned s, unsigned c) { return op(rip.getDryContOpacity(s, c)); });
  channels(j, grid, "o2", [&](unsigned s, unsigned c) { return op(rip.getO2LinesOpacity(s, c)); });
  channels(j, grid, "o2_ground", [&](unsigned s, unsigned c) { return op(rip.get16O16OLinesOpacity(s, c)); });
  channels(j, grid, "o2_vibrational", [&](unsigned s, unsigned c) { return op(rip.get16O16OVIBLinesOpacity(s, c)); });
  channels(j, grid, "o2_isotope18", [&](unsigned s, unsigned c) { return op(rip.get16O18OLinesOpacity(s, c)); });
  channels(j, grid, "o2_isotope17", [&](unsigned s, unsigned c) { return op(rip.get16O17OLinesOpacity(s, c)); });
  channels(j, grid, "o3", [&](unsigned s, unsigned c) { return op(rip.getO3LinesOpacity(s, c)); });
  channels(j, grid, "o3_ground", [&](unsigned s, unsigned c) { return op(rip.get16O16O16OLinesOpacity(s, c)); });
  channels(j, grid, "o3_nu1", [&](unsigned s, unsigned c) { return op(rip.get16O16O16OV1LinesOpacity(s, c)); });
  channels(j, grid, "o3_nu2", [&](unsigned s, unsigned c) { return op(rip.get16O16O16OV2LinesOpacity(s, c)); });
  channels(j, grid, "o3_nu3", [&](unsigned s, unsigned c) { return op(rip.get16O16O16OV3LinesOpacity(s, c)); });
  channels(j, grid, "o3_668", [&](unsigned s, unsigned c) { return op(rip.get16O16O18OLinesOpacity(s, c)); });
  channels(j, grid, "o3_667", [&](unsigned s, unsigned c) { return op(rip.get16O16O17OLinesOpacity(s, c)); });
  channels(j, grid, "o3_686", [&](unsigned s, unsigned c) { return op(rip.get16O18O16OLinesOpacity(s, c)); });
  channels(j, grid, "o3_676", [&](unsigned s, unsigned c) { return op(rip.get16O17O16OLinesOpacity(s, c)); });
  channels(j, grid, "co", [&](unsigned s, unsigned c) { return op(rip.getCOLinesOpacity(s, c)); });
  channels(j, grid, "n2o", [&](unsigned s, unsigned c) { return op(rip.getN2OLinesOpacity(s, c)); });
  channels(j, grid, "no2", [&](unsigned s, unsigned c) { return op(rip.getNO2LinesOpacity(s, c)); });
  channels(j, grid, "so2", [&](unsigned s, unsigned c) { return op(rip.getSO2LinesOpacity(s, c)); });
  channels(j, grid, "hcl", [&](unsigned s, unsigned c) { return op(rip.getHClLinesOpacity(s, c)); });
  channels(j, grid, "hcn", [&](unsigned s, unsigned c) { return op(rip.getHCNLinesOpacity(s, c)); });
  channels(j, grid, "wet_ground", [&](unsigned s, unsigned c) { return op(rip.getWetOpacity(g, s, c)); });
  channels(j, grid, "h2o_lines_ground", [&](unsigned s, unsigned c) { return op(rip.getH2OLinesOpacity(g, s, c)); });
  channels(j, grid, "h2o_continuum_ground", [&](unsigned s, unsigned c) { return op(rip.getH2OContOpacity(g, s, c)); });
  channels(j, grid, "h2o_ground_state_ground", [&](unsigned s, unsigned c) { return op(rip.getHH16OLinesOpacity(g, s, c)); });
  channels(j, grid, "wet", [&](unsigned s, unsigned c) { return op(rip.getWetOpacity(w, s, c)); });
  channels(j, grid, "h2o_lines", [&](unsigned s, unsigned c) { return op(rip.getH2OLinesOpacity(w, s, c)); });
  channels(j, grid, "h2o_continuum", [&](unsigned s, unsigned c) { return op(rip.getH2OContOpacity(w, s, c)); });
  channels(j, grid, "dispersive_h2o_phase", [&](unsigned s, unsigned c) { return deg(rip.getDispersiveH2OPhaseDelay(w, s, c)); });
  channels(j, grid, "dispersive_h2o_path", [&](unsigned s, unsigned c) { return m(rip.getDispersiveH2OPathLength(w, s, c)); });
  channels(j, grid, "non_dispersive_h2o_phase", [&](unsigned s, unsigned c) { return deg(rip.getNonDispersiveH2OPhaseDelay(w, s, c)); });
  channels(j, grid, "non_dispersive_h2o_path", [&](unsigned s, unsigned c) { return m(rip.getNonDispersiveH2OPathLength(w, s, c)); });
  channels(j, grid, "dispersive_dry_phase", [&](unsigned s, unsigned c) { return deg(rip.getDispersiveDryPhaseDelay(s, c)); });
  channels(j, grid, "dispersive_dry_path", [&](unsigned s, unsigned c) { return m(rip.getDispersiveDryPathLength(s, c)); });
  channels(j, grid, "non_dispersive_dry_phase", [&](unsigned s, unsigned c) { return deg(rip.getNonDispersiveDryPhaseDelay(s, c)); });
  channels(j, grid, "non_dispersive_dry_path", [&](unsigned s, unsigned c) { return m(rip.getNonDispersiveDryPathLength(s, c)); });
  channels(j, grid, "o2_phase", [&](unsigned s, unsigned c) { return deg(rip.getO2LinesPhaseDelay(s, c)); });
  channels(j, grid, "o2_path", [&](unsigned s, unsigned c) { return m(rip.getO2LinesPathLength(s, c)); });
  channels(j, grid, "o3_phase", [&](unsigned s, unsigned c) { return deg(rip.getO3LinesPhaseDelay(s, c)); });
  channels(j, grid, "o3_path", [&](unsigned s, unsigned c) { return m(rip.getO3LinesPathLength(s, c)); });
  channels(j, grid, "co_phase", [&](unsigned s, unsigned c) { return deg(rip.getCOLinesPhaseDelay(s, c)); });
  channels(j, grid, "co_path", [&](unsigned s, unsigned c) { return m(rip.getCOLinesPathLength(s, c)); });
  channels(j, grid, "n2o_phase", [&](unsigned s, unsigned c) { return deg(rip.getN2OLinesPhaseDelay(s, c)); });
  channels(j, grid, "n2o_path", [&](unsigned s, unsigned c) { return m(rip.getN2OLinesPathLength(s, c)); });
  channels(j, grid, "no2_phase", [&](unsigned s, unsigned c) { return deg(rip.getNO2LinesPhaseDelay(s, c)); });
  channels(j, grid, "so2_phase", [&](unsigned s, unsigned c) { return deg(rip.getSO2LinesPhaseDelay(s, c)); });
  channels(j, grid, "hcl_phase", [&](unsigned s, unsigned c) { return deg(rip.getHClLinesPhaseDelay(s, c)); });
  channels(j, grid, "hcn_phase", [&](unsigned s, unsigned c) { return deg(rip.getHCNLinesPhaseDelay(s, c)); });
}

void write_rip_windows(Json& j, RefractiveIndexProfile& rip, SpectralGrid& grid) {
  Length g = rip.getGroundWH2O(), w(kWater, Length::UnitMeter);
  auto op = [](Opacity o) { return o.get(Opacity::UnitNeper); };
  auto deg = [](Angle a) { return a.get(Angle::UnitDegree); };
  auto m = [](Length l) { return l.get(Length::UnitMeter); };
  windows(j, grid, "average_dry", [&](unsigned s) { return op(rip.getAverageDryOpacity(s)); });
  windows(j, grid, "average_dry_continuum", [&](unsigned s) { return op(rip.getAverageDryContOpacity(s)); });
  windows(j, grid, "average_o2", [&](unsigned s) { return op(rip.getAverageO2LinesOpacity(s)); });
  windows(j, grid, "average_o3", [&](unsigned s) { return op(rip.getAverageO3LinesOpacity(s)); });
  windows(j, grid, "average_co", [&](unsigned s) { return op(rip.getAverageCOLinesOpacity(s)); });
  windows(j, grid, "average_wet", [&](unsigned s) { return op(rip.getAverageWetOpacity(w, s)); });
  windows(j, grid, "average_h2o_lines", [&](unsigned s) { return op(rip.getAverageH2OLinesOpacity(w, s)); });
  windows(j, grid, "average_h2o_continuum", [&](unsigned s) { return op(rip.getAverageH2OContOpacity(w, s)); });
  windows(j, grid, "average_dispersive_h2o_phase", [&](unsigned s) { return deg(rip.getAverageDispersiveH2OPhaseDelay(w, s)); });
  windows(j, grid, "average_dispersive_h2o_path", [&](unsigned s) { return m(rip.getAverageDispersiveH2OPathLength(w, s)); });
  windows(j, grid, "average_non_dispersive_h2o_path", [&](unsigned s) { return m(rip.getAverageNonDispersiveH2OPathLength(w, s)); });
  windows(j, grid, "average_dispersive_dry_phase", [&](unsigned s) { return deg(rip.getAverageDispersiveDryPhaseDelay(s)); });
  windows(j, grid, "average_dispersive_dry_path", [&](unsigned s) { return m(rip.getAverageDispersiveDryPathLength(s)); });
  windows(j, grid, "average_non_dispersive_dry_phase", [&](unsigned s) { return deg(rip.getAverageNonDispersiveDryPhaseDelay(s)); });
  windows(j, grid, "average_non_dispersive_dry_path", [&](unsigned s) { return m(rip.getAverageNonDispersiveDryPathLength(s)); });
  // ATM's RefractiveIndexProfile average passes the channel index where a
  // Length is expected; recorded to document the defect.
  windows(j, grid, "rip_average_non_dispersive_h2o_phase", [&](unsigned s) { return deg(rip.getAverageNonDispersiveH2OPhaseDelay(w, s)); });
  (void)g;
}

// Per-layer absorption coefficients: the dry and wet totals (which the
// radiative transfer uses) for every second channel, the species for every
// sixth; `layer_channels_*` list the flat channel indices included.
void write_layers(Json& j, RefractiveIndexProfile& rip, SpectralGrid& grid) {
  const char* names[] = {"layer_total_dry", "layer_total_wet", "layer_o2",  "layer_dry_continuum", "layer_o3",
                         "layer_co",        "layer_n2o",       "layer_h2o", "layer_h2o_continuum"};
  std::vector<double> totals_channels, species_channels;
  for (unsigned int flat = 0; flat < grid.v_chanFreq_.size(); flat++) {
    if (flat % 2 == 0) totals_channels.push_back(flat);
    if (flat % 6 == 0) species_channels.push_back(flat);
  }
  j.array("layer_channels_totals", totals_channels);
  j.array("layer_channels_species", species_channels);
  for (unsigned int k = 0; k < 9; k++) {
    std::vector<double> v;
    unsigned int stride = k < 2 ? 2 : 6;
    unsigned int flat = 0;
    for (unsigned int s = 0; s < grid.getNumSpectralWindow(); s++)
      for (unsigned int c = 0; c < grid.getNumChan(s); c++, flat++)
        for (unsigned int l = 0; l < rip.getNumLayer() && flat % stride == 0; l++) {
          InverseLength a = k == 0 ? rip.getAbsTotalDry(s, c, l)
                          : k == 1 ? rip.getAbsTotalWet(s, c, l)
                          : k == 2 ? rip.getAbsO2Lines(s, c, l)
                          : k == 3 ? rip.getAbsDryCont(s, c, l)
                          : k == 4 ? rip.getAbsO3Lines(s, c, l)
                          : k == 5 ? rip.getAbsCOLines(s, c, l)
                          : k == 6 ? rip.getAbsN2OLines(s, c, l)
                          : k == 7 ? rip.getAbsH2OLines(s, c, l)
                                   : rip.getAbsH2OCont(s, c, l);
          v.push_back(a.get(InverseLength::UnitInverseMeter));
        }
    j.array(names[k], v);
  }
}

void write_sky_channels(Json& j, SkyStatus& sky, SpectralGrid& grid) {
  auto op = [](Opacity o) { return o.get(Opacity::UnitNeper); };
  auto deg = [](Angle a) { return a.get(Angle::UnitDegree); };
  auto m = [](Length l) { return l.get(Length::UnitMeter); };
  auto k = [](Temperature t) { return t.get(Temperature::UnitKelvin); };
  auto flat = [&](unsigned s, unsigned c) { return grid.v_transfertId_[s] + c; };
  channels(j, grid, "sky_wet", [&](unsigned s, unsigned c) { return op(sky.getWetOpacity(s, c)); });
  channels(j, grid, "sky_h2o_lines", [&](unsigned s, unsigned c) { return op(sky.getH2OLinesOpacity(flat(s, c))); });
  channels(j, grid, "sky_h2o_continuum", [&](unsigned s, unsigned c) { return op(sky.getH2OContOpacity(flat(s, c))); });
  channels(j, grid, "sky_h2o_ground_state", [&](unsigned s, unsigned c) { return op(sky.getHH16OLinesOpacity(flat(s, c))); });
  channels(j, grid, "sky_h2o_bending", [&](unsigned s, unsigned c) { return op(sky.getHH16OV2LinesOpacity(flat(s, c))); });
  channels(j, grid, "sky_h2o_isotope17", [&](unsigned s, unsigned c) { return op(sky.getHH17OLinesOpacity(flat(s, c))); });
  channels(j, grid, "sky_h2o_isotope18", [&](unsigned s, unsigned c) { return op(sky.getHH18OLinesOpacity(flat(s, c))); });
  channels(j, grid, "sky_hdo", [&](unsigned s, unsigned c) { return op(sky.getHDOLinesOpacity(flat(s, c))); });
  channels(j, grid, "sky_dispersive_phase", [&](unsigned s, unsigned c) { return deg(sky.getDispersiveH2OPhaseDelay(s, c)); });
  channels(j, grid, "sky_dispersive_path", [&](unsigned s, unsigned c) { return m(sky.getDispersiveH2OPathLength(s, c)); });
  channels(j, grid, "sky_non_dispersive_phase", [&](unsigned s, unsigned c) { return deg(sky.getNonDispersiveH2OPhaseDelay(s, c)); });
  channels(j, grid, "sky_non_dispersive_path", [&](unsigned s, unsigned c) { return m(sky.getNonDispersiveH2OPathLength(s, c)); });
  channels(j, grid, "tebb", [&](unsigned s, unsigned c) { return k(sky.getTebbSky(s, c)); });
  channels(j, grid, "trj", [&](unsigned s, unsigned c) { return k(sky.getTrjSky(s, c)); });
  windows(j, grid, "average_tebb", [&](unsigned s) { return k(sky.getAverageTebbSky(s)); });
  windows(j, grid, "average_trj", [&](unsigned s) { return k(sky.getAverageTrjSky(s)); });
  windows(j, grid, "sky_average_dispersive_phase", [&](unsigned s) { return deg(sky.getAverageDispersiveH2OPhaseDelay(s)); });
  windows(j, grid, "sky_average_non_dispersive_phase", [&](unsigned s) { return deg(sky.getAverageNonDispersiveH2OPhaseDelay(s)); });
  windows(j, grid, "sky_average_dispersive_path", [&](unsigned s) { return m(sky.getAverageDispersiveH2OPathLength(s)); });
  windows(j, grid, "sky_average_non_dispersive_path", [&](unsigned s) { return m(sky.getAverageNonDispersiveH2OPathLength(s)); });
  windows(j, grid, "sky_average_wet", [&](unsigned s) { return op(sky.getAverageWetOpacity(s)); });
}

void write_absorption(const std::string& dir) {
  FILE* out = std::fopen((dir + "/absorption.json").c_str(), "w");
  Json j(out);
  std::fprintf(out, "{");
  j.open_list("windows");
  for (const WindowSpec& s : window_specs()) {
    j.open_object();
    j.number("channels", s.channels);
    j.number("ref_chan", s.ref_chan);
    j.number("ref_hz", s.ref_hz);
    j.number("sep_hz", s.sep_hz);
    j.close();
  }
  j.close_list();
  j.number("water_column_m", kWater);
  j.open_list("profiles");
  for (const SkyProfile& k : kProfiles) {
    AtmProfile profile = make_profile(k);
    SpectralGrid* grid = make_grid();
    RefractiveIndexProfile rip(*grid, profile);
    SkyStatus sky(rip);
    sky.setUserWH2O(sky.getGroundWH2O());  // as the `at` tool does
    j.open_object();
    j.number("altitude_m", k.altitude_m);
    j.number("ground_pressure_hpa", k.pressure_hpa);
    j.number("ground_temperature_k", k.temperature_k);
    j.number("lapse_rate_k_per_km", k.lapse_rate);
    j.number("relative_humidity_percent", k.humidity_percent);
    j.number("water_vapour_scale_height_km", k.scale_height_km);
    j.number("pressure_step_hpa", k.step_hpa);
    j.number("top_altitude_km", k.top_km);
    j.number("atmosphere", k.type);
    j.number("layers", rip.getNumLayer());
    j.number("ground_water_column_m", rip.getGroundWH2O().get(Length::UnitMeter));
    write_rip_channels(j, rip, *grid);
    write_rip_windows(j, rip, *grid);
    write_layers(j, rip, *grid);
    write_sky_channels(j, sky, *grid);
    j.close();
    delete grid;
  }
  j.close_list();
  std::fprintf(out, "}\n");
  std::fclose(out);
}

// Tebb and Trj over airmass, water column, coupling and spillover for
// every third channel, and a SkyStatus call sequence.
void write_sky(const std::string& dir) {
  FILE* out = std::fopen((dir + "/sky.json").c_str(), "w");
  Json j(out);
  std::fprintf(out, "{");
  const double airmasses[] = {1.0, 1.5, 2.2, 3.0};
  const double waters_mm[] = {0.1, 0.5, 1.0, 2.5, 5.0};
  const double couplings[][2] = {{1.0, 100.0}, {0.95, 270.0}, {0.9, 290.0}};
  j.open_list("sweeps");
  for (unsigned int p = 0; p < 2; p++) {
    AtmProfile profile = make_profile(kProfiles[p]);
    SpectralGrid* grid = make_grid();
    RefractiveIndexProfile rip(*grid, profile);
    SkyStatus sky(rip);
    std::vector<double> rows;  // flat, airmass, water m, coupling, spill, tebb, trj
    unsigned int flat = 0;
    for (unsigned int s = 0; s < grid->getNumSpectralWindow(); s++)
      for (unsigned int c = 0; c < grid->getNumChan(s); c++, flat++) {
        if (flat % 3) continue;
        for (double a : airmasses)
          for (double wmm : waters_mm)
            for (const auto& cs : couplings) {
              Length w(wmm, Length::UnitMilliMeter);
              Temperature spill(cs[1], Temperature::UnitKelvin);
              rows.insert(rows.end(), {double(flat), a, w.get(Length::UnitMeter), cs[0], cs[1],
                                       sky.getTebbSky(s, c, w, a, cs[0], spill).get(Temperature::UnitKelvin),
                                       sky.getTrjSky(s, c, w, a, cs[0], spill).get(Temperature::UnitKelvin)});
            }
      }
    j.open_object();
    j.number("profile", p);
    j.array("rows", rows);
    j.close();
    delete grid;
  }
  j.close_list();

  // The call sequence: SkyStatus state set by setters, read by the
  // defaulted overloads, on the CalCorruptor profile.
  AtmProfile profile = make_profile(kProfiles[0]);
  SpectralGrid* grid = make_grid();
  RefractiveIndexProfile rip(*grid, profile);
  SkyStatus sky(rip);
  auto k = [](Temperature t) { return t.get(Temperature::UnitKelvin); };
  std::vector<double> steps;  // step, window, channel, value
  auto record = [&](double step, unsigned s, unsigned c, double v) { steps.insert(steps.end(), {step, double(s), double(c), v}); };
  j.number("initial_user_water_m", sky.getUserWH2O().get(Length::UnitMeter));
  const unsigned int c0 = 0;
  for (unsigned int s = 0; s < grid->getNumSpectralWindow(); s += 3) {
    record(0, s, c0, k(sky.getTebbSky(s, c0)));  // 1 mm, airmass 1
  }
  sky.setUserWH2O(Length(2.0, Length::UnitMilliMeter));
  sky.setAirMass(1.7);
  sky.setSkyBackgroundTemperature(Temperature(5.0, Temperature::UnitKelvin));
  for (unsigned int s = 0; s < grid->getNumSpectralWindow(); s += 3) {
    record(1, s, c0, k(sky.getTebbSky(s, c0)));                                         // state
    record(2, s, c0, k(sky.getTebbSky(s, c0, Length(1.0, Length::UnitMilliMeter))));  // explicit water
    record(3, s, c0, k(sky.getTebbSky(s, c0)));                                         // state unchanged
    record(4, s, c0, k(sky.getTrjSky(s, c0)));
    record(5, s, c0, k(sky.getAverageTebbSky(s)));  // spill = ground T
    record(6, s, c0, k(sky.getAverageTrjSky(s)));
    record(7, s, c0, sky.getWetOpacity(s, c0).get(Opacity::UnitNeper));
    record(8, s, c0, sky.getDispersiveH2OPhaseDelay(s, c0).get(Angle::UnitDegree));
    record(9, s, c0, k(sky.getTebbSky(s, c0, 0.93, Temperature(280.0, Temperature::UnitKelvin))));
  }
  j.array("sequence", steps);
  delete grid;
  std::fprintf(out, "}\n");
  std::fclose(out);
}

}  // namespace

void write_sky_fixtures(const std::string& dir) {
  write_absorption(dir);
  write_sky(dir);
}

// Times building the RefractiveIndexProfile (and SkyStatus) for the
// CalCorruptor profile and `channels` channels over 2 GHz at 345 GHz.
int time_profile(unsigned int channels) {
  AtmProfile profile = make_profile(kProfiles[0]);
  SpectralGrid grid(channels, channels / 2, Frequency(345.0e9, Frequency::UnitHertz),
                    Frequency(2.0e9 / channels, Frequency::UnitHertz));
  auto start = std::chrono::steady_clock::now();
  RefractiveIndexProfile rip(grid, profile);
  double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  std::printf("c++ RefractiveIndexProfile: %u channels x %u layers: %.3f s (%.1f us per channel) [%g]\n", channels,
              rip.getNumLayer(), seconds, 1e6 * seconds / channels,
              rip.getDryOpacity(0, channels / 2).get(Opacity::UnitNeper));
  return 0;
}
