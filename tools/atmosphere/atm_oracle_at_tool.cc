// SPDX-License-Identifier: LGPL-3.0-or-later
// The `--at-tool` mode of the ATM oracle: the quantities
// tools/atmosphere/at_tool_check.py reads from CASA's `at` tool, computed by
// the compiled ATM through the calls the tool makes
// (casatools/src/tools/atmosphere/atmosphere_cmpt.cc), in the order the
// check makes them. Linked into atm_oracle by build-atm-oracle.sh.
//
// The check sends its configuration on stdin as numbers:
//
//   <profiles>
//   altitude_m temperature_k pressure_mbar top_km humidity_percent
//       lapse_k_per_km step_mbar scale_height_km type      (one line each)
//   <bands>
//   channels centre_ghz res_ghz                           (one line each)
//   <sets>
//   water_m airmass                                       (one line each)
//
// and reads back the JSON casa-atmosphere's `at_tool_quantities` example
// writes, every value with 17 significant digits.

#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ATMProfile.h"
#include "ATMRefractiveIndexProfile.h"
#include "ATMSkyStatus.h"
#include "ATMSpectralGrid.h"

using namespace atm;

namespace {

struct Profile {
  double altitude_m, temperature_k, pressure_mbar, top_km, humidity_percent;
  double lapse_k_per_km, step_mbar, scale_height_km;
  unsigned int type;
};

struct Band {
  unsigned int channels;
  double centre_ghz, res_ghz;
};

struct Set {
  double water_m, airmass;
};

bool read_config(std::vector<Profile>& profiles, std::vector<Band>& bands, std::vector<Set>& sets) {
  std::size_t n = 0;
  if (!(std::cin >> n)) return false;
  profiles.resize(n);
  for (Profile& p : profiles) {
    if (!(std::cin >> p.altitude_m >> p.temperature_k >> p.pressure_mbar >> p.top_km >> p.humidity_percent >>
          p.lapse_k_per_km >> p.step_mbar >> p.scale_height_km >> p.type))
      return false;
  }
  if (!(std::cin >> n)) return false;
  bands.resize(n);
  for (Band& b : bands) {
    if (!(std::cin >> b.channels >> b.centre_ghz >> b.res_ghz)) return false;
  }
  if (!(std::cin >> n)) return false;
  sets.resize(n);
  for (Set& s : sets) {
    if (!(std::cin >> s.water_m >> s.airmass)) return false;
  }
  return true;
}

// `initAtmProfile`, its quantities converted to the units it converts to.
AtmProfile make_profile(const Profile& p) {
  return AtmProfile(Length(p.altitude_m, Length::UnitMeter), Pressure(p.pressure_mbar, Pressure::UnitMilliBar),
                    Temperature(p.temperature_k, Temperature::UnitKelvin), p.lapse_k_per_km,
                    Humidity(p.humidity_percent, Percent::UnitPercent),
                    Length(p.scale_height_km, Length::UnitKiloMeter), Pressure(p.step_mbar, Pressure::UnitMilliBar),
                    1.2, Length(p.top_km * 1000.0, Length::UnitMeter), p.type);
}

// `initSpectralWindow`: reference channel (n-1)/2, and for an even count
// the reference frequency half a channel below the centre.
std::unique_ptr<SpectralGrid> make_grid(const std::vector<Band>& bands) {
  std::unique_ptr<SpectralGrid> grid;
  for (const Band& b : bands) {
    Frequency reference(b.centre_ghz, "GHz"), separation(b.res_ghz, "GHz");
    if (b.channels % 2 == 0) reference = reference - separation * 0.5;
    if (!grid)
      grid = std::make_unique<SpectralGrid>(b.channels, (b.channels - 1) / 2, reference, separation);
    else
      grid->add(b.channels, (b.channels - 1) / 2, reference, separation);
  }
  return grid;
}

using Getter = std::function<double(unsigned int, unsigned int)>;

void write_profile(const Profile& p, const std::vector<Band>& bands, const std::vector<Set>& sets) {
  AtmProfile profile = make_profile(p);
  std::unique_ptr<SpectralGrid> owned = make_grid(bands);
  SpectralGrid& grid = *owned;
  RefractiveIndexProfile rip(grid, profile);
  SkyStatus sky(rip);
  sky.setUserWH2O(sky.getGroundWH2O());  // as initSpectralWindow does
  const Length one_mm(1.0, "mm");
  const std::vector<std::pair<const char*, Getter>> channel_quantities = {
      {"freq_hz", [&](unsigned s, unsigned c) { return grid.getChanFreq(s, c).get(Frequency::UnitHertz); }},
      {"dry", [&](unsigned s, unsigned c) { return rip.getDryOpacity(s, c).get(Opacity::UnitNeper); }},
      {"dry_cont", [&](unsigned s, unsigned c) { return rip.getDryContOpacity(s, c).get(Opacity::UnitNeper); }},
      {"o2", [&](unsigned s, unsigned c) { return rip.getO2LinesOpacity(s, c).get(Opacity::UnitNeper); }},
      {"o3", [&](unsigned s, unsigned c) { return rip.getO3LinesOpacity(s, c).get(Opacity::UnitNeper); }},
      {"co", [&](unsigned s, unsigned c) { return rip.getCOLinesOpacity(s, c).get(Opacity::UnitNeper); }},
      {"n2o", [&](unsigned s, unsigned c) { return rip.getN2OLinesOpacity(s, c).get(Opacity::UnitNeper); }},
      {"wet", [&](unsigned s, unsigned c) { return sky.getWetOpacity(s, c).get(Opacity::UnitNeper); }},
      {"h2o_lines", [&](unsigned s, unsigned c) { return sky.getH2OLinesOpacity(s, c).get(Opacity::UnitNeper); }},
      {"h2o_cont", [&](unsigned s, unsigned c) { return sky.getH2OContOpacity(s, c).get(Opacity::UnitNeper); }},
      {"tebb", [&](unsigned s, unsigned c) { return sky.getTebbSky(s, c).get(Temperature::UnitKelvin); }},
      {"tebb_1mm", [&](unsigned s, unsigned c) { return sky.getTebbSky(s, c, one_mm).get(Temperature::UnitKelvin); }},
      {"trj", [&](unsigned s, unsigned c) { return sky.getTrjSky(s, c).get(Temperature::UnitKelvin); }},
      {"disp_phase",
       [&](unsigned s, unsigned c) { return sky.getDispersiveH2OPhaseDelay(s, c).get(Angle::UnitDegree); }},
      {"nondisp_phase",
       [&](unsigned s, unsigned c) { return sky.getNonDispersiveH2OPhaseDelay(s, c).get(Angle::UnitDegree); }},
      {"nondisp_dry_phase",
       [&](unsigned s, unsigned c) { return rip.getNonDispersiveDryPhaseDelay(s, c).get(Angle::UnitDegree); }},
      {"disp_path",
       [&](unsigned s, unsigned c) { return sky.getDispersiveH2OPathLength(s, c).get(Length::UnitMeter); }},
      {"nondisp_path",
       [&](unsigned s, unsigned c) { return sky.getNonDispersiveH2OPathLength(s, c).get(Length::UnitMeter); }},
      {"nondisp_dry_path",
       [&](unsigned s, unsigned c) { return rip.getNonDispersiveDryPathLength(s, c).get(Length::UnitMeter); }},
      {"o2_path", [&](unsigned s, unsigned c) { return rip.getO2LinesPathLength(s, c).get(Length::UnitMeter); }},
      {"o3_path", [&](unsigned s, unsigned c) { return rip.getO3LinesPathLength(s, c).get(Length::UnitMeter); }},
      {"co_path", [&](unsigned s, unsigned c) { return rip.getCOLinesPathLength(s, c).get(Length::UnitMeter); }},
      {"n2o_path", [&](unsigned s, unsigned c) { return rip.getN2OLinesPathLength(s, c).get(Length::UnitMeter); }},
  };
  const std::vector<std::pair<const char*, Getter>> set_quantities = {
      {"set_tebb", [&](unsigned s, unsigned c) { return sky.getTebbSky(s, c).get(Temperature::UnitKelvin); }},
      {"set_wet", [&](unsigned s, unsigned c) { return sky.getWetOpacity(s, c).get(Opacity::UnitNeper); }},
      {"set_disp_phase",
       [&](unsigned s, unsigned c) { return sky.getDispersiveH2OPhaseDelay(s, c).get(Angle::UnitDegree); }},
      {"set_nondisp_phase",
       [&](unsigned s, unsigned c) { return sky.getNonDispersiveH2OPhaseDelay(s, c).get(Angle::UnitDegree); }},
  };
  const unsigned int windows = grid.getNumSpectralWindow();
  // Every window's quantities at the profile's own water column first, then
  // every window's at each set, as the check calls the tool.
  std::vector<std::string> defaults(windows);
  for (unsigned int s = 0; s < windows; s++) {
    std::string text;
    for (const auto& [key, f] : channel_quantities) {
      text += std::string("\"") + key + "\":[";
      for (unsigned int c = 0; c < grid.getNumChan(s); c++) {
        char value[32];
        std::snprintf(value, sizeof value, c ? ",%.17g" : "%.17g", f(s, c));
        text += value;
      }
      text += "],";
    }
    char averages[96];
    std::snprintf(averages, sizeof averages, "\"average_tebb\":%.17g,\"average_trj\":%.17g",
                  sky.getAverageTebbSky(s).get(Temperature::UnitKelvin),
                  sky.getAverageTrjSky(s).get(Temperature::UnitKelvin));
    defaults[s] = text + averages;
  }
  // per_set[s][q][k]: window s, set quantity q, set k.
  std::vector<std::vector<std::vector<std::string>>> per_set(
      windows, std::vector<std::vector<std::string>>(set_quantities.size()));
  for (const Set& set : sets) {
    sky.setUserWH2O(Length(set.water_m * 1e3, "mm"));
    sky.setAirMass(set.airmass);
    for (unsigned int s = 0; s < windows; s++) {
      for (std::size_t q = 0; q < set_quantities.size(); q++) {
        std::string text = "[";
        for (unsigned int c = 0; c < grid.getNumChan(s); c++) {
          char value[32];
          std::snprintf(value, sizeof value, c ? ",%.17g" : "%.17g", set_quantities[q].second(s, c));
          text += value;
        }
        per_set[s][q].push_back(text + "]");
      }
    }
  }
  std::printf("{\"ground_water_m\":%.17g,\"layers\":%u,\"windows\":[",
              profile.getGroundWH2O().get(Length::UnitMeter), profile.getNumLayer());
  for (unsigned int s = 0; s < windows; s++) {
    std::printf("%s{%s", s ? "," : "", defaults[s].c_str());
    for (std::size_t q = 0; q < set_quantities.size(); q++) {
      std::printf(",\"%s\":[", set_quantities[q].first);
      for (std::size_t k = 0; k < per_set[s][q].size(); k++)
        std::printf("%s%s", k ? "," : "", per_set[s][q][k].c_str());
      std::printf("]");
    }
    std::printf("}");
  }
  std::printf("]}");
}

}  // namespace

int at_tool_quantities() {
  std::vector<Profile> profiles;
  std::vector<Band> bands;
  std::vector<Set> sets;
  if (!read_config(profiles, bands, sets) || bands.empty()) {
    std::fprintf(stderr, "atm_oracle --at-tool: unreadable configuration on stdin\n");
    return 2;
  }
  std::printf("{\"profiles\":[");
  for (std::size_t i = 0; i < profiles.size(); i++) {
    if (i) std::printf(",");
    write_profile(profiles[i], bands, sets);
  }
  std::printf("]}\n");
  return 0;
}
