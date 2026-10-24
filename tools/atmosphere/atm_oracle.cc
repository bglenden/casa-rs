// SPDX-License-Identifier: LGPL-3.0-or-later
// The C++ oracle for casa-atmosphere's tests: it links CASA's ATM sources and
// writes what they compute for a fixed set of inputs. Build and run it with
// tools/atmosphere/build-atm-oracle.sh, which writes the fixtures into
// crates/casa-atmosphere/tests/data/.
//
// Every input is printed with 17 significant digits, so the Rust tests read
// back exactly the doubles the C++ used.
//
//   profiles.json            AtmProfile layers for every atmosphere type
//   refractivity.json        inputs of the specific-refractivity grid
//   refractivity.bin         its values: little-endian f64, (re, im) for
//                            condition, species 1..25, frequency, innermost last
//   layer_refractivity.json  ATM's per-unit-path layer refractivities
//
// With `--time <channels>` it instead times the species refractivities for
// a Chajnantor profile, to compare with casa-atmosphere's
// `refractivity_timing` example.

#include <chrono>
#include <cmath>
#include <complex>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

// The oracle reads ATM's internals (the layer vectors, the species functions).
#define private public
#define protected public
#include "ATMProfile.h"
#include "ATMRefractiveIndex.h"
#undef private
#undef protected

using namespace atm;

namespace {

struct ProfileCase {
  const char* name;
  double altitude_m, pressure_hpa, temperature_k, lapse_rate, humidity_percent;
  double scale_height_km, step_hpa, top_km;
  unsigned int type;
  std::vector<std::pair<double, double>> levels;  // (altitude m, temperature K)
};

void print_array(FILE* out, const char* name, const std::vector<double>& values, bool last = false) {
  std::fprintf(out, "\"%s\":[", name);
  for (size_t i = 0; i < values.size(); i++) std::fprintf(out, "%s%.17g", i ? "," : "", values[i]);
  std::fprintf(out, "]%s", last ? "" : ",");
}

std::vector<ProfileCase> profile_cases() {
  std::vector<ProfileCase> cases;
  const unsigned int types[] = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14};
  for (unsigned int type : types) {
    cases.push_back({"sea level", 0.0, 1013.0, 288.0, -6.5, 50.0, 2.0, 10.0, 48.0, type, {}});
    cases.push_back({"chajnantor", 5000.0, 560.0, 270.0, -5.6, 20.0, 2.0, 10.0, 48.0, type, {}});
  }
  cases.push_back({"tropical 500 m", 500.0, 955.0, 295.0, -6.0, 70.0, 1.5, 5.0, 60.0, 1, {}});
  cases.push_back({"vla", 2124.0, 785.0, 283.0, -6.5, 30.0, 2.5, 12.5, 40.0, 1, {}});
  cases.push_back({"vla midlatitude summer", 2124.0, 790.0, 300.0, -6.5, 90.0, 2.2, 7.5, 48.0, 2, {}});
  cases.push_back({"step out of range", 5000.0, 560.0, 270.0, -5.6, 20.0, 2.0, 20.0, 48.0, 1, {}});
  cases.push_back({"step below range", 3000.0, 700.0, 275.0, -6.0, 40.0, 1.8, 3.0, 45.0, 3, {}});
  cases.push_back({"humid sea level", 0.0, 1010.0, 303.0, -6.5, 95.0, 2.0, 10.0, 48.0, 1, {}});
  cases.push_back({"top 100 km tropical", 5000.0, 560.0, 270.0, -5.6, 20.0, 2.0, 10.0, 100.0, 1, {}});
  cases.push_back({"top 100 km us standard", 1000.0, 900.0, 281.0, -6.5, 60.0, 2.0, 10.0, 100.0, 6, {}});
  cases.push_back({"user temperatures", 5000.0, 560.0, 270.0, -5.6, 20.0, 2.0, 10.0, 48.0, 1,
                   {{5050.0, 268.5}, {5300.0, 267.0}, {6100.0, 262.0}, {8000.0, 250.0}, {12000.0, 226.0},
                    {20000.0, 205.0}, {30000.0, 230.0}}});
  return cases;
}

void write_profiles(const std::string& dir) {
  FILE* out = std::fopen((dir + "/profiles.json").c_str(), "w");
  std::fprintf(out, "[\n");
  std::vector<ProfileCase> cases = profile_cases();
  for (size_t c = 0; c < cases.size(); c++) {
    const ProfileCase& k = cases[c];
    Length altitude(k.altitude_m, Length::UnitMeter);
    Pressure pressure(k.pressure_hpa, Pressure::UnitMilliBar);
    Temperature temperature(k.temperature_k, Temperature::UnitKelvin);
    Humidity humidity(k.humidity_percent, Percent::UnitPercent);
    Length scale(k.scale_height_km, Length::UnitKiloMeter);
    Pressure step(k.step_hpa, Pressure::UnitMilliBar);
    Length top(k.top_km, Length::UnitKiloMeter);
    AtmProfile* p;
    if (k.levels.empty()) {
      p = new AtmProfile(altitude, pressure, temperature, k.lapse_rate, humidity, scale, step, 1.2, top, k.type);
    } else {
      std::vector<Length> boundaries;
      std::vector<Temperature> temperatures;
      for (const auto& level : k.levels) {
        boundaries.push_back(Length(level.first, Length::UnitMeter));
        temperatures.push_back(Temperature(level.second, Temperature::UnitKelvin));
      }
      p = new AtmProfile(altitude, pressure, temperature, k.lapse_rate, humidity, scale, step, 1.2, top, k.type,
                         boundaries, temperatures);
    }
    std::fprintf(out, "{\"name\":\"%s\",\"altitude_m\":%.17g,\"ground_pressure_hpa\":%.17g,"
                 "\"ground_temperature_k\":%.17g,\"lapse_rate_k_per_km\":%.17g,\"relative_humidity_percent\":%.17g,"
                 "\"water_vapour_scale_height_km\":%.17g,\"pressure_step_hpa\":%.17g,\"top_altitude_km\":%.17g,"
                 "\"atmosphere\":%u,\"levels\":[",
                 k.name, k.altitude_m, k.pressure_hpa, k.temperature_k, k.lapse_rate, k.humidity_percent,
                 k.scale_height_km, k.step_hpa, k.top_km, k.type);
    for (size_t i = 0; i < k.levels.size(); i++)
      std::fprintf(out, "%s[%.17g,%.17g]", i ? "," : "", k.levels[i].first, k.levels[i].second);
    std::fprintf(out, "],\"corrected_step_hpa\":%.17g,\"num_layers\":%u,\"tropopause_layer\":%u,"
                 "\"tropopause_temperature_k\":%.17g,\"tropopause_altitude_m\":%.17g,\"ground_water_column_m\":%.17g,",
                 p->pressureStep_.get(Pressure::UnitMilliBar), p->numLayer_, p->tropoLayer_,
                 p->tropoTemperature_.get(Temperature::UnitKelvin), p->tropoAltitude_.get(Length::UnitMeter),
                 p->getGroundWH2O().get(Length::UnitMeter));
    print_array(out, "thickness_m", p->v_layerThickness_);
    print_array(out, "temperature_k", p->v_layerTemperature_);
    print_array(out, "bottom_temperature_k", p->v_layerTemperature0_);
    print_array(out, "top_temperature_k", p->v_layerTemperature1_);
    print_array(out, "pressure_hpa", p->v_layerPressure_);
    print_array(out, "bottom_pressure_hpa", p->v_layerPressure0_);
    print_array(out, "top_pressure_hpa", p->v_layerPressure1_);
    print_array(out, "water_vapour_kg_m3", p->v_layerWaterVapor_);
    print_array(out, "bottom_water_vapour_kg_m3", p->v_layerWaterVapor0_);
    print_array(out, "top_water_vapour_kg_m3", p->v_layerWaterVapor1_);
    print_array(out, "o3_per_m3", p->v_layerO3_);
    print_array(out, "co_per_m3", p->v_layerCO_);
    print_array(out, "n2o_per_m3", p->v_layerN2O_);
    print_array(out, "no2_per_m3", p->v_layerNO2_);
    print_array(out, "so2_per_m3", p->v_layerSO2_);
    print_array(out, "hcl_per_m3", p->v_layerHCl_);
    print_array(out, "hcn_per_m3", p->v_layerHCN_, true);
    std::fprintf(out, "}%s\n", c + 1 < cases.size() ? "," : "");
    delete p;
  }
  std::fprintf(out, "]\n");
  std::fclose(out);
}

// The frequencies (GHz) of the refractivity grid.
std::vector<double> refractivity_frequencies() {
  std::vector<double> f;
  // Either side of ATM's frequency-bin edges (even GHz), and on a few.
  for (int k = 1; k <= 800; k += 7) {
    f.push_back(2.0 * k - 1e-7);
    f.push_back(2.0 * k + 1e-7);
  }
  for (int k = 1; k <= 800; k += 37) f.push_back(2.0 * k);
  // Line centres and the 60-GHz O2 band.
  const double lines[] = {22.235080, 50.474, 53.5960, 56.2648, 58.4466, 59.5910, 60.3061, 61.1506, 62.4863, 64.1278,
                          65.2240, 66.3020, 67.3694, 115.271202, 118.750343, 118.75034, 118.7504, 119.5, 121.0,
                          183.310117, 230.538, 325.152919, 345.795990, 380.197372, 439.150812, 448.001075,
                          556.936002, 620.700807, 625.918, 752.033227, 987.926764, 1097.364, 1113.343,
                          1162.912, 1207.639, 1410.618, 1602.219, 1661.007, 1716.770, 1867.749};
  for (double line : lines) f.push_back(line);
  // ATM's cut-offs and window overrides.
  const double cuts[] = {20.0, 118.750345, 124.0, 499.99, 624.0, 627.8, 999.9, 999.99, 1595.9, 1599.9, 1599.999};
  for (double cut : cuts) {
    f.push_back(cut - 1e-9);
    f.push_back(cut);
    f.push_back(cut + 1e-9);
  }
  // Below ATM's first bin and below 1 GHz.
  const double low[] = {0.05, 0.5, 0.999999, 1.0, 1.3, 2.7};
  for (double v : low) f.push_back(v);
  // Pseudo-random points up to 2 THz (a fixed LCG, so the set is stable).
  uint64_t state = 0x2545F4914F6CDD1DULL;
  for (int i = 0; i < 100; i++) {
    state = state * 6364136223846793005ULL + 1442695040888963407ULL;
    f.push_back(1.0 + 1999.0 * double(state >> 11) / double(1ULL << 53));
  }
  return f;
}

struct Condition {
  double temperature_k, pressure_hpa, water_vapour_pressure_hpa;
};

const Condition kConditions[] = {
    {280.0, 900.0, 10.0},  // wet, high pressure (wide windows)
    {265.0, 550.0, 2.5},   // Chajnantor ground
    {220.0, 120.0, 0.01},  // upper troposphere (middle windows)
    {250.0, 1.0, 1e-6},    // mesosphere: narrow windows, O2 mesospheric factor
};

void write_refractivity(const std::string& dir) {
  std::vector<double> frequencies = refractivity_frequencies();
  RefractiveIndex ri;
  FILE* meta = std::fopen((dir + "/refractivity.json").c_str(), "w");
  std::fprintf(meta, "{\"layout\":\"condition, species 1..25, frequency; (re, im) little-endian f64\",\"conditions\":[");
  size_t nc = sizeof(kConditions) / sizeof(kConditions[0]);
  for (size_t c = 0; c < nc; c++)
    std::fprintf(meta, "%s[%.17g,%.17g,%.17g]", c ? "," : "", kConditions[c].temperature_k,
                 kConditions[c].pressure_hpa, kConditions[c].water_vapour_pressure_hpa);
  std::fprintf(meta, "],");
  print_array(meta, "frequencies_ghz", frequencies, true);
  std::fprintf(meta, "}\n");
  std::fclose(meta);

  FILE* bin = std::fopen((dir + "/refractivity.bin").c_str(), "wb");
  for (const Condition& k : kConditions) {
    for (unsigned int species = 1; species <= 25; species++) {
      for (double nu : frequencies) {
        std::complex<double> n = ri.mkEarthSpecificRefractivity(species, k.temperature_k, k.pressure_hpa,
                                                                k.water_vapour_pressure_hpa, nu);
        double pair[2] = {n.real(), n.imag()};
        std::fwrite(pair, sizeof(double), 2, bin);
      }
    }
  }
  std::fclose(bin);
}

void write_layer_refractivity(const std::string& dir) {
  RefractiveIndex ri;
  FILE* out = std::fopen((dir + "/layer_refractivity.json").c_str(), "w");
  const double frequencies[] = {22.235, 60.3, 118.75, 183.31, 345.8, 557.0, 690.0, 987.9};
  const double o3_density = 2.7e18, co_density = 1.4e18;
  std::fprintf(out, "{\"o3_per_m3\":%.17g,\"co_per_m3\":%.17g,\"rows\":[", o3_density, co_density);
  bool first = true;
  for (const Condition& k : kConditions) {
    for (double nu : frequencies) {
      double t = k.temperature_k, p = k.pressure_hpa, e = k.water_vapour_pressure_hpa;
      std::complex<double> values[] = {
          ri.getRefractivity_16o16o(t, p, e, nu),
          ri.getRefractivity_16o16o_vib(t, p, e, nu),
          ri.getRefractivity_16o18o(t, p, e, nu),
          ri.getRefractivity_hh16o(t, p, e, nu),
          ri.getRefractivity_hh16ov2(t, p, e, nu),
          ri.getRefractivity_hdo(t, p, e, nu),
          ri.getRefractivity_16o16o16o(t, p, nu, o3_density),
          ri.getEarthSpecificRefractivity_co(t, p, nu) * co_density,
      };
      std::fprintf(out, "%s{\"t\":%.17g,\"p\":%.17g,\"e\":%.17g,\"nu\":%.17g,\"values\":[", first ? "" : ",", t, p,
                   e, nu);
      for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++)
        std::fprintf(out, "%s[%.17g,%.17g]", i ? "," : "", values[i].real(), values[i].imag());
      std::fprintf(out, "]}\n");
      first = false;
    }
  }
  std::fprintf(out, "]}\n");
  std::fclose(out);
}

// Times every species' refractivity once per (layer, channel) for the
// default Chajnantor profile and `channels` channels over 2 GHz at 345 GHz:
// the work casa-atmosphere's `refractivity_timing` example does.
int time_refractivity(unsigned int channels) {
  AtmProfile profile(Length(5000.0, Length::UnitMeter), Pressure(560.0, Pressure::UnitMilliBar),
                     Temperature(270.0, Temperature::UnitKelvin), -5.6, Humidity(20.0, Percent::UnitPercent),
                     Length(2.0, Length::UnitKiloMeter), Pressure(10.0, Pressure::UnitMilliBar), 1.2,
                     Length(48.0, Length::UnitKiloMeter), 1);
  RefractiveIndex ri;
  auto start = std::chrono::steady_clock::now();
  double sink = 0.0;
  for (unsigned int c = 0; c < channels; c++) {
    double nu = 344.0 + 2.0 * c / channels;
    for (unsigned int j = 0; j < profile.numLayer_; j++) {
      double t = profile.v_layerTemperature_[j], p = profile.v_layerPressure_[j];
      double e = profile.v_layerWaterVapor_[j] * 1000.0 * t / 217.0;
      for (unsigned int species = 1; species <= 25; species++)
        sink += ri.mkEarthSpecificRefractivity(species, t, p, e, nu).imag();
    }
  }
  double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  std::printf("c++: %u channels x %u layers x 25 species: %.3f s (%.1f us per channel) [%g]\n", channels,
              profile.numLayer_, seconds, 1e6 * seconds / channels, sink);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 3 && std::string(argv[1]) == "--time") return time_refractivity(std::stoul(argv[2]));
  if (argc != 2) {
    std::fprintf(stderr, "usage: %s <fixture directory> | --time <channels>\n", argv[0]);
    return 2;
  }
  std::string dir = argv[1];
  write_profiles(dir);
  write_refractivity(dir);
  write_layer_refractivity(dir);
  return 0;
}
