// casacore_vs_sofa_deviation.cpp
//
// Compare direction conversion results between casacore's bespoke algorithms
// and the IAU SOFA/ERFA reference implementation. Both implement the same
// IAU standards (precession, nutation, aberration) but with different
// polynomial series and internal decompositions.
//
// Build (requires casacore and ERFA):
//   g++ -std=c++17 -O2 -o casacore_vs_sofa_deviation casacore_vs_sofa_deviation.cpp \
//       $(pkg-config --cflags --libs casacore) $(pkg-config --cflags --libs erfa) -lm
//
// Or with SOFA C library instead of ERFA (add -DUSE_SOFA):
//   g++ -std=c++17 -O2 -DUSE_SOFA -o casacore_vs_sofa_deviation casacore_vs_sofa_deviation.cpp \
//       $(pkg-config --cflags --libs casacore) -lsofa_c -lm

#include <cstdio>
#include <cmath>

// casacore headers
#include <casacore/measures/Measures/MDirection.h>
#include <casacore/measures/Measures/MCDirection.h>
#include <casacore/measures/Measures/MeasConvert.h>
#include <casacore/measures/Measures/MeasFrame.h>
#include <casacore/measures/Measures/MEpoch.h>
#include <casacore/measures/Measures/MPosition.h>
#include <casacore/measures/Measures/MeasTable.h>
#include <casacore/measures/Measures/Aberration.h>
#include <casacore/measures/Measures/Nutation.h>
#include <casacore/measures/Measures/Precession.h>
#include <casacore/casa/System/AipsrcValue.h>
#include <casacore/casa/Quanta/MVDirection.h>
#include <casacore/casa/Quanta/MVPosition.h>

// ERFA (or SOFA) — the IAU reference implementation
// ERFA is a BSD-licensed reimplementation of SOFA with identical algorithms.
// Use <erfa.h> for ERFA or <sofa.h>/<sofam.h> for SOFA.
#ifdef USE_SOFA
#include <sofa.h>
#include <sofam.h>
#define SOFA_OR_ERFA(erfa_fn, sofa_fn) sofa_fn
#else
#include <erfa.h>
#include <erfam.h>
#define SOFA_OR_ERFA(erfa_fn, sofa_fn) erfa_fn
#endif

using namespace casacore;

static const double RAD_TO_MAS = 180.0 / M_PI * 3600.0 * 1000.0; // radians to milliarcsec
static const double RAD_TO_ARCSEC = 180.0 / M_PI * 3600.0;

// Helper: angular separation between two unit vectors (in mas)
static double angular_sep_mas(const double a[3], const double b[3]) {
    double cross[3] = {
        a[1]*b[2] - a[2]*b[1],
        a[2]*b[0] - a[0]*b[2],
        a[0]*b[1] - a[1]*b[0]
    };
    double sinang = std::sqrt(cross[0]*cross[0] + cross[1]*cross[1] + cross[2]*cross[2]);
    double cosang = a[0]*b[0] + a[1]*b[1] + a[2]*b[2];
    return std::atan2(sinang, cosang) * RAD_TO_MAS;
}

// Helper: 3x3 matrix multiply C = A * B
static void mat_mul(const double A[3][3], const double B[3][3], double C[3][3]) {
    for (int i = 0; i < 3; i++)
        for (int j = 0; j < 3; j++) {
            C[i][j] = 0.0;
            for (int k = 0; k < 3; k++)
                C[i][j] += A[i][k] * B[k][j];
        }
}

// Helper: matrix-vector multiply y = M * x
static void mat_vec(const double M[3][3], const double x[3], double y[3]) {
    for (int i = 0; i < 3; i++)
        y[i] = M[i][0]*x[0] + M[i][1]*x[1] + M[i][2]*x[2];
}

// Helper: print a 3x3 matrix
static void print_matrix(const char* name, const double m[3][3]) {
    printf("  %s:\n", name);
    for (int i = 0; i < 3; i++)
        printf("    [%+.17e, %+.17e, %+.17e]\n", m[i][0], m[i][1], m[i][2]);
}

// Helper: maximum element-wise difference between two 3x3 matrices (in mas)
static double max_matrix_diff_mas(const double A[3][3], const double B[3][3]) {
    double maxdiff = 0.0;
    for (int i = 0; i < 3; i++)
        for (int j = 0; j < 3; j++) {
            double d = std::fabs(A[i][j] - B[i][j]) * RAD_TO_MAS;
            if (d > maxdiff) maxdiff = d;
        }
    return maxdiff;
}

// Set casacore to use IAU 2000A nutation model
static void enable_iau2000a() {
    // Force static init
    (void)MeasTable::useIAU2000();
    (void)MeasTable::useIAU2000A();
    // Set flags
    AipsrcValue<Bool>::set(
        AipsrcValue<Bool>::registerRC("measures.iau2000.b_use", False), True);
    AipsrcValue<Bool>::set(
        AipsrcValue<Bool>::registerRC("measures.iau2000.b_use2000a", False), True);
}

static void disable_iau2000a() {
    AipsrcValue<Bool>::set(
        AipsrcValue<Bool>::registerRC("measures.iau2000.b_use", False), False);
    AipsrcValue<Bool>::set(
        AipsrcValue<Bool>::registerRC("measures.iau2000.b_use2000a", False), False);
}

// -----------------------------------------------------------------------
// Test at a specific epoch
// -----------------------------------------------------------------------
void compare_at_epoch(const char* label, double mjd_utc,
                      double obs_lon_rad, double obs_lat_rad, double obs_h_m,
                      double dir_lon_rad, double dir_lat_rad) {
    printf("\n========================================\n");
    printf("Epoch: %s (MJD UTC = %.1f)\n", label, mjd_utc);
    printf("Direction: lon=%.6f rad, lat=%.6f rad\n", dir_lon_rad, dir_lat_rad);
    printf("Observer: lon=%.6f° lat=%.6f° h=%.1f m\n",
           obs_lon_rad * 180.0/M_PI, obs_lat_rad * 180.0/M_PI, obs_h_m);
    printf("========================================\n");

    // ------------------------------------------------------------------
    // 1. casacore conversion (IAU 1976/1980 default)
    // ------------------------------------------------------------------
    MVDirection mvdir(dir_lon_rad, dir_lat_rad);
    MDirection dir_j2000(mvdir, MDirection::J2000);

    MPosition pos(MVPosition(Quantity(obs_h_m, "m"),
                             Quantity(obs_lon_rad, "rad"),
                             Quantity(obs_lat_rad, "rad")),
                  MPosition::WGS84);

    MEpoch epoch(MVEpoch(Quantity(mjd_utc, "d")), MEpoch::UTC);
    MeasFrame frame(epoch, pos);

    // IAU 1976/1980 (default)
    {
        MDirection::Convert toApp(dir_j2000, MDirection::Ref(MDirection::APP, frame));
        MDirection app = toApp();
        MVDirection appMV = app.getValue();
        double casa_app_lon = appMV.getLong();
        double casa_app_lat = appMV.getLat();
        printf("\n--- IAU 1976/1980 (casacore default) ---\n");
        printf("  casacore APP: lon=%.15f lat=%.15f rad\n", casa_app_lon, casa_app_lat);

        // SOFA/ERFA equivalent: compute TT from UTC
        // (simplified: using fixed offset; a real comparison should use proper leap seconds)
        double tt_mjd = mjd_utc + 64.184 / 86400.0; // approximate for J2000
        double tt1 = 2400000.5;
        double tt2 = tt_mjd;

        // SOFA BPN (IAU 1976/1980 equivalent: pnm80)
        // Note: ERFA doesn't have pnm80, so we compute P*N manually
        double rp_sofa[3][3], rn_sofa[3][3], bpn_sofa[3][3];
        SOFA_OR_ERFA(eraPmat76, iauPmat76)(tt1, tt2, rp_sofa);  // precession
        SOFA_OR_ERFA(eraNutm80, iauNutm80)(tt1, tt2, rn_sofa); // nutation matrix (Wahr 1981)

        // Actually use nutm80 if available, or compute N*P
        // ERFA has eraPnm80 which gives the full product
#ifdef USE_SOFA
        double bpn80[3][3];
        iauPnm80(tt1, tt2, bpn80);
#else
        double bpn80[3][3];
        eraPnm80(tt1, tt2, bpn80);
#endif

        // Direction cosines
        double p[3] = { std::cos(dir_lat_rad)*std::cos(dir_lon_rad),
                        std::cos(dir_lat_rad)*std::sin(dir_lon_rad),
                        std::sin(dir_lat_rad) };

        // Apply BPN to get true-of-date direction
        double p_true[3];
        mat_vec(bpn80, p, p_true);

        // Earth velocity from ERFA epv00
        double pvh[2][3], pvb[2][3];
        SOFA_OR_ERFA(eraEpv00, iauEpv00)(tt1, tt2, pvh, pvb);

        // Velocity in units of c (AU/day → fraction of c)
        const double C_AU_PER_DAY = 173.14463267424034;
        double v[3] = { pvb[1][0]/C_AU_PER_DAY,
                        pvb[1][1]/C_AU_PER_DAY,
                        pvb[1][2]/C_AU_PER_DAY };
        // Rotate velocity to true frame
        double v_true[3];
        mat_vec(bpn80, v, v_true);

        // Sun distance
        double s = std::sqrt(pvh[0][0]*pvh[0][0] +
                             pvh[0][1]*pvh[0][1] +
                             pvh[0][2]*pvh[0][2]);
        double v2 = v[0]*v[0] + v[1]*v[1] + v[2]*v[2];
        double bm1 = std::sqrt(1.0 - v2);

        // Apply aberration
        double p_app[3];
        SOFA_OR_ERFA(eraAb, iauAb)(p_true, v_true, s, bm1, p_app);

        double sofa_app_lon = std::atan2(p_app[1], p_app[0]);
        double sofa_app_lat = std::asin(p_app[2]);

        printf("  SOFA/ERFA APP: lon=%.15f lat=%.15f rad\n", sofa_app_lon, sofa_app_lat);
        double sep = angular_sep_mas(
            (double[]){std::cos(casa_app_lat)*std::cos(casa_app_lon),
                       std::cos(casa_app_lat)*std::sin(casa_app_lon),
                       std::sin(casa_app_lat)},
            p_app);
        printf("  Angular separation: %.3f mas\n", sep);
    }

    // ------------------------------------------------------------------
    // 2. casacore conversion (IAU 2000A mode)
    // ------------------------------------------------------------------
    {
        enable_iau2000a();

        // Need a fresh converter to pick up the IAU 2000A flags
        MeasFrame frame2(epoch, pos);
        MDirection::Convert toApp2(dir_j2000, MDirection::Ref(MDirection::APP, frame2));
        MDirection app2 = toApp2();
        MVDirection appMV2 = app2.getValue();
        double casa_app_lon = appMV2.getLong();
        double casa_app_lat = appMV2.getLat();

        disable_iau2000a();

        printf("\n--- IAU 2000A ---\n");
        printf("  casacore APP: lon=%.15f lat=%.15f rad\n", casa_app_lon, casa_app_lat);

        double tt_mjd = mjd_utc + 64.184 / 86400.0;
        double tt1 = 2400000.5;
        double tt2 = tt_mjd;

        // SOFA BPN (IAU 2000A: pnm00a includes frame bias + precession + nutation)
        double bpn00a[3][3];
        SOFA_OR_ERFA(eraPnm00a, iauPnm00a)(tt1, tt2, bpn00a);

        double p[3] = { std::cos(dir_lat_rad)*std::cos(dir_lon_rad),
                        std::cos(dir_lat_rad)*std::sin(dir_lon_rad),
                        std::sin(dir_lat_rad) };
        double p_true[3];
        mat_vec(bpn00a, p, p_true);

        double pvh[2][3], pvb[2][3];
        SOFA_OR_ERFA(eraEpv00, iauEpv00)(tt1, tt2, pvh, pvb);
        const double C_AU_PER_DAY = 173.14463267424034;
        double v[3] = { pvb[1][0]/C_AU_PER_DAY,
                        pvb[1][1]/C_AU_PER_DAY,
                        pvb[1][2]/C_AU_PER_DAY };
        double v_true[3];
        mat_vec(bpn00a, v, v_true);
        double s = std::sqrt(pvh[0][0]*pvh[0][0] +
                             pvh[0][1]*pvh[0][1] +
                             pvh[0][2]*pvh[0][2]);
        double v2 = v[0]*v[0] + v[1]*v[1] + v[2]*v[2];
        double bm1 = std::sqrt(1.0 - v2);

        double p_app[3];
        SOFA_OR_ERFA(eraAb, iauAb)(p_true, v_true, s, bm1, p_app);

        double sofa_app_lon = std::atan2(p_app[1], p_app[0]);
        double sofa_app_lat = std::asin(p_app[2]);

        printf("  SOFA/ERFA APP: lon=%.15f lat=%.15f rad\n", sofa_app_lon, sofa_app_lat);
        double sep = angular_sep_mas(
            (double[]){std::cos(casa_app_lat)*std::cos(casa_app_lon),
                       std::cos(casa_app_lat)*std::sin(casa_app_lon),
                       std::sin(casa_app_lat)},
            p_app);
        printf("  Angular separation: %.3f mas\n", sep);
    }

    // ------------------------------------------------------------------
    // 3. Detailed breakdown: where does the deviation come from?
    // ------------------------------------------------------------------
    printf("\n--- Breakdown: intermediate computation differences ---\n");
    {
        double tt_mjd = mjd_utc + 64.184 / 86400.0;
        double tt1 = 2400000.5;
        double tt2 = tt_mjd;

        // casacore Earth velocity (Stumpff series)
        Aberration aber(Aberration::STANDARD);
        MVPosition casa_vel = aber(tt2);  // note: Aberration takes MJD TDB
        printf("  casacore Earth velocity (Stumpff, fraction of c):\n");
        printf("    [%+.17e, %+.17e, %+.17e]\n",
               casa_vel(0), casa_vel(1), casa_vel(2));

        // SOFA Earth velocity (VSOP87 via epv00)
        double pvh[2][3], pvb[2][3];
        SOFA_OR_ERFA(eraEpv00, iauEpv00)(tt1, tt2, pvh, pvb);
        const double C_AU_PER_DAY = 173.14463267424034;
        printf("  SOFA/ERFA Earth velocity (VSOP87/epv00, fraction of c):\n");
        printf("    [%+.17e, %+.17e, %+.17e]\n",
               pvb[1][0]/C_AU_PER_DAY, pvb[1][1]/C_AU_PER_DAY, pvb[1][2]/C_AU_PER_DAY);
        double dv = std::sqrt(
            std::pow(casa_vel(0) - pvb[1][0]/C_AU_PER_DAY, 2) +
            std::pow(casa_vel(1) - pvb[1][1]/C_AU_PER_DAY, 2) +
            std::pow(casa_vel(2) - pvb[1][2]/C_AU_PER_DAY, 2));
        printf("  |delta_v| = %.3e c  (aberration effect: ~%.3f mas)\n",
               dv, dv * RAD_TO_MAS);

        // Note: casacore also applies gravitational light deflection by the
        // Sun (MeasMath::applySolarPos, up to ~1.75" at the limb).
        // SOFA's iauAb() includes a tiny gravitational correction (~0.4 µas)
        // but not the full Sun deflection that casacore applies.
        printf("\n  Note: casacore applies full Sun gravitational deflection\n");
        printf("  (up to ~1.75\" at the limb, typically ~1-2 mas at moderate\n");
        printf("  elongation). SOFA's ab() has only a ~0.4 µas correction.\n");
        printf("  This is likely the dominant source of the ~2 mas deviation.\n");
    }
}

int main() {
    printf("casacore vs SOFA/ERFA direction conversion deviation test\n");
    printf("=========================================================\n");
    printf("\n");
    printf("Both libraries implement the same IAU standards but with different\n");
    printf("polynomial series and internal decompositions:\n");
    printf("  - Precession: casacore uses Euler angle polynomials;\n");
    printf("                SOFA uses Lieske 1977 angles + IAU 2000 corrections\n");
    printf("  - Aberration: casacore uses Stumpff polynomial series;\n");
    printf("                SOFA uses VSOP87 (via epv00) for Earth velocity\n");
    printf("  - Sun deflection: casacore applies full gravitational deflection;\n");
    printf("                    SOFA ab() has only a ~0.4 µas term\n");

    // Test 1: near J2000.0
    compare_at_epoch("J2000.0",
                     51544.5,                            // MJD UTC
                     -1.8782832, 0.5953703, 2124.0,     // VLA
                     1.0, 0.5);                          // direction (lon, lat)

    // Test 2: 2020-01-01 (larger precession effects)
    compare_at_epoch("2020-01-01",
                     58849.0,                            // MJD UTC
                     -1.8782832, 0.5953703, 2124.0,     // VLA
                     1.0, 0.5);                          // direction (lon, lat)

    // Test 3: direction near the ecliptic pole (minimal aberration)
    compare_at_epoch("J2000.0, ecliptic-pole dir",
                     51544.5,
                     -1.8782832, 0.5953703, 2124.0,
                     4.64, 1.16);                        // near ecliptic pole

    printf("\n=========================================================\n");
    printf("Summary: Deviations of ~1-3 mas are expected from the\n");
    printf("different algorithm implementations. The largest contributor\n");
    printf("is likely the Sun gravitational deflection which casacore\n");
    printf("applies but SOFA's ab() does not include at full precision.\n");

    return 0;
}
