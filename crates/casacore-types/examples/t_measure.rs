// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demo program for the measures module.
//!
//! Equivalent to a subset of C++ casacore's `tMeasure` program.
//! Demonstrates epoch, position, direction, frequency, and Doppler conversions,
//! plus record serialization.

use casacore_types::measures::direction::{DirectionRef, MDirection};
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casacore_types::measures::{
    EpochRef, MEpoch, MPosition, MeasFrame, MjdHighPrec, PositionRef, direction_from_record,
    direction_to_record, doppler_from_record, doppler_to_record, epoch_from_record,
    epoch_to_record, frequency_from_record, frequency_to_record, position_from_record,
    position_to_record, radial_velocity_from_record, radial_velocity_to_record,
};

fn main() {
    println!("=== Measures Demo ===");
    println!();

    // --- Epoch conversions ---

    println!("--- Epoch conversions ---");

    // J2000.0 in UTC
    let j2000_utc = MEpoch::from_mjd(51544.5, EpochRef::UTC);
    println!("J2000.0 UTC:  {j2000_utc}");

    let frame = MeasFrame::new();

    // UTC → TAI
    let tai = j2000_utc.convert_to(EpochRef::TAI, &frame).unwrap();
    let diff_s = (tai.value().as_mjd() - j2000_utc.value().as_mjd()) * 86400.0;
    println!("J2000.0 TAI:  {tai}  (TAI-UTC = {diff_s:.3}s)");

    // UTC → TT
    let tt = j2000_utc.convert_to(EpochRef::TT, &frame).unwrap();
    let diff_s = (tt.value().as_mjd() - j2000_utc.value().as_mjd()) * 86400.0;
    println!("J2000.0 TT:   {tt}  (TT-UTC = {diff_s:.3}s)");

    // TT → TDB
    let tdb = tt.convert_to(EpochRef::TDB, &frame).unwrap();
    let diff_ms = (tdb.value().as_mjd() - tt.value().as_mjd()) * 86400.0 * 1000.0;
    println!("J2000.0 TDB:  {tdb}  (TDB-TT = {diff_ms:.3}ms)");

    // TT → TCG
    let tcg = tt.convert_to(EpochRef::TCG, &frame).unwrap();
    let diff_s = (tcg.value().as_mjd() - tt.value().as_mjd()) * 86400.0;
    println!("J2000.0 TCG:  {tcg}  (TCG-TT = {diff_s:.6}s)");

    // TDB → TCB
    let tcb = tdb.convert_to(EpochRef::TCB, &frame).unwrap();
    let diff_s = (tcb.value().as_mjd() - tdb.value().as_mjd()) * 86400.0;
    println!("J2000.0 TCB:  {tcb}  (TCB-TDB = {diff_s:.3}s)");

    println!();

    // --- Epoch roundtrip ---

    println!("--- Epoch roundtrip: UTC→TAI→TT→TDB→TT→TAI→UTC ---");
    let tt2 = tdb.convert_to(EpochRef::TT, &frame).unwrap();
    let tai2 = tt2.convert_to(EpochRef::TAI, &frame).unwrap();
    let utc2 = tai2.convert_to(EpochRef::UTC, &frame).unwrap();
    let roundtrip_err = (utc2.value().as_mjd() - j2000_utc.value().as_mjd()) * 86400.0;
    println!("Roundtrip error: {roundtrip_err:.3e} seconds");

    println!();

    // --- UT1/GMST with dUT1 ---

    println!("--- UT1/GMST conversions ---");
    let frame_ut1 = MeasFrame::new().with_dut1(0.3);
    let ut1 = j2000_utc.convert_to(EpochRef::UT1, &frame_ut1).unwrap();
    println!("J2000.0 UT1:  {ut1}  (with dUT1=0.3s)");

    let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let frame_full = MeasFrame::new().with_dut1(0.3).with_position(vla.clone());
    let gmst = ut1.convert_to(EpochRef::GMST1, &frame_full).unwrap();
    let gmst_hours = gmst.value().frac() * 24.0;
    println!("J2000.0 GMST: {gmst}  ({gmst_hours:.4} hours)");

    println!();

    // --- Position conversions ---

    println!("--- Position conversions ---");
    println!("VLA ITRF:     {vla}");
    let vla_wgs = vla.convert_to(PositionRef::WGS84).unwrap();
    println!("VLA WGS84:    {vla_wgs}");

    let vla_back = vla_wgs.convert_to(PositionRef::ITRF).unwrap();
    let err: f64 = vla
        .values()
        .iter()
        .zip(vla_back.values().iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f64>()
        .sqrt();
    println!("Position roundtrip error: {err:.3e} m");

    println!();

    // --- Direction conversions ---

    println!("--- Direction conversions ---");

    // Galactic center → J2000
    let gc = MDirection::from_angles(0.0, 0.0, DirectionRef::GALACTIC);
    let gc_j2000 = gc.convert_to(DirectionRef::J2000, &frame).unwrap();
    let (ra, dec) = gc_j2000.as_angles();
    println!(
        "Galactic center → J2000: RA={:.2}°, Dec={:.2}°",
        ra.to_degrees(),
        dec.to_degrees()
    );

    // J2000 → GALACTIC roundtrip
    let src = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let gal = src.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let back = gal.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = angular_sep(&src.cosines(), &back.cosines());
    println!(
        "J2000→GAL→J2000 roundtrip error: {:.3e} rad ({:.3e} arcsec)",
        sep,
        sep.to_degrees() * 3600.0
    );

    // ICRS ↔ J2000 frame tie
    let icrs = MDirection::from_angles(1.0, 0.5, DirectionRef::ICRS);
    let j2k = icrs.convert_to(DirectionRef::J2000, &frame).unwrap();
    let tie_sep = angular_sep(&icrs.cosines(), &j2k.cosines());
    println!(
        "ICRS↔J2000 frame tie: {:.3e} rad ({:.1} mas)",
        tie_sep,
        tie_sep.to_degrees() * 3_600_000.0
    );

    // Precession: J2000 → JMEAN (needs epoch)
    let epoch = MEpoch::from_mjd(51544.5, EpochRef::UTC);
    let frame_epoch = MeasFrame::new().with_epoch(epoch);
    let jmean = src.convert_to(DirectionRef::JMEAN, &frame_epoch).unwrap();
    let (lon_jm, lat_jm) = jmean.as_angles();
    println!(
        "J2000 → JMEAN at J2000.0: lon={:.6}°, lat={:.6}°",
        lon_jm.to_degrees(),
        lat_jm.to_degrees()
    );

    println!();

    // --- Frequency conversions ---

    println!("--- Frequency conversions ---");

    let freq_1420 = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    println!("HI line: {freq_1420}");

    // LSRK → BARY (needs direction)
    let m31_dir = MDirection::from_angles(
        10.68_f64.to_radians(),
        41.27_f64.to_radians(),
        DirectionRef::J2000,
    );
    let freq_frame = MeasFrame::new().with_direction(m31_dir);
    let bary = freq_1420
        .convert_to(FrequencyRef::BARY, &freq_frame)
        .unwrap();
    let shift_hz = bary.hz() - freq_1420.hz();
    println!("LSRK → BARY (toward M31): {bary}  (shift = {shift_hz:.0} Hz)");

    // BARY → LGROUP
    let lgroup = bary.convert_to(FrequencyRef::LGROUP, &freq_frame).unwrap();
    let shift_hz = lgroup.hz() - bary.hz();
    println!("BARY → LGROUP: {lgroup}  (shift = {shift_hz:.0} Hz)");

    // BARY → CMB
    let cmb = bary.convert_to(FrequencyRef::CMB, &freq_frame).unwrap();
    let shift_hz = cmb.hz() - bary.hz();
    println!("BARY → CMB:    {cmb}  (shift = {shift_hz:.0} Hz)");

    println!();

    // --- Doppler conversions ---

    println!("--- Doppler conversions ---");

    let z_val = MDoppler::new(1.0, DopplerRef::Z);
    println!("Redshift z=1: {z_val}");

    let frame_d = MeasFrame::new();
    let radio = z_val.convert_to(DopplerRef::RADIO, &frame_d).unwrap();
    println!("  → RADIO:    {radio}");

    let beta = z_val.convert_to(DopplerRef::BETA, &frame_d).unwrap();
    println!("  → BETA:     {beta}");

    let ratio = z_val.convert_to(DopplerRef::RATIO, &frame_d).unwrap();
    println!("  → RATIO:    {ratio}");

    let gamma = z_val.convert_to(DopplerRef::GAMMA, &frame_d).unwrap();
    println!("  → GAMMA:    {gamma}");

    // Roundtrip
    let back_z = gamma.convert_to(DopplerRef::Z, &frame_d).unwrap();
    println!(
        "GAMMA→Z roundtrip error: {:.3e}",
        (back_z.value() - z_val.value()).abs()
    );

    println!();

    // --- Radial velocity conversions ---

    println!("--- Radial velocity conversions ---");

    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    println!("RV: {rv}");

    let m31_dir_rv = MDirection::from_angles(
        10.68_f64.to_radians(),
        41.27_f64.to_radians(),
        DirectionRef::J2000,
    );
    let rv_frame = MeasFrame::new().with_direction(m31_dir_rv);

    let rv_bary = rv.convert_to(RadialVelocityRef::BARY, &rv_frame).unwrap();
    let shift = rv_bary.ms() - rv.ms();
    println!("LSRK → BARY (toward M31): {rv_bary}  (shift = {shift:.1} m/s)");

    let rv_lgroup = rv_bary
        .convert_to(RadialVelocityRef::LGROUP, &rv_frame)
        .unwrap();
    let shift = rv_lgroup.ms() - rv_bary.ms();
    println!("BARY → LGROUP: {rv_lgroup}  (shift = {shift:.1} m/s)");

    // Roundtrip
    let rv_back = rv_lgroup
        .convert_to(RadialVelocityRef::LSRK, &rv_frame)
        .unwrap();
    println!(
        "LGROUP→LSRK roundtrip error: {:.3e} m/s",
        (rv_back.ms() - rv.ms()).abs()
    );

    println!();

    // --- REST frequency with radial velocity ---

    println!("--- REST frequency (with radial velocity) ---");

    let rest_freq = MFrequency::new(1.42e9, FrequencyRef::REST);
    let rv_for_freq = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let rest_frame = MeasFrame::new()
        .with_direction(MDirection::from_angles(
            10.68_f64.to_radians(),
            41.27_f64.to_radians(),
            DirectionRef::J2000,
        ))
        .with_radial_velocity(rv_for_freq);

    let lsrk_freq = rest_freq
        .convert_to(FrequencyRef::LSRK, &rest_frame)
        .unwrap();
    let shift_hz = lsrk_freq.hz() - rest_freq.hz();
    println!("REST → LSRK (RV=50km/s): {lsrk_freq}  (shift = {shift_hz:.0} Hz)");

    let back_rest = lsrk_freq
        .convert_to(FrequencyRef::REST, &rest_frame)
        .unwrap();
    println!(
        "LSRK→REST roundtrip error: {:.3e} Hz",
        (back_rest.hz() - rest_freq.hz()).abs()
    );

    println!();

    // --- B1950 direction ---

    println!("--- B1950 (FK4/FK5) direction ---");

    let j2000_dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let b1950 = j2000_dir.convert_to(DirectionRef::B1950, &frame).unwrap();
    let (b_lon, b_lat) = b1950.as_angles();
    println!("J2000 (1.0, 0.5) → B1950: ({:.6}, {:.6}) rad", b_lon, b_lat);

    let back_j = b1950.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = angular_sep(&j2000_dir.cosines(), &back_j.cosines());
    println!(
        "B1950→J2000 roundtrip error: {:.3e} rad ({:.3e} arcsec)",
        sep,
        sep.to_degrees() * 3600.0
    );

    println!();

    // --- High-precision MJD ---

    println!("--- High-precision MJD ---");
    let hp = MjdHighPrec::new(51544.0, 0.5);
    let (jd1, jd2) = hp.as_jd_pair();
    println!("MJD {hp} → JD pair ({jd1}, {jd2}) → JD = {:.1}", jd1 + jd2);

    println!();

    // --- Record serialization ---

    println!("--- Record serialization ---");

    let rec = epoch_to_record(&j2000_utc);
    let decoded = epoch_from_record(&rec).unwrap();
    println!("Epoch:     {j2000_utc} → record → {decoded}");

    let pos_rec = position_to_record(&vla);
    let decoded_pos = position_from_record(&pos_rec).unwrap();
    println!("Position:  {vla} → record → {decoded_pos}");

    let dir_rec = direction_to_record(&src);
    let decoded_dir = direction_from_record(&dir_rec).unwrap();
    println!("Direction: {src} → record → {decoded_dir}");

    let freq_rec = frequency_to_record(&freq_1420);
    let decoded_freq = frequency_from_record(&freq_rec).unwrap();
    println!("Frequency: {freq_1420} → record → {decoded_freq}");

    let dop_rec = doppler_to_record(&z_val);
    let decoded_dop = doppler_from_record(&dop_rec).unwrap();
    println!("Doppler:   {z_val} → record → {decoded_dop}");

    let rv_rec = radial_velocity_to_record(&rv);
    let decoded_rv = radial_velocity_from_record(&rv_rec).unwrap();
    println!("RadVel:    {rv} → record → {decoded_rv}");

    println!();
    println!("=== Done ===");
}

fn angular_sep(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    let dot = a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    dot.clamp(-1.0, 1.0).acos()
}
