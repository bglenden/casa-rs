// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antenna mount `simutil.setcfg` gives a telescope it is not told one
//! for.

/// `simutil.setcfg`'s mount for `telescope` when none is given: ASKAP, DRAO
/// and WSRT are equatorial; LOFAR, LWA and MOST, long-wavelength arrays,
/// `BIZARRE` (CASA's stand-in for a fixed, ground-based array); every other
/// telescope alt-az. The name is compared ignoring ASCII case.
pub fn default_mount(telescope: &str) -> &'static str {
    let upper = telescope.to_ascii_uppercase();
    match upper.as_str() {
        "ASKAP" | "DRAO" | "WSRT" => "EQUATORIAL",
        "LOFAR" | "LWA" | "MOST" => "BIZARRE",
        _ => "alt-az",
    }
}

#[cfg(test)]
mod tests {
    use super::default_mount;

    #[test]
    fn mounts_are_casas_guesses() {
        assert_eq!(default_mount("askap"), "EQUATORIAL");
        assert_eq!(default_mount("WSRT"), "EQUATORIAL");
        assert_eq!(default_mount("lwa"), "BIZARRE");
        assert_eq!(default_mount("ALMA"), "alt-az");
        assert_eq!(default_mount("LWA1"), "alt-az");
    }
}
