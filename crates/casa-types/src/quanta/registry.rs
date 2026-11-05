// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unit registry: lookup, caching, and user-defined units.
//!
//! [`UnitRegistry`] holds all known unit definitions and provides the lookup
//! that the parser uses to resolve unit names.  It is the Rust counterpart
//! of C++ `casa::UnitMap`.
//!
//! The global registry is initialised once from the built-in tables in
//! [`registry_data`] and extended at runtime via
//! [`UnitRegistry::put_user`] and [`UnitRegistry::remove_user`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Once, OnceLock, RwLock};

use crate::quanta::dim::UnitDim;
use crate::quanta::registry_data;
use crate::quanta::unit_val::UnitVal;

/// A named unit definition: name, value, and description.
///
/// Corresponds to C++ `casa::UnitName`.
#[derive(Debug, Clone)]
pub struct UnitName {
    /// Short name (e.g. `"km"`, `"Jy"`).
    pub name: String,
    /// Resolved value (factor + dimensions).
    pub val: UnitVal,
    /// Human-readable description.
    pub description: String,
}

impl UnitName {
    /// Creates a new `UnitName`.
    pub fn new(name: impl Into<String>, val: UnitVal, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            val,
            description: description.into(),
        }
    }
}

/// A prefix entry: name, scale factor, and description.
#[derive(Debug, Clone)]
pub struct PrefixEntry {
    /// The prefix string (e.g. `"k"`, `"da"`).
    pub name: String,
    /// The multiplicative factor (e.g. `1e3` for "k").
    pub factor: f64,
    /// Human-readable description (e.g. `"kilo"`).
    pub description: String,
}

/// A parsed unit and the FITS state under which it was resolved.
#[derive(Clone, Copy)]
struct CachedUnit {
    value: UnitVal,
    fits_enabled: bool,
}

/// The global unit registry.
///
/// Contains immutable built-in maps (defining, SI, customary, prefixes) and
/// a mutable section for user-defined units and parse cache.
pub struct UnitRegistry {
    /// SI prefixes, keyed by prefix string.
    prefixes: HashMap<String, PrefixEntry>,
    /// Defining (base SI) units.
    defining: HashMap<String, UnitName>,
    /// SI derived + extra units.
    si: HashMap<String, UnitName>,
    /// Customary (non-SI) units.
    customary: HashMap<String, UnitName>,
    /// User-defined units (mutable at runtime).
    user: RwLock<HashMap<String, UnitName>>,
    /// Parse cache (mutable at runtime).
    cache: RwLock<HashMap<String, CachedUnit>>,
    /// CASA installs the FITS aliases once per process.
    fits_once: Once,
    fits_enabled: AtomicBool,
}

impl UnitRegistry {
    /// Builds a new registry from the built-in data tables.
    fn from_builtin() -> Self {
        let mut prefixes = HashMap::new();
        for &(name, factor, desc) in registry_data::PREFIXES {
            prefixes.insert(
                name.to_owned(),
                PrefixEntry {
                    name: name.to_owned(),
                    factor,
                    description: desc.to_owned(),
                },
            );
        }

        let mut defining = HashMap::new();
        for &(name, factor, desc, dims) in registry_data::DEFINING {
            defining.insert(
                name.to_owned(),
                UnitName::new(name, UnitVal::new(factor, UnitDim::new(dims)), desc),
            );
        }

        let mut si = HashMap::new();
        for &(name, factor, desc, dims) in registry_data::SI_DERIVED {
            si.insert(
                name.to_owned(),
                UnitName::new(name, UnitVal::new(factor, UnitDim::new(dims)), desc),
            );
        }
        for &(name, factor, desc, dims) in registry_data::SI_EXTRA {
            si.insert(
                name.to_owned(),
                UnitName::new(name, UnitVal::new(factor, UnitDim::new(dims)), desc),
            );
        }

        let mut customary = HashMap::new();
        for &(name, factor, desc, dims) in registry_data::CUSTOMARY {
            customary.insert(
                name.to_owned(),
                UnitName::new(name, UnitVal::new(factor, UnitDim::new(dims)), desc),
            );
        }

        Self {
            prefixes,
            defining,
            si,
            customary,
            user: RwLock::new(HashMap::new()),
            cache: RwLock::new(HashMap::new()),
            fits_once: Once::new(),
            fits_enabled: AtomicBool::new(false),
        }
    }

    /// Looks up a unit by name.
    ///
    /// Search order: defining → SI → customary → cache → user, except that
    /// after FITS registration a user-defined `S` takes precedence.
    /// The immutable built-in maps are otherwise checked first (lock-free).
    pub fn lookup_unit(&self, name: &str) -> Option<UnitVal> {
        // FITS redefines `S` from siemens to seconds. CASA's user map takes
        // precedence for that name, even if a caller already defined it.
        if name == "S"
            && self.fits_enabled.load(Ordering::Acquire)
            && let Some(entry) = self.user.read().unwrap().get(name)
        {
            return Some(entry.val);
        }
        // Check immutable built-in maps first (no locking needed).
        if let Some(entry) = self.defining.get(name) {
            return Some(entry.val);
        }
        if let Some(entry) = self.si.get(name) {
            return Some(entry.val);
        }
        if let Some(entry) = self.customary.get(name) {
            return Some(entry.val);
        }
        // A parse begun before FITS registration can finish after the cache
        // is cleared. Any FITS alias can change an expression's meaning.
        if let Some(cached) = self.cache.read().unwrap().get(name)
            && cached.fits_enabled == self.fits_enabled.load(Ordering::Acquire)
        {
            return Some(cached.value);
        }
        // User-defined units (rarely used).
        if let Some(entry) = self.user.read().unwrap().get(name) {
            return Some(entry.val);
        }
        None
    }

    /// Looks up a prefix by name.
    pub fn lookup_prefix(&self, name: &str) -> Option<f64> {
        self.prefixes.get(name).map(|p| p.factor)
    }

    /// Adds a parsed unit expression to the cache, tagged with the current
    /// FITS state. The caller must ensure `val` was computed in that state;
    /// the parser captures the state before it begins instead.
    pub fn cache_put(&self, name: &str, val: UnitVal) {
        self.cache_put_at_fits_state(name, val, self.cache_fits_state());
    }

    /// Capture the relevant registry state before parsing a cache entry.
    pub(crate) fn cache_fits_state(&self) -> bool {
        self.fits_enabled.load(Ordering::Acquire)
    }

    /// Cache the value with the state captured before its parse began.
    pub(crate) fn cache_put_at_fits_state(&self, name: &str, val: UnitVal, fits_enabled: bool) {
        self.cache.write().unwrap().insert(
            name.to_owned(),
            CachedUnit {
                value: val,
                fits_enabled,
            },
        );
    }

    /// Installs casacore's process-wide FITS unit aliases once.
    ///
    /// Existing user definitions are retained, as in `UnitMap::addFITS`.
    /// Unless already user-defined, `S` changes from SI siemens to seconds.
    pub fn add_fits_units(&self) {
        self.fits_once.call_once(|| {
            const ALIASES: &[(&str, &str, &str)] = &[
                ("BEAM", "beam", "dimensionless beam"),
                ("DAYS", "d", "day"),
                ("DEGREES", "deg", "degree"),
                ("DEG", "deg", "degree"),
                ("HZ", "Hz", "hertz"),
                ("JY", "Jy", "jansky"),
                ("KELVINS", "K", "kelvin"),
                ("KELVIN", "K", "kelvin"),
                ("KM", "km", "km"),
                ("METERS", "m", "meter"),
                ("M", "m", "meter"),
                ("PASCAL", "Pa", "pascal"),
                ("PIXEL", "pixel", "dimensionless pixel"),
                ("SECONDS", "s", "second"),
                ("SEC", "s", "second"),
                ("S", "s", "second"),
                ("VOLTS", "V", "volt"),
                ("YEARS", "a", "year"),
                ("YEAR", "a", "year"),
            ];
            let entries: Vec<_> = ALIASES
                .iter()
                .map(|&(alias, unit, description)| {
                    let val = crate::quanta::parser::parse_unit(unit)
                        .expect("built-in FITS base unit must be valid");
                    UnitName::new(alias, val, description)
                })
                .collect();
            let mut user = self.user.write().unwrap();
            for entry in entries {
                user.entry(entry.name.clone()).or_insert(entry);
            }
            self.fits_enabled.store(true, Ordering::Release);
            drop(user);
            self.clear_cache();
        });
    }

    /// Registers a user-defined unit.
    ///
    /// If a unit with the same name already exists in the user map it is
    /// replaced.  Built-in units are not affected.
    pub fn put_user(&self, name: impl Into<String>, val: UnitVal, desc: impl Into<String>) {
        let name = name.into();
        self.user
            .write()
            .unwrap()
            .insert(name.clone(), UnitName::new(name, val, desc));
        // Invalidate the cache since user units affect lookup.
        self.cache.write().unwrap().clear();
    }

    /// Removes a user-defined unit.
    ///
    /// Returns `true` if the unit existed and was removed.
    pub fn remove_user(&self, name: &str) -> bool {
        let removed = self.user.write().unwrap().remove(name).is_some();
        if removed {
            self.cache.write().unwrap().clear();
        }
        removed
    }

    /// Clears the parse cache.
    pub fn clear_cache(&self) {
        self.cache.write().unwrap().clear();
    }
}

/// Global singleton registry.
static REGISTRY: OnceLock<UnitRegistry> = OnceLock::new();

/// Returns a reference to the global [`UnitRegistry`].
///
/// The registry is initialised on first access from the built-in data tables
/// in [`registry_data`].
pub fn global_registry() -> &'static UnitRegistry {
    REGISTRY.get_or_init(UnitRegistry::from_builtin)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fits_registration_preserves_user_values_and_invalidates_siemens_cache() {
        let reg = UnitRegistry::from_builtin();
        let siemens = reg.lookup_unit("S").unwrap();
        let seconds = reg.lookup_unit("s").unwrap();
        assert_ne!(siemens.dim, seconds.dim);
        reg.cache_put("S.m", siemens);
        reg.put_user("DEGREES", UnitVal::NODIM, "existing user unit");
        // Restore a stale compound after the user-unit cache was cleared.
        reg.cache_put("S.m", siemens);

        reg.add_fits_units();
        assert_eq!(reg.lookup_unit("S").unwrap().dim, seconds.dim);
        assert!(reg.lookup_unit("S.m").is_none());
        // A parse begun before registration can finish after cache clearing.
        reg.cache_put_at_fits_state("S.m", siemens, false);
        assert!(reg.lookup_unit("S.m").is_none());
        reg.cache_put("S.m", seconds);
        assert_eq!(reg.lookup_unit("S.m").unwrap().dim, seconds.dim);
        assert_eq!(reg.lookup_unit("DEGREES").unwrap().dim, UnitVal::NODIM.dim);
        reg.put_user("S", siemens, "user override");
        reg.add_fits_units();
        assert_eq!(reg.lookup_unit("S").unwrap().dim, siemens.dim);

        let existing = UnitRegistry::from_builtin();
        existing.put_user("S", UnitVal::NODIM, "existing user override");
        existing.add_fits_units();
        assert_eq!(existing.lookup_unit("S").unwrap().dim, UnitVal::NODIM.dim);
    }

    #[test]
    fn fits_registration_rejects_a_late_cache_write_for_other_aliases() {
        let reg = UnitRegistry::from_builtin();
        reg.put_user("IXEL", UnitVal::new(2.0, UnitVal::TIME.dim), "test");
        // Before FITS, PIXEL can parse as peta-IXEL. A parse that began then
        // may finish and write its result after the PIXEL alias is installed.
        let stale = UnitVal::new(2.0e15, (UnitVal::TIME * UnitVal::LENGTH).dim);
        reg.add_fits_units();
        reg.cache_put_at_fits_state("PIXEL.m", stale, false);
        assert!(reg.lookup_unit("PIXEL.m").is_none());
    }

    #[test]
    fn lookup_base_units() {
        let reg = global_registry();
        let m = reg.lookup_unit("m").expect("metre");
        assert_eq!(m.factor, 1.0);
        assert_eq!(m.dim.get(crate::quanta::dim::Dimension::Length), 1);

        let kg = reg.lookup_unit("kg").expect("kilogram");
        assert_eq!(kg.factor, 1.0);
        assert_eq!(kg.dim.get(crate::quanta::dim::Dimension::Mass), 1);
    }

    #[test]
    fn lookup_si_derived() {
        let reg = global_registry();
        let hz = reg.lookup_unit("Hz").expect("hertz");
        assert_eq!(hz.factor, 1.0);
        assert_eq!(hz.dim.get(crate::quanta::dim::Dimension::Time), -1);
    }

    #[test]
    fn lookup_customary() {
        let reg = global_registry();
        let jy = reg.lookup_unit("Jy").expect("jansky");
        assert!((jy.factor - 1e-26).abs() < 1e-40);
    }

    #[test]
    fn lookup_prefix() {
        let reg = global_registry();
        assert_eq!(reg.lookup_prefix("k"), Some(1e3));
        assert_eq!(reg.lookup_prefix("da"), Some(1e1));
        assert_eq!(reg.lookup_prefix("x"), None);
    }

    #[test]
    fn user_units() {
        let reg = global_registry();
        reg.put_user("test_unit", UnitVal::new(42.0, UnitDim::default()), "test");
        let val = reg.lookup_unit("test_unit").expect("user unit");
        assert_eq!(val.factor, 42.0);
        assert!(reg.remove_user("test_unit"));
        assert!(reg.lookup_unit("test_unit").is_none());
    }
}
