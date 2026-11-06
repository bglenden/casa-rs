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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    user_generation: u64,
}

/// An opaque version of the process-wide unit definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitState {
    fits_enabled: bool,
    user_generation: u64,
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
    /// User definitions may explicitly override otherwise immutable units.
    has_builtin_overrides: AtomicBool,
    /// Changes whenever a user definition is inserted, replaced or removed.
    user_generation: AtomicU64,
    /// Stateful operations hold a read lease while definitions mutate under a
    /// write lease. Pure quanta operations can instead retry on a state change.
    operation: RwLock<()>,
}

impl UnitRegistry {
    /// Whether a user definition has ever overridden a built-in unit.
    ///
    /// This is conservative: it remains true after that definition is
    /// removed. FITS aliases are tracked separately and may override `S`
    /// even when this returns false.
    pub fn has_user_builtin_overrides(&self) -> bool {
        self.has_builtin_overrides.load(Ordering::Acquire)
    }

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
            has_builtin_overrides: AtomicBool::new(false),
            user_generation: AtomicU64::new(0),
            operation: RwLock::new(()),
        }
    }

    /// Looks up a unit by name.
    ///
    /// User definitions override built-in names. The immutable built-in maps
    /// remain lock-free until a caller explicitly overrides one.
    pub fn lookup_unit(&self, name: &str) -> Option<UnitVal> {
        // FITS redefines `S` from siemens to seconds. CASA's user map takes
        // precedence for that name, even if a caller already defined it.
        if (self.has_builtin_overrides.load(Ordering::Acquire)
            || (name == "S" && self.fits_enabled.load(Ordering::Acquire)))
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
            && cached.user_generation == self.user_generation()
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
    /// FITS and user-definition state. The caller must ensure `val` was
    /// computed in that state; the parser captures it before it begins.
    pub fn cache_put(&self, name: &str, val: UnitVal) {
        self.cache_put_at_state(name, val, self.cache_fits_state(), self.user_generation());
    }

    /// Capture the relevant registry state before parsing a cache entry.
    pub(crate) fn cache_fits_state(&self) -> bool {
        self.fits_enabled.load(Ordering::Acquire)
    }

    /// Capture the current user-definition generation.
    pub(crate) fn user_generation(&self) -> u64 {
        self.user_generation.load(Ordering::Acquire)
    }

    /// Capture a version that changes when unit definitions change.
    pub fn unit_state(&self) -> UnitState {
        UnitState {
            fits_enabled: self.cache_fits_state(),
            user_generation: self.user_generation(),
        }
    }

    /// Repeats a read-only operation if unit definitions changed while it ran.
    ///
    /// `work` must have no externally visible side effects because it may run
    /// more than once. This keeps multiple parses in a single operation on one
    /// registry generation, including a concurrent FITS registration.
    pub fn with_stable_units<T>(&self, mut work: impl FnMut() -> T) -> T {
        loop {
            let state = self.unit_state();
            let result = work();
            if state == self.unit_state() {
                return result;
            }
        }
    }

    /// Run a stateful operation while user definitions remain unchanged.
    /// The closure must not mutate this unit registry.
    pub fn with_locked_units<T>(&self, work: impl FnOnce() -> T) -> T {
        let _operation = self.operation.read().unwrap();
        work()
    }

    /// Cache the value with the registry state captured before its parse.
    pub(crate) fn cache_put_at_state(
        &self,
        name: &str,
        val: UnitVal,
        fits_enabled: bool,
        user_generation: u64,
    ) {
        self.cache.write().unwrap().insert(
            name.to_owned(),
            CachedUnit {
                value: val,
                fits_enabled,
                user_generation,
            },
        );
    }

    /// Installs casacore's process-wide FITS unit aliases once.
    ///
    /// Existing user definitions are retained, as in `UnitMap::addFITS`.
    /// Unless already user-defined, `S` changes from SI siemens to seconds.
    pub fn add_fits_units(&self) {
        self.fits_once.call_once(|| {
            let _operation = self.operation.write().unwrap();
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
    /// replaced. An explicit user definition takes precedence over a built-in.
    pub fn put_user(&self, name: impl Into<String>, val: UnitVal, desc: impl Into<String>) {
        let _operation = self.operation.write().unwrap();
        self.put_user_locked(name.into(), val, desc.into());
    }

    /// Evaluate and install a user definition as one serialized mutation.
    /// The value-producing closure must not itself mutate this registry.
    pub fn define_user<E>(
        &self,
        name: impl Into<String>,
        desc: impl Into<String>,
        value: impl FnOnce() -> Result<UnitVal, E>,
    ) -> Result<(), E> {
        let _operation = self.operation.write().unwrap();
        self.put_user_locked(name.into(), value()?, desc.into());
        Ok(())
    }

    fn put_user_locked(&self, name: String, val: UnitVal, desc: String) {
        let overrides_builtin = self.defining.contains_key(&name)
            || self.si.contains_key(&name)
            || self.customary.contains_key(&name);
        let mut user = self.user.write().unwrap();
        user.insert(name.clone(), UnitName::new(name, val, desc));
        if overrides_builtin {
            self.has_builtin_overrides.store(true, Ordering::Release);
        }
        self.user_generation.fetch_add(1, Ordering::Release);
        drop(user);
        // Invalidate the cache since user units affect lookup.
        self.cache.write().unwrap().clear();
    }

    /// Removes a user-defined unit.
    ///
    /// Returns `true` if the unit existed and was removed.
    pub fn remove_user(&self, name: &str) -> bool {
        let _operation = self.operation.write().unwrap();
        let mut user = self.user.write().unwrap();
        let removed = user.remove(name).is_some();
        if removed {
            self.user_generation.fetch_add(1, Ordering::Release);
            drop(user);
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
    fn builtin_override_hint_is_conservative_and_excludes_fits_aliases() {
        let reg = UnitRegistry::from_builtin();
        assert!(!reg.has_user_builtin_overrides());
        reg.put_user("novel", UnitVal::NODIM, "a user unit");
        assert!(!reg.has_user_builtin_overrides());
        reg.add_fits_units();
        assert!(!reg.has_user_builtin_overrides());
        reg.put_user("Hz", UnitVal::TIME, "override a built-in");
        assert!(reg.has_user_builtin_overrides());
        assert!(reg.remove_user("Hz"));
        assert!(reg.has_user_builtin_overrides());
    }

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
        reg.cache_put_at_state("S.m", siemens, false, reg.user_generation());
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
    fn a_late_cache_write_cannot_restore_an_old_user_definition() {
        let reg = UnitRegistry::from_builtin();
        reg.put_user("custom", UnitVal::new(2.0, UnitDim::NODIM), "test");
        let old_generation = reg.user_generation();
        reg.put_user("custom", UnitVal::new(3.0, UnitDim::NODIM), "test");
        // A parse started before redefinition can finish after cache clearing.
        reg.cache_put_at_state(
            "custom.custom",
            UnitVal::new(4.0, UnitDim::NODIM),
            false,
            old_generation,
        );
        assert!(reg.lookup_unit("custom.custom").is_none());
        reg.cache_put("custom.custom", UnitVal::new(9.0, UnitDim::NODIM));
        assert_eq!(reg.lookup_unit("custom.custom").unwrap().factor, 9.0);
        assert!(reg.remove_user("custom"));
        assert!(reg.lookup_unit("custom.custom").is_none());
    }

    #[test]
    fn a_read_operation_retries_if_a_definition_changes_between_its_inputs() {
        let reg = UnitRegistry::from_builtin();
        reg.put_user("sample", UnitVal::new(2.0, UnitDim::NODIM), "test");
        let mut changed = false;
        let ratio = reg.with_stable_units(|| {
            let from = reg.lookup_unit("sample").unwrap().factor;
            if !changed {
                changed = true;
                reg.put_user("sample", UnitVal::new(3.0, UnitDim::NODIM), "test");
            }
            let to = reg.lookup_unit("sample").unwrap().factor;
            from / to
        });
        assert_eq!(ratio, 1.0);
    }

    #[test]
    fn concurrent_definitions_resolve_and_publish_as_serial_transactions() {
        use std::sync::mpsc;
        use std::time::Duration;

        let reg = UnitRegistry::from_builtin();
        reg.put_user("first", UnitVal::new(2.0, UnitDim::NODIM), "test");
        reg.put_user("second", UnitVal::new(3.0, UnitDim::NODIM), "test");
        let (first_read, wait_for_first) = mpsc::channel();
        let (release_first, first_wait) = mpsc::channel();
        let (attempt_second, wait_for_attempt) = mpsc::channel();
        let (second_read, wait_for_second) = mpsc::channel();
        std::thread::scope(|scope| {
            let first_registry = &reg;
            scope.spawn(move || {
                first_registry
                    .define_user("first", "test", || {
                        let value = first_registry.lookup_unit("second").unwrap();
                        first_read.send(()).unwrap();
                        first_wait.recv().unwrap();
                        Ok::<_, ()>(value)
                    })
                    .unwrap();
            });
            wait_for_first.recv().unwrap();
            let second_registry = &reg;
            scope.spawn(move || {
                attempt_second.send(()).unwrap();
                second_registry
                    .define_user("second", "test", || {
                        let value = second_registry.lookup_unit("first").unwrap();
                        second_read.send(()).unwrap();
                        Ok::<_, ()>(value)
                    })
                    .unwrap();
            });
            wait_for_attempt.recv().unwrap();
            let second_entered_early = wait_for_second
                .recv_timeout(Duration::from_millis(100))
                .is_ok();
            release_first.send(()).unwrap();
            if !second_entered_early {
                wait_for_second.recv().unwrap();
            }
            assert!(!second_entered_early);
        });
        assert_eq!(reg.lookup_unit("first").unwrap().factor, 3.0);
        assert_eq!(reg.lookup_unit("second").unwrap().factor, 3.0);
    }

    #[test]
    fn a_stateful_operation_holds_one_definition_until_it_finishes() {
        use std::sync::mpsc;
        use std::time::Duration;

        let reg = UnitRegistry::from_builtin();
        reg.put_user("sample", UnitVal::new(2.0, UnitDim::NODIM), "test");
        let (reader_started, wait_for_reader) = mpsc::channel();
        let (release_reader, reader_wait) = mpsc::channel();
        let (writer_started, wait_for_writer) = mpsc::channel();
        let (writer_done, wait_for_done) = mpsc::channel();
        std::thread::scope(|scope| {
            let reader_registry = &reg;
            scope.spawn(move || {
                reader_registry.with_locked_units(|| {
                    let first = reader_registry.lookup_unit("sample").unwrap().factor;
                    reader_started.send(()).unwrap();
                    reader_wait.recv().unwrap();
                    let second = reader_registry.lookup_unit("sample").unwrap().factor;
                    assert_eq!((first, second), (2.0, 2.0));
                });
            });
            wait_for_reader.recv().unwrap();
            let writer_registry = &reg;
            scope.spawn(move || {
                writer_started.send(()).unwrap();
                writer_registry.put_user("sample", UnitVal::new(3.0, UnitDim::NODIM), "test");
                writer_done.send(()).unwrap();
            });
            wait_for_writer.recv().unwrap();
            let writer_finished_early = wait_for_done
                .recv_timeout(Duration::from_millis(100))
                .is_ok();
            release_reader.send(()).unwrap();
            if !writer_finished_early {
                wait_for_done.recv().unwrap();
            }
            assert!(!writer_finished_early);
        });
        assert_eq!(reg.lookup_unit("sample").unwrap().factor, 3.0);
    }

    #[test]
    fn explicit_user_definition_overrides_a_builtin_and_removal_restores_it() {
        let reg = UnitRegistry::from_builtin();
        assert_eq!(reg.lookup_unit("m").unwrap().dim, UnitVal::LENGTH.dim);
        reg.put_user("m", UnitVal::new(2.0, UnitVal::TIME.dim), "override");
        let value = reg.lookup_unit("m").unwrap();
        assert_eq!((value.factor, value.dim), (2.0, UnitVal::TIME.dim));
        assert!(reg.remove_user("m"));
        assert_eq!(reg.lookup_unit("m").unwrap().dim, UnitVal::LENGTH.dim);
    }

    #[test]
    fn fits_registration_rejects_a_late_cache_write_for_other_aliases() {
        let reg = UnitRegistry::from_builtin();
        reg.put_user("IXEL", UnitVal::new(2.0, UnitVal::TIME.dim), "test");
        // Before FITS, PIXEL can parse as peta-IXEL. A parse that began then
        // may finish and write its result after the PIXEL alias is installed.
        let stale = UnitVal::new(2.0e15, (UnitVal::TIME * UnitVal::LENGTH).dim);
        reg.add_fits_units();
        reg.cache_put_at_state("PIXEL.m", stale, false, reg.user_generation());
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
