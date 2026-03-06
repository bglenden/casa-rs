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
use std::sync::{OnceLock, RwLock};

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
    cache: RwLock<HashMap<String, UnitVal>>,
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
        }
    }

    /// Looks up a unit by name.
    ///
    /// Search order: defining → SI → customary → user → cache.
    /// The immutable built-in maps are checked first (lock-free) before
    /// falling back to the `RwLock`-protected user and cache maps.
    pub fn lookup_unit(&self, name: &str) -> Option<UnitVal> {
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
        // Check cache (compound/prefixed expressions that were previously parsed).
        if let Some(val) = self.cache.read().unwrap().get(name) {
            return Some(*val);
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

    /// Adds a parsed unit expression to the cache.
    pub fn cache_put(&self, name: &str, val: UnitVal) {
        self.cache.write().unwrap().insert(name.to_owned(), val);
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
