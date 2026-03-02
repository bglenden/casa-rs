#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StorageKey(String);

impl StorageKey {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
