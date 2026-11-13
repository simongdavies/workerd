// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Trusted, one-shot admission of one application Worker isolate.

use std::cell::Cell;
use std::fmt;

enum Source {
    Configured(u32),
    Dynamic,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Pending,
    Reserved,
    Created,
}

/// A factory's immutable source selection. A failed initialization consumes its admission;
/// retrying requires a new owner rather than another isolate in the same factory.
pub struct WorkerAdmission {
    source: Source,
    name: String,
    state: Cell<State>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionError {
    EmptyName,
    WrongSource,
    AlreadyStarted,
    NotReserved,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::EmptyName => "the selected Worker name must not be empty",
            Self::WrongSource => "the Worker source does not match the factory's admission",
            Self::AlreadyStarted => "the factory's Worker admission has already been consumed",
            Self::NotReserved => "isolate creation requires a reserved Worker admission",
        })
    }
}

impl std::error::Error for AdmissionError {}

impl WorkerAdmission {
    pub fn configured(index: u32, name: String) -> Result<Self, AdmissionError> {
        Self::new(Source::Configured(index), name)
    }

    pub fn dynamic(name: String) -> Result<Self, AdmissionError> {
        Self::new(Source::Dynamic, name)
    }

    fn new(source: Source, name: String) -> Result<Self, AdmissionError> {
        if name.is_empty() {
            return Err(AdmissionError::EmptyName);
        }
        Ok(Self {
            source,
            name,
            state: Cell::new(State::Pending),
        })
    }

    fn check_source(&self, configured: bool, index: u32, name: &str) -> Result<(), AdmissionError> {
        let matches = match self.source {
            Source::Configured(expected) => configured && index == expected,
            Source::Dynamic => !configured,
        };
        if matches && name == self.name {
            Ok(())
        } else {
            Err(AdmissionError::WrongSource)
        }
    }

    /// Reserves before fetching a dynamic source or awaiting initialization. Competing
    /// startups cannot both pass admission while neither has constructed its isolate yet.
    pub fn begin_worker(
        &self,
        configured: bool,
        index: u32,
        name: &str,
    ) -> Result<(), AdmissionError> {
        self.check_source(configured, index, name)?;
        if self.state.get() != State::Pending {
            return Err(AdmissionError::AlreadyStarted);
        }
        self.state.set(State::Reserved);
        Ok(())
    }

    /// Called immediately before constructing the actual `Worker::Isolate`.
    pub fn admit_isolate(
        &self,
        configured: bool,
        index: u32,
        name: &str,
    ) -> Result<(), AdmissionError> {
        self.check_source(configured, index, name)?;
        if self.state.get() != State::Reserved {
            return Err(AdmissionError::NotReserved);
        }
        self.state.set(State::Created);
        Ok(())
    }
}

#[cfg(test)]
#[path = "admission-test.rs"]
mod tests;
