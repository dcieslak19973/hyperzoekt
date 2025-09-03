// Copyright 2025 HyperZoekt Project
// Derived from sourcegraph/zoekt (https://github.com/sourcegraph/zoekt)
// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod builder;
pub mod error;
mod git;
pub mod in_memory;
pub(crate) mod process;
mod utils;

pub use builder::IndexBuilder;
pub use error::IndexError;
pub use in_memory::{InMemoryIndex, InMemoryIndexInner};

pub type RepoDocId = u32;
// Re-export process types for crate-internal test helpers
pub(crate) use process::PendingFile;
