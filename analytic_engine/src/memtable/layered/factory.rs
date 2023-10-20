// Copyright 2023 The CeresDB Authors
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

//! Skiplist memtable factory

use std::sync::{atomic::AtomicU64, Arc};

use arena::MonoIncArena;
use skiplist::{BytewiseComparator, Skiplist};

use crate::memtable::{
    factory::{Factory, FactoryRef, Options},
    layered::LayeredMemTable,
    skiplist::SkiplistMemTable,
    MemTableRef, Result,
};
/// Factory to create memtable
#[derive(Debug)]
pub struct LayeredMemtableFactory {
    inner_memtable_factory: FactoryRef,
    mutable_switch_threshold: usize,
}

impl Factory for LayeredMemtableFactory {
    fn create_memtable(&self, opts: Options) -> Result<MemTableRef> {
        let memtable = LayeredMemTable::new(
            &opts,
            self.inner_memtable_factory.clone(),
            self.mutable_switch_threshold,
        )?;

        Ok(Arc::new(memtable))
    }
}
