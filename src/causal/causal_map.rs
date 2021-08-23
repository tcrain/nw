use std::{collections::HashMap, hash::Hash};
use std::{collections::HashSet, fmt::Debug};

use bincode::deserialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    log::{
        op::{OpData, OpEntryInfo},
        ordered_log::{OrderedState, OrderingError, Result},
    },
    verification::Id,
};

const KEY_SIZE: usize = 32;

// type Key = [u8; KEY_SIZE];
type HMap<K, V> = HashMap<K, V>;
type HSet<K> = HashSet<K>;

pub trait Key: Serialize + DeserializeOwned + Eq + Hash + Debug {}
impl<T: Serialize + DeserializeOwned + Eq + Hash + Debug> Key for T {}

pub trait SendValue: Serialize + DeserializeOwned + Debug {}
impl<T: Serialize + DeserializeOwned + Debug> SendValue for T {}

pub trait Value: Default {
    type Sv: SendValue;

    fn process_new_val(&mut self, other: Self::Sv);
}
pub struct CausalMap<K, V> {
    map: HMap<K, V>,
}

impl<K, V> Default for CausalMap<K, V> {
    fn default() -> Self {
        CausalMap {
            map: HMap::default(),
        }
    }
}

impl<K: Key, V: Value> CausalMap<K, V> {
    fn deser_kv(&self, op: &OpData) -> Result<(K, V::Sv)> {
        let k: K = deserialize(op).map_err(|e| OrderingError::wrap_deser_error(e))?;
        let s: V::Sv = deserialize(op).map_err(|e| OrderingError::wrap_deser_error(e))?;
        Ok((k, s))
    }

    fn process_op(&mut self, op: &OpEntryInfo) {
        let (k, s) = self.deser_kv(&op.op.data).expect("should have checked op");
        self.map.entry(k).or_default().process_new_val(s);
    }
}

impl<K: Key, V: Value> OrderedState for CausalMap<K, V> {
    fn check_op(&mut self, op: &OpData) -> Result<()> {
        self.deser_kv(op).map(|(_, _)| ())
    }

    fn received_op(&mut self, _op: &OpEntryInfo) {
        // no pre operations
    }

    fn after_recv_sp<'a, I: Iterator<Item = &'a OpEntryInfo>>(&mut self, _from: Id, deps: I) {
        for op in deps {
            self.process_op(op)
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct LastWrtierWins<V>(V);

impl<V: Default> Default for LastWrtierWins<V> {
    fn default() -> Self {
        LastWrtierWins(V::default())
    }
}

impl<V: SendValue + Default> Value for LastWrtierWins<V> {
    type Sv = V;

    fn process_new_val(&mut self, other: Self::Sv) {
        self.0 = other;
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct KeepAll<V>(Vec<V>);

impl<V> Default for KeepAll<V> {
    fn default() -> Self {
        KeepAll(Vec::default())
    }
}

impl<V: SendValue> Value for KeepAll<V> {
    type Sv = V;

    fn process_new_val(&mut self, other: Self::Sv) {
        self.0.push(other);
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CounterInc(usize);

struct Counter(usize);

impl Default for Counter {
    fn default() -> Self {
        Counter(0)
    }
}

impl Value for Counter {
    type Sv = CounterInc;

    fn process_new_val(&mut self, v: Self::Sv) {
        self.0 += v.0
    }
}

enum OpType<V> {
    Add(V),
    Remove(V),
}

#[derive(Serialize, Deserialize, Debug)]
struct AddWins<V: Hash + Eq>(HSet<V>);

impl<V: Hash + Eq> Default for AddWins<V> {
    fn default() -> Self {
        AddWins(HSet::default())
    }
}

impl<V: SendValue + Hash + Eq> Value for AddWins<V> {
    type Sv = V;

    fn process_new_val(&mut self, _v: Self::Sv) {}
}
