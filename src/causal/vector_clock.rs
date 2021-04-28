use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    hash::Hash,
    iter::{repeat, Peekable},
    marker::PhantomData,
};

use num::ToPrimitive;
use serde::{Deserialize, Serialize};

pub trait VectorClock: Default + PartialEq + PartialOrd {
    type K: Ord + Eq;
    type V: Ord + Eq;

    fn get_val(&self, key: Self::K) -> Self::V;
    fn set_entry(&mut self, key: Self::K, val: Self::V);
    fn max_in_place(&mut self, other: &Self);
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VecClock<K, V>(Vec<V>, PhantomData<K>);

impl<K, V> Default for VecClock<K, V> {
    fn default() -> Self {
        VecClock(vec![], PhantomData)
    }
}

impl<K, V> From<Vec<V>> for VecClock<K, V> {
    fn from(v: Vec<V>) -> Self {
        VecClock(v, PhantomData)
    }
}

impl<K: Ord + ToPrimitive, V: Ord + Eq + Default + Copy> VectorClock for VecClock<K, V> {
    type K = K;
    type V = V;

    #[inline(always)]
    fn get_val(&self, id: Self::K) -> V {
        let id = id.to_usize().unwrap();
        if id >= self.0.len() {
            return V::default();
        }
        self.0[id]
    }

    #[inline(always)]
    fn set_entry(&mut self, id: Self::K, val: V) {
        let l = self.0.len();
        let id = id.to_usize().unwrap();
        if id >= l {
            self.0
                .extend(repeat(V::default()).take((id + 1) as usize - l));
        }
        self.0[id] = val;
    }

    fn max_in_place(&mut self, other: &Self) {
        let l = self.0.len();
        let other_l = other.0.len();
        if other_l > l {
            self.0.extend(repeat(V::default()).take(other_l - l));
        }
        for (o_v, s_v) in other.0.iter().cloned().zip(self.0.iter_mut()) {
            if o_v > *s_v {
                *s_v = o_v;
            }
        }
    }
}

impl<K, V: Ord + Eq + Default + Copy> PartialEq for VecClock<K, V> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other)
            .map_or(false, |o| matches!(o, Ordering::Equal))
    }
}

impl<K, V: Default + Copy + Ord + Eq> PartialOrd for VecClock<K, V> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let dif = self.0.len() as isize - other.0.len() as isize;
        match dif.cmp(&0) {
            Ordering::Equal => check_eq_iter(self.0.iter().cloned().zip(other.0.iter().cloned())),
            Ordering::Less => check_eq_iter(
                self.0
                    .iter()
                    .cloned()
                    .chain(repeat(V::default()))
                    .zip(other.0.iter().cloned()),
            ),
            Ordering::Greater => check_eq_iter(
                self.0
                    .iter()
                    .cloned()
                    .zip(other.0.iter().cloned().chain(repeat(V::default()))),
            ),
        }
    }
}

fn check_eq_iter<V: Ord + Eq, I: Iterator<Item = (V, V)>>(iter: I) -> Option<Ordering> {
    let mut res = None;
    for (l, r) in iter {
        match l.cmp(&r) {
            Ordering::Less => {
                if let Some(o) = res {
                    if matches!(o, Ordering::Greater) {
                        return None;
                    }
                }
                res = Some(Ordering::Less)
            }
            Ordering::Equal => {
                let _ = res.get_or_insert(Ordering::Equal);
            }
            Ordering::Greater => {
                if let Some(o) = res {
                    if matches!(o, Ordering::Less) {
                        return None;
                    }
                }
                res = Some(Ordering::Greater)
            }
        }
    }
    res
}

struct OrdIter<K, V, I>
where
    I: Iterator<Item = (K, V)>,
{
    iter1: Peekable<I>,
    iter2: Peekable<I>,
}

impl<K, V, I> Iterator for OrdIter<K, V, I>
where
    V: Ord + Default,
    K: Ord,
    I: Iterator<Item = (K, V)>,
{
    type Item = (V, V);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter1.peek() {
            None => match self.iter2.peek() {
                None => None,
                Some(_) => Some((V::default(), self.iter2.next().unwrap().1)),
            },
            Some((k1, _)) => match self.iter2.peek() {
                None => Some((self.iter1.next().unwrap().1, V::default())),
                Some((k2, _)) => match k1.cmp(k2) {
                    Ordering::Less => Some((self.iter1.next().unwrap().1, V::default())),
                    Ordering::Equal => {
                        Some((self.iter1.next().unwrap().1, self.iter2.next().unwrap().1))
                    }
                    Ordering::Greater => Some((V::default(), self.iter2.next().unwrap().1)),
                },
            },
        }
    }
}

#[derive(Debug)]
struct VectorTree<K, V>(BTreeMap<K, V>);

impl<V> From<Vec<V>> for VectorTree<usize, V> {
    fn from(v: Vec<V>) -> Self {
        let m = v.into_iter().enumerate().collect();
        VectorTree(m)
    }
}

impl<K: Ord, V> Default for VectorTree<K, V> {
    fn default() -> Self {
        VectorTree(BTreeMap::default())
    }
}

impl<K: Ord + Clone, V: Clone + Default + Ord> PartialEq for VectorTree<K, V> {
    fn eq(&self, other: &Self) -> bool {
        let clo = |(k, v): (&K, &V)| (k.clone(), v.clone());
        let iter = OrdIter {
            iter1: self.0.iter().map(clo).peekable(),
            iter2: other.0.iter().map(clo).peekable(),
        };
        check_eq_iter(iter).map_or(false, |order| matches!(order, Ordering::Equal))
    }
}

impl<K: Ord + Clone, V: Clone + Default + Ord> PartialOrd for VectorTree<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let clo = |(k, v): (&K, &V)| (k.clone(), v.clone());
        let iter = OrdIter {
            iter1: self.0.iter().map(clo).peekable(),
            iter2: other.0.iter().map(clo).peekable(),
        };
        check_eq_iter(iter)
    }
}

impl<K: Ord + Clone + Eq, V: Default + Clone + Ord + Eq> VectorClock for VectorTree<K, V> {
    type K = K;
    type V = V;

    fn get_val(&self, key: Self::K) -> Self::V {
        self.0.get(&key).cloned().unwrap_or_default()
    }
    fn set_entry(&mut self, key: Self::K, val: Self::V) {
        self.0.insert(key, val);
    }
    fn max_in_place(&mut self, other: &Self) {
        for (k, v) in other.0.iter() {
            self.0
                .entry(k.clone())
                .and_modify(|prev_v| {
                    if *prev_v < *v {
                        *prev_v = v.clone();
                    }
                })
                .or_insert_with(|| v.clone());
        }
    }
}

#[derive(Debug)]
struct VectorMap<K, V>(HashMap<K, V>);

impl<V> From<Vec<V>> for VectorMap<usize, V> {
    fn from(v: Vec<V>) -> Self {
        let m = v.into_iter().enumerate().collect();
        VectorMap(m)
    }
}

impl<K: Ord, V> Default for VectorMap<K, V> {
    fn default() -> Self {
        VectorMap(HashMap::default())
    }
}

impl<K: Ord + Clone + Hash, V: Clone + Default + Ord> PartialEq for VectorMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        let iter = OrdMapIter::new(&self, &other);
        check_eq_iter(iter).map_or(false, |order| matches!(order, Ordering::Equal))
    }
}

impl<K: Ord + Clone + Hash, V: Clone + Default + Ord> PartialOrd for VectorMap<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let iter = OrdMapIter::new(&self, &other);
        check_eq_iter(iter)
    }
}

impl<K: Hash + Ord + Clone + Eq, V: Default + Clone + Ord + Eq> VectorClock for VectorMap<K, V> {
    type K = K;
    type V = V;

    fn get_val(&self, key: Self::K) -> Self::V {
        self.0.get(&key).cloned().unwrap_or_default()
    }
    fn set_entry(&mut self, key: Self::K, val: Self::V) {
        self.0.insert(key, val);
    }
    fn max_in_place(&mut self, other: &Self) {
        for (k, v) in other.0.iter() {
            self.0
                .entry(k.clone())
                .and_modify(|prev_v| {
                    if *prev_v < *v {
                        *prev_v = v.clone();
                    }
                })
                .or_insert_with(|| v.clone());
        }
    }
}

type OrdIterInternal<'a, K, V> =
    std::iter::Map<std::collections::hash_map::Iter<'a, K, V>, fn((&K, &V)) -> (K, V)>;
struct OrdMapIter<'a, K, V> {
    iter1: OrdIterInternal<'a, K, V>,
    m1: &'a VectorMap<K, V>,
    iter2: OrdIterInternal<'a, K, V>,
    m2: &'a VectorMap<K, V>,
}

fn clo<K: Clone, V: Clone>((k, v): (&K, &V)) -> (K, V) {
    (k.clone(), v.clone())
}

impl<'a, K, V> OrdMapIter<'a, K, V>
where
    K: Clone,
    V: Clone,
{
    fn new(m1: &'a VectorMap<K, V>, m2: &'a VectorMap<K, V>) -> Self {
        Self {
            iter1: m1.0.iter().map(clo),
            m1,
            iter2: m2.0.iter().map(clo),
            m2,
        }
    }
}

impl<'a, K, V> Iterator for OrdMapIter<'a, K, V>
where
    V: Ord + Default + Clone,
    K: Ord + Hash,
{
    type Item = (V, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.iter1.next() {
            Some((v, self.m2.0.get(&k).cloned().unwrap_or_default()))
        } else if let Some((k, v)) = self.iter2.next() {
            Some((self.m1.0.get(&k).cloned().unwrap_or_default(), v))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {

    use super::{VecClock, VectorClock, VectorMap, VectorTree};
    use std::fmt::Debug;

    #[test]
    fn vector_clock() {
        vec_clock::<VecClock<usize, usize>>();
        vec_clock::<VectorTree<usize, usize>>();
        vec_clock::<VectorMap<usize, usize>>();
    }

    fn vec_clock<V: Debug + VectorClock<K = usize, V = usize> + From<Vec<usize>>>() {
        assert!(V::from(vec![1, 2, 3, 4]) == V::from(vec![1, 2, 3, 4]));
        assert!(V::from(vec![1, 2, 3, 4]) == V::from(vec![1, 2, 3, 4, 0, 0, 0]));
        assert!(V::from(vec![2, 2, 3, 4]) >= V::from(vec![1, 2, 3, 4]));
        assert!(V::from(vec![2, 2, 3, 4, 5]) > V::from(vec![1, 2, 3, 4]));
        assert!(V::from(vec![1, 2, 3, 4]) > V::from(vec![]));
        assert!(V::from(vec![]) < V::from(vec![1, 2, 3, 4]));

        assert!(V::from(vec![1, 2])
            .partial_cmp(&V::from(vec![2, 1]))
            .is_none());
        assert!(V::from(vec![1, 1, 1])
            .partial_cmp(&V::from(vec![2, 2]))
            .is_none());

        let mut v1 = V::default();
        let mut v2 = V::default();
        for (i, v) in (0..10).rev().enumerate() {
            v1.set_entry(v, i);
            v2.set_entry(i, v);
        }
        assert!(v1 == v2);

        let mut v1 = V::from(vec![1, 2, 3, 4]);
        let v2 = V::from(vec![4, 1, 1, 1, 1]);
        v1.max_in_place(&v2);
        assert_eq!(v1, V::from(vec![4, 2, 3, 4, 1]));

        let mut v1 = V::from(vec![]);
        v1.set_entry(10, 15);
        v1.set_entry(1, 5);

        let mut v2 = V::from(vec![]);
        v2.set_entry(5, 6);

        assert!(v1.partial_cmp(&v2).is_none());
        v1.set_entry(5, 6);
        assert!(v1 > v2);
        v2.set_entry(10, 15);
        assert!(v1 > v2);
        v2.set_entry(1, 5);
        assert_eq!(v1, v2);
    }
}
