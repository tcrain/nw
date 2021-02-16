use std::{cell::RefCell, rc::Rc};

use rustc_hash::FxHashMap;

pub const DEFAULT_DISK_ENTRIES: usize = 64;
pub const DEFAULT_MAX_ENTRIES: usize = 64;

#[inline(always)]
fn is_pow2(x: usize) -> bool {
    x != 0 && x & (x - 1) == 0
}

#[inline(always)]
fn is_pow2_u64(x: u64) -> bool {
    x != 0 && x & (x - 1) == 0
}

#[inline(always)]
fn modulo_pow2(x: usize, d: usize) -> usize {
    debug_assert!(is_pow2(d));
    debug_assert!(x & (d - 1) == x % d);
    x & (d - 1)
}

#[inline(always)]
fn modulo_pow2_u64(x: u64, d: u64) -> u64 {
    debug_assert!(is_pow2_u64(d));
    debug_assert!(x & (d - 1) == x % d);
    x & (d - 1)
}

struct HItems<T> {
    map: FxHashMap<u64, (usize, Rc<RefCell<T>>)>,
    entries_id: Vec<Option<u64>>,
    entries_idx: usize,
}

impl<T> Default for HItems<T> {
    fn default() -> Self {
        HItems::new(DEFAULT_DISK_ENTRIES)
    }
}

impl<T> HItems<T> {
    pub fn new(num_disk_entries: usize) -> Self {
        assert!(is_pow2(num_disk_entries) && num_disk_entries > 2);
        HItems {
            map: FxHashMap::default(),
            entries_id: vec![None; num_disk_entries],
            entries_idx: 0,
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.entries_idx = 0;
        for nxt in self.entries_id.iter_mut() {
            *nxt = None;
        }
    }

    pub fn entries_count(&self) -> usize {
        self.map.len()
    }

    pub fn get(&self, id: u64) -> Option<&Rc<RefCell<T>>> {
        self.map.get(&id).map(|e| &e.1)
    }

    /// store a log entry that was loaded from disk, up to num_disk_entries of items loaded from disk
    /// will be stored in the hash table
    pub fn store(
        &mut self,
        id: u64,
        item: Rc<RefCell<T>>,
    ) -> (Option<Rc<RefCell<T>>>, Option<u64>) {
        let entries_idx = self.entries_idx;

        // see if there was an entry previously in this index
        let old_id = self.entries_id[entries_idx];
        if let Some(oid) = old_id.as_ref() {
            self.map.remove(oid).unwrap();
        }
        // add to the entires array
        self.entries_id[entries_idx] = Some(id);
        let ret = self.map.insert(id, (entries_idx, item));

        // update the index for the next item
        self.entries_idx += 1;
        let l = self.entries_id.len();
        if self.entries_idx >= l {
            self.entries_idx = modulo_pow2(self.entries_idx, l)
        }
        (ret.map(|(_, entry)| entry), old_id)
    }
}

pub struct HashItems<T> {
    disk_entries: HItems<T>,
    recent_entries: HItems<T>,
    max_recent: u64,
    min_recent: Option<u64>,
}

impl<T> Default for HashItems<T> {
    fn default() -> Self {
        HashItems::new(DEFAULT_DISK_ENTRIES, DEFAULT_MAX_ENTRIES)
    }
}

impl<T> HashItems<T> {
    pub fn new(num_disk_entries: usize, num_recent_entries: usize) -> Self {
        assert!(is_pow2(num_disk_entries) && num_disk_entries > 2);
        assert!(is_pow2(num_recent_entries) && num_recent_entries > 2);
        HashItems {
            disk_entries: HItems::new(num_disk_entries),
            recent_entries: HItems::new(num_recent_entries),
            max_recent: 0,
            min_recent: None,
        }
    }

    pub fn clear(&mut self) {
        self.disk_entries.clear();
        self.recent_entries.clear();
        self.min_recent = None;
        self.max_recent = 0;
    }

    pub fn recent_entries_count(&self) -> usize {
        self.recent_entries.entries_count()
    }

    pub fn disk_entries_count(&self) -> usize {
        self.disk_entries.entries_count()
    }

    pub fn get(&self, id: u64) -> Option<&Rc<RefCell<T>>> {
        if let Some(min) = self.min_recent {
            if id >= min && id <= self.max_recent {
                let rc = self.recent_entries.get(id);
                debug_assert!(rc.is_some());
                return rc;
            }
        }
        self.disk_entries.get(id)
    }

    /// store a log entry that was loaded from disk, up to num_disk_entries of items loaded from disk
    /// will be stored in the hash table
    pub fn store_from_disk(&mut self, id: u64, item: Rc<RefCell<T>>) -> Option<Rc<RefCell<T>>> {
        self.disk_entries.store(id, item).0
    }

    pub fn store_recent(
        &mut self,
        id: u64,
        item: Rc<RefCell<T>>,
    ) -> (Option<Rc<RefCell<T>>>, Option<u64>) {
        self.max_recent = id;
        let (ret, rem) = self.recent_entries.store(id, item);
        if let Some(v) = rem {
            self.min_recent = Some(v + 1);
        } else {
            self.min_recent.get_or_insert(id);
        }
        (ret, rem)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, cmp, collections::VecDeque, rc::Rc};

    use rand::{prelude::StdRng, Rng, SeedableRng};

    use super::HashItems;

    type IdPtr = Rc<RefCell<u64>>;

    fn new_id_ptr(id: u64) -> IdPtr {
        Rc::new(RefCell::new(id))
    }

    #[test]
    fn append_recent() {
        let mut hi: HashItems<u64> = HashItems::new(16, 16);
        for i in 0..50 {
            let (rep, rem) = hi.store_recent(i, new_id_ptr(i));
            assert!(rep.is_none());
            if i >= 16 {
                assert_eq!(i - 16, rem.unwrap());
            }
            for j in 0..=i {
                let nxt = hi.get(j);
                if j >= i.saturating_sub(15) {
                    assert_eq!(j, *nxt.unwrap().as_ref().borrow());
                } else {
                    assert!(nxt.is_none());
                }
            }
            let count = hi.recent_entries_count();
            assert_eq!(cmp::min(i + 1, 16), count as u64);
        }
    }

    struct Queue {
        v: VecDeque<u64>,
        max: usize,
    }

    impl Queue {
        fn new(max: usize) -> Self {
            Queue {
                v: VecDeque::new(),
                max,
            }
        }

        fn push(&mut self, v: u64) -> Option<u64> {
            self.v.push_back(v);
            if self.v.len() > self.max {
                self.v.pop_front()
            } else {
                None
            }
        }
    }

    #[test]
    fn insert_disk() {
        let entries = 16;
        let mut hi: HashItems<u64> = HashItems::new(entries, entries);
        let mut rng = StdRng::seed_from_u64(100);
        let mut q = Queue::new(entries);

        for _ in 1..100 {
            let nxt = rng.gen();
            let _ = hi.store_from_disk(nxt, new_id_ptr(nxt));
            let old_q = q.push(nxt);

            if let Some(v) = old_q {
                assert!(hi.get(v).is_none());
            }
            for &v in q.v.iter() {
                let nxt = hi.get(v).unwrap();
                assert_eq!(v, *nxt.as_ref().borrow());
            }
            assert_eq!(q.v.len(), hi.disk_entries_count());
        }
    }
}
