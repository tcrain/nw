use std::io::Error;
use std::time::SystemTime;
use crate::verification::{Verify, Hash};
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::borrow::Borrow;
use std::slice::Iter;

enum PrevEntry {
    SP(OuterSP),
    TX(OuterTX),
}

impl PrevEntry {
    fn prev_iterator(&self) -> LogIterator {
        match self {
            PrevEntry::SP(sp) => {
                LogIterator{
                    prv_entry: Rc::clone(&sp.prev_entry.borrow()),
                }
            }
            PrevEntry::TX(tx) => {
                LogIterator{
                    prv_entry: Rc::clone(&tx.prev_entry.borrow()),
                }
            }
        }
    }
}

pub struct OuterTX {
    tx: TX,
    log_index: u64,
    hash: Hash,
    verification: Verify,
    prev_tx: RefCell<Weak<OuterTX>>,
    next_tx: RefCell<Weak<OuterTX>>,
    prev_entry: RefCell<Rc<Option<PrevEntry>>>,
}

pub struct OuterSP {
    sp: SP,
    log_index: u64,
    not_included_tx: Vec<Rc<OuterTX>>,
    prev_local: RefCell<Weak<OuterSP>>,
    prev_to: RefCell<Weak<OuterSP>>,
    prev_entry: RefCell<Rc<Option<PrevEntry>>>,
}

impl OuterSP {
    fn sp_prev_local_iterator(&self) -> SPPrevLocal {
        return SPPrevLocal{
            prv_outer_sp: self.prev_local.borrow().upgrade(),
        }
    }
}

struct LogIterator {
    prv_entry: Rc<Option<PrevEntry>>
}

impl<'a> Iterator for LogIterator {
    type Item = Option<&'a PrevEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.prv_entry.borrow() {
            None => None,
            Some(prv) => {
                self.prv_entry = match prv {
                    PrevEntry::SP(sp) => Rc::clone(&sp.prev_entry.borrow()),
                    PrevEntry::TX(tx) => Rc::clone(&tx.prev_entry.borrow())
                }
            }
        }
    }
}

struct SPPrevLocal {
    prv_outer_sp: Option<RC<OuterSP>>,
}

impl <'a> Iterator for SPPrevLocal {
    type Item = &'a OuterSP;

    fn next(&mut self) -> Option<Self::Item> {
        match self.prv_outer_sp {
            None => None,
            Some(prv) => {
                self.prv_outer_sp = prv.prev_local.borrow().upgrade();
                Some(prv)
            }
        }
    }
}

impl SPResult {
    pub fn prev_sp() -> SPPrevLocal {
        SPPrevLocal
    }
}

pub struct Log {

}

impl Log {
    pub fn new() -> Log {
        Log
    }

    pub fn insert_tx() -> Result<(), Error> {
        Ok(())
    }

    pub fn insert_sp(_sp: SP) -> Result<&OuterSP, Error> {

    }
}