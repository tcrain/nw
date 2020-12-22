use std::rc::{Rc, Weak};
use std::cell::{RefCell, Ref};
use std::borrow::Borrow;
use std::ops::Deref;
use std::fmt::{Display, Formatter};
use std::{fmt, mem, io};
use crate::errors::{Error, LogError};

enum PrevEntry {
    SP(OuterSP),
    TX(OuterTX),
}

impl Display for PrevEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrevEntry::SP(sp) => write!(f, "{}", sp),
            PrevEntry::TX(tx) => write!(f, "{}", tx)
        }
    }
}

impl Drop for PrevEntry {
    fn drop(&mut self) {
        println!("drop {}", self)
    }
}

impl PrevEntry {

    fn get_next(&self) -> Option<PrevEntryStrong> {
        match self {
            PrevEntry::SP(sp) => sp.log_pointers.get_next(),
            PrevEntry::TX(tx) => tx.log_pointers.get_next(),
        }
    }

    fn set_next(&mut self, next: &PrevEntryStrong) {
        match self {
            PrevEntry::SP(sp) => {
                sp.log_pointers.next_entry = Some(Rc::downgrade(next));
            },
            PrevEntry::TX(tx) => {
                tx.log_pointers.next_entry = Some(Rc::downgrade(next));
            },
        };
    }

    fn drop_previous(&mut self) {
        match self {
            PrevEntry::SP(sp) => sp.log_pointers.prev_entry = None,
            PrevEntry::TX(tx) => tx.log_pointers.prev_entry = None,
        }
    }

    fn get_prev(&self) -> Option<PrevEntryStrong> {
        match self {
            PrevEntry::SP(sp) => sp.log_pointers.get_prev(),
            PrevEntry::TX(tx) => tx.log_pointers.get_prev(),
        }
/*        match self {
            PrevEntry::SP(sp) => &sp.prev_entry,
            PrevEntry::TX(tx) => &tx.prev_entry,
        }.as_ref().map(|prv| {
            Rc::clone(prv)
        })
*/    }
}

type PrevEntryStrong = Rc<RefCell<PrevEntry>>;
type PrevEntryWeak = Weak<RefCell<PrevEntry>>;

struct LogPointers {
    next_entry: Option<PrevEntryWeak>,
    prev_entry: Option<PrevEntryStrong>,
}

impl Display for LogPointers {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let prv = match &self.prev_entry {
            None => "None",
            Some(_) => "Some",
        };
        let nxt = match &self.next_entry {
            None => "None",
            Some(ne) => {
                match ne.upgrade() {
                    None => "None",
                    Some(_) => "Some"
                }
            },
        };
        write!(f, "prev_entry: {}, next_entry: {}", prv, nxt)
    }
}

impl LogPointers {
    fn get_next(&self) -> Option<PrevEntryStrong> {
        match self.next_entry.as_ref() {
            None => None,
            Some(nxt) => nxt.upgrade()
        }
    }

    fn get_prev(&self) -> Option<PrevEntryStrong> {
        self.prev_entry.as_ref().map(|prv| {
            Rc::clone(prv)
        })
    }
}

pub struct OuterTX {
    log_pointers: LogPointers,
    log_index: u64,
    // tx: TX,
    // hash: Hash,
    // verification: Verify,
    // prev_tx: RefCell<Weak<OuterTX>>,
    // next_tx: RefCell<Weak<OuterTX>>,
    // next_entry: Option<PrevEntryWeak>,
    // prev_entry: Option<PrevEntryStrong>,
}

impl Display for OuterTX {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TX(log_index: {}, {})", self.log_index, self.log_pointers)
    }
}


pub struct OuterSP {
    log_index: u64,
    log_pointers: LogPointers,
    // sp: SP,
    // not_included_tx: Vec<Rc<OuterTX>>,
    // prev_local: RefCell<Weak<OuterSP>>,
    // prev_to: RefCell<Weak<OuterSP>>,
    // prev_entry: Option<PrevEntryStrong>,
}

impl Display for OuterSP {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SP(log_index: {}, {})", self.log_index, self.log_pointers)
    }
}


/*
struct TxIterator<'a> {
    tx: Option<&'a OuterTX>,
}

impl<'a> Iterator for TxIterator<'a> {
    type Item = &'a OuterTX;

    fn next(&mut self) -> Option<Self::Item> {
        match self.tx {
            None => None,
            Some(tx) => {
                match tx.prev_tx.borrow().upgrade() {
                    None => None,
                    Some(nxt) => {
                        self.tx = Some(&*Rc::clone(&nxt));
                        None
                    }
                }
            }
        }
    }
}
*/

fn to_prev_entry_holder(prv: &PrevEntryStrong) -> PrevEntryHolder {
    PrevEntryHolder{prv_entry: (&**prv).borrow()}
}

struct PrevEntryHolder<'a> {
    prv_entry: Ref<'a, PrevEntry>,
}

impl<'a> Deref for PrevEntryHolder<'a> {
    type Target = PrevEntry;

    fn deref(&self) -> &PrevEntry {
        &self.prv_entry
    }
}

struct LogIterator {
    prv_entry: Option<PrevEntryStrong>,
}

impl Iterator for LogIterator {
    type Item = PrevEntryStrong;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prv_entry.as_ref().map(|prv| {
            Rc::clone(prv)
        });
        self.prv_entry = match self.prv_entry.take() {
            None => None,
            Some(prv) => {
                (& *prv).borrow().get_prev()
            }
        };
        ret
    }
}

enum LogItems {
    Empty,
    Single(LogSingle),
    Multiple(LogMultiple)
}

impl LogItems {

    pub fn drop_first(&mut self) -> Result<(), Error> {
        match self {
            LogItems::Empty => Result::Err(Error::LogError(LogError::EmptyLogError)),
            LogItems::Single(_) =>  {
                *self = LogItems::Empty;
                Ok(())
            },
            LogItems::Multiple(mi) => {
                let nxt = (&*mi.first_entry).borrow().get_next().unwrap();
                nxt.borrow_mut().drop_previous();
                if (&*mi.last_entry).borrow().get_prev().is_none() {
                        *self = LogItems::Single(LogSingle{entry: nxt})
                }
                Ok(())
            }
        }
    }

    pub fn log_iterator(&self) -> LogIterator {
        LogIterator {
            prv_entry: match self {
                LogItems::Empty => None,
                LogItems::Single(si) => Some(Rc::clone( &si.entry)),
                LogItems::Multiple(mi) => Some(Rc::clone( &mi.last_entry))
            }
        }
    }

    fn add_item(&mut self, idx: u64) -> PrevEntryStrong {
        let prv = match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(Rc::clone(&si.entry)),
            LogItems::Multiple(mi) => Some(Rc::clone(&mi.last_entry))
        };
        let new_tx = Rc::new(RefCell::new(PrevEntry::TX(OuterTX{
            log_pointers: LogPointers {
                next_entry: None,
                prev_entry: prv,
            },
            log_index: idx,
        })));
        let new_tx_ref = Rc::clone(&new_tx);
        match self {
            LogItems::Empty => {
                *self = LogItems::Single(LogSingle{
                    entry: new_tx_ref
                })
            }
            LogItems::Single(si) => {
                si.entry.borrow_mut().set_next(&new_tx_ref);
                let first = Rc::clone(&si.entry);
                *self = LogItems::Multiple(LogMultiple{
                    last_entry: new_tx_ref,
                    first_entry: first,
                })
            }
            LogItems::Multiple(lm) => {
                lm.last_entry.borrow_mut().set_next(&new_tx_ref);
                lm.last_entry = new_tx_ref
            }
        };
        new_tx
    }

    fn get_last(&self) -> Option<PrevEntryStrong> {
        match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(Rc::clone(&si.entry)),
            LogItems::Multiple(mi) => Some(Rc::clone(&mi.last_entry)),
        }
    }
}

struct LogSingle {
    entry: PrevEntryStrong,
}

struct LogMultiple {
    last_entry: PrevEntryStrong,
    first_entry: PrevEntryStrong
}

pub struct Log {
    index: u64,
    first_last: LogItems
}

impl Log {
    pub fn new() -> Log {
        Log{
            index: 0,
            first_last: LogItems::Empty,
        }
    }

    pub fn log_iterator(&self) -> LogIterator {
        self.first_last.log_iterator()
    }

    pub fn insert_tx() -> Result<(), Error> {
        Ok(())
    }

    pub fn new_tx(&mut self) -> PrevEntryStrong {
        self.index += 1;
        // let old_last = self.last_entry.as_ref();
        // let old_last = self.last_entry.as_ref().map(|last| Rc::clone(last));
        self.first_last.add_item(self.index)
        // self.last_entry = Some(Rc::clone(&new_tx));
        // old_last.map(|last| last.borrow_mut().set_next(&self.last_entry));
        // new_tx
    }

/*    pub fn insert_sp<'a>(&mut self, sp: SP) -> Result<&'a OuterSP, Error> {
        self.index += 1;
        match self.prev_entry.borrow().take() {
            None => None,
            Some(prv) => Some(Rc::clone(&prv))
        };
        let outer_sp = Rc::new(Some(PrevEntry::SP(OuterSP{
            sp: SP,
            log_index: self.index,
            not_included_tx: vec![],
            prev_local: RefCell::new(Weak::new()), // RefCell<Weak<OuterSP>>,
            prev_to: RefCell::new(Weak::new()), // RefCell<Weak<OuterSP>>,
            prev_entry: RefCell::new(Rc::clone(&self.prev_entry.borrow())), // RefCell<Rc<Option<PrevEntry>>>,
        })));
        *self.prev_entry.borrow_mut() = Rc::clone(&outer_sp);
        let PrevEntry::SP(ret) = Rc::clone(&outer_sp).unwrap();
        Ok(&ret)
    }*/
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::log::PrevEntry;

    #[test]
    fn it_works() {
        let mut l = Log::new();
        for _ in 0..5 {
            let _ = l.new_tx();
        }
        for tx in l.log_iterator() {
            println!("it {}", (&*tx).borrow());
        }
        l.first_last.drop_first().unwrap();
        for tx in l.log_iterator() {
            println!("it {}", (&*tx).borrow());
        }

        l.log_iterator().next().map(|prv| {
            println!("it2 {}", *to_prev_entry_holder(&prv))
        });
        // println!("it2 {:#?}", a.take())
    }

    #[test]
    fn test2() {
        print!("test {}", 2)
    }
}
