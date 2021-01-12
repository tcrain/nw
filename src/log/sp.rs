use std::{cmp::Ordering, fmt::{self, Debug, Display, Formatter}, vec};

use crate::{config::Time, errors::{Error, LogError}, verification::{Hash, Id, TimeCheck, TimeInfo, hash, new_hasher}};

use super::op::{BasicInfo, EntryInfo, EntryInfoData};
use bincode::serialize;
use serde::{Serialize, Deserialize};

pub type SupportCount = u64;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SpState {
    pub sp: Sp,
    pub hash: Hash,
}

impl SpState {
    
    pub fn new<I>(id: Id, time: Time, ops_supported: I, additional_ops: Vec<EntryInfoData>, prev_sp: EntryInfo) -> Result<SpState, Error>
    where I: Iterator<Item = Hash> {
        let sp = Sp::new(id, time, ops_supported, additional_ops, prev_sp);
        SpState::from_sp(sp)
    }
    
    pub fn from_sp(sp: Sp) -> Result<SpState, Error> {
        let enc = serialize(&sp).map_err(|_err| Error::LogError(LogError::SerializeError))?;
        Ok(SpState {
            sp,
            hash: hash(&enc),
        })
    }

    pub fn get_entry_info(&self) -> EntryInfo {
        EntryInfo{
            basic: self.sp.info,
            hash: self.hash,
        }
    }
}

impl PartialOrd for SpState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SpState {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.sp.info.cmp(&other.sp.info) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.hash.cmp(&other.hash),
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl Eq for SpState {}

impl PartialEq for SpState {
    fn eq(&self, other: &Self) -> bool {
        self.hash.eq(&other.hash)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Sp {
    pub info: BasicInfo,
    pub new_ops_supported: SupportCount, // number of new operations supported by this sp
    pub support_hash: Hash,
    // data: OpData,
    pub prev_sp: EntryInfo, // the sp that this one comes after
    // pub unsupported_ops: Vec<EntryInfo>, // operations before this entry in the log not supported by this item
    pub additional_ops: Vec<EntryInfoData>, // operations smaller than the time, but included
    // verify: Verify,
}

impl Display for Sp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(id: {}, time: {})", self.info.id, self.info.time)
    }
}

impl Debug for Sp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Sp {
    
    pub fn new<I>(id: Id, time: Time, ops_supported: I, additional_ops: Vec<EntryInfoData>, prev_sp: EntryInfo) -> Sp 
    where I: Iterator<Item = Hash> {
        // let data = gen_rand_data();
        let mut hasher = new_hasher();
        hasher.update(prev_sp.hash.as_bytes());
        let mut op_count = 0;
        for nxt in ops_supported {
            op_count += 1;
            hasher.update(nxt.as_bytes());
        }
        Sp{
            info: BasicInfo{
                time,
                id,
            },
            new_ops_supported: op_count,
            prev_sp,
            additional_ops,
            support_hash: hasher.finalize(),
        }
    }
    
    pub fn get_init(ei: EntryInfo) -> Sp {
        Sp{
            info: BasicInfo{
                time: 0,
                id: 0,
            },
            support_hash: hash(&[]),
            new_ops_supported: 0,
            prev_sp: ei,
            additional_ops: vec![],
        }
    }
    
    pub fn is_init(&self) -> bool {
        self.info.id == 0 && self.info.time == 0
    }
    
    pub fn prev_is_init(&self, init_sp_hash: &Hash) -> Result<bool, Error> {
        if self.prev_sp.basic.id == 0 && self.prev_sp.basic.time == 0 {
            if &self.prev_sp.hash != init_sp_hash {
                return Err(Error::LogError(LogError::SpInvalidInitHash));
            }
            return Ok(true);
        }
        Ok(false)
    }
    
    pub fn validate<T>(&self, init_sp_hash: &Hash, ti: &T) -> Result<(), Error> where T: TimeInfo{
        if self.new_ops_supported == 0 { // must support at leat 1 op
            return Err(Error::LogError(LogError::SpNoOpsSupported))
        }
        if self.info.time <= self.prev_sp.basic.time { // must happen after the previous sp
            return Err(Error::LogError(LogError::SpPrevLaterTime))
        }
        if !self.prev_is_init(init_sp_hash)? && self.info.id != self.prev_sp.basic.id { // the previous sp must be from the same id
            return Err(Error::LogError(LogError::SpPrevIdDifferent))
        }
        let TimeCheck {time_not_passed: _ , include_in_hash, arrived_late: _} = ti.arrived_time_check(self.info.time);
        if include_in_hash { // the sp must arrive after the time operations can arrive
            return Err(Error::LogError(LogError::SpArrivedEarly))
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    
    use bincode::{deserialize, serialize};
    
    use crate::{errors::{Error, LogError}, log::op::{BasicInfo, EntryInfo, OpState}, verification::{Id, TimeInfo, TimeTest, hash}};
    
    use super::{Sp, SpState};
    
    fn make_sp(id: Id, ti: &mut TimeTest) -> SpState {
        let info = EntryInfo{
            basic: BasicInfo{
                time: 0,
                id,
            },
            hash: hash(b"init msg"),
        };
        let ops = [OpState::new(1, ti).unwrap()];
        
        SpState::new(id, ti.now_monotonic(),ops.iter().map(|op| op.hash), vec![], info).unwrap()
    }
    
    #[test]
    fn sp_serialize() {
        let mut ti = TimeTest::new();
        let id = 1;
        let sp1 = make_sp(id, &mut ti);
        
        let mut enc = serialize(&sp1).unwrap();
        enc.push(10);
        println!("enc bytes {}", enc.len());
        
        let dec = deserialize(&enc).unwrap();
        
        assert_eq!(sp1, dec);
    }
    
    #[test]
    fn validate() {
        let mut ti = TimeTest::new();
        let id = 1;
        let info = EntryInfo{
            basic: BasicInfo{
                time: 0,
                id,
            },
            hash: hash(b"init msg"),
        };
        // let init_hash = hash(b"init hash");
        let init_hash = info.hash;
        let ops = [OpState::new(1, &mut ti).unwrap()];
        
        let sp1_invalid_time = SpState::new(id, ti.now_monotonic(),ops.iter().map(|op| op.hash), vec![], info).unwrap();
        // the sp must arrive late enough so new operations that are on time will be later in the log
        assert_eq!(Error::LogError(LogError::SpArrivedEarly), sp1_invalid_time.sp.validate(&init_hash, &ti).unwrap_err());        
        
        let mut sp1 = SpState::new(id, ti.now_monotonic(),ops.iter().map(|op| op.hash), vec![], info).unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        sp1.sp.validate(&init_hash, &ti).unwrap();
        
        let mut sp2 = SpState::new(id, ti.now_monotonic(),ops.iter().map(|op| op.hash), vec![], sp1.get_entry_info()).unwrap();
        ti.set_sp_time_valid(sp2.sp.info.time);
        sp2.sp.validate(&init_hash, &ti).unwrap();
        assert!(sp1 < sp2);
        
        let mut sp3 = SpState::new(id, ti.now_monotonic(),ops.iter().map(|op| op.hash), vec![], info).unwrap();
        ti.set_sp_time_valid(sp3.sp.info.time);
        assert!(sp1 != sp3);
        sp3.sp.info.time = sp1.sp.info.time;
        sp3.hash = sp1.hash;
        assert!(sp1 == sp3);
        
        // clocks cannot be in reverse order
        sp1.sp.prev_sp = sp2.get_entry_info();
        assert_eq!(Error::LogError(LogError::SpPrevLaterTime), sp1.sp.validate(&init_hash, &ti).unwrap_err());
        
        // must have same id as prev sp
        sp2.sp.prev_sp.basic.id += 1;
        assert_eq!(Error::LogError(LogError::SpPrevIdDifferent), sp2.sp.validate(&init_hash, &ti).unwrap_err());
        
        // must have at least 1 op supported
        let sp2 = Sp::new(id, ti.now_monotonic(),[].iter().cloned(), vec![], info);
        assert_eq!(Error::LogError(LogError::SpNoOpsSupported), sp2.validate(&init_hash, &ti).unwrap_err());
    }
}