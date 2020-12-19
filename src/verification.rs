use ed25519_dalek::Signature;
use blake3;

pub type Hash = blake3::Hash;
pub type Verify = Signature;

pub fn check_verify(_v: Verify, _h: Hash) -> bool {
    true
}