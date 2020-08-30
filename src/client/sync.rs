use crate::Result;
use bytes::Bytes;
use semver::Version;
use std::collections::{BTreeMap, HashMap};
use super::AuthResponse;

pub trait Proto:
Operation + MultiOperation + ServerOperation + NoReplyOperation + CasOperation + AuthOperation
{
}

impl<T> Proto for T where
    T: Operation
    + MultiOperation
    + ServerOperation
    + NoReplyOperation
    + CasOperation
    + AuthOperation
{
}

pub trait Operation {
    fn set(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<()>;
    fn add(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<()>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
    fn replace(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<()>;
    fn get(&mut self, key: &[u8]) -> Result<(Bytes, u32)>;
    fn getk(&mut self, key: &[u8]) -> Result<(Bytes, Bytes, u32)>;
    fn increment(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64>;
    fn decrement(&mut self, key: &[u8], amount: u64, initial: u64, expiration: u32) -> Result<u64>;
    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn prepend(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn touch(&mut self, key: &[u8], expiration: u32) -> Result<()>;
}

pub trait CasOperation {
    fn set_cas(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: u32,
        expiration: u32,
        cas: u64,
    ) -> Result<u64>;
    fn add_cas(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<u64>;
    fn replace_cas(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: u32,
        expiration: u32,
        cas: u64,
    ) -> Result<u64>;
    fn get_cas(&mut self, key: &[u8]) -> Result<(Bytes, u32, u64)>;
    fn getk_cas(&mut self, key: &[u8]) -> Result<(Bytes, Bytes, u32, u64)>;
    fn increment_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> Result<(u64, u64)>;
    fn decrement_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> Result<(u64, u64)>;
    fn append_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> Result<u64>;
    fn prepend_cas(&mut self, key: &[u8], value: &[u8], cas: u64) -> Result<u64>;
    fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> Result<u64>;
}

pub trait ServerOperation {
    fn quit(&mut self) -> Result<()>;
    fn flush(&mut self, expiration: u32) -> Result<()>;
    fn noop(&mut self) -> Result<()>;
    fn version(&mut self) -> Result<Version>;
    fn stat(&mut self) -> Result<BTreeMap<String, String>>;
}

pub trait MultiOperation {
    fn set_multi(&mut self, kv: BTreeMap<&[u8], (&[u8], u32, u32)>) -> Result<()>;
    fn delete_multi(&mut self, keys: &[&[u8]]) -> Result<()>;
    fn increment_multi<'a>(
        &mut self,
        kv: HashMap<&'a [u8], (u64, u64, u32)>,
    ) -> Result<HashMap<&'a [u8], u64>>;
    fn get_multi(&mut self, keys: &[&[u8]]) -> Result<HashMap<Bytes, (Bytes, u32)>>;
}

pub trait NoReplyOperation {
    fn set_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<()>;
    fn add_noreply(&mut self, key: &[u8], value: &[u8], flags: u32, expiration: u32) -> Result<()>;
    fn delete_noreply(&mut self, key: &[u8]) -> Result<()>;
    fn replace_noreply(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: u32,
        expiration: u32,
    ) -> Result<()>;
    fn increment_noreply(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
    ) -> Result<()>;
    fn decrement_noreply(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
    ) -> Result<()>;
    fn append_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn prepend_noreply(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
}


pub trait AuthOperation {
    fn list_mechanisms(&mut self) -> Result<Vec<String>>;
    fn auth_start(&mut self, mech: &str, init: &[u8]) -> Result<AuthResponse>;
    fn auth_continue(&mut self, mech: &str, data: &[u8]) -> Result<AuthResponse>;
}
