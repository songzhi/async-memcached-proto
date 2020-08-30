use super::AuthResponse;
use crate::Result;
use async_trait::async_trait;
use bytes::Bytes;
use semver::Version;
use std::collections::{BTreeMap, HashMap};

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

#[async_trait]
pub trait Operation {
    async fn set(&mut self, key: &[u8], val: &[u8], flags: u32, expiration: u32) -> Result<()>;
    async fn add(&mut self, key: &[u8], val: &[u8], flags: u32, expiration: u32) -> Result<()>;
    async fn delete(&mut self, key: &[u8]) -> Result<()>;
    async fn replace(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: u32,
        expiration: u32,
    ) -> Result<()>;
    async fn get(&mut self, key: &[u8]) -> Result<(Bytes, u32)>;
    async fn getk(&mut self, key: &[u8]) -> Result<(Bytes, Bytes, u32)>;
    async fn increment(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
    ) -> Result<u64>;
    async fn decrement(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
    ) -> Result<u64>;
    async fn append(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    async fn prepend(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    async fn touch(&mut self, key: &[u8], expiration: u32) -> Result<()>;
}

#[async_trait]
pub trait CasOperation {
    async fn set_cas(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: u32,
        expiration: u32,
        cas: u64,
    ) -> Result<u64>;
    async fn add_cas(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: u32,
        expiration: u32,
    ) -> Result<u64>;
    async fn replace_cas(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: u32,
        expiration: u32,
        cas: u64,
    ) -> Result<u64>;
    async fn get_cas(&mut self, key: &[u8]) -> Result<(Bytes, u32, u64)>;
    async fn getk_cas(&mut self, key: &[u8]) -> Result<(Bytes, Bytes, u32, u64)>;
    async fn increment_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> Result<(u64, u64)>;
    async fn decrement_cas(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
        cas: u64,
    ) -> Result<(u64, u64)>;
    async fn append_cas(&mut self, key: &[u8], val: &[u8], cas: u64) -> Result<u64>;
    async fn prepend_cas(&mut self, key: &[u8], val: &[u8], cas: u64) -> Result<u64>;
    async fn touch_cas(&mut self, key: &[u8], expiration: u32, cas: u64) -> Result<u64>;
}

#[async_trait]
pub trait ServerOperation {
    async fn quit(&mut self) -> Result<()>;
    async fn flush(&mut self, expiration: u32) -> Result<()>;
    async fn noop(&mut self) -> Result<()>;
    async fn version(&mut self) -> Result<Version>;
    async fn stat(&mut self) -> Result<BTreeMap<String, String>>;
}

#[async_trait]
pub trait MultiOperation {
    async fn set_multi(&mut self, kv: BTreeMap<&[u8], (&[u8], u32, u32)>) -> Result<()>;
    async fn delete_multi(&mut self, keys: &[&[u8]]) -> Result<()>;
    async fn increment_multi<'a>(
        &mut self,
        kv: HashMap<&'a [u8], (u64, u64, u32)>,
    ) -> Result<HashMap<&'a [u8], u64>>;
    async fn get_multi(&mut self, keys: &[&[u8]]) -> Result<HashMap<Bytes, (Bytes, u32)>>;
}

#[async_trait]
pub trait NoReplyOperation {
    async fn set_noreply(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: u32,
        expiration: u32,
    ) -> Result<()>;
    async fn add_noreply(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: u32,
        expiration: u32,
    ) -> Result<()>;
    async fn delete_noreply(&mut self, key: &[u8]) -> Result<()>;
    async fn replace_noreply(
        &mut self,
        key: &[u8],
        val: &[u8],
        flags: u32,
        expiration: u32,
    ) -> Result<()>;
    async fn increment_noreply(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
    ) -> Result<()>;
    async fn decrement_noreply(
        &mut self,
        key: &[u8],
        amount: u64,
        initial: u64,
        expiration: u32,
    ) -> Result<()>;
    async fn append_noreply(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    async fn prepend_noreply(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
}

#[async_trait]
pub trait AuthOperation {
    async fn list_mechanisms(&mut self) -> Result<Vec<String>>;
    async fn auth_start(&mut self, mech: &str, init: &[u8]) -> Result<AuthResponse>;
    async fn auth_continue(&mut self, mech: &str, data: &[u8]) -> Result<AuthResponse>;
}
