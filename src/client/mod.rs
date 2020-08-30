use bytes::Bytes;

pub mod r#async;
mod async_impl;
pub mod sync;

#[derive(Debug)]
pub enum AuthResponse {
    Continue(Bytes),
    Succeeded,
    Failed,
}

#[inline]
fn gen_opaque() -> u32 {
    fastrand::u32(..)
}
#[inline]
fn discard_packet(_: crate::Packet) {}

pub use async_impl::BinaryProto;
pub use r#async::Proto;
