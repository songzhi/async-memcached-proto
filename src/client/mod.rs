use bytes::Bytes;

mod r#async;
mod sync;

#[derive(Debug)]
pub enum AuthResponse {
    Continue(Bytes),
    Succeeded,
    Failed,
}
