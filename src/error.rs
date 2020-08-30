use crate::Status;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Proto(#[from] ProtoError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub struct ProtoError {
    status: Status,
    desc: &'static str,
    detail: Option<String>,
}

impl ProtoError {
    pub(crate) fn from_status(status: Status, detail: Option<String>) -> Self {
        Self {
            status,
            desc: status.desc(),
            detail,
        }
    }

    /// Get error description
    pub fn detail(&self) -> Option<String> {
        self.detail.clone()
    }

    /// Get status code
    pub fn status(&self) -> Status {
        self.status
    }
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.desc)?;
        match self.detail {
            Some(ref s) => write!(f, " ({})", s),
            None => Ok(()),
        }
    }
}
