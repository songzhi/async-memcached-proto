use crate::error::ProtoError;
use crate::Result;
use num_derive::FromPrimitive;

#[derive(Debug, Eq, PartialEq, Copy, Clone, FromPrimitive)]
#[non_exhaustive]
pub enum Opcode {
    Get = 0x00,
    Set = 0x01,
    Add = 0x02,
    Replace = 0x03,
    Delete = 0x04,
    Increment = 0x05,
    Decrement = 0x06,
    Quit = 0x07,
    Flush = 0x08,
    GetQ = 0x09,
    NoOp = 0x0a,
    Version = 0x0b,
    GetK = 0x0c,
    GetKQ = 0x0d,
    Append = 0x0e,
    Prepend = 0x0f,
    Stat = 0x10,
    SetQ = 0x11,
    AddQ = 0x12,
    ReplaceQ = 0x13,
    DeleteQ = 0x14,
    IncrementQ = 0x15,
    DecrementQ = 0x16,
    QuitQ = 0x17,
    FlushQ = 0x18,
    AppendQ = 0x19,
    PrependQ = 0x1a,
    Verbosity = 0x1b,
    Touch = 0x1c,
    /// Get and touch
    GAT = 0x1d,
    GATQ = 0x1e,
    SASLListMechs = 0x20,
    SASLAuth = 0x21,
    SASLStep = 0x22,
    // These commands are used for range operations and exist within
    // this header for use in other projects.  Range operations are
    // not expected to be implemented in the memcached server itself.
    RGet = 0x30,
    RSet = 0x31,
    RSetQ = 0x32,
    RAppend = 0x33,
    RAppendQ = 0x34,
    RPrepend = 0x35,
    RPrependQ = 0x36,
    RDelete = 0x37,
    RDeleteQ = 0x38,
    RIncr = 0x39,
    RIncrQ = 0x3a,
    RDecr = 0x3b,
    RDecrQ = 0x3c,
    // End Range operations
    SetVBucket = 0x3d,
    GetVBucket = 0x3e,
    DelVBucket = 0x3f,
    TapConnect = 0x40,
    TapMutation = 0x41,
    TapDelete = 0x42,
    TapFlush = 0x43,
    TapOpaque = 0x44,
    TapVBucketSet = 0x45,
    TapCheckPointStart = 0x46,
    TabCheckPointEnd = 0x47,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, FromPrimitive)]
pub enum Magic {
    Request = 0x80,
    Response = 0x81,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, FromPrimitive)]
#[non_exhaustive]
pub enum Status {
    NoError = 0x0000,
    KeyNotFound = 0x0001,
    KeyExits = 0x0002,
    ValueTooLarge = 0x0003,
    InvalidArguments = 0x0004,
    ItemNotStored = 0x0005,
    IncrOrDecrOnNonNumericValue = 0x0006,
    VbucketBelongsToAnotherServer = 0x0007,
    AuthenticationError = 0x0008,
    AuthenticationContinue = 0x0009,
    UnknownCommand = 0x0081,
    OutOfMemory = 0x0082,
    NotSupported = 0x0083,
    InternalError = 0x0084,
    Busy = 0x0085,
    TemporaryFailure = 0x0086,
    AuthenticationRequired = 0x0020,
    AuthenticationFurtherStepRequired = 0x0021,
}
impl Status {
    pub fn desc(&self) -> &'static str {
        match self {
            Self::NoError => "no error",
            Self::KeyNotFound => "key not found",
            Self::KeyExits => "key exists",
            Self::ValueTooLarge => "value too large",
            Self::InvalidArguments => "invalid arguments",
            Self::ItemNotStored => "item not stored",
            Self::IncrOrDecrOnNonNumericValue => "incr/Decr on non-numeric value",
            Self::VbucketBelongsToAnotherServer => "the vbucket belongs to another server",
            Self::AuthenticationError => "authentication error",
            Self::AuthenticationContinue => "authentication continue",
            Self::UnknownCommand => "unknown command",
            Self::OutOfMemory => "out of memory",
            Self::NotSupported => "not supported",
            Self::InternalError => "internal error",
            Self::Busy => "busy",
            Self::TemporaryFailure => "temporary failure",
            Self::AuthenticationRequired => "authentication required/not successful",
            Self::AuthenticationFurtherStepRequired => "further authentication steps required",
        }
    }
    pub fn ok_or(self, detail: Option<String>) -> Result<()> {
        match self {
            Self::NoError => Ok(()),
            status => Err(ProtoError::from_status(status, detail).into()),
        }
    }
}
impl Default for Status {
    fn default() -> Self {
        Self::NoError
    }
}
