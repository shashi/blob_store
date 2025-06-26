pub mod memory;
pub mod local;

use std::io;

#[derive(Debug)]
pub enum ObjectStoreError {
    Io(io::Error),
    PreconditionFailed,
    Other(String),
}

pub type Result<T> = std::result::Result<T, ObjectStoreError>;

#[derive(Debug, Clone)]
pub enum IfMatch<'a> {
    Any,
    Tag(&'a str),
    NoneMatch,
}

impl<'a> Default for IfMatch<'a> {
    fn default() -> Self {
        IfMatch::Any
    }
}

pub trait ObjectStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &str, body: &[u8], cond: IfMatch) -> Result<String>;
    fn list(&self, prefix: &str, continuation: Option<String>) -> Result<(Vec<String>, Option<String>)>;
}
