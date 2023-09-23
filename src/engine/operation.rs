use std::fmt::Display;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operation {
    Insert(String),
    Delete,
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Insert(value) => write!(f, "INSERT\t{}", value),
            Operation::Delete => write!(f, "DELETE"),
        }
    }
}

// implement a trait for Operation that shows how many bytes it takes up
impl Operation {
    pub fn size_bytes(&self) -> usize {
        match self {
            Operation::Insert(value) => value.len(),
            Operation::Delete => 0,
        }
    }
}