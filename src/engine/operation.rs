use std::fmt::Display;

#[derive(Clone, Debug, PartialEq)]
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