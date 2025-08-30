use crate::index::RepoDocId;
use std::fmt;

#[derive(Debug, Clone)]
pub enum Query {
    Literal(String),
    Regex(String),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
    Not(Box<Query>),
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub doc: RepoDocId,
    pub path: String,
    /// symbol name (if this result is a symbol-select)
    pub symbol: Option<String>,
    /// optional symbol location with offsets/line when available
    pub symbol_loc: Option<crate::types::Symbol>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SelectKind {
    #[default]
    File,
    Repo,
    Symbol,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Query::Literal(s) => write!(f, "L({})", s),
            Query::Regex(r) => write!(f, "R({})", r),
            Query::And(a, b) => write!(f, "(AND {} {})", a, b),
            Query::Or(a, b) => write!(f, "(OR {} {})", a, b),
            Query::Not(inner) => write!(f, "(NOT {})", inner),
        }
    }
}
