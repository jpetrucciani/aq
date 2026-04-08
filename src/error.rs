use std::path::PathBuf;

use crate::format::Format;
use crate::value::Value;

#[derive(Debug, thiserror::Error)]
pub enum AqError {
    #[error("{0}")]
    Message(String),
    #[error("I/O error for {path:?}: {source}")]
    Io {
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse {format} input: {message}")]
    ParseInput {
        format: &'static str,
        message: String,
    },
    #[error("invalid expression: {0}")]
    InvalidExpression(String),
    #[error("invalid starlark: {0}")]
    InvalidStarlark(String),
    #[error("query error: {0}")]
    Query(String),
    #[error("break")]
    Break,
    #[error("break")]
    BreakLabel(String),
    #[error("starlark error: {0}")]
    Starlark(String),
    #[error("{}", format_thrown_value(.0))]
    Thrown(Value),
    #[error("cannot emit a multi-result stream as {format}")]
    OutputShape { format: Format },
    #[error("unsupported output format {0}")]
    UnsupportedOutputFormat(String),
    #[error("cannot combine --null-input with file arguments")]
    NullInputWithFiles,
}

impl AqError {
    pub fn exit_code(&self) -> i32 {
        match self {
            AqError::InvalidExpression(_) | AqError::InvalidStarlark(_) => 2,
            AqError::ParseInput { .. } => 3,
            AqError::Io { .. } => 4,
            AqError::Break => 5,
            _ => 1,
        }
    }

    pub fn into_catch_value(self) -> Value {
        match self {
            AqError::Message(message)
            | AqError::InvalidStarlark(message)
            | AqError::Query(message)
            | AqError::Starlark(message) => Value::String(message),
            AqError::Break | AqError::BreakLabel(_) => Value::String("break".to_string()),
            AqError::Thrown(value) => value,
            other => Value::String(other.to_string()),
        }
    }

    pub fn io(path: Option<PathBuf>, source: std::io::Error) -> Self {
        Self::Io { path, source }
    }

    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

fn format_thrown_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => {
            let rendered = value
                .to_json()
                .ok()
                .and_then(|value| serde_json::to_string(&value).ok())
                .unwrap_or_else(|| "<unrenderable value>".to_string());
            format!("(not a string): {rendered}")
        }
    }
}
