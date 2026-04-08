use std::str::FromStr;
use std::thread;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, Utc};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_yaml_ng::value::{Tag as YamlTag, TaggedValue as YamlTaggedValue};

use crate::error::AqError;

#[derive(Debug, Clone, Copy)]
enum RelaxedJsonNumber {
    Nan,
    PositiveInfinity,
    NegativeInfinity,
}

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_992;
const MAX_JSON_PARSE_DEPTH: usize = 10_000;
pub(crate) const MAX_JSON_PRINT_DEPTH: usize = 10_000;
const LARGE_JSON_STACK_SIZE: usize = 64 * 1024 * 1024;
const LARGE_JSON_STACK_THRESHOLD: usize = 256;
const JSON_SKIP_MARKER: &str = "<skipped: too deep>";

#[derive(Debug, Clone)]
pub struct DecimalValue {
    value: BigDecimal,
    rendered: String,
    semantics: DecimalSemantics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecimalSemantics {
    Exact,
    LossyFloat,
}

impl DecimalValue {
    pub fn parse(raw: &str) -> Result<Self, AqError> {
        let rendered = canonicalize_decimal_literal(raw)?;
        let value = BigDecimal::from_str(&rendered).map_err(|error| {
            AqError::message(format!("invalid decimal literal `{raw}`: {error}"))
        })?;
        Ok(Self {
            value,
            rendered,
            semantics: DecimalSemantics::Exact,
        })
    }

    pub fn from_lossy_f64(raw: f64) -> Result<Self, AqError> {
        let rendered = if raw.fract() == 0.0 && raw >= i64::MIN as f64 && raw <= i64::MAX as f64 {
            format!("{raw:.0}")
        } else {
            raw.to_string()
        };
        let value = BigDecimal::from_str(&rendered).map_err(|error| {
            AqError::message(format!(
                "invalid lossy decimal literal `{rendered}`: {error}"
            ))
        })?;
        Ok(Self {
            value,
            rendered,
            semantics: DecimalSemantics::LossyFloat,
        })
    }

    pub fn rendered(&self) -> &str {
        &self.rendered
    }

    pub fn as_bigdecimal(&self) -> &BigDecimal {
        &self.value
    }

    pub fn is_lossy_float(&self) -> bool {
        self.semantics == DecimalSemantics::LossyFloat
    }

    pub fn is_integer(&self) -> bool {
        !self.rendered.contains('.') && !self.rendered.contains('E')
    }

    pub fn as_i64_exact(&self) -> Option<i64> {
        if self.is_integer() {
            self.rendered.parse::<i64>().ok()
        } else {
            None
        }
    }

    pub fn to_f64_lossy(&self) -> f64 {
        match self.rendered.parse::<f64>() {
            Ok(value) => value,
            Err(_) if self.rendered.starts_with('-') => f64::NEG_INFINITY,
            Err(_) => f64::INFINITY,
        }
    }

    pub fn negated(&self) -> Self {
        if self.rendered == "0" {
            return self.clone();
        }
        if let Some(rendered) = self.rendered.strip_prefix('-') {
            Self {
                value: -self.value.clone(),
                rendered: rendered.to_string(),
                semantics: self.semantics,
            }
        } else {
            Self {
                value: -self.value.clone(),
                rendered: format!("-{}", self.rendered),
                semantics: self.semantics,
            }
        }
    }

    pub fn abs(&self) -> Self {
        if let Some(rendered) = self.rendered.strip_prefix('-') {
            Self {
                value: self.value.clone().abs(),
                rendered: rendered.to_string(),
                semantics: self.semantics,
            }
        } else {
            self.clone()
        }
    }
}

fn canonicalize_decimal_literal(raw: &str) -> Result<String, AqError> {
    let (negative, unsigned) = if let Some(rest) = raw.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix('+') {
        (false, rest)
    } else {
        (false, raw)
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => {
            let exponent = unsigned[index + 1..].parse::<i64>().map_err(|error| {
                AqError::message(format!("invalid decimal literal `{raw}`: {error}"))
            })?;
            (&unsigned[..index], exponent)
        }
        None => (unsigned, 0),
    };
    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fractional.is_empty() {
        return Err(AqError::message(format!("invalid decimal literal `{raw}`")));
    }
    if !whole.chars().all(|value| value.is_ascii_digit())
        || !fractional.chars().all(|value| value.is_ascii_digit())
    {
        return Err(AqError::message(format!("invalid decimal literal `{raw}`")));
    }

    let digits = format!("{whole}{fractional}");
    let Some(first_nonzero) = digits.find(|value| value != '0') else {
        let scale = fractional.len() as i64 - exponent;
        if scale > 0 {
            return Ok(format!("0.{}", "0".repeat(scale as usize)));
        }
        return Ok("0".to_string());
    };
    let significant = &digits[first_nonzero..];
    let decimal_exponent = exponent + whole.len() as i64 - first_nonzero as i64 - 1;

    let mut rendered = if decimal_exponent >= -6 && decimal_exponent < significant.len() as i64 {
        render_plain_decimal(significant, decimal_exponent)
    } else {
        render_scientific_decimal(significant, decimal_exponent)
    };
    if negative && rendered != "0" {
        rendered.insert(0, '-');
    }
    Ok(rendered)
}

fn render_plain_decimal(significant: &str, decimal_exponent: i64) -> String {
    let decimal_index = decimal_exponent + 1;
    if decimal_index <= 0 {
        format!(
            "0.{}{}",
            "0".repeat(decimal_index.unsigned_abs() as usize),
            significant
        )
    } else if decimal_index as usize >= significant.len() {
        format!(
            "{}{}",
            significant,
            "0".repeat(decimal_index as usize - significant.len())
        )
    } else {
        format!(
            "{}.{}",
            &significant[..decimal_index as usize],
            &significant[decimal_index as usize..]
        )
    }
}

fn rendered_requires_exact_decimal(rendered: &str) -> bool {
    rendered.contains('.') || rendered.contains('e') || rendered.contains('E')
}

fn render_scientific_decimal(significant: &str, decimal_exponent: i64) -> String {
    let mut rendered = String::with_capacity(significant.len() + 16);
    rendered.push_str(&significant[..1]);
    if significant.len() > 1 {
        rendered.push('.');
        rendered.push_str(&significant[1..]);
    }
    rendered.push('E');
    if decimal_exponent >= 0 {
        rendered.push('+');
    }
    rendered.push_str(&decimal_exponent.to_string());
    rendered
}

fn render_finite_float(value: f64) -> String {
    serde_json::to_string(&serde_json::json!(value)).unwrap_or_else(|_| value.to_string())
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Decimal(DecimalValue),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(IndexMap<String, Value>),
    #[expect(
        dead_code,
        reason = "binary IR support is intentionally kept internal for now"
    )]
    Bytes(Vec<u8>),
    DateTime(DateTime<Utc>),
    Date(NaiveDate),
    Tagged {
        tag: String,
        value: Box<Value>,
    },
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self.untagged(), other.untagged()) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(left), Value::Bool(right)) => left == right,
            (Value::Integer(left), Value::Integer(right)) => left == right,
            (Value::Decimal(left), Value::Decimal(right))
                if left.is_lossy_float() || right.is_lossy_float() =>
            {
                left.to_f64_lossy() == right.to_f64_lossy()
            }
            (Value::Decimal(left), Value::Decimal(right)) => decimal_exact_equals(left, right),
            (Value::Integer(left), Value::Decimal(right))
            | (Value::Decimal(right), Value::Integer(left)) => {
                if right.is_lossy_float() {
                    integer_equals_float(*left, right.to_f64_lossy())
                } else {
                    integer_equals_decimal(*left, right)
                }
            }
            (Value::Float(left), Value::Float(right)) => left == right,
            (Value::Decimal(left), Value::Float(right))
            | (Value::Float(right), Value::Decimal(left)) => decimal_equals_float(left, *right),
            (Value::Integer(left), Value::Float(right)) => integer_equals_float(*left, *right),
            (Value::Float(left), Value::Integer(right)) => integer_equals_float(*right, *left),
            (Value::String(left), Value::String(right)) => left == right,
            (Value::Array(left), Value::Array(right)) => left == right,
            (Value::Object(left), Value::Object(right)) => left == right,
            (Value::Bytes(left), Value::Bytes(right)) => left == right,
            (Value::DateTime(left), Value::DateTime(right)) => left == right,
            (Value::Date(left), Value::Date(right)) => left == right,
            _ => false,
        }
    }
}

fn decimal_equals_float(decimal: &DecimalValue, float: f64) -> bool {
    float.is_finite() && decimal.to_f64_lossy() == float
}

fn integer_equals_float(integer: i64, float: f64) -> bool {
    if float.is_finite()
        && float.fract() == 0.0
        && float >= i64::MIN as f64
        && float <= i64::MAX as f64
        && (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&integer)
    {
        integer == float as i64
    } else {
        (integer as f64) == float
    }
}

fn decimal_exact_equals(left: &DecimalValue, right: &DecimalValue) -> bool {
    left.as_bigdecimal().cmp(right.as_bigdecimal()) == std::cmp::Ordering::Equal
}

fn integer_equals_decimal(integer: i64, decimal: &DecimalValue) -> bool {
    BigDecimal::from(integer).cmp(decimal.as_bigdecimal()) == std::cmp::Ordering::Equal
}

pub fn parse_json_str(input: &str) -> Result<Value, String> {
    if let Some(message) = relaxed_json_numeric_literal_error(input) {
        return Err(message);
    }
    if let Some(message) = relaxed_json_string_literal_error(input) {
        return Err(message);
    }

    let max_depth = json_parse_max_depth(input)?;
    if let Some(depth) = singleton_empty_array_chain_text_depth(input) {
        debug_assert_eq!(max_depth, depth.saturating_add(1));
        return Ok(build_singleton_empty_array_chain(depth));
    }
    let (normalized, replacements) = normalize_relaxed_json_numbers(input);
    if max_depth > LARGE_JSON_STACK_THRESHOLD {
        return parse_json_str_on_large_stack(normalized, replacements);
    }
    let value = serde_json::from_str::<serde_json::Value>(&normalized)
        .map_err(|error| error.to_string())?;
    value_from_json_relaxed(value, &replacements)
}

fn json_parse_max_depth(input: &str) -> Result<usize, String> {
    let mut depth = 0usize;
    let mut max_depth = 0usize;
    let mut line = 1usize;
    let mut column = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for ch in input.chars() {
        if ch == '\n' {
            line += 1;
            column = 0;
        } else {
            column += 1;
        }

        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '[' | '{' => {
                if depth >= MAX_JSON_PARSE_DEPTH {
                    return Err(format!(
                        "Exceeds depth limit for parsing at line {line}, column {column}"
                    ));
                }
                depth += 1;
                max_depth = max_depth.max(depth);
            }
            ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }

    Ok(max_depth)
}

fn parse_json_str_on_large_stack(
    normalized: String,
    replacements: IndexMap<String, RelaxedJsonNumber>,
) -> Result<Value, String> {
    thread::Builder::new()
        .stack_size(LARGE_JSON_STACK_SIZE)
        .spawn(move || {
            let mut deserializer = serde_json::Deserializer::from_str(&normalized);
            deserializer.disable_recursion_limit();
            let value = serde_json::Value::deserialize(&mut deserializer)
                .map_err(|error| error.to_string())?;
            value_from_json_relaxed(value, &replacements)
        })
        .map_err(|error| format!("failed to spawn JSON parser worker: {error}"))?
        .join()
        .map_err(|_| "JSON parser worker panicked".to_string())?
}

fn singleton_empty_array_chain_text_depth(input: &str) -> Option<usize> {
    let mut open_count = 0usize;
    let mut close_count = 0usize;
    let mut seen_close = false;

    for ch in input.chars() {
        if ch.is_whitespace() {
            continue;
        }
        match ch {
            '[' if !seen_close => open_count = open_count.saturating_add(1),
            ']' => {
                seen_close = true;
                close_count = close_count.saturating_add(1);
            }
            _ => return None,
        }
    }

    if open_count == 0 || open_count != close_count {
        return None;
    }
    Some(open_count.saturating_sub(1))
}

fn build_singleton_empty_array_chain(depth: usize) -> Value {
    let mut value = Value::Array(Vec::new());
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    value
}

fn relaxed_json_numeric_literal_error(input: &str) -> Option<String> {
    let trimmed = input.trim();
    let invalid = trimmed
        .strip_prefix("NaN")
        .or_else(|| trimmed.strip_prefix("nan"))
        .or_else(|| trimmed.strip_prefix("-NaN"))
        .or_else(|| trimmed.strip_prefix("-nan"))?;
    if invalid.is_empty() || !invalid.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    Some(format!(
        "Invalid numeric literal at EOF at line 1, column {} (while parsing '{}')",
        trimmed.chars().count(),
        trimmed
    ))
}

fn relaxed_json_string_literal_error(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if !trimmed.starts_with("{'") {
        return None;
    }
    let closing_quote = trimmed[2..].find('\'')?;
    Some(format!(
        "Invalid string literal; expected \", but got ' at line 1, column {} (while parsing '{}')",
        closing_quote + 4,
        trimmed
    ))
}

fn normalize_relaxed_json_numbers(input: &str) -> (String, IndexMap<String, RelaxedJsonNumber>) {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut replacements = IndexMap::new();
    let mut index = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut counter = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            index += 1;
            continue;
        }

        if let Some((token_len, replacement)) = relaxed_json_number_at(&chars, index) {
            let placeholder = format!("__AQ_RELAXED_JSON_NONFINITE_{counter}__");
            counter = counter.saturating_add(1);
            replacements.insert(placeholder.clone(), replacement);
            out.push('"');
            out.push_str(&placeholder);
            out.push('"');
            index += token_len;
            continue;
        }

        out.push(ch);
        index += 1;
    }

    (out, replacements)
}

fn relaxed_json_number_at(chars: &[char], start: usize) -> Option<(usize, RelaxedJsonNumber)> {
    const TOKENS: [(&str, RelaxedJsonNumber); 8] = [
        ("-Infinity", RelaxedJsonNumber::NegativeInfinity),
        ("-infinity", RelaxedJsonNumber::NegativeInfinity),
        ("Infinity", RelaxedJsonNumber::PositiveInfinity),
        ("infinity", RelaxedJsonNumber::PositiveInfinity),
        ("-NaN", RelaxedJsonNumber::Nan),
        ("-nan", RelaxedJsonNumber::Nan),
        ("NaN", RelaxedJsonNumber::Nan),
        ("nan", RelaxedJsonNumber::Nan),
    ];

    for (token, replacement) in TOKENS {
        if matches_relaxed_json_token(chars, start, token) {
            return Some((token.chars().count(), replacement));
        }
    }
    None
}

fn matches_relaxed_json_token(chars: &[char], start: usize, token: &str) -> bool {
    let token_chars: Vec<char> = token.chars().collect();
    let end = start.saturating_add(token_chars.len());
    if end > chars.len() {
        return false;
    }
    if chars[start..end] != token_chars[..] {
        return false;
    }
    matches!(
        chars.get(end),
        None | Some(' ')
            | Some('\n')
            | Some('\r')
            | Some('\t')
            | Some(',')
            | Some(']')
            | Some('}')
            | Some(':')
    )
}

fn value_from_json_relaxed(
    value: serde_json::Value,
    replacements: &IndexMap<String, RelaxedJsonNumber>,
) -> Result<Value, String> {
    Ok(match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(value) => {
            Value::from_json(serde_json::Value::Number(value)).map_err(|error| error.to_string())?
        }
        serde_json::Value::String(value) => match replacements.get(&value) {
            Some(RelaxedJsonNumber::Nan) => Value::Float(f64::NAN),
            Some(RelaxedJsonNumber::PositiveInfinity) => Value::Float(f64::MAX),
            Some(RelaxedJsonNumber::NegativeInfinity) => Value::Float(-f64::MAX),
            None => Value::String(value),
        },
        serde_json::Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(value_from_json_relaxed(value, replacements)?);
            }
            Value::Array(out)
        }
        serde_json::Value::Object(values) => {
            let mut out = IndexMap::with_capacity(values.len());
            for (key, value) in values {
                out.insert(key, value_from_json_relaxed(value, replacements)?);
            }
            Value::Object(out)
        }
    })
}

impl Value {
    pub fn yaml_tag(&self) -> Option<&str> {
        match self {
            Value::Tagged { tag, .. } => Some(tag.as_str()),
            _ => None,
        }
    }

    pub fn untagged(&self) -> &Self {
        let mut current = self;
        while let Value::Tagged { value, .. } = current {
            current = value.as_ref();
        }
        current
    }

    pub fn without_yaml_tag(&self) -> Self {
        self.untagged().clone()
    }

    pub fn with_yaml_tag(&self, tag: impl Into<String>) -> Self {
        Value::Tagged {
            tag: tag.into(),
            value: Box::new(self.without_yaml_tag()),
        }
    }

    pub fn retagged_like(&self, value: Value) -> Self {
        match self {
            Value::Tagged { tag, value: inner } => Value::Tagged {
                tag: tag.clone(),
                value: Box::new(inner.retagged_like(value)),
            },
            _ => value,
        }
    }

    pub fn merged_with(&self, right: &Value, deep: bool) -> Self {
        if let Value::Tagged { value, .. } = self {
            return self.retagged_like(value.merged_with(right, deep));
        }

        match (self, right) {
            (Value::Object(left_fields), Value::Object(right_fields)) => {
                let mut merged = left_fields.clone();
                for (key, right_value) in right_fields {
                    let merged_value = if deep {
                        match merged.get(key) {
                            Some(left_value) => left_value.merged_with(right_value, true),
                            None => right_value.clone(),
                        }
                    } else {
                        right_value.clone()
                    };
                    merged.insert(key.clone(), merged_value);
                }
                Value::Object(merged)
            }
            _ => right.clone(),
        }
    }

    pub fn drop_nulls(&self, recursive: bool) -> Self {
        if let Value::Tagged { value, .. } = self {
            return self.retagged_like(value.drop_nulls(recursive));
        }

        match self {
            Value::Object(fields) => {
                let mut out = IndexMap::with_capacity(fields.len());
                for (key, field_value) in fields {
                    let field_value = if recursive {
                        field_value.drop_nulls(true)
                    } else {
                        field_value.clone()
                    };
                    if field_value != Value::Null {
                        out.insert(key.clone(), field_value);
                    }
                }
                Value::Object(out)
            }
            Value::Array(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    let value = if recursive {
                        value.drop_nulls(true)
                    } else {
                        value.clone()
                    };
                    if value != Value::Null {
                        out.push(value);
                    }
                }
                Value::Array(out)
            }
            _ => self.clone(),
        }
    }

    pub fn sort_object_keys(&self, recursive: bool) -> Self {
        if let Value::Tagged { value, .. } = self {
            return self.retagged_like(value.sort_object_keys(recursive));
        }

        match self {
            Value::Object(fields) => {
                let mut pairs = fields.iter().collect::<Vec<_>>();
                pairs.sort_by_key(|(key, _)| *key);
                let mut sorted = IndexMap::with_capacity(pairs.len());
                for (key, value) in pairs {
                    let value = if recursive {
                        value.sort_object_keys(true)
                    } else {
                        value.clone()
                    };
                    sorted.insert(key.clone(), value);
                }
                Value::Object(sorted)
            }
            Value::Array(values) if recursive => Value::Array(
                values
                    .iter()
                    .map(|value| value.sort_object_keys(true))
                    .collect(),
            ),
            _ => self.clone(),
        }
    }

    pub fn rendered_string(&self) -> Option<String> {
        match self {
            Value::String(value) => Some(value.clone()),
            Value::DateTime(value) => {
                Some(value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
            }
            Value::Date(value) => Some(value.to_string()),
            Value::Tagged { value, .. } => value.rendered_string(),
            _ => None,
        }
    }

    pub(crate) fn scalar_json_text(&self) -> Option<Result<String, AqError>> {
        match self.untagged() {
            Value::Null => Some(Ok("null".to_string())),
            Value::Bool(value) => Some(Ok(if *value { "true" } else { "false" }.to_string())),
            Value::Integer(value) => Some(Ok(value.to_string())),
            Value::Decimal(value) => Some(Ok(value.rendered().to_string())),
            Value::Float(value) if value.is_finite() => Some(Ok(render_finite_float(*value))),
            Value::Float(value) => Some(serde_json::to_string(&serde_json::json!(value)).map_err(
                |error| AqError::message(format!("failed to render JSON float: {error}")),
            )),
            Value::String(value) => Some(serde_json::to_string(value).map_err(|error| {
                AqError::message(format!("failed to render JSON string: {error}"))
            })),
            Value::DateTime(_) | Value::Date(_) => {
                let rendered = match self.rendered_string() {
                    Some(rendered) => rendered,
                    None => {
                        return Some(Err(AqError::message(
                            "failed to render string-like value".to_string(),
                        )));
                    }
                };
                Some(serde_json::to_string(&rendered).map_err(|error| {
                    AqError::message(format!("failed to render JSON date value: {error}"))
                }))
            }
            Value::Bytes(_) | Value::Array(_) | Value::Object(_) => None,
            Value::Tagged { .. } => unreachable!("untagged values should not be tagged"),
        }
    }

    pub fn to_json_text(&self) -> Result<String, AqError> {
        if let Some(rendered) = self.scalar_json_text() {
            return rendered;
        }
        if let Some(depth) = Self::singleton_empty_array_chain_depth(self) {
            return Ok(Self::render_singleton_empty_array_chain(depth, 0));
        }
        if self.nesting_depth_exceeds(LARGE_JSON_STACK_THRESHOLD) {
            return thread::scope(|scope| {
                thread::Builder::new()
                    .stack_size(LARGE_JSON_STACK_SIZE)
                    .spawn_scoped(scope, || Self::render_json_text(self, 0))
                    .map_err(|error| {
                        AqError::message(format!("failed to spawn JSON renderer worker: {error}"))
                    })?
                    .join()
                    .map_err(|_| AqError::message("JSON renderer worker panicked"))?
            });
        }
        Self::render_json_text(self, 0)
    }

    pub(crate) fn nesting_depth_exceeds(&self, limit: usize) -> bool {
        Self::json_print_exceeds_indent(self, limit)
    }

    pub(crate) fn json_text_contains_skip_marker_fast_path(&self) -> Option<bool> {
        if Self::json_text_might_contain_substring(self, JSON_SKIP_MARKER) {
            return None;
        }
        Some(self.nesting_depth_exceeds(MAX_JSON_PRINT_DEPTH))
    }

    fn json_print_exceeds_indent(value: &Value, limit: usize) -> bool {
        let mut stack = vec![(value, 0usize)];

        while let Some((value, indent)) = stack.pop() {
            if indent > limit {
                return true;
            }
            match value.untagged() {
                Value::Array(values) => {
                    for value in values.iter().rev() {
                        stack.push((value, indent + 1));
                    }
                }
                Value::Object(values) => {
                    for value in values.values().rev() {
                        stack.push((value, indent + 1));
                    }
                }
                _ => {}
            }
        }

        false
    }

    fn render_json_text(value: &Value, indent: usize) -> Result<String, AqError> {
        let mut out = String::new();
        Self::append_json_text(value, indent, &mut out)?;
        Ok(out)
    }

    fn render_singleton_empty_array_chain(depth: usize, indent: usize) -> String {
        let mut out = String::with_capacity(depth.saturating_mul(2).saturating_add(32));
        for current_depth in 0..depth {
            if indent + current_depth > MAX_JSON_PRINT_DEPTH {
                out.push_str(JSON_SKIP_MARKER);
                for _ in current_depth..depth {
                    out.push(']');
                }
                return out;
            }
            out.push('[');
        }
        if indent + depth > MAX_JSON_PRINT_DEPTH {
            out.push_str(JSON_SKIP_MARKER);
        } else {
            out.push_str("[]");
        }
        for _ in 0..depth {
            out.push(']');
        }
        out
    }

    fn append_json_text(value: &Value, indent: usize, out: &mut String) -> Result<(), AqError> {
        if indent > MAX_JSON_PRINT_DEPTH {
            out.push_str(JSON_SKIP_MARKER);
            return Ok(());
        }

        match value.untagged() {
            Value::Null => out.push_str("null"),
            Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
            Value::Integer(value) => out.push_str(&value.to_string()),
            Value::Decimal(value) => out.push_str(value.rendered()),
            Value::Float(value) if value.is_finite() => out.push_str(&render_finite_float(*value)),
            Value::Float(value) => {
                out.push_str(&serde_json::to_string(&serde_json::json!(value)).map_err(
                    |error| AqError::message(format!("failed to render JSON float: {error}")),
                )?)
            }
            Value::String(value) => {
                out.push_str(&serde_json::to_string(value).map_err(|error| {
                    AqError::message(format!("failed to render JSON string: {error}"))
                })?)
            }
            Value::Array(values) => {
                if Self::append_singleton_array_chain_json_text(value, indent, out)? {
                    return Ok(());
                }
                out.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    Self::append_json_text(value, indent + 1, out)?;
                }
                out.push(']');
            }
            Value::Object(values) => {
                out.push('{');
                for (index, (key, value)) in values.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(key).map_err(|error| {
                        AqError::message(format!("failed to render JSON object key: {error}"))
                    })?);
                    out.push(':');
                    Self::append_json_text(value, indent + 1, out)?;
                }
                out.push('}');
            }
            Value::Bytes(_) => {
                out.push_str(&serde_json::to_string(&value.to_json()?).map_err(|error| {
                    AqError::message(format!("failed to render JSON bytes: {error}"))
                })?)
            }
            Value::DateTime(_) | Value::Date(_) => {
                out.push_str(
                    &serde_json::to_string(&value.rendered_string().ok_or_else(|| {
                        AqError::message("failed to render string-like value".to_string())
                    })?)
                    .map_err(|error| {
                        AqError::message(format!("failed to render JSON date value: {error}"))
                    })?,
                );
            }
            Value::Tagged { .. } => unreachable!("untagged values should not be tagged"),
        }

        Ok(())
    }

    fn append_singleton_array_chain_json_text(
        value: &Value,
        indent: usize,
        out: &mut String,
    ) -> Result<bool, AqError> {
        if let Some(depth) = Self::singleton_empty_array_chain_depth(value) {
            out.push_str(&Self::render_singleton_empty_array_chain(depth, indent));
            return Ok(true);
        }

        let mut depth = 0usize;
        let mut current = value;

        while let Value::Array(values) = current.untagged() {
            if values.len() != 1 {
                break;
            }
            if indent + depth > MAX_JSON_PRINT_DEPTH {
                out.push_str(JSON_SKIP_MARKER);
                for _ in 0..depth {
                    out.push(']');
                }
                return Ok(true);
            }
            out.push('[');
            depth += 1;
            current = &values[0];
        }

        if depth == 0 {
            return Ok(false);
        }

        if indent + depth > MAX_JSON_PRINT_DEPTH {
            out.push_str(JSON_SKIP_MARKER);
        } else {
            Self::append_json_text(current, indent + depth, out)?;
        }
        for _ in 0..depth {
            out.push(']');
        }
        Ok(true)
    }

    fn json_text_might_contain_substring(value: &Value, needle: &str) -> bool {
        let mut stack = vec![value];
        while let Some(current) = stack.pop() {
            match current {
                Value::Tagged { value, .. } => stack.push(value),
                Value::String(text) if text.contains(needle) => return true,
                Value::String(_) => {}
                Value::Object(values) => {
                    if values.keys().any(|key| key.contains(needle)) {
                        return true;
                    }
                    for value in values.values() {
                        stack.push(value);
                    }
                }
                Value::Array(values) => {
                    for value in values {
                        stack.push(value);
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn singleton_empty_array_chain_depth(value: &Value) -> Option<usize> {
        let mut depth = 0usize;
        let mut current = value;
        while let Value::Array(values) = current.untagged() {
            if values.is_empty() {
                return Some(depth);
            }
            if values.len() != 1 {
                return None;
            }
            depth += 1;
            current = &values[0];
        }
        None
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, AqError> {
        Ok(match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(value) => Value::Bool(value),
            serde_json::Value::Number(value) => {
                let rendered = value.to_string();
                if rendered_requires_exact_decimal(&rendered) {
                    Value::Decimal(DecimalValue::parse(&rendered)?)
                } else if let Some(value) = value.as_i64() {
                    Value::Integer(value)
                } else if let Some(value) = value.as_u64() {
                    if let Ok(value) = i64::try_from(value) {
                        Value::Integer(value)
                    } else {
                        Value::Decimal(DecimalValue::parse(&rendered)?)
                    }
                } else {
                    Value::Decimal(DecimalValue::parse(&rendered)?)
                }
            }
            serde_json::Value::String(value) => Value::String(value),
            serde_json::Value::Array(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(Value::from_json(value)?);
                }
                Value::Array(out)
            }
            serde_json::Value::Object(values) => {
                let mut out = IndexMap::with_capacity(values.len());
                for (key, value) in values {
                    out.insert(key, Value::from_json(value)?);
                }
                Value::Object(out)
            }
        })
    }

    pub fn from_yaml_str(input: &str) -> Result<Vec<Self>, AqError> {
        let mut documents = Vec::new();
        for document in serde_yaml_ng::Deserializer::from_str(input) {
            let value = serde_yaml_ng::Value::deserialize(document).map_err(|error| {
                AqError::ParseInput {
                    format: "yaml",
                    message: error.to_string(),
                }
            })?;
            documents.push(Self::from_yaml(value)?);
        }
        Ok(documents)
    }

    pub fn from_yaml(value: serde_yaml_ng::Value) -> Result<Self, AqError> {
        Ok(match value {
            serde_yaml_ng::Value::Null => Value::Null,
            serde_yaml_ng::Value::Bool(value) => Value::Bool(value),
            serde_yaml_ng::Value::Number(value) => {
                let rendered = value.to_string();
                if rendered_requires_exact_decimal(&rendered) {
                    Value::Decimal(DecimalValue::parse(&rendered)?)
                } else if let Some(value) = value.as_i64() {
                    Value::Integer(value)
                } else if let Some(value) = value.as_u64() {
                    if let Ok(value) = i64::try_from(value) {
                        Value::Integer(value)
                    } else {
                        Value::Decimal(DecimalValue::parse(&rendered)?)
                    }
                } else {
                    Value::Decimal(DecimalValue::parse(&rendered)?)
                }
            }
            serde_yaml_ng::Value::String(value) => Value::String(value),
            serde_yaml_ng::Value::Sequence(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(Value::from_yaml(value)?);
                }
                Value::Array(out)
            }
            serde_yaml_ng::Value::Mapping(values) => {
                let mut out = IndexMap::with_capacity(values.len());
                for (key, value) in values {
                    let key = match key {
                        serde_yaml_ng::Value::String(key) => key,
                        _ => {
                            return Err(AqError::message(
                                "YAML mappings with non-string keys are not supported yet",
                            ));
                        }
                    };
                    out.insert(key, Value::from_yaml(value)?);
                }
                Value::Object(out)
            }
            serde_yaml_ng::Value::Tagged(tagged) => Value::Tagged {
                tag: tagged.tag.to_string(),
                value: Box::new(Value::from_yaml(tagged.value)?),
            },
        })
    }

    pub fn from_toml(value: toml::Value) -> Result<Self, AqError> {
        Ok(match value {
            toml::Value::String(value) => Value::String(value),
            toml::Value::Integer(value) => Value::Integer(value),
            toml::Value::Float(value) => Value::Decimal(DecimalValue::parse(&value.to_string())?),
            toml::Value::Boolean(value) => Value::Bool(value),
            toml::Value::Datetime(value) => Value::from_toml_datetime(value)?,
            toml::Value::Array(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(Value::from_toml(value)?);
                }
                Value::Array(out)
            }
            toml::Value::Table(values) => {
                let mut out = IndexMap::with_capacity(values.len());
                for (key, value) in values {
                    out.insert(key, Value::from_toml(value)?);
                }
                Value::Object(out)
            }
        })
    }

    pub fn to_json(&self) -> Result<serde_json::Value, AqError> {
        Ok(match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(value) => serde_json::Value::Bool(*value),
            Value::Integer(value) => serde_json::Value::Number((*value).into()),
            Value::Decimal(value) => {
                serde_json::Value::Number(value.rendered().parse::<serde_json::Number>().map_err(
                    |error| {
                        AqError::message(format!(
                            "failed to render arbitrary-precision JSON number `{}`: {error}",
                            value.rendered()
                        ))
                    },
                )?)
            }
            Value::Float(value) if value.is_finite() => serde_json::Value::Number(
                render_finite_float(*value)
                    .parse::<serde_json::Number>()
                    .map_err(|error| {
                        AqError::message(format!(
                            "failed to render finite JSON float `{value}`: {error}"
                        ))
                    })?,
            ),
            Value::Float(value) => serde_json::json!(value),
            Value::String(value) => serde_json::Value::String(value.clone()),
            Value::Array(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(value.to_json()?);
                }
                serde_json::Value::Array(out)
            }
            Value::Object(values) => {
                let mut out = serde_json::Map::with_capacity(values.len());
                for (key, value) in values {
                    out.insert(key.clone(), value.to_json()?);
                }
                serde_json::Value::Object(out)
            }
            Value::Bytes(value) => serde_json::json!(value),
            Value::DateTime(_) | Value::Date(_) => serde_json::Value::String(
                self.rendered_string()
                    .ok_or_else(|| AqError::message("failed to render string-like value"))?,
            ),
            Value::Tagged { value, .. } => value.to_json()?,
        })
    }

    pub fn to_toml(&self) -> Result<toml::Value, AqError> {
        Ok(match self {
            Value::Null => {
                return Err(AqError::UnsupportedOutputFormat(
                    "toml does not support null values".to_string(),
                ));
            }
            Value::Bool(value) => toml::Value::Boolean(*value),
            Value::Integer(value) => toml::Value::Integer(*value),
            Value::Decimal(value) => {
                if let Some(value) = value.as_i64_exact() {
                    toml::Value::Integer(value)
                } else {
                    let value = value.rendered().parse::<f64>().map_err(|error| {
                        AqError::UnsupportedOutputFormat(format!(
                            "failed to render TOML decimal value `{}`: {error}",
                            value.rendered()
                        ))
                    })?;
                    if !value.is_finite() {
                        return Err(AqError::UnsupportedOutputFormat(
                            "toml cannot represent arbitrary-precision decimals outside f64 range"
                                .to_string(),
                        ));
                    }
                    toml::Value::Float(value)
                }
            }
            Value::Float(value) => toml::Value::Float(*value),
            Value::String(value) => toml::Value::String(value.clone()),
            Value::Array(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(value.to_toml()?);
                }
                toml::Value::Array(out)
            }
            Value::Object(values) => {
                let mut out = toml::map::Map::new();
                for (key, value) in values {
                    out.insert(key.clone(), value.to_toml()?);
                }
                toml::Value::Table(out)
            }
            Value::Bytes(_) => {
                return Err(AqError::UnsupportedOutputFormat(
                    "toml does not support raw byte values".to_string(),
                ));
            }
            Value::DateTime(_) | Value::Date(_) => {
                let rendered = self
                    .rendered_string()
                    .ok_or_else(|| AqError::message("failed to render string-like value"))?;
                let datetime = rendered.parse::<toml::value::Datetime>().map_err(|error| {
                    AqError::UnsupportedOutputFormat(format!(
                        "failed to render TOML datetime value: {error}"
                    ))
                })?;
                toml::Value::Datetime(datetime)
            }
            Value::Tagged { value, .. } => value.to_toml()?,
        })
    }

    pub fn to_yaml(&self) -> Result<serde_yaml_ng::Value, AqError> {
        Ok(match self {
            Value::Null => serde_yaml_ng::Value::Null,
            Value::Bool(value) => serde_yaml_ng::Value::Bool(*value),
            Value::Integer(value) => serde_yaml_ng::Value::Number((*value).into()),
            Value::Decimal(value) => {
                let parsed = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(value.rendered())
                    .map_err(|error| {
                        AqError::UnsupportedOutputFormat(format!(
                            "failed to render YAML decimal value `{}`: {error}",
                            value.rendered()
                        ))
                    })?;
                match parsed {
                    serde_yaml_ng::Value::Number(_) => parsed,
                    _ => {
                        return Err(AqError::UnsupportedOutputFormat(format!(
                            "yaml did not accept decimal scalar `{}` as numeric output",
                            value.rendered()
                        )));
                    }
                }
            }
            Value::Float(value) if value.is_finite() => {
                let rendered = render_finite_float(*value);
                let parsed = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&rendered).map_err(
                    |error| {
                        AqError::UnsupportedOutputFormat(format!(
                            "failed to render YAML float value `{rendered}`: {error}"
                        ))
                    },
                )?;
                match parsed {
                    serde_yaml_ng::Value::Number(_) => parsed,
                    _ => {
                        return Err(AqError::UnsupportedOutputFormat(format!(
                            "yaml did not accept float scalar `{rendered}` as numeric output"
                        )));
                    }
                }
            }
            Value::Float(value) => serde_yaml_ng::Value::Number((*value).into()),
            Value::String(value) => serde_yaml_ng::Value::String(value.clone()),
            Value::Array(values) => {
                serde_yaml_ng::Value::Sequence(values.iter().map(Value::to_yaml).collect::<Result<
                    Vec<_>,
                    _,
                >>(
                )?)
            }
            Value::Object(values) => {
                let mut out = serde_yaml_ng::Mapping::new();
                for (key, value) in values {
                    out.insert(serde_yaml_ng::Value::String(key.clone()), value.to_yaml()?);
                }
                serde_yaml_ng::Value::Mapping(out)
            }
            Value::Bytes(values) => serde_yaml_ng::Value::Sequence(
                values
                    .iter()
                    .map(|value| serde_yaml_ng::Value::Number(i64::from(*value).into()))
                    .collect(),
            ),
            Value::DateTime(_) | Value::Date(_) => serde_yaml_ng::Value::String(
                self.rendered_string()
                    .ok_or_else(|| AqError::message("failed to render string-like value"))?,
            ),
            Value::Tagged { tag, value } => {
                serde_yaml_ng::Value::Tagged(Box::new(YamlTaggedValue {
                    tag: YamlTag::new(tag.clone()),
                    value: value.to_yaml()?,
                }))
            }
        })
    }

    fn from_toml_datetime(value: toml::value::Datetime) -> Result<Self, AqError> {
        let raw = value.to_string();
        if value.date.is_some() && value.time.is_none() && value.offset.is_none() {
            let date = parse_date_string(&raw)
                .ok_or_else(|| AqError::message(format!("failed to parse TOML date `{raw}`")))?;
            return Ok(Value::Date(date));
        }

        if value.date.is_some() && value.time.is_some() && value.offset.is_some() {
            let datetime = parse_rfc3339_datetime(&raw).map_err(|error| {
                AqError::message(format!("failed to parse TOML datetime `{raw}`: {error}"))
            })?;
            return Ok(Value::DateTime(datetime));
        }

        Err(AqError::message(format!(
            "unsupported TOML datetime literal `{raw}`: local times and offset-free datetimes are not supported yet"
        )))
    }
}

pub(crate) fn parse_date_string(raw: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()
}

pub(crate) fn parse_rfc3339_datetime(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    chrono::DateTime::parse_from_rfc3339(raw).map(|value| value.with_timezone(&Utc))
}

pub(crate) fn datetime_at_midnight(date: &NaiveDate) -> Result<DateTime<Utc>, AqError> {
    date.and_hms_opt(0, 0, 0)
        .map(|value| value.and_utc())
        .ok_or_else(|| AqError::message("failed to construct midnight datetime"))
}

pub(crate) fn parse_common_datetime_string(raw: &str) -> Option<DateTime<Utc>> {
    parse_rfc3339_datetime(raw)
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| parse_date_string(raw).and_then(|value| datetime_at_midnight(&value).ok()))
}

#[cfg(test)]
mod tests {
    use crate::value::{parse_json_str, Value, MAX_JSON_PARSE_DEPTH, MAX_JSON_PRINT_DEPTH};

    fn nested_json_array(depth: usize) -> String {
        let mut out = String::with_capacity(depth * 2);
        for _ in 0..depth {
            out.push('[');
        }
        for _ in 0..depth {
            out.push(']');
        }
        out
    }

    fn nested_empty_array(depth: usize) -> Value {
        let mut value = Value::Array(Vec::new());
        for _ in 0..depth {
            value = Value::Array(vec![value]);
        }
        value
    }

    #[test]
    fn supports_jq_json_parse_depth_contract() {
        assert!(parse_json_str(&nested_json_array(MAX_JSON_PARSE_DEPTH)).is_ok());

        let error = parse_json_str(&nested_json_array(MAX_JSON_PARSE_DEPTH + 1))
            .expect_err("parsing should reject values beyond jq's depth limit");
        assert!(error.contains("Exceeds depth limit for parsing"));
    }

    #[test]
    fn supports_jq_json_print_depth_contract() {
        let rendered = nested_empty_array(MAX_JSON_PRINT_DEPTH)
            .to_json_text()
            .expect("renderer should support jq's maximum print depth");
        assert!(!rendered.contains("<skipped: too deep>"));

        let rendered = nested_empty_array(MAX_JSON_PRINT_DEPTH + 1)
            .to_json_text()
            .expect("renderer should skip values beyond jq's maximum print depth");
        assert!(rendered.contains("<skipped: too deep>"));
    }

    #[test]
    fn renders_singleton_array_chains_without_semantic_changes() {
        let rendered = nested_empty_array(4)
            .to_json_text()
            .expect("renderer should handle shallow singleton chains");
        assert_eq!(rendered, "[[[[[]]]]]");
    }

    #[test]
    fn parses_singleton_array_chains_without_full_json_parser_work() {
        let parsed = parse_json_str(" [ [ [ ] ] ] ").expect("json should parse");
        assert_eq!(parsed, nested_empty_array(2));
    }

    #[test]
    fn preserves_exact_decimal_rendering_from_json_input() {
        let rendered = parse_json_str("[1, 1.000, 1.0, 100e-2]")
            .expect("json should parse")
            .to_json_text()
            .expect("json should render");
        assert_eq!(rendered, "[1,1.000,1.0,1.00]");
    }

    #[test]
    fn compares_exact_decimals_by_numeric_value() {
        let integer = parse_json_str("10").expect("json should parse");
        let decimal = parse_json_str("10.0").expect("json should parse");
        assert_eq!(integer, decimal);
    }
}
