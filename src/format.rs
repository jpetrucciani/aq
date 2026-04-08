use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use serde::Serialize;

use crate::error::AqError;
use crate::value::{parse_json_str, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Json,
    Jsonl,
    Toml,
    Yaml,
    Csv,
    Tsv,
    Table,
}

impl fmt::Display for Format {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Format::Json => formatter.write_str("json"),
            Format::Jsonl => formatter.write_str("jsonl"),
            Format::Toml => formatter.write_str("toml"),
            Format::Yaml => formatter.write_str("yaml"),
            Format::Csv => formatter.write_str("csv"),
            Format::Tsv => formatter.write_str("tsv"),
            Format::Table => formatter.write_str("table"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DetectConflictPolicy {
    WarnFallback,
    Extension,
    Sniff,
}

impl fmt::Display for DetectConflictPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DetectConflictPolicy::WarnFallback => formatter.write_str("warn-fallback"),
            DetectConflictPolicy::Extension => formatter.write_str("extension"),
            DetectConflictPolicy::Sniff => formatter.write_str("sniff"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum TabularCoercion {
    #[default]
    Strings,
    InferScalars,
}

impl fmt::Display for TabularCoercion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TabularCoercion::Strings => formatter.write_str("strings"),
            TabularCoercion::InferScalars => formatter.write_str("infer-scalars"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InputDocument {
    pub value: Value,
    pub format: Format,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputTerminator {
    #[default]
    Newline,
    None,
    Nul,
}

impl OutputTerminator {
    fn as_str(self) -> &'static str {
        match self {
            OutputTerminator::Newline => "\n",
            OutputTerminator::None => "",
            OutputTerminator::Nul => "\0",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonIndent {
    Spaces(u8),
    Tab,
}

impl Default for JsonIndent {
    fn default() -> Self {
        JsonIndent::Spaces(2)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RenderOptions {
    pub compact: bool,
    pub raw_output: bool,
    pub terminator: OutputTerminator,
    pub sort_keys: bool,
    pub json_indent: JsonIndent,
}

pub fn read_path(
    path: &Path,
    override_format: Option<Format>,
    raw_input: bool,
    detect_conflicts: DetectConflictPolicy,
) -> Result<Vec<InputDocument>, AqError> {
    read_path_with_tabular_coercion(
        path,
        override_format,
        raw_input,
        detect_conflicts,
        TabularCoercion::Strings,
    )
}

pub fn read_path_with_tabular_coercion(
    path: &Path,
    override_format: Option<Format>,
    raw_input: bool,
    detect_conflicts: DetectConflictPolicy,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<InputDocument>, AqError> {
    let bytes = fs::read(path).map_err(|error| AqError::io(Some(path.to_path_buf()), error))?;
    read_bytes(
        Some(path.to_path_buf()),
        &bytes,
        override_format,
        raw_input,
        detect_conflicts,
        tabular_coercion,
    )
}

pub fn read_stdin_with_tabular_coercion(
    override_format: Option<Format>,
    raw_input: bool,
    detect_conflicts: DetectConflictPolicy,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<InputDocument>, AqError> {
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut std::io::stdin(), &mut bytes)
        .map_err(|error| AqError::io(None, error))?;
    read_bytes(
        None,
        &bytes,
        override_format,
        raw_input,
        detect_conflicts,
        tabular_coercion,
    )
}

pub fn render(values: &[Value], format: Format, options: RenderOptions) -> Result<String, AqError> {
    if options.raw_output {
        return render_raw(values, format, options);
    }
    render_non_raw(values, format, options)
}

pub fn parse_text(input: &str, format: Format) -> Result<Vec<Value>, AqError> {
    parse_text_with_tabular_coercion(input, format, TabularCoercion::Strings)
}

pub fn parse_text_with_tabular_coercion(
    input: &str,
    format: Format,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<Value>, AqError> {
    read_bytes(
        None,
        input.as_bytes(),
        Some(format),
        false,
        DetectConflictPolicy::WarnFallback,
        tabular_coercion,
    )
    .map(|documents| {
        documents
            .into_iter()
            .map(|document| document.value)
            .collect()
    })
}

pub fn default_output_format(formats: &[Format]) -> Format {
    if formats.is_empty() {
        return Format::Json;
    }
    let first = formats[0];
    if formats.iter().all(|format| *format == first) {
        first
    } else {
        Format::Json
    }
}

pub fn detect_format_for_input(
    path: Option<&Path>,
    input: &str,
    detect_conflicts: DetectConflictPolicy,
) -> Format {
    resolve_detected_format(path, strip_utf8_bom(input), detect_conflicts)
}

pub fn detect_format_from_path(path: &Path) -> Option<Format> {
    detect_format_from_extension(Some(path))
}

fn read_bytes(
    path: Option<PathBuf>,
    bytes: &[u8],
    override_format: Option<Format>,
    raw_input: bool,
    detect_conflicts: DetectConflictPolicy,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<InputDocument>, AqError> {
    let input = String::from_utf8(bytes.to_vec())
        .map_err(|error| AqError::message(format!("input is not valid UTF-8: {error}")))?;

    if raw_input {
        return Ok(input
            .lines()
            .map(|line| InputDocument {
                value: Value::String(line.to_string()),
                format: Format::Json,
            })
            .collect());
    }

    let resolved_format = if let Some(format) = override_format {
        format
    } else {
        resolve_detected_format(path.as_deref(), &input, detect_conflicts)
    };

    let documents = parse_documents(&input, resolved_format, tabular_coercion)?;

    Ok(documents
        .into_iter()
        .map(|value| InputDocument {
            value,
            format: resolved_format,
        })
        .collect())
}

fn resolve_detected_format(
    path: Option<&Path>,
    input: &str,
    detect_conflicts: DetectConflictPolicy,
) -> Format {
    let input = strip_utf8_bom(input);
    let extension = detect_format_from_extension(path);
    let sniffed = sniff_format(input);
    match (extension, detect_conflicts) {
        (Some(extension), DetectConflictPolicy::WarnFallback) => {
            if let Some(sniffed) =
                sniffed.filter(|sniffed| formats_conflict(extension, *sniffed, input))
            {
                eprintln!("warning: detected content as {sniffed}, falling back from {extension}");
                sniffed
            } else {
                extension
            }
        }
        (Some(extension), DetectConflictPolicy::Extension) => extension,
        (Some(extension), DetectConflictPolicy::Sniff) => sniffed
            .filter(|sniffed| formats_conflict(extension, *sniffed, input))
            .unwrap_or(extension),
        (None, _) => sniffed.unwrap_or(Format::Yaml),
    }
}

fn formats_conflict(extension: Format, sniffed: Format, input: &str) -> bool {
    if extension == sniffed {
        return false;
    }

    !matches!(
        (extension, sniffed),
        (Format::Jsonl, Format::Json) if is_single_json_record(input)
    )
}

fn is_single_json_record(input: &str) -> bool {
    let mut non_empty_lines = input.lines().map(str::trim).filter(|line| !line.is_empty());
    let Some(line) = non_empty_lines.next() else {
        return false;
    };
    if non_empty_lines.next().is_some() {
        return false;
    }
    parse_json_str(line).is_ok()
}

fn parse_documents(
    input: &str,
    format: Format,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<Value>, AqError> {
    let input = strip_utf8_bom(input);
    match format {
        Format::Json => {
            let value = parse_json_str(input).map_err(|error| AqError::ParseInput {
                format: "json",
                message: error,
            })?;
            Ok(vec![value])
        }
        Format::Jsonl => {
            let mut documents = Vec::new();
            for (line_number, line) in input.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let value = parse_json_str(line).map_err(|error| AqError::ParseInput {
                    format: "jsonl",
                    message: format!("line {}: {}", line_number + 1, error),
                })?;
                documents.push(value);
            }
            Ok(documents)
        }
        Format::Toml => {
            let value =
                toml::from_str::<toml::Value>(input).map_err(|error| AqError::ParseInput {
                    format: "toml",
                    message: error.to_string(),
                })?;
            Ok(vec![Value::from_toml(value)?])
        }
        Format::Yaml => Value::from_yaml_str(input),
        Format::Csv => parse_csv_documents(input, tabular_coercion),
        Format::Tsv => parse_tsv_documents(input, tabular_coercion),
        Format::Table => Err(AqError::ParseInput {
            format: "table",
            message: "table is an output-only format".to_string(),
        }),
    }
}

fn strip_utf8_bom(input: &str) -> &str {
    input.strip_prefix('\u{FEFF}').unwrap_or(input)
}

fn detect_format_from_extension(path: Option<&Path>) -> Option<Format> {
    let extension = path
        .and_then(|path| path.extension())
        .and_then(|ext| ext.to_str())?;
    match extension {
        "json" => Some(Format::Json),
        "jsonl" | "ndjson" => Some(Format::Jsonl),
        "toml" => Some(Format::Toml),
        "yaml" | "yml" => Some(Format::Yaml),
        "csv" => Some(Format::Csv),
        "tsv" => Some(Format::Tsv),
        _ => None,
    }
}

fn sniff_format(input: &str) -> Option<Format> {
    let trimmed = input.trim_start();
    if trimmed.trim().is_empty() {
        return None;
    }
    let non_empty_lines: Vec<&str> = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if non_empty_lines.len() > 1
        && non_empty_lines
            .iter()
            .all(|line| line.starts_with('{') || line.starts_with('['))
    {
        return Some(Format::Jsonl);
    }

    if (trimmed.starts_with('{') || trimmed.starts_with('[')) && parse_json_str(trimmed).is_ok() {
        return Some(Format::Json);
    }

    if toml::from_str::<toml::Value>(input).is_ok() {
        return Some(Format::Toml);
    }

    if looks_like_tsv(input) {
        return Some(Format::Tsv);
    }

    if looks_like_csv(input) {
        return Some(Format::Csv);
    }

    if looks_like_yaml(input) {
        return Some(Format::Yaml);
    }

    None
}

fn render_non_raw(
    values: &[Value],
    format: Format,
    options: RenderOptions,
) -> Result<String, AqError> {
    let mut out = String::new();
    match format {
        Format::Json => {
            for value in values {
                let rendered = render_json_value(value, options)?;
                out.push_str(&rendered);
                out.push('\n');
            }
        }
        Format::Jsonl => {
            for value in values {
                let rendered = render_jsonl_value(value, options)?;
                out.push_str(&rendered);
                out.push('\n');
            }
        }
        Format::Toml => {
            if values.len() != 1 {
                return Err(AqError::OutputShape {
                    format: Format::Toml,
                });
            }
            out.push_str(&render_toml_value(
                &prepare_output_value(&values[0], options.sort_keys),
                options,
            )?);
            out.push('\n');
        }
        Format::Yaml => {
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push_str("---\n");
                }
                out.push_str(&render_yaml_value(&prepare_output_value(
                    value,
                    options.sort_keys,
                ))?);
            }
        }
        Format::Csv => {
            for value in values {
                out.push_str(&render_csv_row(value)?);
                out.push('\n');
            }
        }
        Format::Tsv => {
            for value in values {
                out.push_str(&render_tsv_row(value)?);
                out.push('\n');
            }
        }
        Format::Table => {
            let rendered = render_table(values, options)?;
            if !rendered.is_empty() {
                out.push_str(&rendered);
                out.push('\n');
            }
        }
    }
    Ok(out)
}

fn render_raw(values: &[Value], format: Format, options: RenderOptions) -> Result<String, AqError> {
    if matches!(format, Format::Table) {
        return Err(AqError::message(
            "table output does not support --raw-output, --raw-output0, or --join-output",
        ));
    }

    let mut out = String::new();
    for value in values {
        if let Some(value) = value.rendered_string() {
            if matches!(options.terminator, OutputTerminator::Nul) && value.contains('\0') {
                return Err(AqError::message(
                    "Cannot dump a string containing NUL with --raw-output0 option",
                ));
            }
            out.push_str(&value);
        } else {
            let rendered = match format {
                Format::Json => render_json_value(value, options)?,
                Format::Jsonl => render_jsonl_value(value, options)?,
                Format::Toml => {
                    render_toml_value(&prepare_output_value(value, options.sort_keys), options)?
                }
                Format::Yaml => render_yaml_value(&prepare_output_value(value, options.sort_keys))?,
                Format::Csv => render_csv_row(value)?,
                Format::Tsv => render_tsv_row(value)?,
                Format::Table => unreachable!("table output is rejected above"),
            };
            out.push_str(&rendered);
        }
        out.push_str(options.terminator.as_str());
    }
    Ok(out)
}

fn render_json_value(value: &Value, options: RenderOptions) -> Result<String, AqError> {
    let json = prepare_output_value(value, options.sort_keys).to_json()?;
    if options.compact {
        return serde_json::to_string(&json)
            .map_err(|error| AqError::UnsupportedOutputFormat(error.to_string()));
    }

    let indent_bytes = match options.json_indent {
        JsonIndent::Spaces(width) => vec![b' '; usize::from(width)],
        JsonIndent::Tab => vec![b'\t'],
    };
    serialize_json_with_formatter(&json, &indent_bytes)
}

fn render_jsonl_value(value: &Value, options: RenderOptions) -> Result<String, AqError> {
    let json = prepare_output_value(value, options.sort_keys).to_json()?;
    serde_json::to_string(&json)
        .map_err(|error| AqError::UnsupportedOutputFormat(error.to_string()))
}

fn render_toml_value(value: &Value, options: RenderOptions) -> Result<String, AqError> {
    let value = value.to_toml()?;
    let toml::Value::Table(_) = value else {
        return Err(AqError::UnsupportedOutputFormat(
            "toml output requires an object at the document root".to_string(),
        ));
    };

    let mut out = if options.compact {
        toml::to_string(&value)
    } else {
        toml::to_string_pretty(&value)
    }
    .map_err(|error| AqError::UnsupportedOutputFormat(error.to_string()))?;
    if out.ends_with('\n') {
        out.pop();
    }
    Ok(out)
}

fn render_yaml_value(value: &Value) -> Result<String, AqError> {
    if let Value::Decimal(value) = value.untagged() {
        return Ok(format!("{}\n", value.rendered()));
    }
    serde_yaml_ng::to_string(&value.to_yaml()?)
        .map_err(|error| AqError::UnsupportedOutputFormat(error.to_string()))
}

fn prepare_output_value(value: &Value, sort_keys: bool) -> Value {
    if sort_keys {
        value.sort_object_keys(true)
    } else {
        value.clone()
    }
}

fn serialize_json_with_formatter(
    json: &serde_json::Value,
    indent: &[u8],
) -> Result<String, AqError> {
    let mut bytes = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(indent);
    let mut serializer = serde_json::Serializer::with_formatter(&mut bytes, formatter);
    json.serialize(&mut serializer)
        .map_err(|error| AqError::UnsupportedOutputFormat(error.to_string()))?;
    String::from_utf8(bytes).map_err(|error| AqError::UnsupportedOutputFormat(error.to_string()))
}

fn parse_csv_documents(
    input: &str,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<Value>, AqError> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = input.chars().peekable();
    let mut in_quotes = false;
    let mut closed_quote = false;
    let mut quoted_field = false;
    let mut row_started = false;

    while let Some(ch) = chars.next() {
        if in_quotes {
            match ch {
                '"' => {
                    if matches!(chars.peek(), Some('"')) {
                        field.push('"');
                        let _ = chars.next();
                    } else {
                        in_quotes = false;
                        closed_quote = true;
                    }
                }
                other => field.push(other),
            }
            continue;
        }

        match ch {
            '"' if field.is_empty() => {
                in_quotes = true;
                quoted_field = true;
                row_started = true;
            }
            '"' => {
                return Err(AqError::ParseInput {
                    format: "csv",
                    message: "unexpected quote in unquoted field".to_string(),
                });
            }
            ',' => {
                row.push(std::mem::take(&mut field));
                closed_quote = false;
                quoted_field = false;
                row_started = true;
            }
            '\n' => {
                finalize_csv_row(
                    &mut rows,
                    &mut row,
                    &mut field,
                    quoted_field,
                    row_started,
                    tabular_coercion,
                );
                closed_quote = false;
                quoted_field = false;
                row_started = false;
            }
            '\r' => {
                if matches!(chars.peek(), Some('\n')) {
                    let _ = chars.next();
                }
                finalize_csv_row(
                    &mut rows,
                    &mut row,
                    &mut field,
                    quoted_field,
                    row_started,
                    tabular_coercion,
                );
                closed_quote = false;
                quoted_field = false;
                row_started = false;
            }
            other => {
                if closed_quote {
                    return Err(AqError::ParseInput {
                        format: "csv",
                        message: "unexpected characters after closing quote".to_string(),
                    });
                }
                field.push(other);
                row_started = true;
            }
        }
    }

    if in_quotes {
        return Err(AqError::ParseInput {
            format: "csv",
            message: "unterminated quoted field".to_string(),
        });
    }

    finalize_csv_row(
        &mut rows,
        &mut row,
        &mut field,
        quoted_field,
        row_started,
        tabular_coercion,
    );

    Ok(rows)
}

fn finalize_csv_row(
    rows: &mut Vec<Value>,
    row: &mut Vec<String>,
    field: &mut String,
    quoted_field: bool,
    row_started: bool,
    tabular_coercion: TabularCoercion,
) {
    if !row_started && row.is_empty() && field.is_empty() && !quoted_field {
        return;
    }
    row.push(std::mem::take(field));
    rows.push(csv_row_value(std::mem::take(row), tabular_coercion));
}

fn parse_tsv_documents(
    input: &str,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<Value>, AqError> {
    let mut rows = Vec::new();
    for (line_number, raw_line) in input.lines().enumerate() {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        let mut fields = Vec::new();
        for field in line.split('\t') {
            fields.push(parse_tsv_field(field, line_number + 1)?);
        }
        rows.push(csv_row_value(fields, tabular_coercion));
    }
    Ok(rows)
}

fn parse_tsv_field(field: &str, line_number: usize) -> Result<String, AqError> {
    let mut out = String::new();
    let mut chars = field.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => {
                return Err(AqError::ParseInput {
                    format: "tsv",
                    message: format!("line {line_number}: trailing escape"),
                });
            }
        }
    }
    Ok(out)
}

fn looks_like_csv(input: &str) -> bool {
    let Ok(rows) = parse_csv_documents(input, TabularCoercion::Strings) else {
        return false;
    };
    has_consistent_tabular_shape(&rows) && input.contains(',')
}

fn looks_like_tsv(input: &str) -> bool {
    let Ok(rows) = parse_tsv_documents(input, TabularCoercion::Strings) else {
        return false;
    };
    has_consistent_tabular_shape(&rows) && input.contains('\t')
}

fn looks_like_yaml(input: &str) -> bool {
    let has_yaml_marker = input.lines().map(str::trim).any(|line| {
        line == "---"
            || line == "..."
            || line.starts_with("- ")
            || line.contains(": ")
            || line.ends_with(':')
    });
    has_yaml_marker && Value::from_yaml_str(input).is_ok()
}

fn has_consistent_tabular_shape(rows: &[Value]) -> bool {
    if rows.len() < 2 {
        return false;
    }

    let Some(width) = rows.first().and_then(row_width) else {
        return false;
    };
    width >= 2 && rows.iter().all(|row| row_width(row) == Some(width))
}

fn row_width(row: &Value) -> Option<usize> {
    match row {
        Value::Array(values) => Some(values.len()),
        _ => None,
    }
}

fn csv_row_value(fields: Vec<String>, tabular_coercion: TabularCoercion) -> Value {
    Value::Array(
        fields
            .into_iter()
            .map(|field| coerce_tabular_field(field, tabular_coercion))
            .collect(),
    )
}

fn coerce_tabular_field(field: String, tabular_coercion: TabularCoercion) -> Value {
    match tabular_coercion {
        TabularCoercion::Strings => Value::String(field),
        TabularCoercion::InferScalars => infer_tabular_scalar(field),
    }
}

fn infer_tabular_scalar(field: String) -> Value {
    if field.is_empty() {
        return Value::String(field);
    }

    let Ok(value) = parse_json_str(&field) else {
        return Value::String(field);
    };

    match value.untagged() {
        Value::Null | Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            value
        }
        _ => Value::String(field),
    }
}

fn render_csv_row(value: &Value) -> Result<String, AqError> {
    let row = expect_tabular_row("csv", value)?;
    let mut out = String::new();
    for (index, value) in row.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&render_csv_field(value)?);
    }
    Ok(out)
}

fn render_csv_field(value: &Value) -> Result<String, AqError> {
    match value {
        Value::Null => Ok(String::new()),
        Value::String(_) | Value::DateTime(_) | Value::Date(_) => {
            let escaped = render_scalar_text(value)?.replace('"', "\"\"");
            Ok(format!("\"{escaped}\""))
        }
        Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            render_scalar_text(value)
        }
        other => Err(AqError::UnsupportedOutputFormat(format!(
            "{} is not valid in a csv row",
            value_kind(other)
        ))),
    }
}

fn render_tsv_row(value: &Value) -> Result<String, AqError> {
    let row = expect_tabular_row("tsv", value)?;
    let mut out = String::new();
    for (index, value) in row.iter().enumerate() {
        if index > 0 {
            out.push('\t');
        }
        out.push_str(&render_tsv_field(value)?);
    }
    Ok(out)
}

fn render_tsv_field(value: &Value) -> Result<String, AqError> {
    match value {
        Value::Null => Ok(String::new()),
        Value::String(_) | Value::DateTime(_) | Value::Date(_) => {
            let mut out = String::new();
            for ch in render_scalar_text(value)?.chars() {
                match ch {
                    '\t' => out.push_str("\\t"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\\' => out.push_str("\\\\"),
                    other => out.push(other),
                }
            }
            Ok(out)
        }
        Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            render_scalar_text(value)
        }
        other => Err(AqError::UnsupportedOutputFormat(format!(
            "{} is not valid in a tsv row",
            value_kind(other)
        ))),
    }
}

fn expect_tabular_row<'a>(format: &str, value: &'a Value) -> Result<&'a [Value], AqError> {
    match value.untagged() {
        Value::Array(values) => Ok(values),
        other => Err(AqError::UnsupportedOutputFormat(format!(
            "{format} output requires each result to be an array, got {}",
            value_kind(other)
        ))),
    }
}

fn render_scalar_text(value: &Value) -> Result<String, AqError> {
    if let Some(value) = value.rendered_string() {
        return Ok(value);
    }
    match value.untagged() {
        Value::Null | Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            serde_json::to_string(&value.to_json()?).map_err(|error| {
                AqError::UnsupportedOutputFormat(format!("failed to render scalar value: {error}"))
            })
        }
        other => Err(AqError::UnsupportedOutputFormat(format!(
            "{} is not valid in a scalar field",
            value_kind(other)
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableRowFamily {
    Object,
    Array,
    Scalar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableAlignment {
    Left,
    Right,
}

fn render_table(values: &[Value], options: RenderOptions) -> Result<String, AqError> {
    let rows = table_rows(values, options.sort_keys);
    if rows.is_empty() {
        return Ok(String::new());
    }

    match detect_table_row_family(&rows)? {
        TableRowFamily::Object => render_object_table(&rows),
        TableRowFamily::Array => render_array_table(&rows),
        TableRowFamily::Scalar => render_scalar_table(&rows),
    }
}

fn table_rows(values: &[Value], sort_keys: bool) -> Vec<Value> {
    let prepared = values
        .iter()
        .map(|value| prepare_output_value(value, sort_keys))
        .collect::<Vec<_>>();

    if prepared.len() == 1 {
        if let Value::Array(rows) = prepared[0].untagged() {
            if should_expand_table_rows(rows) {
                return rows.clone();
            }
        }
    }

    prepared
}

fn should_expand_table_rows(rows: &[Value]) -> bool {
    !rows.is_empty()
        && rows.iter().all(|row| {
            matches!(
                row.untagged(),
                Value::Object(_) | Value::Array(_) | Value::Null
            )
        })
        && rows
            .iter()
            .any(|row| matches!(row.untagged(), Value::Object(_) | Value::Array(_)))
}

fn detect_table_row_family(rows: &[Value]) -> Result<TableRowFamily, AqError> {
    let mut family = None;

    for row in rows {
        let row_family = match row.untagged() {
            Value::Object(_) => Some(TableRowFamily::Object),
            Value::Array(_) => Some(TableRowFamily::Array),
            Value::Null => None,
            _ => Some(TableRowFamily::Scalar),
        };

        let Some(row_family) = row_family else {
            continue;
        };

        match family {
            None => family = Some(row_family),
            Some(existing) if existing == row_family => {}
            Some(existing) => {
                return Err(AqError::UnsupportedOutputFormat(format!(
                    "table output requires rows with a consistent shape, got {} alongside {} rows",
                    value_kind(row),
                    table_row_family_name(existing)
                )))
            }
        }
    }

    Ok(family.unwrap_or(TableRowFamily::Scalar))
}

fn table_row_family_name(family: TableRowFamily) -> &'static str {
    match family {
        TableRowFamily::Object => "object",
        TableRowFamily::Array => "array",
        TableRowFamily::Scalar => "scalar",
    }
}

fn render_object_table(rows: &[Value]) -> Result<String, AqError> {
    let mut headers = indexmap::IndexMap::<String, ()>::new();
    for row in rows {
        if let Value::Object(fields) = row.untagged() {
            for key in fields.keys() {
                headers.entry(key.clone()).or_insert(());
            }
        }
    }

    let headers = headers.into_keys().collect::<Vec<_>>();
    let mut rendered_rows = Vec::with_capacity(rows.len());
    let mut column_kinds = vec![None; headers.len()];
    for row in rows {
        let mut cells = vec![String::new(); headers.len()];
        if let Value::Object(fields) = row.untagged() {
            for (index, header) in headers.iter().enumerate() {
                if let Some(value) = fields.get(header) {
                    cells[index] = render_table_cell(value)?;
                    column_kinds[index] = merge_table_column_kind(column_kinds[index], value);
                }
            }
        }
        rendered_rows.push(cells);
    }

    Ok(render_text_table(
        Some(headers),
        rendered_rows,
        table_alignments(&column_kinds),
    ))
}

fn render_array_table(rows: &[Value]) -> Result<String, AqError> {
    let width = rows
        .iter()
        .filter_map(|row| match row.untagged() {
            Value::Array(values) => Some(values.len()),
            Value::Null => Some(0),
            _ => None,
        })
        .max()
        .unwrap_or(0);

    let headers = (0..width)
        .map(|index| index.to_string())
        .collect::<Vec<_>>();
    let mut rendered_rows = Vec::with_capacity(rows.len());
    let mut column_kinds = vec![None; width];
    for row in rows {
        let mut cells = vec![String::new(); width];
        if let Value::Array(values) = row.untagged() {
            for (index, value) in values.iter().enumerate() {
                cells[index] = render_table_cell(value)?;
                column_kinds[index] = merge_table_column_kind(column_kinds[index], value);
            }
        }
        rendered_rows.push(cells);
    }

    Ok(render_text_table(
        Some(headers),
        rendered_rows,
        table_alignments(&column_kinds),
    ))
}

fn render_scalar_table(rows: &[Value]) -> Result<String, AqError> {
    let mut column_kind = None;
    let rendered_rows = rows
        .iter()
        .map(|row| {
            column_kind = merge_table_column_kind(column_kind, row);
            render_table_cell(row).map(|value| vec![value])
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(render_text_table(
        Some(vec!["value".to_string()]),
        rendered_rows,
        vec![table_alignment_for_kind(column_kind)],
    ))
}

fn render_table_cell(value: &Value) -> Result<String, AqError> {
    let text = render_scalar_text(value)?;
    Ok(escape_table_text(&text))
}

fn escape_table_text(text: &str) -> String {
    let mut escaped = String::new();
    for ch in text.chars() {
        match ch {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            other => escaped.push(other),
        }
    }
    escaped
}

fn render_text_table(
    headers: Option<Vec<String>>,
    rows: Vec<Vec<String>>,
    alignments: Vec<TableAlignment>,
) -> String {
    let column_count = headers
        .as_ref()
        .map(|headers| headers.len())
        .or_else(|| rows.first().map(Vec::len))
        .unwrap_or(0);

    if column_count == 0 {
        return String::new();
    }

    let mut widths = vec![0; column_count];
    if let Some(headers) = &headers {
        for (index, header) in headers.iter().enumerate() {
            widths[index] = display_width(header);
        }
    }
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(display_width(cell));
        }
    }

    let mut lines = Vec::new();
    if let Some(headers) = headers {
        lines.push(render_table_line(&headers, &widths, &alignments));
        lines.push(
            widths
                .iter()
                .map(|width| "-".repeat(*width))
                .collect::<Vec<_>>()
                .join("  "),
        );
    }
    lines.extend(
        rows.iter()
            .map(|row| render_table_line(row, &widths, &alignments)),
    );

    lines.join("\n")
}

fn render_table_line(cells: &[String], widths: &[usize], alignments: &[TableAlignment]) -> String {
    let mut out = String::new();
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            out.push_str("  ");
        }
        let padding = widths[index].saturating_sub(display_width(cell));
        match alignments[index] {
            TableAlignment::Left => {
                out.push_str(cell);
                if index + 1 < cells.len() {
                    write_padding(&mut out, padding);
                }
            }
            TableAlignment::Right => {
                write_padding(&mut out, padding);
                out.push_str(cell);
            }
        }
    }
    out
}

fn write_padding(out: &mut String, count: usize) {
    for _ in 0..count {
        out.push(' ');
    }
}

fn display_width(text: &str) -> usize {
    text.chars().count()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableColumnKind {
    Numeric,
    Other,
}

fn merge_table_column_kind(
    kind: Option<TableColumnKind>,
    value: &Value,
) -> Option<TableColumnKind> {
    match value.untagged() {
        Value::Null => kind,
        Value::Integer(_) | Value::Float(_) => kind.or(Some(TableColumnKind::Numeric)),
        _ => Some(TableColumnKind::Other),
    }
}

fn table_alignments(kinds: &[Option<TableColumnKind>]) -> Vec<TableAlignment> {
    kinds
        .iter()
        .copied()
        .map(table_alignment_for_kind)
        .collect()
}

fn table_alignment_for_kind(kind: Option<TableColumnKind>) -> TableAlignment {
    match kind {
        Some(TableColumnKind::Numeric) => TableAlignment::Right,
        Some(TableColumnKind::Other) | None => TableAlignment::Left,
    }
}

fn value_kind(value: &Value) -> &'static str {
    match value.untagged() {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::Bytes(_) => "bytes",
        Value::DateTime(_) => "datetime",
        Value::Date(_) => "date",
        Value::Tagged { .. } => unreachable!("untagged values should not be tagged"),
    }
}

#[cfg(test)]
mod tests {
    use crate::format::{
        detect_format_for_input, parse_text, parse_text_with_tabular_coercion, render,
        sniff_format, DetectConflictPolicy, Format, RenderOptions, TabularCoercion,
    };
    use crate::value::Value;
    use std::path::Path;

    #[test]
    fn parses_csv_rows_with_quotes_and_embedded_newlines() {
        let values = parse_text(
            "\"name\",\"note\"\n\"alice\",\"line 1\nline 2\"\n",
            Format::Csv,
        )
        .expect("csv should parse");
        assert_eq!(
            values,
            vec![
                Value::Array(vec![
                    Value::String("name".to_string()),
                    Value::String("note".to_string())
                ]),
                Value::Array(vec![
                    Value::String("alice".to_string()),
                    Value::String("line 1\nline 2".to_string())
                ])
            ]
        );
    }

    #[test]
    fn parses_tsv_escapes() {
        let values = parse_text("alice\ta\\tb\tline\\n2\tbackslash\\\\x\n", Format::Tsv)
            .expect("tsv should parse");
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::String("alice".to_string()),
                Value::String("a\tb".to_string()),
                Value::String("line\n2".to_string()),
                Value::String("backslash\\x".to_string())
            ])]
        );
    }

    #[test]
    fn infers_scalar_csv_fields_when_requested() {
        let values = parse_text_with_tabular_coercion(
            include_str!("../tests/fixtures/infer_scalars.csv"),
            Format::Csv,
            TabularCoercion::InferScalars,
        )
        .expect("csv should parse");
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::Integer(1),
                Value::Bool(true),
                Value::Null,
                Value::from_json(serde_json::json!(1.25)).expect("value should parse"),
                Value::String("{}".to_string()),
                Value::String("[1]".to_string()),
                Value::String("x".to_string()),
            ])]
        );
    }

    #[test]
    fn infers_scalar_tsv_fields_when_requested() {
        let values = parse_text_with_tabular_coercion(
            include_str!("../tests/fixtures/infer_scalars.tsv"),
            Format::Tsv,
            TabularCoercion::InferScalars,
        )
        .expect("tsv should parse");
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::Integer(1),
                Value::Bool(true),
                Value::Null,
                Value::from_json(serde_json::json!(1.25)).expect("value should parse"),
                Value::String("{}".to_string()),
                Value::String("[1]".to_string()),
                Value::String("x".to_string()),
            ])]
        );
    }

    #[test]
    fn parses_json_with_utf8_bom() {
        let values =
            parse_text("\u{FEFF}\"byte order mark\"\n", Format::Json).expect("json should parse");
        assert_eq!(values, vec![Value::String("byte order mark".to_string())]);
    }

    #[test]
    fn rejects_unterminated_csv_quotes() {
        let error = parse_text("\"alice", Format::Csv).expect_err("csv should fail");
        assert!(error.to_string().contains("unterminated quoted field"));
    }

    #[test]
    fn preserves_yaml_tags_on_parse_and_render() {
        let values = parse_text("value: !Thing x\n", Format::Yaml).expect("yaml should parse");
        assert_eq!(
            values,
            vec![Value::Object(indexmap::indexmap! {
                "value".to_string() => Value::Tagged {
                    tag: "!Thing".to_string(),
                    value: Box::new(Value::String("x".to_string())),
                }
            })]
        );

        let rendered = crate::format::render(
            &values,
            Format::Yaml,
            crate::format::RenderOptions::default(),
        )
        .expect("yaml should render");
        assert!(rendered.contains("!Thing"));
    }

    #[test]
    fn sniffs_tabular_formats_conservatively() {
        assert_eq!(
            detect_format_for_input(
                None,
                "{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n",
                DetectConflictPolicy::Sniff
            ),
            Format::Jsonl
        );
        assert_eq!(
            detect_format_for_input(None, "a,b\nc,d\n", DetectConflictPolicy::Sniff),
            Format::Csv
        );
        assert_eq!(
            detect_format_for_input(None, "a\tb\nc\td\n", DetectConflictPolicy::Sniff),
            Format::Tsv
        );
        assert_eq!(
            detect_format_for_input(
                None,
                "name: alice\nrole: admin\n",
                DetectConflictPolicy::Sniff
            ),
            Format::Yaml
        );
    }

    #[test]
    fn does_not_sniff_blank_input_as_toml() {
        assert_eq!(sniff_format(" \n\t\r\n"), None);
    }

    #[test]
    fn keeps_single_json_record_jsonl_extensions_without_conflict() {
        let path = Path::new("data.jsonl");
        let input = "{\"name\":\"alice\"}\n";
        assert_eq!(
            detect_format_for_input(Some(path), input, DetectConflictPolicy::WarnFallback),
            Format::Jsonl
        );
        assert_eq!(
            detect_format_for_input(Some(path), input, DetectConflictPolicy::Sniff),
            Format::Jsonl
        );
    }

    #[test]
    fn sniffs_toml_section_headers_as_toml_instead_of_json() {
        let path = Path::new("Cargo.toml");
        let input = "[package]\nname = \"aq\"\nversion = \"0.1.0\"\n";
        assert_eq!(
            detect_format_for_input(Some(path), input, DetectConflictPolicy::WarnFallback),
            Format::Toml
        );
        assert_eq!(
            detect_format_for_input(Some(path), input, DetectConflictPolicy::Sniff),
            Format::Toml
        );
    }

    #[test]
    fn renders_object_table_output() {
        let rendered = render(
            &[
                Value::Object(indexmap::indexmap! {
                    "name".to_string() => Value::String("alice".to_string()),
                    "role".to_string() => Value::String("admin".to_string()),
                }),
                Value::Object(indexmap::indexmap! {
                    "name".to_string() => Value::String("bob".to_string()),
                    "role".to_string() => Value::String("ops".to_string()),
                }),
            ],
            Format::Table,
            RenderOptions::default(),
        )
        .expect("table should render");
        assert_eq!(
            rendered,
            "name   role\n-----  -----\nalice  admin\nbob    ops\n"
        );
    }

    #[test]
    fn renders_single_array_of_objects_as_table_rows() {
        let rendered = render(
            &[Value::Array(vec![
                Value::Object(indexmap::indexmap! {
                    "name".to_string() => Value::String("alice".to_string()),
                    "role".to_string() => Value::String("admin".to_string()),
                }),
                Value::Object(indexmap::indexmap! {
                    "name".to_string() => Value::String("bob".to_string()),
                    "role".to_string() => Value::String("ops".to_string()),
                }),
            ])],
            Format::Table,
            RenderOptions::default(),
        )
        .expect("table should render");
        assert_eq!(
            rendered,
            "name   role\n-----  -----\nalice  admin\nbob    ops\n"
        );
    }

    #[test]
    fn renders_array_table_output_with_escaped_cells() {
        let rendered = render(
            &[
                Value::Array(vec![
                    Value::String("alice".to_string()),
                    Value::String("line 1\nline 2".to_string()),
                ]),
                Value::Array(vec![Value::String("bob".to_string()), Value::Null]),
            ],
            Format::Table,
            RenderOptions::default(),
        )
        .expect("table should render");
        assert_eq!(
            rendered,
            "0      1\n-----  --------------\nalice  line 1\\nline 2\nbob    null\n"
        );
    }

    #[test]
    fn right_aligns_numeric_table_columns() {
        let rendered = render(
            &[
                Value::Object(indexmap::indexmap! {
                    "name".to_string() => Value::String("alice".to_string()),
                    "count".to_string() => Value::Integer(12),
                    "score".to_string() => Value::Float(3.5),
                }),
                Value::Object(indexmap::indexmap! {
                    "name".to_string() => Value::String("bob".to_string()),
                    "count".to_string() => Value::Integer(2),
                    "score".to_string() => Value::Integer(42),
                }),
            ],
            Format::Table,
            RenderOptions::default(),
        )
        .expect("table should render");
        assert_eq!(
            rendered,
            "name   count  score\n-----  -----  -----\nalice     12    3.5\nbob        2     42\n"
        );
    }

    #[test]
    fn rejects_table_raw_output_modes() {
        let error = render(
            &[Value::String("alice".to_string())],
            Format::Table,
            RenderOptions {
                raw_output: true,
                ..RenderOptions::default()
            },
        )
        .expect_err("table raw output should fail");
        assert!(error
            .to_string()
            .contains("table output does not support --raw-output"));
    }
}
