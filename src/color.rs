use crate::format::Format;

const RESET: &str = "\x1b[0m";
const PUNCT: &str = "\x1b[1;39m";
const KEY: &str = "\x1b[1;34m";
const STRING: &str = "\x1b[0;32m";
const SCALAR: &str = "\x1b[0;39m";

pub fn colorize(text: &str, format: Format) -> String {
    match format {
        Format::Json | Format::Jsonl => colorize_json(text),
        Format::Toml => colorize_toml(text),
        Format::Yaml => colorize_yaml(text),
        Format::Csv | Format::Tsv | Format::Table => text.to_string(),
    }
}

fn wrap(style: &str, text: &str) -> String {
    let mut out = String::with_capacity(style.len() + text.len() + RESET.len());
    out.push_str(style);
    out.push_str(text);
    out.push_str(RESET);
    out
}

fn colorize_json(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 32);
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                let end = scan_quoted(bytes, index, b'"');
                let literal = &text[index..end];
                let next = next_non_whitespace(bytes, end);
                let style = if next == Some(b':') { KEY } else { STRING };
                out.push_str(&wrap(style, literal));
                index = end;
            }
            b'{' | b'}' | b'[' | b']' | b':' | b',' => {
                out.push_str(&wrap(PUNCT, &text[index..index + 1]));
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let end = scan_number_like(bytes, index);
                out.push_str(&wrap(SCALAR, &text[index..end]));
                index = end;
            }
            b't' if text[index..].starts_with("true") && is_token_end(bytes, index + 4) => {
                out.push_str(&wrap(SCALAR, "true"));
                index += 4;
            }
            b'f' if text[index..].starts_with("false") && is_token_end(bytes, index + 5) => {
                out.push_str(&wrap(SCALAR, "false"));
                index += 5;
            }
            b'n' if text[index..].starts_with("null") && is_token_end(bytes, index + 4) => {
                out.push_str(&wrap(SCALAR, "null"));
                index += 4;
            }
            _ => {
                out.push(bytes[index] as char);
                index += 1;
            }
        }
    }
    out
}

fn colorize_toml(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 32);
    for line in split_inclusive_lines(text) {
        out.push_str(&colorize_toml_line(line));
    }
    out
}

fn colorize_toml_line(line: &str) -> String {
    let line_without_newline = line.trim_end_matches('\n');
    let newline = &line[line_without_newline.len()..];
    if line_without_newline.trim().is_empty() {
        return line.to_string();
    }

    let indent_end = line_without_newline
        .find(|ch: char| !ch.is_whitespace())
        .unwrap_or(line_without_newline.len());
    let indent = &line_without_newline[..indent_end];
    let trimmed = &line_without_newline[indent_end..];

    let body = if trimmed.starts_with('[') {
        colorize_toml_table_header(trimmed)
    } else if let Some(eq_index) = find_unquoted_char(trimmed, '=') {
        let left = &trimmed[..eq_index];
        let right = &trimmed[eq_index + 1..];
        let left_trimmed = left.trim_end();
        let left_ws = &left[left_trimmed.len()..];
        let right_ws_end = right
            .find(|ch: char| !ch.is_whitespace())
            .unwrap_or(right.len());
        let right_ws = &right[..right_ws_end];
        let right_value = &right[right_ws_end..];

        let mut colored = String::new();
        colored.push_str(&colorize_toml_key_path(left_trimmed));
        colored.push_str(left_ws);
        colored.push_str(&wrap(PUNCT, "="));
        colored.push_str(right_ws);
        colored.push_str(&colorize_plain_scalar_text(right_value));
        colored
    } else {
        colorize_plain_scalar_text(trimmed)
    };

    let mut out = String::new();
    out.push_str(indent);
    out.push_str(&body);
    out.push_str(newline);
    out
}

fn colorize_toml_table_header(text: &str) -> String {
    let mut out = String::new();
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'[' | b']' | b'.' => {
                out.push_str(&wrap(PUNCT, &text[index..index + 1]));
                index += 1;
            }
            b'"' | b'\'' => {
                let quote = bytes[index];
                let end = scan_quoted(bytes, index, quote);
                out.push_str(&wrap(KEY, &text[index..end]));
                index = end;
            }
            _ => {
                let end = scan_until(bytes, index, |ch| ch == b'[' || ch == b']' || ch == b'.');
                out.push_str(&wrap(KEY, &text[index..end]));
                index = end;
            }
        }
    }
    out
}

fn colorize_toml_key_path(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'.' => {
                out.push_str(&wrap(PUNCT, "."));
                index += 1;
            }
            b'"' | b'\'' => {
                let quote = bytes[index];
                let end = scan_quoted(bytes, index, quote);
                out.push_str(&wrap(KEY, &text[index..end]));
                index = end;
            }
            b' ' | b'\t' => {
                out.push(bytes[index] as char);
                index += 1;
            }
            _ => {
                let end = scan_until(bytes, index, |ch| ch == b'.' || ch == b' ' || ch == b'\t');
                out.push_str(&wrap(KEY, &text[index..end]));
                index = end;
            }
        }
    }
    out
}

fn colorize_yaml(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 32);
    for line in split_inclusive_lines(text) {
        out.push_str(&colorize_yaml_line(line));
    }
    out
}

fn colorize_yaml_line(line: &str) -> String {
    let line_without_newline = line.trim_end_matches('\n');
    let newline = &line[line_without_newline.len()..];
    if line_without_newline.trim().is_empty() {
        return line.to_string();
    }

    let indent_end = line_without_newline
        .find(|ch: char| !ch.is_whitespace())
        .unwrap_or(line_without_newline.len());
    let indent = &line_without_newline[..indent_end];
    let trimmed = &line_without_newline[indent_end..];

    let mut out = String::new();
    out.push_str(indent);

    if trimmed == "---" || trimmed == "..." {
        out.push_str(&wrap(PUNCT, trimmed));
        out.push_str(newline);
        return out;
    }

    let mut remainder = trimmed;
    if let Some(rest) = remainder.strip_prefix("- ") {
        out.push_str(&wrap(PUNCT, "-"));
        out.push(' ');
        remainder = rest;
    }

    if let Some(colon_index) = find_yaml_key_colon(remainder) {
        let key = &remainder[..colon_index];
        let after_colon = &remainder[colon_index + 1..];
        out.push_str(&wrap(KEY, key));
        out.push_str(&wrap(PUNCT, ":"));
        out.push_str(&colorize_plain_scalar_text(after_colon));
    } else {
        out.push_str(&colorize_plain_scalar_text(remainder));
    }

    out.push_str(newline);
    out
}

fn colorize_plain_scalar_text(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 16);
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                let end = scan_quoted(bytes, index, quote);
                out.push_str(&wrap(STRING, &text[index..end]));
                index = end;
            }
            b'[' | b']' | b'{' | b'}' | b',' | b':' | b'=' => {
                out.push_str(&wrap(PUNCT, &text[index..index + 1]));
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let end = scan_bare_token(bytes, index);
                let token = &text[index..end];
                if looks_like_scalar_literal(token) {
                    out.push_str(&wrap(SCALAR, token));
                } else {
                    out.push_str(token);
                }
                index = end;
            }
            b't' | b'f' | b'n' | b'~' => {
                let end = scan_bare_token(bytes, index);
                let token = &text[index..end];
                if looks_like_scalar_literal(token) {
                    out.push_str(&wrap(SCALAR, token));
                } else {
                    out.push_str(token);
                }
                index = end;
            }
            _ => {
                out.push(bytes[index] as char);
                index += 1;
            }
        }
    }
    out
}

fn looks_like_scalar_literal(token: &str) -> bool {
    if matches!(
        token,
        "true" | "false" | "True" | "False" | "null" | "Null" | "NULL" | "~"
    ) {
        return true;
    }

    if token.parse::<i64>().is_ok() || token.parse::<f64>().is_ok() {
        return true;
    }

    let mut has_digit = false;
    let mut all_allowed = true;
    for ch in token.chars() {
        has_digit |= ch.is_ascii_digit();
        all_allowed &= matches!(
            ch,
            '0'..='9' | '-' | '+' | ':' | 'T' | 'Z' | 't' | 'z' | '.' | '_'
        );
    }
    has_digit && all_allowed
}

fn scan_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut index = start + 1;
    let mut escaped = false;
    while index < bytes.len() {
        let ch = bytes[index];
        if quote == b'"' && !escaped && ch == b'\\' {
            escaped = true;
            index += 1;
            continue;
        }
        if ch == quote && !escaped {
            return index + 1;
        }
        escaped = false;
        index += 1;
    }
    bytes.len()
}

fn scan_number_like(bytes: &[u8], start: usize) -> usize {
    scan_until(bytes, start, |ch| {
        !matches!(ch, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
    })
}

fn scan_bare_token(bytes: &[u8], start: usize) -> usize {
    scan_until(bytes, start, |ch| {
        ch.is_ascii_whitespace() || matches!(ch, b',' | b':' | b'=' | b'[' | b']' | b'{' | b'}')
    })
}

fn scan_until<F>(bytes: &[u8], start: usize, stop: F) -> usize
where
    F: Fn(u8) -> bool,
{
    let mut index = start;
    while index < bytes.len() && !stop(bytes[index]) {
        index += 1;
    }
    index
}

fn next_non_whitespace(bytes: &[u8], mut index: usize) -> Option<u8> {
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    bytes.get(index).copied()
}

fn is_token_end(bytes: &[u8], index: usize) -> bool {
    bytes
        .get(index)
        .map(|byte| byte.is_ascii_whitespace() || matches!(byte, b',' | b']' | b'}' | b':'))
        .unwrap_or(true)
}

fn split_inclusive_lines(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    text.split_inclusive('\n').collect()
}

fn find_unquoted_char(text: &str, needle: char) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut quote = None;
    let mut escaped = false;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if let Some(active_quote) = quote {
            if active_quote == '"' && !escaped && ch == '\\' {
                escaped = true;
                index += 1;
                continue;
            }
            if ch == active_quote && !escaped {
                quote = None;
            }
            escaped = false;
            index += 1;
            continue;
        }

        if ch == '"' || ch == '\'' {
            quote = Some(ch);
        } else if ch == needle {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn find_yaml_key_colon(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut quote = None;
    let mut escaped = false;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if let Some(active_quote) = quote {
            if active_quote == '"' && !escaped && ch == '\\' {
                escaped = true;
                index += 1;
                continue;
            }
            if ch == active_quote && !escaped {
                quote = None;
            }
            escaped = false;
            index += 1;
            continue;
        }

        match ch {
            '"' | '\'' => quote = Some(ch),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth += 1,
            '}' => brace_depth = brace_depth.saturating_sub(1),
            ':' if bracket_depth == 0 && brace_depth == 0 => {
                let next = bytes.get(index + 1).copied();
                if next.is_none() || next.is_some_and(|byte| byte.is_ascii_whitespace()) {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}
