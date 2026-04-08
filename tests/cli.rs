use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

fn assert_multiline_json_output_close(actual: &str, expected: &str, tolerance: f64) {
    let actual_lines: Vec<&str> = actual.lines().collect();
    let expected_lines: Vec<&str> = expected.lines().collect();
    assert_eq!(
        actual_lines.len(),
        expected_lines.len(),
        "output line count mismatch"
    );

    for (line_number, (actual_line, expected_line)) in
        actual_lines.iter().zip(expected_lines.iter()).enumerate()
    {
        let actual_value: serde_json::Value =
            serde_json::from_str(actual_line).expect("actual output line should be valid json");
        let expected_value: serde_json::Value =
            serde_json::from_str(expected_line).expect("expected output line should be valid json");

        match (&actual_value, &expected_value) {
            (
                serde_json::Value::Number(actual_number),
                serde_json::Value::Number(expected_number),
            ) if !(actual_number.is_i64() || actual_number.is_u64())
                || !(expected_number.is_i64() || expected_number.is_u64()) =>
            {
                let actual_float = actual_number
                    .as_f64()
                    .expect("actual numeric output should fit in f64");
                let expected_float = expected_number
                    .as_f64()
                    .expect("expected numeric output should fit in f64");
                let difference = (actual_float - expected_float).abs();
                assert!(
                    difference <= tolerance,
                    "float mismatch on line {}: actual={} expected={} diff={}",
                    line_number + 1,
                    actual_float,
                    expected_float,
                    difference
                );
            }
            _ => assert_eq!(
                actual_value,
                expected_value,
                "mismatch on line {}",
                line_number + 1
            ),
        }
    }
}

fn run_aq(args: &[&str], stdin: Option<&str>) -> Output {
    run_aq_with_env(args, stdin, &[])
}

fn run_aq_with_env(args: &[&str], stdin: Option<&str>, envs: &[(&str, &str)]) -> Output {
    run_aq_with_env_in_dir(args, stdin, envs, None)
}

fn run_aq_in_dir(args: &[&str], stdin: Option<&str>, dir: &std::path::Path) -> Output {
    run_aq_with_env_in_dir(args, stdin, &[], Some(dir))
}

fn run_aq_with_env_in_dir(
    args: &[&str],
    stdin: Option<&str>,
    envs: &[(&str, &str)],
    dir: Option<&std::path::Path>,
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aq"));
    command.args(args);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    if let Some(dir) = dir {
        command.current_dir(dir);
    }
    command.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    for (key, value) in envs {
        command.env(key, value);
    }

    let mut child = command.spawn().expect("aq should spawn");
    if let Some(stdin) = stdin {
        use std::io::Write;
        child
            .stdin
            .take()
            .expect("stdin should be available")
            .write_all(stdin.as_bytes())
            .expect("stdin write should succeed");
    }
    child.wait_with_output().expect("aq should exit")
}

fn temp_file(name: &str, contents: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("aq-{unique}-{name}"));
    fs::write(&path, contents).expect("temp file should write");
    path
}

fn temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("aq-{unique}-{name}"));
    fs::create_dir_all(&path).expect("temp dir should create");
    path
}

fn write_upstream_module_fixtures(root: &std::path::Path) {
    fs::create_dir_all(root.join("b")).expect("b dir should create");
    fs::create_dir_all(root.join("c")).expect("c dir should create");
    fs::create_dir_all(root.join("lib").join("jq").join("e"))
        .expect("nested lib dir should create");

    fs::write(root.join("a.jq"), "module {version:1.7};\ndef a: \"a\";\n")
        .expect("a module should write");
    fs::write(
        root.join("b").join("b.jq"),
        "def a: \"b\";\ndef b: \"c\";\n",
    )
    .expect("b module should write");
    fs::write(
        root.join("c").join("c.jq"),
        "module {whatever:null};\nimport \"a\" as foo;\nimport \"d\" as d {search:\"./\"};\nimport \"d\" as d2{search:\"./\"};\nimport \"e\" as e {search:\"./../lib/jq\"};\nimport \"f\" as f {search:\"./../lib/jq\"};\nimport \"data\" as $d;\ndef a: 0;\ndef c:\n  if $d::d[0] != {this:\"is a test\",that:\"is too\"} then error(\"data import is busted\")\n  elif d2::meh != d::meh then error(\"import twice doesn't work\")\n  elif foo::a != \"a\" then error(\"foo::a didn't work as expected\")\n  elif d::meh != \"meh\" then error(\"d::meh didn't work as expected\")\n  elif e::bah != \"bah\" then error(\"e::bah didn't work as expected\")\n  elif f::f != \"f is here\" then error(\"f::f didn't work as expected\")\n  else foo::a + \"c\" + d::meh + e::bah end;\n",
    )
    .expect("c module should write");
    fs::write(root.join("c").join("d.jq"), "def meh: \"meh\";\n").expect("d module should write");
    fs::write(
        root.join("lib").join("jq").join("e").join("e.jq"),
        "def bah: \"bah\";\n",
    )
    .expect("e module should write");
    fs::write(
        root.join("lib").join("jq").join("f.jq"),
        "def f: \"f is here\";\n",
    )
    .expect("f module should write");
    fs::write(
        root.join("data.json"),
        "{\"this\":\"is a test\",\"that\":\"is too\"}\n",
    )
    .expect("data module should write");
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn example_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("starlark")
        .join(name)
}

fn temp_fixture(name: &str) -> PathBuf {
    let source = fixture_path(name);
    let contents = fs::read_to_string(&source).expect("fixture should read");
    temp_file(name, &contents)
}

fn fixture_text(name: &str) -> String {
    fs::read_to_string(fixture_path(name)).expect("fixture should read")
}

#[derive(Debug, Deserialize)]
struct JqCompatSuite {
    cases: Vec<JqCompatCase>,
}

#[derive(Debug, Deserialize)]
struct JqCompatCase {
    program: String,
    input: Option<String>,
    null_input: bool,
    expected: String,
}

fn load_jq_compat_suite() -> JqCompatSuite {
    serde_json::from_str(&fixture_text("jq_compat_suite.json"))
        .expect("jq compatibility suite should parse")
}

fn canonicalize_slurped(input_format: &str, input: &str) -> String {
    let output = run_aq(
        &[
            "--input-format",
            input_format,
            "--output-format",
            "json",
            "--slurp",
            ".",
            "--compact",
        ],
        Some(input),
    );
    assert!(
        output.status.success(),
        "canonicalization failed for {input_format}"
    );
    String::from_utf8(output.stdout).expect("stdout should be utf8")
}

fn convert_formats(input_format: &str, output_format: &str, input: &str) -> String {
    let output = run_aq(
        &[
            "--input-format",
            input_format,
            "--output-format",
            output_format,
            ".",
            "--compact",
        ],
        Some(input),
    );
    assert!(
        output.status.success(),
        "conversion {input_format} -> {output_format} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout should be utf8")
}

fn starlark_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn skip_if_starlark_unavailable() -> bool {
    !cfg!(feature = "starlark")
}

#[test]
fn supports_starlark_inline_expressions() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["--starlark", "data[\"user\"][\"name\"]", "-r"],
        Some(r#"{"user":{"name":"alice"}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn supports_starlark_top_level_log_to_stderr() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(&["-n", "--starlark", "log(1 + 2); \"ok\"", "-r"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "ok\n"
    );
    assert_eq!(
        String::from_utf8(output.stderr).expect("stderr should be utf8"),
        "3\n"
    );
}

#[test]
fn supports_starlark_inline_expressions_without_input_with_null_input() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(&["-n", "--starlark", "1 + 2"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n"
    );
}

#[test]
fn supports_starlark_inline_expressions_with_explicit_stdin_sentinel() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["--starlark", "data[\"user\"][\"name\"]", "-r", "-"],
        Some(r#"{"user":{"name":"alice"}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn supports_starlark_file_mode_with_positional_inputs() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let script = temp_file(
        "transform.star",
        "def main(data):\n    return {\"name\": data[\"name\"], \"age\": data[\"age\"] + 1}\n",
    );
    let input = temp_file("input.json", r#"{"name":"alice","age":30}"#);
    let output = run_aq(
        &[
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--compact",
            input.to_str().expect("input path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\",\"age\":31}\n"
    );
}

#[test]
fn supports_starlark_repl_with_piped_commands() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(&["-n", "-P"], Some("x = 1\nx + 2\n:quit\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n"
    );
}

#[test]
fn supports_starlark_repl_with_positional_input_files_and_reset() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let input = temp_file("repl-input.json", r#"{"x":1}"#);
    let output = run_aq(
        &[
            "--starlark-repl",
            "--compact",
            input.to_str().expect("input path should be utf8"),
        ],
        Some("data[\"x\"]\ndata = 7\n:data\n:reset\n:data\n:quit\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n7\n{\"x\":1}\n"
    );
}

#[test]
fn supports_starlark_repl_multiline_definitions() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["-n", "--starlark-repl"],
        Some("def inc(x):\n    return x + 1\ninc(2)\n:quit\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n"
    );
}

#[test]
fn supports_starlark_repl_last_result_and_data_commands() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["-n", "--starlark-repl", "--compact"],
        Some("1 + 2\nans * 5\n:data {\"x\": 7}\ndata[\"x\"]\n:quit\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n15\n{\"x\":7}\n7\n"
    );
}

#[test]
fn supports_starlark_repl_load_globals_and_format_switching() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let script = temp_file(
        "repl-load.star",
        "x = 4\ndef triple(v):\n    return v * 3\n",
    );
    let commands = format!(
        ":load {}\n:globals\ntriple(x)\n:format yaml\n{{\"ok\": True}}\n:quit\n",
        script.to_string_lossy()
    );
    let output = run_aq(&["-n", "--starlark-repl", "--compact"], Some(&commands));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"data\",\"triple\",\"x\"]\n12\nok: true\n"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("output format: yaml"));
}

#[test]
fn supports_starlark_repl_aq_introspection_command() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["-n", "--starlark-repl", "--compact"],
        Some(":aq slug\n:quit\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"aq.slug\"]\n"
    );
}

#[test]
fn supports_starlark_repl_constants_command() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["-n", "--starlark-repl", "--compact"],
        Some(":constants\n:quit\n"),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("stdout should be json");
    let constants = value.as_array().expect("constants should be an array");
    assert!(constants.iter().any(|entry| {
        entry.get("name") == Some(&serde_json::Value::String("data".to_string()))
            && entry.get("kind") == Some(&serde_json::Value::String("binding".to_string()))
    }));
    assert!(constants.iter().any(|entry| {
        entry.get("name") == Some(&serde_json::Value::String("True".to_string()))
            && entry.get("kind") == Some(&serde_json::Value::String("literal".to_string()))
    }));
}

#[test]
fn supports_starlark_repl_doc_type_and_capabilities_commands() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["-n", "--starlark-repl", "--compact"],
        Some("x = 1\n:type x\n:doc aq.slug\n:doc log\n:capabilities\n:quit\n"),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 4);

    let type_value: serde_json::Value =
        serde_json::from_str(lines[0]).expect("type output should be json");
    assert_eq!(type_value, serde_json::Value::String("int".to_string()));

    let doc_value: serde_json::Value =
        serde_json::from_str(lines[1]).expect("doc output should be json");
    assert_eq!(doc_value["name"], "aq.slug");
    assert_eq!(doc_value["kind"], "helper");
    assert_eq!(doc_value["signature"], "aq.slug(text)");
    assert_eq!(
        doc_value["description"],
        "normalize text to a URL-safe slug"
    );

    let log_doc_value: serde_json::Value =
        serde_json::from_str(lines[2]).expect("log doc output should be json");
    assert_eq!(log_doc_value["name"], "log");
    assert_eq!(log_doc_value["kind"], "builtin");
    assert_eq!(log_doc_value["signature"], "log(value)");
    assert_eq!(
        log_doc_value["description"],
        "write one Starlark value to stderr and return None"
    );

    let capabilities_value: serde_json::Value =
        serde_json::from_str(lines[3]).expect("capabilities output should be json");
    assert_eq!(
        capabilities_value,
        serde_json::json!({
            "filesystem": false,
            "environment": false,
            "time": false
        })
    );
}

#[test]
fn supports_starlark_repl_log_to_stderr() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(&["-n", "-P"], Some("log(data)\n:quit\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("None"));
}

#[test]
fn supports_starlark_repl_pwd_command() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let cwd = std::env::current_dir().expect("cwd should exist");
    let output = run_aq(&["-n", "--starlark-repl", "-r"], Some(":pwd\n:quit\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!("{}\n", cwd.to_string_lossy())
    );
}

#[test]
fn rejects_starlark_filesystem_access_by_default() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let input = temp_file("other.json", r#"{"name":"alice"}"#);
    let source = format!(
        "aq.read({})[\"name\"]",
        starlark_string(&input.to_string_lossy())
    );
    let output = run_aq(&["-n", "--starlark", &source], None);
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("aq.read is disabled"));
}

#[test]
fn allows_starlark_filesystem_access_with_flag() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let input = temp_file("other.json", r#"{"name":"alice"}"#);
    let source = format!(
        "aq.read({})[\"name\"]",
        starlark_string(&input.to_string_lossy())
    );
    let output = run_aq(
        &["-n", "--starlark", "--starlark-filesystem", &source, "-r"],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn rejects_starlark_glob_without_filesystem_access() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(&["-n", "--starlark", "aq.glob(\"*.yaml\")"], None);
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("aq.glob is disabled"));
}

#[test]
fn starlark_unsafe_enables_environment_access() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq_with_env(
        &[
            "-n",
            "--starlark",
            "--starlark-unsafe",
            "aq.env(\"AQ_STARLARK_TEST\")",
            "-r",
        ],
        None,
        &[("AQ_STARLARK_TEST", "hello")],
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "hello\n"
    );
}

#[test]
fn allows_starlark_time_access_with_flag() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["-n", "--starlark", "--starlark-time", "aq.timestamp() > 0"],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn aq_flags_env_can_enable_basic_cli_options() {
    let output = run_aq_with_env(
        &["."],
        Some("{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n"),
        &[(
            "AQ_FLAGS",
            "--slurp --input-format jsonl --output-format json --compact",
        )],
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"name\":\"alice\"},{\"name\":\"bob\"}]\n"
    );
}

#[test]
fn aq_flags_env_can_enable_starlark_mode_and_capabilities() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq_with_env(
        &["aq.env(\"AQ_STARLARK_TEST\")", "-r"],
        None,
        &[
            ("AQ_FLAGS", "-n --starlark --starlark-environment"),
            ("AQ_STARLARK_TEST", "hello"),
        ],
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "hello\n"
    );
}

#[test]
fn cli_args_override_aq_flags_value_options() {
    let output = run_aq_with_env(
        &["--output-format", "json", "--compact", "."],
        Some("{\"name\":\"alice\"}"),
        &[("AQ_FLAGS", "--output-format yaml")],
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\"}\n"
    );
}

#[test]
fn malformed_aq_flags_reports_parse_error() {
    let output = run_aq_with_env(&["."], Some("{\"name\":\"alice\"}"), &[("AQ_FLAGS", "\"")]);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("failed to parse AQ_FLAGS"));
    assert!(stderr.contains("unterminated double quote"));
}

#[test]
fn help_mentions_aq_flags_env() {
    let output = run_aq(&["--help"], None);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("Input:"));
    assert!(stdout.contains("Output:"));
    assert!(stdout.contains("Query Variables:"));
    assert!(stdout.contains("Starlark:"));
    assert!(stdout.contains("Examples:"));
    assert!(stdout.contains("jq-style query to run"));
    assert!(stdout.contains("-P, --starlark-repl"));
    assert!(stdout.contains("AQ_FLAGS"));
    assert!(stdout.contains("AQ_DETECT_CONFLICTS"));
}

#[test]
fn supports_arg_and_argjson_flags() {
    let output = run_aq(
        &[
            "-n",
            "--arg",
            "name",
            "alice",
            "--argjson",
            "settings",
            "{\"port\":8080,\"tls\":true}",
            "{$name, $settings}",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\",\"settings\":{\"port\":8080,\"tls\":true}}\n"
    );
}

#[test]
fn supports_args_and_jsonargs_flags() {
    let args_output = run_aq(
        &["-n", "--args", "$ARGS", "alpha", "beta", "--compact"],
        None,
    );
    assert!(args_output.status.success());
    assert_eq!(
        String::from_utf8(args_output.stdout).expect("stdout should be utf8"),
        "{\"named\":{},\"positional\":[\"alpha\",\"beta\"]}\n"
    );

    let jsonargs_output = run_aq(
        &[
            "-n",
            "--arg",
            "name",
            "alice",
            "--jsonargs",
            "$ARGS",
            "1",
            "{\"enabled\":true}",
            "--compact",
        ],
        None,
    );
    assert!(jsonargs_output.status.success());
    assert_eq!(
        String::from_utf8(jsonargs_output.stdout).expect("stdout should be utf8"),
        "{\"named\":{\"name\":\"alice\"},\"positional\":[1,{\"enabled\":true}]}\n"
    );
}

#[test]
fn supports_rawfile_and_slurpfile_flags() {
    let note = temp_file("note.txt", "hello\nworld\n");
    let docs = temp_file("docs.yaml", "---\nname: alice\n---\nname: bob\n");
    let output = run_aq(
        &[
            "-n",
            "--rawfile",
            "note",
            note.to_str().expect("path should be utf8"),
            "--slurpfile",
            "docs",
            docs.to_str().expect("path should be utf8"),
            "{$note, $docs}",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"note\":\"hello\\nworld\\n\",\"docs\":[{\"name\":\"alice\"},{\"name\":\"bob\"}]}\n"
    );
}

#[test]
fn rejects_invalid_argjson_flag() {
    let output = run_aq(&["-n", "--argjson", "settings", "{", "$settings"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--argjson settings failed to parse JSON value"));
}

#[test]
fn supports_exit_status_flag() {
    let truthy = run_aq(&["-n", "-e", "true"], None);
    assert_eq!(truthy.status.code(), Some(0));

    let falsy = run_aq(&["-n", "-e", "false"], None);
    assert_eq!(falsy.status.code(), Some(1));

    let empty = run_aq(&["-n", "-e", "empty"], None);
    assert_eq!(empty.status.code(), Some(4));

    let last_result = run_aq(&["-n", "-e", "1, false"], None);
    assert_eq!(last_result.status.code(), Some(1));
}

#[test]
fn supports_join_output_flag() {
    let output = run_aq(&["-n", "-j", "\"a\", \"b\""], None);
    assert!(output.status.success());
    assert_eq!(output.stdout, b"ab");
}

#[test]
fn supports_raw_output0_flag() {
    let output = run_aq(&["-n", "--raw-output0", "\"a\", 2"], None);
    assert!(output.status.success());
    let mut expected = b"a\0".to_vec();
    expected.extend_from_slice(b"2\0");
    assert_eq!(output.stdout, expected);
}

#[test]
fn rejects_raw_output0_for_strings_containing_nul() {
    let output = run_aq(&["-n", "--raw-output0", "\"a\\u0000b\""], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("Cannot dump a string containing NUL with --raw-output0 option"));
}

#[test]
fn supports_sort_keys_output_flag() {
    let output = run_aq(&["-S", "."], Some(r#"{"b":1,"a":{"d":4,"c":3}}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\n  \"a\": {\n    \"c\": 3,\n    \"d\": 4\n  },\n  \"b\": 1\n}\n"
    );
}

#[test]
fn supports_indent_output_flag() {
    let output = run_aq(&["--indent", "1", "."], Some(r#"{"a":1,"b":2}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\n \"a\": 1,\n \"b\": 2\n}\n"
    );
}

#[test]
fn supports_tab_output_flag() {
    let output = run_aq(&["--tab", "."], Some(r#"{"a":1,"b":2}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\n\t\"a\": 1,\n\t\"b\": 2\n}\n"
    );
}

#[test]
fn supports_color_output_flag_for_json() {
    let output = run_aq(
        &["-C", "."],
        Some(r#"{"name":"alice","active":true,"count":2}"#),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("\u{1b}[1;34m\"name\"\u{1b}[0m"));
    assert!(stdout.contains("\u{1b}[0;32m\"alice\"\u{1b}[0m"));
    assert!(stdout.contains("\u{1b}[0;39mtrue\u{1b}[0m"));
}

#[test]
fn supports_color_output_flag_for_toml() {
    let output = run_aq(
        &["-C", "-f", "toml", "-o", "toml", "."],
        Some("name = \"alice\"\nactive = true\ncount = 2\n"),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("\u{1b}[1;34mname\u{1b}[0m"));
    assert!(stdout.contains("\u{1b}[0;32m\"alice\"\u{1b}[0m"));
    assert!(stdout.contains("\u{1b}[0;39mtrue\u{1b}[0m"));
}

#[test]
fn supports_color_output_flag_for_yaml() {
    let output = run_aq(
        &["-C", "-o", "yaml", "."],
        Some(r#"{"name":"alice","count":2}"#),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("\u{1b}[1;34mname\u{1b}[0m"));
    assert!(stdout.contains("\u{1b}[0;39m2\u{1b}[0m"));
}

#[test]
fn monochrome_output_overrides_forced_color() {
    let output = run_aq(&["-C", "--no-color", "."], Some(r#"{"name":"alice"}"#));
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(!stdout.contains("\u{1b}["));
    assert_eq!(stdout, "{\n  \"name\": \"alice\"\n}\n");
}

#[test]
fn rejects_query_variable_flags_with_starlark() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["-n", "--starlark", "--arg", "name", "alice", "data"],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("are not supported with --starlark"));
}

#[test]
fn supports_starlark_query_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "{\"active\": aq.query_all(\".users[] | select(.active)\", data), \"port\": aq.query_one(\".service.port\", data)}",
            "--compact",
        ],
        Some(
            r#"{"service":{"port":8080},"users":[{"name":"alice","active":true},{"name":"bob","active":false},{"name":"carol","active":true}]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"active\":[{\"name\":\"alice\",\"active\":true},{\"name\":\"carol\",\"active\":true}],\"port\":8080}\n"
    );
}

#[test]
fn supports_starlark_parse_and_render_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "aq.render({\"name\": aq.parse(\"name: alice\\nage: 30\\n\", \"yaml\")[\"name\"]}, \"json\", compact = True)",
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\"}\n\n"
    );
}

#[test]
fn supports_starlark_toml_parse_and_render_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "aq.render({\"app\": {\"name\": aq.parse('name = \"alice\"\\n', \"toml\")[\"name\"]}}, \"toml\")",
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[app]\nname = \"alice\"\n\n"
    );
}

#[test]
fn supports_starlark_typed_date_and_datetime_arithmetic() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "{\"date\": aq.date(\"2026-03-30\") + aq.timedelta(days = 1), \"datetime\": aq.datetime(\"2026-03-30T12:30:00Z\") + aq.timedelta(hours = 2), \"delta_seconds\": (aq.datetime(\"2026-03-30T14:30:00Z\") - aq.datetime(\"2026-03-30T12:30:00Z\")).total_seconds()}",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"date\":\"2026-03-31\",\"datetime\":\"2026-03-30T14:30:00Z\",\"delta_seconds\":7200.0}\n"
    );
}

#[test]
fn supports_starlark_toml_date_arithmetic() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "aq.render({\"day\": aq.parse(\"day = 2026-03-30\\n\", \"toml\")[\"day\"] + aq.timedelta(days = 1)}, \"toml\")",
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "day = 2026-03-31\n\n"
    );
}

#[test]
fn supports_starlark_example_script_shift_release_window() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let script = example_path("shift_release_window.star");
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"release\":{\"day\":\"2026-03-31\",\"start\":\"2026-03-30T12:30:00Z\",\"cutoff\":\"2026-03-30T10:30:00Z\"},\"delay_seconds\":86400.0}\n"
    );
}

#[test]
fn supports_starlark_temporal_methods() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "{\"day\": aq.date(\"2026-03-30\").replace(day = 31), \"weekday\": aq.date(\"2026-03-30\").weekday(), \"ordinal\": aq.date(\"2026-03-30\").ordinal, \"ship_at\": aq.date(\"2026-03-30\").at(hour = 9, minute = 15).replace(day = 31, hour = 17, minute = 0, second = 0), \"ship_day\": aq.datetime(\"2026-03-31T17:00:00Z\").date(), \"epoch\": aq.datetime(\"2026-03-31T17:00:00Z\").timestamp()}",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"day\":\"2026-03-31\",\"weekday\":0,\"ordinal\":89,\"ship_at\":\"2026-03-31T17:00:00Z\",\"ship_day\":\"2026-03-31\",\"epoch\":1774976400.0}\n"
    );
}

#[test]
fn supports_starlark_time_helpers_and_extended_timedelta() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "--starlark-time",
            "{\"same_day\": aq.now().date() == aq.today(), \"shifted\": (aq.datetime(\"2026-03-30T12:30:00Z\") + aq.timedelta(weeks = 1, milliseconds = 250)).isoformat(), \"fractional\": aq.timedelta(milliseconds = 250, microseconds = 500, nanoseconds = 600).total_seconds()}",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout should be valid json");
    assert_eq!(value["same_day"], serde_json::Value::Bool(true));
    assert_eq!(
        value["shifted"],
        serde_json::Value::String("2026-04-06T12:30:00.250Z".to_string())
    );
    assert_eq!(
        value["fractional"],
        serde_json::Value::Number(
            serde_json::Number::from_f64(0.2505006).expect("number should be valid")
        )
    );
}

#[test]
fn supports_starlark_example_script_calendar_rollup() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            example_path("calendar_rollup.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"day\":\"2026-03-31\",\"weekday\":1,\"ordinal\":90,\"ship_at\":\"2026-03-31T17:00:00Z\",\"ship_day\":\"2026-03-31\"}\n"
    );
}

#[test]
fn supports_starlark_example_script_time_snapshot() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            example_path("time_snapshot.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-time",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout should be valid json");
    assert_eq!(value["same_day"], serde_json::Value::Bool(true));
    assert_eq!(
        value["next_week"],
        serde_json::Value::String("2026-04-06".to_string())
    );
    assert_eq!(
        value["grace_seconds"],
        serde_json::Value::Number(
            serde_json::Number::from_f64(0.2505006).expect("number should be valid")
        )
    );
}

#[test]
fn supports_starlark_parse_all_and_render_all_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "aq.render_all(aq.parse_all('{\"name\":\"alice\"}\\n{\"name\":\"bob\"}\\n', \"jsonl\"), \"yaml\")",
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "name: alice\n---\nname: bob\n\n"
    );
}

#[test]
fn supports_starlark_format_helper() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["--starlark", "aq.format()", "-r"],
        Some(r#"{"name":"alice"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "json\n"
    );
}

#[test]
fn supports_starlark_mixed_format_helper_when_slurped() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let json = temp_file("data.json", r#"{"name":"alice"}"#);
    let yaml = temp_file("data.yaml", "name: bob\n");
    let output = run_aq(
        &[
            "--slurp",
            "--starlark",
            "aq.format()",
            "-r",
            json.to_str().expect("json path should be utf8"),
            yaml.to_str().expect("yaml path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "mixed\n"
    );
}

#[test]
fn supports_starlark_read_as_with_explicit_format() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let source = temp_file("data.txt", "name: alice\nage: 30\n");
    let expression = format!(
        "aq.read_as({}, \"yaml\")[\"name\"]",
        starlark_string(&source.to_string_lossy())
    );
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "--starlark-filesystem",
            &expression,
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn supports_starlark_read_glob_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-read-glob");
    let configs = directory.join("configs");
    fs::create_dir_all(&configs).expect("configs dir should create");
    fs::write(
        configs.join("app.yaml"),
        "kind: ConfigMap\nmetadata:\n  name: app-config\n",
    )
    .expect("yaml should write");
    fs::write(
        configs.join("bundle.txt"),
        "---\nkind: Service\nmetadata:\n  name: app-service\n---\nkind: ConfigMap\nmetadata:\n  name: extra-config\n",
    )
    .expect("bundle should write");
    let script = directory.join("read_glob.star");
    fs::write(
        &script,
        "def main(data):\n    return {\"files\": aq.read_glob(\"configs/*.yaml\"), \"docs\": aq.read_glob_all_as(\"configs/*.txt\", \"yaml\")}\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"files\":[{\"path\":\"configs/app.yaml\",\"value\":{\"kind\":\"ConfigMap\",\"metadata\":{\"name\":\"app-config\"}}}],\"docs\":[{\"path\":\"configs/bundle.txt\",\"index\":0,\"value\":{\"kind\":\"Service\",\"metadata\":{\"name\":\"app-service\"}}},{\"path\":\"configs/bundle.txt\",\"index\":1,\"value\":{\"kind\":\"ConfigMap\",\"metadata\":{\"name\":\"extra-config\"}}}]}\n"
    );
}

#[test]
fn supports_starlark_write_batch_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-write-batch");
    let out_dir = directory.join("out");
    let expression = format!(
        "single = aq.write_batch([{{\"path\": {}, \"value\": {{\"name\": \"alice\"}}}}], \"json\", compact = True, parents = True)\nmulti = aq.write_batch_all([{{\"path\": {}, \"values\": [{{\"name\": \"alice\"}}, {{\"name\": \"bob\"}}]}}], \"yaml\", parents = True)\n{{\"single\": single, \"multi\": multi, \"one\": aq.read_as({}, \"json\"), \"two\": aq.read_all_as({}, \"yaml\")}}",
        starlark_string(&out_dir.join("one.json").to_string_lossy()),
        starlark_string(&out_dir.join("two.yaml").to_string_lossy()),
        starlark_string(&out_dir.join("one.json").to_string_lossy()),
        starlark_string(&out_dir.join("two.yaml").to_string_lossy()),
    );
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "--starlark-filesystem",
            &expression,
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "{{\"single\":[{{\"path\":\"{}\",\"bytes\":17}}],\"multi\":[{{\"path\":\"{}\",\"bytes\":26}}],\"one\":{{\"name\":\"alice\"}},\"two\":[{{\"name\":\"alice\"}},{{\"name\":\"bob\"}}]}}\n",
            out_dir.join("one.json").to_string_lossy(),
            out_dir.join("two.yaml").to_string_lossy(),
        )
    );
}

#[test]
fn supports_starlark_text_glob_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-text-glob");
    let notes = directory.join("notes");
    fs::create_dir_all(notes.join("nested")).expect("dirs should create");
    fs::write(notes.join("alpha.txt"), "alpha\n").expect("alpha should write");
    fs::write(notes.join("nested").join("beta.txt"), "beta\n").expect("beta should write");
    let script = directory.join("text_glob.star");
    fs::write(
        &script,
        "def main(data):\n    entries = aq.read_text_glob(\"notes/**/*.txt\")\n    writes = aq.write_text_batch([\n        {\"path\": \"out/\" + entry[\"path\"], \"text\": \"# Source: \" + entry[\"path\"] + \"\\n\\n\" + entry[\"text\"]}\n        for entry in entries\n    ], parents = True)\n    return {\"entries\": entries, \"writes\": writes, \"alpha\": aq.read_text(\"out/notes/alpha.txt\"), \"beta\": aq.read_text(\"out/notes/nested/beta.txt\")}\n",
    )
    .expect("script should write");
    let output = run_aq(
        &[
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        Some("{}"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "{{\"entries\":[{{\"path\":\"notes/alpha.txt\",\"text\":\"alpha\\n\"}},{{\"path\":\"notes/nested/beta.txt\",\"text\":\"beta\\n\"}}],\"writes\":[{{\"path\":\"out/notes/alpha.txt\",\"bytes\":33}},{{\"path\":\"out/notes/nested/beta.txt\",\"bytes\":38}}],\"alpha\":\"# Source: notes/alpha.txt\\n\\nalpha\\n\",\"beta\":\"# Source: notes/nested/beta.txt\\n\\nbeta\\n\"}}\n"
        )
    );
}

#[test]
fn supports_starlark_rewrite_text_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-rewrite-text");
    let notes = directory.join("notes");
    fs::create_dir_all(notes.join("nested")).expect("dirs should create");
    fs::write(notes.join("alpha.txt"), "alpha\n").expect("alpha should write");
    fs::write(notes.join("nested").join("beta.txt"), "beta\n").expect("beta should write");
    let script = directory.join("rewrite.star");
    fs::write(
        &script,
        "def annotate(path, text):\n    return \"# Source: \" + path + \"\\n\\n\" + text.upper()\n\ndef main(data):\n    return {\"single\": aq.rewrite_text(\"notes/alpha.txt\", annotate), \"batch\": aq.rewrite_text_glob(\"notes/nested/**/*.txt\", annotate), \"alpha\": aq.read_text(\"notes/alpha.txt\"), \"beta\": aq.read_text(\"notes/nested/beta.txt\")}\n",
    )
    .expect("script should write");
    let output = run_aq(
        &[
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        Some("{}"),
    );
    assert!(output.status.success());
    let parsed: serde_json::Value =
        serde_json::from_str(&String::from_utf8(output.stdout).expect("stdout should be utf8"))
            .expect("stdout should parse");
    assert_eq!(parsed["alpha"], "# Source: notes/alpha.txt\n\nALPHA\n");
    assert_eq!(parsed["beta"], "# Source: notes/nested/beta.txt\n\nBETA\n");
    assert!(parsed["single"].as_i64().expect("single should be integer") > 0);
    assert_eq!(
        parsed["batch"],
        serde_json::json!([{
            "path": "notes/nested/beta.txt",
            "bytes": 38
        }])
    );
}

#[test]
fn rejects_starlark_load_without_filesystem_flag() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-load-disabled");
    let library = directory.join("lib.star");
    let script = directory.join("main.star");
    fs::write(
        &library,
        "def pick_name(data):\n    return data[\"name\"]\n",
    )
    .expect("library should write");
    fs::write(
        &script,
        "load(\"lib.star\", \"pick_name\")\n\ndef main(data):\n    return pick_name(data)\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
        ],
        Some(r#"{"name":"alice"}"#),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("starlark load() is disabled"));

    fs::remove_file(library).expect("library should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_load_from_relative_library() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-load-enabled");
    let library = directory.join("lib.star");
    let script = directory.join("main.star");
    fs::write(
        &library,
        "def decorate(data):\n    return {\"name\": data[\"name\"], \"port\": data[\"service\"][\"port\"]}\n",
    )
    .expect("library should write");
    fs::write(
        &script,
        "load(\"lib.star\", \"decorate\")\n\ndef main(data):\n    return decorate(data)\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--compact",
        ],
        Some(r#"{"name":"alice","service":{"port":8080}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\",\"port\":8080}\n"
    );

    fs::remove_file(library).expect("library should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_relative_read_from_script_directory() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-relative-read");
    let data = directory.join("extra.yaml");
    let script = directory.join("main.star");
    fs::write(&data, "name: alice\nport: 8080\n").expect("data should write");
    fs::write(
        &script,
        "def main(data):\n    return aq.read(\"extra.yaml\")[\"name\"]\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );

    fs::remove_file(data).expect("data should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_read_all_as_with_explicit_format() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-read-all");
    let data = directory.join("docs.txt");
    let script = directory.join("main.star");
    fs::write(&data, "---\nname: alice\n---\nname: bob\n").expect("data should write");
    fs::write(
        &script,
        "def main(data):\n    return aq.read_all_as(\"docs.txt\", \"yaml\")\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"name\":\"alice\"},{\"name\":\"bob\"}]\n"
    );

    fs::remove_file(data).expect("data should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_filesystem_path_and_text_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-fs-helpers");
    let data = directory.join("note.txt");
    let nested = directory.join("nested");
    let script = directory.join("main.star");
    fs::write(&data, "hello\n").expect("data should write");
    fs::create_dir(&nested).expect("nested should create");
    fs::write(
        &script,
        "def main(data):\n    return {\"base\": aq.base_dir(), \"text\": aq.read_text(\"note.txt\"), \"entries\": aq.list_dir(), \"file\": aq.is_file(\"note.txt\"), \"dir\": aq.is_dir(\"nested\"), \"exists\": aq.exists(\"nested\")}\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "{{\"base\":\"{}\",\"text\":\"hello\\n\",\"entries\":[\"main.star\",\"nested\",\"note.txt\"],\"file\":true,\"dir\":true,\"exists\":true}}\n",
            directory.to_string_lossy()
        )
    );

    fs::remove_file(data).expect("data should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(nested).expect("nested should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_glob_and_path_resolution_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-glob-paths");
    let configs = directory.join("configs");
    let nested = directory.join("nested");
    let deeper = nested.join("deeper");
    let script = directory.join("main.star");
    fs::create_dir(&configs).expect("configs should create");
    fs::create_dir(&nested).expect("nested should create");
    fs::create_dir(&deeper).expect("deeper should create");
    fs::write(configs.join("app.yaml"), "name: api\n").expect("yaml should write");
    fs::write(configs.join("app.json"), "{\"name\":\"api\"}\n").expect("json should write");
    fs::write(deeper.join("service.yaml"), "kind: Service\n").expect("service should write");
    fs::write(nested.join("x1.txt"), "xray").expect("text should write");
    fs::write(
        &script,
        "def main(data):\n    return {\"yaml\": aq.glob(\"**/*.yaml\"), \"txt\": aq.glob(\"nested/?1.txt\"), \"absolute\": aq.resolve_path(\"nested/../configs/app.yaml\"), \"relative\": aq.relative_path(\"configs/app.yaml\", start = \"nested/deeper\")}\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "{{\"yaml\":[\"configs/app.yaml\",\"nested/deeper/service.yaml\"],\"txt\":[\"nested/x1.txt\"],\"absolute\":\"{}\",\"relative\":\"../../configs/app.yaml\"}}\n",
            configs.join("app.yaml").to_string_lossy()
        )
    );

    fs::remove_file(configs.join("app.yaml")).expect("yaml should clean up");
    fs::remove_file(configs.join("app.json")).expect("json should clean up");
    fs::remove_file(deeper.join("service.yaml")).expect("service should clean up");
    fs::remove_file(nested.join("x1.txt")).expect("text should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(deeper).expect("deeper should clean up");
    fs::remove_dir(nested).expect("nested should clean up");
    fs::remove_dir(configs).expect("configs should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_atomic_write_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-write-helpers");
    let script = directory.join("main.star");
    fs::write(
        &script,
        "def main(data):\n    written = aq.write_text(\"note.txt\", \"hello\\n\")\n    payload = aq.write_all(\"docs.yaml\", [{\"name\": \"alice\"}, {\"name\": \"bob\"}], \"yaml\")\n    return {\"written\": written, \"payload\": payload, \"note\": aq.read_text(\"note.txt\"), \"docs\": aq.read_all_as(\"docs.yaml\", \"yaml\")}\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"written\":6,\"payload\":26,\"note\":\"hello\\n\",\"docs\":[{\"name\":\"alice\"},{\"name\":\"bob\"}]}\n"
    );
    assert_eq!(
        fs::read_to_string(directory.join("note.txt")).expect("note should read"),
        "hello\n"
    );
    assert_eq!(
        fs::read_to_string(directory.join("docs.yaml")).expect("docs should read"),
        "name: alice\n---\nname: bob\n"
    );

    fs::remove_file(directory.join("note.txt")).expect("note should clean up");
    fs::remove_file(directory.join("docs.yaml")).expect("docs should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_walk_files_and_mkdir_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-walk-mkdir");
    let nested = directory.join("nested");
    let script = directory.join("main.star");
    fs::create_dir(&nested).expect("nested should create");
    fs::write(nested.join("a.txt"), "alpha").expect("file should write");
    fs::write(directory.join("b.txt"), "bravo").expect("file should write");
    fs::write(
        &script,
        "def main(data):\n    created = aq.mkdir(\"out/deeper\", parents = True)\n    return {\"created\": created, \"files\": aq.walk_files(include_dirs = True), \"nested\": aq.walk_files(path = \"nested\")}\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "{{\"created\":\"{}\",\"files\":[\"b.txt\",\"main.star\",\"nested\",\"nested/a.txt\",\"out\",\"out/deeper\"],\"nested\":[\"a.txt\"]}}\n",
            directory.join("out/deeper").to_string_lossy()
        )
    );

    fs::remove_file(nested.join("a.txt")).expect("file should clean up");
    fs::remove_file(directory.join("b.txt")).expect("file should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory.join("out").join("deeper")).expect("dir should clean up");
    fs::remove_dir(directory.join("out")).expect("dir should clean up");
    fs::remove_dir(nested).expect("nested should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_stat_copy_rename_and_remove_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-mutate-helpers");
    let script = directory.join("main.star");
    fs::write(directory.join("source.txt"), "alpha").expect("file should write");
    fs::write(
        &script,
        "def main(data):\n    copied = aq.copy(\"source.txt\", \"copy.txt\")\n    renamed = aq.rename(\"copy.txt\", \"final.txt\")\n    removed = aq.remove(\"source.txt\")\n    return {\"copied\": copied, \"renamed\": renamed, \"removed\": removed, \"stat\": aq.stat(\"final.txt\"), \"missing\": aq.remove(\"missing.txt\", missing_ok = True)}\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(parsed["copied"], 5);
    assert_eq!(parsed["removed"], true);
    assert_eq!(parsed["missing"], false);
    assert_eq!(parsed["stat"]["type"], "file");
    assert_eq!(parsed["stat"]["size"], 5);
    assert_eq!(
        parsed["renamed"],
        serde_json::Value::String(directory.join("final.txt").to_string_lossy().into_owned())
    );
    assert_eq!(
        fs::read_to_string(directory.join("final.txt")).expect("file should read"),
        "alpha"
    );

    fs::remove_file(directory.join("final.txt")).expect("file should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_regex_base64_and_hash_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "{\"matched\": aq.regex_is_match(\"user-[0-9]+\", data[\"id\"]), \"name\": aq.regex_capture(\"(?P<name>[^@]+)@(?P<domain>.+)\", data[\"email\"])[\"named\"][\"name\"], \"masked\": aq.regex_replace(\"^[^@]+@\", \"***@\", data[\"email\"]), \"token\": aq.base64_encode(data[\"id\"]), \"decoded\": aq.base64_decode(aq.base64_encode(data[\"id\"])), \"fingerprint\": aq.hash(data[\"email\"], algorithm = \"blake3\")}",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(r#"{"id":"user-42","email":"alice@example.com"}"#),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(parsed["matched"], true);
    assert_eq!(parsed["name"], "alice");
    assert_eq!(parsed["masked"], "***@example.com");
    assert_eq!(parsed["token"], "dXNlci00Mg==");
    assert_eq!(parsed["decoded"], "user-42");
    assert_eq!(parsed["fingerprint"].as_str().map(str::len), Some(64));
}

#[test]
fn supports_starlark_regex_capture_all_and_split_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "{\"captures\": aq.regex_capture_all(\"(?P<word>[a-z]+)-(?P<id>[0-9]+)\", data[\"text\"]), \"split\": aq.regex_split(\"[,;]\", data[\"csv\"])}",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(r#"{"text":"user-42 admin-7","csv":"alpha,beta;gamma"}"#),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(parsed["captures"][0]["named"]["word"], "user");
    assert_eq!(parsed["captures"][1]["named"]["id"], "7");
    assert_eq!(
        parsed["split"],
        serde_json::json!(["alpha", "beta", "gamma"])
    );
}

#[test]
fn supports_starlark_hash_file_helper() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-hash-file");
    let script = directory.join("main.star");
    fs::write(directory.join("payload.bin"), [0_u8, 255_u8, 16_u8]).expect("payload should write");
    fs::write(
        &script,
        "def main(data):\n    return {\"sha256\": aq.hash_file(\"payload.bin\"), \"sha1\": aq.hash_file(\"payload.bin\", algorithm = \"sha1\")}\n",
    )
    .expect("script should write");

    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            script.to_str().expect("script path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"sha256\":\"2da45f2cd1f9c8e69a67abf7a6b26c282533d0a7686787a9533265418680d4d2\",\"sha1\":\"a14c2fba17201c1ead45b6c4af4409fbfc16ba8a\"}\n"
    );

    fs::remove_file(directory.join("payload.bin")).expect("payload should clean up");
    fs::remove_file(script).expect("script should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_string_normalization_and_safe_text_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "{\"slug\": aq.slug(data[\"name\"]), \"snake\": aq.snake_case(data[\"name\"]), \"kebab\": aq.kebab_case(data[\"title\"]), \"camel\": aq.camel_case(data[\"title\"]), \"title\": aq.title_case(data[\"name\"]), \"prefix\": aq.trim_prefix(data[\"tag\"], \"refs/tags/\"), \"suffix\": aq.trim_suffix(data[\"file\"], \".tar.gz\"), \"regex\": aq.regex_escape(data[\"pattern\"]), \"shell\": aq.shell_escape(data[\"shell\"]), \"encoded\": aq.url_encode_component(data[\"urlish\"]), \"decoded\": aq.url_decode_component(data[\"encoded\"]), \"sha1\": aq.sha1(data[\"text\"]), \"sha256\": aq.sha256(data[\"text\"], encoding = \"base64\"), \"sha512\": aq.sha512(data[\"text\"]), \"blake3\": aq.blake3(data[\"text\"])}",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"name":"HTTPServer v2","title":"user profile_id","tag":"refs/tags/v1.2.3","file":"artifact.tar.gz","pattern":"a+b?(c)","shell":"hello 'quoted' world","urlish":"a b/c?d=e&f","encoded":"a%20b%2Fc%3Fd%3De%26f","text":"hello"}"#,
        ),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(parsed["slug"], "http-server-v-2");
    assert_eq!(parsed["snake"], "http_server_v_2");
    assert_eq!(parsed["kebab"], "user-profile-id");
    assert_eq!(parsed["camel"], "userProfileId");
    assert_eq!(parsed["title"], "Http Server V 2");
    assert_eq!(parsed["prefix"], "v1.2.3");
    assert_eq!(parsed["suffix"], "artifact");
    assert_eq!(parsed["regex"], "a\\+b\\?\\(c\\)");
    assert_eq!(parsed["shell"], "'hello '\\''quoted'\\'' world'");
    assert_eq!(parsed["encoded"], "a%20b%2Fc%3Fd%3De%26f");
    assert_eq!(parsed["decoded"], "a b/c?d=e&f");
    assert_eq!(parsed["sha1"], "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
    assert_eq!(
        parsed["sha256"],
        "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ="
    );
    assert_eq!(
        parsed["sha512"],
        "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
    );
    assert_eq!(
        parsed["blake3"],
        "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f"
    );
}

#[test]
fn supports_starlark_semver_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark",
            "{\"parsed\": aq.semver_parse(\"1.2.3-rc.4+git.7\"), \"cmp_release\": aq.semver_compare(\"1.2.3-rc.1\", \"1.2.3\"), \"cmp_build\": aq.semver_compare(\"1.2.3\", \"1.2.3+build.9\"), \"minor\": aq.semver_bump(\"1.2.3\", \"minor\"), \"pre\": aq.semver_bump(\"1.2.3\", \"prerelease\"), \"pre_next\": aq.semver_bump(\"1.2.3-rc.4+git.7\", \"prerelease\"), \"release\": aq.semver_bump(\"1.2.3-rc.4+git.7\", \"release\")}",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(
        parsed["parsed"],
        serde_json::json!({
            "major": 1,
            "minor": 2,
            "patch": 3,
            "prerelease": ["rc", 4],
            "build": ["git", "7"],
            "is_prerelease": true,
            "version": "1.2.3-rc.4+git.7",
        })
    );
    assert_eq!(parsed["cmp_release"], -1);
    assert_eq!(parsed["cmp_build"], 0);
    assert_eq!(parsed["minor"], "1.3.0");
    assert_eq!(parsed["pre"], "1.2.3-rc.1");
    assert_eq!(parsed["pre_next"], "1.2.3-rc.5");
    assert_eq!(parsed["release"], "1.2.3");
}

#[test]
fn supports_starlark_merge_drop_nulls_and_sort_keys_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "aq.sort_keys(aq.drop_nulls(aq.merge_all([data[\"base\"], data[\"overlay\"]], deep = True), recursive = True), recursive = True)",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"base":{"service":{"port":8080,"name":"api"},"flags":[1,null,2],"meta":{"owner":null}},"overlay":{"service":{"port":8443},"meta":{"team":"platform"},"extra":null}}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"flags\":[1,2],\"meta\":{\"team\":\"platform\"},\"service\":{\"name\":\"api\",\"port\":8443}}\n"
    );
}

#[test]
fn supports_starlark_get_set_and_delete_path_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "{\"port\": aq.get_path(data, [\"service\", \"port\"]), \"missing\": aq.get_path(data, [\"service\", \"host\"]), \"created\": aq.set_path(None, [\"meta\", \"labels\", \"env\"], \"prod\"), \"rewritten\": aq.delete_paths(aq.set_path(data, [\"items\", -1], 9), [[\"meta\", \"uid\"], [\"items\", 0]])}",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(r#"{"service":{"port":8080},"items":[1,2],"meta":{"uid":"x","name":"api"}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"port\":8080,\"missing\":null,\"created\":{\"meta\":{\"labels\":{\"env\":\"prod\"}}},\"rewritten\":{\"service\":{\"port\":8080},\"items\":[9],\"meta\":{\"name\":\"api\"}}}\n"
    );
}

#[test]
fn supports_starlark_clean_k8s_metadata_helper() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "aq.clean_k8s_metadata(data)",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(concat!(
            "apiVersion: v1\n",
            "kind: List\n",
            "metadata:\n",
            "  resourceVersion: \"12\"\n",
            "  annotations:\n",
            "    team: platform\n",
            "items:\n",
            "  - kind: ConfigMap\n",
            "    metadata:\n",
            "      name: app-config\n",
            "      namespace: staging\n",
            "      uid: abc123\n",
            "      ownerReferences:\n",
            "        - apiVersion: apps/v1\n",
            "          kind: Deployment\n",
            "          name: app\n",
            "      resourceVersion: \"7\"\n",
            "      creationTimestamp: \"2024-01-01T00:00:00Z\"\n",
            "      managedFields:\n",
            "        - manager: kubectl\n",
            "      annotations:\n",
            "        note: keep-me\n",
            "      labels:\n",
            "        tier: backend\n"
        )),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"apiVersion\":\"v1\",\"kind\":\"List\",\"metadata\":{\"annotations\":{\"team\":\"platform\"}},\"items\":[{\"kind\":\"ConfigMap\",\"metadata\":{\"annotations\":{\"note\":\"keep-me\"},\"labels\":{\"tier\":\"backend\"},\"name\":\"app-config\",\"namespace\":\"staging\"}}]}\n"
    );
}

#[test]
fn supports_starlark_walk_and_walk_paths_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "def normalize(value):\n    if type(value) == \"string\":\n        return value.strip()\n    return value\n\ndef patch(path, value):\n    if path == [\"metadata\", \"labels\", \"tier\"]:\n        return value.upper()\n    return value\n\n{\"trimmed\": aq.walk(data[\"raw\"], normalize), \"patched\": aq.walk_paths(data[\"manifest\"], patch)}",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"raw":{"name":"  api  ","items":[" one ",2]},"manifest":{"metadata":{"labels":{"tier":"backend","name":"api"}}}}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"trimmed\":{\"name\":\"api\",\"items\":[\"one\",2]},\"patched\":{\"metadata\":{\"labels\":{\"tier\":\"BACKEND\",\"name\":\"api\"}}}}\n"
    );
}

#[test]
fn supports_starlark_paths_find_paths_and_collect_paths_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "def is_secret(path, value):\n    leaf = path[len(path) - 1]\n    return type(leaf) == \"string\" and leaf in [\"password\", \"token\"]\n\ndef describe(path, value):\n    return {\"path\": path, \"value\": value}\n\n{\"all\": aq.paths(data, leaves_only = True), \"matches\": aq.find_paths(data, is_secret, leaves_only = True), \"described\": aq.collect_paths(data, describe, leaves_only = True)}",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(r#"{"auth":{"password":"secret"},"nested":[{"token":"abc"},{"name":"api"}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"all\":[[\"auth\",\"password\"],[\"nested\",0,\"token\"],[\"nested\",1,\"name\"]],\"matches\":[[\"auth\",\"password\"],[\"nested\",0,\"token\"]],\"described\":[{\"path\":[\"auth\",\"password\"],\"value\":\"secret\"},{\"path\":[\"nested\",0,\"token\"],\"value\":\"abc\"},{\"path\":[\"nested\",1,\"name\"],\"value\":\"api\"}]}\n"
    );
}

#[test]
fn supports_starlark_pick_and_omit_helpers() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark",
            "def is_secret(path, value):\n    leaf = path[len(path) - 1]\n    return type(leaf) == \"string\" and leaf in [\"password\", \"token\"]\n\n{\"picked\": aq.pick_paths(data, [[\"service\", \"port\"], [\"metadata\", \"labels\", \"missing\"], [\"items\", 0, \"name\"]]), \"omitted\": aq.omit_paths(data, [[\"metadata\", \"annotations\"], [\"auth\", \"token\"]]), \"picked_where\": aq.pick_where(data, is_secret, leaves_only = True), \"omitted_where\": aq.omit_where(data, is_secret, leaves_only = True)}",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"service":{"port":8080},"metadata":{"labels":{"tier":"backend"},"annotations":{"note":"remove"}},"auth":{"token":"abc"},"items":[{"name":"api","password":"secret"},{"name":"worker"}]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"picked\":{\"service\":{\"port\":8080},\"metadata\":{\"labels\":{\"missing\":null}},\"items\":[{\"name\":\"api\"}]},\"omitted\":{\"service\":{\"port\":8080},\"metadata\":{\"labels\":{\"tier\":\"backend\"}},\"auth\":{},\"items\":[{\"name\":\"api\",\"password\":\"secret\"},{\"name\":\"worker\"}]},\"picked_where\":{\"auth\":{\"token\":\"abc\"},\"items\":[{\"password\":\"secret\"}]},\"omitted_where\":{\"service\":{\"port\":8080},\"metadata\":{\"labels\":{\"tier\":\"backend\"},\"annotations\":{\"note\":\"remove\"}},\"auth\":{},\"items\":[{\"name\":\"api\"},{\"name\":\"worker\"}]}}\n"
    );
}

#[test]
fn starlark_query_one_reports_wrong_result_cardinality() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &["--starlark", "aq.query_one(\".users[]\", data)"],
        Some(r#"{"users":[{"name":"alice"},{"name":"bob"}]}"#),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("aq.query_one expected exactly one result"));
}

#[test]
fn supports_starlark_example_script_users_over_30() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("users_over_30.star")
                .to_str()
                .expect("example path should be utf8"),
            "--compact",
        ],
        Some(
            r#"{"users":[{"name":"bob","age":28},{"name":"alice","age":34},{"name":"carol","age":52}]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"name\":\"alice\",\"age\":34},{\"name\":\"carol\",\"age\":52}]\n"
    );
}

#[test]
fn supports_starlark_example_script_manifest_summary() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--slurp",
            "--starlark-file",
            example_path("manifest_summary.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        Some(concat!(
            "---\nkind: ConfigMap\nmetadata:\n  name: app-config\n",
            "---\nkind: Service\nmetadata:\n  name: app-service\n",
            "---\nkind: ConfigMap\nmetadata:\n  name: extra-config\n"
        )),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"format\":\"yaml\",\"config_maps\":[\"app-config\",\"extra-config\"]}\n"
    );
}

#[test]
fn supports_starlark_example_script_embedded_config() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("embedded_config.star")
                .to_str()
                .expect("example path should be utf8"),
            "--compact",
        ],
        Some(r#"{"config":"name: api\nport: 8080\n"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"config\":\"name: api\\nport: 8443\\n\"}\n"
    );
}

#[test]
fn supports_starlark_example_script_config_map_names() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--slurp",
            "--starlark-file",
            example_path("config_map_names.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(concat!(
            "---\nkind: ConfigMap\nmetadata:\n  name: app-config\n",
            "---\nkind: Service\nmetadata:\n  name: app-service\n",
            "---\nkind: ConfigMap\nmetadata:\n  name: extra-config\n"
        )),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"app-config\",\"extra-config\"]\n"
    );
}

#[test]
fn supports_starlark_example_script_self_inventory() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            example_path("self_inventory.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"files\":[\"k8s.star\"],\"has_lib\":true}\n"
    );
}

#[test]
fn supports_starlark_example_script_library_index() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            example_path("library_index.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "{{\"files\":[\"lib/k8s.star\"],\"resolved\":[\"{}\"],\"relative_from_lib\":\"k8s.star\"}}\n",
            example_path("lib/k8s.star").to_string_lossy()
        )
    );
}

#[test]
fn supports_starlark_example_script_fingerprint_library() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            example_path("fingerprint_library.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(
        parsed,
        serde_json::json!([{
            "path": "lib/k8s.star",
            "sha256": "21064945392618f78afb76e2a6dcd03f6531d6e1899035e10cdcbab45b87f196"
        }])
    );
}

#[test]
fn supports_starlark_example_script_merge_overlay() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("merge_overlay.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"base":{"service":{"port":8080,"name":"api"},"flags":[1,null,2],"meta":{"owner":null}},"overlay":{"service":{"port":8443},"meta":{"team":"platform"},"extra":null}}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"flags\":[1,2],\"meta\":{\"team\":\"platform\"},\"service\":{\"name\":\"api\",\"port\":8443}}\n"
    );
}

#[test]
fn supports_starlark_example_script_generate_app_toml() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            example_path("generate_app_toml.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "toml",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[app]\nname = \"aq\"\nport = 8443\nfeatures = [\n    \"query\",\n    \"starlark\",\n]\n\n[database]\nhost = \"db.internal\"\npool = 16\n"
    );
}

#[test]
fn supports_starlark_example_script_clean_k8s_metadata() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("clean_k8s_metadata.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        Some(concat!(
            "apiVersion: v1\n",
            "kind: ConfigMap\n",
            "metadata:\n",
            "  name: app-config\n",
            "  namespace: staging\n",
            "  uid: abc123\n",
            "  ownerReferences:\n",
            "    - apiVersion: apps/v1\n",
            "      kind: Deployment\n",
            "      name: app\n",
            "  resourceVersion: \"7\"\n",
            "  creationTimestamp: \"2024-01-01T00:00:00Z\"\n",
            "  managedFields:\n",
            "    - manager: kubectl\n",
            "  annotations:\n",
            "    note: keep-me\n",
            "  labels:\n",
            "    tier: backend\n"
        )),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"metadata\":{\"annotations\":{\"note\":\"keep-me\"},\"labels\":{\"tier\":\"backend\",\"managed-by\":\"aq\"},\"name\":\"app-config\",\"namespace\":\"staging\"}}\n"
    );
}

#[test]
fn supports_starlark_example_script_normalize_strings() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("normalize_strings.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        Some(r#"{"metadata":{"labels":{"tier":"backend ","name":" api "}},"items":[" one ",2]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"metadata\":{\"labels\":{\"tier\":\"BACKEND\",\"name\":\"api\"}},\"items\":[\"one\",2]}\n"
    );
}

#[test]
fn supports_starlark_example_script_find_secretish_fields() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("find_secretish_fields.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"auth":{"password":"secret"},"nested":[{"token":"abc"},{"name":"api"}],"secret":"top"}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"paths\":[[\"auth\",\"password\"],[\"nested\",0,\"token\"],[\"secret\"]],\"matches\":[{\"path\":[\"auth\",\"password\"],\"value\":\"secret\"},{\"path\":[\"nested\",0,\"token\"],\"value\":\"abc\"},{\"path\":[\"secret\"],\"value\":\"top\"}]}\n"
    );
}

#[test]
fn supports_starlark_example_script_redact_secretish_fields() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("redact_secretish_fields.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"auth":{"password":"secret"},"nested":[{"token":"abc"},{"name":"api"}],"secret":"top","metadata":{"name":"service"}}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"auth\":{},\"nested\":[{},{\"name\":\"api\"}],\"metadata\":{\"name\":\"service\"}}\n"
    );
}

#[test]
fn supports_starlark_example_script_manifest_tree_summary() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "-n",
            "--starlark-file",
            example_path("manifest_tree_summary.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"path\":\"data/manifests/app.yaml\",\"index\":0,\"kind\":\"ConfigMap\",\"name\":\"app-config\"},{\"path\":\"data/manifests/nested/bundle.yaml\",\"index\":0,\"kind\":\"Service\",\"name\":\"app-service\"},{\"path\":\"data/manifests/nested/bundle.yaml\",\"index\":1,\"kind\":\"Deployment\",\"name\":\"worker\"}]\n"
    );
}

#[test]
fn supports_starlark_example_script_emit_manifest_summaries() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-emit-manifest-summaries");
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("emit_manifest_summaries.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(&format!(
            "{{\"out_dir\":{}}}",
            starlark_string(&directory.to_string_lossy())
        )),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "[{{\"path\":\"{}/app-config.json\",\"bytes\":84}},{{\"path\":\"{}/app-service.json\",\"bytes\":93}},{{\"path\":\"{}/worker.json\",\"bytes\":91}}]\n",
            directory.to_string_lossy(),
            directory.to_string_lossy(),
            directory.to_string_lossy(),
        )
    );
    assert_eq!(
        fs::read_to_string(directory.join("app-config.json")).expect("app-config should read"),
        "{\"path\":\"data/manifests/app.yaml\",\"index\":0,\"kind\":\"ConfigMap\",\"name\":\"app-config\"}\n"
    );
    assert_eq!(
        fs::read_to_string(directory.join("app-service.json")).expect("app-service should read"),
        "{\"path\":\"data/manifests/nested/bundle.yaml\",\"index\":0,\"kind\":\"Service\",\"name\":\"app-service\"}\n"
    );
    assert_eq!(
        fs::read_to_string(directory.join("worker.json")).expect("worker should read"),
        "{\"path\":\"data/manifests/nested/bundle.yaml\",\"index\":1,\"kind\":\"Deployment\",\"name\":\"worker\"}\n"
    );
}

#[test]
fn supports_starlark_example_script_emit_note_copies() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-emit-note-copies");
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("emit_note_copies.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(&format!(
            "{{\"out_dir\":{}}}",
            starlark_string(&directory.to_string_lossy())
        )),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        format!(
            "[{{\"path\":\"{}/data/notes/alpha.txt\",\"bytes\":38}},{{\"path\":\"{}/data/notes/nested/beta.txt\",\"bytes\":43}}]\n",
            directory.to_string_lossy(),
            directory.to_string_lossy(),
        )
    );
    assert_eq!(
        fs::read_to_string(directory.join("data").join("notes").join("alpha.txt"))
            .expect("alpha copy should read"),
        "# Source: data/notes/alpha.txt\n\nalpha\n"
    );
    assert_eq!(
        fs::read_to_string(
            directory
                .join("data")
                .join("notes")
                .join("nested")
                .join("beta.txt")
        )
        .expect("beta copy should read"),
        "# Source: data/notes/nested/beta.txt\n\nbeta\n"
    );
}

#[test]
fn supports_starlark_example_script_normalize_note_files() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-normalize-note-files");
    let alpha = directory.join("alpha.txt");
    let beta = directory.join("beta.txt");
    fs::write(&alpha, "alpha\n").expect("alpha should write");
    fs::write(&beta, "beta\n").expect("beta should write");
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("normalize_note_files.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(&format!(
            "{{\"paths\":[{},{}]}}",
            starlark_string(&alpha.to_string_lossy()),
            starlark_string(&beta.to_string_lossy()),
        )),
    );
    assert!(output.status.success());
    let parsed: serde_json::Value =
        serde_json::from_str(&String::from_utf8(output.stdout).expect("stdout should be utf8"))
            .expect("stdout should parse");
    assert_eq!(parsed.as_array().expect("array").len(), 2);
    assert_eq!(
        fs::read_to_string(&alpha).expect("alpha should read"),
        "# Source: alpha.txt\n\nALPHA\n"
    );
    assert_eq!(
        fs::read_to_string(&beta).expect("beta should read"),
        "# Source: beta.txt\n\nBETA\n"
    );
}

#[test]
fn supports_starlark_example_script_stage_report() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let directory = temp_dir("starlark-stage-report");
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("stage_report.star")
                .to_str()
                .expect("example path should be utf8"),
            "--starlark-filesystem",
            "--output-format",
            "json",
            "--compact",
        ],
        Some(&format!(
            "{{\"name\":\"alice\",\"out_dir\":{}}}",
            starlark_string(&directory.to_string_lossy())
        )),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(parsed["report"]["type"], "file");
    assert_eq!(
        fs::read_to_string(directory.join("summary.json")).expect("report should read"),
        "{\"name\":\"alice\"}\n"
    );

    fs::remove_file(directory.join("summary.json")).expect("report should clean up");
    fs::remove_dir(directory).expect("temp dir should clean up");
}

#[test]
fn supports_starlark_example_script_sanitize_contacts() {
    if skip_if_starlark_unavailable() {
        return;
    }
    let output = run_aq(
        &[
            "--starlark-file",
            example_path("sanitize_contacts.star")
                .to_str()
                .expect("example path should be utf8"),
            "--output-format",
            "json",
            "--compact",
        ],
        Some(
            r#"{"users":[{"name":"alice","email":"alice@example.com"},{"name":"bob","email":"bob@example.com"}]}"#,
        ),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should parse");
    assert_eq!(parsed[0]["name"], "alice");
    assert_eq!(parsed[0]["email"], "***@example.com");
    assert_eq!(parsed[0]["token"], "YWxpY2VAZXhhbXBsZS5jb20=");
    assert_eq!(parsed[1]["name"], "bob");
    assert_eq!(parsed[1]["email"], "***@example.com");
}

#[test]
fn generates_bash_completions() {
    let output = run_aq(&["--generate-completions", "bash"], None);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("_aq()"));
    assert!(stdout.contains("--generate-completions"));
    assert!(stdout.contains("--in-place"));
    assert!(stdout.contains("--starlark"));
}

#[test]
fn supports_editing_json_from_fixture_with_assignment_syntax() {
    let input = fixture_path("edit.json");
    let output = run_aq(
        &[
            ".service.port = 8443 | .features[] |= ascii_upcase | .metadata.labels.env = \"staging\"",
            "--compact",
            input.to_str().expect("fixture path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"service\":{\"name\":\"api\",\"port\":8443},\"features\":[\"ALPHA\",\"BETA\"],\"metadata\":{\"labels\":{\"team\":\"platform\",\"env\":\"staging\"}}}\n"
    );
}

#[test]
fn supports_compound_editing_json_from_fixture() {
    let input = fixture_path("edit.json");
    let output = run_aq(
        &[
            ".service.port += 235 | .metadata.labels.env //= \"staging\" | .features += [\"gamma\"]",
            "--compact",
            input.to_str().expect("fixture path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"service\":{\"name\":\"api\",\"port\":8315},\"features\":[\"alpha\",\"beta\",\"gamma\"],\"metadata\":{\"labels\":{\"team\":\"platform\",\"env\":\"staging\"}}}\n"
    );
}

#[test]
fn supports_filtering_and_editing_multidoc_yaml_from_fixture() {
    let input = fixture_path("multidoc.yaml");
    let output = run_aq(
        &[
            "select(.kind == \"ConfigMap\") | .metadata.labels.env = \"staging\"",
            input.to_str().expect("fixture path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "kind: ConfigMap\nmetadata:\n  name: keep-one\n  labels:\n    env: staging\n---\nkind: ConfigMap\nmetadata:\n  name: keep-two\n  labels:\n    env: staging\n"
    );
}

#[test]
fn supports_compound_editing_multidoc_yaml_from_fixture() {
    let input = fixture_path("multidoc.yaml");
    let output = run_aq(
        &[
            "select(.kind == \"ConfigMap\") | .metadata.labels.env //= \"staging\" | .metadata.name += \"-edited\"",
            input.to_str().expect("fixture path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "kind: ConfigMap\nmetadata:\n  name: keep-one-edited\n  labels:\n    env: staging\n---\nkind: ConfigMap\nmetadata:\n  name: keep-two-edited\n  labels:\n    env: staging\n"
    );
}

#[test]
fn supports_in_place_editing_json_files() {
    let path = temp_fixture("edit.json");
    let output = run_aq(
        &[
            "--in-place",
            ".service.port += 235 | .metadata.labels.env //= \"staging\" | .features += [\"gamma\"]",
            "--compact",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
    assert_eq!(
        fs::read_to_string(&path).expect("file should read"),
        "{\"service\":{\"name\":\"api\",\"port\":8315},\"features\":[\"alpha\",\"beta\",\"gamma\"],\"metadata\":{\"labels\":{\"team\":\"platform\",\"env\":\"staging\"}}}\n"
    );
}

#[test]
fn supports_in_place_editing_toml_files() {
    let path = temp_fixture("edit.toml");
    let output = run_aq(
        &[
            "--in-place",
            ".service.port += 235 | .metadata.labels.env //= \"staging\" | .features += [\"gamma\"]",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
    let actual = canonicalize_slurped(
        "toml",
        &fs::read_to_string(&path).expect("file should read"),
    );
    let expected =
        canonicalize_slurped("json", "{\"service\":{\"name\":\"api\",\"port\":8315},\"features\":[\"alpha\",\"beta\",\"gamma\"],\"metadata\":{\"labels\":{\"team\":\"platform\",\"env\":\"staging\"}}}");
    let actual: serde_json::Value =
        serde_json::from_str(actual.trim()).expect("actual should parse as json");
    let expected: serde_json::Value =
        serde_json::from_str(expected.trim()).expect("expected should parse as json");
    assert_eq!(actual, expected);
}

#[test]
fn supports_in_place_filtering_and_editing_multidoc_yaml_files() {
    let path = temp_fixture("multidoc.yaml");
    let output = run_aq(
        &[
            "--in-place",
            "select(.kind == \"ConfigMap\") | .metadata.labels.env //= \"staging\" | .metadata.name += \"-edited\"",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
    assert_eq!(
        fs::read_to_string(&path).expect("file should read"),
        "kind: ConfigMap\nmetadata:\n  name: keep-one-edited\n  labels:\n    env: staging\n---\nkind: ConfigMap\nmetadata:\n  name: keep-two-edited\n  labels:\n    env: staging\n"
    );
}

#[test]
fn preserves_original_file_when_in_place_query_fails() {
    let path = temp_fixture("edit.json");
    let original = fs::read_to_string(&path).expect("file should read");
    let output = run_aq(
        &[
            "--in-place",
            ".service[0]",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("cannot index object with [0]"));
    assert_eq!(
        fs::read_to_string(&path).expect("file should read"),
        original
    );
}

#[test]
fn rejects_in_place_without_file_arguments() {
    let output = run_aq(&["--in-place", ".a = 1"], Some(r#"{"a":0}"#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--in-place requires one or more file arguments"));
}

#[test]
fn rejects_in_place_with_raw_output() {
    let path = temp_fixture("edit.json");
    let output = run_aq(
        &[
            "--in-place",
            "--raw-output",
            ".service.name",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--in-place does not support --raw-output"));
}

#[test]
fn rejects_in_place_json_rewrites_with_zero_results() {
    let path = temp_fixture("edit.json");
    let original = fs::read_to_string(&path).expect("file should read");
    let output = run_aq(
        &[
            "--in-place",
            "select(false)",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--in-place requires exactly one result when writing json"));
    assert_eq!(
        fs::read_to_string(&path).expect("file should read"),
        original
    );
}

#[test]
fn rejects_in_place_toml_rewrites_with_zero_results() {
    let path = temp_fixture("edit.toml");
    let original = fs::read_to_string(&path).expect("file should read");
    let output = run_aq(
        &[
            "--in-place",
            "select(false)",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--in-place requires exactly one result when writing toml"));
    assert_eq!(
        fs::read_to_string(&path).expect("file should read"),
        original
    );
}

#[test]
fn rejects_in_place_table_output() {
    let path = temp_fixture("edit.json");
    let original = fs::read_to_string(&path).expect("file should read");
    let output = run_aq(
        &[
            "--in-place",
            "--output-format",
            "table",
            ".",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--in-place does not support table output"));
    assert_eq!(
        fs::read_to_string(&path).expect("file should read"),
        original
    );
}

#[test]
fn supports_upstream_jq_case_subset() {
    for case in load_jq_compat_suite().cases {
        let mut args = Vec::new();
        if case.null_input {
            args.push("-n");
        } else {
            args.push("--input-format");
            args.push("json");
        }
        args.push("--compact");
        args.push(case.program.as_str());
        let output = run_aq(&args, case.input.as_deref());
        assert!(
            output.status.success(),
            "upstream jq case failed for program {}: {}",
            case.program,
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).expect("stdout should be utf8"),
            case.expected,
            "upstream jq case mismatch for program {}",
            case.program
        );
    }
}

#[test]
fn supports_upstream_jq_last_datetime_roundtrip_case() {
    let output = run_aq(
        &[
            "-n",
            "--compact",
            "last(range(365 * 67)|(\"1970-03-01T01:02:03Z\"|strptime(\"%Y-%m-%dT%H:%M:%SZ\")|mktime) + (86400 * .)|strftime(\"%Y-%m-%dT%H:%M:%SZ\")|strptime(\"%Y-%m-%dT%H:%M:%SZ\"))",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[2037,1,11,1,2,3,3,41]\n"
    );
}

#[test]
fn supports_upstream_jq_deep_json_roundtrip_case() {
    let output = run_aq(
        &[
            "-n",
            "--compact",
            "reduce range(9999) as $_ ([];[.]) | tojson | fromjson | flatten",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[]\n"
    );
}

#[test]
fn supports_upstream_jq_deep_json_parse_limit_case() {
    let output = run_aq(
        &[
            "-n",
            "--compact",
            "reduce range(10000) as $_ ([];[.]) | tojson | try (fromjson) catch . | (contains(\"<skipped: too deep>\") | not) and contains(\"Exceeds depth limit for parsing\")",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_upstream_jq_deep_json_print_limit_case() {
    let output = run_aq(
        &[
            "-n",
            "--compact",
            "reduce range(10001) as $_ ([];[.]) | tojson | contains(\"<skipped: too deep>\")",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_upstream_jq_stream_builtin_cases() {
    let truncate_output = run_aq(
        &[
            "truncate_stream([[0],\"a\"],[[1,0],\"b\"],[[1,0]],[[1]])",
            "--compact",
            "--output-format",
            "json",
        ],
        Some("1"),
    );
    assert!(truncate_output.status.success());
    assert_eq!(
        String::from_utf8(truncate_output.stdout).expect("stdout should be utf8"),
        "[[0],\"b\"]\n[[0]]\n"
    );

    let fromstream_output = run_aq(
        &[
            "-n",
            "fromstream(1|truncate_stream([[0],\"a\"],[[1,0],\"b\"],[[1,0]],[[1]]))",
            "--compact",
            "--output-format",
            "json",
        ],
        None,
    );
    assert!(fromstream_output.status.success());
    assert_eq!(
        String::from_utf8(fromstream_output.stdout).expect("stdout should be utf8"),
        "[\"b\"]\n"
    );

    let tostream_output = run_aq(
        &[
            ". as $dot|fromstream($dot|tostream)|.==$dot",
            "--compact",
            "--output-format",
            "json",
        ],
        Some("[0,[1,{\"a\":1},{\"b\":2}]]"),
    );
    assert!(tostream_output.status.success());
    assert_eq!(
        String::from_utf8(tostream_output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_uppercase_sql_style_builtins() {
    let output = run_aq(
        &["-n", "--compact", "INDEX(range(5)|[., \"foo\\(.)\"]; .[0])"],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"0\":[0,\"foo0\"],\"1\":[1,\"foo1\"],\"2\":[2,\"foo2\"],\"3\":[3,\"foo3\"],\"4\":[4,\"foo4\"]}\n"
    );

    let output = run_aq(
        &[
            "--input-format",
            "json",
            "--compact",
            "JOIN({\"0\":[0,\"abc\"],\"1\":[1,\"bcd\"],\"2\":[2,\"def\"],\"3\":[3,\"efg\"],\"4\":[4,\"fgh\"]}; .[0]|tostring)",
        ],
        Some("[[5,\"foo\"],[3,\"bar\"],[1,\"foobar\"]]\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[[[5,\"foo\"],null],[[3,\"bar\"],[3,\"efg\"]],[[1,\"foobar\"],[1,\"bcd\"]]]\n"
    );

    let output = run_aq(&["-n", "--compact", "IN(range(5;20); range(10))"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn removes_k8s_server_metadata_from_yaml() {
    let input = fixture_path("remove_k8s_metadata.yaml");
    let output = run_aq(
        &[
            "del(.metadata | (.annotations,.creationTimestamp,.uid,.resourceVersion))",
            input.to_str().expect("fixture path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "apiVersion: v1\ndata:\n  config.json: '{}'\nkind: ConfigMap\nmetadata:\n  name: test\n  namespace: default\n"
    );
}

#[test]
fn removes_k8s_server_metadata_in_place() {
    let path = temp_fixture("remove_k8s_metadata.yaml");
    let output = run_aq(
        &[
            "--in-place",
            "del(.metadata | (.annotations,.creationTimestamp,.uid,.resourceVersion))",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
    assert_eq!(
        fs::read_to_string(&path).expect("file should read"),
        "apiVersion: v1\ndata:\n  config.json: '{}'\nkind: ConfigMap\nmetadata:\n  name: test\n  namespace: default\n"
    );
}

#[test]
fn supports_single_document_format_round_trip_matrix() {
    let cases = [
        ("json", "roundtrip_single.json"),
        ("jsonl", "roundtrip_single.jsonl"),
        ("toml", "roundtrip_single.toml"),
        ("yaml", "roundtrip_single.yaml"),
    ];
    let outputs = ["json", "jsonl", "toml", "yaml"];

    for (input_format, fixture) in cases {
        let input = fixture_text(fixture);
        let expected = canonicalize_slurped(input_format, &input);
        for output_format in outputs {
            let converted = convert_formats(input_format, output_format, &input);
            let actual = canonicalize_slurped(output_format, &converted);
            assert_eq!(
                actual, expected,
                "single-doc round trip mismatch for {input_format} -> {output_format}"
            );
        }
    }
}

#[test]
fn supports_multi_document_format_round_trip_matrix_for_yaml_and_jsonl() {
    let cases = [
        ("jsonl", "roundtrip_stream.jsonl"),
        ("yaml", "roundtrip_stream.yaml"),
    ];
    let outputs = ["jsonl", "yaml"];

    for (input_format, fixture) in cases {
        let input = fixture_text(fixture);
        let expected = canonicalize_slurped(input_format, &input);
        for output_format in outputs {
            let converted = convert_formats(input_format, output_format, &input);
            let actual = canonicalize_slurped(output_format, &converted);
            assert_eq!(
                actual, expected,
                "multi-doc round trip mismatch for {input_format} -> {output_format}"
            );
        }
    }
}

#[test]
fn supports_single_row_tabular_round_trip_matrix_for_csv_and_tsv() {
    let cases = [
        ("csv", "roundtrip_single.csv"),
        ("tsv", "roundtrip_single.tsv"),
    ];
    let outputs = ["json", "jsonl", "yaml", "csv", "tsv"];

    for (input_format, fixture) in cases {
        let input = fixture_text(fixture);
        let expected = canonicalize_slurped(input_format, &input);
        for output_format in outputs {
            let converted = convert_formats(input_format, output_format, &input);
            let actual = canonicalize_slurped(output_format, &converted);
            assert_eq!(
                actual, expected,
                "single-row round trip mismatch for {input_format} -> {output_format}"
            );
        }
    }
}

#[test]
fn supports_multi_row_tabular_round_trip_matrix_for_csv_and_tsv() {
    let cases = [
        ("csv", "roundtrip_stream.csv"),
        ("tsv", "roundtrip_stream.tsv"),
    ];
    let outputs = ["jsonl", "yaml", "csv", "tsv"];

    for (input_format, fixture) in cases {
        let input = fixture_text(fixture);
        let expected = canonicalize_slurped(input_format, &input);
        for output_format in outputs {
            let converted = convert_formats(input_format, output_format, &input);
            let actual = canonicalize_slurped(output_format, &converted);
            assert_eq!(
                actual, expected,
                "multi-row round trip mismatch for {input_format} -> {output_format}"
            );
        }
    }
}

#[test]
fn supports_table_output_for_single_array_of_objects() {
    let output = run_aq(
        &["--output-format", "table", ".rows"],
        Some(r#"{"rows":[{"name":"alice","role":"admin"},{"name":"bob","role":"ops"}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "name   role\n-----  -----\nalice  admin\nbob    ops\n"
    );
}

#[test]
fn supports_table_output_with_right_aligned_numeric_columns() {
    let output = run_aq(
        &["--output-format", "table", ".rows"],
        Some(
            r#"{"rows":[{"name":"alice","count":12,"score":3.5},{"name":"bob","count":2,"score":42}]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "name   count  score\n-----  -----  -----\nalice     12  3.5\nbob        2  42\n"
    );
}

#[test]
fn rejects_table_input_format() {
    let output = run_aq(&["--input-format", "table", "."], Some("name,alice\n"));
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("table is an output-only format"));
}

#[test]
fn rejects_table_raw_output_modes() {
    let output = run_aq(
        &["--output-format", "table", "--raw-output", ".name"],
        Some(r#"{"name":"alice"}"#),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("table output does not support --raw-output"));
}

#[test]
fn reads_identity_from_stdin() {
    let output = run_aq(&["."], Some(r#"{"name":"alice"}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\n  \"name\": \"alice\"\n}\n"
    );
}

#[test]
fn reads_identity_from_explicit_stdin_sentinel() {
    let output = run_aq(&[".", "-"], Some(r#"{"name":"alice"}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\n  \"name\": \"alice\"\n}\n"
    );
}

#[test]
fn reads_nested_field() {
    let output = run_aq(&[".user.name"], Some(r#"{"user":{"name":"alice"}}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n"
    );
}

#[test]
fn supports_pipe_queries() {
    let output = run_aq(
        &[".users[] | .name", "--compact"],
        Some(r#"{"users":[{"name":"alice"},{"name":"bob"}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn supports_object_shorthand_and_variable_shorthand() {
    let output = run_aq(
        &[".name as $name | {title, $name}", "--compact"],
        Some(r#"{"title":"hello","name":"alice"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"title\":\"hello\",\"name\":\"alice\"}\n"
    );
}

#[test]
fn supports_comma_queries() {
    let output = run_aq(
        &[".name, .age", "--compact"],
        Some(r#"{"name":"alice","age":30}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n30\n"
    );
}

#[test]
fn supports_array_constructors() {
    let output = run_aq(
        &["[.name, .age]", "--compact"],
        Some(r#"{"name":"alice","age":30}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"alice\",30]\n"
    );
}

#[test]
fn supports_object_constructors() {
    let output = run_aq(
        &["{name: .name, age: .age}", "--compact"],
        Some(r#"{"name":"alice","age":30}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\",\"age\":30}\n"
    );
}

#[test]
fn supports_pipe_to_object_constructor() {
    let output = run_aq(
        &[".users[] | {name: .name}", "--compact"],
        Some(r#"{"users":[{"name":"alice"},{"name":"bob"}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n"
    );
}

#[test]
fn supports_length_builtin() {
    let output = run_aq(&[".items | length"], Some(r#"{"items":[1,2,3]}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n"
    );
}

#[test]
fn supports_jq_abs_and_numeric_length_compat() {
    let output = run_aq(&["abs", "-r"], Some("\"abc\""));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "abc\n"
    );

    let output = run_aq(
        &["map(abs == length) | unique", "--compact"],
        Some("[-10, -1.1, -1e-1, 1000000000000000002]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[true]\n"
    );
}

#[test]
fn supports_keys_builtin() {
    let output = run_aq(
        &[".obj | keys", "--compact"],
        Some(r#"{"obj":{"a":1,"b":2}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"a\",\"b\"]\n"
    );
}

#[test]
fn supports_type_builtin() {
    let output = run_aq(&[".age | type"], Some(r#"{"age":30}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"number\"\n"
    );
}

#[test]
fn supports_select_builtin() {
    let output = run_aq(
        &[".users[] | select(.active == true) | .name", "--compact"],
        Some(r#"{"users":[{"name":"alice","active":true},{"name":"bob","active":false}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n"
    );
}

#[test]
fn supports_comparisons() {
    let output = run_aq(&[".age >= 21"], Some(r#"{"age":30}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_add_operator() {
    let output = run_aq(&[".left + .right"], Some(r#"{"left":2,"right":3}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "5\n"
    );
}

#[test]
fn supports_arithmetic_operators_and_precedence() {
    let output = run_aq(&["-n", "2 + 3 * 4 - 5, 7 / 2, 7 % 3"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "9\n3.5\n1\n"
    );
}

#[test]
fn supports_multi_output_binary_operators_in_jq_order() {
    let output = run_aq(&["[.[] / .[]]", "--compact"], Some(r#"[1,2]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2,0.5,1]\n"
    );
}

#[test]
fn supports_array_difference_and_string_division() {
    let output = run_aq(
        &[".items - .drop, .csv / \",\"", "--compact"],
        Some(r#"{"items":[1,2,3,2],"drop":[2],"csv":"a,b,c"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,3]\n[\"a\",\"b\",\"c\"]\n"
    );
}

#[test]
fn supports_alt_operator() {
    let output = run_aq(
        &[".nickname // .name"],
        Some(r#"{"nickname":null,"name":"alice"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n"
    );
}

#[test]
fn supports_boolean_operators() {
    let output = run_aq(
        &[".active and not .deleted"],
        Some(r#"{"active":true,"deleted":false}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_jq_boolean_short_circuiting() {
    let output = run_aq(
        &["type == \"object\" and has(\"b\")"],
        Some(r#"[1,{"b":3}]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "false\n"
    );

    let output = run_aq(
        &["type != \"object\" or has(\"b\")"],
        Some(r#"[1,{"b":3}]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_map_builtin() {
    let output = run_aq(
        &[".users | map(.name)", "--compact"],
        Some(r#"{"users":[{"name":"alice"},{"name":"bob"}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"alice\",\"bob\"]\n"
    );
}

#[test]
fn supports_add_builtin() {
    let output = run_aq(&[".numbers | add"], Some(r#"{"numbers":[1,2,3]}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "6\n"
    );
}

#[test]
fn supports_add_builtin_with_query_arguments() {
    let output = run_aq(
        &[
            "-n",
            "[add(null), add(range(range(10))), add(empty), add(10,range(10))]",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[null,120,null,55]\n"
    );

    let output = run_aq(&[".sum = add(.arr[])", "--compact"], Some(r#"{"arr":[]}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"arr\":[],\"sum\":null}\n"
    );

    let output = run_aq(
        &["add({(.[]):1}) | keys", "--compact"],
        Some(r#"["a","a","b","a","d","b","d","a","d"]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"a\",\"b\",\"d\"]\n"
    );
}

#[test]
fn supports_first_and_last_builtins() {
    let output = run_aq(
        &["(.numbers | first), (.numbers | last)"],
        Some(r#"{"numbers":[1,2,3]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n3\n"
    );
}

#[test]
fn supports_has_builtin() {
    let output = run_aq(&["has(\"name\")", "--compact"], Some(r#"{"name":"alice"}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );

    let array_output = run_aq(
        &["has(nan), has(1.0), has(1.5)", "--compact"],
        Some("[0,1,2]"),
    );
    assert!(array_output.status.success());
    assert_eq!(
        String::from_utf8(array_output.stdout).expect("stdout should be utf8"),
        "false\ntrue\nfalse\n"
    );
}

#[test]
fn supports_contains_builtin() {
    let output = run_aq(
        &["contains({user: {name: \"alice\"}})"],
        Some(r#"{"user":{"name":"alice","active":true},"tags":["ops","prod"]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_inside_in_and_isempty_builtins() {
    let output = run_aq(
        &[
            "-n",
            "({\"name\":\"alice\"} | inside({\"name\":\"alice\",\"active\":true})), (\"name\" | in({\"name\":1})), (0 | in([\"present\"])), isempty([\"present\"] | .[] | select(. == \"missing\"))",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\ntrue\ntrue\ntrue\n"
    );
}

#[test]
fn supports_variable_bindings() {
    let output = run_aq(
        &[".bar as $x | .foo | . + $x"],
        Some(r#"{"foo":10,"bar":200}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "210\n"
    );
}

#[test]
fn supports_variable_binding_across_iteration() {
    let output = run_aq(
        &[".items | length as $n | .[] | . * $n"],
        Some(r#"{"items":[1,2,3]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n6\n9\n"
    );
}

#[test]
fn supports_nested_variable_shadowing() {
    let output = run_aq(&[". as $x | (1 as $x | $x) + $x"], Some("5"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "6\n"
    );
}

#[test]
fn supports_user_defined_functions() {
    let output = run_aq(
        &[
            "(def inc: . + 1; 1 | inc), (1 as $x | def capture: $x; 2 | capture)",
            "--output-format",
            "json",
        ],
        Some("null"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2\n1\n"
    );
}

#[test]
fn supports_user_defined_functions_with_filter_parameters() {
    let output = run_aq(
        &[
            "def apply_each(f): .[] | f; [1,2] | apply_each(. + 1)",
            "--output-format",
            "json",
        ],
        Some("null"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2\n3\n"
    );
}

#[test]
fn supports_include_and_import_directives() {
    let dir = temp_dir("jq-module-cli");
    fs::write(
        dir.join("math.jq"),
        "def inc: . + 1; def twice_inc: inc | inc;",
    )
    .expect("module should write");
    let output = run_aq_in_dir(
        &[
            "include \"math\"; import \"math\" as m; (1 | inc), (1 | m::twice_inc)",
            "--output-format",
            "json",
        ],
        Some("null"),
        &dir,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2\n3\n"
    );
}

#[test]
fn supports_library_path_and_module_metadata_syntax() {
    let dir = temp_dir("jq-library-path-cli");
    let lib = dir.join("lib");
    fs::create_dir_all(&lib).expect("lib dir should create");
    fs::write(
        lib.join("math.jq"),
        "module {kind: \"math\"}; def inc: . + 1; def twice_inc: inc | inc;",
    )
    .expect("module should write");
    let output = run_aq_in_dir(
        &[
            "--library-path",
            lib.to_str().expect("path should be utf8"),
            "include \"math\" {search: \"lib\"}; import \"math\" as m {search: \"lib\"}; (1 | inc), (1 | m::twice_inc)",
            "--output-format",
            "json",
        ],
        Some("null"),
        &dir,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2\n3\n"
    );
}

#[test]
fn rejects_invalid_function_definitions() {
    let forward_ref = run_aq(&["def a: b; def b: . + 1; 1 | a"], Some("null"));
    assert!(!forward_ref.status.success());
    let stderr = String::from_utf8(forward_ref.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("unsupported identifier `b`"));

    let late_capture = run_aq(&["def foo: $x; 1 as $x | 2 | foo"], Some("null"));
    assert!(!late_capture.status.success());
    let stderr = String::from_utf8(late_capture.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("$x is not defined"));
}

#[test]
fn rejects_undefined_variables() {
    let output = run_aq(&["$missing"], Some("null"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("$missing is not defined"));
}

#[test]
fn reports_upstream_jq_failure_contract_slice() {
    let object_key = run_aq(&["-n", "{(0):1}"], None);
    assert!(!object_key.status.success());
    let stderr = String::from_utf8(object_key.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("Cannot use number (0) as object key"));

    let missing_label = run_aq(&["-n", ". as $foo | break $foo"], None);
    assert!(!missing_label.status.success());
    let stderr = String::from_utf8(missing_label.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("label foo is not defined"));

    let metadata = run_aq(&["-n", "module (.+1); 0"], None);
    assert!(!metadata.status.success());
    let stderr = String::from_utf8(metadata.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("module metadata must be constant"));

    let invalid_escape = run_aq(&["-n", "include \"\\ \"; 0"], None);
    assert!(!invalid_escape.status.success());
    let stderr = String::from_utf8(invalid_escape.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("invalid escape at line 1 column 4"));

    let invalid_percent = run_aq(&["-n", "%::wat"], None);
    assert!(!invalid_percent.status.success());
    let stderr = String::from_utf8(invalid_percent.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("syntax error, unexpected `%`, expecting end of file"));

    let unterminated_object = run_aq(&["-n", "{"], None);
    assert!(!unterminated_object.status.success());
    let stderr = String::from_utf8(unterminated_object.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("syntax error, unexpected end of file"));

    let stray_close = run_aq(&["-n", "}"], None);
    assert!(!stray_close.status.success());
    let stderr = String::from_utf8(stray_close.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("syntax error, unexpected INVALID_CHARACTER, expecting end of file"));
}

#[test]
fn supports_array_destructuring_bindings() {
    let output = run_aq(
        &[". as [$a, $b, {c: $c}] | $a + $b + $c"],
        Some(r#"[2,3,{"c":4,"d":5}]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "9\n"
    );
}

#[test]
fn supports_object_destructuring_with_postfix_lookup() {
    let output = run_aq(
        &[
            ". as {realnames: $names, posts: $posts} | $posts[] | {title: .title, author: $names[.author]}",
            "--compact",
        ],
        Some(
            r#"{"posts":[{"title":"First post","author":"anon"},{"title":"A well-written article","author":"person1"}],"realnames":{"anon":"Anonymous Coward","person1":"Person McPherson"}}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"title\":\"First post\",\"author\":\"Anonymous Coward\"}\n{\"title\":\"A well-written article\",\"author\":\"Person McPherson\"}\n"
    );
}

#[test]
fn missing_destructured_values_bind_null() {
    let output = run_aq(
        &[". as {a: [$x], b: $b} | [$x, $b]", "--compact"],
        Some(r#"{"b":1}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[null,1]\n"
    );
}

#[test]
fn supports_destructuring_alternative_bindings() {
    let output = run_aq(
        &[
            ".[] as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]",
            "--compact",
        ],
        Some(r#"[{"a":1,"b":[2,{"d":3}]},[4,{"b":5,"c":6},7,8,9],"foo"]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,null,2,3,null,null]\n[4,5,null,null,7,null]\n[null,null,null,null,null,\"foo\"]\n"
    );
}

#[test]
fn supports_destructuring_alternative_fallback_on_later_errors() {
    let output = run_aq(
        &[
            ".[] as [$a] ?// [$b] | if $a != null then error(\"boom\") else {$a, $b} end",
            "--compact",
        ],
        Some("[[3]]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":null,\"b\":3}\n"
    );
}

#[test]
fn reports_final_destructuring_alternative_errors() {
    let output = run_aq(
        &[
            ".[] as [$a] ?// [$b] | if $a != null then error(\"boom\") else error(\"fallback boom\") end",
        ],
        Some("[[3]]"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("fallback boom"));
}

#[test]
fn supports_reduce_sum() {
    let output = run_aq(
        &["reduce .[] as $item (0; . + $item)"],
        Some(r#"[1,2,3,4]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "10\n"
    );
}

#[test]
fn supports_reduce_with_destructuring_pattern() {
    let output = run_aq(
        &["reduce .[] as [$x, $y] (0; . + $x + $y)"],
        Some(r#"[[1,2],[3,4],[5,6]]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "21\n"
    );
}

#[test]
fn supports_reduce_with_outer_bindings() {
    let output = run_aq(
        &[".factor as $f | reduce .items[] as $item (0; . + ($item * $f))"],
        Some(r#"{"factor":10,"items":[1,2,3]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "60\n"
    );
}

#[test]
fn supports_reduce_with_multi_output_init() {
    let output = run_aq(&["-n", "reduce [1,2][] as $x (0,10; . + $x)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n13\n"
    );
}

#[test]
fn supports_unary_minus_before_reduce_expression() {
    let output = run_aq(
        &["[-reduce -.[] as $x (0; . + $x)]", "--compact"],
        Some(r#"[1,2,3]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[6]\n"
    );
}

#[test]
fn supports_foreach_with_default_extract() {
    let output = run_aq(&["foreach .[] as $x (0; . + $x)"], Some(r#"[1,2,3]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n3\n6\n"
    );
}

#[test]
fn supports_foreach_with_explicit_extract() {
    let output = run_aq(
        &["foreach .[] as $x (0; . + $x; . * 10)"],
        Some(r#"[1,2,3]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "10\n30\n60\n"
    );
}

#[test]
fn supports_foreach_with_multi_output_init() {
    let output = run_aq(&["-n", "foreach [1,2][] as $x (0,10; . + $x)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n3\n11\n13\n"
    );
}

#[test]
fn supports_foreach_with_destructuring_and_outer_bindings() {
    let output = run_aq(
        &[".factor as $f | foreach .pairs[] as [$x, $y] (0; . + $x + $y; . * $f)"],
        Some(r#"{"factor":10,"pairs":[[1,2],[3,4]]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "30\n100\n"
    );
}

#[test]
fn supports_unary_minus_before_foreach_expression() {
    let output = run_aq(
        &["[-foreach -.[] as $x (0; . + $x)]", "--compact"],
        Some(r#"[1,2,3]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,3,6]\n"
    );
}

#[test]
fn supports_range_builtin() {
    let output = run_aq(
        &["-n", "range(3), range(1;4), range(0;10;3), range(0;1;0.25)"],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "0\n1\n2\n1\n2\n3\n0\n3\n6\n9\n0\n0.25\n0.5\n0.75\n"
    );
}

#[test]
fn supports_range_with_multi_output_arguments() {
    let output = run_aq(
        &[
            "-n",
            "[range(0,1;3,4)], [range(0,1;4,5;1,2)], [range(0,1,2;4,3,2;2,3)], [range(3,5)]",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1,2,0,1,2,3,1,2,1,2,3]\n[0,1,2,3,0,2,0,1,2,3,4,0,2,4,1,2,3,1,3,1,2,3,4,1,3]\n[0,2,0,3,0,2,0,0,0,1,3,1,1,1,1,1,2,2,2,2]\n[0,1,2,0,1,2,3,4]\n"
    );
}

#[test]
fn supports_limit_builtin() {
    let output = run_aq(
        &["limit(.n; .items[]), limit(1.1; .items[])"],
        Some(r#"{"n":2,"items":[1,2,3]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n2\n1\n2\n"
    );
}

#[test]
fn supports_limit_builtin_on_direct_ranges() {
    let output = run_aq(&["-n", "[limit(5,7; range(9))]", "--compact"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1,2,3,4,0,1,2,3,4,5,6]\n"
    );
}

#[test]
fn limit_short_circuits_after_requested_results() {
    let output = run_aq(
        &[
            "[limit(0; error)], [limit(1; 1, error)]",
            "--compact",
            "--output-format",
            "json",
        ],
        Some(r#""badness""#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[]\n[1]\n"
    );
}

#[test]
fn rejects_negative_limit_count() {
    let output = run_aq(&["limit(-1; .items[])"], Some(r#"{"items":[1,2,3]}"#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("limit doesn't support negative count"));
}

#[test]
fn supports_generator_selection_builtins() {
    let output = run_aq(
        &[
            "-n",
            "first(range(5)), last(range(5)), nth(2; range(5)), nth(range(3); range(10))",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "0\n4\n2\n0\n1\n2\n"
    );
}

#[test]
fn supports_unary_nth_builtin_on_arrays() {
    let output = run_aq(&["-n", "[range(10)] | nth(5)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "5\n"
    );
}

#[test]
fn nth_short_circuits_after_requested_index() {
    let output = run_aq(&["-n", "nth(1; 0,1,error(\"foo\"))"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n"
    );
}

#[test]
fn supports_skip_generator_builtin() {
    let output = run_aq(
        &["[skip(3; .[])]", "--compact"],
        Some("[1,2,3,4,5,6,7,8,9]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[4,5,6,7,8,9]\n"
    );

    let output = run_aq(&["[skip(0,2,3,4; .[])]", "--compact"], Some("[1,2,3]"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2,3,3]\n"
    );
}

#[test]
fn generator_selection_empty_cases_produce_no_output() {
    let output = run_aq(
        &["-n", "first(empty), last(empty), nth(10; range(5))"],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
}

#[test]
fn rejects_negative_nth_indices() {
    let output = run_aq(&["-n", "nth(-1; range(5))"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("nth doesn't support negative indices"));
}

#[test]
fn supports_while_builtin() {
    let output = run_aq(
        &[
            "-n",
            "(0 | while(. < 3; . + 1)), ([1,2,3] | while(length > 0; .[1:]))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "0\n1\n2\n[1,2,3]\n[2,3]\n[3]\n"
    );
}

#[test]
fn supports_repeat_builtin() {
    let output = run_aq(
        &[
            "[repeat(.*2, error)?]",
            "--compact",
            "--output-format",
            "json",
        ],
        Some("1"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[2]\n"
    );
}

#[test]
fn supports_until_builtin_with_outer_bindings() {
    let output = run_aq(
        &[".limit as $n | 0 | until(. >= $n; . + 1), 0 | until(. >= 2; . + 1, 10)"],
        Some(r#"{"limit":5}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "5\n2\n10\n10\n"
    );
}

#[test]
fn supports_recurse_builtin_and_recursive_descent_alias() {
    let output = run_aq(
        &[
            "-n",
            "([1,[2,[3]],4] | recurse | numbers), ({a:1,b:{c:2},d:[3]} | .. | numbers)",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n2\n3\n4\n1\n2\n3\n"
    );
}

#[test]
fn supports_recurse_with_custom_query() {
    let output = run_aq(
        &["recurse(.children[]) | .name", "-r"],
        Some(
            r#"{"name":"root","children":[{"name":"a","children":[{"name":"a1","children":[]}]},{"name":"b","children":[]}]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "root\na\na1\nb\n"
    );
}

#[test]
fn supports_recurse_with_condition() {
    let output = run_aq(&["-n", "2 | recurse(. * .; . < 20)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2\n4\n16\n"
    );
}

#[test]
fn supports_nested_defs_with_outer_parameters() {
    let output = run_aq(
        &[
            "-n",
            "def range(init; upto; by): def _range: if (by > 0 and . < upto) or (by < 0 and . > upto) then ., ((.+by)|_range) else empty end; if init == upto then empty elif by == 0 then init else init|_range end; [range(0; 10; 3)]",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,3,6,9]\n"
    );
}

#[test]
fn supports_map_values_builtin() {
    let output = run_aq(
        &[
            "-n",
            "([1,2] | map_values(. + 1)), ({a:1,b:2} | map_values(empty)), ([1,2] | map_values(., . + 10))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[2,3]\n{}\n[1,2]\n"
    );
}

#[test]
fn supports_indices_builtin() {
    let output = run_aq(
        &[
            "-n",
            "(\"aaaa\" | indices(\"aa\")), ([1,1,1] | indices([1,1])), (\"abc\" | indices(\"\"))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1,2]\n[0,1]\n[]\n"
    );
}

#[test]
fn supports_multi_output_index_builtins() {
    let output = run_aq(
        &[
            "[(index(\",\",\"|\"), rindex(\",\",\"|\")), indices(\",\",\"|\")]",
            "--compact",
            "--output-format",
            "json",
        ],
        Some(r#""a,b|c,d,e||f,g,h,|,|,i,j""#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,3,22,19,[1,5,7,12,14,16,18,20,22],[3,9,10,17,19]]\n"
    );

    let output = run_aq(
        &["index([1,2]), rindex([1,2])"],
        Some("[0,1,2,3,1,4,2,5,1,2,6,7]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n8\n"
    );
}

#[test]
fn supports_getpath_setpath_and_delpaths() {
    let output = run_aq(
        &[
            "-n",
            "({\"a\":{\"b\":1},\"items\":[10,20,30]} | getpath([\"a\",\"b\"])), ({\"a\":{\"b\":1},\"items\":[10,20,30]} | getpath([\"items\",-1])), (null | setpath([\"a\",\"b\"]; 1)), ([] | setpath([2]; 7)), ({\"a\":1,\"b\":2,\"c\":3} | del(.b, .c)), ({\"a\":1,\"b\":2,\"c\":3} | delpaths([[\"b\"],[\"c\"]]))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n30\n{\"a\":{\"b\":1}}\n[null,null,7]\n{\"a\":1}\n{\"a\":1}\n"
    );

    let output = run_aq(
        &[
            "-n",
            "[{\"a\":{\"b\":0,\"c\":1}} | getpath([\"a\",\"b\"], [\"a\",\"c\"])]",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1]\n"
    );
}

#[test]
fn supports_setpath_multi_output_values_and_delpaths_root_delete() {
    let output = run_aq(
        &[
            "-n",
            "(null | setpath([\"a\"]; 1, 2)), (1 | delpaths([[]]))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":1}\n{\"a\":2}\nnull\n"
    );
}

#[test]
fn rejects_invalid_path_updates() {
    let getpath_output = run_aq(&["getpath([0])"], Some("1"));
    assert!(!getpath_output.status.success());
    let getpath_stderr = String::from_utf8(getpath_output.stderr).expect("stderr should be utf8");
    assert!(getpath_stderr.contains("Cannot index number with number"));

    let setpath_output = run_aq(&["-n", "setpath([-1]; 9)"], None);
    assert!(!setpath_output.status.success());
    let setpath_stderr = String::from_utf8(setpath_output.stderr).expect("stderr should be utf8");
    assert!(setpath_stderr.contains("Out of bounds negative array index"));

    let delpaths_output = run_aq(&["delpaths([[\"a\", \"x\"]])"], Some(r#"{"a":1}"#));
    assert!(!delpaths_output.status.success());
    let delpaths_stderr = String::from_utf8(delpaths_output.stderr).expect("stderr should be utf8");
    assert!(delpaths_stderr.contains("cannot delete fields from integer"));
}

#[test]
fn reports_contextual_setpath_array_path_errors() {
    let output = run_aq(
        &[
            "try [\"OK\", setpath([[1]]; 1)] catch [\"KO\", .]",
            "--compact",
        ],
        Some("[]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"KO\",\"Cannot update field at array index of array\"]\n"
    );
}

#[test]
fn supports_paths_and_leaf_paths() {
    let output = run_aq(
        &[
            "-n",
            "({\"a\":[1,{\"b\":2}],\"c\":3} | paths), ({\"a\":[1,{\"b\":2}],\"c\":3} | paths(type == \"number\")), ({\"a\":[1,{\"b\":2}],\"c\":3} | leaf_paths)",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"a\"]\n[\"a\",0]\n[\"a\",1]\n[\"a\",1,\"b\"]\n[\"c\"]\n[\"a\",0]\n[\"a\",1,\"b\"]\n[\"c\"]\n[\"a\",0]\n[\"a\",1,\"b\"]\n[\"c\"]\n"
    );
}

#[test]
fn paths_on_scalars_produce_no_output() {
    let output = run_aq(
        &[
            "-n",
            "1 | paths, 1 | paths(type == \"number\"), 1 | leaf_paths",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
}

#[test]
fn supports_walk_builtin() {
    let output = run_aq(
        &[
            "-n",
            "([1,{\"a\":2},[3]] | walk(if type == \"number\" then . + 1 else . end)), ({\"a\":1,\"b\":{\"c\":2}} | walk(if type == \"object\" then with_entries({key: (.key | ascii_upcase), value: .value}) else . end))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[2,{\"a\":3},[4]]\n{\"A\":1,\"B\":{\"C\":2}}\n"
    );
}

#[test]
fn supports_walk_multi_output_and_empty_cases() {
    let output = run_aq(
        &[
            "-n",
            "([1,2] | walk(if type == \"number\" then ., . + 10 else . end)), ({\"a\":1} | walk(empty))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,11,2,12]\n"
    );
}

#[test]
fn supports_transpose_builtin() {
    let output = run_aq(
        &[
            "-n",
            "([[1,2,3],[4,5,6]] | transpose), ([[1,2],[3],null] | transpose), ([[],[]] | transpose)",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[[1,4],[2,5],[3,6]]\n[[1,3,null],[2,null,null]]\n[]\n"
    );
}

#[test]
fn rejects_invalid_transpose_rows() {
    let output = run_aq(&["-n", "[[1],{\"a\":2}] | transpose"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("cannot index object with number"));
}

#[test]
fn supports_flatten_with_depth() {
    let output = run_aq(
        &[
            "-n",
            "([[[[1]]],2] | flatten(1)), ([[[[1]]],2] | flatten(2)), ([[[[1]]],2] | flatten(0))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[[[1]],2]\n[[1],2]\n[[[[1]]],2]\n"
    );
}

#[test]
fn supports_flatten_with_multi_output_depths() {
    let output = run_aq(
        &["flatten(3,2,1)", "--compact"],
        Some("[0,[1],[[2]],[[[3]]]]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1,2,3]\n[0,1,2,[3]]\n[0,1,[2],[[3]]]\n"
    );
}

#[test]
fn supports_avg_median_take_and_skip_extensions() {
    let output = run_aq(
        &[
            "-n",
            "([1,2,3,4] | avg), ([1,2,3,4] | median), ([1,2,3,4] | take(2)), ([1,2,3,4] | skip(2))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2.5\n2.5\n[1,2]\n[3,4]\n"
    );
}

#[test]
fn supports_histogram_extension() {
    let output = run_aq(
        &[
            "-n",
            "([1,2,3,4] | histogram(2)), ([5,5,5] | histogram(4))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"start\":1,\"end\":2.5,\"count\":2},{\"start\":2.5,\"end\":4,\"count\":2}]\n[{\"start\":5,\"end\":5,\"count\":3}]\n"
    );
}

#[test]
fn rejects_invalid_take_count() {
    let output = run_aq(&["-n", "[1,2,3] | take(1.5)"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("take count must be a non-negative integer"));
}

#[test]
fn supports_aq_extension_aliases_and_grouping_helpers() {
    let output = run_aq(
        &[
            "-n",
            "(\"42\" | to_number), (\"false\" | to_bool), ([{\"b\":1,\"a\":2},{\"c\":3,\"a\":4},null] | columns), ([1,2,3,4] | stddev), ([1,2,3,4] | percentile(50)), ([{\"a\":2,\"name\":\"x\"},{\"a\":1,\"name\":\"y\"},{\"a\":2,\"name\":\"z\"}] | uniq_by(.a)), ([{\"a\":2,\"name\":\"x\"},{\"a\":1,\"name\":\"y\"},{\"a\":2,\"name\":\"z\"}] | sort_by_desc(.a)), ([{\"a\":2},{\"a\":1},{\"a\":2}] | count_by(.a))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "42\nfalse\n[\"b\",\"a\",\"c\"]\n1.118033988749895\n2.5\n[{\"a\":1,\"name\":\"y\"},{\"a\":2,\"name\":\"x\"}]\n[{\"a\":2,\"name\":\"x\"},{\"a\":2,\"name\":\"z\"},{\"a\":1,\"name\":\"y\"}]\n[{\"key\":[1],\"count\":1},{\"key\":[2],\"count\":2}]\n"
    );
}

#[test]
fn supports_pick_and_omit_extensions() {
    let output = run_aq(
        &[
            "-n",
            "{\"a\":1,\"b\":2,\"c\":{\"d\":3,\"e\":4},\"items\":[{\"x\":1,\"y\":2},{\"x\":3,\"y\":4}]} | pick(.a, .c.d, .items[].x), {\"a\":1,\"b\":2,\"c\":{\"d\":3,\"e\":4},\"items\":[{\"x\":1,\"y\":2},{\"x\":3,\"y\":4}]} | omit(.b, .c.e, .items[].y)",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":1,\"c\":{\"d\":3},\"items\":[{\"x\":1},{\"x\":3}]}\n{\"a\":1,\"c\":{\"d\":3},\"items\":[{\"x\":1},{\"x\":3}]}\n"
    );
}

#[test]
fn supports_rename_extension() {
    let output = run_aq(
        &[
            "-n",
            "({\"old\":1,\"new\":2,\"keep\":3,\"items\":[{\"old\":4,\"keep\":5},{\"keep\":6}]} | rename(.old; \"renamed\")), ({\"old\":1,\"new\":2,\"keep\":3,\"items\":[{\"old\":4,\"keep\":5},{\"keep\":6}]} | rename(.items[].old; \"renamed\"))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"renamed\":1,\"new\":2,\"keep\":3,\"items\":[{\"old\":4,\"keep\":5},{\"keep\":6}]}\n{\"old\":1,\"new\":2,\"keep\":3,\"items\":[{\"renamed\":4,\"keep\":5},{\"keep\":6}]}\n"
    );
}

#[test]
fn rejects_invalid_pick_and_omit_paths() {
    let output = run_aq(&["-n", "pick(length)"], Some(r#"{"items":[1,2]}"#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("Invalid path expression"));

    let output = run_aq(&["-n", "omit(length)"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("Invalid path expression"));
}

#[test]
fn rejects_invalid_aq_type_and_rename_inputs() {
    let output = run_aq(&["-n", "\"maybe\" | to_bool"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("to_bool cannot parse string"));

    let output = run_aq(&["-n", "\" FALSE \" | toboolean"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("string (\" FALSE \") cannot be parsed as a boolean"));

    let output = run_aq(&["-n", "[1,2] | columns"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("columns expects objects or arrays of objects"));

    let output = run_aq(
        &["-n", "{\"items\":[1,2]} | rename(.items[0]; \"x\")"],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("rename requires exact field paths"));

    let output = run_aq(&["-n", "[1,\"x\"] | stddev"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("stddev is not defined for string"));

    let output = run_aq(&["-n", "[1,2,3] | percentile(101)"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("percentile expects a percentile between 0 and 100"));
}

#[test]
fn supports_count_by_multi_output_expression() {
    let output = run_aq(&["-n", "[{\"a\":1}] | count_by(., .a)", "--compact"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"key\":[{\"a\":1},1],\"count\":1}]\n"
    );
}

#[test]
fn rejects_invalid_histogram_inputs() {
    let output = run_aq(&["-n", "[1,2,3] | histogram(0)"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("histogram bins must be a positive integer"));

    let output = run_aq(&["-n", "[1,2,3] | histogram(1.5)"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("histogram count must be a non-negative integer"));
}

#[test]
fn rejects_invalid_flatten_depth() {
    let output = run_aq(&["-n", "[[[1]]] | flatten(-1)"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("flatten depth must not be negative"));
}

#[test]
fn supports_math_builtins() {
    let output = run_aq(
        &[
            "-n",
            "(1.2 | floor), (1.2 | ceil), (1.2 | round), (1.2 | fabs), (4 | sqrt), (2 | log), (8 | log2), (100 | log10), (1 | exp), (3 | exp2), (2 | sin), (2 | cos), (2 | tan), (0.5 | asin), (0.5 | acos), (2 | atan), (-1.2 | floor), (-1.2 | ceil), (-1.2 | round), (-1.2 | fabs), (-1 | sqrt)",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_multiline_json_output_close(
        &String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n2\n1\n1.2\n2\n0.6931471805599453\n3\n2\n2.718281828459045\n8\n0.9092974268256817\n-0.4161468365471424\n-2.185039863261519\n0.5235987755982989\n1.0471975511965979\n1.1071487177940904\n-2\n-1\n-1\n1.2\nnull\n"
        ,
        1e-15,
    );

    let merge_output = run_aq(
        &[
            "{\"k\":{\"a\":1,\"b\":2}} * ., {\"k\":{\"a\":1,\"b\":2},\"hello\":{\"x\":1}} * ., {\"k\":{\"a\":1,\"b\":2},\"hello\":1} * .",
            "--compact",
            "-o",
            "json",
        ],
        Some(r#"{"k":{"a":0,"c":3},"hello":1}"#),
    );
    assert!(merge_output.status.success());
    assert_eq!(
        String::from_utf8(merge_output.stdout).expect("stdout should be utf8"),
        concat!(
            "{\"k\":{\"a\":0,\"b\":2,\"c\":3},\"hello\":1}\n",
            "{\"k\":{\"a\":0,\"b\":2,\"c\":3},\"hello\":1}\n",
            "{\"k\":{\"a\":0,\"b\":2,\"c\":3},\"hello\":1}\n"
        )
    );

    let nested_merge_output = run_aq(
        &[
            "{\"a\":{\"b\":1},\"c\":{\"d\":2},\"e\":5} * .",
            "--compact",
            "-o",
            "json",
        ],
        Some(r#"{"a":{"b":2},"c":{"d":3,"f":9}}"#),
    );
    assert!(nested_merge_output.status.success());
    assert_eq!(
        String::from_utf8(nested_merge_output.stdout).expect("stdout should be utf8"),
        "{\"a\":{\"b\":2},\"c\":{\"d\":3,\"f\":9},\"e\":5}\n"
    );
}

#[test]
fn supports_jq_boolean_and_float_constant_builtins() {
    let output = run_aq(
        &["map(toboolean)", "--compact"],
        Some(r#"["false","true",false,true]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[false,true,false,true]\n"
    );

    let output = run_aq(
        &[
            "-n",
            "[(infinite, -infinite) % (1, -1, infinite)], [nan % 1, 1 % nan | isnan]",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,0,0,0,0,-1]\n[true,true]\n"
    );
}

#[test]
fn treats_integer_and_float_values_as_equal() {
    let output = run_aq(
        &[
            "[{\"a\":42},.object,10,.num,false,true,null,\"b\",[1,4]] | .[] as $x | [$x == .[]]",
            "--compact",
        ],
        Some(r#"{"object":{"a":42},"num":10.0}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[true,true,false,false,false,false,false,false,false]\n[true,true,false,false,false,false,false,false,false]\n[false,false,true,true,false,false,false,false,false]\n[false,false,true,true,false,false,false,false,false]\n[false,false,false,false,true,false,false,false,false]\n[false,false,false,false,false,true,false,false,false]\n[false,false,false,false,false,false,true,false,false]\n[false,false,false,false,false,false,false,true,false]\n[false,false,false,false,false,false,false,false,true]\n"
    );
}

#[test]
fn supports_jq_pipe_precedence_with_comma_generators() {
    let output = run_aq(&["-n", "{x:(1,2)},{x:3} | .x", "--compact"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n2\n3\n"
    );

    let output = run_aq(&["-n", "[nan % 1, 1 % nan | isnan]", "--compact"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[true,true]\n"
    );
}

#[test]
fn rejects_invalid_math_inputs() {
    let output = run_aq(&["-n", "\"x\" | floor"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("floor is not defined for string"));
}

#[test]
fn supports_pow_and_math_domain_edges() {
    let output = run_aq(
        &[
            "-n",
            "pow(2; 3), pow(2; 0.5), pow(-1; 0.5), pow(0; -1), (0 | log), (2 | asin), (2 | acos)",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "8\n1.4142135623730951\nnull\n1.7976931348623157e+308\n-1.7976931348623157e+308\nnull\nnull\n"
    );
}

#[test]
fn supports_date_builtins() {
    let output = run_aq(
        &[
            "-n",
            "(1.9 | todate), (\"1970-01-01T00:00:00Z\" | fromdate), (1425599507 | gmtime), (1425599507.25 | gmtime[5]), ([2015,2,5,23,51,47,4,63] | strftime(\"%Y-%m-%dT%H:%M:%SZ\")), (1435677542.822351 | strftime(\"%A, %B %d, %Y\")), ([2024,2,15] | strftime(\"%Y-%m-%dT%H:%M:%SZ\")), ([2024,8,21] | mktime), (\"2015-03-05T23:51:47Z\" | strptime(\"%Y-%m-%dT%H:%M:%SZ\")), (\"2015-03-05T23:51:47Z\" | strptime(\"%Y-%m-%dT%H:%M:%SZ\") | mktime), (0 | strflocaltime(\"\" | ., @uri))",
            "--compact",
            "-o",
            "json",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!(
            "\"1970-01-01T00:00:01Z\"\n",
            "0\n",
            "[2015,2,5,23,51,47,4,63]\n",
            "47.25\n",
            "\"2015-03-05T23:51:47Z\"\n",
            "\"Tuesday, June 30, 2015\"\n",
            "\"2024-03-15T00:00:00Z\"\n",
            "1726876800\n",
            "[2015,2,5,23,51,47,4,63]\n",
            "1425599507\n",
            "\"\"\n",
            "\"\"\n"
        )
    );
}

#[test]
fn supports_to_datetime_extension() {
    let output = run_aq(
        &[
            "-n",
            "(\"1970-01-01T01:00:00+01:00\" | to_datetime | type), (\"1970-01-01T01:00:00+01:00\" | to_datetime | fromdate), (\"1970-01-02\" | to_datetime)",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"datetime\"\n0\n\"1970-01-02T00:00:00Z\"\n"
    );

    let raw_output = run_aq(&["-n", "\"1970-01-02\" | to_datetime", "-r"], None);
    assert!(raw_output.status.success());
    assert_eq!(
        String::from_utf8(raw_output.stdout).expect("stdout should be utf8"),
        "1970-01-02T00:00:00Z\n"
    );
}

#[test]
fn supports_now_builtin() {
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_secs_f64();
    let output = run_aq(&["-n", "now", "--compact"], None);
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_secs_f64();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let value = stdout
        .trim()
        .parse::<f64>()
        .expect("now output should be a float");
    assert!(value >= before);
    assert!(value <= after);
}

#[test]
fn rejects_invalid_date_inputs() {
    let output = run_aq(&["-n", "null | todate"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("todate is not defined for null"));

    let output = run_aq(&["-n", "\"x\" | fromdate"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("date \"x\" does not match format"));

    let output = run_aq(
        &[
            "-n",
            "[\"a\",1,2,3,4,5,6,7] | strftime(\"%Y-%m-%dT%H:%M:%SZ\")",
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("strftime/1 requires parsed datetime inputs"));

    let output = run_aq(&["-n", "0 | strftime([])"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("strftime/1 requires a string format"));

    let output = run_aq(&["-n", "[\"a\",1,2,3,4,5,6,7] | mktime"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("mktime requires parsed datetime inputs"));

    let output = run_aq(&["-n", "1 | strptime(\"%Y-%m-%d\")"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("strptime/1 requires string inputs"));

    let output = run_aq(
        &[
            "-n",
            "[\"a\",1,2,3,4,5,6,7] | strflocaltime(\"%Y-%m-%dT%H:%M:%SZ\")",
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("strflocaltime/1 requires parsed datetime inputs"));

    let output = run_aq(&["-n", "0 | strflocaltime({})"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("strflocaltime/1 requires a string format"));
}

#[test]
fn rejects_invalid_to_datetime_inputs() {
    let output = run_aq(&["-n", "1 | to_datetime"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("to_datetime is not defined for integer"));

    let output = run_aq(&["-n", "\"x\" | to_datetime"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("to_datetime cannot parse string \"x\""));
}

#[test]
fn supports_env_builtin() {
    let output = run_aq_with_env(
        &["-n", "env.TEST_AQ_ENV, $ENV.TEST_AQ_ENV", "-r"],
        None,
        &[("TEST_AQ_ENV", "codex")],
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "codex\ncodex\n"
    );
}

#[test]
fn supports_postfix_dynamic_lookup_with_bound_variables() {
    let output = run_aq(
        &[
            ".realnames as $names | .posts[] | {title: .title, author: $names[.author]}",
            "--compact",
        ],
        Some(
            r#"{"posts":[{"title":"First post","author":"anon"},{"title":"A well-written article","author":"person1"}],"realnames":{"anon":"Anonymous Coward","person1":"Person McPherson"}}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"title\":\"First post\",\"author\":\"Anonymous Coward\"}\n{\"title\":\"A well-written article\",\"author\":\"Person McPherson\"}\n"
    );
}

#[test]
fn supports_postfix_field_access_on_parenthesized_queries() {
    let output = run_aq(
        &["(.user).name", "-r"],
        Some(r#"{"user":{"name":"alice"}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn supports_reverse_builtin() {
    let output = run_aq(&["reverse", "--compact"], Some(r#"[1,2,3]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[3,2,1]\n"
    );
}

#[test]
fn supports_sort_builtin() {
    let output = run_aq(&["sort", "--compact"], Some(r#"[3,1,2]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2,3]\n"
    );
}

#[test]
fn supports_empty_builtin() {
    let output = run_aq(&["empty"], Some(r#"{"name":"alice"}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
}

#[test]
fn supports_values_filter() {
    let output = run_aq(&[".items[] | values"], Some(r#"{"items":[1,null,false]}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\nfalse\n"
    );
}

#[test]
fn supports_type_filters() {
    let output = run_aq(
        &["(.items[] | strings), (.items[] | numbers)", "--compact"],
        Some(r#"{"items":["alice",2,true]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n2\n"
    );
}

#[test]
fn supports_tostring_builtin() {
    let output = run_aq(&[".obj | tostring", "-r"], Some(r#"{"obj":{"a":1}}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":1}\n"
    );
}

#[test]
fn supports_tonumber_builtin() {
    let output = run_aq(&[".count | tonumber"], Some(r#"{"count":"3.5"}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3.5\n"
    );
}

#[test]
fn reports_jq_style_tonumber_errors() {
    let output = run_aq(
        &[
            "--input-format",
            "json",
            "try tonumber catch .",
            "--compact",
        ],
        Some(r#""123\u0000456""#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"string (\\\"123\\\\u0000456\\\") cannot be parsed as a number\"\n"
    );
}

#[test]
fn supports_startswith_and_endswith_builtins() {
    let output = run_aq(
        &["(.name | startswith(\"ali\")), (.name | endswith(\"ice\"))"],
        Some(r#"{"name":"alice"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\ntrue\n"
    );
}

#[test]
fn supports_regex_builtins() {
    let output = run_aq(
        &[
            "(.id | test(\"^(?<name>[a-z]+)-(?<num>[0-9]+)$\")), (.id | capture(\"^(?<name>[a-z]+)-(?<num>[0-9]+)$\")), (.value | sub(\"cat\"; \"dog\")), (.numbers | gsub(\"[0-9]+\"; \"#\"))",
            "--compact",
        ],
        Some(r#"{"id":"alice-42","value":"catapult cat cat","numbers":"a1 b22 c333"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n{\"name\":\"alice\",\"num\":\"42\"}\n\"dogapult cat cat\"\n\"a# b# c#\"\n"
    );
}

#[test]
fn supports_regex_flags_and_replacement_streams() {
    let output = run_aq(
        &[
            "(.mixed | test(\"^(?<letters>[a-z]+)(?<digits>[0-9]+)$\"; \"i\")), (.mixed | capture(\"^(?<letters>[a-z]+)(?<digits>[0-9]+)$\"; \"i\")), (.replace_once | sub(\"ab\"; \"X\"; \"ig\")), (.replace_all | gsub(\"(?<letters>[a-z]+)(?<digits>[0-9]+)\"; if .letters == \"ab\" then \"A\" else \"C\", \"D\" end))",
            "--compact",
        ],
        Some(r#"{"mixed":"ABC123","replace_once":"abABab","replace_all":"ab12--cd34"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!(
            "true\n",
            "{\"letters\":\"ABC\",\"digits\":\"123\"}\n",
            "\"XXX\"\n",
            "\"A--C\"\n",
            "\"--D\"\n"
        )
    );
}

#[test]
fn supports_match_and_scan_builtins() {
    let output = run_aq(
        &[
            "(.text | [match(\"([a-z]+)([0-9]+)\"; \"g\")]), (.text | [scan(\"([a-z]+)([0-9]+)\")]), (.upper | [scan(\"abc\"; \"i\")])",
            "--compact",
        ],
        Some(r#"{"text":"abc123def456","upper":"ABCabc"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!(
            "[{\"offset\":0,\"length\":6,\"string\":\"abc123\",\"captures\":[{\"offset\":0,\"length\":3,\"string\":\"abc\",\"name\":null},{\"offset\":3,\"length\":3,\"string\":\"123\",\"name\":null}]},{\"offset\":6,\"length\":6,\"string\":\"def456\",\"captures\":[{\"offset\":6,\"length\":3,\"string\":\"def\",\"name\":null},{\"offset\":9,\"length\":3,\"string\":\"456\",\"name\":null}]}]\n",
            "[[\"abc\",\"123\"],[\"def\",\"456\"]]\n",
            "[\"ABC\",\"abc\"]\n"
        )
    );
}

#[test]
fn supports_jq_style_regex_argument_forms_and_splits() {
    let output = run_aq(
        &[
            "(.text | [match([\"foo\", \"ig\"])]), (.text | [test(\"( )*\"; \"gn\")]), (.csv | split(\", *\"; null)), (.csv | [splits(\", *\")]), (.optional | [splits(\",? *\"; \"n\")]), (.trail | gsub(\"[^a-z]*(?<x>[a-z]*)\"; \"Z\\(.x)\"))",
            "--compact",
        ],
        Some(
            r#"{"text":"foo bar FOO","csv":"ab,cd,   ef, gh","optional":"ab,cd ef,  gh","trail":"123foo456bar"}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!(
            "[{\"offset\":0,\"length\":3,\"string\":\"foo\",\"captures\":[]},{\"offset\":8,\"length\":3,\"string\":\"FOO\",\"captures\":[]}]\n",
            "[true]\n",
            "[\"ab\",\"cd\",\"ef\",\"gh\"]\n",
            "[\"ab\",\"cd\",\"ef\",\"gh\"]\n",
            "[\"ab\",\"cd\",\"ef\",\"gh\"]\n",
            "\"ZfooZbarZ\"\n"
        )
    );
}

#[test]
fn supports_positive_lookahead_gsub() {
    let output = run_aq(&["gsub(\"(?=u)\"; \"u\")", "--compact"], Some(r#""qux""#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "quux\n"
    );
}

#[test]
fn supports_format_operators() {
    let output = run_aq(
        &[
            "(.json | @json), (.json | @text), (.csv | @csv), (.tsv | @tsv), (.html | @html), (.uri | @uri), (.urid | @urid), (.sh | @sh), (.json | @base64), (.base64d | @base64d)",
            "-r",
        ],
        Some(
            r#"{"json":{"name":"alice","roles":["admin"]},"csv":["abc","a,b","a\"b","c\nd",null,1,1.25,true],"tsv":["abc","a\tb","c\nd","e\\f",null,1,1.25,true],"html":"a&b<c>d\"e'f","uri":"a b/c?d=e&f","urid":"a%20b%2Fc%3Fd%3De%26f","sh":["abc","a b","c'd",3,1.25,true,null],"base64d":"/w=="}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!(
            "{\"name\":\"alice\",\"roles\":[\"admin\"]}\n",
            "{\"name\":\"alice\",\"roles\":[\"admin\"]}\n",
            "\"abc\",\"a,b\",\"a\"\"b\",\"c\nd\",,1,1.25,true\n",
            "abc\ta\\tb\tc\\nd\te\\\\f\t\t1\t1.25\ttrue\n",
            "a&amp;b&lt;c&gt;d&quot;e&apos;f\n",
            "a%20b%2Fc%3Fd%3De%26f\n",
            "a b/c?d=e&f\n",
            "'abc' 'a b' 'c'\\''d' 3 1.25 true null\n",
            "eyJuYW1lIjoiYWxpY2UiLCJyb2xlcyI6WyJhZG1pbiJdfQ==\n",
            "�\n"
        )
    );
}

#[test]
fn supports_decimal_text_and_tabular_rendering() {
    let output = run_aq(
        &[
            "-n",
            "(1.25 | @text), ([1.25, true, null] | @csv), ([1.25, true, null] | @tsv)",
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!("1.25\n", "1.25,true,\n", "1.25\ttrue\t\n")
    );

    let csv_output = run_aq(
        &["-n", "--output-format", "csv", "[1.25, true, null]"],
        None,
    );
    assert!(csv_output.status.success());
    assert_eq!(
        String::from_utf8(csv_output.stdout).expect("stdout should be utf8"),
        "1.25,true,\n"
    );

    let tsv_output = run_aq(
        &["-n", "--output-format", "tsv", "[1.25, true, null]"],
        None,
    );
    assert!(tsv_output.status.success());
    assert_eq!(
        String::from_utf8(tsv_output.stdout).expect("stdout should be utf8"),
        "1.25\ttrue\t\n"
    );
}

#[test]
fn supports_format_operators_on_structured_values() {
    let output = run_aq(
        &[
            "(.value | @html), (.value | @uri), (.value | @base64)",
            "-r",
        ],
        Some(r#"{"value":{"a":"x y","b":[1,true]}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!(
            "{&quot;a&quot;:&quot;x y&quot;,&quot;b&quot;:[1,true]}\n",
            "%7B%22a%22%3A%22x%20y%22%2C%22b%22%3A%5B1%2Ctrue%5D%7D\n",
            "eyJhIjoieCB5IiwiYiI6WzEsdHJ1ZV19\n"
        )
    );
}

#[test]
fn rejects_invalid_regex_builtin() {
    let output = run_aq(&[".name | test(\"[\")"], Some(r#"{"name":"alice"}"#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("test failed to compile regex"));

    let output = run_aq(
        &[".name | sub(\"a\"; \"x\"; \"z\")"],
        Some(r#"{"name":"alice"}"#),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("sub does not support regex flag `z`"));
}

#[test]
fn supports_format_string_interpolation() {
    let output = run_aq(
        &[
            "(@uri \"x=\\(.x)&y=\\(.y)\"), (.n | @text \"\\(.,.+1)-\\(.+10,.+20)\")",
            "-r",
        ],
        Some(r#"{"x":"a b","y":"c/d","n":1}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        concat!("x=a%20b&y=c%2Fd\n", "1-11\n", "2-11\n", "1-21\n", "2-21\n")
    );
}

#[test]
fn rejects_invalid_format_operator_inputs() {
    let output = run_aq(&["@csv"], Some(r#"[{"a":1}]"#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("is not valid in a csv row"));

    let output = run_aq(&["@sh"], Some(r#"{"a":1}"#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("is not valid in a shell string"));

    let output = run_aq(&["@base64d"], Some(r#""%%%%""#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("is not valid base64 data"));

    let output = run_aq(&["@urid"], Some(r#""abc%ZZ""#));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("is not a valid uri encoding"));
}

#[test]
fn rejects_invalid_format_string_escapes() {
    let output = run_aq(&["@text \"\\q\""], Some("null"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("invalid quoted format string"));
    assert!(stderr.contains("invalid escape"));
}

#[test]
fn detects_csv_and_tsv_from_file_extensions() {
    let csv = temp_file("rows.csv", "\"alice\",\"admin\"\n");
    let csv_output = run_aq(
        &[".[1]", "-r", csv.to_str().expect("csv path should be utf8")],
        None,
    );
    assert!(csv_output.status.success());
    assert_eq!(
        String::from_utf8(csv_output.stdout).expect("stdout should be utf8"),
        "admin\n"
    );

    let tsv = temp_file("rows.tsv", "alice\tadmin\n");
    let tsv_output = run_aq(
        &[".[1]", "-r", tsv.to_str().expect("tsv path should be utf8")],
        None,
    );
    assert!(tsv_output.status.success());
    assert_eq!(
        String::from_utf8(tsv_output.stdout).expect("stdout should be utf8"),
        "admin\n"
    );
}

#[test]
fn supports_tabular_scalar_inference_for_csv_and_tsv_input() {
    let csv_fixture = fixture_text("infer_scalars.csv");
    let tsv_fixture = fixture_text("infer_scalars.tsv");

    let csv_default = run_aq(
        &["--input-format", "csv", ".[] | type", "-r"],
        Some(&csv_fixture),
    );
    assert!(csv_default.status.success());
    assert_eq!(
        String::from_utf8(csv_default.stdout).expect("stdout should be utf8"),
        "string\nstring\nstring\nstring\nstring\nstring\nstring\n"
    );

    let csv_inferred = run_aq(
        &[
            "--input-format",
            "csv",
            "--tabular-coercion",
            "infer-scalars",
            ".[] | type",
            "-r",
        ],
        Some(&csv_fixture),
    );
    assert!(csv_inferred.status.success());
    assert_eq!(
        String::from_utf8(csv_inferred.stdout).expect("stdout should be utf8"),
        "number\nboolean\nnull\nnumber\nstring\nstring\nstring\n"
    );

    let tsv_inferred = run_aq(
        &[
            "--input-format",
            "tsv",
            "--tabular-coercion",
            "infer-scalars",
            ".[] | type",
            "-r",
        ],
        Some(&tsv_fixture),
    );
    assert!(tsv_inferred.status.success());
    assert_eq!(
        String::from_utf8(tsv_inferred.stdout).expect("stdout should be utf8"),
        "number\nboolean\nnull\nnumber\nstring\nstring\nstring\n"
    );
}

#[test]
fn rejects_invalid_csv_input() {
    let output = run_aq(&["--input-format", "csv", "."], Some("\"alice"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("unterminated quoted field"));
}

#[test]
fn rejects_non_row_csv_and_tsv_output_shapes() {
    let csv_output = run_aq(&["-n", "--output-format", "csv", "{name: \"alice\"}"], None);
    assert!(!csv_output.status.success());
    let csv_stderr = String::from_utf8(csv_output.stderr).expect("stderr should be utf8");
    assert!(csv_stderr.contains("csv output requires each result to be an array"));

    let tsv_output = run_aq(&["-n", "--output-format", "tsv", "{name: \"alice\"}"], None);
    assert!(!tsv_output.status.success());
    let tsv_stderr = String::from_utf8(tsv_output.stderr).expect("stderr should be utf8");
    assert!(tsv_stderr.contains("tsv output requires each result to be an array"));
}

#[test]
fn supports_input_and_inputs_builtins_with_null_input() {
    let output = run_aq(
        &[
            "-n",
            "--input-format",
            "jsonl",
            "--output-format",
            "json",
            "input, inputs",
        ],
        Some("1\n2\n3\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n2\n3\n"
    );
}

#[test]
fn supports_input_consuming_the_remaining_main_input_stream() {
    let output = run_aq(
        &[
            ". as $x | [$x, try input catch -1]",
            "--compact",
            "--input-format",
            "jsonl",
            "--output-format",
            "json",
        ],
        Some("1\n2\n3\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2]\n[3,-1]\n"
    );
}

#[test]
fn supports_slurped_inputs_with_null_input() {
    let output = run_aq(
        &[
            "-n",
            "-s",
            "--input-format",
            "jsonl",
            "., inputs",
            "--compact",
            "--output-format",
            "json",
        ],
        Some("1\n2\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "null\n[1,2]\n"
    );
}

#[test]
fn returns_break_exit_code_when_input_reaches_eof() {
    let output = run_aq(&["-n", "input"], None);
    assert_eq!(output.status.code(), Some(5));
    assert_eq!(
        String::from_utf8(output.stderr).expect("stderr should be utf8"),
        "break\n"
    );
}

#[test]
fn supports_try_catch_around_input_eof() {
    let output = run_aq(&["-n", "try input catch \"caught\""], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"caught\"\n"
    );
}

#[test]
fn supports_split_builtin() {
    let output = run_aq(
        &[
            "(.csv | split(\",\")), (.csv | [splits(\"\")])",
            "--compact",
        ],
        Some(r#"{"csv":"a,b"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"a\",\"b\"]\n[\"\",\"a\",\",\",\"b\",\"\"]\n"
    );
}

#[test]
fn supports_min_and_max_builtins() {
    let output = run_aq(
        &["(.items | min), (.items | max)"],
        Some(r#"{"items":[3,1,2]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n3\n"
    );
}

#[test]
fn supports_unique_builtin() {
    let output = run_aq(
        &[".items | unique", "--compact"],
        Some(r#"{"items":[3,1,2,1]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2,3]\n"
    );
}

#[test]
fn supports_flatten_builtin() {
    let output = run_aq(
        &[".items | flatten", "--compact"],
        Some(r#"{"items":[1,[2,[3]],4]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2,3,4]\n"
    );
}

#[test]
fn supports_any_and_all_builtins() {
    let output = run_aq(
        &["(.items | any), (.items | all)"],
        Some(r#"{"items":[true,false,true]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\nfalse\n"
    );
}

#[test]
fn supports_any_and_all_predicates() {
    let output = run_aq(
        &["(.items | any(.active)), (.items | all(.active))"],
        Some(r#"{"items":[{"active":true},{"active":false}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\nfalse\n"
    );
}

#[test]
fn supports_any_and_all_query_forms() {
    let output = run_aq(
        &["(. as $dot | any($dot[]; not)), (. as $dot | all($dot[]; .))"],
        Some(r#"[1,2,3,4,true,false,1,2,3,4,5]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\nfalse\n"
    );

    let output = run_aq(&["-n", "any(true, error; .), all(false, error; .)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\nfalse\n"
    );

    let output = run_aq(&["-n", "[false] | any(not)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );

    let output = run_aq(&["-n", "[] | all(not)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );

    let output = run_aq(&["-n", "[false] | any(not)"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );
}

#[test]
fn supports_join_builtin() {
    let output = run_aq(
        &[".items | join(\",\")", "-r"],
        Some(r#"{"items":["a",2,true,null]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "a,2,true,\n"
    );
}

#[test]
fn supports_join_with_multi_output_separators() {
    let output = run_aq(&["join(\",\",\"/\")", "-r"], Some(r#"["a","b","c","d"]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "a,b,c,d\na/b/c/d\n"
    );
}

#[test]
fn supports_ascii_case_and_trim_builtins() {
    let output = run_aq(
        &[
            "(.name | ascii_downcase), (.name | ascii_upcase), (.name | trim), (.name | ltrim), (.name | rtrim)",
            "-r",
        ],
        Some(r#"{"name":"  AlIce  "}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "  alice  \n  ALICE  \nAlIce\nAlIce  \n  AlIce\n"
    );
}

#[test]
fn supports_to_entries_and_from_entries() {
    let output = run_aq(
        &[".obj | to_entries | from_entries", "--compact"],
        Some(r#"{"obj":{"a":1,"b":2}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":1,\"b\":2}\n"
    );
}

#[test]
fn supports_with_entries_builtin() {
    let output = run_aq(
        &[
            ".obj | with_entries({key: (.key | ascii_upcase), value: .value})",
            "--compact",
        ],
        Some(r#"{"obj":{"a":1,"b":2}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"A\":1,\"B\":2}\n"
    );
}

#[test]
fn supports_sort_by_builtin() {
    let output = run_aq(
        &[".items | sort_by(.name | ascii_downcase)", "--compact"],
        Some(r#"{"items":[{"name":"Bob"},{"name":"alice"}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"name\":\"alice\"},{\"name\":\"Bob\"}]\n"
    );
}

#[test]
fn supports_jq_style_sorting_and_grouping() {
    let sort_output = run_aq(
        &["sort", "--compact", "-o", "json"],
        Some(
            r#"[42,[2,5,3,11],10,{"a":42,"b":2},{"a":42},true,2,[2,6],"hello",null,[2,5,6],{"a":[],"b":1},"abc","ab",[3,10],{},false,"abcd",null]"#,
        ),
    );
    assert!(sort_output.status.success());
    assert_eq!(
        String::from_utf8(sort_output.stdout).expect("stdout should be utf8"),
        r#"[null,null,false,true,2,10,42,"ab","abc","abcd","hello",[2,5,3,11],[2,5,6],[2,6],[3,10],{},{"a":42},{"a":42,"b":2},{"a":[],"b":1}]"#
            .to_string()
            + "\n"
    );

    let grouping_output = run_aq(
        &[
            "(sort_by(.b) | sort_by(.a)), sort_by(.a, .b), sort_by(.b, .c), group_by(.b), group_by(.a + .b - .c == 2)",
            "--compact",
            "-o",
            "json",
        ],
        Some(
            r#"[{"a":1,"b":4,"c":14},{"a":4,"b":1,"c":3},{"a":1,"b":4,"c":3},{"a":0,"b":2,"c":43}]"#,
        ),
    );
    assert!(grouping_output.status.success());
    assert_eq!(
        String::from_utf8(grouping_output.stdout).expect("stdout should be utf8"),
        concat!(
            "[{\"a\":0,\"b\":2,\"c\":43},{\"a\":1,\"b\":4,\"c\":14},{\"a\":1,\"b\":4,\"c\":3},{\"a\":4,\"b\":1,\"c\":3}]\n",
            "[{\"a\":0,\"b\":2,\"c\":43},{\"a\":1,\"b\":4,\"c\":14},{\"a\":1,\"b\":4,\"c\":3},{\"a\":4,\"b\":1,\"c\":3}]\n",
            "[{\"a\":4,\"b\":1,\"c\":3},{\"a\":0,\"b\":2,\"c\":43},{\"a\":1,\"b\":4,\"c\":3},{\"a\":1,\"b\":4,\"c\":14}]\n",
            "[[{\"a\":4,\"b\":1,\"c\":3}],[{\"a\":0,\"b\":2,\"c\":43}],[{\"a\":1,\"b\":4,\"c\":14},{\"a\":1,\"b\":4,\"c\":3}]]\n",
            "[[{\"a\":1,\"b\":4,\"c\":14},{\"a\":0,\"b\":2,\"c\":43}],[{\"a\":4,\"b\":1,\"c\":3},{\"a\":1,\"b\":4,\"c\":3}]]\n"
        )
    );

    let extrema_output = run_aq(
        &[
            "[min, max, min_by(.[1]), max_by(.[1]), min_by(.[2]), max_by(.[2])]",
            "--compact",
            "-o",
            "json",
        ],
        Some(r#"[[4,2,"a"],[3,1,"a"],[2,4,"a"],[1,3,"a"]]"#),
    );
    assert!(extrema_output.status.success());
    assert_eq!(
        String::from_utf8(extrema_output.stdout).expect("stdout should be utf8"),
        "[[1,3,\"a\"],[4,2,\"a\"],[3,1,\"a\"],[2,4,\"a\"],[4,2,\"a\"],[1,3,\"a\"]]\n"
    );
}

#[test]
fn supports_structural_aq_builtins() {
    let output = run_aq(
        &[
            "(. as $doc | $doc.base | merge($doc.overlay; true) | drop_nulls(true) | sort_keys(true)), (.merge_inputs | merge_all(true) | sort_keys(true))",
            "--compact",
        ],
        Some(
            r#"{"base":{"service":{"port":8080,"name":"api"},"flags":[1,null,2],"meta":{"owner":null}},"overlay":{"service":{"port":8443},"meta":{"team":"platform"},"extra":null},"merge_inputs":[{"a":{"x":1},"b":null},{"a":{"y":2},"c":3}]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"flags\":[1,2],\"meta\":{\"team\":\"platform\"},\"service\":{\"name\":\"api\",\"port\":8443}}\n{\"a\":{\"x\":1,\"y\":2},\"b\":null,\"c\":3}\n"
    );
}

#[test]
fn supports_metadata_aq_builtins() {
    let output = run_aq(
        &[
            "(.node | xml_attr), (.node | xml_attr(\"id\")), (.rows | csv_header), (. as $doc | $doc.rows[1] | csv_header($doc.rows[0]))",
            "--compact",
        ],
        Some(
            r#"{"node":{"name":"user","attributes":{"id":"42","role":"admin"}},"rows":[["name","role"],["alice","admin"],["bob","ops"]]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"id\":\"42\",\"role\":\"admin\"}\n\"42\"\n[{\"name\":\"alice\",\"role\":\"admin\"},{\"name\":\"bob\",\"role\":\"ops\"}]\n{\"name\":\"alice\",\"role\":\"admin\"}\n"
    );

    let output = run_aq(
        &["--input-format", "yaml", ".value | yaml_tag", "-r"],
        Some("value: !Thing x\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "!Thing\n"
    );
}

#[test]
fn preserves_yaml_tags_through_updates() {
    let output = run_aq(
        &[
            "--input-format",
            "yaml",
            "--output-format",
            "yaml",
            ".service.port = 8443",
        ],
        Some("service: !Thing\n  name: api\n  port: 8080\n"),
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("!Thing"));
    assert!(stdout.contains("port: 8443"));
}

#[test]
fn supports_group_by_and_unique_by_builtins() {
    let output = run_aq(
        &[
            "(.items | group_by(.kind)), (.items | unique_by(.kind))",
            "--compact",
        ],
        Some(
            r#"{"items":[{"kind":"b","name":"beta"},{"kind":"a","name":"alpha"},{"kind":"b","name":"bravo"}]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[[{\"kind\":\"a\",\"name\":\"alpha\"}],[{\"kind\":\"b\",\"name\":\"beta\"},{\"kind\":\"b\",\"name\":\"bravo\"}]]\n[{\"kind\":\"a\",\"name\":\"alpha\"},{\"kind\":\"b\",\"name\":\"beta\"}]\n"
    );
}

#[test]
fn supports_min_by_and_max_by_builtins() {
    let output = run_aq(
        &[
            "(.items | min_by(.score)), (.items | max_by(.score)), (.empty | min_by(.score)), (.empty | max_by(.score))",
            "--compact",
        ],
        Some(
            r#"{"items":[{"name":"alice","score":7},{"name":"bob","score":3},{"name":"carol","score":9}],"empty":[]}"#,
        ),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"bob\",\"score\":3}\n{\"name\":\"carol\",\"score\":9}\nnull\nnull\n"
    );
}

#[test]
fn supports_dynamic_object_keys() {
    let output = run_aq(
        &["{(.key | ascii_upcase): .value}", "--compact"],
        Some(r#"{"key":"name","value":1}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"NAME\":1}\n"
    );
}

#[test]
fn supports_index_and_rindex() {
    let output = run_aq(
        &["(.text | index(\"na\")), (.text | rindex(\"na\")), (.items | index(2)), (.items | rindex(2))"],
        Some(r#"{"text":"banana","items":[1,2,3,2]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2\n4\n1\n3\n"
    );
}

#[test]
fn supports_ltrimstr_and_rtrimstr() {
    let output = run_aq(
        &[
            "(.text | ltrimstr(\"pre\")), (.text | rtrimstr(\"post\"))",
            "-r",
        ],
        Some(r#"{"text":"prefixpost"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "fixpost\nprefix\n"
    );
}

#[test]
fn reports_jq_style_trimstr_input_errors() {
    let output = run_aq(
        &[
            ".[] as [$x, $y] | try [\"ok\", ($x | ltrimstr($y))] catch [\"ko\", .]",
            "--compact",
        ],
        Some("[[\"hi\",1],[1,\"hi\"],[\"hi\",\"hi\"],[1,1]]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"ko\",\"startswith() requires string inputs\"]\n[\"ko\",\"startswith() requires string inputs\"]\n[\"ok\",\"\"]\n[\"ko\",\"startswith() requires string inputs\"]\n"
    );

    let output = run_aq(
        &[
            ".[] as [$x, $y] | try [\"ok\", ($x | rtrimstr($y))] catch [\"ko\", .]",
            "--compact",
        ],
        Some("[[\"hi\",1],[1,\"hi\"],[\"hi\",\"hi\"],[1,1]]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"ko\",\"endswith() requires string inputs\"]\n[\"ko\",\"endswith() requires string inputs\"]\n[\"ok\",\"\"]\n[\"ko\",\"endswith() requires string inputs\"]\n"
    );
}

#[test]
fn supports_if_then_else() {
    let output = run_aq(
        &["if .active then .name else \"inactive\" end", "-r"],
        Some(r#"{"active":true,"name":"alice"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn supports_if_elif_else() {
    let output = run_aq(
        &[
            "if .kind == \"user\" then .name elif .kind == \"team\" then .team else \"unknown\" end",
            "-r",
        ],
        Some(r#"{"kind":"team","team":"ops"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "ops\n"
    );
}

#[test]
fn supports_if_branch_pipelines_and_multiple_outputs() {
    let output = run_aq(
        &[
            "if .active then .items[] | .name else empty end",
            "--compact",
        ],
        Some(r#"{"active":true,"items":[{"name":"alice"},{"name":"bob"}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn supports_jq_style_if_semantics() {
    let output = run_aq(
        &[
            "[if 1,null,2 then 3 else 4 end], [if empty then 3 else 4 end], [if true then 3 end], [if false then 3 end], [if false then 3 elif false then 4 end]",
            "--compact",
            "-o",
            "json",
        ],
        Some("null"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[3,4,3]\n[]\n[3]\n[null]\n[null]\n"
    );
}

#[test]
fn supports_postfix_after_control_flow_expressions() {
    let output = run_aq(
        &["if true then [.] else . end []", "--compact", "-o", "json"],
        Some("null"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "null\n"
    );
}

#[test]
fn supports_try_precedence_and_optional_postfix() {
    let output = run_aq(
        &[
            "try error(0) // 1, 1 + try 2 catch 3 + 4, [.[]|(.a, .a)?], [[.[]|[.a,.a]]?]",
            "--compact",
            "-o",
            "json",
        ],
        Some("[null,true,{\"a\":1}]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n7\n[null,null,1,1]\n[]\n"
    );

    let root_output = run_aq(
        &["try -.? catch .", "--compact", "-o", "json"],
        Some("\"foo\""),
    );
    assert!(root_output.status.success());
    assert_eq!(
        String::from_utf8(root_output.stdout).expect("stdout should be utf8"),
        "\"string (\\\"foo\\\") cannot be negated\"\n"
    );

    let alt_output = run_aq(
        &["[.[] | [.foo[] // .bar]]", "--compact", "-o", "json"],
        Some(
            r#"[{"foo":[1,2],"bar":42},{"foo":[1],"bar":null},{"foo":[null,false,3],"bar":18},{"foo":[],"bar":42},{"foo":[null,false,null],"bar":41}]"#,
        ),
    );
    assert!(alt_output.status.success());
    assert_eq!(
        String::from_utf8(alt_output.stdout).expect("stdout should be utf8"),
        "[[1,2],[1],[3],[42],[41]]\n"
    );
}

#[test]
fn supports_trimstr_and_scalar_array_indices() {
    let output = run_aq(
        &[
            "[.[]|trimstr(\"foo\")], indices(1)",
            "--compact",
            "-o",
            "json",
        ],
        Some("[\"fo\",\"foo\",\"barfoo\",\"foobarfoo\",\"foob\"]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"fo\",\"\",\"bar\",\"bar\",\"b\"]\n[]\n"
    );

    let index_output = run_aq(
        &["indices(1)", "--compact", "-o", "json"],
        Some("[0,1,1,2,3,4,1,5]"),
    );
    assert!(index_output.status.success());
    assert_eq!(
        String::from_utf8(index_output.stdout).expect("stdout should be utf8"),
        "[1,2,6]\n"
    );

    let unicode_output = run_aq(
        &[
            "index(\"!\"), rindex(\"в\"), indices(\"в\")",
            "--compact",
            "-o",
            "json",
        ],
        Some("\"здравствуй мир!\""),
    );
    assert!(unicode_output.status.success());
    assert_eq!(
        String::from_utf8(unicode_output.stdout).expect("stdout should be utf8"),
        "14\n7\n[4,7]\n"
    );

    let trim_error_output = run_aq(
        &[
            "try trim catch ., try ltrim catch ., try rtrim catch .",
            "--compact",
            "-o",
            "json",
        ],
        Some("123"),
    );
    assert!(trim_error_output.status.success());
    assert_eq!(
        String::from_utf8(trim_error_output.stdout).expect("stdout should be utf8"),
        "\"trim input must be a string\"\n\"trim input must be a string\"\n\"trim input must be a string\"\n"
    );
}

#[test]
fn supports_string_repetition() {
    let string_output = run_aq(
        &["[.[] * 3]", "--compact", "-o", "json"],
        Some("[\"a\",\"ab\",\"abc\"]"),
    );
    assert!(string_output.status.success());
    assert_eq!(
        String::from_utf8(string_output.stdout).expect("stdout should be utf8"),
        "[\"aaa\",\"ababab\",\"abcabcabc\"]\n"
    );

    let numeric_output = run_aq(
        &["[.[] * \"abc\"]", "--compact", "-o", "json"],
        Some("[-1.0,-0.5,0.0,0.5,1.0,1.5,3.7,10.0]"),
    );
    assert!(numeric_output.status.success());
    assert_eq!(
        String::from_utf8(numeric_output.stdout).expect("stdout should be utf8"),
        "[null,null,\"\",\"\",\"abc\",\"abc\",\"abcabcabc\",\"abcabcabcabcabcabcabcabcabcabc\"]\n"
    );

    let nan_output = run_aq(
        &["[. * (nan,-nan)]", "--compact", "-o", "json"],
        Some("\"abc\""),
    );
    assert!(nan_output.status.success());
    assert_eq!(
        String::from_utf8(nan_output.stdout).expect("stdout should be utf8"),
        "[null,null]\n"
    );

    let large_output = run_aq(
        &[". * 100000 | [.[:10], .[-10:]]", "--compact", "-o", "json"],
        Some("\"abc\""),
    );
    assert!(large_output.status.success());
    assert_eq!(
        String::from_utf8(large_output.stdout).expect("stdout should be utf8"),
        "[\"abcabcabca\",\"cabcabcabc\"]\n"
    );

    let unicode_output = run_aq(
        &[". * 5 | [.[:4], .[-4:], .[1:7]]", "--compact", "-o", "json"],
        Some("\"muμ\""),
    );
    assert!(unicode_output.status.success());
    assert_eq!(
        String::from_utf8(unicode_output.stdout).expect("stdout should be utf8"),
        "[\"muμm\",\"μmuμ\",\"uμmuμm\"]\n"
    );

    let too_large_output = run_aq(
        &["try (. * 1000000000) catch .", "--compact", "-o", "json"],
        Some("\"abc\""),
    );
    assert!(too_large_output.status.success());
    assert_eq!(
        String::from_utf8(too_large_output.stdout).expect("stdout should be utf8"),
        "\"Repeat string result too long\"\n"
    );

    let too_large_sliced_output = run_aq(
        &[
            "try (. * 1000000000 | [.[:10], .[-10:]]) catch .",
            "--compact",
            "-o",
            "json",
        ],
        Some("\"abc\""),
    );
    assert!(too_large_sliced_output.status.success());
    assert_eq!(
        String::from_utf8(too_large_sliced_output.stdout).expect("stdout should be utf8"),
        "\"Repeat string result too long\"\n"
    );
}

#[test]
fn reports_jq_style_field_access_errors() {
    let output = run_aq(&["try .a catch .", "--compact", "-o", "json"], Some("1"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"Cannot index number with string (\\\"a\\\")\"\n"
    );
}

#[test]
fn supports_try_without_catch() {
    let output = run_aq(&["try length"], Some("1"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n"
    );
}

#[test]
fn supports_try_catch() {
    let output = run_aq(&["try length catch .", "--compact"], Some("1"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n"
    );
}

#[test]
fn supports_try_catch_with_parenthesized_body() {
    let output = run_aq(&["-n", "try (1 / 0) catch \"fallback\"", "-r"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "fallback\n"
    );
}

#[test]
fn supports_error_builtin_with_try_catch() {
    let output = run_aq(
        &[
            "-n",
            "try error(\"boom\") catch ., try error catch ., try error({a: 1}) catch .",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"boom\"\nnull\n{\"a\":1}\n"
    );
}

#[test]
fn reports_uncaught_error_builtin() {
    let output = run_aq(&["-n", "error({a: 1})"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("(not a string): {\"a\":1}"));
}

#[test]
fn supports_builtins_builtin() {
    let output = run_aq(
        &[
            "-n",
            "builtins | map(select(startswith(\"error/\") or startswith(\"flatten/\") or startswith(\"range/\") or startswith(\"builtins/\")))",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"builtins/0\",\"error/0\",\"error/1\",\"flatten/0\",\"flatten/1\",\"range/1\",\"range/2\",\"range/3\"]\n"
    );
}

#[test]
fn supports_debug_builtin() {
    let output = run_aq(
        &["-n", "(1 | debug), (1 | debug(\"tag\"))", "--compact"],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n1\n"
    );
    assert_eq!(
        String::from_utf8(output.stderr).expect("stderr should be utf8"),
        "[\"DEBUG:\",1]\n[\"DEBUG:\",\"tag\"]\n"
    );
}

#[test]
fn supports_combinations_and_bsearch() {
    let output = run_aq(
        &[
            "-n",
            "([[1,2],{\"a\":3,\"b\":4}] | combinations), ([1,2] | combinations(0.5)), ([1,3,5] | bsearch(3)), ([1,3,5] | bsearch(4)), ([1,2,3] | bsearch(0,1,2,3,4)), (try (\"aa\" | [\"OK\", bsearch(0)]) catch [\"KO\",.])",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,3]\n[1,4]\n[2,3]\n[2,4]\n[1]\n[2]\n1\n-3\n-1\n0\n1\n2\n-4\n[\"KO\",\"string (\\\"aa\\\") cannot be searched from\"]\n"
    );
}

#[test]
fn rejects_invalid_combinations_inputs() {
    let output = run_aq(&["-n", "[1,2] | combinations"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("Cannot iterate over number (1)"));
}

#[test]
fn supports_keys_and_keys_unsorted() {
    let output = run_aq(
        &["(.obj | keys), (.obj | keys_unsorted)", "--compact"],
        Some(r#"{"obj":{"b":1,"a":2}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"a\",\"b\"]\n[\"b\",\"a\"]\n"
    );
}

#[test]
fn supports_utf8bytelength() {
    let output = run_aq(&[".text | utf8bytelength"], Some(r#"{"text":"aé"}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n"
    );
}

#[test]
fn reports_jq_style_utf8bytelength_errors() {
    let output = run_aq(
        &["[.[] | try utf8bytelength catch .]", "--compact"],
        Some(r#"[[],{},[1,2],55,true,false]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"array ([]) only strings have UTF-8 byte length\",\"object ({}) only strings have UTF-8 byte length\",\"array ([1,2]) only strings have UTF-8 byte length\",\"number (55) only strings have UTF-8 byte length\",\"boolean (true) only strings have UTF-8 byte length\",\"boolean (false) only strings have UTF-8 byte length\"]\n"
    );
}

#[test]
fn supports_tojson_and_fromjson() {
    let output = run_aq(
        &[".raw | fromjson | tojson", "-r"],
        Some(r#"{"raw":"{\"a\":1}"}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":1}\n"
    );
}

#[test]
fn supports_jq_nonstandard_json_numbers() {
    let output = run_aq(
        &[".[] = 1", "--compact"],
        Some("[1,null,Infinity,-Infinity,NaN,-NaN]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,1,1,1,1,1]\n"
    );

    let output = run_aq(&["fromjson | isnan"], Some("\"nan\""));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\n"
    );

    let output = run_aq(&["tojson | fromjson", "--compact"], Some("{\"a\":nan}"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":null}\n"
    );

    let output = run_aq(
        &[".[] | try (fromjson | isnan) catch .", "--compact"],
        Some("[\"NaN\",\"-NaN\",\"NaN1\",\"NaN10\",\"NaN100\",\"NaN1000\",\"NaN10000\",\"NaN100000\"]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "true\ntrue\n\"Invalid numeric literal at EOF at line 1, column 4 (while parsing 'NaN1')\"\n\"Invalid numeric literal at EOF at line 1, column 5 (while parsing 'NaN10')\"\n\"Invalid numeric literal at EOF at line 1, column 6 (while parsing 'NaN100')\"\n\"Invalid numeric literal at EOF at line 1, column 7 (while parsing 'NaN1000')\"\n\"Invalid numeric literal at EOF at line 1, column 8 (while parsing 'NaN10000')\"\n\"Invalid numeric literal at EOF at line 1, column 9 (while parsing 'NaN100000')\"\n"
    );

    let output = run_aq(
        &["map(try implode catch .)", "--compact"],
        Some("[123,[\"a\"],[nan]]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"implode input must be an array\",\"string (\\\"a\\\") can't be imploded, unicode codepoint needs to be numeric\",\"number (null) can't be imploded, unicode codepoint needs to be numeric\"]\n"
    );

    let output = run_aq(
        &["try fromjson catch .", "--compact"],
        Some("\"{'a': 123}\""),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "'Invalid string literal; expected \", but got '' at line 1, column 5 (while parsing ''{''a'': 123}'')'\n"
    );
}

#[test]
fn supports_explode_and_implode() {
    let output = run_aq(
        &["(.text | explode), (.codes | implode)", "--compact"],
        Some(r#"{"text":"Aé","codes":[65,233]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[65,233]\n\"Aé\"\n"
    );
}

#[test]
fn supports_jq_implode_and_tonumber_compat() {
    let output = run_aq(
        &["implode|explode", "--compact"],
        Some("[-1,0,1,2,3,1114111,1114112,55295,55296,57343,57344,1.1,1.9]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[65533,0,1,2,3,1114111,65533,55295,65533,65533,57344,1,1]\n"
    );

    let output = run_aq(
        &[".[] |= try tonumber", "--compact"],
        Some("[\"1\", \"2a\", \"3\", \" 4\", \"5 \", \"6.7\", \".89\", \"-876\", \"+5.43\", 21]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,3,6.7,0.89,-876,5.43,21]\n"
    );
}

#[test]
fn supports_quoted_field_names() {
    let output = run_aq(&[".\"foo-bar\""], Some(r#"{"foo-bar":1}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n"
    );
}

#[test]
fn supports_bracket_string_field_access() {
    let output = run_aq(&[".[\"foo-bar\"]"], Some(r#"{"foo-bar":1}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n"
    );
}

#[test]
fn supports_negative_indices() {
    let output = run_aq(&[".[-1]"], Some(r#"[1,2,3]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n"
    );
}

#[test]
fn supports_slices() {
    let output = run_aq(&[".[1:3]", "--compact"], Some(r#"[1,2,3,4]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[2,3]\n"
    );
}

#[test]
fn supports_negative_slice_bounds() {
    let output = run_aq(&[".[-2:]", "--compact"], Some(r#"[1,2,3,4]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[3,4]\n"
    );
}

#[test]
fn supports_dynamic_slice_bounds() {
    let output = run_aq(&[".[:rindex(\"x\")]"], Some(r#""正xyz""#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "正\n"
    );

    let output = run_aq(&[".[1.5:3.5]", "--compact"], Some(r#"[0,1,2,3,4]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2,3]\n"
    );

    let output = run_aq(&[".[nan:1], .[1:nan]", "--compact"], Some(r#"[0,1,2]"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0]\n[1,2]\n"
    );
}

#[test]
fn reports_numeric_lookup_errors_with_rendered_indexes() {
    let output = run_aq(&["-n", "try (\"foobar\" | .[1.5]) catch ."], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"Cannot index string with number (1.5)\"\n"
    );

    let output = run_aq(&["try 0[implode] catch ."], Some("[]"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"Cannot index number with string (\\\"\\\")\"\n"
    );

    let output = run_aq(&["-n", "try ([range(3)] | .[nan] = 9) catch ."], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"Cannot set array element at NaN index\"\n"
    );
}

#[test]
fn supports_slice_delete_and_assignment_paths() {
    let output = run_aq(
        &["del(.[2:4],.[0],.[-2:])", "--compact"],
        Some(r#"[0,1,2,3,4,5,6,7]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,4,5]\n"
    );

    let output = run_aq(
        &[
            ".[2:4] = ([], [\"a\",\"b\"], [\"a\",\"b\",\"c\"])",
            "--compact",
        ],
        Some(r#"[0,1,2,3,4,5,6,7]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1,4,5,6,7]\n[0,1,\"a\",\"b\",4,5,6,7]\n[0,1,\"a\",\"b\",\"c\",4,5,6,7]\n"
    );

    let output = run_aq(
        &[".[1.5:3.5] = [\"x\"]", "--compact"],
        Some(r#"[0,1,2,3,4,5]"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,\"x\",4,5]\n"
    );
}

#[test]
fn optional_access_omits_results_on_type_errors() {
    let output = run_aq(&[".foo?"], Some("1"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
}

#[test]
fn optional_access_preserves_null_inputs() {
    let output = run_aq(&[".foo?"], Some("null"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "null\n"
    );
}

#[test]
fn iterate_over_null_errors_without_optional() {
    let output = run_aq(&[".[]"], Some("null"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("Cannot iterate over null (null)"));
}

#[test]
fn supports_scientific_notation_literals() {
    let output = run_aq(
        &[
            "-n",
            "1e+0+0.001e3, 1e-19 + 1e-20 - 5e-21, 1 / 1e-17",
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "2\n1.05e-19\n1e+17\n"
    );
}

#[test]
fn rejects_excessive_array_growth_in_assignment() {
    let output = run_aq(&["-n", ".[999999999] = 0"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("Array index too large"));
}

#[test]
fn supports_path_builtin() {
    let exact_output = run_aq(
        &[
            "-n",
            "path(.a), path(.a[0]), path(.a | .b), path(.a?), path(.a[1:2]), path(.[1,2])",
            "--compact",
        ],
        None,
    );
    assert!(exact_output.status.success());
    assert_eq!(
        String::from_utf8(exact_output.stdout).expect("stdout should be utf8"),
        "[\"a\"]\n[\"a\",0]\n[\"a\",\"b\"]\n[\"a\"]\n[\"a\",{\"start\":1,\"end\":2}]\n[1]\n[2]\n"
    );

    let pattern_output = run_aq(
        &["path(.a[].b)", "--compact"],
        Some(r#"{"a":[{"b":1},{"b":2}]}"#),
    );
    assert!(pattern_output.status.success());
    assert_eq!(
        String::from_utf8(pattern_output.stdout).expect("stdout should be utf8"),
        "[\"a\",0,\"b\"]\n[\"a\",1,\"b\"]\n"
    );
}

#[test]
fn reports_invalid_path_expression_details() {
    let output = run_aq(
        &[
            "try path(length) catch .,
             try path(.a | map(select(.b == 0))) catch .,
             try path(.a | map(select(.b == 0)) | .[0]) catch .,
             try path(.a | map(select(.b == 0)) | .c) catch .,
             try path(.a | map(select(.b == 0)) | .[]) catch .",
            "--compact",
        ],
        Some(r#"{"a":[{"b":0}]}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"Invalid path expression with result 1\"\n\"Invalid path expression with result [{\\\"b\\\":0}]\"\n\"Invalid path expression near attempt to access element 0 of [{\\\"b\\\":0}]\"\n\"Invalid path expression near attempt to access element \\\"c\\\" of [{\\\"b\\\":0}]\"\n\"Invalid path expression near attempt to iterate through [{\\\"b\\\":0}]\"\n"
    );
}

#[test]
fn supports_nested_dynamic_path_queries_and_pick_last_errors() {
    let output = run_aq(
        &["path(.a[path(.b)[0]]), try pick(last) catch .", "--compact"],
        Some(r#"{"a":{"b":0}}"#),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"a\",\"b\"]\n\"last is not defined for object\"\n"
    );

    let array_output = run_aq(
        &["path(last), try pick(last) catch .", "--compact"],
        Some("[1,2]"),
    );
    assert!(array_output.status.success());
    assert_eq!(
        String::from_utf8(array_output.stdout).expect("stdout should be utf8"),
        "[-1]\n\"Out of bounds negative array index\"\n"
    );
}

#[test]
fn supports_descending_array_path_updates() {
    let output = run_aq(
        &[
            "(.[] | select(. >= 2)) |= empty, .[] |= select(. % 2 == 0)",
            "--compact",
        ],
        Some("[0,1,2,3,4,5]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1]\n[0,2,4]\n"
    );

    let object_output = run_aq(
        &[".foo[1,4,2,3] |= empty", "--compact"],
        Some(r#"{"foo":[0,1,2,3,4,5]}"#),
    );
    assert!(object_output.status.success());
    assert_eq!(
        String::from_utf8(object_output.stdout).expect("stdout should be utf8"),
        "{\"foo\":[0,5]}\n"
    );
}

#[test]
fn update_assignments_use_only_the_first_rhs_result() {
    let output = run_aq(
        &[
            "-n",
            "(null | .a |= range(3)), (null | (.a, .b) |= range(3))",
            "--compact",
            "--output-format",
            "json",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"a\":0}\n{\"a\":0,\"b\":0}\n"
    );
}

#[test]
fn emits_multiple_results_separately() {
    let output = run_aq(&[".items[]"], Some(r#"{"items":[1,2,3]}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n2\n3\n"
    );
}

#[test]
fn supports_null_input_with_literals() {
    let output = run_aq(&["-n", "\"hello\", 42, true, null", "--compact"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"hello\"\n42\ntrue\nnull\n"
    );
}

#[test]
fn supports_null_input_with_object_constructor() {
    let output = run_aq(&["-n", "{name: \"alice\", age: 30}", "--compact"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\",\"age\":30}\n"
    );
}

#[test]
fn supports_raw_input() {
    let output = run_aq(&["-R", "."], Some("alice\nbob\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn supports_raw_input_with_raw_output() {
    let output = run_aq(&["-R", "-r", "."], Some("alice\nbob\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\nbob\n"
    );
}

#[test]
fn supports_raw_input_with_slurp() {
    let output = run_aq(&["-R", "--slurp", ".", "--compact"], Some("alice\nbob\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[\"alice\",\"bob\"]\n"
    );
}

#[test]
fn supports_sniffed_jsonl_input_from_stdin() {
    let output = run_aq(
        &[".name", "--compact"],
        Some("{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn supports_jsonl_streaming() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", ".name", "--compact"],
        Some("{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn supports_jsonl_streaming_with_sniffed_stdin() {
    let output = run_aq(
        &["--stream", ".name", "--compact"],
        Some("{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn supports_jsonl_streaming_with_explicit_stdin_sentinel() {
    let output = run_aq(
        &["--stream", ".name", "--compact", "-"],
        Some("{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn rejects_repeated_explicit_stdin_sentinels() {
    let output = run_aq(&[".", "-", "-"], Some(r#"{"name":"alice"}"#));
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("stdin may be referenced at most once with `-`"));
}

#[test]
fn rejects_in_place_with_explicit_stdin_sentinel() {
    let output = run_aq(&["--in-place", ".", "-"], Some(r#"{"name":"alice"}"#));
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be utf8")
        .contains("--in-place does not support `-`"));
}

#[test]
fn supports_jsonl_streaming_from_single_record_jsonl_file() {
    let input = temp_file("single-record.jsonl", "{\"name\":\"alice\"}\n");
    let output = run_aq(
        &[
            "--stream",
            ".name",
            input.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n"
    );
}

#[test]
fn supports_raw_input_streaming() {
    let output = run_aq(&["--stream", "-R", "-r", "."], Some("alice\nbob\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\nbob\n"
    );
}

#[test]
fn supports_jsonl_streaming_csv_output() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", "-o", "csv", "[.name]"],
        Some("{\"name\":\"alice\"}\n{\"name\":\"bob\"}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n\"bob\"\n"
    );
}

#[test]
fn supports_streaming_per_record_reduce() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", "reduce .[] as $x (0; . + $x)"],
        Some("[1,2]\n[3,4]\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "3\n7\n"
    );
}

#[test]
fn supports_streaming_per_record_group_by() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", "group_by(.)", "--compact"],
        Some("[1,2,1]\n[3,3,2]\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[[1,1],[2]]\n[[2],[3,3]]\n"
    );
}

#[test]
fn supports_streaming_with_args() {
    let output = run_aq(
        &[
            "--stream",
            "-f",
            "jsonl",
            "--args",
            "$ARGS.positional[0]",
            "foo",
        ],
        Some("{\"name\":\"alice\"}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"foo\"\n"
    );
}

#[test]
fn supports_streaming_with_jsonargs() {
    let output = run_aq(
        &[
            "--stream",
            "-f",
            "jsonl",
            "--jsonargs",
            "$ARGS.positional[0]",
            "1",
        ],
        Some("{\"name\":\"alice\"}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "1\n"
    );
}

#[test]
fn stream_rejects_input_builtin_queries() {
    let output = run_aq(&["--stream", "input"], Some("{\"name\":\"alice\"}\n"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("use `input`"));
    assert!(stderr.contains("rerun without --stream"));
}

#[test]
fn stream_rejects_inputs_builtin_in_nested_definitions() {
    let output = run_aq(
        &["--stream", "def rest: inputs; rest"],
        Some("{\"name\":\"alice\"}\n"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("use `inputs`"));
    assert!(stderr.contains("rerun without --stream"));
}

#[test]
fn stream_rejects_slurp() {
    let output = run_aq(&["--stream", "--slurp", "."], Some("{}"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("cannot combine --stream with --slurp"));
}

#[test]
fn stream_rejects_null_input() {
    let output = run_aq(&["--stream", "-n", "."], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("does not support --null-input"));
    assert!(stderr.contains("rerun without --stream"));
}

#[test]
fn stream_rejects_non_line_oriented_input() {
    let output = run_aq(&["--stream", "."], Some("{\"name\":\"alice\"}"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("supports only jsonl input or --raw-input"));
    assert!(stderr.contains("rerun without --stream"));
    assert!(stderr.contains("pass -f jsonl"));
}

#[test]
fn stream_rejects_csv_input_with_actionable_hint() {
    let output = run_aq(&["--stream", "-f", "csv", "."], Some("alice,admin\n"));
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("supports only jsonl input or --raw-input"));
    assert!(stderr.contains("not csv"));
    assert!(stderr.contains("convert the input to jsonl first"));
}

#[test]
fn stream_ignores_blank_only_input() {
    let output = run_aq(&["--stream", "."], Some("\n \n\t\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        ""
    );
}

#[test]
fn stream_reports_jsonl_parse_line_numbers() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", "."],
        Some("{\"name\":\"alice\"}\n{\"name\":\n"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("failed to parse jsonl input: line 2:"));
}

#[test]
fn stream_reports_unsupported_input_file_context() {
    let input = temp_file("plain-json.json", "{\"name\":\"alice\"}\n");
    let output = run_aq(
        &[
            "--stream",
            ".",
            input.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("supports only jsonl input or --raw-input"));
    assert!(stderr.contains("plain-json.json"));
    assert!(stderr.contains("pass -f jsonl"));
}

#[test]
fn stream_reports_jsonl_parse_file_context() {
    let input = temp_file("bad-stream.jsonl", "{\"name\":\"alice\"}\n{\"name\":\n");
    let output = run_aq(
        &[
            "--stream",
            "-f",
            "jsonl",
            ".",
            input.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("failed to parse jsonl input:"));
    assert!(stderr.contains("bad-stream.jsonl"));
    assert!(stderr.contains("line 2:"));
}

#[test]
fn stream_reports_output_error_file_context() {
    let input = temp_file("bad-output.jsonl", "{\"name\":\"alice\"}\n");
    let output = run_aq(
        &[
            "--stream",
            "-f",
            "jsonl",
            "-o",
            "csv",
            ".",
            input.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("stream result at"));
    assert!(stderr.contains("bad-output.jsonl"));
    assert!(stderr.contains("line 1"));
    assert!(stderr.contains("csv output requires each result to be an array"));
}

#[test]
fn stream_rejects_yaml_output() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", "-o", "yaml", "."],
        Some("{\"name\":\"alice\"}\n"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("does not support yaml output"));
    assert!(stderr.contains("rerun without --stream"));
}

#[test]
fn stream_rejects_toml_output() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", "-o", "toml", "."],
        Some("{\"name\":\"alice\"}\n"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("does not support toml output"));
    assert!(stderr.contains("rerun without --stream"));
}

#[test]
fn stream_rejects_table_output() {
    let output = run_aq(
        &["--stream", "-f", "jsonl", "-o", "table", "."],
        Some("{\"name\":\"alice\"}\n"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("does not support table output"));
    assert!(stderr.contains("rerun without --stream"));
}

#[test]
fn supports_yaml_input() {
    let output = run_aq(&["-f", "yaml", ".name"], Some("name: alice\n"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn supports_toml_input() {
    let output = run_aq(
        &[
            "-f",
            "toml",
            "--output-format",
            "json",
            "--compact",
            ".name",
        ],
        Some("name = \"alice\"\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"alice\"\n"
    );
}

#[test]
fn autodetects_cargo_manifest_toml_from_section_header_files() {
    let path = temp_file(
        "Cargo.toml",
        "[package]\nname = \"aq\"\nversion = \"0.1.0\"\n",
    );
    let output = run_aq(
        &[
            "--output-format",
            "json",
            "--compact",
            ".package.version",
            &path.display().to_string(),
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"0.1.0\"\n"
    );
    assert_eq!(
        String::from_utf8(output.stderr).expect("stderr should be utf8"),
        ""
    );
}

#[test]
fn supports_raw_output() {
    let output = run_aq(&["-r", ".name"], Some(r#"{"name":"alice"}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
}

#[test]
fn pretty_prints_json_by_default() {
    let output = run_aq(&["."], Some(r#"{"name":"alice","count":2}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\n  \"name\": \"alice\",\n  \"count\": 2\n}\n"
    );
}

#[test]
fn compact_output_condenses_json() {
    let output = run_aq(&["-c", "."], Some(r#"{"name":"alice","count":2}"#));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"name\":\"alice\",\"count\":2}\n"
    );
}

#[test]
fn pretty_prints_multiple_json_results_when_not_compact() {
    let output = run_aq(
        &["-f", "jsonl", "-o", "json", "."],
        Some("{\"a\":1}\n{\"b\":2}\n"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\n  \"a\": 1\n}\n{\n  \"b\": 2\n}\n"
    );
}

#[test]
fn slurps_multiple_files() {
    let left = temp_file("left.json", r#"{"name":"alice"}"#);
    let right = temp_file("right.json", r#"{"name":"bob"}"#);
    let output = run_aq(
        &[
            "--slurp",
            ".",
            left.to_str().expect("left path should be utf8"),
            right.to_str().expect("right path should be utf8"),
            "--compact",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[{\"name\":\"alice\"},{\"name\":\"bob\"}]\n"
    );
    fs::remove_file(left).expect("left temp should delete");
    fs::remove_file(right).expect("right temp should delete");
}

#[test]
fn falls_back_when_extension_and_content_disagree() {
    let path = temp_file("mismatch.json", "name: alice\n");
    let output = run_aq(
        &[".name", path.to_str().expect("path should be utf8")],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("warning: detected content as"));
    fs::remove_file(path).expect("temp should delete");
}

#[test]
fn detect_conflicts_extension_policy_prefers_extension() {
    let path = temp_file("mismatch.json", "name: alice\n");
    let output = run_aq(
        &[
            "--detect-conflicts",
            "extension",
            ".name",
            path.to_str().expect("path should be utf8"),
        ],
        None,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("failed to parse json input"));
    fs::remove_file(path).expect("temp should delete");
}

#[test]
fn detect_conflicts_env_can_force_sniff_without_warning() {
    let path = temp_file("mismatch.json", "name: alice\n");
    let output = run_aq_with_env(
        &[".name", path.to_str().expect("path should be utf8")],
        None,
        &[("AQ_DETECT_CONFLICTS", "sniff")],
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "alice\n"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.is_empty());
    fs::remove_file(path).expect("temp should delete");
}

#[test]
fn supports_upstream_module_imports_from_working_directory() {
    let dir = temp_dir("upstream-modules");
    write_upstream_module_fixtures(&dir);
    let output = run_aq_in_dir(
        &[
            "--null-input",
            "--compact",
            "import \"c\" as foo; [foo::a, foo::c]",
        ],
        None,
        &dir,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,\"acmehbah\"]\n"
    );
}

#[test]
fn supports_modulemeta_for_upstream_module_fixtures() {
    let dir = temp_dir("modulemeta");
    write_upstream_module_fixtures(&dir);
    let output = run_aq_in_dir(
        &["--input-format", "json", "--compact", "modulemeta"],
        Some("\"c\"\n"),
        &dir,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"whatever\":null,\"deps\":[{\"as\":\"foo\",\"is_data\":false,\"relpath\":\"a\"},{\"search\":\"./\",\"as\":\"d\",\"is_data\":false,\"relpath\":\"d\"},{\"search\":\"./\",\"as\":\"d2\",\"is_data\":false,\"relpath\":\"d\"},{\"search\":\"./../lib/jq\",\"as\":\"e\",\"is_data\":false,\"relpath\":\"e\"},{\"search\":\"./../lib/jq\",\"as\":\"f\",\"is_data\":false,\"relpath\":\"f\"},{\"as\":\"d\",\"is_data\":true,\"relpath\":\"data\"}],\"defs\":[\"a/0\",\"c/0\"]}\n"
    );
}

#[test]
fn supports_label_and_break_control_flow() {
    let output = run_aq(
        &[
            "--null-input",
            "--compact",
            "[ label $if | range(10) | ., (select(. == 5) | break $if) ]",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[0,1,2,3,4,5]\n"
    );
}

#[test]
fn supports_try_preserving_prior_outputs() {
    let output = run_aq(
        &[
            "--null-input",
            "--compact",
            "try ([\"hi\",\"ho\"]|.[]|(try . catch (if .==\"ho\" then \"BROKEN\"|error else empty end)) | if .==\"ho\" then error else \"\\(.) there!\" end) catch \"caught outside \\(.)\"",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"hi there!\"\n\"caught outside ho\"\n"
    );
}

#[test]
fn preserves_prior_stdout_before_late_error() {
    let output = run_aq(
        &[
            ".[]|(try . catch (if .==\"ho\" then \"BROKEN\"|error else empty end)) | if .==\"ho\" then error else \"\\(.) there!\" end",
            "--compact",
        ],
        Some("[\"hi\",\"ho\"]"),
    );
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"hi there!\"\n"
    );
    assert_eq!(
        String::from_utf8(output.stderr).expect("stderr should be utf8"),
        "ho\n"
    );
}

#[test]
fn supports_jq_object_binding_and_location_compat() {
    let output = run_aq(
        &[
            ". as {as: $kw, \"str\": $str, (\"e\"+\"x\"+\"p\"): $exp} | [$kw, $str, $exp]",
            "--compact",
        ],
        Some("{\"as\": 1, \"str\": 2, \"exp\": 3}"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,2,3]\n"
    );

    let output = run_aq(
        &[". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]", "--compact"],
        Some("{\"a\":1, \"b\":[2,{\"d\":3}]}"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "[1,[2,{\"d\":3}],2,{\"d\":3}]\n"
    );

    let output = run_aq(
        &[
            "1 as $x | \"2\" as $y | \"3\" as $z | { $x, as, $y: 4, ($z): 5, if: 6, foo: 7 }",
            "--compact",
        ],
        Some("{\"as\":8}"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "{\"x\":1,\"as\":8,\"2\":4,\"3\":5,\"if\":6,\"foo\":7}\n"
    );

    let output = run_aq(
        &[
            "try error(\"\\($__loc__)\") catch ., { a, $__loc__, c }",
            "--compact",
        ],
        Some("{\"a\":[1,2,3],\"b\":\"foo\",\"c\":{\"hi\":\"hey\"}}"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "\"{\\\"file\\\":\\\"<top-level>\\\",\\\"line\\\":1}\"\n{\"a\":[1,2,3],\"__loc__\":{\"file\":\"<top-level>\",\"line\":1},\"c\":{\"hi\":\"hey\"}}\n"
    );
}

#[test]
fn reports_jq_style_join_and_zero_division_errors() {
    let output = run_aq(
        &["try join(\",\") catch .", "-r"],
        Some("[\"1\",\"2\",{\"a\":{\"b\":{\"c\":33}}}]"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "string (\"1,2,\") and object ({\"a\":{\"b\":{\"c\":33}}}) cannot be added\n"
    );

    let output = run_aq(&["try (1/.) catch .", "-r"], Some("0"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "number (1) and number (0) cannot be divided because the divisor is zero\n"
    );

    let output = run_aq(&["try (1%.) catch .", "-r"], Some("0"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "number (1) and number (0) cannot be divided (remainder) because the divisor is zero\n"
    );
}

#[test]
fn reports_jq_style_truncated_type_errors() {
    let output = run_aq(
        &["try -. catch .", "-r"],
        Some("\"very-long-long-long-long-string\""),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "string (\"very-long-long-long-long...\") cannot be negated\n"
    );

    let output = run_aq(
        &[
            "-n",
            "\"x\" * range(0; 12; 2) + \"☆\" * 8 | try -. catch .",
            "-r",
        ],
        None,
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "string (\"☆☆☆☆☆☆☆☆\") cannot be negated\nstring (\"xx☆☆☆☆☆☆☆☆\") cannot be negated\nstring (\"xxxx☆☆☆☆☆☆...\") cannot be negated\nstring (\"xxxxxx☆☆☆☆☆☆...\") cannot be negated\nstring (\"xxxxxxxx☆☆☆☆☆...\") cannot be negated\nstring (\"xxxxxxxxxx☆☆☆☆...\") cannot be negated\n"
    );

    let output = run_aq(
        &["-f", "json", "-r", "try (. + \"x\") catch ."],
        Some("123456789012345678901234567890"),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "number (12345678901234567890123456...) and string (\"x\") cannot be added\n"
    );
}

#[test]
fn supports_jq_isempty_short_circuit() {
    let output = run_aq(&["-n", "isempty(1,error(\"foo\"))"], None);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout should be utf8"),
        "false\n"
    );
}
