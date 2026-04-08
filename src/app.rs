#[cfg(feature = "starlark")]
use std::borrow::Cow;
use std::ffi::OsString;
use std::io::Write;
use std::io::{BufRead, BufReader, IsTerminal};
use std::path::{Path, PathBuf};
#[cfg(feature = "starlark")]
use std::sync::{Arc, Mutex};
use std::thread;

use clap::{CommandFactory, Parser, ValueEnum};
use clap_complete::{generate, Shell};
use indexmap::IndexMap;
#[cfg(feature = "starlark")]
use reedline::{
    default_emacs_keybindings, Completer, DescriptionMode, Emacs, FileBackedHistory, IdeMenu,
    KeyCode, KeyModifiers, MenuBuilder, Prompt, PromptEditMode, PromptHistorySearch,
    PromptHistorySearchStatus, Reedline, ReedlineEvent, ReedlineMenu, Signal, Span, Suggestion,
    ValidationResult as ReedlineValidationResult, Validator,
};

use crate::color::colorize;
use crate::error::AqError;
#[cfg(feature = "starlark")]
use crate::format::read_stdin_with_tabular_coercion;
use crate::format::{
    default_output_format, detect_format_for_input, detect_format_from_path,
    read_path_with_tabular_coercion, render, DetectConflictPolicy, Format, InputDocument,
    JsonIndent, OutputTerminator, RenderOptions, TabularCoercion,
};
use crate::inplace::write_atomically;
use crate::query::{
    evaluate_with_bindings_and_context, evaluate_with_bindings_and_context_preserving_partial,
    parse_with_options, validate_streaming_query, EvaluationContext, ParseOptions,
};
#[cfg(feature = "starlark")]
use crate::starlark::{
    aq_helper_completion_detail, evaluate_file, evaluate_inline,
    starlark_top_level_builtin_completion_detail, starlark_top_level_builtin_description,
    starlark_top_level_builtin_signature, StarlarkCapabilities, StarlarkContext,
    StarlarkReplSession, StarlarkReplValue,
};
use crate::value::{parse_json_str, Value};

const LONG_ABOUT: &str = "\
Read JSON, JSONL, TOML, YAML, CSV, or TSV, evaluate a jq-style query or
Starlark program, and render the results in the requested output format.";
const ENV_HELP: &str = "\
Environment:
  AQ_FLAGS                  Shell-like default flags prepended before argv
  AQ_DETECT_CONFLICTS       warn-fallback, extension, or sniff
  NO_COLOR                  Disable automatic syntax coloring on TTY output";
const LONG_HELP_FOOTER: &str = "\
Examples:
  aq '.name' config.yaml
  aq '.name' -
  aq --slurp 'map(.id)' *.json
  aq --stream 'select(.status >= 500)' logs.jsonl
  aq -n --starlark '1 + 2'
  aq --starlark 'aq.slug(data[\"name\"])' user.json
  aq -n -P

Environment:
  AQ_FLAGS                  Shell-like default flags prepended before argv
  AQ_DETECT_CONFLICTS       warn-fallback, extension, or sniff
  NO_COLOR                  Disable automatic syntax coloring on TTY output";
const CLI_WORKER_STACK_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "aq",
    version,
    about = "Universal data query tool",
    long_about = LONG_ABOUT,
    args_override_self = true,
    after_help = ENV_HELP,
    after_long_help = LONG_HELP_FOOTER
)]
pub struct Cli {
    #[arg(
        short = 'f',
        long,
        value_enum,
        help_heading = "Input",
        help = "Force the input reader instead of using extension and content detection"
    )]
    input_format: Option<Format>,
    #[arg(
        short = 'o',
        long,
        value_enum,
        help_heading = "Output",
        help = "Render results as this format"
    )]
    output_format: Option<Format>,
    #[arg(
        short = 's',
        long,
        help_heading = "Input",
        help = "Read all input documents into one array before evaluation"
    )]
    slurp: bool,
    #[arg(
        short = 'c',
        long,
        visible_alias = "compact-output",
        help_heading = "Output",
        help = "Use compact rendering when the output format supports it"
    )]
    compact: bool,
    #[arg(
        short = 'C',
        long,
        overrides_with = "monochrome_output",
        help_heading = "Output",
        help = "Force colorized output"
    )]
    color_output: bool,
    #[arg(
        short = 'M',
        long,
        visible_alias = "no-color",
        overrides_with = "color_output",
        help_heading = "Output",
        help = "Disable colorized output"
    )]
    monochrome_output: bool,
    #[arg(
        short = 'r',
        long,
        help_heading = "Output",
        help = "Write string results without JSON quoting"
    )]
    raw_output: bool,
    #[arg(
        short = 'j',
        long,
        conflicts_with = "raw_output0",
        help_heading = "Output",
        help = "Suppress the trailing newline between outputs"
    )]
    join_output: bool,
    #[arg(
        long,
        conflicts_with = "join_output",
        help_heading = "Output",
        help = "Write string results without JSON quoting and terminate each result with NUL"
    )]
    raw_output0: bool,
    #[arg(
        short = 'R',
        long,
        help_heading = "Input",
        help = "Read each input line as a raw string instead of parsing structured data"
    )]
    raw_input: bool,
    #[arg(
        long,
        value_enum,
        default_value_t = TabularCoercion::Strings,
        help_heading = "Input",
        help = "Coerce CSV and TSV input fields on read"
    )]
    tabular_coercion: TabularCoercion,
    #[arg(
        short = 'S',
        long,
        help_heading = "Output",
        help = "Sort object keys before rendering"
    )]
    sort_keys: bool,
    #[arg(
        long,
        overrides_with = "indent",
        help_heading = "Output",
        help = "Indent JSON output with tabs"
    )]
    tab: bool,
    #[arg(
        long,
        value_parser = clap::value_parser!(i8).range(-1..=7),
        overrides_with = "tab",
        help_heading = "Output",
        help = "Indent JSON output with this many spaces, use -1 for tabs"
    )]
    indent: Option<i8>,
    #[arg(
        short = 'i',
        long,
        help_heading = "Advanced",
        help = "Rewrite file arguments atomically; comment and formatting preservation is not guaranteed"
    )]
    in_place: bool,
    #[arg(
        long,
        value_enum,
        help_heading = "Advanced",
        help = "Print shell completion scripts and exit"
    )]
    generate_completions: Option<CompletionShell>,
    #[arg(
        short = 'n',
        long,
        help_heading = "Input",
        help = "Run the expression once with null instead of reading input documents"
    )]
    null_input: bool,
    #[arg(
        long,
        value_names = ["NAME", "VALUE"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help_heading = "Query Variables",
        help = "Bind a string variable, available as $NAME"
    )]
    arg: Vec<String>,
    #[arg(
        long,
        conflicts_with = "jsonargs",
        help_heading = "Query Variables",
        help = "Treat remaining FILES as positional string arguments in $ARGS.positional"
    )]
    args: bool,
    #[arg(
        long,
        value_names = ["NAME", "JSON"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help_heading = "Query Variables",
        help = "Bind a JSON variable, available as $NAME"
    )]
    argjson: Vec<String>,
    #[arg(
        long,
        conflicts_with = "args",
        help_heading = "Query Variables",
        help = "Treat remaining FILES as positional JSON arguments in $ARGS.positional"
    )]
    jsonargs: bool,
    #[arg(
        long,
        value_names = ["NAME", "PATH"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help_heading = "Query Variables",
        help = "Bind a string variable from a file's raw text contents"
    )]
    rawfile: Vec<String>,
    #[arg(
        long,
        value_names = ["NAME", "PATH"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help_heading = "Query Variables",
        help = "Bind a variable to all parsed documents from a file as an array"
    )]
    slurpfile: Vec<String>,
    #[arg(
        long,
        value_name = "DIR",
        action = clap::ArgAction::Append,
        help_heading = "Advanced",
        help = "Add a directory to the jq-style module search path"
    )]
    library_path: Vec<PathBuf>,
    #[arg(
        short = 'L',
        long,
        help_heading = "Starlark",
        help = "Evaluate EXPRESSION as Starlark with top-level data bound to the input value"
    )]
    starlark: bool,
    #[arg(
        short = 'F',
        long,
        help_heading = "Starlark",
        help = "Run a Starlark file by calling main(data)"
    )]
    starlark_file: Option<PathBuf>,
    #[arg(
        short = 'P',
        long,
        help_heading = "Starlark",
        help = "Start a persistent Starlark REPL with line editing, history, menu completion, Ctrl-C cancel, and Ctrl-D exit on a tty"
    )]
    starlark_repl: bool,
    #[arg(
        long,
        help_heading = "Starlark",
        help = "Allow Starlark programs to read and write files"
    )]
    starlark_filesystem: bool,
    #[arg(
        long,
        help_heading = "Starlark",
        help = "Allow Starlark programs to read environment variables"
    )]
    starlark_environment: bool,
    #[arg(
        long,
        help_heading = "Starlark",
        help = "Allow Starlark date, time, and clock helpers"
    )]
    starlark_time: bool,
    #[arg(
        long,
        help_heading = "Starlark",
        help = "Enable all Starlark capability flags"
    )]
    starlark_unsafe: bool,
    #[arg(
        long,
        help_heading = "Input",
        help = "Line-oriented streaming mode for JSONL input or --raw-input"
    )]
    stream: bool,
    #[arg(
        long,
        help_heading = "Advanced",
        help = "Print the parsed query AST and exit"
    )]
    explain: bool,
    #[arg(
        short = 'e',
        long,
        help_heading = "Advanced",
        help = "Exit 0 if the last output is truthy, 1 if it is false or null, 4 if there was no output"
    )]
    exit_status: bool,
    #[arg(
        long,
        value_enum,
        help_heading = "Advanced",
        help = "Choose how extension-vs-content format conflicts are resolved"
    )]
    detect_conflicts: Option<DetectConflictPolicy>,
    /// jq-style query to run, defaults to `.` when omitted.
    expression: Option<String>,
    /// Input files. Reads stdin when omitted. Use `-` to read stdin explicitly.
    files: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CompletionShell {
    Bash,
    Elvish,
    Fish,
    PowerShell,
    Zsh,
}

impl CompletionShell {
    fn into_shell(self) -> Shell {
        match self {
            CompletionShell::Bash => Shell::Bash,
            CompletionShell::Elvish => Shell::Elvish,
            CompletionShell::Fish => Shell::Fish,
            CompletionShell::PowerShell => Shell::PowerShell,
            CompletionShell::Zsh => Shell::Zsh,
        }
    }
}

impl Cli {
    fn has_starlark_capability_flags(&self) -> bool {
        self.starlark_filesystem
            || self.starlark_environment
            || self.starlark_time
            || self.starlark_unsafe
    }

    fn has_query_variable_flags(&self) -> bool {
        self.args
            || self.jsonargs
            || !self.arg.is_empty()
            || !self.argjson.is_empty()
            || !self.rawfile.is_empty()
            || !self.slurpfile.is_empty()
    }

    fn raw_output_mode(&self) -> OutputTerminator {
        if self.raw_output0 {
            OutputTerminator::Nul
        } else if self.join_output {
            OutputTerminator::None
        } else {
            OutputTerminator::Newline
        }
    }

    fn uses_raw_output(&self) -> bool {
        self.raw_output || self.join_output || self.raw_output0
    }

    fn json_indent(&self) -> JsonIndent {
        if self.tab || self.indent == Some(-1) {
            JsonIndent::Tab
        } else {
            JsonIndent::Spaces(self.indent.unwrap_or(2) as u8)
        }
    }

    fn render_options(&self) -> RenderOptions {
        RenderOptions {
            compact: self.compact,
            raw_output: self.uses_raw_output(),
            terminator: self.raw_output_mode(),
            sort_keys: self.sort_keys,
            json_indent: self.json_indent(),
        }
    }

    fn use_color(&self) -> bool {
        if self.monochrome_output {
            return false;
        }
        if self.color_output {
            return true;
        }
        std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
    }

    fn input_files(&self) -> Vec<PathBuf> {
        if self.args || self.jsonargs {
            return Vec::new();
        }
        if self.starlark_file.is_some() || self.starlark_repl {
            let mut files =
                Vec::with_capacity(self.files.len() + usize::from(self.expression.is_some()));
            if let Some(first_file) = &self.expression {
                files.push(PathBuf::from(first_file));
            }
            files.extend(self.files.iter().cloned());
            files
        } else {
            self.files.clone()
        }
    }
}

pub fn run_cli() -> i32 {
    let cli = match parse_cli() {
        Ok(cli) => cli,
        Err(CliParseError::Clap(error)) => {
            let _ = error.print();
            return error.exit_code();
        }
        Err(CliParseError::Env(message)) => {
            eprintln!("{message}");
            return 2;
        }
    };
    let result = match thread::Builder::new()
        .stack_size(CLI_WORKER_STACK_SIZE)
        .spawn(move || run(cli))
    {
        Ok(handle) => match handle.join() {
            Ok(result) => result,
            Err(_) => {
                eprintln!("aq worker thread panicked");
                return 1;
            }
        },
        Err(error) => {
            eprintln!("failed to spawn aq worker thread: {error}");
            return 1;
        }
    };
    match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            error.exit_code()
        }
    }
}

enum CliParseError {
    Clap(clap::Error),
    Env(String),
}

fn parse_cli() -> Result<Cli, CliParseError> {
    let env_args = parse_aq_flags_env().map_err(CliParseError::Env)?;
    let argv = std::env::args_os().collect::<Vec<_>>();
    let mut merged = Vec::with_capacity(argv.len() + env_args.len());
    merged.push(
        argv.first()
            .cloned()
            .unwrap_or_else(|| OsString::from("aq")),
    );
    merged.extend(env_args.into_iter().map(OsString::from));
    merged.extend(argv.into_iter().skip(1));
    Cli::try_parse_from(merged).map_err(CliParseError::Clap)
}

fn parse_aq_flags_env() -> Result<Vec<String>, String> {
    let raw = match std::env::var("AQ_FLAGS") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(Vec::new()),
        Err(error) => return Err(format!("failed to read AQ_FLAGS: {error}")),
    };
    split_shell_like_words(&raw).map_err(|error| format!("failed to parse AQ_FLAGS: {error}"))
}

fn split_shell_like_words(input: &str) -> Result<Vec<String>, String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum QuoteMode {
        None,
        Single,
        Double,
    }

    let mut out = Vec::new();
    let mut current = String::new();
    let mut mode = QuoteMode::None;
    let mut escaped = false;
    let mut token_started = false;

    for ch in input.chars() {
        match mode {
            QuoteMode::None => {
                if escaped {
                    current.push(unescape_shell_char(ch));
                    escaped = false;
                    token_started = true;
                    continue;
                }

                match ch {
                    '\\' => {
                        escaped = true;
                        token_started = true;
                    }
                    '\'' => {
                        mode = QuoteMode::Single;
                        token_started = true;
                    }
                    '"' => {
                        mode = QuoteMode::Double;
                        token_started = true;
                    }
                    _ if ch.is_whitespace() => {
                        if token_started {
                            out.push(std::mem::take(&mut current));
                            token_started = false;
                        }
                    }
                    _ => {
                        current.push(ch);
                        token_started = true;
                    }
                }
            }
            QuoteMode::Single => {
                if ch == '\'' {
                    mode = QuoteMode::None;
                } else {
                    current.push(ch);
                }
            }
            QuoteMode::Double => {
                if escaped {
                    current.push(unescape_shell_char(ch));
                    escaped = false;
                } else {
                    match ch {
                        '\\' => escaped = true,
                        '"' => mode = QuoteMode::None,
                        _ => current.push(ch),
                    }
                }
            }
        }
    }

    if escaped {
        return Err("trailing escape".to_string());
    }
    match mode {
        QuoteMode::None => {}
        QuoteMode::Single => return Err("unterminated single quote".to_string()),
        QuoteMode::Double => return Err("unterminated double quote".to_string()),
    }
    if token_started {
        out.push(current);
    }
    Ok(out)
}

fn unescape_shell_char(ch: char) -> char {
    match ch {
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        other => other,
    }
}

pub fn run(cli: Cli) -> Result<i32, AqError> {
    if let Some(shell) = cli.generate_completions {
        let mut command = Cli::command();
        generate(
            shell.into_shell(),
            &mut command,
            "aq",
            &mut std::io::stdout(),
        );
        return Ok(0);
    }

    let starlark_mode = cli.starlark || cli.starlark_file.is_some() || cli.starlark_repl;
    let starlark_entrypoints = usize::from(cli.starlark)
        + usize::from(cli.starlark_file.is_some())
        + usize::from(cli.starlark_repl);
    if starlark_entrypoints > 1 {
        return Err(AqError::message(
            "cannot combine --starlark, --starlark-file, and --starlark-repl",
        ));
    }
    if cli.has_starlark_capability_flags() && !starlark_mode {
        return Err(AqError::message(
            "starlark capability flags require --starlark or --starlark-file",
        ));
    }
    if starlark_mode && cli.has_query_variable_flags() {
        return Err(AqError::message(
            "--args, --jsonargs, --arg, --argjson, --rawfile, and --slurpfile are not supported with --starlark",
        ));
    }
    if starlark_mode && !cli.library_path.is_empty() {
        return Err(AqError::message(
            "--library-path is not supported with --starlark",
        ));
    }
    if cli.exit_status && (cli.stream || cli.in_place) {
        return Err(AqError::message(
            "-e/--exit-status is not supported with --stream or --in-place",
        ));
    }

    let input_files = cli.input_files();
    if cli.null_input && !input_files.is_empty() {
        return Err(AqError::NullInputWithFiles);
    }

    let detect_conflicts = resolve_detect_conflicts(cli.detect_conflicts)?;
    let query_bindings = build_query_bindings(&cli, detect_conflicts)?;
    if cli.in_place {
        validate_in_place_mode(&cli, &input_files)?;
    }
    if starlark_mode {
        #[cfg(feature = "starlark")]
        {
            if cli.starlark_repl {
                return run_starlark_repl(&cli, &input_files, detect_conflicts);
            }
            return run_starlark(&cli, &input_files, detect_conflicts);
        }
        #[cfg(not(feature = "starlark"))]
        {
            let _ = detect_conflicts;
            return Err(AqError::message(
                "this build does not include starlark support",
            ));
        }
    }

    let expression = cli.expression.clone().unwrap_or_else(|| ".".to_string());
    let current_dir = std::env::current_dir().map_err(|error| {
        AqError::message(format!("failed to resolve current directory: {error}"))
    })?;
    let parse_options =
        ParseOptions::with_module_search_paths(current_dir, cli.library_path.clone());
    let query = parse_with_options(&expression, &parse_options)?;
    if cli.explain {
        println!("{query:#?}");
        return Ok(0);
    }
    if cli.stream {
        validate_stream_output_format(&cli)?;
        validate_streaming_query(&query)?;
        return run_streaming(&cli, &query, &query_bindings, detect_conflicts);
    }
    if cli.in_place {
        return run_query_in_place(
            &cli,
            &query,
            &query_bindings,
            &input_files,
            detect_conflicts,
        );
    }

    let documents = load_query_documents(&cli, &input_files, detect_conflicts)?;
    let input_formats: Vec<Format> = documents.iter().map(|document| document.format).collect();
    let output_format = cli
        .output_format
        .unwrap_or_else(|| default_output_format(&input_formats));
    let results = match evaluate_documents_preserving_partial(
        &query,
        &query_bindings,
        documents,
        cli.slurp,
        cli.null_input,
    ) {
        Ok(results) => results,
        Err(partial) => {
            if !partial.values.is_empty() {
                let rendered = render(&partial.values, output_format, cli.render_options())?;
                let rendered = maybe_colorize_stdout(&cli, output_format, rendered);
                std::io::stdout()
                    .write_all(rendered.as_bytes())
                    .map_err(|error| AqError::io(None, error))?;
            }
            return Err(partial.error);
        }
    };

    let rendered = render(&results, output_format, cli.render_options())?;
    let rendered = maybe_colorize_stdout(&cli, output_format, rendered);
    std::io::stdout()
        .write_all(rendered.as_bytes())
        .map_err(|error| AqError::io(None, error))?;
    Ok(exit_status_for_results(&cli, &results))
}

#[cfg(feature = "starlark")]
const STARLARK_REPL_PROMPT: &str = "aq> ";
#[cfg(feature = "starlark")]
const STARLARK_REPL_CONTINUATION_PROMPT: &str = "... ";
#[cfg(feature = "starlark")]
const STARLARK_REPL_COMPLETION_MENU: &str = "aq_starlark_completion_menu";
#[cfg(feature = "starlark")]
const STARLARK_REPL_HISTORY_SIZE: usize = 1_000;
#[cfg(feature = "starlark")]
const STARLARK_REPL_COMMANDS: &[(&str, &str, &str)] = &[
    (":help", ":help", "show repl commands"),
    (":quit", ":quit", "exit the repl"),
    (":exit", ":exit", "exit the repl"),
    (
        ":data",
        ":data [EXPR]",
        "print current data or evaluate EXPR and replace it",
    ),
    (
        ":type",
        ":type EXPR",
        "evaluate EXPR and print its Starlark type without changing ans",
    ),
    (
        ":doc",
        ":doc NAME",
        "show a signature and summary for a helper, command, constant, or binding",
    ),
    (
        ":constants",
        ":constants",
        "print well-known repl constants and literals",
    ),
    (
        ":capabilities",
        ":capabilities",
        "print enabled Starlark capabilities",
    ),
    (":globals", ":globals", "print current top-level names"),
    (":aq", ":aq [PREFIX]", "list aq helper names"),
    (
        ":load",
        ":load PATH",
        "evaluate a Starlark file inside the session",
    ),
    (
        ":format",
        ":format [FORMAT]",
        "print or change the output format",
    ),
    (":pwd", ":pwd", "print the current base directory"),
    (
        ":reset",
        ":reset",
        "restore the original session data and bindings",
    ),
];
#[cfg(feature = "starlark")]
const STARLARK_REPL_FORMATS: &[(&str, &str)] = &[
    ("json", "render aq-compatible values as JSON"),
    (
        "jsonl",
        "render aq-compatible values as newline-delimited JSON",
    ),
    ("toml", "render aq-compatible values as TOML"),
    ("yaml", "render aq-compatible values as YAML"),
    ("csv", "render aq-compatible values as CSV"),
    ("tsv", "render aq-compatible values as TSV"),
    ("table", "render aq-compatible values as a terminal table"),
];
#[cfg(feature = "starlark")]
const STARLARK_REPL_KEYWORDS: &[&str] = &[
    "None", "True", "False", "and", "as", "def", "elif", "else", "for", "if", "in", "lambda",
    "load", "not", "or", "return",
];
#[cfg(feature = "starlark")]
const STARLARK_REPL_CONSTANTS: &[(&str, &str, &str)] = &[
    (
        "data",
        "binding",
        "current input value for this repl session",
    ),
    (
        "aq",
        "namespace",
        "aq helper namespace for builtins and utility functions",
    ),
    (
        "ans",
        "binding",
        "most recent non-None result, available after the first result",
    ),
    (
        "prev",
        "binding",
        "previous ans value, available after the second result",
    ),
    (
        "_",
        "binding",
        "alias for ans, available after the first result",
    ),
    ("None", "literal", "Starlark null literal"),
    ("True", "literal", "Starlark true literal"),
    ("False", "literal", "Starlark false literal"),
];

#[cfg(feature = "starlark")]
fn starlark_repl_special_name_description(name: &str) -> Option<String> {
    match name {
        "data" => Some("current input value for this repl session".to_string()),
        "aq" => Some("aq helper namespace for builtins and utility functions".to_string()),
        "ans" => Some("most recent non-None result, available after the first result".to_string()),
        "prev" => Some("previous ans value, available after the second result".to_string()),
        "_" => Some("alias for ans, available after the first result".to_string()),
        "None" => Some("Starlark null literal".to_string()),
        "True" => Some("Starlark true literal".to_string()),
        "False" => Some("Starlark false literal".to_string()),
        _ => None,
    }
}

#[cfg(feature = "starlark")]
fn starlark_repl_top_level_builtin_doc_value(name: &str) -> Option<StarlarkReplValue> {
    let signature = starlark_top_level_builtin_signature(name)?;
    let description = starlark_top_level_builtin_description(name)?;
    let mut fields = IndexMap::new();
    fields.insert("name".to_string(), Value::String(name.to_string()));
    fields.insert("kind".to_string(), Value::String("builtin".to_string()));
    fields.insert(
        "signature".to_string(),
        Value::String(signature.to_string()),
    );
    fields.insert(
        "description".to_string(),
        Value::String(description.to_string()),
    );
    Some(StarlarkReplValue::Aq(Value::Object(fields)))
}

#[cfg(feature = "starlark")]
fn starlark_repl_command_detail(name: &str) -> Option<String> {
    STARLARK_REPL_COMMANDS
        .iter()
        .find(|(command, _, _)| *command == name)
        .map(|(_, signature, description)| format!("{signature}\n{description}"))
}

#[cfg(feature = "starlark")]
fn starlark_repl_command_doc_value(name: &str) -> Option<StarlarkReplValue> {
    STARLARK_REPL_COMMANDS
        .iter()
        .find(|(command, _, _)| *command == name)
        .map(|(command, signature, description)| {
            let mut fields = IndexMap::new();
            fields.insert("name".to_string(), Value::String((*command).to_string()));
            fields.insert("kind".to_string(), Value::String("command".to_string()));
            fields.insert(
                "signature".to_string(),
                Value::String((*signature).to_string()),
            );
            fields.insert(
                "description".to_string(),
                Value::String((*description).to_string()),
            );
            StarlarkReplValue::Aq(Value::Object(fields))
        })
}

#[cfg(feature = "starlark")]
fn starlark_repl_capabilities_value(session: &StarlarkReplSession) -> StarlarkReplValue {
    let capabilities = session.capabilities();
    let mut fields = IndexMap::new();
    fields.insert(
        "filesystem".to_string(),
        Value::Bool(capabilities.filesystem),
    );
    fields.insert(
        "environment".to_string(),
        Value::Bool(capabilities.environment),
    );
    fields.insert("time".to_string(), Value::Bool(capabilities.time));
    StarlarkReplValue::Aq(Value::Object(fields))
}

#[cfg(feature = "starlark")]
fn starlark_repl_helper_requirement(name: &str) -> Option<&'static str> {
    match name {
        "base_dir" | "copy" | "exists" | "glob" | "hash_file" | "is_dir" | "is_file"
        | "list_dir" | "mkdir" | "read" | "read_all" | "read_all_as" | "read_as" | "read_glob"
        | "read_glob_all" | "read_glob_all_as" | "read_glob_as" | "read_text"
        | "read_text_glob" | "relative_path" | "remove" | "rename" | "resolve_path"
        | "rewrite_text" | "rewrite_text_glob" | "stat" | "walk_files" | "write" | "write_all"
        | "write_batch" | "write_batch_all" | "write_text" | "write_text_batch" => {
            Some("filesystem")
        }
        "env" => Some("environment"),
        "now" | "timestamp" | "today" => Some("time"),
        _ => None,
    }
}

#[cfg(feature = "starlark")]
fn starlark_repl_helper_doc_value(name: &str) -> Option<StarlarkReplValue> {
    let helper_name = name.strip_prefix("aq.").unwrap_or(name);
    let signature = crate::starlark::aq_helper_signature(helper_name)?;
    let description = crate::starlark::aq_helper_description(helper_name)?;
    let mut fields = IndexMap::new();
    fields.insert(
        "name".to_string(),
        Value::String(format!("aq.{helper_name}")),
    );
    fields.insert("kind".to_string(), Value::String("helper".to_string()));
    fields.insert(
        "signature".to_string(),
        Value::String(signature.to_string()),
    );
    fields.insert(
        "description".to_string(),
        Value::String(description.to_string()),
    );
    if let Some(requirement) = starlark_repl_helper_requirement(helper_name) {
        fields.insert(
            "requires".to_string(),
            Value::String(requirement.to_string()),
        );
    }
    Some(StarlarkReplValue::Aq(Value::Object(fields)))
}

#[cfg(feature = "starlark")]
fn starlark_repl_special_name_doc_value(name: &str) -> Option<StarlarkReplValue> {
    let description = starlark_repl_special_name_description(name)?;
    let kind = match name {
        "aq" => "namespace",
        "None" | "True" | "False" => "literal",
        _ => "binding",
    };
    let mut fields = IndexMap::new();
    fields.insert("name".to_string(), Value::String(name.to_string()));
    fields.insert("kind".to_string(), Value::String(kind.to_string()));
    fields.insert("description".to_string(), Value::String(description));
    Some(StarlarkReplValue::Aq(Value::Object(fields)))
}

#[cfg(feature = "starlark")]
fn starlark_repl_session_name_doc_value(
    session: &StarlarkReplSession,
    name: &str,
) -> Option<StarlarkReplValue> {
    let type_name = session.name_type(name)?;
    let mut fields = IndexMap::new();
    fields.insert("name".to_string(), Value::String(name.to_string()));
    fields.insert("kind".to_string(), Value::String("session".to_string()));
    fields.insert("type".to_string(), Value::String(type_name));
    fields.insert(
        "description".to_string(),
        Value::String("session binding or function".to_string()),
    );
    Some(StarlarkReplValue::Aq(Value::Object(fields)))
}

#[cfg(feature = "starlark")]
fn starlark_repl_doc_value(session: &StarlarkReplSession, name: &str) -> Option<StarlarkReplValue> {
    starlark_repl_command_doc_value(name)
        .or_else(|| starlark_repl_top_level_builtin_doc_value(name))
        .or_else(|| starlark_repl_special_name_doc_value(name))
        .or_else(|| starlark_repl_session_name_doc_value(session, name))
        .or_else(|| starlark_repl_helper_doc_value(name))
}

#[cfg(feature = "starlark")]
fn starlark_repl_data_summary(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => "number".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(values) => format!("array[{}]", values.len()),
        Value::Object(fields) => format!("object[{}]", fields.len()),
        Value::Bytes(bytes) => format!("bytes[{}]", bytes.len()),
        Value::DateTime(_) => "datetime".to_string(),
        Value::Date(_) => "date".to_string(),
        Value::Tagged { tag, .. } => format!("tagged:{tag}"),
    }
}

#[cfg(feature = "starlark")]
fn starlark_repl_banner_line(
    session: &StarlarkReplSession,
    initial_data: &Value,
    output_format: Format,
) -> String {
    let caps = session.capabilities();
    let input_format = session.current_format_name().unwrap_or("none");
    format!(
        "aq Starlark REPL, data={}, input={}, output={}, fs={}, env={}, time={}, :help commands, :aq helpers, :doc NAME, Ctrl-C cancel, Ctrl-D exit",
        starlark_repl_data_summary(initial_data),
        input_format,
        output_format,
        if caps.filesystem { "on" } else { "off" },
        if caps.environment { "on" } else { "off" },
        if caps.time { "on" } else { "off" },
    )
}

#[cfg(feature = "starlark")]
fn starlark_repl_constants_value() -> StarlarkReplValue {
    let values = STARLARK_REPL_CONSTANTS
        .iter()
        .map(|(name, kind, description)| {
            let mut fields = IndexMap::new();
            fields.insert("name".to_string(), Value::String((*name).to_string()));
            fields.insert("kind".to_string(), Value::String((*kind).to_string()));
            fields.insert(
                "description".to_string(),
                Value::String((*description).to_string()),
            );
            Value::Object(fields)
        })
        .collect::<Vec<_>>();
    StarlarkReplValue::Aq(Value::Array(values))
}

#[cfg(feature = "starlark")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct AqStarlarkReplCompletion {
    value: String,
    display: String,
    description: Option<String>,
    append_whitespace: bool,
}

#[cfg(feature = "starlark")]
impl AqStarlarkReplCompletion {
    fn new(
        value: impl Into<String>,
        display: impl Into<String>,
        description: impl Into<Option<String>>,
        append_whitespace: bool,
    ) -> Self {
        Self {
            value: value.into(),
            display: display.into(),
            description: description.into(),
            append_whitespace,
        }
    }

    fn to_suggestion(&self, start: usize, end: usize) -> Suggestion {
        Suggestion {
            value: self.value.clone(),
            display_override: Some(self.display.clone()),
            description: self.description.clone(),
            style: None,
            extra: None,
            span: Span::new(start, end),
            append_whitespace: self.append_whitespace,
            match_indices: None,
        }
    }
}

#[cfg(feature = "starlark")]
#[derive(Debug, Default)]
struct AqStarlarkReplCompletionState {
    base_dir: PathBuf,
    session_names: Vec<String>,
    aq_names: Vec<String>,
}

#[cfg(feature = "starlark")]
impl AqStarlarkReplCompletionState {
    fn refresh_from_session(&mut self, session: &StarlarkReplSession) {
        self.base_dir = session.base_dir().to_path_buf();
        self.session_names = session.names();
        self.aq_names = session.aq_names().to_vec();
    }
}

#[cfg(feature = "starlark")]
#[derive(Debug, Clone)]
struct AqStarlarkReplCompleter {
    state: Arc<Mutex<AqStarlarkReplCompletionState>>,
}

#[cfg(feature = "starlark")]
impl AqStarlarkReplCompleter {
    fn new(state: Arc<Mutex<AqStarlarkReplCompletionState>>) -> Self {
        Self { state }
    }

    fn complete_candidates(
        &self,
        line: &str,
        pos: usize,
    ) -> (usize, Vec<AqStarlarkReplCompletion>) {
        let state = self.state.lock().expect("repl completion state poisoned");
        state.complete_candidates(line, pos)
    }
}

#[cfg(feature = "starlark")]
impl AqStarlarkReplCompletionState {
    fn complete_candidates(
        &self,
        line: &str,
        pos: usize,
    ) -> (usize, Vec<AqStarlarkReplCompletion>) {
        let prefix = &line[..pos];
        if prefix.trim_start().starts_with(':') {
            return self.complete_command_line(prefix);
        }
        self.complete_starlark_line(prefix)
    }

    fn complete_command_line(&self, prefix: &str) -> (usize, Vec<AqStarlarkReplCompletion>) {
        let trimmed = prefix.trim_start();
        if let Some(rest) = trimmed.strip_prefix(":load") {
            let args = rest.trim_start();
            if !args.is_empty() || prefix.ends_with(":load ") {
                let start = prefix.len().saturating_sub(args.len());
                return (start, complete_path_candidates(&self.base_dir, args));
            }
        }
        if let Some(rest) = trimmed.strip_prefix(":doc") {
            let args = rest.trim_start();
            if !args.is_empty() || prefix.ends_with(":doc ") {
                let arg_offset = prefix.len().saturating_sub(args.len());
                let (start, completions) = self.complete_starlark_line(args);
                return (arg_offset + start, completions);
            }
        }
        if let Some(rest) = trimmed.strip_prefix(":type") {
            let args = rest.trim_start();
            if !args.is_empty() || prefix.ends_with(":type ") {
                let arg_offset = prefix.len().saturating_sub(args.len());
                let (start, completions) = self.complete_starlark_line(args);
                return (arg_offset + start, completions);
            }
        }
        if let Some(rest) = trimmed.strip_prefix(":format") {
            let args = rest.trim_start();
            if !args.is_empty() || prefix.ends_with(":format ") {
                let start = prefix.len().saturating_sub(args.len());
                return (
                    start,
                    complete_described_word_candidates(args, STARLARK_REPL_FORMATS, true),
                );
            }
        }

        let start = find_completion_start(prefix);
        let token = &prefix[start..];
        let completions = STARLARK_REPL_COMMANDS
            .iter()
            .filter(|(command, _, _)| command.trim_start_matches(':').starts_with(token))
            .map(|(command, _, _)| {
                AqStarlarkReplCompletion::new(
                    command.trim_start_matches(':'),
                    *command,
                    starlark_repl_command_detail(command),
                    true,
                )
            })
            .collect::<Vec<_>>();
        (start, completions)
    }

    fn complete_starlark_line(&self, prefix: &str) -> (usize, Vec<AqStarlarkReplCompletion>) {
        let start = find_completion_start(prefix);
        let token = &prefix[start..];
        if let Some(member_prefix) = token.strip_prefix("aq.") {
            let completions = self
                .aq_names
                .iter()
                .filter(|name| name.starts_with(member_prefix))
                .map(|name| {
                    AqStarlarkReplCompletion::new(
                        format!("aq.{name}"),
                        format!("aq.{name}"),
                        aq_helper_completion_detail(name),
                        false,
                    )
                })
                .collect::<Vec<_>>();
            return (start, completions);
        }

        let mut completions = self
            .session_names
            .iter()
            .map(|name| {
                AqStarlarkReplCompletion::new(
                    name.clone(),
                    name.clone(),
                    starlark_repl_special_name_description(name)
                        .or_else(|| Some("session binding or function".to_string())),
                    false,
                )
            })
            .chain(STARLARK_REPL_KEYWORDS.iter().map(|keyword| {
                AqStarlarkReplCompletion::new(
                    *keyword,
                    *keyword,
                    starlark_repl_special_name_description(keyword)
                        .or_else(|| Some("Starlark keyword".to_string())),
                    false,
                )
            }))
            .chain(["log"].into_iter().map(|name| {
                AqStarlarkReplCompletion::new(
                    name,
                    name,
                    starlark_top_level_builtin_completion_detail(name),
                    false,
                )
            }))
            .chain(std::iter::once(AqStarlarkReplCompletion::new(
                "aq",
                "aq",
                starlark_repl_special_name_description("aq"),
                false,
            )))
            .collect::<Vec<_>>();
        completions.sort_by(|left, right| left.value.cmp(&right.value));
        completions.dedup_by(|left, right| left.value == right.value);
        (
            start,
            completions
                .into_iter()
                .filter(|candidate| candidate.value.starts_with(token))
                .collect(),
        )
    }
}

#[cfg(feature = "starlark")]
impl Completer for AqStarlarkReplCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let (start, completions) = self.complete_candidates(line, pos);
        completions
            .iter()
            .map(|completion| completion.to_suggestion(start, pos))
            .collect()
    }
}

#[cfg(feature = "starlark")]
struct AqStarlarkReplPrompt;

#[cfg(feature = "starlark")]
impl Prompt for AqStarlarkReplPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _prompt_mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed(STARLARK_REPL_PROMPT)
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed(STARLARK_REPL_CONTINUATION_PROMPT)
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        let prefix = match history_search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        Cow::Owned(format!(
            "({prefix}reverse-search: {}) ",
            history_search.term
        ))
    }
}

#[cfg(feature = "starlark")]
struct AqStarlarkReplValidator;

#[cfg(feature = "starlark")]
impl Validator for AqStarlarkReplValidator {
    fn validate(&self, line: &str) -> ReedlineValidationResult {
        if line.trim_start().starts_with(':') {
            return ReedlineValidationResult::Complete;
        }
        if repl_needs_more_input(line) {
            ReedlineValidationResult::Incomplete
        } else {
            ReedlineValidationResult::Complete
        }
    }
}

#[cfg(feature = "starlark")]
fn starlark_repl_edit_mode() -> Box<dyn reedline::EditMode> {
    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu(STARLARK_REPL_COMPLETION_MENU.to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );
    keybindings.add_binding(
        KeyModifiers::SHIFT,
        KeyCode::BackTab,
        ReedlineEvent::MenuPrevious,
    );
    Box::new(Emacs::new(keybindings))
}

#[cfg(feature = "starlark")]
fn starlark_repl_menu() -> ReedlineMenu {
    ReedlineMenu::EngineCompleter(Box::new(
        IdeMenu::default()
            .with_name(STARLARK_REPL_COMPLETION_MENU)
            .with_default_border()
            .with_max_completion_height(10)
            .with_min_completion_width(18)
            .with_max_completion_width(40)
            .with_description_mode(DescriptionMode::PreferRight)
            .with_min_description_width(24)
            .with_max_description_width(48)
            .with_description_offset(1)
            .with_correct_cursor_pos(true),
    ))
}

#[cfg(feature = "starlark")]
fn starlark_repl_history() -> Result<Option<Box<dyn reedline::History>>, AqError> {
    let Some(path) = repl_history_path() else {
        return Ok(None);
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| AqError::io(Some(parent.to_path_buf()), error))?;
    }
    let history = FileBackedHistory::with_file(STARLARK_REPL_HISTORY_SIZE, path)
        .map_err(|error| AqError::message(format!("failed to configure repl history: {error}")))?;
    Ok(Some(Box::new(history)))
}

fn validate_stream_output_format(cli: &Cli) -> Result<(), AqError> {
    let Some(format) = cli.output_format else {
        return Ok(());
    };

    match format {
        Format::Json | Format::Jsonl | Format::Csv | Format::Tsv => Ok(()),
        Format::Toml | Format::Yaml => Err(AqError::message(format!(
            "--stream does not support {format} output in this slice because it does not provide unambiguous per-result framing; rerun without --stream or choose json, jsonl, csv, or tsv"
        ))),
        Format::Table => Err(AqError::message(
            "--stream does not support table output in this slice because it needs the full result set to size columns; rerun without --stream or choose json, jsonl, csv, or tsv",
        )),
    }
}

fn exit_status_for_results(cli: &Cli, results: &[Value]) -> i32 {
    if !cli.exit_status {
        return 0;
    }
    match results.last() {
        None => 4,
        Some(Value::Null) | Some(Value::Bool(false)) => 1,
        Some(_) => 0,
    }
}

#[cfg(feature = "starlark")]
fn run_starlark(
    cli: &Cli,
    input_files: &[PathBuf],
    detect_conflicts: DetectConflictPolicy,
) -> Result<i32, AqError> {
    if cli.in_place {
        return run_starlark_in_place(cli, input_files, detect_conflicts);
    }
    if cli.stream {
        return Err(AqError::message(
            "--stream is not supported with --starlark in this slice",
        ));
    }
    if cli.explain {
        return Err(AqError::message(
            "--explain is not supported with --starlark in this slice",
        ));
    }

    let capabilities = StarlarkCapabilities::from_flags(
        cli.starlark_filesystem,
        cli.starlark_environment,
        cli.starlark_time,
        cli.starlark_unsafe,
    );
    let base_dir = starlark_base_dir(cli)?;

    let documents = load_documents(cli, input_files, detect_conflicts)?;
    let input_formats: Vec<Format> = documents.iter().map(|document| document.format).collect();

    let results = if cli.slurp {
        let context = StarlarkContext::new(
            capabilities,
            detect_conflicts,
            starlark_format_name(&input_formats),
            base_dir.clone(),
        );
        let array = Value::Array(
            documents
                .into_iter()
                .map(|document| document.value)
                .collect(),
        );
        vec![evaluate_starlark_program(cli, &array, &context)?]
    } else {
        let mut out = Vec::new();
        for document in documents {
            let context = StarlarkContext::new(
                capabilities,
                detect_conflicts,
                Some(document.format.to_string()),
                base_dir.clone(),
            );
            out.push(evaluate_starlark_program(cli, &document.value, &context)?);
        }
        out
    };

    let output_format = cli
        .output_format
        .unwrap_or_else(|| default_output_format(&input_formats));
    let rendered = render(&results, output_format, cli.render_options())?;
    let rendered = maybe_colorize_stdout(cli, output_format, rendered);
    std::io::stdout()
        .write_all(rendered.as_bytes())
        .map_err(|error| AqError::io(None, error))?;
    Ok(exit_status_for_results(cli, &results))
}

#[cfg(feature = "starlark")]
fn run_starlark_repl(
    cli: &Cli,
    input_files: &[PathBuf],
    detect_conflicts: DetectConflictPolicy,
) -> Result<i32, AqError> {
    if cli.in_place {
        return Err(AqError::message(
            "--in-place is not supported with --starlark-repl",
        ));
    }
    if cli.stream {
        return Err(AqError::message(
            "--stream is not supported with --starlark-repl",
        ));
    }
    if cli.explain {
        return Err(AqError::message(
            "--explain is not supported with --starlark-repl",
        ));
    }
    if cli.exit_status {
        return Err(AqError::message(
            "-e/--exit-status is not supported with --starlark-repl",
        ));
    }

    let capabilities = StarlarkCapabilities::from_flags(
        cli.starlark_filesystem,
        cli.starlark_environment,
        cli.starlark_time,
        cli.starlark_unsafe,
    );
    let base_dir = starlark_base_dir(cli)?;
    let repl_input = load_starlark_repl_input(cli, input_files, detect_conflicts)?;
    let context = StarlarkContext::new(
        capabilities,
        detect_conflicts,
        starlark_format_name(&repl_input.input_formats),
        base_dir,
    );
    let output_format = cli.output_format.unwrap_or_else(|| {
        if repl_input.input_formats.is_empty() {
            Format::Json
        } else {
            default_output_format(&repl_input.input_formats)
        }
    });
    let mut session = StarlarkReplSession::new(&repl_input.initial_data, context)?;

    if !repl_input.stdin_consumed
        && std::io::stdin().is_terminal()
        && std::io::stderr().is_terminal()
    {
        return run_starlark_repl_editor_loop(
            cli,
            &mut session,
            &repl_input.initial_data,
            output_format,
        );
    }

    if repl_input.stdin_consumed {
        let mut reader = BufReader::new(open_repl_input_tty()?);
        let mut prompt_writer = open_repl_output_tty()?;
        return run_starlark_repl_loop(
            cli,
            &mut session,
            &repl_input.initial_data,
            output_format,
            &mut reader,
            &mut prompt_writer,
            true,
        );
    }

    let stdin = std::io::stdin();
    let stderr = std::io::stderr();
    let mut reader = BufReader::new(stdin.lock());
    let mut prompt_writer = stderr.lock();
    run_starlark_repl_loop(
        cli,
        &mut session,
        &repl_input.initial_data,
        output_format,
        &mut reader,
        &mut prompt_writer,
        std::io::stdin().is_terminal(),
    )
}

#[cfg(feature = "starlark")]
struct StarlarkReplInput {
    initial_data: Value,
    input_formats: Vec<Format>,
    stdin_consumed: bool,
}

#[cfg(feature = "starlark")]
fn load_starlark_repl_input(
    cli: &Cli,
    input_files: &[PathBuf],
    detect_conflicts: DetectConflictPolicy,
) -> Result<StarlarkReplInput, AqError> {
    if cli.null_input {
        return Ok(StarlarkReplInput {
            initial_data: Value::Null,
            input_formats: vec![cli.input_format.unwrap_or(Format::Json)],
            stdin_consumed: false,
        });
    }

    let stdin_is_terminal = std::io::stdin().is_terminal();
    if input_files.is_empty() && stdin_is_terminal {
        return Ok(StarlarkReplInput {
            initial_data: if cli.slurp {
                Value::Array(Vec::new())
            } else {
                Value::Null
            },
            input_formats: Vec::new(),
            stdin_consumed: false,
        });
    }

    let (documents, stdin_consumed) = if input_files.is_empty() {
        (
            read_stdin_with_tabular_coercion(
                cli.input_format,
                cli.raw_input,
                detect_conflicts,
                cli.tabular_coercion,
            )?,
            true,
        )
    } else {
        load_input_documents_from_paths(
            input_files,
            || {
                read_stdin_with_tabular_coercion(
                    cli.input_format,
                    cli.raw_input,
                    detect_conflicts,
                    cli.tabular_coercion,
                )
            },
            |file| {
                read_path_with_tabular_coercion(
                    file,
                    cli.input_format,
                    cli.raw_input,
                    detect_conflicts,
                    cli.tabular_coercion,
                )
            },
        )?
    };

    let input_formats = documents.iter().map(|document| document.format).collect();
    let initial_data = collapse_repl_documents(documents, cli.slurp);
    Ok(StarlarkReplInput {
        initial_data,
        input_formats,
        stdin_consumed,
    })
}

#[cfg(feature = "starlark")]
fn collapse_repl_documents(documents: Vec<InputDocument>, slurp: bool) -> Value {
    if slurp {
        return Value::Array(
            documents
                .into_iter()
                .map(|document| document.value)
                .collect(),
        );
    }

    let mut values = documents.into_iter().map(|document| document.value);
    match (values.next(), values.next()) {
        (None, _) => Value::Null,
        (Some(first), None) => first,
        (Some(first), Some(second)) => {
            let mut out = vec![first, second];
            out.extend(values);
            Value::Array(out)
        }
    }
}

#[cfg(feature = "starlark")]
fn run_starlark_repl_editor_loop(
    cli: &Cli,
    session: &mut StarlarkReplSession,
    initial_data: &Value,
    output_format: Format,
) -> Result<i32, AqError> {
    let completion_state = Arc::new(Mutex::new(AqStarlarkReplCompletionState::default()));
    completion_state
        .lock()
        .expect("repl completion state poisoned")
        .refresh_from_session(session);

    let mut editor = Reedline::create()
        .with_validator(Box::new(AqStarlarkReplValidator))
        .with_completer(Box::new(AqStarlarkReplCompleter::new(Arc::clone(
            &completion_state,
        ))))
        .with_menu(starlark_repl_menu())
        .with_edit_mode(starlark_repl_edit_mode());
    if let Some(history) = starlark_repl_history()? {
        editor = editor.with_history(history);
    }

    let prompt = AqStarlarkReplPrompt;
    let mut output_format = output_format;
    let mut stderr = std::io::stderr();
    writeln!(
        stderr,
        "{}",
        starlark_repl_banner_line(session, initial_data, output_format)
    )
    .map_err(|error| AqError::io(None, error))?;
    loop {
        completion_state
            .lock()
            .expect("repl completion state poisoned")
            .refresh_from_session(session);
        match editor
            .read_line(&prompt)
            .map_err(|error| AqError::message(format!("repl input failed: {error}")))?
        {
            Signal::Success(line) => {
                let line = line.trim_end();
                if line.is_empty() {
                    continue;
                }
                if line.starts_with(':') {
                    match handle_starlark_repl_command(
                        cli,
                        line,
                        session,
                        initial_data,
                        &mut output_format,
                        &mut stderr,
                    ) {
                        Ok(true) => break,
                        Ok(false) => {}
                        Err(error) => {
                            eprintln!("{error}");
                        }
                    }
                    continue;
                }
                match session.evaluate(line) {
                    Ok(Some(value)) => write_starlark_repl_result(cli, output_format, value)?,
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("{error}");
                    }
                }
            }
            Signal::CtrlC => {
                eprintln!("^C");
                continue;
            }
            Signal::CtrlD => break,
        }
    }
    Ok(0)
}

#[cfg(feature = "starlark")]
fn run_starlark_repl_loop<R: BufRead, W: Write>(
    cli: &Cli,
    session: &mut StarlarkReplSession,
    initial_data: &Value,
    output_format: Format,
    reader: &mut R,
    prompt_writer: &mut W,
    show_prompts: bool,
) -> Result<i32, AqError> {
    let mut buffer = String::new();
    let mut output_format = output_format;

    if show_prompts {
        let banner = starlark_repl_banner_line(session, initial_data, output_format);
        prompt_writer
            .write_all(banner.as_bytes())
            .map_err(|error| AqError::io(None, error))?;
        prompt_writer
            .write_all(b"\n")
            .map_err(|error| AqError::io(None, error))?;
    }

    loop {
        if show_prompts {
            let prompt = if buffer.is_empty() {
                STARLARK_REPL_PROMPT
            } else {
                STARLARK_REPL_CONTINUATION_PROMPT
            };
            prompt_writer
                .write_all(prompt.as_bytes())
                .map_err(|error| AqError::io(None, error))?;
            prompt_writer
                .flush()
                .map_err(|error| AqError::io(None, error))?;
        }

        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| AqError::io(None, error))?;
        if bytes == 0 {
            if buffer.trim().is_empty() {
                return Ok(0);
            }
            if repl_needs_more_input(&buffer) {
                prompt_writer
                    .write_all(b"incomplete input\n")
                    .map_err(|error| AqError::io(None, error))?;
                return Ok(0);
            }
        }

        let line = line.trim_end_matches(['\n', '\r']);
        if buffer.is_empty() && line.starts_with(':') {
            match handle_starlark_repl_command(
                cli,
                line,
                session,
                initial_data,
                &mut output_format,
                prompt_writer,
            ) {
                Ok(true) => return Ok(0),
                Ok(false) => {}
                Err(error) => {
                    let message = format!("{error}\n");
                    prompt_writer
                        .write_all(message.as_bytes())
                        .map_err(|write_error| AqError::io(None, write_error))?;
                }
            }
            if bytes == 0 {
                return Ok(0);
            }
            continue;
        }

        if !line.is_empty() {
            buffer.push_str(line);
        }
        buffer.push('\n');

        if bytes != 0 && repl_needs_more_input(&buffer) {
            continue;
        }

        match session.evaluate(&buffer) {
            Ok(Some(value)) => write_starlark_repl_result(cli, output_format, value)?,
            Ok(None) => {}
            Err(error) => {
                let message = format!("{error}\n");
                prompt_writer
                    .write_all(message.as_bytes())
                    .map_err(|write_error| AqError::io(None, write_error))?;
            }
        }
        buffer.clear();

        if bytes == 0 {
            return Ok(0);
        }
    }
}

#[cfg(feature = "starlark")]
fn handle_starlark_repl_command<W: Write>(
    cli: &Cli,
    line: &str,
    session: &mut StarlarkReplSession,
    initial_data: &Value,
    output_format: &mut Format,
    prompt_writer: &mut W,
) -> Result<bool, AqError> {
    let (command, args) = split_repl_command(line);
    match command {
        ":quit" | ":exit" => Ok(true),
        ":help" => {
            prompt_writer
                .write_all(
                    b":help             show repl commands\n:quit             exit the repl\n:exit             exit the repl\n:data [EXPR]      print current data or evaluate EXPR and replace it\n:type EXPR        evaluate EXPR and print its Starlark type\n:doc NAME         show a signature and summary for NAME\n:constants        print well-known repl constants and literals\n:capabilities     print enabled Starlark capabilities\n:globals          print current top-level names\n:aq [PREFIX]      print aq helper names, optionally filtered by prefix\n:load PATH        evaluate a starlark file inside the current session\n:format [FORMAT]  print or change the repl output format\n:pwd              print the current base directory\n:reset            restore the original session data and bindings\n",
                )
                .map_err(|error| AqError::io(None, error))?;
            Ok(false)
        }
        ":data" => {
            if let Some(source) = args {
                let value = session.set_data_from_source(source)?;
                write_starlark_repl_result(cli, *output_format, value)?;
            } else {
                let value = session.current_data()?;
                write_starlark_repl_result(cli, *output_format, value)?;
            }
            Ok(false)
        }
        ":constants" => {
            write_starlark_repl_result(cli, *output_format, starlark_repl_constants_value())?;
            Ok(false)
        }
        ":capabilities" => {
            write_starlark_repl_result(
                cli,
                *output_format,
                starlark_repl_capabilities_value(session),
            )?;
            Ok(false)
        }
        ":type" => {
            let Some(source) = args else {
                prompt_writer
                    .write_all(b":type requires an expression\n")
                    .map_err(|error| AqError::io(None, error))?;
                return Ok(false);
            };
            let value = StarlarkReplValue::Aq(Value::String(session.evaluate_type(source)?));
            write_starlark_repl_result(cli, *output_format, value)?;
            Ok(false)
        }
        ":doc" => {
            let Some(name) = args else {
                prompt_writer
                    .write_all(b":doc requires a name\n")
                    .map_err(|error| AqError::io(None, error))?;
                return Ok(false);
            };
            let Some(value) = starlark_repl_doc_value(session, name) else {
                let message = format!("no documentation found for {name}\n");
                prompt_writer
                    .write_all(message.as_bytes())
                    .map_err(|error| AqError::io(None, error))?;
                return Ok(false);
            };
            write_starlark_repl_result(cli, *output_format, value)?;
            Ok(false)
        }
        ":globals" => {
            let names = session
                .names()
                .into_iter()
                .map(Value::String)
                .collect::<Vec<_>>();
            write_starlark_repl_result(
                cli,
                *output_format,
                StarlarkReplValue::Aq(Value::Array(names)),
            )?;
            Ok(false)
        }
        ":aq" => {
            let names = session
                .aq_names()
                .iter()
                .filter(|name| args.is_none_or(|prefix| name.starts_with(prefix)))
                .map(|name| Value::String(format!("aq.{name}")))
                .collect::<Vec<_>>();
            write_starlark_repl_result(
                cli,
                *output_format,
                StarlarkReplValue::Aq(Value::Array(names)),
            )?;
            Ok(false)
        }
        ":load" => {
            let Some(path) = args else {
                prompt_writer
                    .write_all(b":load requires a path\n")
                    .map_err(|error| AqError::io(None, error))?;
                return Ok(false);
            };
            if let Some(value) = session.evaluate_file_in_session(Path::new(path))? {
                write_starlark_repl_result(cli, *output_format, value)?;
            }
            Ok(false)
        }
        ":format" => {
            if let Some(format) = args {
                *output_format = parse_repl_output_format(format)?;
            }
            let message = format!("output format: {}\n", output_format);
            prompt_writer
                .write_all(message.as_bytes())
                .map_err(|error| AqError::io(None, error))?;
            Ok(false)
        }
        ":pwd" => {
            write_starlark_repl_result(
                cli,
                *output_format,
                StarlarkReplValue::Aq(Value::String(
                    session.base_dir().to_string_lossy().into_owned(),
                )),
            )?;
            Ok(false)
        }
        ":reset" => {
            session.reset(initial_data)?;
            Ok(false)
        }
        _ => {
            let message = format!("unknown repl command: {line}\n");
            prompt_writer
                .write_all(message.as_bytes())
                .map_err(|error| AqError::io(None, error))?;
            Ok(false)
        }
    }
}

fn split_repl_command(line: &str) -> (&str, Option<&str>) {
    let trimmed = line.trim();
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let command = parts.next().unwrap_or(trimmed);
    let args = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    (command, args)
}

#[cfg(feature = "starlark")]
fn find_completion_start(prefix: &str) -> usize {
    let mut start = prefix.len();
    for (index, ch) in prefix.char_indices().rev() {
        if ch.is_whitespace() || "([{,:=+-*/%<>!&|^;".contains(ch) {
            break;
        }
        start = index;
    }
    start
}

#[cfg(feature = "starlark")]
fn complete_described_word_candidates(
    prefix: &str,
    candidates: &[(&str, &str)],
    append_whitespace: bool,
) -> Vec<AqStarlarkReplCompletion> {
    let mut completions = candidates
        .iter()
        .filter(|(candidate, _)| candidate.starts_with(prefix))
        .map(|(candidate, description)| {
            AqStarlarkReplCompletion::new(
                *candidate,
                *candidate,
                Some((*description).to_string()),
                append_whitespace,
            )
        })
        .collect::<Vec<_>>();
    completions.sort_by(|left, right| left.value.cmp(&right.value));
    completions.dedup_by(|left, right| left.value == right.value);
    completions
}

#[cfg(feature = "starlark")]
fn complete_path_candidates(base_dir: &Path, raw_prefix: &str) -> Vec<AqStarlarkReplCompletion> {
    let (dir_prefix, file_prefix) = split_path_prefix(raw_prefix);
    let search_dir = if dir_prefix.as_os_str().is_empty() {
        base_dir.to_path_buf()
    } else if dir_prefix.is_absolute() {
        dir_prefix.clone()
    } else {
        base_dir.join(&dir_prefix)
    };
    let Ok(entries) = std::fs::read_dir(&search_dir) else {
        return Vec::new();
    };

    let mut completions = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !file_name.starts_with(file_prefix) {
                return None;
            }
            let mut replacement = if dir_prefix.as_os_str().is_empty() {
                file_name.into_owned()
            } else {
                dir_prefix
                    .join(file_name.as_ref())
                    .to_string_lossy()
                    .into_owned()
            };
            if entry.file_type().ok()?.is_dir() {
                replacement.push(std::path::MAIN_SEPARATOR);
            }
            let description = if replacement.ends_with(std::path::MAIN_SEPARATOR) {
                Some("directory".to_string())
            } else {
                Some("path".to_string())
            };
            Some(AqStarlarkReplCompletion::new(
                replacement.clone(),
                replacement,
                description,
                false,
            ))
        })
        .collect::<Vec<_>>();
    completions.sort_by(|left, right| left.value.cmp(&right.value));
    completions
}

#[cfg(feature = "starlark")]
fn split_path_prefix(prefix: &str) -> (PathBuf, &str) {
    let path = Path::new(prefix);
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => (
            parent.to_path_buf(),
            path.file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default(),
        ),
        _ => (PathBuf::new(), prefix),
    }
}

#[cfg(feature = "starlark")]
fn repl_history_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("AQ_STARLARK_REPL_HISTORY") {
        return Some(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
        return Some(PathBuf::from(path).join("aq/starlark-history.txt"));
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Some(PathBuf::from(home).join(".local/state/aq/starlark-history.txt"));
    }
    std::env::var_os("USERPROFILE")
        .map(PathBuf::from)
        .map(|path| path.join(".aq/starlark-history.txt"))
}

fn parse_repl_output_format(raw: &str) -> Result<Format, AqError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "json" => Ok(Format::Json),
        "jsonl" => Ok(Format::Jsonl),
        "toml" => Ok(Format::Toml),
        "yaml" => Ok(Format::Yaml),
        "csv" => Ok(Format::Csv),
        "tsv" => Ok(Format::Tsv),
        "table" => Ok(Format::Table),
        other => Err(AqError::message(format!(
            "unknown repl output format `{other}`"
        ))),
    }
}

#[cfg(feature = "starlark")]
fn write_starlark_repl_result(
    cli: &Cli,
    output_format: Format,
    value: StarlarkReplValue,
) -> Result<(), AqError> {
    let rendered = match value {
        StarlarkReplValue::Aq(value) => {
            let rendered = render(&[value], output_format, cli.render_options())?;
            maybe_colorize_stdout(cli, output_format, rendered)
        }
        StarlarkReplValue::Starlark(text) => append_output_terminator(text, cli.raw_output_mode()),
    };
    std::io::stdout()
        .write_all(rendered.as_bytes())
        .map_err(|error| AqError::io(None, error))
}

#[cfg(feature = "starlark")]
fn open_repl_input_tty() -> Result<std::fs::File, AqError> {
    #[cfg(unix)]
    {
        std::fs::File::open("/dev/tty").map_err(|error| {
            AqError::message(format!(
                "failed to open /dev/tty for repl commands: {error}"
            ))
        })
    }
    #[cfg(windows)]
    {
        return std::fs::File::open("CONIN$").map_err(|error| {
            AqError::message(format!("failed to open CONIN$ for repl commands: {error}"))
        });
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(AqError::message(
            "starlark repl needs terminal input on this platform, but no tty helper is implemented",
        ))
    }
}

#[cfg(feature = "starlark")]
fn open_repl_output_tty() -> Result<std::fs::File, AqError> {
    #[cfg(unix)]
    {
        std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/tty")
            .map_err(|error| {
                AqError::message(format!("failed to open /dev/tty for repl output: {error}"))
            })
    }
    #[cfg(windows)]
    {
        return std::fs::OpenOptions::new()
            .write(true)
            .open("CONOUT$")
            .map_err(|error| {
                AqError::message(format!("failed to open CONOUT$ for repl output: {error}"))
            });
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(AqError::message(
            "starlark repl needs terminal output on this platform, but no tty helper is implemented",
        ))
    }
}

#[cfg(feature = "starlark")]
fn repl_needs_more_input(source: &str) -> bool {
    let mut stack = Vec::new();
    let mut in_string = None;
    let mut escaped = false;
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        if let Some(quote) = in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                value if value == quote => in_string = None,
                _ => {}
            }
            continue;
        }

        match ch {
            '\'' | '"' => in_string = Some(ch),
            '#' => {
                for next in chars.by_ref() {
                    if next == '\n' {
                        break;
                    }
                }
            }
            '(' | '[' | '{' => stack.push(ch),
            ')' | ']' | '}' => {
                let _ = stack.pop();
            }
            _ => {}
        }
    }

    if in_string.is_some() || !stack.is_empty() {
        return true;
    }

    source
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .is_some_and(|line| line.trim_end().ends_with(':'))
}

#[cfg(feature = "starlark")]
fn evaluate_starlark_program(
    cli: &Cli,
    input: &Value,
    context: &StarlarkContext,
) -> Result<Value, AqError> {
    if let Some(path) = &cli.starlark_file {
        return evaluate_file(path, input, context);
    }

    let source = cli
        .expression
        .as_deref()
        .ok_or_else(|| AqError::message("--starlark requires inline source"))?;
    evaluate_inline(source, input, context)
}

fn build_query_bindings(
    cli: &Cli,
    detect_conflicts: DetectConflictPolicy,
) -> Result<IndexMap<String, Value>, AqError> {
    let mut bindings = IndexMap::new();

    for pair in cli.arg.chunks_exact(2) {
        let name = parse_query_binding_name(&pair[0])?;
        bindings.insert(name, Value::String(pair[1].clone()));
    }

    for pair in cli.argjson.chunks_exact(2) {
        let name = parse_query_binding_name(&pair[0])?;
        let parsed = parse_json_str(&pair[1]).map_err(|error| {
            AqError::message(format!(
                "--argjson {name} failed to parse JSON value: {error}"
            ))
        })?;
        bindings.insert(name, parsed);
    }

    for pair in cli.rawfile.chunks_exact(2) {
        let name = parse_query_binding_name(&pair[0])?;
        let path = PathBuf::from(&pair[1]);
        let contents = std::fs::read_to_string(&path)
            .map_err(|error| AqError::io(Some(path.clone()), error))?;
        bindings.insert(name, Value::String(contents));
    }

    for pair in cli.slurpfile.chunks_exact(2) {
        let name = parse_query_binding_name(&pair[0])?;
        let path = PathBuf::from(&pair[1]);
        let documents = read_path_with_tabular_coercion(
            &path,
            cli.input_format,
            false,
            detect_conflicts,
            cli.tabular_coercion,
        )?;
        bindings.insert(
            name,
            Value::Array(
                documents
                    .into_iter()
                    .map(|document| document.value)
                    .collect(),
            ),
        );
    }

    let positional = if cli.args {
        cli.files
            .iter()
            .map(|value| Value::String(value.to_string_lossy().into_owned()))
            .collect::<Vec<_>>()
    } else if cli.jsonargs {
        let mut values = Vec::with_capacity(cli.files.len());
        for raw in &cli.files {
            let raw = raw.to_string_lossy();
            let parsed = parse_json_str(&raw).map_err(|error| {
                AqError::message(format!(
                    "--jsonargs failed to parse JSON value `{raw}`: {error}"
                ))
            })?;
            values.push(parsed);
        }
        values
    } else {
        Vec::new()
    };

    let named_bindings = bindings.clone();
    let mut args = IndexMap::new();
    args.insert("named".to_string(), Value::Object(named_bindings));
    args.insert("positional".to_string(), Value::Array(positional));
    bindings.insert("ARGS".to_string(), Value::Object(args));

    Ok(bindings)
}

fn parse_query_binding_name(raw: &str) -> Result<String, AqError> {
    let mut chars = raw.chars();
    let Some(first) = chars.next() else {
        return Err(AqError::message("query variable names cannot be empty"));
    };
    if !is_query_identifier_start(first) {
        return Err(AqError::message(format!(
            "invalid query variable name `{raw}`"
        )));
    }
    if !chars.all(is_query_identifier_continue) {
        return Err(AqError::message(format!(
            "invalid query variable name `{raw}`"
        )));
    }
    Ok(raw.to_string())
}

fn is_query_identifier_start(value: char) -> bool {
    value == '_' || value.is_ascii_alphabetic()
}

fn is_query_identifier_continue(value: char) -> bool {
    value == '_' || value.is_ascii_alphanumeric()
}

fn validate_in_place_mode(cli: &Cli, input_files: &[PathBuf]) -> Result<(), AqError> {
    if input_files.is_empty() {
        return Err(AqError::message(
            "--in-place requires one or more file arguments",
        ));
    }
    if input_files.iter().any(|path| is_stdin_sentinel(path)) {
        return Err(AqError::message(
            "--in-place does not support `-`; rewrite a real file path instead",
        ));
    }
    if cli.null_input {
        return Err(AqError::message(
            "--in-place cannot be combined with --null-input",
        ));
    }
    if cli.stream {
        return Err(AqError::message(
            "--in-place cannot be combined with --stream",
        ));
    }
    if cli.explain {
        return Err(AqError::message(
            "--in-place cannot be combined with --explain",
        ));
    }
    if cli.raw_input {
        return Err(AqError::message(
            "--in-place does not support --raw-input in this slice",
        ));
    }
    if cli.uses_raw_output() {
        return Err(AqError::message(
            "--in-place does not support --raw-output, --raw-output0, or --join-output in this slice",
        ));
    }
    Ok(())
}

fn run_query_in_place(
    cli: &Cli,
    query: &crate::query::Query,
    bindings: &IndexMap<String, Value>,
    input_files: &[PathBuf],
    detect_conflicts: DetectConflictPolicy,
) -> Result<i32, AqError> {
    for file in input_files {
        let documents = read_path_with_tabular_coercion(
            file,
            cli.input_format,
            cli.raw_input,
            detect_conflicts,
            cli.tabular_coercion,
        )?;
        let input_formats: Vec<Format> = documents.iter().map(|document| document.format).collect();
        let results = evaluate_documents(query, bindings, documents, cli.slurp, false)?;
        rewrite_file(file, cli, &results, &input_formats)?;
    }
    Ok(0)
}

#[cfg(feature = "starlark")]
fn run_starlark_in_place(
    cli: &Cli,
    input_files: &[PathBuf],
    detect_conflicts: DetectConflictPolicy,
) -> Result<i32, AqError> {
    let capabilities = StarlarkCapabilities::from_flags(
        cli.starlark_filesystem,
        cli.starlark_environment,
        cli.starlark_time,
        cli.starlark_unsafe,
    );
    let base_dir = starlark_base_dir(cli)?;

    for file in input_files {
        let documents = read_path_with_tabular_coercion(
            file,
            cli.input_format,
            cli.raw_input,
            detect_conflicts,
            cli.tabular_coercion,
        )?;
        let input_formats: Vec<Format> = documents.iter().map(|document| document.format).collect();
        let results =
            evaluate_starlark_documents(cli, documents, capabilities, detect_conflicts, &base_dir)?;
        rewrite_file(file, cli, &results, &input_formats)?;
    }
    Ok(0)
}

fn rewrite_file(
    path: &Path,
    cli: &Cli,
    results: &[Value],
    input_formats: &[Format],
) -> Result<(), AqError> {
    let output_format = cli.output_format.unwrap_or_else(|| {
        if input_formats.is_empty() {
            cli.input_format
                .or_else(|| detect_format_from_path(path))
                .unwrap_or(Format::Json)
        } else {
            default_output_format(input_formats)
        }
    });
    validate_in_place_output(results, output_format)?;
    let rendered = render(results, output_format, cli.render_options())?;
    write_atomically(path, &rendered)
}

fn validate_in_place_output(results: &[Value], format: Format) -> Result<(), AqError> {
    match format {
        Format::Json if results.len() != 1 => Err(AqError::message(
            "--in-place requires exactly one result when writing json",
        )),
        Format::Toml if results.len() != 1 => Err(AqError::message(
            "--in-place requires exactly one result when writing toml",
        )),
        Format::Table => Err(AqError::message("--in-place does not support table output")),
        Format::Json | Format::Jsonl | Format::Toml | Format::Yaml | Format::Csv | Format::Tsv => {
            Ok(())
        }
    }
}

fn evaluate_documents(
    query: &crate::query::Query,
    bindings: &IndexMap<String, Value>,
    documents: Vec<InputDocument>,
    slurp: bool,
    null_input: bool,
) -> Result<Vec<Value>, AqError> {
    let context = EvaluationContext::from_remaining_inputs(logical_input_stream(documents, slurp));
    if null_input {
        return Ok(
            evaluate_with_bindings_and_context(query, &Value::Null, bindings, &context)?.into_vec(),
        );
    }

    let mut out = Vec::new();
    while let Some(input) = context.pop_next_input() {
        out.extend(
            evaluate_with_bindings_and_context(query, &input, bindings, &context)?.into_vec(),
        );
    }
    Ok(out)
}

fn is_stdin_sentinel(path: &Path) -> bool {
    path.as_os_str() == "-"
}

fn load_input_documents_from_paths<ReadStdin, ReadPath>(
    input_files: &[PathBuf],
    mut read_stdin: ReadStdin,
    mut read_path: ReadPath,
) -> Result<(Vec<InputDocument>, bool), AqError>
where
    ReadStdin: FnMut() -> Result<Vec<InputDocument>, AqError>,
    ReadPath: FnMut(&Path) -> Result<Vec<InputDocument>, AqError>,
{
    let mut documents = Vec::new();
    let mut stdin_consumed = false;
    for file in input_files {
        if is_stdin_sentinel(file) {
            if stdin_consumed {
                return Err(AqError::message(
                    "stdin may be referenced at most once with `-`",
                ));
            }
            documents.extend(read_stdin()?);
            stdin_consumed = true;
        } else {
            documents.extend(read_path(file)?);
        }
    }
    Ok((documents, stdin_consumed))
}

struct PartialDocumentEvaluation {
    values: Vec<Value>,
    error: AqError,
}

fn evaluate_documents_preserving_partial(
    query: &crate::query::Query,
    bindings: &IndexMap<String, Value>,
    documents: Vec<InputDocument>,
    slurp: bool,
    null_input: bool,
) -> Result<Vec<Value>, PartialDocumentEvaluation> {
    let context = EvaluationContext::from_remaining_inputs(logical_input_stream(documents, slurp));
    if null_input {
        return evaluate_with_bindings_and_context_preserving_partial(
            query,
            &Value::Null,
            bindings,
            &context,
        )
        .map(|values| values.into_vec())
        .map_err(|partial| PartialDocumentEvaluation {
            values: partial.values,
            error: partial.error,
        });
    }

    let mut out = Vec::new();
    while let Some(input) = context.pop_next_input() {
        match evaluate_with_bindings_and_context_preserving_partial(
            query, &input, bindings, &context,
        ) {
            Ok(values) => out.extend(values.into_vec()),
            Err(partial) => {
                out.extend(partial.values);
                return Err(PartialDocumentEvaluation {
                    values: out,
                    error: partial.error,
                });
            }
        }
    }
    Ok(out)
}

fn logical_input_stream(documents: Vec<InputDocument>, slurp: bool) -> Vec<Value> {
    if slurp {
        return vec![Value::Array(
            documents
                .into_iter()
                .map(|document| document.value)
                .collect(),
        )];
    }

    documents
        .into_iter()
        .map(|document| document.value)
        .collect()
}

#[cfg(feature = "starlark")]
fn evaluate_starlark_documents(
    cli: &Cli,
    documents: Vec<InputDocument>,
    capabilities: StarlarkCapabilities,
    detect_conflicts: DetectConflictPolicy,
    base_dir: &Path,
) -> Result<Vec<Value>, AqError> {
    let input_formats: Vec<Format> = documents.iter().map(|document| document.format).collect();
    if cli.slurp {
        let context = StarlarkContext::new(
            capabilities,
            detect_conflicts,
            starlark_format_name(&input_formats),
            base_dir.to_path_buf(),
        );
        let array = Value::Array(
            documents
                .into_iter()
                .map(|document| document.value)
                .collect(),
        );
        return Ok(vec![evaluate_starlark_program(cli, &array, &context)?]);
    }

    let mut out = Vec::new();
    for document in documents {
        let context = StarlarkContext::new(
            capabilities,
            detect_conflicts,
            Some(document.format.to_string()),
            base_dir.to_path_buf(),
        );
        out.push(evaluate_starlark_program(cli, &document.value, &context)?);
    }
    Ok(out)
}

#[cfg(feature = "starlark")]
fn starlark_format_name(formats: &[Format]) -> Option<String> {
    if formats.is_empty() {
        return None;
    }
    let first = formats[0];
    if formats.iter().all(|format| *format == first) {
        Some(first.to_string())
    } else {
        Some("mixed".to_string())
    }
}

#[cfg(feature = "starlark")]
fn starlark_base_dir(cli: &Cli) -> Result<PathBuf, AqError> {
    if let Some(path) = &cli.starlark_file {
        return Ok(path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf());
    }

    std::env::current_dir().map_err(|error| AqError::io(None, error))
}

fn run_streaming(
    cli: &Cli,
    query: &crate::query::Query,
    bindings: &IndexMap<String, Value>,
    detect_conflicts: DetectConflictPolicy,
) -> Result<i32, AqError> {
    if cli.slurp {
        return Err(AqError::message("cannot combine --stream with --slurp"));
    }

    if cli.null_input {
        return Err(AqError::message(
            "--stream does not support --null-input in this slice because stream mode requires line-oriented input; rerun without --stream",
        ));
    }

    let input_files = cli.input_files();
    let mut stdout = std::io::stdout();
    if input_files.is_empty() {
        let stdin = std::io::stdin();
        let reader = BufReader::new(stdin.lock());
        stream_reader(
            reader,
            None,
            cli,
            query,
            bindings,
            detect_conflicts,
            &mut stdout,
        )?;
        return Ok(0);
    }

    let mut stdin_consumed = false;
    for file in &input_files {
        if is_stdin_sentinel(file) {
            if stdin_consumed {
                return Err(AqError::message(
                    "stdin may be referenced at most once with `-`",
                ));
            }
            stdin_consumed = true;
            let stdin = std::io::stdin();
            let reader = BufReader::new(stdin.lock());
            stream_reader(
                reader,
                None,
                cli,
                query,
                bindings,
                detect_conflicts,
                &mut stdout,
            )?;
            continue;
        }

        let handle =
            std::fs::File::open(file).map_err(|error| AqError::io(Some(file.clone()), error))?;
        let reader = BufReader::new(handle);
        stream_reader(
            reader,
            Some(file.as_path()),
            cli,
            query,
            bindings,
            detect_conflicts,
            &mut stdout,
        )?;
    }
    Ok(0)
}

fn stream_reader<R: BufRead>(
    mut reader: R,
    path: Option<&std::path::Path>,
    cli: &Cli,
    query: &crate::query::Query,
    bindings: &IndexMap<String, Value>,
    detect_conflicts: DetectConflictPolicy,
    stdout: &mut dyn Write,
) -> Result<(), AqError> {
    let mut first_non_empty = None;
    let mut non_empty_lines = 0usize;
    let mut buffered_lines = Vec::new();
    let mut next_line_number = 1usize;
    loop {
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| AqError::io(path.map(|path| path.to_path_buf()), error))?;
        if bytes == 0 {
            break;
        }
        if !line.trim().is_empty() && first_non_empty.is_none() {
            first_non_empty = Some(line.trim().to_string());
        }
        if !line.trim().is_empty() {
            non_empty_lines += 1;
        }
        buffered_lines.push((next_line_number, line));
        next_line_number += 1;
        if non_empty_lines >= 2 {
            break;
        }
    }

    if !cli.raw_input && first_non_empty.is_none() {
        return Ok(());
    }

    let preview = buffered_lines
        .iter()
        .map(|(_, line)| line.as_str())
        .collect::<String>();
    let input_format = resolve_stream_input_format(path, cli, detect_conflicts, &preview)?;
    let output_format = cli
        .output_format
        .unwrap_or_else(|| default_stream_output_format(cli, input_format));
    let plan = StreamPlan {
        cli,
        query,
        bindings,
        output_format,
        render_options: cli.render_options(),
    };

    for (line_number, line) in buffered_lines {
        stream_line(&line, path, line_number, input_format, &plan, stdout)?;
    }

    loop {
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| AqError::io(path.map(|path| path.to_path_buf()), error))?;
        if bytes == 0 {
            break;
        }
        stream_line(&line, path, next_line_number, input_format, &plan, stdout)?;
        next_line_number += 1;
    }

    Ok(())
}

fn resolve_stream_input_format(
    path: Option<&std::path::Path>,
    cli: &Cli,
    detect_conflicts: DetectConflictPolicy,
    preview: &str,
) -> Result<Format, AqError> {
    if cli.raw_input {
        return Ok(Format::Json);
    }

    if let Some(format) = cli.input_format {
        return match format {
            Format::Jsonl => Ok(Format::Jsonl),
            other => Err(AqError::message(unsupported_stream_input_message(
                path, other, preview,
            ))),
        };
    }

    match detect_format_for_input(path, preview, detect_conflicts) {
        Format::Jsonl => Ok(Format::Jsonl),
        other => Err(AqError::message(unsupported_stream_input_message(
            path, other, preview,
        ))),
    }
}

fn unsupported_stream_input_message(
    path: Option<&std::path::Path>,
    detected: Format,
    preview: &str,
) -> String {
    let source = match path {
        Some(path) => format!(" for {}", path.display()),
        None => String::new(),
    };
    let mut message = format!(
        "--stream currently supports only jsonl input or --raw-input, not {detected}{source}"
    );
    match detected {
        Format::Json => {
            message.push_str("; rerun without --stream for document-at-a-time evaluation")
        }
        Format::Toml | Format::Yaml => {
            message.push_str("; rerun without --stream for document-at-a-time evaluation because ");
            message.push_str(match detected {
                Format::Toml => "toml input is document-oriented in this slice",
                Format::Yaml => "yaml input is document-oriented in this slice",
                _ => unreachable!(),
            });
        }
        Format::Csv | Format::Tsv => {
            message.push_str("; rerun without --stream for file-at-a-time tabular evaluation or convert the input to jsonl first");
        }
        Format::Table => {
            message.push_str("; table is an output-only format");
        }
        Format::Jsonl => {}
    }
    if detected == Format::Json && looks_like_single_json_line(preview) {
        message.push_str(" or pass -f jsonl if the input is one JSON value per line");
    }
    message
}

fn looks_like_single_json_line(input: &str) -> bool {
    let mut non_empty_lines = input.lines().map(str::trim).filter(|line| !line.is_empty());
    let Some(line) = non_empty_lines.next() else {
        return false;
    };
    if non_empty_lines.next().is_some() {
        return false;
    }
    parse_json_str(line).is_ok()
}

fn default_stream_output_format(cli: &Cli, input_format: Format) -> Format {
    if cli.raw_input {
        Format::Json
    } else {
        input_format
    }
}

struct StreamPlan<'a> {
    cli: &'a Cli,
    query: &'a crate::query::Query,
    bindings: &'a IndexMap<String, Value>,
    output_format: Format,
    render_options: RenderOptions,
}

fn stream_line(
    line: &str,
    path: Option<&std::path::Path>,
    line_number: usize,
    input_format: Format,
    plan: &StreamPlan<'_>,
    stdout: &mut dyn Write,
) -> Result<(), AqError> {
    let trimmed = line.trim_end_matches(['\n', '\r']);
    let value = match input_format {
        Format::Jsonl => {
            if trimmed.trim().is_empty() {
                return Ok(());
            }
            parse_json_str(trimmed).map_err(|error| AqError::ParseInput {
                format: "jsonl",
                message: stream_jsonl_parse_location(path, line_number, &error),
            })?
        }
        Format::Json => Value::String(trimmed.to_string()),
        Format::Toml => {
            return Err(AqError::message(
                "--stream does not support toml input in this slice",
            ))
        }
        Format::Yaml => {
            return Err(AqError::message(
                "--stream does not support yaml input in this slice",
            ))
        }
        Format::Csv => {
            return Err(AqError::message(
                "--stream does not support csv input in this slice",
            ))
        }
        Format::Tsv => {
            return Err(AqError::message(
                "--stream does not support tsv input in this slice",
            ))
        }
        Format::Table => {
            return Err(AqError::message(
                "--stream does not support table input because table is an output-only format",
            ))
        }
    };

    let results = evaluate_with_bindings_and_context(
        plan.query,
        &value,
        plan.bindings,
        &EvaluationContext::empty(),
    )?
    .into_vec();
    let rendered = render(&results, plan.output_format, plan.render_options).map_err(|error| {
        AqError::message(format!(
            "stream result at {}: {error}",
            stream_record_location(path, line_number)
        ))
    })?;
    let rendered = maybe_colorize_stdout(plan.cli, plan.output_format, rendered);
    stdout
        .write_all(rendered.as_bytes())
        .map_err(|error| AqError::io(None, error))?;
    Ok(())
}

fn stream_jsonl_parse_location(
    path: Option<&std::path::Path>,
    line_number: usize,
    message: &str,
) -> String {
    let location = stream_record_location(path, line_number);
    format!("{location}: {message}")
}

fn stream_record_location(path: Option<&std::path::Path>, line_number: usize) -> String {
    match path {
        Some(path) => format!("{}: line {line_number}", path.display()),
        None => format!("line {line_number}"),
    }
}

fn maybe_colorize_stdout(cli: &Cli, format: Format, rendered: String) -> String {
    if cli.use_color() && !cli.uses_raw_output() {
        colorize(&rendered, format)
    } else {
        rendered
    }
}

fn append_output_terminator(mut text: String, terminator: OutputTerminator) -> String {
    match terminator {
        OutputTerminator::Newline => text.push('\n'),
        OutputTerminator::Nul => text.push('\0'),
        OutputTerminator::None => {}
    }
    text
}

#[cfg(feature = "starlark")]
fn load_documents(
    cli: &Cli,
    input_files: &[PathBuf],
    detect_conflicts: DetectConflictPolicy,
) -> Result<Vec<InputDocument>, AqError> {
    if cli.null_input {
        return Ok(vec![InputDocument {
            value: Value::Null,
            format: cli.input_format.unwrap_or(Format::Json),
        }]);
    }

    if input_files.is_empty() {
        return read_stdin_with_tabular_coercion(
            cli.input_format,
            cli.raw_input,
            detect_conflicts,
            cli.tabular_coercion,
        );
    }

    let (documents, _) = load_input_documents_from_paths(
        input_files,
        || {
            read_stdin_with_tabular_coercion(
                cli.input_format,
                cli.raw_input,
                detect_conflicts,
                cli.tabular_coercion,
            )
        },
        |file| {
            read_path_with_tabular_coercion(
                file,
                cli.input_format,
                cli.raw_input,
                detect_conflicts,
                cli.tabular_coercion,
            )
        },
    )?;
    Ok(documents)
}

fn load_query_documents(
    cli: &Cli,
    input_files: &[PathBuf],
    detect_conflicts: DetectConflictPolicy,
) -> Result<Vec<InputDocument>, AqError> {
    if input_files.is_empty() {
        if cli.null_input && std::io::stdin().is_terminal() {
            return Ok(Vec::new());
        }
        return read_query_stdin(
            cli.input_format,
            cli.raw_input,
            detect_conflicts,
            cli.tabular_coercion,
        );
    }

    let (documents, _) = load_input_documents_from_paths(
        input_files,
        || {
            read_query_stdin(
                cli.input_format,
                cli.raw_input,
                detect_conflicts,
                cli.tabular_coercion,
            )
        },
        |file| {
            read_path_with_tabular_coercion(
                file,
                cli.input_format,
                cli.raw_input,
                detect_conflicts,
                cli.tabular_coercion,
            )
        },
    )?;
    Ok(documents)
}

fn read_query_stdin(
    override_format: Option<Format>,
    raw_input: bool,
    detect_conflicts: DetectConflictPolicy,
    tabular_coercion: TabularCoercion,
) -> Result<Vec<InputDocument>, AqError> {
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut std::io::stdin(), &mut bytes)
        .map_err(|error| AqError::io(None, error))?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let input = String::from_utf8(bytes)
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

    let format =
        override_format.unwrap_or_else(|| detect_format_for_input(None, &input, detect_conflicts));
    let documents =
        crate::format::parse_text_with_tabular_coercion(&input, format, tabular_coercion)?;
    Ok(documents
        .into_iter()
        .map(|value| InputDocument { value, format })
        .collect())
}

fn resolve_detect_conflicts(
    cli_value: Option<DetectConflictPolicy>,
) -> Result<DetectConflictPolicy, AqError> {
    if let Some(value) = cli_value {
        return Ok(value);
    }

    let raw = match std::env::var("AQ_DETECT_CONFLICTS") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => {
            return Ok(DetectConflictPolicy::WarnFallback);
        }
        Err(error) => {
            return Err(AqError::message(format!(
                "failed to read AQ_DETECT_CONFLICTS: {error}"
            )));
        }
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "warn-fallback" | "warn_fallback" => Ok(DetectConflictPolicy::WarnFallback),
        "extension" => Ok(DetectConflictPolicy::Extension),
        "sniff" => Ok(DetectConflictPolicy::Sniff),
        other => Err(AqError::message(format!(
            "invalid AQ_DETECT_CONFLICTS value `{other}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "starlark")]
    use crate::app::{
        complete_described_word_candidates, complete_path_candidates, find_completion_start,
        AqStarlarkReplCompletionState,
    };
    #[cfg(feature = "starlark")]
    use crate::error::AqError;
    #[cfg(feature = "starlark")]
    use crate::format::DetectConflictPolicy;
    #[cfg(feature = "starlark")]
    use crate::starlark::{StarlarkCapabilities, StarlarkContext, StarlarkReplSession};
    #[cfg(feature = "starlark")]
    use crate::value::Value;
    #[cfg(feature = "starlark")]
    fn repl_session() -> Result<StarlarkReplSession, AqError> {
        let context = StarlarkContext::new(
            StarlarkCapabilities::from_flags(false, false, false, false),
            DetectConflictPolicy::WarnFallback,
            None,
            std::env::current_dir().expect("cwd should exist"),
        );
        StarlarkReplSession::new(&Value::Null, context)
    }

    #[cfg(feature = "starlark")]
    #[test]
    fn starlark_repl_completion_finds_namespaces_and_commands() {
        let mut state = AqStarlarkReplCompletionState::default();
        state.refresh_from_session(&repl_session().expect("session should build"));

        let (start, completions) = state.complete_candidates("aq.sl", "aq.sl".len());
        assert_eq!(start, 0);
        assert!(completions.iter().any(|completion| {
            completion.value == "aq.slug"
                && completion
                    .description
                    .as_deref()
                    .is_some_and(|description| {
                        description.contains("aq.slug(text)")
                            && description.contains("normalize text to a URL-safe slug")
                    })
        }));

        let (start, completions) = state.complete_candidates("da", "da".len());
        assert_eq!(start, 0);
        assert!(completions.iter().any(|completion| {
            completion.value == "data"
                && completion.description.as_deref()
                    == Some("current input value for this repl session")
        }));

        let (start, completions) = state.complete_candidates("lo", "lo".len());
        assert_eq!(start, 0);
        assert!(completions.iter().any(|completion| {
            completion.value == "log"
                && completion
                    .description
                    .as_deref()
                    .is_some_and(|description| {
                        description.contains("log(value)")
                            && description.contains("write one Starlark value to stderr")
                    })
        }));

        let (start, completions) = state.complete_candidates(":doc aq.sl", ":doc aq.sl".len());
        assert_eq!(start, 5);
        assert!(completions.iter().any(|completion| {
            completion.value == "aq.slug"
                && completion
                    .description
                    .as_deref()
                    .is_some_and(|description| description.contains("aq.slug(text)"))
        }));

        let (start, completions) = state.complete_candidates(":doc lo", ":doc lo".len());
        assert_eq!(start, 5);
        assert!(completions.iter().any(|completion| {
            completion.value == "log"
                && completion
                    .description
                    .as_deref()
                    .is_some_and(|description| description.contains("log(value)"))
        }));

        let (start, completions) = state.complete_candidates(":fo", ":fo".len());
        assert_eq!(start, 1);
        assert!(completions.iter().any(|completion| {
            completion.value == "format"
                && completion.display == ":format"
                && completion
                    .description
                    .as_deref()
                    .is_some_and(|description| {
                        description.contains(":format [FORMAT]")
                            && description.contains("print or change the output format")
                    })
        }));
    }

    #[cfg(feature = "starlark")]
    #[test]
    fn starlark_repl_completion_handles_paths_and_boundaries() {
        let temp_dir =
            std::env::temp_dir().join(format!("aq-repl-completion-{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
        let file_path = temp_dir.join("example.star");
        std::fs::write(&file_path, "x = 1\n").expect("temp file should write");

        let completions = complete_path_candidates(&temp_dir, "ex");
        assert!(completions
            .iter()
            .any(|completion| completion.value == "example.star"));
        let _ = std::fs::remove_file(&file_path);
        let _ = std::fs::remove_dir(&temp_dir);

        assert_eq!(find_completion_start("foo(aq.sl"), 4);
        let completions = complete_described_word_candidates(
            "json",
            &[("json", "render aq-compatible values as JSON")],
            true,
        );
        assert!(completions
            .iter()
            .any(|completion| completion.value == "json"));
    }
}
