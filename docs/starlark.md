# Starlark

`aq` has three Starlark entry points:

- `aq --starlark 'EXPR'`
- `aq --starlark-file path/to/script.star`
- `aq -P`

Inline mode evaluates a Starlark expression with `data` bound to the current input value. File mode executes the file and requires `main(data)`.

For arbitrary no-input Starlark evaluation, use `-n`:

```bash
aq -n --starlark '1 + 2'
```

## Execution Model

- Inline mode executes inline Starlark source as a module and returns the final expression value.
- File mode executes the file, then calls `main(data)`.
- REPL mode evaluates one snippet at a time in a persistent Starlark module, so assignments and `def` statements survive across later inputs.
- `data` is the incoming `aq` value for the current document.
- `aq` is the helper namespace.
- `log(value)` writes one Starlark value to stderr and returns `None`.
- Standard Starlark builtins like `dir`, `len`, `type`, comprehensions, and `load(...)` are available through the embedded engine.
- If you do not pass `-n`, Starlark mode follows normal `aq` input rules:
  - no file arguments means read stdin
  - use `-` as an explicit stdin file argument when you want stdin at a specific file position
  - one or more file arguments means parse those files
  - `--slurp` changes `data` from “current document” to “array of all input documents”
- REPL mode binds one session-level `data` value:
  - `-n` starts with `data = null`
  - `--slurp` starts with `data = [...]` across all loaded input documents
  - one loaded document starts with that value directly
  - multiple loaded documents without `--slurp` collapse into `data = [...]` for one interactive session
  - if stdin is already being used for input data, REPL commands are read from the controlling terminal instead
- Output to stdout is the returned value from the inline program or `main(data)`, rendered through normal `aq` output flags like `-r`, `-c`, and `--output-format`.
- There is no `print(...)` helper. Use `log(value)` for stderr diagnostics. If you want text on stdout, return a string, often via `aq.render(...)`.

## REPL

The REPL is intentionally small, but it is a real persistent session:

```bash
aq -n -P
```

When the REPL is attached to a real terminal, it now uses a line editor:

- arrow-key history navigation
- in-line editing
- tab-triggered IDE-style completion menus for REPL commands, top-level session names, paths, formats, and `aq.*` helpers
- completion details for commands, constants, formats, and individual `aq.*` helpers
- `aq.*` helper details include both a call signature and a one-line summary in the right-hand menu pane
- Shift-Tab and arrow-key menu navigation
- Ctrl-C cancels the current input without leaving the REPL
- Ctrl-D exits from an empty prompt
- multiline entry for `def`, dict/list literals, and other incomplete input

Interactive history is saved between sessions when a writable history path is available:

- `$AQ_STARLARK_REPL_HISTORY` if set
- otherwise `$XDG_STATE_HOME/aq/starlark-history.txt`
- otherwise `$HOME/.local/state/aq/starlark-history.txt`
- on Windows, `%USERPROFILE%/.aq/starlark-history.txt`

If you drive the REPL through a pipe instead of an interactive terminal, `aq` keeps the older line-at-a-time fallback so scripted REPL tests and command feeds still work.

Useful REPL commands:

- `:help`
  Shows the built-in REPL commands.
- `:data`
  Prints the current top-level `data` value.
- `:data EXPR`
  Evaluates `EXPR`, stores the result back into `data`, and prints the new value.
- `:type EXPR`
  Evaluates `EXPR` and prints its Starlark type without changing `ans`.
- `:doc NAME`
  Prints a structured help record for a REPL command, `aq.*` helper, well-known constant, or current session binding.
- `:constants`
  Prints the well-known REPL constants and literals, including `data`, `aq`, `ans`, `prev`, `_`, `None`, `True`, and `False`.
- `:capabilities`
  Prints which restricted Starlark capability groups are enabled in the current session.
- `:globals`
  Prints the current top-level names defined in the session.
- `:aq [PREFIX]`
  Prints the available `aq.*` helper names, optionally filtered by a prefix like `slug` or `sha`.
- `:load PATH`
  Evaluates another Starlark file inside the current session, relative to the current base directory unless `PATH` is absolute.
- `:format`
  Prints the current REPL output format.
- `:format FORMAT`
  Changes the REPL output format for aq-compatible results.
- `:pwd`
  Prints the current base directory used for relative Starlark path operations.
- `:reset`
  Resets the session back to the original `data` value and clears earlier definitions.
- `:quit`, `:exit`
  Leaves the REPL.

The REPL also keeps result variables:

- `ans`
  The most recent non-`None` result.
- `prev`
  The previous `ans` value.
- `_`
  Alias for `ans`.

Examples:

```text
aq> x = 1
aq> x + 2
3
aq> :type x
"int"
aq> aq.sl<TAB>
[menu]
aq.slug
[detail]
aq.slug(text)
normalize text to a URL-safe slug
aq> :doc aq.slug
{"name":"aq.slug","kind":"helper","signature":"aq.slug(text)","description":"normalize text to a URL-safe slug"}
aq> ans * 10
30
aq> def inc(x):
...     return x + 1
aq> inc(4)
5
```

To explore piped input interactively:

```bash
cat config.json | aq -P
```

In that shape, `aq` reads `config.json` from stdin first, binds it to `data`, then switches REPL command input to the controlling terminal.

When the REPL starts on a real terminal, it prints a short session banner with the current `data` shape, detected input format, output format, enabled capability groups, and the main discovery hints.

## Discoverability

The helper surface is introspectable from inside Starlark:

```bash
aq -n --starlark 'dir(aq)'
```

That prints the current `aq.*` helper names, including capability-gated helpers. Names may still require the relevant flag at runtime, for example `aq.read(...)` still needs `--starlark-filesystem`, and `aq.now()` still needs `--starlark-time`.

Inside the REPL, `:aq` is the quick version:

```text
aq> :aq sha
["aq.sha1","aq.sha256","aq.sha512"]
```

For input-aware inspection, the useful top-level anchors are:

- `data`
  The incoming value for the current document, or an array under `--slurp`.
- `aq`
  The helper namespace. Use `dir(aq)` or `:aq` to see the available `aq.*` helpers.
- `ans`, `_`
  The most recent non-`None` result from the REPL. They appear after the first successful result.
- `prev`
  The previous `ans` value. It appears after the second successful non-`None` result.
- `None`, `True`, `False`
  Standard Starlark literals, available in the REPL like any other Starlark module.
- `aq.format()`
  The detected input format for the current document, or `"mixed"` when slurping heterogeneous inputs.
- `aq.base_dir()`
  The current filesystem base directory for relative Starlark path operations.

Starlark is restricted by default. These capabilities are opt-in:

- `--starlark-filesystem` enables path helpers, `aq.read*`, `aq.write*`, and local `load(...)`
- `--starlark-environment` enables `aq.env(...)`
- `--starlark-time` enables `aq.timestamp()`, `aq.now()`, and `aq.today()`
- `--starlark-unsafe` enables all of the above

## Engine

`aq` currently embeds Meta's Rust `starlark` crate. That is an implementation
choice, not a public API promise.

For v1, the supported contract is the CLI behavior documented here: the
available `aq.*` helpers, the capability flags, and the data model visible to
Starlark scripts. Internal embedding details may change later if a different
engine or integration shape becomes a better fit.

## Support Snapshot

`aq` Starlark is in good shape for real CLI work now:

- inline eval with `data`
- file mode via `main(data)`
- a persistent REPL with history, completion, and session commands
- a bridge back into the `aq` query engine
- typed temporal values
- a real structured-data/path helper layer
- optional filesystem, environment, and time capabilities

The easiest way to read the helper surface is by group, not by a flat `dir(aq)` dump.

## Helper Reference

### Type Conventions

| Term | Meaning |
| --- | --- |
| `value` | Any aq-compatible structured value. |
| `values` | `list[value]`. |
| `text` | UTF-8 string. |
| `format` | One of `json`, `jsonl`, `toml`, `yaml`, `csv`, `tsv`, or output-only `table`. |
| `path` | `list[string | int]` path components. |
| `paths` | `list[path]`. |
| `date` | Typed Starlark date value. |
| `datetime` | Typed Starlark datetime value. |
| `timedelta` | Typed Starlark duration value. |
| `transform(value)` | Callback returning a replacement value. |
| `transform(path, value)` | Callback receiving the current path and value, then returning a replacement value. |
| `predicate(path, value)` | Callback returning `bool`. |

### Query and Rendering Helpers

| Helper | Inputs | Returns | Notes |
| --- | --- | --- | --- |
| `aq.format()` | none | `string` | Current detected input format, or `"mixed"` when `--slurp` loaded heterogeneous inputs. |
| `aq.query_all(filter, value)` | `filter: string`, `value: value` | `values` | Runs an `aq` query and returns the full result stream. |
| `aq.query_one(filter, value)` | `filter: string`, `value: value` | `value` | Requires exactly one result. |
| `aq.parse(text, format)` | `text: string`, `format: string` | `value` | Parses one text payload using `aq`'s normal collapse rules. |
| `aq.parse_all(text, format)` | `text: string`, `format: string` | `values` | Always returns every parsed document. |
| `aq.render(value, format, compact = False)` | `value: value`, `format: string`, `compact: bool` | `string` | Renders one value to text. |
| `aq.render_all(values, format, compact = False)` | `values: values`, `format: string`, `compact: bool` | `string` | Renders multiple values using normal `aq` multi-result output semantics. |

### Temporal Helpers

| Helper | Inputs | Returns | Notes |
| --- | --- | --- | --- |
| `aq.date(text)` | `text: string` | `date` | Parses an ISO-style date. |
| `aq.datetime(text)` | `text: string` | `datetime` | Parses an ISO-style datetime. |
| `aq.timedelta(...)` | duration fields | `timedelta` | Supports `weeks`, `days`, `hours`, `minutes`, `seconds`, `milliseconds`, `microseconds`, `nanoseconds`. |
| `date` methods | receiver: `date` | `date`, `datetime`, `int`, `string` | `.weekday()`, `.isoformat()`, `.replace(...)`, `.at(...)`, plus `.year`, `.month`, `.day`, `.ordinal`. |
| `datetime` methods | receiver: `datetime` | `date`, `datetime`, `float`, `int`, `string` | `.date()`, `.timestamp()`, `.weekday()`, `.isoformat()`, `.replace(...)`, plus `.year`, `.month`, `.day`, `.hour`, `.minute`, `.second`, `.ordinal`. |
| `timedelta.total_seconds()` | receiver: `timedelta` | `float` | `timedelta` is Starlark-only and cannot be emitted directly to aq output formats. |
| typed temporal preservation | parsed TOML dates/datetimes | typed values | TOML dates and datetimes stay typed inside Starlark instead of flattening to strings immediately. |

### Structured Data and Path Helpers

| Helper | Inputs | Returns | Notes |
| --- | --- | --- | --- |
| `aq.merge(left, right, deep = False)` | `value`, `value`, `bool` | `value` | Right-biased merge. `deep = True` merges nested objects recursively. |
| `aq.merge_all(values, deep = False)` | `values`, `bool` | `value` | Folds `aq.merge(...)` across a non-empty list. |
| `aq.drop_nulls(value, recursive = False)` | `value`, `bool` | `value` | Removes `null` object fields and array elements. |
| `aq.sort_keys(value, recursive = False)` | `value`, `bool` | `value` | Sorts object keys lexicographically. |
| `aq.get_path(value, path)` | `value`, `path` | `value` | Missing paths and out-of-range indexes yield `null`. |
| `aq.set_path(value, path, replacement)` | `value`, `path`, `value` | `value` | Creates missing object/array structure as needed. |
| `aq.delete_path(value, path)` | `value`, `path` | `value` | Removes one path. |
| `aq.delete_paths(value, paths)` | `value`, `paths` | `value` | Removes many paths. |
| `aq.pick_paths(value, paths)` | `value`, `paths` | `value` | jq-style projection, including `null` for missing picked paths. |
| `aq.omit_paths(value, paths)` | `value`, `paths` | `value` | Removes explicit path lists. |
| `aq.walk(value, function)` | `value`, `transform(value)` | `value` | Bottom-up recursive transform. |
| `aq.walk_paths(value, function)` | `value`, `transform(path, value)` | `value` | Like `aq.walk(...)`, but includes the current path. |
| `aq.paths(value, leaves_only = False)` | `value`, `bool` | `paths` | Stable depth-first traversal, excluding the root path. |
| `aq.find_paths(value, function, leaves_only = False)` | `value`, `predicate(path, value)`, `bool` | `paths` | Returns the matching paths. |
| `aq.collect_paths(value, function, leaves_only = False)` | `value`, callback, `bool` | `values` | Returns callback results in traversal order. |
| `aq.pick_where(value, function, leaves_only = False)` | `value`, `predicate(path, value)`, `bool` | `value` | Predicate-driven projection. |
| `aq.omit_where(value, function, leaves_only = False)` | `value`, `predicate(path, value)`, `bool` | `value` | Predicate-driven redaction. |
| `aq.clean_k8s_metadata(value)` | `value` | `value` | Keeps portable manifest metadata, currently `name`, `generateName`, `namespace`, `labels`, and `annotations`, while dropping live-object fields like `uid`, `resourceVersion`, `managedFields`, and `ownerReferences`. Applies at manifest scope and within `List.items`. |

### Text, Regex, URL, Hashing, and Release Helpers

| Helper | Inputs | Returns | Notes |
| --- | --- | --- | --- |
| `aq.regex_is_match(pattern, text)` | `string`, `string` | `bool` | Built on Rust `regex`. |
| `aq.regex_find(pattern, text)` | `string`, `string` | `string | None` | First match only. |
| `aq.regex_find_all(pattern, text)` | `string`, `string` | `list[string]` | Every match in order. |
| `aq.regex_capture(pattern, text)` | `string`, `string` | `dict | None` | Returns `{match, groups, named}` for the first capture match. |
| `aq.regex_capture_all(pattern, text)` | `string`, `string` | `list[dict]` | Capture metadata for every match. |
| `aq.regex_split(pattern, text)` | `string`, `string` | `list[string]` | Splits text by regex pattern. |
| `aq.regex_replace(pattern, replacement, text)` | `string`, `string`, `string` | `string` | First replacement only. |
| `aq.regex_replace_all(pattern, replacement, text)` | `string`, `string`, `string` | `string` | Global replacement. |
| `aq.base64_encode(text, urlsafe = False, pad = True)` | `string`, `bool`, `bool` | `string` | UTF-8 text in, base64 text out. |
| `aq.base64_decode(text, urlsafe = False)` | `string`, `bool` | `string` | UTF-8 text only. Binary payloads should use file helpers instead. |
| `aq.slug(text)` | `string` | `string` | URL-safe slug. |
| `aq.snake_case(text)` | `string` | `string` | `snake_case` normalization. |
| `aq.kebab_case(text)` | `string` | `string` | `kebab-case` normalization. |
| `aq.camel_case(text)` | `string` | `string` | `camelCase` normalization. |
| `aq.title_case(text)` | `string` | `string` | Title-cased label text. |
| `aq.trim_prefix(text, prefix)` | `string`, `string` | `string` | Leaves input unchanged when the prefix is absent. |
| `aq.trim_suffix(text, suffix)` | `string`, `string` | `string` | Leaves input unchanged when the suffix is absent. |
| `aq.regex_escape(text)` | `string` | `string` | Escapes literal text for safe regex embedding. |
| `aq.shell_escape(text)` | `string` | `string` | Escapes one POSIX shell argument. |
| `aq.url_encode_component(text)` | `string` | `string` | Percent-encodes one URL component. |
| `aq.url_decode_component(text)` | `string` | `string` | Percent-decodes one URL component. Does not treat `+` as space. |
| `aq.hash(text, algorithm = "sha256", encoding = "hex")` | `string`, `string`, `string` | `string` | Supports `sha1`, `sha256`, `sha512`, and `blake3`. |
| `aq.sha1(text, encoding = "hex")` | `string`, `string` | `string` | Convenience wrapper. |
| `aq.sha256(text, encoding = "hex")` | `string`, `string` | `string` | Convenience wrapper. |
| `aq.sha512(text, encoding = "hex")` | `string`, `string` | `string` | Convenience wrapper. |
| `aq.blake3(text, encoding = "hex")` | `string`, `string` | `string` | Convenience wrapper. |
| `aq.semver_parse(text)` | `string` | `dict` | Parses SemVer 2.0.0 into structured fields. |
| `aq.semver_compare(left, right)` | `string`, `string` | `int` | Returns negative, zero, or positive ordering. |
| `aq.semver_bump(text, part, prerelease_label = "rc")` | `string`, `string`, `string` | `string` | Supports `major`, `minor`, `patch`, `prerelease`, and `release`. |

### Filesystem and File I/O Helpers

These require `--starlark-filesystem` or `--starlark-unsafe`. In `--starlark-file` mode, relative `aq.read*` paths and `load("...")` resolve against the script file directory. Inline `--starlark` mode resolves them against the current working directory.

| Helper | Inputs | Returns | Notes |
| --- | --- | --- | --- |
| `aq.base_dir()` | none | `string` | Current base directory for relative filesystem operations. |
| `aq.resolve_path(path)` | `string` | `string` | Lexically resolves against the current base directory. |
| `aq.relative_path(path, start = ".")` | `string`, `string` | `string` | Lexical relative path helper. |
| `aq.exists(path)` | `string` | `bool` | Path existence check. |
| `aq.is_file(path)` | `string` | `bool` | File check. |
| `aq.is_dir(path)` | `string` | `bool` | Directory check. |
| `aq.list_dir(path = ".")` | `string` | `list[string]` | Sorted directory entry names. |
| `aq.walk_files(path = ".", include_dirs = False)` | `string`, `bool` | `list[string]` | Recursive relative walk. |
| `aq.glob(pattern, include_dirs = False)` | `string`, `bool` | `list[string]` | Supports `*`, `?`, and `**`. Absolute patterns are rejected. |
| `aq.mkdir(path, parents = False)` | `string`, `bool` | `string` | Creates a directory and returns the resolved path. |
| `aq.read_text(path)` | `string` | `string` | Reads UTF-8 text. |
| `aq.read_text_glob(pattern)` | `string` | `list[dict]` | Returns `[{path, text}]`. |
| `aq.rewrite_text(path, function)` | `string`, callback | `int` | In-place text rewrite. Callback receives `(path, text)` and returns replacement text. |
| `aq.rewrite_text_glob(pattern, function)` | `string`, callback | `list[dict]` | Batch text rewrite over matching files. |
| `aq.hash_file(path, algorithm = "sha256", encoding = "hex")` | `string`, `string`, `string` | `string` | Hashes raw file bytes, including binary payloads. |
| `aq.read(path)` | `string` | `value` | Reads and parses another file using normal detection rules. |
| `aq.read_as(path, format)` | `string`, `string` | `value` | Explicit format override. |
| `aq.read_all(path)` | `string` | `values` | Always returns every parsed document. |
| `aq.read_all_as(path, format)` | `string`, `string` | `values` | Explicit format override. |
| `aq.read_glob(pattern)` | `string` | `list[dict]` | Returns `[{path, value}]`. |
| `aq.read_glob_as(pattern, format)` | `string`, `string` | `list[dict]` | Explicit format override. |
| `aq.read_glob_all(pattern)` | `string` | `list[dict]` | Returns `[{path, index, value}]`. |
| `aq.read_glob_all_as(pattern, format)` | `string`, `string` | `list[dict]` | Explicit format override. |
| `aq.write_text(path, text, parents = False)` | `string`, `string`, `bool` | `int` | Atomically writes UTF-8 text and returns bytes written. |
| `aq.write_text_batch(entries, parents = False)` | `list[dict]`, `bool` | `list[dict]` | Writes `[{path, text}]` and returns `[{path, bytes}]`. |
| `aq.write(path, value, format, compact = False, parents = False)` | `string`, `value`, `string`, `bool`, `bool` | `int` | Atomically writes one rendered value. |
| `aq.write_all(path, values, format, compact = False, parents = False)` | `string`, `values`, `string`, `bool`, `bool` | `int` | Atomically writes a multi-result rendering. |
| `aq.write_batch(entries, format, compact = False, parents = False)` | `list[dict]`, `string`, `bool`, `bool` | `list[dict]` | Writes `[{path, value}]` and returns `[{path, bytes}]`. |
| `aq.write_batch_all(entries, format, compact = False, parents = False)` | `list[dict]`, `string`, `bool`, `bool` | `list[dict]` | Writes `[{path, values}]` and returns `[{path, bytes}]`. |
| `aq.stat(path)` | `string` | `dict | None` | Basic metadata for one path. |
| `aq.copy(source, destination, overwrite = False)` | `string`, `string`, `bool` | `int` | Copies one file and returns the copied byte count. |
| `aq.rename(source, destination, overwrite = False)` | `string`, `string`, `bool` | `string` | Renames or moves a path and returns the resolved destination. |
| `aq.remove(path, recursive = False, missing_ok = False)` | `string`, `bool`, `bool` | `bool` | Removes a file or directory. |
| `load("...")` | relative or absolute path | module names | Enabled for local Starlark files when filesystem capability is on. |

### Restricted Helpers

These stay gated behind explicit flags even though `dir(aq)` will still show the names.

| Helper | Requires | Inputs | Returns | Notes |
| --- | --- | --- | --- | --- |
| `aq.env(name)` | `--starlark-environment` | `string` | `string | None` | Reads one environment variable. |
| `aq.timestamp()` | `--starlark-time` | none | `datetime` | Current UTC datetime. |
| `aq.now()` | `--starlark-time` | none | `datetime` | Alias-style current UTC datetime helper. |
| `aq.today()` | `--starlark-time` | none | `date` | Current UTC date. |
| all restricted groups | `--starlark-unsafe` | n/a | n/a | Enables filesystem, environment, and time helpers at once. |

## Pipeline Patterns

Starlark mode is most useful when you want just a bit more control flow or library-style structure than a plain `aq` query gives you.

Read one document from stdin and return a transformed value:

```bash
printf '{"service":{"name":"api","port":8080}}\n' \
  | aq --starlark '{"name": data["service"]["name"], "port": data["service"]["port"]}'
```

Slurp many documents and work over the whole batch:

```bash
printf '{"name":"api"}\n{"name":"worker"}\n' \
  | aq --slurp --starlark '[item["name"] for item in data]' -r
```

Return rendered text instead of structured output:

```bash
aq --starlark 'aq.render({"app": data["name"], "port": data["port"]}, "yaml")' -r config.json
```

Write side files and still return a structured summary to stdout:

```bash
aq -n --starlark --starlark-filesystem '
written = aq.write("out/config.json", {"name": "aq"}, "json", compact = True, parents = True)
{"bytes": written, "path": "out/config.json"}
'
```

Bridge back into the `aq` query engine from Starlark when that is simpler than rewriting the query in pure Starlark:

```bash
aq --starlark '{"active": aq.query_all(".users[] | select(.active)", data)}' users.json
```

The practical rule is:

- return values for stdout
- use `aq.render(...)` when stdout should be plain text
- use `aq.write*` or `aq.rewrite_text*` for filesystem side effects
- use `--slurp` when the Starlark logic wants the whole batch at once

Restricted helpers:

- `aq.env(name)`
- `aq.timestamp()`
- `aq.now()`
- `aq.today()`

## Examples

The full runnable catalog lives in `examples/starlark/README.md` in the repo root. That is the best place to browse copy-pasteable commands.

The examples roughly break down like this:

- Query and transform:
  `users_over_30.star`, `manifest_summary.star`, `embedded_config.star`, `merge_overlay.star`
- Temporal values:
  `shift_release_window.star`, `calendar_rollup.star`, `time_snapshot.star`
- Cleanup and structured redaction:
  `clean_k8s_metadata.star`, `normalize_strings.star`, `find_secretish_fields.star`, `redact_secretish_fields.star`
- Filesystem and reporting:
  `config_map_names.star`, `self_inventory.star`, `library_index.star`, `fingerprint_library.star`, `stage_report.star`
- Batch read/write:
  `manifest_tree_summary.star`, `emit_manifest_summaries.star`, `emit_note_copies.star`, `normalize_note_files.star`
- Text and hashing:
  `sanitize_contacts.star`

## Worked Example Runs

These are the best representative runs to skim first.

Clean Kubernetes metadata before re-rendering YAML:

```bash
cat <<'YAML' | aq --starlark-file examples/starlark/clean_k8s_metadata.star --output-format yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: staging
  uid: abc123
  ownerReferences:
    - apiVersion: apps/v1
      kind: Deployment
      name: app
  resourceVersion: "7"
  creationTimestamp: "2024-01-01T00:00:00Z"
  managedFields:
    - manager: kubectl
  annotations:
    note: keep-me
  labels:
    tier: backend
YAML
```

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: staging
  annotations:
    note: keep-me
  labels:
    tier: backend
    managed-by: aq
```

Normalize and patch nested strings:

```bash
printf '%s\n' '{"metadata":{"labels":{"tier":"backend ","name":" api "}},"items":[" one ",2]}' \
  | aq --starlark-file examples/starlark/normalize_strings.star --output-format json --compact
```

```json
{"metadata":{"labels":{"tier":"BACKEND","name":"api"}},"items":["one",2]}
```

Generate a TOML config with no input:

```bash
aq -n --starlark-file examples/starlark/generate_app_toml.star --output-format toml
```

```toml
[app]
name = "aq"
port = 8443
features = [
    "query",
    "starlark",
]

[database]
host = "db.internal"
pool = 16
```

For the broader command catalog, see `examples/starlark/README.md` in the repo root.

Current limits:

- `timedelta` is currently a Starlark-only value. It cannot be emitted directly as an `aq` output value or rendered to output formats.
- `--stream` and `--explain` are not supported in Starlark mode yet.
- The `aq.*` surface is intentionally curated rather than exhaustive. It now covers a useful path/filesystem layer, but richer schema helpers and broader format helpers are still future work.
