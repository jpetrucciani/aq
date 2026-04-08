use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::rc::Weak;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::{
    STANDARD as BASE64_STANDARD, STANDARD_NO_PAD as BASE64_STANDARD_NO_PAD,
};
use base64::DecodeError as Base64DecodeError;
use base64::Engine as _;
use bigdecimal::BigDecimal;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use indexmap::IndexMap;
use regex::{Regex, RegexBuilder};

use crate::error::AqError;
use crate::value::{
    datetime_at_midnight, parse_common_datetime_string, DecimalValue, Value, MAX_JSON_PRINT_DEPTH,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    functions: Vec<FunctionDef>,
    outputs: Vec<Pipeline>,
    imported_values: IndexMap<String, Value>,
    module_info: Option<ModuleInfo>,
}

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_992;
const MAX_AUTO_GROW_ARRAY_INDEX: usize = 67_108_864;
const LARGE_VALUE_STACK_SIZE: usize = 64 * 1024 * 1024;
const LARGE_VALUE_STACK_THRESHOLD: usize = 256;
const MAX_REPEATED_STRING_BYTES: usize = 1_073_741_824;
const POW2_LOG2_SAFE_MIN_EXP: f64 = -1022.0;
const POW2_LOG2_SAFE_MAX_EXP: f64 = 1023.0;
const RFC3339_UTC_SECONDS_FORMAT: &str = "%Y-%m-%dT%H:%M:%SZ";
const JSON_SKIP_MARKER: &str = "<skipped: too deep>";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParseOptions {
    module_dir: Option<PathBuf>,
    library_paths: Vec<PathBuf>,
}

impl ParseOptions {
    #[cfg(test)]
    pub fn with_module_dir(module_dir: PathBuf) -> Self {
        Self {
            module_dir: Some(module_dir),
            library_paths: Vec::new(),
        }
    }

    pub fn with_module_search_paths(module_dir: PathBuf, library_paths: Vec<PathBuf>) -> Self {
        Self {
            module_dir: Some(module_dir),
            library_paths,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Pipeline {
    stages: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
enum Expr {
    Path(PathExpr),
    Literal(Value),
    FormatString {
        operator: FormatOperator,
        parts: Vec<FormatStringPart>,
    },
    FunctionCall {
        name: String,
        args: Vec<Query>,
    },
    Variable(String),
    Access {
        base: Box<Expr>,
        segments: Vec<Segment>,
    },
    Array(Vec<Expr>),
    Object(Vec<(ObjectKey, Expr)>),
    Builtin(BuiltinExpr),
    Subquery(Box<Query>),
    Bind {
        expr: Box<Expr>,
        pattern: BindingPattern,
    },
    BindingAlt {
        expr: Box<Expr>,
        patterns: Vec<BindingPattern>,
    },
    Reduce {
        source: Box<Query>,
        pattern: BindingPattern,
        init: Box<Query>,
        update: Box<Query>,
    },
    ForEach {
        source: Box<Query>,
        pattern: BindingPattern,
        init: Box<Query>,
        update: Box<Query>,
        extract: Box<Query>,
    },
    If {
        branches: Vec<(Query, Query)>,
        else_branch: Box<Query>,
    },
    Try {
        body: Box<Expr>,
        catch: Option<Box<Expr>>,
    },
    Label {
        name: String,
        body: Box<Query>,
    },
    Break(String),
    Assign {
        path: Box<Query>,
        op: AssignOp,
        value: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum ObjectKey {
    Static(String),
    Dynamic(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
enum BindingPattern {
    Variable(String),
    Array(Vec<BindingPattern>),
    Object(Vec<ObjectBindingField>),
}

#[derive(Debug, Clone, PartialEq)]
struct ObjectBindingField {
    key: ObjectKey,
    bind_name: Option<String>,
    pattern: BindingPattern,
}

#[derive(Debug, Clone, PartialEq)]
struct FunctionDef {
    name: String,
    params: Vec<String>,
    body: Query,
    captured_values: IndexMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
struct ModuleInfo {
    metadata: IndexMap<String, Value>,
    deps: Vec<ModuleDependency>,
    defs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct ModuleDependency {
    alias: Option<String>,
    is_data: bool,
    relpath: String,
    search: Option<String>,
}

impl ModuleInfo {
    fn to_value(&self) -> Value {
        let mut object = self.metadata.clone();
        object.insert(
            "deps".to_string(),
            Value::Array(
                self.deps
                    .iter()
                    .map(ModuleDependency::to_value)
                    .collect::<Vec<_>>(),
            ),
        );
        object.insert(
            "defs".to_string(),
            Value::Array(
                self.defs
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect::<Vec<_>>(),
            ),
        );
        Value::Object(object)
    }
}

impl ModuleDependency {
    fn to_value(&self) -> Value {
        let mut object = IndexMap::new();
        if let Some(search) = &self.search {
            object.insert("search".to_string(), Value::String(search.clone()));
        }
        if let Some(alias) = &self.alias {
            object.insert("as".to_string(), Value::String(alias.clone()));
        }
        object.insert("is_data".to_string(), Value::Bool(self.is_data));
        object.insert("relpath".to_string(), Value::String(self.relpath.clone()));
        Value::Object(object)
    }
}

#[derive(Debug, Clone, Eq)]
struct FunctionKey {
    name: String,
    arity: usize,
}

impl PartialEq for FunctionKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.arity == other.arity
    }
}

impl Hash for FunctionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.arity.hash(state);
    }
}

impl Ord for FunctionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name
            .cmp(&other.name)
            .then(self.arity.cmp(&other.arity))
    }
}

impl PartialOrd for FunctionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
enum FunctionBinding {
    User {
        params: Vec<String>,
        body: Query,
        captured_values: IndexMap<String, Value>,
        captured_scope: Weak<FunctionScope>,
    },
    Arg {
        query: Query,
        captured_values: IndexMap<String, Value>,
        captured_scope: Rc<FunctionScope>,
    },
}

#[derive(Debug, Clone, Default)]
struct FunctionScope {
    parent: Option<Rc<FunctionScope>>,
    bindings: IndexMap<FunctionKey, FunctionBinding>,
}

#[derive(Debug, Default)]
struct ModuleLoader {
    cache: BTreeMap<PathBuf, Query>,
    stack: Vec<PathBuf>,
}

#[derive(Debug, Default)]
struct DirectiveImport {
    functions: Vec<FunctionDef>,
    imported_values: IndexMap<String, Value>,
    dependency: Option<ModuleDependency>,
}

#[derive(Debug, Clone, PartialEq)]
enum BuiltinExpr {
    Input,
    Inputs,
    ModuleMeta(ParseOptions),
    Length,
    Utf8ByteLength,
    Keys,
    KeysUnsorted,
    Type,
    Builtins,
    Debug(Option<Box<Expr>>),
    Del(Box<Query>),
    Error(Option<Box<Expr>>),
    Env,
    Select(Box<Expr>),
    Add,
    AddQuery(Box<Query>),
    Avg,
    Median,
    Stddev,
    Percentile(Box<Expr>),
    Histogram(Box<Expr>),
    Contains(Box<Expr>),
    Inside(Box<Expr>),
    First,
    FirstQuery(Box<Query>),
    Has(Box<Expr>),
    In(Box<Expr>),
    InQuery(Box<Query>),
    InSource {
        source: Box<Query>,
        stream: Box<Query>,
    },
    IsEmpty(Box<Expr>),
    Last,
    LastQuery(Box<Query>),
    Limit {
        count: Box<Query>,
        expr: Box<Query>,
    },
    Take(Box<Expr>),
    Skip(Box<Expr>),
    SkipQuery {
        count: Box<Query>,
        expr: Box<Query>,
    },
    Map(Box<Expr>),
    MapValues(Box<Expr>),
    Nth {
        indexes: Box<Query>,
        expr: Box<Query>,
    },
    Empty,
    Range(Vec<Query>),
    Combinations(Option<Box<Expr>>),
    Bsearch(Box<Expr>),
    Recurse {
        query: Option<Box<Query>>,
        condition: Option<Box<Query>>,
    },
    Repeat(Box<Query>),
    Walk(Box<Expr>),
    While {
        condition: Box<Query>,
        update: Box<Query>,
    },
    Until {
        condition: Box<Query>,
        next: Box<Query>,
    },
    Transpose,
    Reverse,
    Sort,
    Min,
    Max,
    Unique,
    Flatten,
    FlattenDepth(Box<Expr>),
    Floor,
    Ceil,
    Round,
    Abs,
    Fabs,
    Sqrt,
    Log,
    Log2,
    Log10,
    Exp,
    Exp2,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Pow {
        base: Box<Query>,
        exponent: Box<Query>,
    },
    Now,
    ToDate,
    FromDate,
    ToDateTime,
    GmTime,
    MkTime,
    StrFTime(Box<Expr>),
    StrFLocalTime(Box<Expr>),
    StrPTime(Box<Expr>),
    TypeFilter(TypeFilter),
    ToString,
    ToNumber,
    ToBool,
    ToBoolean,
    Infinite,
    Nan,
    IsNan,
    Test {
        regex: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Capture {
        regex: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Match {
        regex: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Scan {
        regex: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Format(FormatOperator),
    StartsWith(Box<Expr>),
    EndsWith(Box<Expr>),
    Split {
        pattern: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Splits {
        pattern: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Sub {
        regex: Box<Query>,
        replacement: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Gsub {
        regex: Box<Query>,
        replacement: Box<Query>,
        flags: Option<Box<Query>>,
    },
    Any(Option<Box<Query>>),
    All(Option<Box<Query>>),
    AnyFrom {
        source: Box<Query>,
        predicate: Box<Query>,
    },
    AllFrom {
        source: Box<Query>,
        predicate: Box<Query>,
    },
    Join(Box<Expr>),
    JoinInput {
        index: Box<Query>,
        key: Box<Query>,
    },
    JoinStream {
        index: Box<Query>,
        source: Box<Query>,
        key: Box<Query>,
        join: Option<Box<Query>>,
    },
    AsciiDowncase,
    AsciiUpcase,
    Trim,
    Ltrim,
    Rtrim,
    ToEntries,
    FromEntries,
    WithEntries(Box<Expr>),
    SortBy(Box<Expr>),
    SortByDesc(Box<Expr>),
    GroupBy(Box<Expr>),
    UniqueBy(Box<Expr>),
    CountBy(Box<Expr>),
    Columns,
    YamlTag(Option<Box<Query>>),
    XmlAttr(Option<Box<Query>>),
    CsvHeader(Option<Box<Query>>),
    Merge {
        value: Box<Query>,
        deep: Option<Box<Query>>,
    },
    MergeAll(Option<Box<Query>>),
    SortKeys(Option<Box<Query>>),
    DropNulls(Option<Box<Query>>),
    Pick(Box<Query>),
    Omit(Box<Query>),
    Rename {
        path: Box<Query>,
        name: Box<Query>,
    },
    MinBy(Box<Expr>),
    MaxBy(Box<Expr>),
    GetPath(Box<Query>),
    SetPath {
        path: Box<Query>,
        value: Box<Query>,
    },
    DelPaths(Box<Query>),
    Path(Box<Query>),
    Paths(Option<Box<Query>>),
    TruncateStream(Box<Query>),
    FromStream(Box<Query>),
    ToStream,
    LeafPaths,
    Indices(Box<Expr>),
    IndexInput(Box<Query>),
    IndexStream {
        source: Box<Query>,
        key: Box<Query>,
    },
    Index(Box<Expr>),
    Rindex(Box<Expr>),
    TrimStr(Box<Expr>),
    LtrimStr(Box<Expr>),
    RtrimStr(Box<Expr>),
    ToJson,
    FromJson,
    Explode,
    Implode,
}

#[derive(Debug, Clone, PartialEq)]
enum FormatStringPart {
    Literal(String),
    Query(Box<Query>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FormatOperator {
    Json,
    Text,
    Csv,
    Tsv,
    Html,
    Uri,
    Urid,
    Sh,
    Base64,
    Base64d,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeFilter {
    Values,
    Nulls,
    Booleans,
    Numbers,
    Strings,
    Arrays,
    Objects,
    Iterables,
    Scalars,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssignOp {
    Set,
    Update,
    UpdateWith(BinaryOp),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Alt,
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
struct PathExpr {
    segments: Vec<Segment>,
}

#[derive(Debug, Clone, PartialEq)]
enum Segment {
    Field {
        name: String,
        optional: bool,
    },
    Lookup {
        expr: Box<Expr>,
        optional: bool,
    },
    Index {
        index: isize,
        optional: bool,
    },
    Slice {
        start: Option<isize>,
        end: Option<isize>,
        optional: bool,
    },
    Iterate {
        optional: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathComponent {
    Field(String),
    Index(isize),
    Slice {
        start: Option<isize>,
        end: Option<isize>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueStream {
    values: Vec<Value>,
}

#[derive(Debug)]
pub(crate) struct PartialValueStream {
    pub(crate) values: Vec<Value>,
    pub(crate) error: AqError,
}

type RegexCacheKey = (String, String);
type RegexCacheEntry = (Regex, RegexBehavior);

#[derive(Debug, Clone, Default)]
pub struct EvaluationContext {
    remaining_inputs: Rc<RefCell<VecDeque<Value>>>,
    functions: Rc<FunctionScope>,
    constant_query_cache: Rc<RefCell<BTreeMap<String, Vec<Value>>>>,
    constant_path_cache: Rc<RefCell<BTreeMap<String, Option<Vec<PathComponent>>>>>,
    regex_cache: Rc<RefCell<BTreeMap<RegexCacheKey, RegexCacheEntry>>>,
}

#[derive(Debug, Clone, Default)]
struct Bindings {
    values: IndexMap<String, Value>,
    functions: Rc<FunctionScope>,
}

#[derive(Debug, Clone)]
struct EvalFrame {
    value: Value,
    bindings: Bindings,
}

enum LabelFlow<T> {
    Continue(T),
    Break(T),
}

struct PartialEvaluation<T> {
    partial: T,
    error: AqError,
}

#[derive(Debug, Clone)]
struct PathValueFrame {
    value: Value,
    path: Vec<Value>,
    bindings: Bindings,
}

#[derive(Clone)]
enum ExactPathValueRef<'a> {
    Borrowed(&'a Value),
    Owned(Value),
    Null,
}

#[derive(Clone)]
struct ExactPathFrameRef<'a> {
    value: ExactPathValueRef<'a>,
    path: Vec<PathComponent>,
}

impl<'a> ExactPathValueRef<'a> {
    fn from_value(value: &'a Value) -> Self {
        match value.untagged() {
            Value::Null => Self::Null,
            untagged => Self::Borrowed(untagged),
        }
    }

    fn from_owned(value: Value) -> Self {
        match value.untagged() {
            Value::Null => Self::Null,
            _ => Self::Owned(value),
        }
    }

    fn into_value(self) -> Value {
        match self {
            Self::Borrowed(value) => value.clone(),
            Self::Owned(value) => value,
            Self::Null => Value::Null,
        }
    }
}

impl ValueStream {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn into_vec(self) -> Vec<Value> {
        self.values
    }
}

impl EvaluationContext {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_remaining_inputs(values: Vec<Value>) -> Self {
        Self {
            remaining_inputs: Rc::new(RefCell::new(VecDeque::from(values))),
            functions: Rc::new(FunctionScope::default()),
            constant_query_cache: Rc::new(RefCell::new(BTreeMap::new())),
            constant_path_cache: Rc::new(RefCell::new(BTreeMap::new())),
            regex_cache: Rc::new(RefCell::new(BTreeMap::new())),
        }
    }

    #[cfg(test)]
    pub fn remaining_inputs_len(&self) -> usize {
        self.remaining_inputs.borrow().len()
    }

    pub fn pop_next_input(&self) -> Option<Value> {
        self.remaining_inputs.borrow_mut().pop_front()
    }

    fn drain_inputs(&self) -> Vec<Value> {
        self.remaining_inputs.borrow_mut().drain(..).collect()
    }

    fn with_functions(&self, functions: Rc<FunctionScope>) -> Self {
        Self {
            remaining_inputs: Rc::clone(&self.remaining_inputs),
            functions,
            constant_query_cache: Rc::clone(&self.constant_query_cache),
            constant_path_cache: Rc::clone(&self.constant_path_cache),
            regex_cache: Rc::clone(&self.regex_cache),
        }
    }

    fn constant_query_values(&self, key: &str) -> Option<Vec<Value>> {
        self.constant_query_cache.borrow().get(key).cloned()
    }

    fn cache_constant_query_values(&self, key: String, values: Vec<Value>) {
        self.constant_query_cache.borrow_mut().insert(key, values);
    }

    fn constant_path_components(&self, key: &str) -> Option<Option<Vec<PathComponent>>> {
        self.constant_path_cache.borrow().get(key).cloned()
    }

    fn cache_constant_path_components(&self, key: String, path: Option<Vec<PathComponent>>) {
        self.constant_path_cache.borrow_mut().insert(key, path);
    }

    fn compiled_regex(&self, pattern: &str, flags: &str) -> Option<(Regex, RegexBehavior)> {
        self.regex_cache
            .borrow()
            .get(&(pattern.to_string(), flags.to_string()))
            .cloned()
    }

    fn cache_compiled_regex(
        &self,
        pattern: String,
        flags: String,
        compiled: (Regex, RegexBehavior),
    ) {
        self.regex_cache
            .borrow_mut()
            .insert((pattern, flags), compiled);
    }
}

impl Bindings {
    fn with_values(values: IndexMap<String, Value>, functions: Rc<FunctionScope>) -> Self {
        Self { values, functions }
    }

    fn get_value(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    fn insert_value(&mut self, name: String, value: Value) {
        self.values.insert(name, value);
    }
}

impl FunctionScope {
    fn lookup(&self, name: &str, arity: usize) -> Option<&FunctionBinding> {
        let key = FunctionKey {
            name: name.to_string(),
            arity,
        };
        if let Some(binding) = self.bindings.get(&key) {
            return Some(binding);
        }
        self.parent
            .as_ref()
            .and_then(|parent| parent.lookup(name, arity))
    }
}

fn bind_query_functions(
    defs: &[FunctionDef],
    values: &IndexMap<String, Value>,
    parent: Rc<FunctionScope>,
) -> Rc<FunctionScope> {
    let mut scope = parent;
    for def in defs {
        let name = def.name.clone();
        let arity = def.params.len();
        let params = def.params.clone();
        let body = def.body.clone();
        let mut captured_values = def.captured_values.clone();
        captured_values.extend(values.clone());
        let parent = Rc::clone(&scope);
        scope = Rc::new_cyclic(|self_scope| {
            let mut functions = IndexMap::new();
            functions.insert(
                FunctionKey { name, arity },
                FunctionBinding::User {
                    params,
                    body,
                    captured_values,
                    captured_scope: self_scope.clone(),
                },
            );
            FunctionScope {
                parent: Some(parent),
                bindings: functions,
            }
        });
    }
    scope
}

struct QueryScope {
    bindings: Bindings,
    context: EvaluationContext,
}

fn prepare_query_scope(
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Option<QueryScope> {
    if query.functions.is_empty() && query.imported_values.is_empty() {
        return None;
    }

    let local_values = if query.imported_values.is_empty() {
        bindings.values.clone()
    } else {
        let mut local_values = query.imported_values.clone();
        local_values.extend(bindings.values.clone());
        local_values
    };

    let functions = if query.functions.is_empty() {
        Rc::clone(&bindings.functions)
    } else {
        bind_query_functions(
            &query.functions,
            &local_values,
            Rc::clone(&bindings.functions),
        )
    };

    let local_context = if Rc::ptr_eq(&functions, &bindings.functions) {
        context.clone()
    } else {
        context.with_functions(Rc::clone(&functions))
    };

    Some(QueryScope {
        bindings: Bindings::with_values(local_values, functions),
        context: local_context,
    })
}

fn resolve_search_root(base_dir: &Path, search: &str) -> PathBuf {
    if let Some(stripped) = search.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    }

    let search_path = Path::new(search);
    if search_path.is_absolute() {
        search_path.to_path_buf()
    } else {
        base_dir.join(search_path)
    }
}

fn module_candidates_for_root(root: &Path, raw_path: &Path, extension: &str) -> Vec<PathBuf> {
    let candidate = root.join(raw_path);
    if raw_path.extension().is_some() {
        return vec![candidate];
    }

    let mut candidates = vec![candidate.clone(), candidate.with_extension(extension)];
    if let Some(file_name) = raw_path.file_name() {
        candidates.push(candidate.join(file_name).with_extension(extension));
    }
    candidates
}

fn effective_module_library_paths(module_dir: &Path, library_paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut effective = vec![module_dir.to_path_buf()];
    for path in library_paths {
        if !effective.contains(path) {
            effective.push(path.clone());
        }
    }
    effective
}

fn resolve_module_path(
    base_dir: &Path,
    library_paths: &[PathBuf],
    module_name: &str,
    search: Option<&str>,
    extension: &str,
) -> Result<PathBuf, AqError> {
    let raw_path = Path::new(module_name);
    let mut candidates = Vec::new();
    if raw_path.is_absolute() {
        candidates.push(raw_path.to_path_buf());
        if raw_path.extension().is_none() {
            candidates.push(raw_path.with_extension(extension));
            if let Some(file_name) = raw_path.file_name() {
                candidates.push(raw_path.join(file_name).with_extension(extension));
            }
        }
    } else {
        let mut roots = Vec::with_capacity(library_paths.len() + 1);
        if let Some(search) = search {
            roots.push(resolve_search_root(base_dir, search));
        } else {
            roots.push(base_dir.to_path_buf());
            roots.extend(library_paths.iter().cloned());
        }
        for root in roots {
            candidates.extend(module_candidates_for_root(&root, raw_path, extension));
        }
    }

    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(AqError::message(format!(
        "module `{module_name}` not found relative to {} or configured library paths",
        base_dir.display()
    )))
}

fn load_module_query(
    module_loader: &Rc<RefCell<ModuleLoader>>,
    path: PathBuf,
    library_paths: &[PathBuf],
) -> Result<Query, AqError> {
    if let Some(query) = module_loader.borrow().cache.get(&path).cloned() {
        return Ok(query);
    }

    {
        let mut loader = module_loader.borrow_mut();
        if loader.stack.contains(&path) {
            return Err(AqError::message(format!(
                "cyclic module import involving {}",
                path.display()
            )));
        }
        loader.stack.push(path.clone());
    }

    let query_result = (|| {
        let source =
            fs::read_to_string(&path).map_err(|error| AqError::io(Some(path.clone()), error))?;
        let module_dir = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut query = Parser::with_options(
            &source,
            ParseOptions::with_module_search_paths(module_dir, library_paths.to_vec()),
            Rc::clone(module_loader),
            true,
        )
        .parse_query()?;
        if !query.outputs.is_empty() {
            return Err(AqError::InvalidExpression(format!(
                "module `{}` must only contain definitions, imports, or includes",
                path.display()
            )));
        }
        if !query.imported_values.is_empty() {
            for function in &mut query.functions {
                let mut captured_values = function.captured_values.clone();
                captured_values.extend(query.imported_values.clone());
                function.captured_values = captured_values;
            }
        }
        Ok(query)
    })();

    let mut loader = module_loader.borrow_mut();
    let popped = loader.stack.pop();
    if popped.as_ref() != Some(&path) {
        return Err(AqError::message(
            "internal error: module loader stack desynchronized",
        ));
    }
    if let Ok(query) = &query_result {
        loader.cache.insert(path.clone(), query.clone());
    }
    query_result
}

fn load_data_module_value(
    base_dir: &Path,
    library_paths: &[PathBuf],
    module_name: &str,
    search: Option<&str>,
) -> Result<Value, AqError> {
    let path = resolve_module_path(base_dir, library_paths, module_name, search, "json")?;
    let source =
        fs::read_to_string(&path).map_err(|error| AqError::io(Some(path.clone()), error))?;
    let stream = serde_json::Deserializer::from_str(&source).into_iter::<serde_json::Value>();
    let mut values = Vec::new();
    for item in stream {
        let json = item.map_err(|error| {
            AqError::message(format!(
                "failed to parse data module `{}`: {error}",
                path.display()
            ))
        })?;
        values.push(Value::from_json(json)?);
    }
    Ok(Value::Array(values))
}

fn metadata_search_path(
    metadata: Option<&IndexMap<String, Value>>,
) -> Result<Option<String>, AqError> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let Some(value) = metadata.get("search") else {
        return Ok(None);
    };
    match value.untagged() {
        Value::String(search) => Ok(Some(search.clone())),
        other => Err(AqError::InvalidExpression(format!(
            "module metadata search must be a string, got {}",
            kind_name(other)
        ))),
    }
}

fn namespace_module_functions(functions: Vec<FunctionDef>, namespace: &str) -> Vec<FunctionDef> {
    let rename_map: BTreeMap<FunctionKey, String> = functions
        .iter()
        .map(|def| {
            (
                FunctionKey {
                    name: def.name.clone(),
                    arity: def.params.len(),
                },
                format!("{namespace}::{}", def.name),
            )
        })
        .collect();

    functions
        .into_iter()
        .map(|def| {
            let key = FunctionKey {
                name: def.name.clone(),
                arity: def.params.len(),
            };
            let mut shadowed = BTreeSet::new();
            shadowed.extend(
                def.params
                    .iter()
                    .cloned()
                    .map(|name| FunctionKey { name, arity: 0 }),
            );
            FunctionDef {
                name: rename_map
                    .get(&key)
                    .cloned()
                    .expect("module rename map should contain function"),
                params: def.params,
                body: rename_query_function_calls(def.body, &rename_map, &shadowed),
                captured_values: def.captured_values,
            }
        })
        .collect()
}

fn rename_query_function_calls(
    query: Query,
    rename_map: &BTreeMap<FunctionKey, String>,
    inherited_shadowed: &BTreeSet<FunctionKey>,
) -> Query {
    let local_keys: BTreeSet<FunctionKey> = query
        .functions
        .iter()
        .map(|def| FunctionKey {
            name: def.name.clone(),
            arity: def.params.len(),
        })
        .collect();
    let scope_shadowed = inherited_shadowed
        .iter()
        .cloned()
        .chain(local_keys.iter().cloned())
        .collect::<BTreeSet<_>>();

    let functions = query
        .functions
        .into_iter()
        .map(|def| {
            let mut function_shadowed = scope_shadowed.clone();
            function_shadowed.extend(
                def.params
                    .iter()
                    .cloned()
                    .map(|name| FunctionKey { name, arity: 0 }),
            );
            FunctionDef {
                name: def.name,
                params: def.params,
                body: rename_query_function_calls(def.body, rename_map, &function_shadowed),
                captured_values: def.captured_values,
            }
        })
        .collect();

    let outputs = query
        .outputs
        .into_iter()
        .map(|pipeline| rename_pipeline_function_calls(pipeline, rename_map, &scope_shadowed))
        .collect();

    Query {
        functions,
        outputs,
        imported_values: query.imported_values,
        module_info: query.module_info,
    }
}

fn rename_pipeline_function_calls(
    pipeline: Pipeline,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> Pipeline {
    Pipeline {
        stages: pipeline
            .stages
            .into_iter()
            .map(|expr| rename_expr_function_calls(expr, rename_map, shadowed))
            .collect(),
    }
}

fn rename_expr_function_calls(
    expr: Expr,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> Expr {
    match expr {
        Expr::FunctionCall { name, args } => {
            let args = args
                .into_iter()
                .map(|query| rename_query_function_calls(query, rename_map, shadowed))
                .collect::<Vec<_>>();
            let key = FunctionKey {
                name: name.clone(),
                arity: args.len(),
            };
            if shadowed.contains(&key) {
                Expr::FunctionCall { name, args }
            } else if let Some(renamed) = rename_map.get(&key) {
                Expr::FunctionCall {
                    name: renamed.clone(),
                    args,
                }
            } else {
                Expr::FunctionCall { name, args }
            }
        }
        Expr::Access { base, segments } => Expr::Access {
            base: Box::new(rename_expr_function_calls(*base, rename_map, shadowed)),
            segments: segments
                .into_iter()
                .map(|segment| rename_segment_function_calls(segment, rename_map, shadowed))
                .collect(),
        },
        Expr::Array(items) => Expr::Array(
            items
                .into_iter()
                .map(|item| rename_expr_function_calls(item, rename_map, shadowed))
                .collect(),
        ),
        Expr::Object(fields) => Expr::Object(
            fields
                .into_iter()
                .map(|(key, value)| {
                    (
                        rename_object_key_function_calls(key, rename_map, shadowed),
                        rename_expr_function_calls(value, rename_map, shadowed),
                    )
                })
                .collect(),
        ),
        Expr::Builtin(builtin) => {
            Expr::Builtin(rename_builtin_function_calls(builtin, rename_map, shadowed))
        }
        Expr::Subquery(query) => Expr::Subquery(Box::new(rename_query_function_calls(
            *query, rename_map, shadowed,
        ))),
        Expr::Bind { expr, pattern } => Expr::Bind {
            expr: Box::new(rename_expr_function_calls(*expr, rename_map, shadowed)),
            pattern: rename_binding_pattern_function_calls(pattern, rename_map, shadowed),
        },
        Expr::BindingAlt { expr, patterns } => Expr::BindingAlt {
            expr: Box::new(rename_expr_function_calls(*expr, rename_map, shadowed)),
            patterns: patterns
                .into_iter()
                .map(|pattern| rename_binding_pattern_function_calls(pattern, rename_map, shadowed))
                .collect(),
        },
        Expr::Reduce {
            source,
            pattern,
            init,
            update,
        } => Expr::Reduce {
            source: Box::new(rename_query_function_calls(*source, rename_map, shadowed)),
            pattern: rename_binding_pattern_function_calls(pattern, rename_map, shadowed),
            init: Box::new(rename_query_function_calls(*init, rename_map, shadowed)),
            update: Box::new(rename_query_function_calls(*update, rename_map, shadowed)),
        },
        Expr::ForEach {
            source,
            pattern,
            init,
            update,
            extract,
        } => Expr::ForEach {
            source: Box::new(rename_query_function_calls(*source, rename_map, shadowed)),
            pattern: rename_binding_pattern_function_calls(pattern, rename_map, shadowed),
            init: Box::new(rename_query_function_calls(*init, rename_map, shadowed)),
            update: Box::new(rename_query_function_calls(*update, rename_map, shadowed)),
            extract: Box::new(rename_query_function_calls(*extract, rename_map, shadowed)),
        },
        Expr::If {
            branches,
            else_branch,
        } => Expr::If {
            branches: branches
                .into_iter()
                .map(|(condition, body)| {
                    (
                        rename_query_function_calls(condition, rename_map, shadowed),
                        rename_query_function_calls(body, rename_map, shadowed),
                    )
                })
                .collect(),
            else_branch: Box::new(rename_query_function_calls(
                *else_branch,
                rename_map,
                shadowed,
            )),
        },
        Expr::Try { body, catch } => Expr::Try {
            body: Box::new(rename_expr_function_calls(*body, rename_map, shadowed)),
            catch: catch
                .map(|expr| Box::new(rename_expr_function_calls(*expr, rename_map, shadowed))),
        },
        Expr::Label { name, body } => Expr::Label {
            name,
            body: Box::new(rename_query_function_calls(*body, rename_map, shadowed)),
        },
        Expr::Break(_) => expr,
        Expr::Assign { path, op, value } => Expr::Assign {
            path: Box::new(rename_query_function_calls(*path, rename_map, shadowed)),
            op,
            value: Box::new(rename_expr_function_calls(*value, rename_map, shadowed)),
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(rename_expr_function_calls(*expr, rename_map, shadowed)),
        },
        Expr::Binary { left, op, right } => Expr::Binary {
            left: Box::new(rename_expr_function_calls(*left, rename_map, shadowed)),
            op,
            right: Box::new(rename_expr_function_calls(*right, rename_map, shadowed)),
        },
        Expr::FormatString { operator, parts } => Expr::FormatString {
            operator,
            parts: parts
                .into_iter()
                .map(|part| match part {
                    FormatStringPart::Literal(text) => FormatStringPart::Literal(text),
                    FormatStringPart::Query(query) => FormatStringPart::Query(Box::new(
                        rename_query_function_calls(*query, rename_map, shadowed),
                    )),
                })
                .collect(),
        },
        Expr::Path(_) | Expr::Literal(_) | Expr::Variable(_) => expr,
    }
}

fn rename_binding_pattern_function_calls(
    pattern: BindingPattern,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> BindingPattern {
    match pattern {
        BindingPattern::Variable(name) => BindingPattern::Variable(name),
        BindingPattern::Array(patterns) => BindingPattern::Array(
            patterns
                .into_iter()
                .map(|pattern| rename_binding_pattern_function_calls(pattern, rename_map, shadowed))
                .collect(),
        ),
        BindingPattern::Object(fields) => BindingPattern::Object(
            fields
                .into_iter()
                .map(|field| ObjectBindingField {
                    key: rename_object_key_function_calls(field.key, rename_map, shadowed),
                    bind_name: field.bind_name,
                    pattern: rename_binding_pattern_function_calls(
                        field.pattern,
                        rename_map,
                        shadowed,
                    ),
                })
                .collect(),
        ),
    }
}

fn rename_segment_function_calls(
    segment: Segment,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> Segment {
    match segment {
        Segment::Lookup { expr, optional } => Segment::Lookup {
            expr: Box::new(rename_expr_function_calls(*expr, rename_map, shadowed)),
            optional,
        },
        other => other,
    }
}

fn rename_object_key_function_calls(
    key: ObjectKey,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> ObjectKey {
    match key {
        ObjectKey::Dynamic(expr) => ObjectKey::Dynamic(Box::new(rename_expr_function_calls(
            *expr, rename_map, shadowed,
        ))),
        other => other,
    }
}

fn rename_expr_box(
    expr: Expr,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> Box<Expr> {
    Box::new(rename_expr_function_calls(expr, rename_map, shadowed))
}

fn rename_query_box(
    query: Query,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> Box<Query> {
    Box::new(rename_query_function_calls(query, rename_map, shadowed))
}

fn rename_optional_expr_box(
    expr: Option<Box<Expr>>,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> Option<Box<Expr>> {
    expr.map(|expr| rename_expr_box(*expr, rename_map, shadowed))
}

fn rename_optional_query_box(
    query: Option<Box<Query>>,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> Option<Box<Query>> {
    query.map(|query| rename_query_box(*query, rename_map, shadowed))
}

fn rename_builtin_function_calls(
    builtin: BuiltinExpr,
    rename_map: &BTreeMap<FunctionKey, String>,
    shadowed: &BTreeSet<FunctionKey>,
) -> BuiltinExpr {
    match builtin {
        BuiltinExpr::Input
        | BuiltinExpr::Inputs
        | BuiltinExpr::ModuleMeta(_)
        | BuiltinExpr::Length
        | BuiltinExpr::Utf8ByteLength
        | BuiltinExpr::Keys
        | BuiltinExpr::KeysUnsorted
        | BuiltinExpr::Type
        | BuiltinExpr::Builtins
        | BuiltinExpr::Env
        | BuiltinExpr::Add
        | BuiltinExpr::Avg
        | BuiltinExpr::Median
        | BuiltinExpr::Stddev
        | BuiltinExpr::First
        | BuiltinExpr::Last
        | BuiltinExpr::Empty
        | BuiltinExpr::Transpose
        | BuiltinExpr::Reverse
        | BuiltinExpr::Sort
        | BuiltinExpr::Min
        | BuiltinExpr::Max
        | BuiltinExpr::Unique
        | BuiltinExpr::Flatten
        | BuiltinExpr::Floor
        | BuiltinExpr::Ceil
        | BuiltinExpr::Round
        | BuiltinExpr::Abs
        | BuiltinExpr::Fabs
        | BuiltinExpr::Sqrt
        | BuiltinExpr::Log
        | BuiltinExpr::Log2
        | BuiltinExpr::Log10
        | BuiltinExpr::Exp
        | BuiltinExpr::Exp2
        | BuiltinExpr::Sin
        | BuiltinExpr::Cos
        | BuiltinExpr::Tan
        | BuiltinExpr::Asin
        | BuiltinExpr::Acos
        | BuiltinExpr::Atan
        | BuiltinExpr::Now
        | BuiltinExpr::ToDate
        | BuiltinExpr::FromDate
        | BuiltinExpr::ToDateTime
        | BuiltinExpr::GmTime
        | BuiltinExpr::MkTime
        | BuiltinExpr::ToString
        | BuiltinExpr::ToNumber
        | BuiltinExpr::ToBool
        | BuiltinExpr::ToBoolean
        | BuiltinExpr::Infinite
        | BuiltinExpr::Nan
        | BuiltinExpr::IsNan
        | BuiltinExpr::AsciiDowncase
        | BuiltinExpr::AsciiUpcase
        | BuiltinExpr::Trim
        | BuiltinExpr::Ltrim
        | BuiltinExpr::Rtrim
        | BuiltinExpr::ToEntries
        | BuiltinExpr::FromEntries
        | BuiltinExpr::Columns
        | BuiltinExpr::LeafPaths
        | BuiltinExpr::ToStream
        | BuiltinExpr::ToJson
        | BuiltinExpr::FromJson
        | BuiltinExpr::Explode
        | BuiltinExpr::Implode
        | BuiltinExpr::TypeFilter(_)
        | BuiltinExpr::Format(_) => builtin,
        BuiltinExpr::Debug(expr) => {
            BuiltinExpr::Debug(rename_optional_expr_box(expr, rename_map, shadowed))
        }
        BuiltinExpr::Error(expr) => {
            BuiltinExpr::Error(rename_optional_expr_box(expr, rename_map, shadowed))
        }
        BuiltinExpr::StrFTime(expr) => {
            BuiltinExpr::StrFTime(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::StrFLocalTime(expr) => {
            BuiltinExpr::StrFLocalTime(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::StrPTime(expr) => {
            BuiltinExpr::StrPTime(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Del(query) => BuiltinExpr::Del(rename_query_box(*query, rename_map, shadowed)),
        BuiltinExpr::AddQuery(query) => {
            BuiltinExpr::AddQuery(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Select(expr) => {
            BuiltinExpr::Select(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Percentile(expr) => {
            BuiltinExpr::Percentile(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Histogram(expr) => {
            BuiltinExpr::Histogram(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Contains(expr) => {
            BuiltinExpr::Contains(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Inside(expr) => {
            BuiltinExpr::Inside(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::FirstQuery(query) => {
            BuiltinExpr::FirstQuery(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Has(expr) => BuiltinExpr::Has(rename_expr_box(*expr, rename_map, shadowed)),
        BuiltinExpr::In(expr) => BuiltinExpr::In(rename_expr_box(*expr, rename_map, shadowed)),
        BuiltinExpr::InQuery(query) => {
            BuiltinExpr::InQuery(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::InSource { source, stream } => BuiltinExpr::InSource {
            source: rename_query_box(*source, rename_map, shadowed),
            stream: rename_query_box(*stream, rename_map, shadowed),
        },
        BuiltinExpr::IsEmpty(expr) => {
            BuiltinExpr::IsEmpty(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::LastQuery(query) => {
            BuiltinExpr::LastQuery(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Limit { count, expr } => BuiltinExpr::Limit {
            count: rename_query_box(*count, rename_map, shadowed),
            expr: rename_query_box(*expr, rename_map, shadowed),
        },
        BuiltinExpr::Take(expr) => BuiltinExpr::Take(rename_expr_box(*expr, rename_map, shadowed)),
        BuiltinExpr::Skip(expr) => BuiltinExpr::Skip(rename_expr_box(*expr, rename_map, shadowed)),
        BuiltinExpr::SkipQuery { count, expr } => BuiltinExpr::SkipQuery {
            count: rename_query_box(*count, rename_map, shadowed),
            expr: rename_query_box(*expr, rename_map, shadowed),
        },
        BuiltinExpr::Map(expr) => BuiltinExpr::Map(rename_expr_box(*expr, rename_map, shadowed)),
        BuiltinExpr::MapValues(expr) => {
            BuiltinExpr::MapValues(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Nth { indexes, expr } => BuiltinExpr::Nth {
            indexes: rename_query_box(*indexes, rename_map, shadowed),
            expr: rename_query_box(*expr, rename_map, shadowed),
        },
        BuiltinExpr::Range(queries) => BuiltinExpr::Range(
            queries
                .into_iter()
                .map(|query| rename_query_function_calls(query, rename_map, shadowed))
                .collect(),
        ),
        BuiltinExpr::Combinations(expr) => {
            BuiltinExpr::Combinations(rename_optional_expr_box(expr, rename_map, shadowed))
        }
        BuiltinExpr::Bsearch(expr) => {
            BuiltinExpr::Bsearch(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Recurse { query, condition } => BuiltinExpr::Recurse {
            query: rename_optional_query_box(query, rename_map, shadowed),
            condition: rename_optional_query_box(condition, rename_map, shadowed),
        },
        BuiltinExpr::Repeat(query) => {
            BuiltinExpr::Repeat(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Walk(expr) => BuiltinExpr::Walk(rename_expr_box(*expr, rename_map, shadowed)),
        BuiltinExpr::While { condition, update } => BuiltinExpr::While {
            condition: rename_query_box(*condition, rename_map, shadowed),
            update: rename_query_box(*update, rename_map, shadowed),
        },
        BuiltinExpr::Until { condition, next } => BuiltinExpr::Until {
            condition: rename_query_box(*condition, rename_map, shadowed),
            next: rename_query_box(*next, rename_map, shadowed),
        },
        BuiltinExpr::FlattenDepth(expr) => {
            BuiltinExpr::FlattenDepth(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Pow { base, exponent } => BuiltinExpr::Pow {
            base: rename_query_box(*base, rename_map, shadowed),
            exponent: rename_query_box(*exponent, rename_map, shadowed),
        },
        BuiltinExpr::Test { regex, flags } => BuiltinExpr::Test {
            regex: rename_query_box(*regex, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::Capture { regex, flags } => BuiltinExpr::Capture {
            regex: rename_query_box(*regex, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::Match { regex, flags } => BuiltinExpr::Match {
            regex: rename_query_box(*regex, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::Scan { regex, flags } => BuiltinExpr::Scan {
            regex: rename_query_box(*regex, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::StartsWith(expr) => {
            BuiltinExpr::StartsWith(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::EndsWith(expr) => {
            BuiltinExpr::EndsWith(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Split { pattern, flags } => BuiltinExpr::Split {
            pattern: rename_query_box(*pattern, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::Splits { pattern, flags } => BuiltinExpr::Splits {
            pattern: rename_query_box(*pattern, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::Sub {
            regex,
            replacement,
            flags,
        } => BuiltinExpr::Sub {
            regex: rename_query_box(*regex, rename_map, shadowed),
            replacement: rename_query_box(*replacement, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::Gsub {
            regex,
            replacement,
            flags,
        } => BuiltinExpr::Gsub {
            regex: rename_query_box(*regex, rename_map, shadowed),
            replacement: rename_query_box(*replacement, rename_map, shadowed),
            flags: rename_optional_query_box(flags, rename_map, shadowed),
        },
        BuiltinExpr::Any(query) => {
            BuiltinExpr::Any(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::All(query) => {
            BuiltinExpr::All(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::AnyFrom { source, predicate } => BuiltinExpr::AnyFrom {
            source: rename_query_box(*source, rename_map, shadowed),
            predicate: rename_query_box(*predicate, rename_map, shadowed),
        },
        BuiltinExpr::AllFrom { source, predicate } => BuiltinExpr::AllFrom {
            source: rename_query_box(*source, rename_map, shadowed),
            predicate: rename_query_box(*predicate, rename_map, shadowed),
        },
        BuiltinExpr::Join(expr) => BuiltinExpr::Join(rename_expr_box(*expr, rename_map, shadowed)),
        BuiltinExpr::JoinInput { index, key } => BuiltinExpr::JoinInput {
            index: rename_query_box(*index, rename_map, shadowed),
            key: rename_query_box(*key, rename_map, shadowed),
        },
        BuiltinExpr::JoinStream {
            index,
            source,
            key,
            join,
        } => BuiltinExpr::JoinStream {
            index: rename_query_box(*index, rename_map, shadowed),
            source: rename_query_box(*source, rename_map, shadowed),
            key: rename_query_box(*key, rename_map, shadowed),
            join: rename_optional_query_box(join, rename_map, shadowed),
        },
        BuiltinExpr::WithEntries(expr) => {
            BuiltinExpr::WithEntries(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::SortBy(expr) => {
            BuiltinExpr::SortBy(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::SortByDesc(expr) => {
            BuiltinExpr::SortByDesc(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::GroupBy(expr) => {
            BuiltinExpr::GroupBy(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::UniqueBy(expr) => {
            BuiltinExpr::UniqueBy(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::CountBy(expr) => {
            BuiltinExpr::CountBy(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::YamlTag(query) => {
            BuiltinExpr::YamlTag(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::XmlAttr(query) => {
            BuiltinExpr::XmlAttr(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::CsvHeader(query) => {
            BuiltinExpr::CsvHeader(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::Merge { value, deep } => BuiltinExpr::Merge {
            value: rename_query_box(*value, rename_map, shadowed),
            deep: rename_optional_query_box(deep, rename_map, shadowed),
        },
        BuiltinExpr::MergeAll(query) => {
            BuiltinExpr::MergeAll(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::SortKeys(query) => {
            BuiltinExpr::SortKeys(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::DropNulls(query) => {
            BuiltinExpr::DropNulls(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::Pick(query) => {
            BuiltinExpr::Pick(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Omit(query) => {
            BuiltinExpr::Omit(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Rename { path, name } => BuiltinExpr::Rename {
            path: rename_query_box(*path, rename_map, shadowed),
            name: rename_query_box(*name, rename_map, shadowed),
        },
        BuiltinExpr::MinBy(expr) => {
            BuiltinExpr::MinBy(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::MaxBy(expr) => {
            BuiltinExpr::MaxBy(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::GetPath(query) => {
            BuiltinExpr::GetPath(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::SetPath { path, value } => BuiltinExpr::SetPath {
            path: rename_query_box(*path, rename_map, shadowed),
            value: rename_query_box(*value, rename_map, shadowed),
        },
        BuiltinExpr::DelPaths(query) => {
            BuiltinExpr::DelPaths(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Path(query) => {
            BuiltinExpr::Path(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Paths(query) => {
            BuiltinExpr::Paths(rename_optional_query_box(query, rename_map, shadowed))
        }
        BuiltinExpr::TruncateStream(query) => {
            BuiltinExpr::TruncateStream(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::FromStream(query) => {
            BuiltinExpr::FromStream(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::Indices(expr) => {
            BuiltinExpr::Indices(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::IndexInput(query) => {
            BuiltinExpr::IndexInput(rename_query_box(*query, rename_map, shadowed))
        }
        BuiltinExpr::IndexStream { source, key } => BuiltinExpr::IndexStream {
            source: rename_query_box(*source, rename_map, shadowed),
            key: rename_query_box(*key, rename_map, shadowed),
        },
        BuiltinExpr::Index(expr) => {
            BuiltinExpr::Index(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::Rindex(expr) => {
            BuiltinExpr::Rindex(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::TrimStr(expr) => {
            BuiltinExpr::TrimStr(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::LtrimStr(expr) => {
            BuiltinExpr::LtrimStr(rename_expr_box(*expr, rename_map, shadowed))
        }
        BuiltinExpr::RtrimStr(expr) => {
            BuiltinExpr::RtrimStr(rename_expr_box(*expr, rename_map, shadowed))
        }
    }
}

pub fn parse(expression: &str) -> Result<Query, AqError> {
    parse_with_options(expression, &ParseOptions::default())
}

pub fn parse_with_options(expression: &str, options: &ParseOptions) -> Result<Query, AqError> {
    let loader = Rc::new(RefCell::new(ModuleLoader::default()));
    Parser::with_options(expression, options.clone(), loader, false).parse_query()
}

pub fn validate_streaming_query(query: &Query) -> Result<(), AqError> {
    if let Some(builtin) = query_stream_builtin_use(query) {
        return Err(AqError::message(format!(
            "--stream does not support queries that use `{builtin}` because stream mode already processes one record at a time; rerun without --stream"
        )));
    }
    Ok(())
}

pub fn evaluate(query: &Query, input: &Value) -> Result<ValueStream, AqError> {
    evaluate_with_bindings_and_context(query, input, &IndexMap::new(), &EvaluationContext::empty())
}

#[cfg(test)]
pub fn evaluate_with_context(
    query: &Query,
    input: &Value,
    context: &EvaluationContext,
) -> Result<ValueStream, AqError> {
    evaluate_with_bindings_and_context(query, input, &IndexMap::new(), context)
}

pub fn evaluate_with_bindings_and_context(
    query: &Query,
    input: &Value,
    bindings: &IndexMap<String, Value>,
    context: &EvaluationContext,
) -> Result<ValueStream, AqError> {
    let bindings = Bindings::with_values(bindings.clone(), Rc::clone(&context.functions));
    let values = evaluate_query_values(query, input, &bindings, context)
        .map_err(finalize_top_level_error)?;
    Ok(ValueStream::new(values))
}

pub(crate) fn evaluate_with_bindings_and_context_preserving_partial(
    query: &Query,
    input: &Value,
    bindings: &IndexMap<String, Value>,
    context: &EvaluationContext,
) -> Result<ValueStream, PartialValueStream> {
    let bindings = Bindings::with_values(bindings.clone(), Rc::clone(&context.functions));
    match evaluate_query_preserving_partial(query, input, &bindings, context) {
        Ok(frames) => Ok(ValueStream::new(frames_to_values(frames))),
        Err(PartialEvaluation { partial, error }) => Err(PartialValueStream {
            values: frames_to_values(partial),
            error: finalize_top_level_error(error),
        }),
    }
}

fn finalize_top_level_error(error: AqError) -> AqError {
    match error {
        AqError::BreakLabel(name) => AqError::Query(format!("label {name} is not defined")),
        other => other,
    }
}

fn bump_column_in_error_message(message: &str, delta: usize) -> String {
    let Some(column_index) = message.rfind("column ") else {
        return message.to_string();
    };
    let digits_start = column_index + "column ".len();
    let digits = message[digits_start..]
        .chars()
        .take_while(|value| value.is_ascii_digit())
        .collect::<String>();
    let Ok(column) = digits.parse::<usize>() else {
        return message.to_string();
    };
    let updated = column.saturating_add(delta);
    format!(
        "{}column {}{}",
        &message[..column_index],
        updated,
        &message[digits_start + digits.len()..]
    )
}

fn query_stream_builtin_use(query: &Query) -> Option<&'static str> {
    for function in &query.functions {
        if let Some(builtin) = query_stream_builtin_use(&function.body) {
            return Some(builtin);
        }
    }
    for pipeline in &query.outputs {
        if let Some(builtin) = pipeline_stream_builtin_use(pipeline) {
            return Some(builtin);
        }
    }
    None
}

fn pipeline_stream_builtin_use(pipeline: &Pipeline) -> Option<&'static str> {
    for stage in &pipeline.stages {
        if let Some(builtin) = expr_stream_builtin_use(stage) {
            return Some(builtin);
        }
    }
    None
}

fn expr_stream_builtin_use(expr: &Expr) -> Option<&'static str> {
    match expr {
        Expr::Path(path) => path_stream_builtin_use(path),
        Expr::Literal(_) | Expr::Variable(_) => None,
        Expr::FormatString { parts, .. } => {
            for part in parts {
                if let FormatStringPart::Query(query) = part {
                    if let Some(builtin) = query_stream_builtin_use(query) {
                        return Some(builtin);
                    }
                }
            }
            None
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                if let Some(builtin) = query_stream_builtin_use(arg) {
                    return Some(builtin);
                }
            }
            None
        }
        Expr::Access { base, segments } => {
            expr_stream_builtin_use(base).or_else(|| segments_stream_builtin_use(segments))
        }
        Expr::Array(values) => {
            for value in values {
                if let Some(builtin) = expr_stream_builtin_use(value) {
                    return Some(builtin);
                }
            }
            None
        }
        Expr::Object(entries) => {
            for (key, value) in entries {
                if let ObjectKey::Dynamic(key) = key {
                    if let Some(builtin) = expr_stream_builtin_use(key) {
                        return Some(builtin);
                    }
                }
                if let Some(builtin) = expr_stream_builtin_use(value) {
                    return Some(builtin);
                }
            }
            None
        }
        Expr::Builtin(builtin) => builtin_stream_builtin_use(builtin),
        Expr::Subquery(query) => query_stream_builtin_use(query),
        Expr::Bind { expr, .. } | Expr::BindingAlt { expr, .. } | Expr::Unary { expr, .. } => {
            expr_stream_builtin_use(expr)
        }
        Expr::Reduce {
            source,
            init,
            update,
            ..
        } => query_stream_builtin_use(source)
            .or_else(|| query_stream_builtin_use(init))
            .or_else(|| query_stream_builtin_use(update)),
        Expr::ForEach {
            source,
            init,
            update,
            extract,
            ..
        } => query_stream_builtin_use(source)
            .or_else(|| query_stream_builtin_use(init))
            .or_else(|| query_stream_builtin_use(update))
            .or_else(|| query_stream_builtin_use(extract)),
        Expr::If {
            branches,
            else_branch,
        } => {
            for (condition, branch) in branches {
                if let Some(builtin) = query_stream_builtin_use(condition) {
                    return Some(builtin);
                }
                if let Some(builtin) = query_stream_builtin_use(branch) {
                    return Some(builtin);
                }
            }
            query_stream_builtin_use(else_branch)
        }
        Expr::Try { body, catch } => expr_stream_builtin_use(body).or_else(|| {
            catch
                .as_ref()
                .and_then(|catch| expr_stream_builtin_use(catch.as_ref()))
        }),
        Expr::Label { body, .. } => query_stream_builtin_use(body),
        Expr::Break(_) => None,
        Expr::Assign { path, value, .. } => {
            query_stream_builtin_use(path).or_else(|| expr_stream_builtin_use(value))
        }
        Expr::Binary { left, right, .. } => {
            expr_stream_builtin_use(left).or_else(|| expr_stream_builtin_use(right))
        }
    }
}

fn path_stream_builtin_use(path: &PathExpr) -> Option<&'static str> {
    segments_stream_builtin_use(&path.segments)
}

fn segments_stream_builtin_use(segments: &[Segment]) -> Option<&'static str> {
    for segment in segments {
        if let Segment::Lookup { expr, .. } = segment {
            if let Some(builtin) = expr_stream_builtin_use(expr) {
                return Some(builtin);
            }
        }
    }
    None
}

fn builtin_stream_builtin_use(builtin: &BuiltinExpr) -> Option<&'static str> {
    match builtin {
        BuiltinExpr::Input => Some("input"),
        BuiltinExpr::Inputs => Some("inputs"),
        BuiltinExpr::ModuleMeta(_) => None,
        BuiltinExpr::Debug(expr) | BuiltinExpr::Error(expr) => expr
            .as_ref()
            .and_then(|expr| expr_stream_builtin_use(expr.as_ref())),
        BuiltinExpr::Select(expr)
        | BuiltinExpr::Percentile(expr)
        | BuiltinExpr::Histogram(expr)
        | BuiltinExpr::Contains(expr)
        | BuiltinExpr::Inside(expr)
        | BuiltinExpr::Has(expr)
        | BuiltinExpr::In(expr)
        | BuiltinExpr::IsEmpty(expr)
        | BuiltinExpr::Take(expr)
        | BuiltinExpr::Skip(expr)
        | BuiltinExpr::Map(expr)
        | BuiltinExpr::MapValues(expr)
        | BuiltinExpr::Bsearch(expr)
        | BuiltinExpr::Walk(expr)
        | BuiltinExpr::FlattenDepth(expr)
        | BuiltinExpr::StartsWith(expr)
        | BuiltinExpr::EndsWith(expr)
        | BuiltinExpr::Join(expr)
        | BuiltinExpr::WithEntries(expr)
        | BuiltinExpr::SortBy(expr)
        | BuiltinExpr::SortByDesc(expr)
        | BuiltinExpr::GroupBy(expr)
        | BuiltinExpr::UniqueBy(expr)
        | BuiltinExpr::CountBy(expr)
        | BuiltinExpr::MinBy(expr)
        | BuiltinExpr::MaxBy(expr)
        | BuiltinExpr::Indices(expr)
        | BuiltinExpr::Index(expr)
        | BuiltinExpr::Rindex(expr)
        | BuiltinExpr::TrimStr(expr)
        | BuiltinExpr::LtrimStr(expr)
        | BuiltinExpr::RtrimStr(expr) => expr_stream_builtin_use(expr),
        BuiltinExpr::InQuery(query) | BuiltinExpr::IndexInput(query) => {
            query_stream_builtin_use(query)
        }
        BuiltinExpr::Split { pattern, flags } | BuiltinExpr::Splits { pattern, flags } => {
            query_stream_builtin_use(pattern).or_else(|| {
                flags
                    .as_ref()
                    .and_then(|flags| query_stream_builtin_use(flags.as_ref()))
            })
        }
        BuiltinExpr::InSource { source, stream } => {
            query_stream_builtin_use(source).or_else(|| query_stream_builtin_use(stream))
        }
        BuiltinExpr::JoinInput { index, key } => {
            query_stream_builtin_use(index).or_else(|| query_stream_builtin_use(key))
        }
        BuiltinExpr::JoinStream {
            index,
            source,
            key,
            join,
        } => query_stream_builtin_use(index)
            .or_else(|| query_stream_builtin_use(source))
            .or_else(|| query_stream_builtin_use(key))
            .or_else(|| {
                join.as_ref()
                    .and_then(|join| query_stream_builtin_use(join.as_ref()))
            }),
        BuiltinExpr::IndexStream { source, key } => {
            query_stream_builtin_use(source).or_else(|| query_stream_builtin_use(key))
        }
        BuiltinExpr::Del(query)
        | BuiltinExpr::FirstQuery(query)
        | BuiltinExpr::LastQuery(query)
        | BuiltinExpr::Pick(query)
        | BuiltinExpr::Omit(query)
        | BuiltinExpr::GetPath(query)
        | BuiltinExpr::DelPaths(query)
        | BuiltinExpr::TruncateStream(query)
        | BuiltinExpr::FromStream(query)
        | BuiltinExpr::Path(query) => query_stream_builtin_use(query),
        BuiltinExpr::Limit { count, expr }
        | BuiltinExpr::SkipQuery { count, expr }
        | BuiltinExpr::Nth {
            indexes: count,
            expr,
        } => query_stream_builtin_use(count).or_else(|| query_stream_builtin_use(expr)),
        BuiltinExpr::Range(queries) => {
            for query in queries {
                if let Some(builtin) = query_stream_builtin_use(query) {
                    return Some(builtin);
                }
            }
            None
        }
        BuiltinExpr::Combinations(expr) => expr
            .as_ref()
            .and_then(|expr| expr_stream_builtin_use(expr.as_ref())),
        BuiltinExpr::Any(query) | BuiltinExpr::All(query) => query
            .as_ref()
            .and_then(|query| query_stream_builtin_use(query.as_ref())),
        BuiltinExpr::AnyFrom { source, predicate } | BuiltinExpr::AllFrom { source, predicate } => {
            query_stream_builtin_use(source).or_else(|| query_stream_builtin_use(predicate))
        }
        BuiltinExpr::Recurse { query, condition } => query
            .as_ref()
            .and_then(|query| query_stream_builtin_use(query.as_ref()))
            .or_else(|| {
                condition
                    .as_ref()
                    .and_then(|query| query_stream_builtin_use(query.as_ref()))
            }),
        BuiltinExpr::Repeat(query) => query_stream_builtin_use(query.as_ref()),
        BuiltinExpr::Paths(query) => query
            .as_ref()
            .and_then(|query| query_stream_builtin_use(query.as_ref())),
        BuiltinExpr::ToStream => None,
        BuiltinExpr::Merge { value, deep } => query_stream_builtin_use(value).or_else(|| {
            deep.as_ref()
                .and_then(|query| query_stream_builtin_use(query.as_ref()))
        }),
        BuiltinExpr::YamlTag(query)
        | BuiltinExpr::XmlAttr(query)
        | BuiltinExpr::MergeAll(query)
        | BuiltinExpr::CsvHeader(query)
        | BuiltinExpr::SortKeys(query)
        | BuiltinExpr::DropNulls(query) => query
            .as_ref()
            .and_then(|query| query_stream_builtin_use(query.as_ref())),
        BuiltinExpr::While { condition, update }
        | BuiltinExpr::Until {
            condition,
            next: update,
        } => query_stream_builtin_use(condition).or_else(|| query_stream_builtin_use(update)),
        BuiltinExpr::Pow { base, exponent } => {
            query_stream_builtin_use(base).or_else(|| query_stream_builtin_use(exponent))
        }
        BuiltinExpr::Test { regex, flags }
        | BuiltinExpr::Capture { regex, flags }
        | BuiltinExpr::Match { regex, flags }
        | BuiltinExpr::Scan { regex, flags } => query_stream_builtin_use(regex).or_else(|| {
            flags
                .as_ref()
                .and_then(|flags| query_stream_builtin_use(flags.as_ref()))
        }),
        BuiltinExpr::Sub {
            regex,
            replacement,
            flags,
        }
        | BuiltinExpr::Gsub {
            regex,
            replacement,
            flags,
        } => query_stream_builtin_use(regex)
            .or_else(|| query_stream_builtin_use(replacement))
            .or_else(|| {
                flags
                    .as_ref()
                    .and_then(|flags| query_stream_builtin_use(flags.as_ref()))
            }),
        BuiltinExpr::Rename { path, name } | BuiltinExpr::SetPath { path, value: name } => {
            query_stream_builtin_use(path).or_else(|| query_stream_builtin_use(name))
        }
        _ => None,
    }
}

fn query_from_expr(expr: Expr) -> Query {
    match expr {
        Expr::Subquery(query) => *query,
        other => Query {
            functions: Vec::new(),
            outputs: vec![Pipeline {
                stages: vec![other],
            }],
            imported_values: IndexMap::new(),
            module_info: None,
        },
    }
}

fn prepend_function_param_bindings(query: Query, names: &[String]) -> Query {
    if names.is_empty() {
        return query;
    }

    let prefix = names
        .iter()
        .map(|name| Expr::Bind {
            expr: Box::new(Expr::FunctionCall {
                name: name.clone(),
                args: Vec::new(),
            }),
            pattern: BindingPattern::Variable(name.clone()),
        })
        .collect::<Vec<_>>();

    let outputs = query
        .outputs
        .into_iter()
        .map(|pipeline| {
            let mut stages = prefix.clone();
            stages.extend(pipeline.stages);
            Pipeline { stages }
        })
        .collect();

    Query {
        functions: query.functions,
        outputs,
        imported_values: query.imported_values,
        module_info: query.module_info,
    }
}

fn expr_from_single_stage_query(name: &str, query: Query) -> Result<Expr, AqError> {
    if !query.functions.is_empty() || query.outputs.len() != 1 || query.outputs[0].stages.len() != 1
    {
        return Err(AqError::InvalidExpression(format!(
            "{name} expects a single expression argument"
        )));
    }
    let mut outputs = query.outputs;
    let pipeline = outputs.pop().ok_or_else(|| {
        AqError::InvalidExpression(format!("{name} expects a single expression argument"))
    })?;
    let mut stages = pipeline.stages;
    stages.pop().ok_or_else(|| {
        AqError::InvalidExpression(format!("{name} expects a single expression argument"))
    })
}

fn evaluate_query(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    evaluate_query_up_to(query, input, bindings, context, usize::MAX)
}

fn evaluate_query_owned(
    query: &Query,
    input: Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    evaluate_query_up_to_owned(query, input, bindings, context, usize::MAX)
}

fn evaluate_query_last(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<EvalFrame>, AqError> {
    let scope = prepare_query_scope(query, bindings, context);
    let (local_bindings, local_context) = if let Some(scope) = &scope {
        (&scope.bindings, &scope.context)
    } else {
        (bindings, context)
    };
    let mut last = None;
    for pipeline in &query.outputs {
        if let Some(frame) = evaluate_pipeline_last(pipeline, input, local_bindings, local_context)?
        {
            last = Some(frame);
        }
    }
    Ok(last)
}

fn evaluate_query_preserving_partial(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    let scope = prepare_query_scope(query, bindings, context);
    let (local_bindings, local_context) = if let Some(scope) = &scope {
        (&scope.bindings, &scope.context)
    } else {
        (bindings, context)
    };
    let mut out = Vec::new();
    for pipeline in &query.outputs {
        match evaluate_pipeline_preserving_partial(pipeline, input, local_bindings, local_context) {
            Ok(frames) => out.extend(frames),
            Err(PartialEvaluation { partial, error }) => {
                out.extend(partial);
                return Err(PartialEvaluation {
                    partial: out,
                    error,
                });
            }
        }
    }
    Ok(out)
}

fn frames_to_values(frames: Vec<EvalFrame>) -> Vec<Value> {
    frames.into_iter().map(|frame| frame.value).collect()
}

fn evaluate_query_values(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(stage) = direct_stage_expr(query) {
        return evaluate_expr(stage, input, bindings, context);
    }

    if query.functions.is_empty()
        && query.imported_values.is_empty()
        && query.outputs.len() == 1
        && pipeline_has_no_binding_stages(&query.outputs[0])
    {
        return evaluate_plain_stages_values(
            &query.outputs[0].stages,
            vec![input.clone()],
            bindings,
            context,
        );
    }

    Ok(frames_to_values(evaluate_query(
        query, input, bindings, context,
    )?))
}

fn pipeline_has_no_binding_stages(pipeline: &Pipeline) -> bool {
    pipeline
        .stages
        .iter()
        .all(|expr| !matches!(expr, Expr::Bind { .. } | Expr::BindingAlt { .. }))
}

fn query_has_no_binding_stages(query: &Query) -> bool {
    query.outputs.iter().all(pipeline_has_no_binding_stages)
}

fn stages_are_value_only(stages: &[Expr]) -> bool {
    stages.iter().all(|expr| {
        !matches!(
            expr,
            Expr::Bind { .. } | Expr::BindingAlt { .. } | Expr::Assign { .. }
        )
    })
}

fn query_has_value_only_fast_path(query: &Query) -> bool {
    direct_stage_expr(query).is_some()
        || (query.functions.is_empty()
            && query.imported_values.is_empty()
            && query_has_no_binding_stages(query))
}

fn evaluate_plain_stages_values(
    stages: &[Expr],
    values: Vec<Value>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let [input] = values.as_slice() {
        if let Some(result) = try_evaluate_literal_strptime_mktime_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_implode_explode_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_length_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_type_filter_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_string_predicate_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_split_values(stages, input, bindings) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_literal_binary_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_trimstr_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_tojson_values(stages, input) {
            return result;
        }
        if let Some(result) = try_evaluate_direct_iter_utf8_byte_length_try_values(stages, input) {
            return result;
        }
    }

    let mut current = values;
    for stage in stages {
        let mut next = Vec::new();
        for value in current {
            next.extend(evaluate_expr(stage, &value, bindings, context)?);
            dispose_value(value);
        }
        current = next;
    }
    Ok(current)
}

fn try_evaluate_literal_strptime_mktime_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Builtin(BuiltinExpr::StrPTime(format)), Expr::Builtin(BuiltinExpr::MkTime)] = stages
    else {
        return None;
    };
    let format = literal_time_format_argument(format.as_ref())?;
    Some(strptime_mktime_of(input, format).map(|value| vec![value]))
}

fn try_apply_bind_value_only_stages(
    stages: &[Expr],
    frames: &[EvalFrame],
    context: &EvaluationContext,
) -> Option<Result<Vec<EvalFrame>, AqError>> {
    let [Expr::Bind { expr, pattern }, suffix @ ..] = stages else {
        return None;
    };
    if !suffix.is_empty() && !stages_are_value_only(suffix) {
        return None;
    }

    Some(frames.iter().try_fold(Vec::new(), |mut out, frame| {
        let bound_values = evaluate_expr(expr, &frame.value, &frame.bindings, context)?;
        for bound_value in bound_values {
            let mut bindings = frame.bindings.clone();
            bind_pattern(pattern, &bound_value, &mut bindings, context)?;
            if suffix.is_empty() {
                out.push(EvalFrame {
                    value: frame.value.clone(),
                    bindings,
                });
                continue;
            }
            if let [stage] = suffix {
                if let Some(value) = direct_single_value_expr_value(stage, &frame.value, &bindings)
                {
                    out.push(EvalFrame { value, bindings });
                    continue;
                }
            }
            let values = evaluate_plain_stages_values(
                suffix,
                vec![frame.value.clone()],
                &bindings,
                context,
            )?;
            out.extend(values.into_iter().map(|value| EvalFrame {
                value,
                bindings: bindings.clone(),
            }));
        }
        Ok(out)
    }))
}

fn try_evaluate_implode_explode_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Builtin(BuiltinExpr::Implode), Expr::Builtin(BuiltinExpr::Explode)] = stages else {
        return None;
    };

    Some(match input.untagged() {
        Value::Array(values) => values
            .iter()
            .map(|value| {
                Ok(Value::Integer(i64::from(u32::from(imploded_char_of(
                    value,
                )?))))
            })
            .collect::<Result<Vec<_>, AqError>>(),
        _ => Err(AqError::Query("implode input must be an array".to_string())),
    })
}

fn try_evaluate_direct_iter_length_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Builtin(BuiltinExpr::Length)] = stages else {
        return None;
    };
    if !matches!(segments.as_slice(), [Segment::Iterate { optional: false }]) {
        return None;
    }

    Some(match input.untagged() {
        Value::Array(values) => values.iter().map(length_of).collect(),
        Value::Object(fields) => fields.values().map(length_of).collect(),
        other => Err(iterate_error(other)),
    })
}

fn try_evaluate_direct_iter_trimstr_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Builtin(BuiltinExpr::TrimStr(trim_expr))] =
        stages
    else {
        return None;
    };
    if !matches!(segments.as_slice(), [Segment::Iterate { optional: false }]) {
        return None;
    }
    let trim = literal_string_expr_value(trim_expr)?;

    Some(match iterate_input_values(input) {
        Ok(values) => values
            .into_iter()
            .map(|value| {
                let text = expect_string_input("trimstr", &value)?;
                if trim.is_empty() || trim.len() > text.len() {
                    return Ok(Value::String(text.to_string()));
                }
                let mut start = 0usize;
                if text.starts_with(trim) {
                    start = trim.len();
                }
                let mut end = text.len();
                if end.saturating_sub(start) >= trim.len() && text[start..].ends_with(trim) {
                    end = end.saturating_sub(trim.len());
                }
                Ok(Value::String(text[start..end].to_string()))
            })
            .collect(),
        Err(error) => Err(error),
    })
}

fn try_evaluate_direct_iter_string_predicate_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Builtin(predicate)] = stages else {
        return None;
    };
    if !matches!(segments.as_slice(), [Segment::Iterate { optional: false }]) {
        return None;
    }

    let literal = match predicate {
        BuiltinExpr::StartsWith(expr) => literal_string_expr_value(expr).map(|value| (value, true)),
        BuiltinExpr::EndsWith(expr) => literal_string_expr_value(expr).map(|value| (value, false)),
        _ => None,
    }?;

    Some(match input.untagged() {
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let Value::String(text) = value.untagged() else {
                    let message = if literal.1 {
                        "startswith() requires string inputs"
                    } else {
                        "endswith() requires string inputs"
                    };
                    return Some(Err(AqError::Query(message.to_string())));
                };
                out.push(Value::Bool(if literal.1 {
                    text.starts_with(literal.0)
                } else {
                    text.ends_with(literal.0)
                }));
            }
            Ok(out)
        }
        Value::Object(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for value in fields.values() {
                let Value::String(text) = value.untagged() else {
                    let message = if literal.1 {
                        "startswith() requires string inputs"
                    } else {
                        "endswith() requires string inputs"
                    };
                    return Some(Err(AqError::Query(message.to_string())));
                };
                out.push(Value::Bool(if literal.1 {
                    text.starts_with(literal.0)
                } else {
                    text.ends_with(literal.0)
                }));
            }
            Ok(out)
        }
        other => Err(iterate_error(other)),
    })
}

fn try_evaluate_direct_iter_split_values(
    stages: &[Expr],
    input: &Value,
    bindings: &Bindings,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Builtin(BuiltinExpr::Split {
        pattern,
        flags: None,
    })] = stages
    else {
        return None;
    };
    if !matches!(segments.as_slice(), [Segment::Iterate { optional: false }]) {
        return None;
    }

    let delimiter = borrowed_single_query_string(pattern, input, bindings)?;

    Some(match input.untagged() {
        Value::Array(values) => values
            .iter()
            .map(|value| {
                let text = expect_string_input("split", value)?;
                Ok(split_literal_string_value(text, delimiter))
            })
            .collect(),
        Value::Object(fields) => fields
            .values()
            .map(|value| {
                let text = expect_string_input("split", value)?;
                Ok(split_literal_string_value(text, delimiter))
            })
            .collect(),
        other => Err(iterate_error(other)),
    })
}

fn try_evaluate_direct_iter_utf8_byte_length_try_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Try {
        body,
        catch: Some(catch),
    }] = stages
    else {
        return None;
    };
    if !matches!(segments.as_slice(), [Segment::Iterate { optional: false }]) {
        return None;
    }
    if !matches!(body.as_ref(), Expr::Builtin(BuiltinExpr::Utf8ByteLength))
        || !expr_is_identity_path(catch)
    {
        return None;
    }

    Some(match input.untagged() {
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match utf8_byte_length_of(value) {
                    Ok(length) => out.push(Value::Integer(length)),
                    Err(error) => out.push(error.into_catch_value()),
                }
            }
            Ok(out)
        }
        Value::Object(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for value in fields.values() {
                match utf8_byte_length_of(value) {
                    Ok(length) => out.push(Value::Integer(length)),
                    Err(error) => out.push(error.into_catch_value()),
                }
            }
            Ok(out)
        }
        other => Err(iterate_error(other)),
    })
}

fn try_evaluate_direct_iter_literal_binary_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Binary { left, op, right }] = stages else {
        return None;
    };
    if !matches!(segments.as_slice(), [Segment::Iterate { optional: false }]) {
        return None;
    }

    let (literal_side, identity_on_left) = if expr_is_identity_path(left) {
        (right.as_ref(), true)
    } else if expr_is_identity_path(right) {
        (left.as_ref(), false)
    } else {
        return None;
    };
    if matches!(op, BinaryOp::And | BinaryOp::Or | BinaryOp::Alt) {
        return None;
    }
    let Expr::Literal(literal) = literal_side else {
        return None;
    };

    Some(match input.untagged() {
        Value::Array(values) => values
            .iter()
            .map(|value| direct_iter_literal_binary_value(value, *op, literal, identity_on_left))
            .collect(),
        Value::Object(fields) => fields
            .values()
            .map(|value| direct_iter_literal_binary_value(value, *op, literal, identity_on_left))
            .collect(),
        other => Err(iterate_error(other)),
    })
}

fn direct_iter_literal_binary_value(
    value: &Value,
    op: BinaryOp,
    literal: &Value,
    identity_on_left: bool,
) -> Result<Value, AqError> {
    let (left, right) = if identity_on_left {
        (value, literal)
    } else {
        (literal, value)
    };
    match op {
        BinaryOp::Add => value_add(left, right),
        BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            value_math(left, op, right)
        }
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            apply_binary_op(left, op, right).map(Value::Bool)
        }
        BinaryOp::And | BinaryOp::Or | BinaryOp::Alt => unreachable!(),
    }
}

fn try_evaluate_direct_iter_tojson_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Builtin(BuiltinExpr::ToJson)] = stages else {
        return None;
    };
    if !matches!(segments.as_slice(), [Segment::Iterate { optional: false }]) {
        return None;
    }

    Some(match input.untagged() {
        Value::Array(values) => values
            .iter()
            .map(|value| to_json_of(value).map(Value::String))
            .collect(),
        Value::Object(fields) => fields
            .values()
            .map(|value| to_json_of(value).map(Value::String))
            .collect(),
        other => Err(iterate_error(other)),
    })
}

fn try_evaluate_direct_iter_type_filter_values(
    stages: &[Expr],
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    let [Expr::Path(PathExpr { segments }), Expr::Builtin(BuiltinExpr::TypeFilter(filter))] =
        stages
    else {
        return None;
    };
    let [Segment::Iterate { optional }] = segments.as_slice() else {
        return None;
    };

    Some(match input.untagged() {
        Value::Array(values) => Ok(values
            .iter()
            .filter(|value| matches_type_filter(value, *filter))
            .cloned()
            .collect()),
        Value::Object(fields) => Ok(fields
            .values()
            .filter(|value| matches_type_filter(value, *filter))
            .cloned()
            .collect()),
        _ if *optional => Ok(Vec::new()),
        other => Err(iterate_error(other)),
    })
}

fn evaluate_plain_query_up_to(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Vec<EvalFrame>, AqError> {
    let mut out = Vec::new();
    for pipeline in &query.outputs {
        if out.len() >= limit {
            break;
        }
        let remaining = limit.saturating_sub(out.len());
        let values =
            evaluate_plain_stages_values(&pipeline.stages, vec![input.clone()], bindings, context)?;
        out.extend(values.into_iter().take(remaining).map(|value| EvalFrame {
            value,
            bindings: bindings.clone(),
        }));
    }
    Ok(out)
}

fn label_flow_values(flow: LabelFlow<Vec<EvalFrame>>) -> LabelFlow<Vec<Value>> {
    match flow {
        LabelFlow::Continue(frames) => LabelFlow::Continue(frames_to_values(frames)),
        LabelFlow::Break(frames) => LabelFlow::Break(frames_to_values(frames)),
    }
}

fn evaluate_query_up_to(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Vec<EvalFrame>, AqError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if let Some(frames) = evaluate_cached_constant_query_up_to(query, bindings, context, limit)? {
        return Ok(frames);
    }
    evaluate_query_up_to_uncached(query, input, bindings, context, limit)
}

fn evaluate_query_up_to_owned(
    query: &Query,
    input: Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Vec<EvalFrame>, AqError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if let Some(frames) = evaluate_cached_constant_query_up_to(query, bindings, context, limit)? {
        return Ok(frames);
    }
    evaluate_query_up_to_uncached_owned(query, input, bindings, context, limit)
}

fn evaluate_query_up_to_uncached(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Vec<EvalFrame>, AqError> {
    if let Some(stage) = direct_stage_expr(query) {
        if let Some(frames) = evaluate_direct_stage_up_to(stage, input, bindings, context, limit)? {
            return Ok(frames);
        }
        let frames = evaluate_stage(
            stage,
            EvalFrame {
                value: input.clone(),
                bindings: bindings.clone(),
            },
            context,
        )?;
        return Ok(frames.into_iter().take(limit).collect());
    }
    if query.functions.is_empty()
        && query.imported_values.is_empty()
        && query_has_no_binding_stages(query)
    {
        return evaluate_plain_query_up_to(query, input, bindings, context, limit);
    }

    let scope = prepare_query_scope(query, bindings, context);
    let (local_bindings, local_context) = if let Some(scope) = &scope {
        (&scope.bindings, &scope.context)
    } else {
        (bindings, context)
    };
    let mut out = Vec::new();
    for pipeline in &query.outputs {
        if out.len() >= limit {
            break;
        }
        let remaining = limit.saturating_sub(out.len());
        let frames = evaluate_pipeline(pipeline, input, local_bindings, local_context)?;
        out.extend(frames.into_iter().take(remaining));
    }
    Ok(out)
}

fn evaluate_query_up_to_uncached_owned(
    query: &Query,
    input: Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Vec<EvalFrame>, AqError> {
    if let Some(stage) = direct_stage_expr(query) {
        if let Some(frames) = evaluate_direct_stage_up_to(stage, &input, bindings, context, limit)?
        {
            return Ok(frames);
        }
        let frames = evaluate_stage(
            stage,
            EvalFrame {
                value: input,
                bindings: bindings.clone(),
            },
            context,
        )?;
        return Ok(frames.into_iter().take(limit).collect());
    }
    if query.functions.is_empty()
        && query.imported_values.is_empty()
        && query_has_no_binding_stages(query)
    {
        return evaluate_plain_query_up_to(query, &input, bindings, context, limit);
    }

    let scope = prepare_query_scope(query, bindings, context);
    let (local_bindings, local_context) = if let Some(scope) = &scope {
        (&scope.bindings, &scope.context)
    } else {
        (bindings, context)
    };
    let mut out = Vec::new();
    let mut input = Some(input);
    for (index, pipeline) in query.outputs.iter().enumerate() {
        if out.len() >= limit {
            break;
        }
        let remaining = limit.saturating_sub(out.len());
        let pipeline_input = if index + 1 == query.outputs.len() {
            input.take().unwrap_or(Value::Null)
        } else {
            input.as_ref().cloned().unwrap_or(Value::Null)
        };
        let frames =
            evaluate_pipeline_owned(pipeline, pipeline_input, local_bindings, local_context)?;
        out.extend(frames.into_iter().take(remaining));
    }
    Ok(out)
}

fn evaluate_direct_stage_up_to(
    stage: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Option<Vec<EvalFrame>>, AqError> {
    match stage {
        Expr::Builtin(BuiltinExpr::Range(args)) => {
            let values = range_of_up_to(args, input, bindings, context, limit)?;
            Ok(Some(
                values
                    .into_iter()
                    .map(|value| EvalFrame {
                        value,
                        bindings: bindings.clone(),
                    })
                    .collect(),
            ))
        }
        Expr::Path(path) => {
            if let Some(values) = evaluate_direct_static_path_up_to(path, input, limit) {
                return Ok(Some(
                    values?
                        .into_iter()
                        .map(|value| EvalFrame {
                            value,
                            bindings: bindings.clone(),
                        })
                        .collect(),
                ));
            }
            Ok(None)
        }
        Expr::Access { base, segments } => {
            if let Some(values) = evaluate_direct_static_access_up_to(
                base, segments, input, bindings, context, limit,
            )? {
                return Ok(Some(
                    values
                        .into_iter()
                        .map(|value| EvalFrame {
                            value,
                            bindings: bindings.clone(),
                        })
                        .collect(),
                ));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn evaluate_direct_static_access_up_to(
    base: &Expr,
    segments: &[Segment],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Option<Vec<Value>>, AqError> {
    if segments
        .iter()
        .any(|segment| matches!(segment, Segment::Lookup { .. } | Segment::Slice { .. }))
    {
        return Ok(None);
    }
    let base_values = evaluate_expr(base, input, bindings, context)?;
    let [value] = base_values.as_slice() else {
        return Ok(None);
    };
    if let Some(values) = evaluate_direct_static_segments_up_to(segments, value, limit) {
        return values.map(Some);
    }
    Ok(None)
}

fn evaluate_cached_constant_query_up_to(
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Option<Vec<EvalFrame>>, AqError> {
    if !query_is_simple_constant(query) {
        return Ok(None);
    }

    let cache_key = format!("{query:?}");
    let values = if let Some(values) = context.constant_query_values(&cache_key) {
        values
    } else {
        let frames =
            evaluate_query_up_to_uncached(query, &Value::Null, bindings, context, usize::MAX)?;
        let values = frames
            .into_iter()
            .map(|frame| frame.value)
            .collect::<Vec<_>>();
        context.cache_constant_query_values(cache_key, values.clone());
        values
    };

    Ok(Some(
        values
            .into_iter()
            .take(limit)
            .map(|value| EvalFrame {
                value,
                bindings: bindings.clone(),
            })
            .collect(),
    ))
}

fn evaluate_query_catching_label(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    let scope = prepare_query_scope(query, bindings, context);
    let (local_bindings, local_context) = if let Some(scope) = &scope {
        (&scope.bindings, &scope.context)
    } else {
        (bindings, context)
    };
    let mut out = Vec::new();
    for pipeline in &query.outputs {
        match evaluate_pipeline_catching_label(
            pipeline,
            input,
            local_bindings,
            local_context,
            catch_label,
        )? {
            LabelFlow::Continue(frames) => out.extend(frames),
            LabelFlow::Break(frames) => {
                out.extend(frames);
                return Ok(LabelFlow::Break(out));
            }
        }
    }
    Ok(LabelFlow::Continue(out))
}

fn evaluate_pipeline(
    pipeline: &Pipeline,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    evaluate_stages(
        &pipeline.stages,
        vec![EvalFrame {
            value: input.clone(),
            bindings: bindings.clone(),
        }],
        context,
    )
}

fn evaluate_pipeline_owned(
    pipeline: &Pipeline,
    input: Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    evaluate_stages(
        &pipeline.stages,
        vec![EvalFrame {
            value: input,
            bindings: bindings.clone(),
        }],
        context,
    )
}

fn evaluate_pipeline_last(
    pipeline: &Pipeline,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<EvalFrame>, AqError> {
    if let Some(result) = try_evaluate_last_range_pipeline(pipeline, input, bindings, context)? {
        return Ok(result);
    }

    evaluate_stages_last(
        &pipeline.stages,
        vec![EvalFrame {
            value: input.clone(),
            bindings: bindings.clone(),
        }],
        context,
    )
}

fn evaluate_pipeline_preserving_partial(
    pipeline: &Pipeline,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    evaluate_stages_preserving_partial(
        &pipeline.stages,
        vec![EvalFrame {
            value: input.clone(),
            bindings: bindings.clone(),
        }],
        context,
    )
}

fn evaluate_pipeline_catching_label(
    pipeline: &Pipeline,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    evaluate_stages_catching_label(
        &pipeline.stages,
        vec![EvalFrame {
            value: input.clone(),
            bindings: bindings.clone(),
        }],
        context,
        catch_label,
    )
}

fn evaluate_stages(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    let Some(index) = stages
        .iter()
        .position(|expr| matches!(expr, Expr::BindingAlt { .. }))
    else {
        return evaluate_plain_stages(stages, frames, context);
    };

    let prefix = evaluate_plain_stages(&stages[..index], frames, context)?;
    let Expr::BindingAlt { expr, patterns } = &stages[index] else {
        return Err(AqError::Query(
            "internal error: binding alternative lookup desynchronized".to_string(),
        ));
    };

    let mut out = Vec::new();
    for frame in prefix {
        out.extend(evaluate_binding_alternatives(
            expr,
            patterns,
            &stages[index + 1..],
            frame,
            context,
        )?);
    }
    Ok(out)
}

fn evaluate_stages_last(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Option<EvalFrame>, AqError> {
    let Some(index) = stages
        .iter()
        .position(|expr| matches!(expr, Expr::BindingAlt { .. }))
    else {
        return evaluate_plain_stages_last(stages, frames, context);
    };

    let prefix = evaluate_plain_stages(&stages[..index], frames, context)?;
    let Expr::BindingAlt { expr, patterns } = &stages[index] else {
        return Err(AqError::Query(
            "internal error: binding alternative lookup desynchronized".to_string(),
        ));
    };

    let mut last = None;
    for frame in prefix {
        let frames =
            evaluate_binding_alternatives(expr, patterns, &stages[index + 1..], frame, context)?;
        if let Some(frame) = frames.into_iter().last() {
            last = Some(frame);
        }
    }
    Ok(last)
}

fn evaluate_stages_preserving_partial(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    let Some(index) = stages
        .iter()
        .position(|expr| matches!(expr, Expr::BindingAlt { .. }))
    else {
        return evaluate_plain_stages_preserving_partial(stages, frames, context);
    };

    let prefix = evaluate_plain_stages_preserving_partial(&stages[..index], frames, context)?;
    let Expr::BindingAlt { expr, patterns } = &stages[index] else {
        return Err(PartialEvaluation {
            partial: Vec::new(),
            error: AqError::Query(
                "internal error: binding alternative lookup desynchronized".to_string(),
            ),
        });
    };

    let mut out = Vec::new();
    for frame in prefix {
        match evaluate_binding_alternatives_preserving_partial(
            expr,
            patterns,
            &stages[index + 1..],
            frame,
            context,
        ) {
            Ok(frames) => out.extend(frames),
            Err(PartialEvaluation { partial, error }) => {
                out.extend(partial);
                return Err(PartialEvaluation {
                    partial: out,
                    error,
                });
            }
        }
    }
    Ok(out)
}

fn evaluate_stages_catching_label(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    let Some(index) = stages
        .iter()
        .position(|expr| matches!(expr, Expr::BindingAlt { .. }))
    else {
        return evaluate_plain_stages_catching_label(stages, frames, context, catch_label);
    };

    let prefix =
        match evaluate_plain_stages_catching_label(&stages[..index], frames, context, catch_label)?
        {
            LabelFlow::Continue(frames) => frames,
            LabelFlow::Break(frames) => return Ok(LabelFlow::Break(frames)),
        };
    let Expr::BindingAlt { expr, patterns } = &stages[index] else {
        return Err(AqError::Query(
            "internal error: binding alternative lookup desynchronized".to_string(),
        ));
    };

    let mut out = Vec::new();
    for frame in prefix {
        match evaluate_binding_alternatives_catching_label(
            expr,
            patterns,
            &stages[index + 1..],
            frame,
            context,
            catch_label,
        )? {
            LabelFlow::Continue(frames) => out.extend(frames),
            LabelFlow::Break(frames) => {
                out.extend(frames);
                return Ok(LabelFlow::Break(out));
            }
        }
    }
    Ok(LabelFlow::Continue(out))
}

fn evaluate_plain_stages(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    let mut current = frames;
    let mut remaining = stages;
    while let Some((stage, suffix)) = remaining.split_first() {
        if stages_start_with_singleton_array_reduce_tojson_contains_skip_marker(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_singleton_array_reduce_tojson_contains_skip_marker_stages(
                    remaining, &current, context,
                )?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_repeat_string_slice_array(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_repeat_string_slice_array_stages(remaining, &current)?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_pow2_log2_round(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_pow2_log2_round_stages(remaining, &current)?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_sparse_array_reduce_tail_slice(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_sparse_array_reduce_tail_slice_stages(remaining, &current, context)?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_tojson_contains_skip_marker(remaining) {
            let (consumed, fused_frames) = apply_tojson_contains_skip_marker_stages(current)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        if stages_start_with_literal_strptime_mktime(remaining) {
            let format = literal_strptime_mktime_format_in_stages(remaining)
                .expect("validated literal strptime->mktime stages");
            let (consumed, fused_frames) = apply_strptime_mktime_stages(current, format)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        if stages_start_with_rfc3339_roundtrip(remaining) {
            let (consumed, fused_frames) = apply_rfc3339_roundtrip_stages(current)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        if let Some(fused_frames) = try_apply_bind_value_only_stages(remaining, &current, context) {
            current = fused_frames?;
            break;
        }
        current = apply_stage(stage, current, context)?;
        remaining = suffix;
    }
    Ok(current)
}

fn evaluate_plain_stages_last(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Option<EvalFrame>, AqError> {
    if stages_start_with_singleton_array_reduce_tojson_contains_skip_marker(stages) {
        if let Some((consumed, fused_frames)) =
            try_apply_singleton_array_reduce_tojson_contains_skip_marker_stages(
                stages, &frames, context,
            )?
        {
            return evaluate_plain_stages_last(&stages[consumed..], fused_frames, context);
        }
    }
    if stages_start_with_repeat_string_slice_array(stages) {
        if let Some((consumed, fused_frames)) =
            try_apply_repeat_string_slice_array_stages(stages, &frames)?
        {
            return evaluate_plain_stages_last(&stages[consumed..], fused_frames, context);
        }
    }
    if stages_start_with_pow2_log2_round(stages) {
        if let Some((consumed, fused_frames)) = try_apply_pow2_log2_round_stages(stages, &frames)? {
            return evaluate_plain_stages_last(&stages[consumed..], fused_frames, context);
        }
    }
    if stages_start_with_sparse_array_reduce_tail_slice(stages) {
        if let Some((consumed, fused_frames)) =
            try_apply_sparse_array_reduce_tail_slice_stages(stages, &frames, context)?
        {
            return evaluate_plain_stages_last(&stages[consumed..], fused_frames, context);
        }
    }
    if stages_start_with_tojson_contains_skip_marker(stages) {
        let (consumed, fused_frames) = apply_tojson_contains_skip_marker_stages(frames)?;
        return evaluate_plain_stages_last(&stages[consumed..], fused_frames, context);
    }
    if stages_start_with_literal_strptime_mktime(stages) {
        let format = literal_strptime_mktime_format_in_stages(stages)
            .expect("validated literal strptime->mktime stages");
        let (consumed, fused_frames) = apply_strptime_mktime_stages(frames, format)?;
        return evaluate_plain_stages_last(&stages[consumed..], fused_frames, context);
    }
    if stages_start_with_rfc3339_roundtrip(stages) {
        let (consumed, fused_frames) = apply_rfc3339_roundtrip_stages(frames)?;
        return evaluate_plain_stages_last(&stages[consumed..], fused_frames, context);
    }

    let Some((stage, suffix)) = stages.split_first() else {
        return Ok(frames.into_iter().last());
    };

    let mut last = None;
    for frame in frames {
        let next_frames = evaluate_stage(stage, frame, context)?;
        if let Some(frame) = evaluate_plain_stages_last(suffix, next_frames, context)? {
            last = Some(frame);
        }
    }
    Ok(last)
}

fn evaluate_plain_stages_preserving_partial(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    let mut current = frames;
    let mut remaining = stages;
    while let Some((stage, suffix)) = remaining.split_first() {
        if stages_start_with_singleton_array_reduce_tojson_contains_skip_marker(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_singleton_array_reduce_tojson_contains_skip_marker_stages(
                    remaining, &current, context,
                )
                .map_err(|error| PartialEvaluation {
                    partial: Vec::new(),
                    error,
                })?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_repeat_string_slice_array(remaining) {
            if let Some((consumed, fused_frames)) = try_apply_repeat_string_slice_array_stages(
                remaining, &current,
            )
            .map_err(|error| PartialEvaluation {
                partial: Vec::new(),
                error,
            })? {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_pow2_log2_round(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_pow2_log2_round_stages(remaining, &current).map_err(|error| {
                    PartialEvaluation {
                        partial: Vec::new(),
                        error,
                    }
                })?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_sparse_array_reduce_tail_slice(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_sparse_array_reduce_tail_slice_stages(remaining, &current, context)
                    .map_err(|error| PartialEvaluation {
                        partial: Vec::new(),
                        error,
                    })?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_tojson_contains_skip_marker(remaining) {
            let (consumed, fused_frames) =
                apply_tojson_contains_skip_marker_stages_preserving_partial(current)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        if stages_start_with_literal_strptime_mktime(remaining) {
            let format = literal_strptime_mktime_format_in_stages(remaining)
                .expect("validated literal strptime->mktime stages");
            let (consumed, fused_frames) =
                apply_strptime_mktime_stages_preserving_partial(current, format)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        if stages_start_with_rfc3339_roundtrip(remaining) {
            let (consumed, fused_frames) =
                apply_rfc3339_roundtrip_stages_preserving_partial(current)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        current = apply_stage_preserving_partial(stage, current, context)?;
        remaining = suffix;
    }
    Ok(current)
}

fn evaluate_plain_stages_catching_label(
    stages: &[Expr],
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    let mut current = frames;
    let mut remaining = stages;
    while let Some((stage, suffix)) = remaining.split_first() {
        if stages_start_with_singleton_array_reduce_tojson_contains_skip_marker(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_singleton_array_reduce_tojson_contains_skip_marker_stages(
                    remaining, &current, context,
                )?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_repeat_string_slice_array(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_repeat_string_slice_array_stages(remaining, &current)?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_pow2_log2_round(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_pow2_log2_round_stages(remaining, &current)?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_sparse_array_reduce_tail_slice(remaining) {
            if let Some((consumed, fused_frames)) =
                try_apply_sparse_array_reduce_tail_slice_stages(remaining, &current, context)?
            {
                current = fused_frames;
                remaining = &remaining[consumed..];
                continue;
            }
        }
        if stages_start_with_tojson_contains_skip_marker(remaining) {
            let (consumed, fused_frames) = apply_tojson_contains_skip_marker_stages(current)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        if stages_start_with_literal_strptime_mktime(remaining) {
            let format = literal_strptime_mktime_format_in_stages(remaining)
                .expect("validated literal strptime->mktime stages");
            let (consumed, fused_frames) = apply_strptime_mktime_stages(current, format)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        if stages_start_with_rfc3339_roundtrip(remaining) {
            let (consumed, fused_frames) = apply_rfc3339_roundtrip_stages(current)?;
            current = fused_frames;
            remaining = &remaining[consumed..];
            continue;
        }
        match apply_stage_catching_label(stage, current, context, catch_label)? {
            LabelFlow::Continue(next) => {
                current = next;
                remaining = suffix;
            }
            LabelFlow::Break(next) => return Ok(LabelFlow::Break(next)),
        }
    }
    Ok(LabelFlow::Continue(current))
}

fn try_apply_pow2_log2_round_stages(
    stages: &[Expr],
    frames: &[EvalFrame],
) -> Result<Option<(usize, Vec<EvalFrame>)>, AqError> {
    let [Expr::Builtin(BuiltinExpr::Pow { base, exponent }), Expr::Builtin(BuiltinExpr::Log2), Expr::Builtin(BuiltinExpr::Round), ..] =
        stages
    else {
        return Ok(None);
    };
    if !query_is_identity(exponent) || !query_is_numeric_literal(base, 2.0) {
        return Ok(None);
    }
    if !frames
        .iter()
        .all(|frame| value_is_pow2_log2_round_safe(&frame.value))
    {
        return Ok(None);
    }

    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        out.push(EvalFrame {
            value: round_of(&frame.value)?,
            bindings: frame.bindings.clone(),
        });
    }
    Ok(Some((3, out)))
}

fn try_apply_repeat_string_slice_array_stages(
    stages: &[Expr],
    frames: &[EvalFrame],
) -> Result<Option<(usize, Vec<EvalFrame>)>, AqError> {
    let [Expr::Binary {
        left,
        op: BinaryOp::Mul,
        right,
    }, Expr::Array(items), ..] = stages
    else {
        return Ok(None);
    };
    let repeat_count = if expr_is_identity_path(left) {
        expr_numeric_constant(right)
    } else if expr_is_identity_path(right) {
        expr_numeric_constant(left)
    } else {
        None
    };
    let Some(repeat_count) = repeat_count else {
        return Ok(None);
    };
    if !repeat_count.is_finite() || repeat_count < 0.0 {
        return Ok(None);
    }

    let [Expr::Subquery(query)] = items.as_slice() else {
        return Ok(None);
    };
    let Some(slices) = query_collects_string_slices(query) else {
        return Ok(None);
    };

    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let Value::String(text) = frame.value.untagged() else {
            return Ok(None);
        };
        out.push(EvalFrame {
            value: repeated_string_slice_array_value(text, repeat_count, &slices)?,
            bindings: frame.bindings.clone(),
        });
    }
    Ok(Some((2, out)))
}

fn try_apply_singleton_array_reduce_tojson_contains_skip_marker_stages(
    stages: &[Expr],
    frames: &[EvalFrame],
    context: &EvaluationContext,
) -> Result<Option<(usize, Vec<EvalFrame>)>, AqError> {
    let [Expr::Reduce {
        source,
        pattern,
        init,
        update,
    }, Expr::Builtin(BuiltinExpr::ToJson), Expr::Builtin(BuiltinExpr::Contains(expected)), ..] =
        stages
    else {
        return Ok(None);
    };
    let Expr::Literal(Value::String(value)) = expected.as_ref() else {
        return Ok(None);
    };
    if value != JSON_SKIP_MARKER
        || !matches!(pattern, BindingPattern::Variable(_))
        || !query_is_empty_array_literal(init)
        || !query_wraps_input_in_singleton_array(update)
    {
        return Ok(None);
    }

    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let Some(iterations) =
            direct_range_iteration_count(source, &frame.value, &frame.bindings, context)?
        else {
            return Ok(None);
        };
        out.push(EvalFrame {
            value: Value::Bool(iterations.saturating_add(1) > MAX_JSON_PRINT_DEPTH),
            bindings: frame.bindings.clone(),
        });
    }
    Ok(Some((3, out)))
}

fn try_apply_sparse_array_reduce_tail_slice_stages(
    stages: &[Expr],
    frames: &[EvalFrame],
    context: &EvaluationContext,
) -> Result<Option<(usize, Vec<EvalFrame>)>, AqError> {
    let [Expr::Reduce {
        source,
        pattern,
        init,
        update,
    }, Expr::Path(PathExpr { segments }), ..] = stages
    else {
        return Ok(None);
    };
    let [Segment::Slice {
        start,
        end,
        optional: false,
    }] = segments.as_slice()
    else {
        return Ok(None);
    };

    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        if sparse_array_reduce_assignment_binding_name(
            pattern,
            init,
            update,
            &frame.value,
            &frame.bindings,
            context,
        )?
        .is_none()
        {
            return Ok(None);
        }
        let Some(indexes) =
            direct_range_exact_integer_values(source, &frame.value, &frame.bindings, context)?
        else {
            return Ok(None);
        };
        out.push(EvalFrame {
            value: sparse_array_slice_from_indexes(&indexes, *start, *end)?,
            bindings: frame.bindings.clone(),
        });
    }
    Ok(Some((2, out)))
}

fn stages_start_with_sparse_array_reduce_tail_slice(stages: &[Expr]) -> bool {
    matches!(
        stages,
        [
            Expr::Reduce { .. },
            Expr::Path(PathExpr { segments }),
            ..
        ] if matches!(
            segments.as_slice(),
            [Segment::Slice {
                optional: false,
                ..
            }]
        )
    )
}

fn stages_start_with_singleton_array_reduce_tojson_contains_skip_marker(stages: &[Expr]) -> bool {
    matches!(
        stages,
        [
            Expr::Reduce { .. },
            Expr::Builtin(BuiltinExpr::ToJson),
            Expr::Builtin(BuiltinExpr::Contains(_)),
            ..
        ]
    )
}

fn stages_start_with_repeat_string_slice_array(stages: &[Expr]) -> bool {
    matches!(
        stages,
        [
            Expr::Binary {
                op: BinaryOp::Mul,
                ..
            },
            Expr::Array(items),
            ..
        ] if matches!(items.as_slice(), [Expr::Subquery(_)])
    )
}

fn stages_start_with_pow2_log2_round(stages: &[Expr]) -> bool {
    matches!(
        stages,
        [
            Expr::Builtin(BuiltinExpr::Pow { .. }),
            Expr::Builtin(BuiltinExpr::Log2),
            Expr::Builtin(BuiltinExpr::Round),
            ..
        ]
    )
}

fn apply_rfc3339_roundtrip_stages(
    frames: Vec<EvalFrame>,
) -> Result<(usize, Vec<EvalFrame>), AqError> {
    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let bindings = frame.bindings.clone();
        let value = rfc3339_roundtrip_of(&frame.value)?;
        dispose_value(frame.value);
        out.push(EvalFrame { value, bindings });
    }
    Ok((2, out))
}

fn apply_strptime_mktime_stages(
    frames: Vec<EvalFrame>,
    format: &str,
) -> Result<(usize, Vec<EvalFrame>), AqError> {
    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let bindings = frame.bindings.clone();
        let value = strptime_mktime_of(&frame.value, format)?;
        dispose_value(frame.value);
        out.push(EvalFrame { value, bindings });
    }
    Ok((2, out))
}

fn apply_tojson_contains_skip_marker_stages(
    frames: Vec<EvalFrame>,
) -> Result<(usize, Vec<EvalFrame>), AqError> {
    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let bindings = frame.bindings.clone();
        let value = match frame.value.json_text_contains_skip_marker_fast_path() {
            Some(found) => Value::Bool(found),
            None => Value::Bool(to_json_of(&frame.value)?.contains(JSON_SKIP_MARKER)),
        };
        dispose_value(frame.value);
        out.push(EvalFrame { value, bindings });
    }
    Ok((2, out))
}

fn apply_tojson_contains_skip_marker_stages_preserving_partial(
    frames: Vec<EvalFrame>,
) -> Result<(usize, Vec<EvalFrame>), PartialEvaluation<Vec<EvalFrame>>> {
    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let bindings = frame.bindings.clone();
        let value = match frame.value.json_text_contains_skip_marker_fast_path() {
            Some(found) => Value::Bool(found),
            None => match to_json_of(&frame.value) {
                Ok(rendered) => Value::Bool(rendered.contains(JSON_SKIP_MARKER)),
                Err(error) => {
                    return Err(PartialEvaluation {
                        partial: out,
                        error,
                    });
                }
            },
        };
        dispose_value(frame.value);
        out.push(EvalFrame { value, bindings });
    }
    Ok((2, out))
}

fn apply_strptime_mktime_stages_preserving_partial(
    frames: Vec<EvalFrame>,
    format: &str,
) -> Result<(usize, Vec<EvalFrame>), PartialEvaluation<Vec<EvalFrame>>> {
    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let bindings = frame.bindings.clone();
        let value = match strptime_mktime_of(&frame.value, format) {
            Ok(value) => value,
            Err(error) => {
                return Err(PartialEvaluation {
                    partial: out,
                    error,
                });
            }
        };
        dispose_value(frame.value);
        out.push(EvalFrame { value, bindings });
    }
    Ok((2, out))
}

fn apply_rfc3339_roundtrip_stages_preserving_partial(
    frames: Vec<EvalFrame>,
) -> Result<(usize, Vec<EvalFrame>), PartialEvaluation<Vec<EvalFrame>>> {
    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        let bindings = frame.bindings.clone();
        let value = match rfc3339_roundtrip_of(&frame.value) {
            Ok(value) => value,
            Err(error) => {
                return Err(PartialEvaluation {
                    partial: out,
                    error,
                })
            }
        };
        dispose_value(frame.value);
        out.push(EvalFrame { value, bindings });
    }
    Ok((2, out))
}

fn stages_start_with_rfc3339_roundtrip(stages: &[Expr]) -> bool {
    matches!(
        stages,
        [
            Expr::Builtin(BuiltinExpr::StrFTime(format)),
            Expr::Builtin(BuiltinExpr::StrPTime(parse_format)),
            ..
        ] if literal_time_format_argument(format.as_ref()) == Some(RFC3339_UTC_SECONDS_FORMAT)
            && literal_time_format_argument(parse_format.as_ref()) == Some(RFC3339_UTC_SECONDS_FORMAT)
    )
}

fn literal_strptime_mktime_format_expr(stages: &[Expr]) -> Option<&Expr> {
    let [Expr::Builtin(BuiltinExpr::StrPTime(format)), Expr::Builtin(BuiltinExpr::MkTime), ..] =
        stages
    else {
        return None;
    };
    Some(format.as_ref())
}

fn literal_strptime_mktime_format_in_stages(stages: &[Expr]) -> Option<&str> {
    let format_expr = literal_strptime_mktime_format_expr(stages)?;
    literal_time_format_argument(format_expr)
}

fn stages_start_with_literal_strptime_mktime(stages: &[Expr]) -> bool {
    literal_strptime_mktime_format_in_stages(stages).is_some()
}

fn stages_start_with_tojson_contains_skip_marker(stages: &[Expr]) -> bool {
    matches!(
        stages,
        [
            Expr::Builtin(BuiltinExpr::ToJson),
            Expr::Builtin(BuiltinExpr::Contains(expected)),
            ..
        ] if matches!(
            expected.as_ref(),
            Expr::Literal(Value::String(value)) if value == JSON_SKIP_MARKER
        )
    )
}

fn rfc3339_roundtrip_of(value: &Value) -> Result<Value, AqError> {
    let (datetime, _) = value_to_utc_datetime("strftime/1", value)?;
    Ok(parsed_datetime_value(&datetime, 0))
}

fn evaluate_binding_alternatives(
    expr: &Expr,
    patterns: &[BindingPattern],
    suffix: &[Expr],
    frame: EvalFrame,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    let bound_values = evaluate_expr(expr, &frame.value, &frame.bindings, context)?;
    let variables = binding_alternative_variables(patterns);
    let mut base_bindings = frame.bindings.clone();
    for name in &variables {
        base_bindings.insert_value(name.clone(), Value::Null);
    }
    let mut out = Vec::new();

    for bound_value in bound_values {
        let mut last_error = None;
        let mut succeeded = false;

        for pattern in patterns {
            let mut bindings = base_bindings.clone();

            match bind_pattern(pattern, &bound_value, &mut bindings, context) {
                Ok(()) => {
                    if suffix.is_empty() {
                        out.push(EvalFrame {
                            value: frame.value.clone(),
                            bindings,
                        });
                        succeeded = true;
                        break;
                    }

                    if let [stage] = suffix {
                        if let Some(value) =
                            direct_single_value_expr_value(stage, &frame.value, &bindings)
                        {
                            out.push(EvalFrame { value, bindings });
                            succeeded = true;
                            break;
                        }
                    }

                    if stages_are_value_only(suffix) {
                        match evaluate_plain_stages_values(
                            suffix,
                            vec![frame.value.clone()],
                            &bindings,
                            context,
                        ) {
                            Ok(values) => {
                                out.extend(values.into_iter().map(|value| EvalFrame {
                                    value,
                                    bindings: bindings.clone(),
                                }));
                                succeeded = true;
                                break;
                            }
                            Err(error) => {
                                last_error = Some(error);
                            }
                        }
                        continue;
                    }

                    match evaluate_stages(
                        suffix,
                        vec![EvalFrame {
                            value: frame.value.clone(),
                            bindings,
                        }],
                        context,
                    ) {
                        Ok(frames) => {
                            out.extend(frames);
                            succeeded = true;
                            break;
                        }
                        Err(error) => {
                            last_error = Some(error);
                        }
                    }
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }

        if !succeeded {
            return Err(last_error.unwrap_or_else(|| {
                AqError::Query(
                    "internal error: binding alternative produced no outcome".to_string(),
                )
            }));
        }
    }

    Ok(out)
}

fn evaluate_binding_alternatives_preserving_partial(
    expr: &Expr,
    patterns: &[BindingPattern],
    suffix: &[Expr],
    frame: EvalFrame,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    let bound_values =
        evaluate_expr(expr, &frame.value, &frame.bindings, context).map_err(|error| {
            PartialEvaluation {
                partial: Vec::new(),
                error,
            }
        })?;
    let variables = binding_alternative_variables(patterns);
    let mut base_bindings = frame.bindings.clone();
    for name in &variables {
        base_bindings.insert_value(name.clone(), Value::Null);
    }
    let mut out = Vec::new();

    for bound_value in bound_values {
        let mut last_error = None;
        let mut last_partial = Vec::new();
        let mut succeeded = false;

        for pattern in patterns {
            let mut bindings = base_bindings.clone();

            match bind_pattern(pattern, &bound_value, &mut bindings, context) {
                Ok(()) => match evaluate_stages_preserving_partial(
                    suffix,
                    vec![EvalFrame {
                        value: frame.value.clone(),
                        bindings,
                    }],
                    context,
                ) {
                    Ok(frames) => {
                        out.extend(frames);
                        succeeded = true;
                        break;
                    }
                    Err(PartialEvaluation { partial, error }) => {
                        last_error = Some(error);
                        last_partial = partial;
                    }
                },
                Err(error) => {
                    last_error = Some(error);
                    last_partial = Vec::new();
                }
            }
        }

        if !succeeded {
            return Err(PartialEvaluation {
                partial: last_partial,
                error: last_error.unwrap_or_else(|| {
                    AqError::Query(
                        "internal error: binding alternative produced no outcome".to_string(),
                    )
                }),
            });
        }
    }

    Ok(out)
}

fn binding_alternative_variables(patterns: &[BindingPattern]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for pattern in patterns {
        collect_binding_pattern_variables(pattern, &mut names);
    }
    names.into_iter().collect()
}

fn collect_binding_pattern_variables(pattern: &BindingPattern, names: &mut BTreeSet<String>) {
    match pattern {
        BindingPattern::Variable(name) => {
            names.insert(name.clone());
        }
        BindingPattern::Array(patterns) => {
            for pattern in patterns {
                collect_binding_pattern_variables(pattern, names);
            }
        }
        BindingPattern::Object(fields) => {
            for field in fields {
                if let Some(name) = &field.bind_name {
                    names.insert(name.clone());
                }
                collect_binding_pattern_variables(&field.pattern, names);
            }
        }
    }
}

fn apply_stage(
    expr: &Expr,
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    let mut out = Vec::new();
    for frame in frames {
        out.extend(evaluate_stage(expr, frame, context)?);
    }
    Ok(out)
}

fn dispose_value(value: Value) {
    if !value.nesting_depth_exceeds(LARGE_VALUE_STACK_THRESHOLD) {
        drop(value);
        return;
    }

    if let Ok(handle) = thread::Builder::new()
        .stack_size(LARGE_VALUE_STACK_SIZE)
        .spawn(move || drop(value))
    {
        let _ = handle.join();
    }
}

fn apply_stage_preserving_partial(
    expr: &Expr,
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    let mut out = Vec::new();
    for frame in frames {
        match evaluate_stage_preserving_partial(expr, frame, context) {
            Ok(frames) => out.extend(frames),
            Err(PartialEvaluation { partial, error }) => {
                out.extend(partial);
                return Err(PartialEvaluation {
                    partial: out,
                    error,
                });
            }
        }
    }
    Ok(out)
}

fn apply_stage_catching_label(
    expr: &Expr,
    frames: Vec<EvalFrame>,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    let mut out = Vec::new();
    for frame in frames {
        match evaluate_stage_catching_label(expr, frame, context, catch_label)? {
            LabelFlow::Continue(frames) => out.extend(frames),
            LabelFlow::Break(frames) => {
                out.extend(frames);
                return Ok(LabelFlow::Break(out));
            }
        }
    }
    Ok(LabelFlow::Continue(out))
}

fn evaluate_binding_alternatives_catching_label(
    expr: &Expr,
    patterns: &[BindingPattern],
    suffix: &[Expr],
    frame: EvalFrame,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    let bound_values = evaluate_expr(expr, &frame.value, &frame.bindings, context)?;
    let variables = binding_alternative_variables(patterns);
    let mut base_bindings = frame.bindings.clone();
    for name in &variables {
        base_bindings.insert_value(name.clone(), Value::Null);
    }
    let mut out = Vec::new();

    for bound_value in bound_values {
        let mut last_error = None;
        let mut succeeded = false;

        for pattern in patterns {
            let mut bindings = base_bindings.clone();

            match bind_pattern(pattern, &bound_value, &mut bindings, context) {
                Ok(()) => match evaluate_stages_catching_label(
                    suffix,
                    vec![EvalFrame {
                        value: frame.value.clone(),
                        bindings,
                    }],
                    context,
                    catch_label,
                ) {
                    Ok(LabelFlow::Continue(frames)) => {
                        out.extend(frames);
                        succeeded = true;
                        break;
                    }
                    Ok(LabelFlow::Break(frames)) => {
                        out.extend(frames);
                        return Ok(LabelFlow::Break(out));
                    }
                    Err(error) => {
                        last_error = Some(error);
                    }
                },
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }

        if !succeeded {
            return Err(last_error.unwrap_or_else(|| {
                AqError::Query(
                    "internal error: binding alternative produced no outcome".to_string(),
                )
            }));
        }
    }
    Ok(LabelFlow::Continue(out))
}

fn evaluate_stage(
    expr: &Expr,
    frame: EvalFrame,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    match expr {
        Expr::Bind { expr, pattern } => evaluate_binding(expr, pattern, frame, context),
        Expr::BindingAlt { .. } => Err(AqError::Query(
            "internal error: binding alternative reached direct stage evaluator".to_string(),
        )),
        Expr::Assign { path, op, value } => {
            let bindings = frame.bindings.clone();
            let values =
                evaluate_assign_owned(path, *op, value, frame.value, &frame.bindings, context)?;
            Ok(values
                .into_iter()
                .map(|value| EvalFrame {
                    value,
                    bindings: bindings.clone(),
                })
                .collect())
        }
        _ => {
            let bindings = frame.bindings.clone();
            let values = evaluate_expr(expr, &frame.value, &frame.bindings, context)?;
            dispose_value(frame.value);
            Ok(values
                .into_iter()
                .map(|value| EvalFrame {
                    value,
                    bindings: bindings.clone(),
                })
                .collect())
        }
    }
}

fn evaluate_stage_preserving_partial(
    expr: &Expr,
    frame: EvalFrame,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    match expr {
        Expr::Bind { expr, pattern } => {
            evaluate_binding_preserving_partial(expr, pattern, frame, context)
        }
        Expr::BindingAlt { .. } => Err(PartialEvaluation {
            partial: Vec::new(),
            error: AqError::Query(
                "internal error: binding alternative reached direct stage evaluator".to_string(),
            ),
        }),
        Expr::Assign { path, op, value } => {
            evaluate_assign_owned(path, *op, value, frame.value, &frame.bindings, context)
                .map(|values| {
                    values
                        .into_iter()
                        .map(|value| EvalFrame {
                            value,
                            bindings: frame.bindings.clone(),
                        })
                        .collect()
                })
                .map_err(|error| PartialEvaluation {
                    partial: Vec::new(),
                    error,
                })
        }
        _ => evaluate_expr(expr, &frame.value, &frame.bindings, context)
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| EvalFrame {
                        value,
                        bindings: frame.bindings.clone(),
                    })
                    .collect()
            })
            .map_err(|error| PartialEvaluation {
                partial: Vec::new(),
                error,
            }),
    }
}

fn evaluate_stage_catching_label(
    expr: &Expr,
    frame: EvalFrame,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    match expr {
        Expr::Bind { expr, pattern } => {
            evaluate_binding_catching_label(expr, pattern, frame, context, catch_label)
        }
        Expr::BindingAlt { .. } => Err(AqError::Query(
            "internal error: binding alternative reached direct stage evaluator".to_string(),
        )),
        _ => match evaluate_expr_catching_label(
            expr,
            &frame.value,
            &frame.bindings,
            context,
            catch_label,
        ) {
            Ok(LabelFlow::Continue(values)) => Ok(LabelFlow::Continue(
                values
                    .into_iter()
                    .map(|value| EvalFrame {
                        value,
                        bindings: frame.bindings.clone(),
                    })
                    .collect(),
            )),
            Ok(LabelFlow::Break(values)) => Ok(LabelFlow::Break(
                values
                    .into_iter()
                    .map(|value| EvalFrame {
                        value,
                        bindings: frame.bindings.clone(),
                    })
                    .collect(),
            )),
            Err(AqError::BreakLabel(name)) if name == catch_label => {
                Ok(LabelFlow::Break(Vec::new()))
            }
            Err(error) => Err(error),
        },
    }
}

fn evaluate_binding(
    expr: &Expr,
    pattern: &BindingPattern,
    frame: EvalFrame,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, AqError> {
    let bound_values = evaluate_expr(expr, &frame.value, &frame.bindings, context)?;
    let mut out = Vec::with_capacity(bound_values.len());
    for bound_value in bound_values {
        let mut bindings = frame.bindings.clone();
        bind_pattern(pattern, &bound_value, &mut bindings, context)?;
        out.push(EvalFrame {
            value: frame.value.clone(),
            bindings,
        });
    }
    Ok(out)
}

fn evaluate_binding_preserving_partial(
    expr: &Expr,
    pattern: &BindingPattern,
    frame: EvalFrame,
    context: &EvaluationContext,
) -> Result<Vec<EvalFrame>, PartialEvaluation<Vec<EvalFrame>>> {
    let bound_values =
        evaluate_expr(expr, &frame.value, &frame.bindings, context).map_err(|error| {
            PartialEvaluation {
                partial: Vec::new(),
                error,
            }
        })?;
    let mut out = Vec::with_capacity(bound_values.len());
    for bound_value in bound_values {
        let mut bindings = frame.bindings.clone();
        if let Err(error) = bind_pattern(pattern, &bound_value, &mut bindings, context) {
            return Err(PartialEvaluation {
                partial: out,
                error,
            });
        }
        out.push(EvalFrame {
            value: frame.value.clone(),
            bindings,
        });
    }
    Ok(out)
}

fn evaluate_binding_catching_label(
    expr: &Expr,
    pattern: &BindingPattern,
    frame: EvalFrame,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<EvalFrame>>, AqError> {
    match evaluate_expr_catching_label(expr, &frame.value, &frame.bindings, context, catch_label) {
        Ok(LabelFlow::Continue(bound_values)) => {
            let mut out = Vec::with_capacity(bound_values.len());
            for bound_value in bound_values {
                let mut bindings = frame.bindings.clone();
                bind_pattern(pattern, &bound_value, &mut bindings, context)?;
                out.push(EvalFrame {
                    value: frame.value.clone(),
                    bindings,
                });
            }
            Ok(LabelFlow::Continue(out))
        }
        Ok(LabelFlow::Break(_)) => Ok(LabelFlow::Break(Vec::new())),
        Err(AqError::BreakLabel(name)) if name == catch_label => Ok(LabelFlow::Break(Vec::new())),
        Err(error) => Err(error),
    }
}

fn evaluate_reduce(
    source: &Query,
    pattern: &BindingPattern,
    init: &Query,
    update: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(values) = try_evaluate_sparse_array_reduce_assignment(
        source, pattern, init, update, input, bindings, context,
    )? {
        return Ok(values);
    }
    if let Some(values) = try_evaluate_direct_iter_reduce_add(
        source, pattern, init, update, input, bindings, context,
    )? {
        return Ok(values);
    }

    let mut accumulators = evaluate_query(init, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();

    if query_wraps_input_in_singleton_array(update) {
        if matches!(pattern, BindingPattern::Variable(_)) {
            if let Some(iterations) =
                direct_range_iteration_count(source, input, bindings, context)?
            {
                for _ in 0..iterations {
                    accumulators = accumulators
                        .into_iter()
                        .map(|accumulator| Value::Array(vec![accumulator]))
                        .collect();
                }
                return Ok(accumulators);
            }
        }

        let items = evaluate_query(source, input, bindings, context)?
            .into_iter()
            .map(|frame| frame.value)
            .collect::<Vec<_>>();
        for item in items {
            let mut iteration_bindings = bindings.clone();
            bind_pattern(pattern, &item, &mut iteration_bindings, context)?;
            let mut next = Vec::with_capacity(accumulators.len());
            for accumulator in accumulators {
                next.push(Value::Array(vec![accumulator]));
            }
            accumulators = next;
        }
        return Ok(accumulators);
    }

    let items = evaluate_query(source, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
    for item in items {
        let mut iteration_bindings = bindings.clone();
        bind_pattern(pattern, &item, &mut iteration_bindings, context)?;
        let mut next = Vec::new();
        for accumulator in accumulators {
            next.extend(
                evaluate_query_owned(update, accumulator, &iteration_bindings, context)?
                    .into_iter()
                    .map(|frame| frame.value),
            );
        }
        accumulators = next;
    }

    Ok(accumulators)
}

fn try_evaluate_direct_iter_reduce_add(
    source: &Query,
    pattern: &BindingPattern,
    init: &Query,
    update: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<Vec<Value>>, AqError> {
    if !query_is_direct_iterate(source) {
        return Ok(None);
    }

    let BindingPattern::Variable(binding_name) = pattern else {
        return Ok(None);
    };
    let Some(update_expr) = direct_stage_expr(update) else {
        return Ok(None);
    };
    let Some(operand_order) = reduce_add_binding_order(update_expr, binding_name) else {
        return Ok(None);
    };

    let mut accumulators = evaluate_query(init, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
    if accumulators.is_empty() {
        return Ok(Some(Vec::new()));
    }

    if let Some(values) = try_reduce_add_integer_inputs_fast_path(input, &accumulators)? {
        return Ok(Some(
            values.into_iter().map(Value::Integer).collect::<Vec<_>>(),
        ));
    }

    let items = iterate_input_values(input)?;
    for item in items {
        for accumulator in &mut accumulators {
            let next = match operand_order {
                ReduceAddBindingOrder::Right => value_add(accumulator, &item)?,
                ReduceAddBindingOrder::Left => value_add(&item, accumulator)?,
            };
            *accumulator = next;
        }
    }
    Ok(Some(accumulators))
}

fn try_reduce_add_integer_inputs_fast_path(
    input: &Value,
    accumulators: &[Value],
) -> Result<Option<Vec<i64>>, AqError> {
    let items = match input.untagged() {
        Value::Array(values) => values,
        Value::Object(values) => {
            return try_reduce_add_integer_values_fast_path(values.values(), accumulators)
        }
        _ => return Ok(None),
    };
    try_reduce_add_integer_values_fast_path(items.iter(), accumulators)
}

fn try_reduce_add_integer_values_fast_path<'a>(
    items: impl IntoIterator<Item = &'a Value>,
    accumulators: &[Value],
) -> Result<Option<Vec<i64>>, AqError> {
    let mut totals = Vec::with_capacity(accumulators.len());
    for accumulator in accumulators {
        let Value::Integer(value) = accumulator.untagged() else {
            return Ok(None);
        };
        if !integer_is_safe_in_f64(*value) {
            return Ok(None);
        }
        totals.push(*value);
    }

    for item in items {
        let Value::Integer(value) = item.untagged() else {
            return Ok(None);
        };
        if !integer_is_safe_in_f64(*value) {
            return Ok(None);
        }
        for total in &mut totals {
            *total = total
                .checked_add(*value)
                .ok_or_else(|| AqError::Query("integer addition overflow".to_string()))?;
        }
    }

    Ok(Some(totals))
}

fn query_is_direct_iterate(query: &Query) -> bool {
    matches!(
        direct_stage_expr(query),
        Some(Expr::Path(PathExpr { segments }))
            if matches!(segments.as_slice(), [Segment::Iterate { optional: false }])
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReduceAddBindingOrder {
    Left,
    Right,
}

fn reduce_add_binding_order(expr: &Expr, binding_name: &str) -> Option<ReduceAddBindingOrder> {
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    if *op != BinaryOp::Add {
        return None;
    }
    if expr_is_identity_path(left)
        && matches!(right.as_ref(), Expr::Variable(name) if name == binding_name)
    {
        return Some(ReduceAddBindingOrder::Right);
    }
    if expr_is_identity_path(right)
        && matches!(left.as_ref(), Expr::Variable(name) if name == binding_name)
    {
        return Some(ReduceAddBindingOrder::Left);
    }
    None
}

fn try_evaluate_sparse_array_reduce_assignment(
    source: &Query,
    pattern: &BindingPattern,
    init: &Query,
    update: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<Vec<Value>>, AqError> {
    if sparse_array_reduce_assignment_binding_name(pattern, init, update, input, bindings, context)?
        .is_none()
    {
        return Ok(None);
    }

    let Some(indexes) = direct_range_exact_integer_values(source, input, bindings, context)? else {
        return Ok(None);
    };
    let value = sparse_array_from_indexes(&indexes)?;
    Ok(Some(vec![value]))
}

fn sparse_array_reduce_assignment_binding_name<'a>(
    pattern: &'a BindingPattern,
    init: &Query,
    update: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<&'a str>, AqError> {
    let init_values = evaluate_query(init, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
    let [Value::Array(initial)] = init_values.as_slice() else {
        return Ok(None);
    };
    if !initial.is_empty() {
        return Ok(None);
    }

    let BindingPattern::Variable(name) = pattern else {
        return Ok(None);
    };
    if !update_is_sparse_array_reduce_assignment(update, name) {
        return Ok(None);
    }
    Ok(Some(name.as_str()))
}

fn update_is_sparse_array_reduce_assignment(update: &Query, binding_name: &str) -> bool {
    let Some(Expr::Assign { path, op, value }) = direct_stage_expr(update) else {
        return false;
    };
    if *op != AssignOp::Set {
        return false;
    }
    if !matches!(value.as_ref(), Expr::Variable(name) if name == binding_name) {
        return false;
    }
    path_is_single_lookup_variable(path, binding_name)
}

fn path_is_single_lookup_variable(path: &Query, binding_name: &str) -> bool {
    matches!(
        direct_stage_expr(path),
        Some(Expr::Path(PathExpr { segments }))
            if matches!(
                segments.as_slice(),
                [Segment::Lookup { expr, optional: false }]
                    if expr_is_variable_lookup(expr, binding_name)
            )
    )
}

fn expr_is_variable_lookup(expr: &Expr, binding_name: &str) -> bool {
    match expr {
        Expr::Variable(name) => name == binding_name,
        Expr::Subquery(query) => matches!(
            direct_stage_expr(query),
            Some(Expr::Variable(name)) if name == binding_name
        ),
        _ => false,
    }
}

fn direct_range_exact_integer_values(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<Vec<i64>>, AqError> {
    let Some(Expr::Builtin(BuiltinExpr::Range(args))) = direct_stage_expr(query) else {
        return Ok(None);
    };

    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        let outputs = evaluate_query(arg, input, bindings, context)?
            .into_iter()
            .map(|frame| frame.value)
            .collect::<Vec<_>>();
        let [value] = outputs.as_slice() else {
            return Ok(None);
        };
        let Some(integer) = exact_integer_value(value) else {
            return Ok(None);
        };
        values.push(integer);
    }

    let range = match values.as_slice() {
        [end] => exact_integer_range_values(0, *end, 1),
        [start, end] => exact_integer_range_values(*start, *end, 1),
        [start, end, step] => exact_integer_range_values(*start, *end, *step),
        _ => return Err(AqError::Query("range expects 1 to 3 arguments".to_string())),
    };
    Ok(range)
}

fn exact_integer_value(value: &Value) -> Option<i64> {
    match value.untagged() {
        Value::Integer(value) => Some(*value),
        Value::Decimal(value) => value.as_i64_exact(),
        Value::Float(value) if value.is_finite() && value.fract() == 0.0 => {
            if *value >= i64::MIN as f64 && *value <= i64::MAX as f64 {
                Some(*value as i64)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn exact_integer_range_values(start: i64, end: i64, step: i64) -> Option<Vec<i64>> {
    if step == 0 {
        return Some(Vec::new());
    }

    let mut current = start;
    let mut out = Vec::new();
    if step > 0 {
        while current < end {
            out.push(current);
            current = current.checked_add(step)?;
        }
    } else {
        while current > end {
            out.push(current);
            current = current.checked_add(step)?;
        }
    }
    Some(out)
}

fn exact_integer_range_values_up_to(
    start: i64,
    end: i64,
    step: i64,
    limit: usize,
) -> Option<Vec<i64>> {
    if step == 0 || limit == 0 {
        return Some(Vec::new());
    }

    let mut current = start;
    let mut out = Vec::new();
    if step > 0 {
        while current < end && out.len() < limit {
            out.push(current);
            current = current.checked_add(step)?;
        }
    } else {
        while current > end && out.len() < limit {
            out.push(current);
            current = current.checked_add(step)?;
        }
    }
    Some(out)
}

fn sparse_array_from_indexes(indexes: &[i64]) -> Result<Value, AqError> {
    let full_len = sparse_array_full_len(indexes)?;
    let mut values = vec![Value::Null; full_len];
    for index in indexes {
        let resolved = validate_sparse_array_index(*index)?;
        values[resolved] = Value::Integer(*index);
    }
    Ok(Value::Array(values))
}

fn sparse_array_slice_from_indexes(
    indexes: &[i64],
    start: Option<isize>,
    end: Option<isize>,
) -> Result<Value, AqError> {
    let full_len = sparse_array_full_len(indexes)?;
    let (start, end) = resolve_slice_bounds(start, end, full_len);
    let mut values = vec![Value::Null; end.saturating_sub(start)];
    for index in indexes {
        let resolved = validate_sparse_array_index(*index)?;
        if (start..end).contains(&resolved) {
            values[resolved - start] = Value::Integer(*index);
        }
    }
    Ok(Value::Array(values))
}

fn sparse_array_full_len(indexes: &[i64]) -> Result<usize, AqError> {
    let mut max_index: Option<usize> = None;
    for index in indexes {
        let resolved = validate_sparse_array_index(*index)?;
        max_index = Some(max_index.map_or(resolved, |current: usize| current.max(resolved)));
    }
    Ok(max_index.map_or(0, |index| index.saturating_add(1)))
}

fn validate_sparse_array_index(index: i64) -> Result<usize, AqError> {
    if index < 0 {
        return Err(AqError::Query(
            "Out of bounds negative array index".to_string(),
        ));
    }
    let resolved = usize::try_from(index)
        .map_err(|_| AqError::Query("array index is out of range".to_string()))?;
    if resolved > MAX_AUTO_GROW_ARRAY_INDEX {
        return Err(AqError::Query("Array index too large".to_string()));
    }
    Ok(resolved)
}

fn query_is_numeric_literal(query: &Query, expected: f64) -> bool {
    match direct_stage_expr(query) {
        Some(expr) => {
            expr_numeric_constant(expr).is_some_and(|value| value.total_cmp(&expected).is_eq())
        }
        None => false,
    }
}

fn expr_is_identity_path(expr: &Expr) -> bool {
    matches!(expr, Expr::Path(PathExpr { segments }) if segments.is_empty())
}

fn expr_numeric_constant(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Literal(value) => numeric_constant_of(value),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => expr_numeric_constant(expr).map(|value| -value),
        _ => None,
    }
}

fn value_is_pow2_log2_round_safe(value: &Value) -> bool {
    let Some(value) = numeric_value_in_pow2_log2_round_safe_range(value) else {
        return false;
    };
    (POW2_LOG2_SAFE_MIN_EXP..=POW2_LOG2_SAFE_MAX_EXP).contains(&value)
}

fn numeric_value_in_pow2_log2_round_safe_range(value: &Value) -> Option<f64> {
    match value.untagged() {
        Value::Integer(value) => Some(*value as f64),
        Value::Decimal(value) => {
            let value = value.to_f64_lossy();
            value.is_finite().then_some(value)
        }
        Value::Float(value) => value.is_finite().then_some(*value),
        _ => None,
    }
}

fn query_wraps_input_in_singleton_array(query: &Query) -> bool {
    matches!(
        query.outputs.as_slice(),
        [Pipeline { stages }] if matches!(
            stages.as_slice(),
            [Expr::Array(items)] if matches!(
                items.as_slice(),
                [Expr::Subquery(inner)] if query_is_identity(inner)
            )
        )
    )
}

fn query_collects_string_slices(query: &Query) -> Option<Vec<(Option<isize>, Option<isize>)>> {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return None;
    }

    query
        .outputs
        .iter()
        .map(pipeline_string_slice)
        .collect::<Option<Vec<_>>>()
}

fn query_is_empty_array_literal(query: &Query) -> bool {
    matches!(direct_stage_expr(query), Some(Expr::Array(items)) if items.is_empty())
}

fn pipeline_string_slice(pipeline: &Pipeline) -> Option<(Option<isize>, Option<isize>)> {
    let [Expr::Path(PathExpr { segments })] = pipeline.stages.as_slice() else {
        return None;
    };
    let [Segment::Slice {
        start,
        end,
        optional: false,
    }] = segments.as_slice()
    else {
        return None;
    };
    Some((*start, *end))
}

fn direct_range_iteration_count(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<usize>, AqError> {
    let Some(Expr::Builtin(BuiltinExpr::Range(args))) = direct_stage_expr(query) else {
        return Ok(None);
    };

    let values = args
        .iter()
        .map(|arg| evaluate_query_numbers("range", arg, input, bindings, context))
        .collect::<Result<Vec<_>, _>>()?;

    let mut total = 0usize;
    match values.as_slice() {
        [ends] => {
            for end in ends {
                total = total.saturating_add(count_range_values(0.0, *end, 1.0));
            }
        }
        [starts, ends] => {
            for start in starts {
                for end in ends {
                    total = total.saturating_add(count_range_values(*start, *end, 1.0));
                }
            }
        }
        [starts, ends, steps] => {
            for start in starts {
                for end in ends {
                    for step in steps {
                        total = total.saturating_add(count_range_values(*start, *end, *step));
                    }
                }
            }
        }
        _ => return Err(AqError::Query("range expects 1 to 3 arguments".to_string())),
    }
    Ok(Some(total))
}

fn count_range_values(start: f64, end: f64, step: f64) -> usize {
    if step == 0.0 {
        return 0;
    }

    let mut current = start;
    let mut count = 0usize;
    if step > 0.0 {
        while current < end {
            count = count.saturating_add(1);
            current += step;
        }
    } else {
        while current > end {
            count = count.saturating_add(1);
            current += step;
        }
    }
    count
}

fn direct_stage_expr(query: &Query) -> Option<&Expr> {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return None;
    }

    let [pipeline] = query.outputs.as_slice() else {
        return None;
    };
    let [stage] = pipeline.stages.as_slice() else {
        return None;
    };
    if matches!(stage, Expr::BindingAlt { .. }) {
        return None;
    }
    Some(stage)
}

fn query_is_identity(query: &Query) -> bool {
    matches!(
        query.outputs.as_slice(),
        [Pipeline { stages }] if matches!(
            stages.as_slice(),
            [Expr::Path(PathExpr { segments })] if segments.is_empty()
        )
    )
}

fn literal_expr_value(expr: &Expr) -> Option<&Value> {
    match expr {
        Expr::Literal(value) => Some(value),
        _ => None,
    }
}

fn literal_string_expr_value(expr: &Expr) -> Option<&str> {
    let Value::String(value) = literal_expr_value(expr)?.untagged() else {
        return None;
    };
    Some(value)
}

struct ForeachSpec<'a> {
    source: &'a Query,
    pattern: &'a BindingPattern,
    init: &'a Query,
    update: &'a Query,
    extract: &'a Query,
}

fn evaluate_foreach(
    spec: &ForeachSpec<'_>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let items = evaluate_query(spec.source, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
    let iteration_bindings = items
        .iter()
        .map(|item| {
            let mut item_bindings = bindings.clone();
            bind_pattern(spec.pattern, item, &mut item_bindings, context)?;
            Ok(item_bindings)
        })
        .collect::<Result<Vec<_>, AqError>>()?;
    let initial_accumulators = evaluate_query(spec.init, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
    let extract_is_identity = query_is_identity(spec.extract);
    let update_direct_stage = direct_stage_expr(spec.update);
    let extract_direct_stage = (!extract_is_identity)
        .then(|| direct_stage_expr(spec.extract))
        .flatten();
    let update_uses_value_path = query_has_value_only_fast_path(spec.update);
    let extract_uses_value_path = query_has_value_only_fast_path(spec.extract);
    let mut outputs = Vec::new();

    for initial_accumulator in initial_accumulators {
        let mut accumulators = vec![initial_accumulator];
        for item_bindings in &iteration_bindings {
            let mut next = Vec::new();
            for accumulator in accumulators {
                if let Some(update_stage) = update_direct_stage {
                    if let Some(updated) =
                        direct_single_value_expr_value(update_stage, &accumulator, item_bindings)
                    {
                        if extract_is_identity {
                            outputs.push(updated.clone());
                            next.push(updated);
                            continue;
                        }

                        if let Some(extract_stage) = extract_direct_stage {
                            if let Some(extracted) = direct_single_value_expr_value(
                                extract_stage,
                                &updated,
                                item_bindings,
                            ) {
                                outputs.push(extracted);
                                next.push(updated);
                                continue;
                            }
                        }
                    }
                }

                let updated = if update_uses_value_path {
                    evaluate_query_values(spec.update, &accumulator, item_bindings, context)?
                } else {
                    evaluate_query_owned(spec.update, accumulator, item_bindings, context)?
                        .into_iter()
                        .map(|frame| frame.value)
                        .collect::<Vec<_>>()
                };
                if extract_is_identity {
                    outputs.extend(updated.iter().cloned());
                } else {
                    for updated_accumulator in &updated {
                        if extract_uses_value_path {
                            outputs.extend(evaluate_query_values(
                                spec.extract,
                                updated_accumulator,
                                item_bindings,
                                context,
                            )?);
                        } else {
                            outputs.extend(
                                evaluate_query(
                                    spec.extract,
                                    updated_accumulator,
                                    item_bindings,
                                    context,
                                )?
                                .into_iter()
                                .map(|frame| frame.value),
                            );
                        }
                    }
                }
                next.extend(updated);
            }
            accumulators = next;
        }
    }

    Ok(outputs)
}

fn evaluate_foreach_catching_label(
    spec: &ForeachSpec<'_>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<Value>>, AqError> {
    let items =
        match evaluate_query_catching_label(spec.source, input, bindings, context, catch_label)? {
            LabelFlow::Continue(frames) => frames_to_values(frames),
            LabelFlow::Break(_) => return Ok(LabelFlow::Break(Vec::new())),
        };
    let initial_accumulators =
        match evaluate_query_catching_label(spec.init, input, bindings, context, catch_label)? {
            LabelFlow::Continue(frames) => frames_to_values(frames),
            LabelFlow::Break(_) => return Ok(LabelFlow::Break(Vec::new())),
        };
    let iteration_bindings = items
        .iter()
        .map(|item| {
            let mut item_bindings = bindings.clone();
            bind_pattern(spec.pattern, item, &mut item_bindings, context)?;
            Ok(item_bindings)
        })
        .collect::<Result<Vec<_>, AqError>>()?;
    let extract_is_identity = query_is_identity(spec.extract);
    let mut outputs = Vec::new();

    for initial_accumulator in initial_accumulators {
        let mut accumulators = vec![initial_accumulator];
        for item_bindings in &iteration_bindings {
            let mut next = Vec::new();
            for accumulator in accumulators {
                let updated = match evaluate_query_catching_label(
                    spec.update,
                    &accumulator,
                    item_bindings,
                    context,
                    catch_label,
                )? {
                    LabelFlow::Continue(frames) => frames_to_values(frames),
                    LabelFlow::Break(_) => return Ok(LabelFlow::Break(outputs)),
                };
                if extract_is_identity {
                    outputs.extend(updated.iter().cloned());
                } else {
                    for updated_accumulator in &updated {
                        match evaluate_query_catching_label(
                            spec.extract,
                            updated_accumulator,
                            item_bindings,
                            context,
                            catch_label,
                        )? {
                            LabelFlow::Continue(frames) => outputs.extend(frames_to_values(frames)),
                            LabelFlow::Break(frames) => {
                                outputs.extend(frames_to_values(frames));
                                return Ok(LabelFlow::Break(outputs));
                            }
                        }
                    }
                }
                next.extend(updated);
            }
            accumulators = next;
        }
    }

    Ok(LabelFlow::Continue(outputs))
}

fn limit_of(
    count: &Query,
    expr: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let limits = evaluate_limit_counts(count, input, bindings, context)?;
    if limits.is_empty() || query_is_plain_empty(expr) {
        return Ok(Vec::new());
    }
    if limits.len() > 1
        && matches!(
            direct_stage_expr(expr),
            Some(Expr::Builtin(BuiltinExpr::Range(_)))
        )
    {
        let Some(max_limit) = limits.iter().copied().max() else {
            return Ok(Vec::new());
        };
        if max_limit == 0 {
            return Ok(Vec::new());
        }
        let values = evaluate_query_up_to(expr, input, bindings, context, max_limit)?
            .into_iter()
            .map(|frame| frame.value)
            .collect::<Vec<_>>();
        let mut out = Vec::new();
        for limit in limits {
            out.extend(values.iter().take(limit).cloned());
        }
        return Ok(out);
    }
    let mut out = Vec::new();
    for limit in limits {
        if limit == 0 {
            continue;
        }
        out.extend(
            evaluate_query_up_to(expr, input, bindings, context, limit)?
                .into_iter()
                .map(|frame| frame.value),
        );
    }
    Ok(out)
}

fn evaluate_nonnegative_count_arg(
    name: &str,
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<usize, AqError> {
    let value = evaluate_single_argument_value(name, expr, input, bindings, context)?;
    nonnegative_integer_count_value(name, value)
}

fn nonnegative_integer_count_value(name: &str, value: Value) -> Result<usize, AqError> {
    match value {
        Value::Integer(value) => {
            if value < 0 {
                Err(AqError::Query(if name == "skip" {
                    "skip doesn't support negative count".to_string()
                } else {
                    format!("{name} count must be a non-negative integer")
                }))
            } else {
                match usize::try_from(value) {
                    Ok(value) => Ok(value),
                    Err(_) => Ok(usize::MAX),
                }
            }
        }
        Value::Float(value) => {
            if !value.is_finite() || value.fract() != 0.0 || value < 0.0 {
                Err(AqError::Query(
                    if name == "skip" && value.is_finite() && value < 0.0 {
                        "skip doesn't support negative count".to_string()
                    } else {
                        format!("{name} count must be a non-negative integer")
                    },
                ))
            } else if value > usize::MAX as f64 {
                Ok(usize::MAX)
            } else {
                Ok(value as usize)
            }
        }
        _ => Err(AqError::Query(format!(
            "{name} count must be a non-negative integer"
        ))),
    }
}

fn evaluate_nonnegative_count_query(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<usize>, AqError> {
    evaluate_query(query, input, bindings, context)?
        .into_iter()
        .map(|frame| nonnegative_integer_count_value(name, frame.value))
        .collect()
}

fn literal_nonnegative_count_query_value(
    name: &str,
    query: &Query,
) -> Option<Result<usize, AqError>> {
    let Expr::Literal(value) = direct_stage_expr(query)? else {
        return None;
    };
    Some(nonnegative_integer_count_value(name, value.clone()))
}

fn limit_count_value(value: Value) -> Result<usize, AqError> {
    match value {
        Value::Integer(value) => {
            if value < 0 {
                Err(AqError::Query(
                    "limit doesn't support negative count".to_string(),
                ))
            } else {
                usize::try_from(value)
                    .map_err(|_| AqError::Query("limit count is out of range".to_string()))
            }
        }
        Value::Float(value) => {
            if !value.is_finite() {
                Err(AqError::Query(
                    "limit doesn't support non-finite count".to_string(),
                ))
            } else if value < 0.0 {
                Err(AqError::Query(
                    "limit doesn't support negative count".to_string(),
                ))
            } else if value >= usize::MAX as f64 {
                Ok(usize::MAX)
            } else {
                Ok(value.ceil() as usize)
            }
        }
        Value::Decimal(value) => {
            if let Some(value) = value.as_i64_exact() {
                if value < 0 {
                    Err(AqError::Query(
                        "limit doesn't support negative count".to_string(),
                    ))
                } else {
                    usize::try_from(value)
                        .map_err(|_| AqError::Query("limit count is out of range".to_string()))
                }
            } else {
                let value = value.to_f64_lossy();
                if !value.is_finite() {
                    Err(AqError::Query(
                        "limit doesn't support non-finite count".to_string(),
                    ))
                } else if value < 0.0 {
                    Err(AqError::Query(
                        "limit doesn't support negative count".to_string(),
                    ))
                } else if value >= usize::MAX as f64 {
                    Ok(usize::MAX)
                } else {
                    Ok(value.ceil() as usize)
                }
            }
        }
        other => Err(AqError::Query(format!(
            "limit requires a numeric argument, got {}",
            kind_name(&other)
        ))),
    }
}

fn evaluate_limit_counts(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<usize>, AqError> {
    evaluate_query(query, input, bindings, context)?
        .into_iter()
        .map(|frame| limit_count_value(frame.value))
        .collect()
}

fn evaluate_positive_count_arg(
    name: &str,
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    label: &str,
    context: &EvaluationContext,
) -> Result<usize, AqError> {
    let count = evaluate_nonnegative_count_arg(name, expr, input, bindings, context)?;
    if count == 0 {
        Err(AqError::Query(format!(
            "{name} {label} must be a positive integer"
        )))
    } else {
        Ok(count)
    }
}

fn take_of(
    input: &Value,
    count: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let limit = evaluate_nonnegative_count_arg("take", count, input, bindings, context)?;
    let values = expect_array_input("take", input)?;
    Ok(Value::Array(values.iter().take(limit).cloned().collect()))
}

fn skip_of(
    input: &Value,
    count: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let skip = evaluate_nonnegative_count_arg("skip", count, input, bindings, context)?;
    let values = expect_array_input("skip", input)?;
    Ok(Value::Array(values.iter().skip(skip).cloned().collect()))
}

fn skip_query_of(
    count: &Query,
    expr: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(skip) = literal_nonnegative_count_query_value("skip", count) {
        let skip = skip?;
        if let Some(Expr::Path(path)) = direct_stage_expr(expr) {
            if let Some(values) = evaluate_direct_static_path(path, input) {
                return Ok(values?.into_iter().skip(skip).collect());
            }
        }
    }

    let skips = evaluate_nonnegative_count_query("skip", count, input, bindings, context)?;
    let mut out = Vec::new();
    for skip in skips {
        if let Some(Expr::Path(path)) = direct_stage_expr(expr) {
            if let Some(values) = evaluate_direct_static_path(path, input) {
                out.extend(values?.into_iter().skip(skip));
                continue;
            }
        }
        out.extend(
            evaluate_query(expr, input, bindings, context)?
                .into_iter()
                .map(|frame| frame.value)
                .skip(skip),
        );
    }
    Ok(out)
}

fn first_query_of(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if query_is_plain_empty(query) {
        return Ok(Vec::new());
    }
    Ok(evaluate_query_up_to(query, input, bindings, context, 1)?
        .into_iter()
        .map(|frame| frame.value)
        .collect())
}

fn last_query_of(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if query_is_plain_empty(query) {
        return Ok(Vec::new());
    }
    Ok(evaluate_query_last(query, input, bindings, context)?
        .map(|frame| vec![frame.value])
        .unwrap_or_default())
}

fn nth_of(
    indexes: &Query,
    expr: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let indexes = evaluate_query(indexes, input, bindings, context)?
        .into_iter()
        .map(|frame| nth_index_of(frame.value))
        .collect::<Result<Vec<_>, _>>()?;
    let Some(max_index) = indexes.iter().copied().max() else {
        return Ok(Vec::new());
    };
    if query_is_plain_empty(expr) {
        return Ok(Vec::new());
    }
    if query_is_identity(expr) {
        if let Value::Array(values) = input.untagged() {
            let mut out = Vec::new();
            for index in indexes {
                if let Some(value) = values.get(index) {
                    out.push(value.clone());
                }
            }
            return Ok(out);
        }
    }
    let values = evaluate_query_up_to(expr, input, bindings, context, max_index.saturating_add(1))?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    for index in indexes {
        if let Some(value) = values.get(index) {
            out.push(value.clone());
        }
    }
    Ok(out)
}

fn recurse_of(
    query: Option<&Query>,
    condition: Option<&Query>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    let mut pending = vec![input.clone()];

    while let Some(current) = pending.pop() {
        out.push(current.clone());
        let mut next = match query {
            Some(query) => evaluate_query_owned(query, current, bindings, context)?
                .into_iter()
                .map(|frame| frame.value)
                .collect::<Vec<_>>(),
            None => recurse_children_of(&current),
        };
        if let Some(condition) = condition {
            let mut filtered = Vec::with_capacity(next.len());
            for value in next {
                if query_is_truthy(condition, &value, bindings, context)? {
                    filtered.push(value);
                }
            }
            next = filtered;
        }
        for value in next.into_iter().rev() {
            pending.push(value);
        }
    }

    Ok(out)
}

fn repeat_of(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    loop {
        out.extend(
            evaluate_query(query, input, bindings, context)?
                .into_iter()
                .map(|frame| frame.value),
        );
    }
}

fn recurse_children_of(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values.clone(),
        Value::Object(values) => values.values().cloned().collect(),
        _ => Vec::new(),
    }
}

fn walk_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let walked = match input {
        Value::Array(values) => {
            let mut walked = Vec::new();
            for value in values {
                walked.extend(walk_of(value, expr, bindings, context)?);
            }
            Value::Array(walked)
        }
        Value::Object(values) => {
            let mut walked = IndexMap::new();
            for (key, value) in values {
                if let Some(first) = walk_of(value, expr, bindings, context)?.into_iter().next() {
                    walked.insert(key.clone(), first);
                }
            }
            Value::Object(walked)
        }
        _ => input.clone(),
    };

    evaluate_expr(expr, &walked, bindings, context)
}

fn while_of(
    condition: &Query,
    update: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    let mut pending = vec![input.clone()];

    while let Some(current) = pending.pop() {
        if !query_is_truthy(condition, &current, bindings, context)? {
            continue;
        }

        out.push(current.clone());
        let next = evaluate_query_owned(update, current, bindings, context)?
            .into_iter()
            .map(|frame| frame.value)
            .collect::<Vec<_>>();
        for value in next.into_iter().rev() {
            pending.push(value);
        }
    }

    Ok(out)
}

fn until_of(
    condition: &Query,
    next: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    let mut pending = vec![input.clone()];

    while let Some(current) = pending.pop() {
        if query_is_truthy(condition, &current, bindings, context)? {
            out.push(current);
            continue;
        }

        let next_values = evaluate_query_owned(next, current, bindings, context)?
            .into_iter()
            .map(|frame| frame.value)
            .collect::<Vec<_>>();
        for value in next_values.into_iter().rev() {
            pending.push(value);
        }
    }

    Ok(out)
}

fn range_of(
    args: &[Query],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    range_of_up_to(args, input, bindings, context, usize::MAX)
}

fn range_of_up_to(
    args: &[Query],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    limit: usize,
) -> Result<Vec<Value>, AqError> {
    if let Some(values) = direct_exact_integer_range_values_up_to(args, input, bindings, limit) {
        return values;
    }

    let values = args
        .iter()
        .map(|arg| evaluate_query_numbers("range", arg, input, bindings, context))
        .collect::<Result<Vec<_>, _>>()?;

    let mut out = Vec::new();
    match values.as_slice() {
        [ends] => {
            for end in ends {
                extend_range_values_up_to(0.0, *end, 1.0, &mut out, limit);
                if out.len() >= limit {
                    break;
                }
            }
        }
        [starts, ends] => {
            'outer_two: for start in starts {
                for end in ends {
                    extend_range_values_up_to(*start, *end, 1.0, &mut out, limit);
                    if out.len() >= limit {
                        break 'outer_two;
                    }
                }
            }
        }
        [starts, ends, steps] => {
            'outer_three: for start in starts {
                for end in ends {
                    for step in steps {
                        extend_range_values_up_to(*start, *end, *step, &mut out, limit);
                        if out.len() >= limit {
                            break 'outer_three;
                        }
                    }
                }
            }
        }
        _ => return Err(AqError::Query("range expects 1 to 3 arguments".to_string())),
    }
    Ok(out)
}

fn direct_exact_integer_range_values_up_to(
    args: &[Query],
    input: &Value,
    bindings: &Bindings,
    limit: usize,
) -> Option<Result<Vec<Value>, AqError>> {
    let values = args
        .iter()
        .map(|arg| direct_single_value_query_value(arg, input, bindings))
        .collect::<Option<Vec<_>>>()?;
    let values = values
        .iter()
        .map(exact_integer_value)
        .collect::<Option<Vec<_>>>()?;

    let integers = match values.as_slice() {
        [end] => exact_integer_range_values_up_to(0, *end, 1, limit)?,
        [start, end] => exact_integer_range_values_up_to(*start, *end, 1, limit)?,
        [start, end, step] => exact_integer_range_values_up_to(*start, *end, *step, limit)?,
        _ => {
            return Some(Err(AqError::Query(
                "range expects 1 to 3 arguments".to_string(),
            )))
        }
    };
    Some(Ok(integers.into_iter().map(Value::Integer).collect()))
}

#[derive(Clone, Copy)]
struct AffineExpr {
    scale: f64,
    offset: f64,
}

impl AffineExpr {
    fn identity() -> Self {
        Self {
            scale: 1.0,
            offset: 0.0,
        }
    }

    fn apply(self, input: f64) -> f64 {
        self.scale * input + self.offset
    }
}

#[derive(Clone, Copy)]
enum LastRangeProjection {
    Number(AffineExpr),
    Rfc3339Roundtrip(AffineExpr),
}

fn try_evaluate_last_range_pipeline(
    pipeline: &Pipeline,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<Option<EvalFrame>>, AqError> {
    let Some((Expr::Builtin(BuiltinExpr::Range(args)), suffix)) = pipeline.stages.split_first()
    else {
        return Ok(None);
    };
    let Some((first, last)) = range_stream_endpoints(args, input, bindings, context)? else {
        return Ok(Some(None));
    };
    let Some(projection) = analyze_last_range_projection(suffix, input, bindings, context)? else {
        return Ok(None);
    };
    if !projection_is_safe_for_range(projection, first, last) {
        return Ok(None);
    }

    let value = project_last_range_value(projection, last)?;
    Ok(Some(Some(EvalFrame {
        value,
        bindings: bindings.clone(),
    })))
}

fn analyze_last_range_projection(
    suffix: &[Expr],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<LastRangeProjection>, AqError> {
    if suffix.is_empty() {
        return Ok(Some(LastRangeProjection::Number(AffineExpr::identity())));
    }

    let (affine, remainder) = match suffix.split_first() {
        Some((expr, rest)) => match affine_expr_of(expr, input, bindings, context)? {
            Some(affine) => (affine, rest),
            None => (AffineExpr::identity(), suffix),
        },
        None => (AffineExpr::identity(), suffix),
    };

    if remainder.is_empty() {
        return Ok(Some(LastRangeProjection::Number(affine)));
    }

    if matches!(
        remainder,
        [
            Expr::Builtin(BuiltinExpr::StrFTime(format)),
            Expr::Builtin(BuiltinExpr::StrPTime(parse_format))
        ] if literal_time_format_argument(format.as_ref()) == Some(RFC3339_UTC_SECONDS_FORMAT)
            && literal_time_format_argument(parse_format.as_ref()) == Some(RFC3339_UTC_SECONDS_FORMAT)
    ) {
        return Ok(Some(LastRangeProjection::Rfc3339Roundtrip(affine)));
    }

    Ok(None)
}

fn projection_is_safe_for_range(projection: LastRangeProjection, first: f64, last: f64) -> bool {
    let first = match projection_endpoint_value(projection, first) {
        Some(value) => value,
        None => return false,
    };
    let last = match projection_endpoint_value(projection, last) {
        Some(value) => value,
        None => return false,
    };

    match projection {
        LastRangeProjection::Number(_) => first.is_finite() && last.is_finite(),
        LastRangeProjection::Rfc3339Roundtrip(_) => {
            epoch_seconds_to_datetime(first, "strftime/1").is_ok()
                && epoch_seconds_to_datetime(last, "strftime/1").is_ok()
        }
    }
}

fn projection_endpoint_value(projection: LastRangeProjection, input: f64) -> Option<f64> {
    let value = match projection {
        LastRangeProjection::Number(affine) | LastRangeProjection::Rfc3339Roundtrip(affine) => {
            affine.apply(input)
        }
    };
    value.is_finite().then_some(value)
}

fn project_last_range_value(projection: LastRangeProjection, input: f64) -> Result<Value, AqError> {
    match projection {
        LastRangeProjection::Number(affine) => Ok(number_value(affine.apply(input))),
        LastRangeProjection::Rfc3339Roundtrip(affine) => {
            rfc3339_roundtrip_of(&number_value(affine.apply(input)))
        }
    }
}

fn range_stream_endpoints(
    args: &[Query],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<(f64, f64)>, AqError> {
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        let outputs = evaluate_query_numbers("range", arg, input, bindings, context)?;
        let [value] = outputs.as_slice() else {
            return Ok(None);
        };
        values.push(*value);
    }

    let endpoints = match values.as_slice() {
        [end] => last_range_endpoints(0.0, *end, 1.0),
        [start, end] => last_range_endpoints(*start, *end, 1.0),
        [start, end, step] => last_range_endpoints(*start, *end, *step),
        _ => return Ok(None),
    };
    Ok(endpoints)
}

fn last_range_endpoints(start: f64, end: f64, step: f64) -> Option<(f64, f64)> {
    if step == 0.0 {
        return None;
    }

    let mut current = start;
    let mut first = None;
    let mut last = None;
    if step > 0.0 {
        while current < end {
            first.get_or_insert(current);
            last = Some(current);
            current += step;
        }
    } else {
        while current > end {
            first.get_or_insert(current);
            last = Some(current);
            current += step;
        }
    }

    first.zip(last)
}

fn affine_expr_of(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<AffineExpr>, AqError> {
    Ok(match expr {
        Expr::Path(path) if path.segments.is_empty() => Some(AffineExpr::identity()),
        Expr::Literal(value) => {
            numeric_constant_of(value).map(|offset| AffineExpr { scale: 0.0, offset })
        }
        Expr::Subquery(query) => affine_query_of(query, input, bindings, context)?,
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => affine_expr_of(expr, input, bindings, context)?.map(|inner| AffineExpr {
            scale: -inner.scale,
            offset: -inner.offset,
        }),
        Expr::Binary {
            left,
            op: BinaryOp::Add,
            right,
        } => combine_affine_add(
            affine_expr_of(left, input, bindings, context)?,
            affine_expr_of(right, input, bindings, context)?,
        ),
        Expr::Binary {
            left,
            op: BinaryOp::Sub,
            right,
        } => combine_affine_sub(
            affine_expr_of(left, input, bindings, context)?,
            affine_expr_of(right, input, bindings, context)?,
        ),
        Expr::Binary {
            left,
            op: BinaryOp::Mul,
            right,
        } => combine_affine_mul(
            affine_expr_of(left, input, bindings, context)?,
            affine_expr_of(right, input, bindings, context)?,
        ),
        _ => None,
    })
}

fn affine_query_of(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<AffineExpr>, AqError> {
    if query_is_simple_constant(query) {
        return Ok(
            constant_numeric_query_value(query, input, bindings, context)?
                .map(|offset| AffineExpr { scale: 0.0, offset }),
        );
    }
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return Ok(None);
    }
    let [pipeline] = query.outputs.as_slice() else {
        return Ok(None);
    };
    let [stage] = pipeline.stages.as_slice() else {
        return Ok(None);
    };
    affine_expr_of(stage, input, bindings, context)
}

fn combine_affine_add(left: Option<AffineExpr>, right: Option<AffineExpr>) -> Option<AffineExpr> {
    let left = left?;
    let right = right?;
    Some(AffineExpr {
        scale: left.scale + right.scale,
        offset: left.offset + right.offset,
    })
}

fn combine_affine_sub(left: Option<AffineExpr>, right: Option<AffineExpr>) -> Option<AffineExpr> {
    let left = left?;
    let right = right?;
    Some(AffineExpr {
        scale: left.scale - right.scale,
        offset: left.offset - right.offset,
    })
}

fn combine_affine_mul(left: Option<AffineExpr>, right: Option<AffineExpr>) -> Option<AffineExpr> {
    let left = left?;
    let right = right?;
    if left.scale == 0.0 {
        return Some(AffineExpr {
            scale: left.offset * right.scale,
            offset: left.offset * right.offset,
        });
    }
    if right.scale == 0.0 {
        return Some(AffineExpr {
            scale: right.offset * left.scale,
            offset: right.offset * left.offset,
        });
    }
    None
}

fn constant_numeric_query_value(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<f64>, AqError> {
    let outputs = match evaluate_query_numbers("last", query, input, bindings, context) {
        Ok(outputs) => outputs,
        Err(_) => return Ok(None),
    };
    let [value] = outputs.as_slice() else {
        return Ok(None);
    };
    Ok(Some(*value))
}

fn numeric_constant_of(value: &Value) -> Option<f64> {
    match value.untagged() {
        Value::Integer(value) => Some(*value as f64),
        Value::Decimal(value) => {
            let value = value.to_f64_lossy();
            value.is_finite().then_some(value)
        }
        Value::Float(value) => value.is_finite().then_some(*value),
        _ => None,
    }
}

fn extend_range_values_up_to(start: f64, end: f64, step: f64, out: &mut Vec<Value>, limit: usize) {
    if step == 0.0 {
        return;
    }

    let mut current = start;
    if step > 0.0 {
        while current < end && out.len() < limit {
            out.push(number_value(current));
            current += step;
        }
    } else {
        while current > end && out.len() < limit {
            out.push(number_value(current));
            current += step;
        }
    }
}

fn combinations_of(
    input: &Value,
    count_expr: Option<&Expr>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let iterables = match count_expr {
        Some(expr) => {
            let count = evaluate_combinations_count(expr, input, bindings, context)?;
            if count <= 0 {
                return Ok(vec![Value::Array(Vec::new())]);
            }
            let values = iterate_values_for_combinations(input)?;
            vec![values; usize::try_from(count).unwrap_or(usize::MAX)]
        }
        None => expect_array_input("combinations", input)?
            .iter()
            .map(iterate_values_for_combinations)
            .collect::<Result<Vec<_>, _>>()?,
    };
    combinations_product(&iterables)
}

fn evaluate_combinations_count(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<i64, AqError> {
    let value = evaluate_single_argument_value("combinations", expr, input, bindings, context)?;
    let count = match value {
        Value::Integer(value) => value,
        Value::Decimal(value) => {
            let ceiled = value.to_f64_lossy().ceil();
            if ceiled >= i64::MAX as f64 {
                i64::MAX
            } else if ceiled <= i64::MIN as f64 {
                i64::MIN
            } else {
                ceiled as i64
            }
        }
        Value::Float(value) if value.is_finite() => {
            let ceiled = value.ceil();
            if ceiled >= i64::MAX as f64 {
                i64::MAX
            } else if ceiled <= i64::MIN as f64 {
                i64::MIN
            } else {
                ceiled as i64
            }
        }
        _ => {
            return Err(AqError::Query(
                "combinations count must be numeric".to_string(),
            ))
        }
    };
    Ok(count)
}

fn iterate_values_for_combinations(value: &Value) -> Result<Vec<Value>, AqError> {
    match value {
        Value::Array(values) => Ok(values.clone()),
        Value::Object(values) => Ok(values.values().cloned().collect()),
        other => Err(iterate_error(other)),
    }
}

fn combinations_product(iterables: &[Vec<Value>]) -> Result<Vec<Value>, AqError> {
    if iterables.is_empty() {
        return Ok(vec![Value::Array(Vec::new())]);
    }

    let mut out = Vec::new();
    let mut current = Vec::with_capacity(iterables.len());
    combinations_product_inner(iterables, 0, &mut current, &mut out);
    Ok(out)
}

fn combinations_product_inner(
    iterables: &[Vec<Value>],
    index: usize,
    current: &mut Vec<Value>,
    out: &mut Vec<Value>,
) {
    if index == iterables.len() {
        out.push(Value::Array(current.clone()));
        return;
    }

    for value in &iterables[index] {
        current.push(value.clone());
        combinations_product_inner(iterables, index + 1, current, out);
        current.pop();
    }
}

fn bsearch_of(
    input: &Value,
    target: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let values = match input.untagged() {
        Value::Array(values) => values,
        other => {
            let (value_type, rendered) = typed_rendered_value(other);
            return Err(AqError::Query(format!(
                "{value_type} ({rendered}) cannot be searched from"
            )));
        }
    };
    if let Some(needle) = literal_expr_value(target) {
        return Ok(vec![bsearch_needle(values, needle)]);
    }
    let needles = evaluate_expr(target, input, bindings, context)?;
    let mut out = Vec::with_capacity(needles.len());
    for needle in needles {
        out.push(bsearch_needle(values, &needle));
    }
    Ok(out)
}

fn bsearch_needle(values: &[Value], needle: &Value) -> Value {
    let mut low = 0usize;
    let mut high = values.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let ordering = match (values[mid].untagged(), needle.untagged()) {
            (Value::Integer(left), Value::Integer(right)) => left.cmp(right),
            _ => compare_sort_values(&values[mid], needle),
        };
        match ordering {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => {
                return Value::Integer(i64::try_from(mid).unwrap_or(i64::MAX))
            }
        }
    }

    let insertion = i64::try_from(low).unwrap_or(i64::MAX);
    Value::Integer(-insertion - 1)
}

fn evaluate_single_query_number(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<f64, AqError> {
    let value = evaluate_single_query_value(name, query, input, bindings, context)?;
    match value {
        Value::Integer(value) => Ok(value as f64),
        Value::Decimal(value) => Ok(value.to_f64_lossy()),
        Value::Float(value) => Ok(value),
        other => Err(AqError::Query(format!(
            "{name} requires a numeric argument, got {}",
            kind_name(&other)
        ))),
    }
}

fn evaluate_query_numbers(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<f64>, AqError> {
    evaluate_query(query, input, bindings, context)?
        .into_iter()
        .map(|frame| match frame.value {
            Value::Integer(value) => Ok(value as f64),
            Value::Decimal(value) => Ok(value.to_f64_lossy()),
            Value::Float(value) => Ok(value),
            other => Err(AqError::Query(format!(
                "{name} requires a numeric argument, got {}",
                kind_name(&other)
            ))),
        })
        .collect()
}

fn evaluate_single_query_string(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    let value = evaluate_single_query_value(name, query, input, bindings, context)?;
    match value {
        Value::String(value) => Ok(value),
        other => Err(AqError::Query(format!(
            "{name} requires a string argument, got {}",
            kind_name(&other)
        ))),
    }
}

fn evaluate_single_query_string_cow<'a>(
    name: &str,
    query: &'a Query,
    input: &'a Value,
    bindings: &'a Bindings,
    context: &EvaluationContext,
) -> Result<Cow<'a, str>, AqError> {
    if let Some(value) = borrowed_single_query_string(query, input, bindings) {
        return Ok(Cow::Borrowed(value));
    }
    evaluate_single_query_string(name, query, input, bindings, context).map(Cow::Owned)
}

fn borrowed_single_query_value<'a>(
    query: &'a Query,
    input: &'a Value,
    bindings: &'a Bindings,
) -> Option<&'a Value> {
    if !query.functions.is_empty()
        || !query.imported_values.is_empty()
        || query.module_info.is_some()
    {
        return None;
    }
    let [pipeline] = query.outputs.as_slice() else {
        return None;
    };
    let [expr] = pipeline.stages.as_slice() else {
        return None;
    };
    borrowed_value_argument(expr, input, bindings)
}

fn evaluate_single_query_value(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let frames = evaluate_query_up_to(query, input, bindings, context, 2)?;
    let Some(frame) = frames.first() else {
        return Err(AqError::Query(format!("{name} requires exactly one value")));
    };
    if frames.len() != 1 {
        return Err(AqError::Query(format!("{name} requires exactly one value")));
    }
    Ok(frame.value.clone())
}

fn query_is_truthy(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    Ok(evaluate_query(query, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .any(|value| is_truthy(&value)))
}

fn borrowed_single_query_string<'a>(
    query: &'a Query,
    input: &'a Value,
    bindings: &'a Bindings,
) -> Option<&'a str> {
    match borrowed_single_query_value(query, input, bindings)?.untagged() {
        Value::String(value) => Some(value),
        _ => None,
    }
}

fn compile_regex_with_flags(
    name: &str,
    pattern: &str,
    flags: &str,
) -> Result<(Regex, RegexBehavior), AqError> {
    let mut builder = RegexBuilder::new(pattern);
    let mut behavior = RegexBehavior {
        global: false,
        no_empty: false,
    };
    for flag in flags.chars() {
        match flag {
            'g' => behavior.global = true,
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            'n' => {
                behavior.no_empty = true;
            }
            other => {
                return Err(AqError::Query(format!(
                    "{name} does not support regex flag `{other}`"
                )));
            }
        }
    }
    let regex = builder
        .build()
        .map_err(|error| AqError::Query(format!("{name} failed to compile regex: {error}")))?;
    Ok((regex, behavior))
}

fn compile_regex_with_flags_cached(
    name: &str,
    pattern: &str,
    flags: &str,
    context: &EvaluationContext,
) -> Result<(Regex, RegexBehavior), AqError> {
    if let Some(compiled) = context.compiled_regex(pattern, flags) {
        return Ok(compiled);
    }
    let compiled = compile_regex_with_flags(name, pattern, flags)?;
    context.cache_compiled_regex(pattern.to_string(), flags.to_string(), compiled.clone());
    Ok(compiled)
}

fn path_components_of(name: &str, value: &Value) -> Result<Vec<PathComponent>, AqError> {
    let Value::Array(components) = value.untagged() else {
        return Err(AqError::Query(format!("{name} expects an array path")));
    };

    components
        .iter()
        .map(|component| path_component_of(name, component))
        .collect()
}

fn path_component_of(name: &str, value: &Value) -> Result<PathComponent, AqError> {
    match value.untagged() {
        Value::String(value) => Ok(PathComponent::Field(value.clone())),
        Value::Integer(value) => Ok(PathComponent::Index(
            isize::try_from(*value)
                .map_err(|_| AqError::Query(format!("{name} path index is out of range")))?,
        )),
        Value::Decimal(value) => {
            let truncated = value.to_f64_lossy().trunc();
            if truncated < isize::MIN as f64 || truncated > isize::MAX as f64 {
                return Err(AqError::Query(format!("{name} path index is out of range")));
            }
            Ok(PathComponent::Index(truncated as isize))
        }
        other => Err(AqError::Query(format!(
            "{name} path components must be strings or integers, got {}",
            kind_name(other)
        ))),
    }
}

fn paths_of(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Vec<PathComponent>>, AqError> {
    let value = evaluate_single_query_value(name, query, input, bindings, context)?;
    let Value::Array(paths) = value.untagged() else {
        return Err(AqError::Query(if name == "delpaths" {
            "Paths must be specified as an array".to_string()
        } else {
            format!("{name} expects an array of paths")
        }));
    };

    paths
        .iter()
        .map(|path| path_components_of(name, path))
        .collect()
}

fn path_of_builtin(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(paths) =
        try_exact_paths_without_value_clones("path", query, input, bindings, context)?
    {
        return Ok(paths.iter().map(|path| path_to_value(path)).collect());
    }
    Ok(evaluate_path_query_frames(query, input, bindings, context)?
        .into_iter()
        .map(|frame| Value::Array(frame.path))
        .collect())
}

fn exact_paths_of(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Vec<PathComponent>>, AqError> {
    if let Some(paths) =
        try_exact_paths_without_value_clones(name, query, input, bindings, context)?
    {
        return Ok(paths);
    }

    let mut paths = Vec::new();
    for frame in evaluate_path_query_frames(query, input, bindings, context)? {
        if name == "del" && contains_non_finite_path_component(&frame.path) {
            continue;
        }
        paths.push(exact_path_components_of(name, &frame.path)?);
    }
    Ok(paths)
}

fn try_exact_paths_without_value_clones(
    name: &str,
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<Vec<Vec<PathComponent>>>, AqError> {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return Ok(None);
    }
    let [pipeline] = query.outputs.as_slice() else {
        return Ok(None);
    };
    let [stage] = pipeline.stages.as_slice() else {
        return Ok(None);
    };
    let Expr::Path(path) = stage else {
        return Ok(None);
    };

    Ok(Some(
        evaluate_exact_path_segments(name, &path.segments, input, bindings, context)?
            .into_iter()
            .map(|frame| frame.path)
            .collect(),
    ))
}

fn evaluate_exact_path_segments<'a>(
    name: &str,
    segments: &[Segment],
    input: &'a Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<ExactPathFrameRef<'a>>, AqError> {
    let mut current = vec![ExactPathFrameRef {
        value: ExactPathValueRef::from_value(input),
        path: Vec::new(),
    }];
    for segment in segments {
        current = apply_exact_path_segment(name, segment, current, input, bindings, context)?;
    }
    Ok(current)
}

fn apply_exact_path_segment<'a>(
    name: &str,
    segment: &Segment,
    frames: Vec<ExactPathFrameRef<'a>>,
    scope_input: &'a Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<ExactPathFrameRef<'a>>, AqError> {
    let mut out = Vec::new();
    let constant_lookup = match segment {
        Segment::Lookup { expr, .. } => constant_lookup_expr_value(expr, bindings, context)?,
        _ => None,
    };
    for frame in frames {
        match segment {
            Segment::Field {
                name: field,
                optional,
            } => apply_exact_path_field(frame, field, *optional, &mut out)?,
            Segment::Lookup { expr, optional } => {
                if let Some(lookup) = constant_lookup.as_ref() {
                    apply_exact_path_lookup(name, frame, lookup.clone(), *optional, &mut out)?;
                } else {
                    let lookups = evaluate_expr(expr, scope_input, bindings, context)?;
                    for lookup in lookups {
                        apply_exact_path_lookup(name, frame.clone(), lookup, *optional, &mut out)?;
                    }
                }
            }
            Segment::Index { index, optional } => {
                apply_exact_path_index(frame, *index, *optional, &mut out)?
            }
            Segment::Slice {
                start,
                end,
                optional,
            } => apply_exact_path_slice(frame, *start, *end, *optional, &mut out)?,
            Segment::Iterate { optional } => apply_exact_path_iterate(frame, *optional, &mut out)?,
        }
    }
    Ok(out)
}

fn constant_lookup_expr_value(
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<Value>, AqError> {
    match expr {
        Expr::Literal(value) => Ok(Some(value.clone())),
        Expr::Builtin(BuiltinExpr::Nan) => Ok(Some(Value::Float(f64::NAN))),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => Ok(literal_slice_bound_expr_value(expr).and_then(|value| value_neg(value).ok())),
        Expr::Object(entries) => {
            let mut fields = IndexMap::with_capacity(entries.len());
            for (key, value_expr) in entries {
                let ObjectKey::Static(name) = key else {
                    return Ok(None);
                };
                let Some(value) = constant_lookup_expr_value(value_expr, bindings, context)? else {
                    return Ok(None);
                };
                fields.insert(name.clone(), value);
            }
            Ok(Some(Value::Object(fields)))
        }
        Expr::Subquery(query) if query_is_simple_constant(query) => {
            let frames = evaluate_query_up_to(query, &Value::Null, bindings, context, 2)?;
            Ok(match frames.as_slice() {
                [frame] => Some(frame.value.clone()),
                _ => None,
            })
        }
        _ => Ok(None),
    }
}

fn apply_exact_path_field<'a>(
    frame: ExactPathFrameRef<'a>,
    field: &str,
    optional: bool,
    out: &mut Vec<ExactPathFrameRef<'a>>,
) -> Result<(), AqError> {
    match frame.value {
        ExactPathValueRef::Borrowed(value) => match value.untagged() {
            Value::Object(fields) => {
                let mut path = frame.path;
                path.push(PathComponent::Field(field.to_string()));
                out.push(ExactPathFrameRef {
                    value: fields
                        .get(field)
                        .map(ExactPathValueRef::from_value)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(field_access_error(other, field))
                }
            }
        },
        ExactPathValueRef::Owned(value) => match value.untagged() {
            Value::Object(fields) => {
                let mut path = frame.path;
                path.push(PathComponent::Field(field.to_string()));
                out.push(ExactPathFrameRef {
                    value: fields
                        .get(field)
                        .cloned()
                        .map(ExactPathValueRef::from_owned)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(field_access_error(other, field))
                }
            }
        },
        ExactPathValueRef::Null => {
            let mut path = frame.path;
            path.push(PathComponent::Field(field.to_string()));
            out.push(ExactPathFrameRef {
                value: ExactPathValueRef::Null,
                path,
            });
            Ok(())
        }
    }
}

fn apply_exact_path_index<'a>(
    frame: ExactPathFrameRef<'a>,
    index: isize,
    optional: bool,
    out: &mut Vec<ExactPathFrameRef<'a>>,
) -> Result<(), AqError> {
    match frame.value {
        ExactPathValueRef::Borrowed(value) => match value.untagged() {
            Value::Array(items) => {
                let mut path = frame.path;
                path.push(PathComponent::Index(index));
                let resolved = resolve_index(index, items.len());
                out.push(ExactPathFrameRef {
                    value: resolved
                        .and_then(|resolved| items.get(resolved))
                        .map(ExactPathValueRef::from_value)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(AqError::Query(format!(
                        "cannot index {} with [{}]",
                        kind_name(other),
                        index
                    )))
                }
            }
        },
        ExactPathValueRef::Owned(value) => match value.untagged() {
            Value::Array(items) => {
                let mut path = frame.path;
                path.push(PathComponent::Index(index));
                let resolved = resolve_index(index, items.len());
                out.push(ExactPathFrameRef {
                    value: resolved
                        .and_then(|resolved| items.get(resolved))
                        .cloned()
                        .map(ExactPathValueRef::from_owned)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(AqError::Query(format!(
                        "cannot index {} with [{}]",
                        kind_name(other),
                        index
                    )))
                }
            }
        },
        ExactPathValueRef::Null => {
            let mut path = frame.path;
            path.push(PathComponent::Index(index));
            out.push(ExactPathFrameRef {
                value: ExactPathValueRef::Null,
                path,
            });
            Ok(())
        }
    }
}

fn apply_exact_path_slice<'a>(
    frame: ExactPathFrameRef<'a>,
    start: Option<isize>,
    end: Option<isize>,
    optional: bool,
    out: &mut Vec<ExactPathFrameRef<'a>>,
) -> Result<(), AqError> {
    match frame.value {
        ExactPathValueRef::Borrowed(value) => match value.untagged() {
            Value::Array(items) => {
                let mut path = frame.path;
                path.push(PathComponent::Slice { start, end });
                let (start, end) = resolve_slice_bounds(start, end, items.len());
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::from_owned(Value::Array(items[start..end].to_vec())),
                    path,
                });
                Ok(())
            }
            Value::String(text) => {
                let mut path = frame.path;
                path.push(PathComponent::Slice { start, end });
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::from_owned(Value::String(slice_string(
                        text, start, end,
                    ))),
                    path,
                });
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(AqError::Query(format!("cannot slice {}", kind_name(other))))
                }
            }
        },
        ExactPathValueRef::Owned(value) => match value.untagged() {
            Value::Array(items) => {
                let mut path = frame.path;
                path.push(PathComponent::Slice { start, end });
                let (start, end) = resolve_slice_bounds(start, end, items.len());
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::from_owned(Value::Array(items[start..end].to_vec())),
                    path,
                });
                Ok(())
            }
            Value::String(text) => {
                let mut path = frame.path;
                path.push(PathComponent::Slice { start, end });
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::from_owned(Value::String(slice_string(
                        text, start, end,
                    ))),
                    path,
                });
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(AqError::Query(format!("cannot slice {}", kind_name(other))))
                }
            }
        },
        ExactPathValueRef::Null => {
            let mut path = frame.path;
            path.push(PathComponent::Slice { start, end });
            out.push(ExactPathFrameRef {
                value: ExactPathValueRef::Null,
                path,
            });
            Ok(())
        }
    }
}

fn apply_exact_path_iterate<'a>(
    frame: ExactPathFrameRef<'a>,
    optional: bool,
    out: &mut Vec<ExactPathFrameRef<'a>>,
) -> Result<(), AqError> {
    match frame.value {
        ExactPathValueRef::Borrowed(value) => match value.untagged() {
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    let mut path = frame.path.clone();
                    path.push(PathComponent::Index(
                        isize::try_from(index).unwrap_or(isize::MAX),
                    ));
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_value(item),
                        path,
                    });
                }
                Ok(())
            }
            Value::Object(fields) => {
                for (key, value) in fields {
                    let mut path = frame.path.clone();
                    path.push(PathComponent::Field(key.clone()));
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_value(value),
                        path,
                    });
                }
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(iterate_error(other))
                }
            }
        },
        ExactPathValueRef::Owned(value) => match value.untagged() {
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    let mut path = frame.path.clone();
                    path.push(PathComponent::Index(
                        isize::try_from(index).unwrap_or(isize::MAX),
                    ));
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_owned(item.clone()),
                        path,
                    });
                }
                Ok(())
            }
            Value::Object(fields) => {
                for (key, value) in fields {
                    let mut path = frame.path.clone();
                    path.push(PathComponent::Field(key.clone()));
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_owned(value.clone()),
                        path,
                    });
                }
                Ok(())
            }
            other => {
                if optional {
                    Ok(())
                } else {
                    Err(iterate_error(other))
                }
            }
        },
        ExactPathValueRef::Null => {
            if optional {
                Ok(())
            } else {
                Err(iterate_error(&Value::Null))
            }
        }
    }
}

fn apply_exact_path_lookup<'a>(
    name: &str,
    frame: ExactPathFrameRef<'a>,
    lookup: Value,
    optional: bool,
    out: &mut Vec<ExactPathFrameRef<'a>>,
) -> Result<(), AqError> {
    if name == "del" && value_contains_non_finite_float(&lookup) {
        return Ok(());
    }

    if let Some((start, end)) = lookup_slice_bounds(&lookup) {
        return match frame.value {
            ExactPathValueRef::Borrowed(value) => match value.untagged() {
                Value::Array(items) => {
                    let mut path = frame.path;
                    path.push(PathComponent::Slice { start, end });
                    let (start, end) = resolve_slice_bounds(start, end, items.len());
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_owned(Value::Array(
                            items[start..end].to_vec(),
                        )),
                        path,
                    });
                    Ok(())
                }
                Value::String(text) => {
                    let mut path = frame.path;
                    path.push(PathComponent::Slice { start, end });
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_owned(Value::String(slice_string(
                            text, start, end,
                        ))),
                        path,
                    });
                    Ok(())
                }
                Value::Object(_) => Err(AqError::Query(
                    "cannot index object with object".to_string(),
                )),
                other => Err(AqError::Query(format!("cannot slice {}", kind_name(other)))),
            },
            ExactPathValueRef::Owned(value) => match value.untagged() {
                Value::Array(items) => {
                    let mut path = frame.path;
                    path.push(PathComponent::Slice { start, end });
                    let (start, end) = resolve_slice_bounds(start, end, items.len());
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_owned(Value::Array(
                            items[start..end].to_vec(),
                        )),
                        path,
                    });
                    Ok(())
                }
                Value::String(text) => {
                    let mut path = frame.path;
                    path.push(PathComponent::Slice { start, end });
                    out.push(ExactPathFrameRef {
                        value: ExactPathValueRef::from_owned(Value::String(slice_string(
                            text, start, end,
                        ))),
                        path,
                    });
                    Ok(())
                }
                Value::Object(_) => Err(AqError::Query(
                    "cannot index object with object".to_string(),
                )),
                other => Err(AqError::Query(format!("cannot slice {}", kind_name(other)))),
            },
            ExactPathValueRef::Null => {
                let mut path = frame.path;
                path.push(PathComponent::Slice { start, end });
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::Null,
                    path,
                });
                Ok(())
            }
        };
    }

    match frame.value {
        ExactPathValueRef::Borrowed(value) => match (value.untagged(), lookup.untagged()) {
            (Value::Object(fields), Value::String(key)) => {
                let mut path = frame.path;
                path.push(PathComponent::Field(key.clone()));
                out.push(ExactPathFrameRef {
                    value: fields
                        .get(key)
                        .map(ExactPathValueRef::from_value)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            (Value::Array(items), Value::Integer(index)) => {
                let index = isize::try_from(*index)
                    .map_err(|_| AqError::Query(format!("{name} path index is out of range")))?;
                let mut path = frame.path;
                path.push(PathComponent::Index(index));
                let resolved = resolve_index(index, items.len());
                out.push(ExactPathFrameRef {
                    value: resolved
                        .and_then(|resolved| items.get(resolved))
                        .map(ExactPathValueRef::from_value)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            (Value::Array(items), Value::Decimal(_)) | (Value::Array(items), Value::Float(_)) => {
                let PathComponent::Index(index) = exact_path_component_of(name, &lookup)? else {
                    return Err(AqError::Query(format!(
                        "{name} requires exact field/index paths, got object path component"
                    )));
                };
                let mut path = frame.path;
                path.push(PathComponent::Index(index));
                let resolved = resolve_index(index, items.len());
                out.push(ExactPathFrameRef {
                    value: resolved
                        .and_then(|resolved| items.get(resolved))
                        .map(ExactPathValueRef::from_value)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            (Value::Object(_), other) => Err(index_lookup_error(value, other)),
            (Value::Array(_), other) => Err(index_lookup_error(value, other)),
            (_other, _) if optional => Ok(()),
            (other, raw_lookup) => Err(index_lookup_error(other, raw_lookup)),
        },
        ExactPathValueRef::Owned(value) => match (value.untagged(), lookup.untagged()) {
            (Value::Object(fields), Value::String(key)) => {
                let mut path = frame.path;
                path.push(PathComponent::Field(key.clone()));
                out.push(ExactPathFrameRef {
                    value: fields
                        .get(key)
                        .cloned()
                        .map(ExactPathValueRef::from_owned)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            (Value::Array(items), Value::Integer(index)) => {
                let index = isize::try_from(*index)
                    .map_err(|_| AqError::Query(format!("{name} path index is out of range")))?;
                let mut path = frame.path;
                path.push(PathComponent::Index(index));
                let resolved = resolve_index(index, items.len());
                out.push(ExactPathFrameRef {
                    value: resolved
                        .and_then(|resolved| items.get(resolved))
                        .cloned()
                        .map(ExactPathValueRef::from_owned)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            (Value::Array(items), Value::Decimal(_)) | (Value::Array(items), Value::Float(_)) => {
                let PathComponent::Index(index) = exact_path_component_of(name, &lookup)? else {
                    return Err(AqError::Query(format!(
                        "{name} requires exact field/index paths, got object path component"
                    )));
                };
                let mut path = frame.path;
                path.push(PathComponent::Index(index));
                let resolved = resolve_index(index, items.len());
                out.push(ExactPathFrameRef {
                    value: resolved
                        .and_then(|resolved| items.get(resolved))
                        .cloned()
                        .map(ExactPathValueRef::from_owned)
                        .unwrap_or(ExactPathValueRef::Null),
                    path,
                });
                Ok(())
            }
            (Value::Object(_), other) => Err(index_lookup_error(value.untagged(), other)),
            (Value::Array(_), other) => Err(index_lookup_error(value.untagged(), other)),
            (_other, _) if optional => Ok(()),
            (other, raw_lookup) => Err(index_lookup_error(other, raw_lookup)),
        },
        ExactPathValueRef::Null => match lookup.untagged() {
            Value::String(key) => {
                let mut path = frame.path;
                path.push(PathComponent::Field(key.clone()));
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::Null,
                    path,
                });
                Ok(())
            }
            Value::Integer(index) => {
                let index = isize::try_from(*index)
                    .map_err(|_| AqError::Query(format!("{name} path index is out of range")))?;
                let mut path = frame.path;
                path.push(PathComponent::Index(index));
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::Null,
                    path,
                });
                Ok(())
            }
            Value::Decimal(_) | Value::Float(_) => {
                let component = exact_path_component_of(name, &lookup)?;
                let mut path = frame.path;
                path.push(component);
                out.push(ExactPathFrameRef {
                    value: ExactPathValueRef::Null,
                    path,
                });
                Ok(())
            }
            _ if optional => Ok(()),
            other => Err(index_lookup_error(&Value::Null, other)),
        },
    }
}

fn contains_non_finite_path_component(values: &[Value]) -> bool {
    values.iter().any(value_contains_non_finite_float)
}

fn value_contains_non_finite_float(value: &Value) -> bool {
    match value.untagged() {
        Value::Float(value) => !value.is_finite(),
        Value::Array(values) => values.iter().any(value_contains_non_finite_float),
        Value::Object(fields) => fields.values().any(value_contains_non_finite_float),
        _ => false,
    }
}

fn exact_path_components_of(name: &str, values: &[Value]) -> Result<Vec<PathComponent>, AqError> {
    values
        .iter()
        .map(|value| exact_path_component_of(name, value))
        .collect()
}

fn exact_path_component_of(name: &str, value: &Value) -> Result<PathComponent, AqError> {
    match value.untagged() {
        Value::String(value) => Ok(PathComponent::Field(value.clone())),
        Value::Integer(value) => Ok(PathComponent::Index(
            isize::try_from(*value)
                .map_err(|_| AqError::Query(format!("{name} path index is out of range")))?,
        )),
        Value::Decimal(value) => {
            let truncated = value.to_f64_lossy().trunc();
            if truncated < isize::MIN as f64 || truncated > isize::MAX as f64 {
                return Err(AqError::Query(format!("{name} path index is out of range")));
            }
            Ok(PathComponent::Index(truncated as isize))
        }
        Value::Float(value) if value.is_finite() => {
            let truncated = value.trunc();
            if truncated < isize::MIN as f64 || truncated > isize::MAX as f64 {
                return Err(AqError::Query(format!("{name} path index is out of range")));
            }
            Ok(PathComponent::Index(truncated as isize))
        }
        Value::Float(value) if !value.is_finite() && name == "assignment" && value.is_nan() => Err(
            AqError::Query("Cannot set array element at NaN index".to_string()),
        ),
        Value::Float(value) if !value.is_finite() => {
            Err(AqError::Query(format!("{name} path index is out of range")))
        }
        Value::Object(fields) => parse_slice_path_component(name, fields)?.ok_or_else(|| {
            AqError::Query(format!(
                "{name} requires exact field/index paths, got object path component"
            ))
        }),
        other => Err(AqError::Query(format!(
            "{name} requires exact field/index paths, got {} path component",
            kind_name(other)
        ))),
    }
}

fn parse_slice_path_component(
    name: &str,
    fields: &IndexMap<String, Value>,
) -> Result<Option<PathComponent>, AqError> {
    if fields.keys().any(|key| key != "start" && key != "end") {
        return Ok(None);
    }

    let start = fields
        .get("start")
        .map(|value| slice_path_start_bound_of(name, value))
        .transpose()?;
    let end = fields
        .get("end")
        .map(|value| slice_path_end_bound_of(name, value))
        .transpose()?;
    Ok(Some(PathComponent::Slice { start, end }))
}

fn slice_path_start_bound_of(name: &str, value: &Value) -> Result<isize, AqError> {
    match value.untagged() {
        Value::Integer(value) => isize::try_from(*value)
            .map_err(|_| AqError::Query(format!("{name} path index is out of range"))),
        Value::Decimal(value) => isize::try_from(
            value
                .as_i64_exact()
                .ok_or_else(|| AqError::Query(format!("{name} path index is out of range")))?,
        )
        .map_err(|_| AqError::Query(format!("{name} path index is out of range"))),
        Value::Float(value) if value.is_finite() => {
            let floored = value.floor();
            if floored < isize::MIN as f64 || floored > isize::MAX as f64 {
                return Err(AqError::Query(format!("{name} path index is out of range")));
            }
            Ok(floored as isize)
        }
        other => Err(AqError::Query(format!(
            "{name} requires exact field/index paths, got {} path component",
            kind_name(other)
        ))),
    }
}

fn slice_path_end_bound_of(name: &str, value: &Value) -> Result<isize, AqError> {
    match value.untagged() {
        Value::Integer(value) => isize::try_from(*value)
            .map_err(|_| AqError::Query(format!("{name} path index is out of range"))),
        Value::Decimal(value) => isize::try_from(
            value
                .as_i64_exact()
                .ok_or_else(|| AqError::Query(format!("{name} path index is out of range")))?,
        )
        .map_err(|_| AqError::Query(format!("{name} path index is out of range"))),
        Value::Float(value) if value.is_finite() => {
            let ceiled = value.ceil();
            if ceiled < isize::MIN as f64 || ceiled > isize::MAX as f64 {
                return Err(AqError::Query(format!("{name} path index is out of range")));
            }
            Ok(ceiled as isize)
        }
        other => Err(AqError::Query(format!(
            "{name} requires exact field/index paths, got {} path component",
            kind_name(other)
        ))),
    }
}

fn evaluate_path_query_frames(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let mut out = Vec::new();
    for pipeline in &query.outputs {
        out.extend(evaluate_path_pipeline_frames(
            pipeline, input, bindings, context,
        )?);
    }
    Ok(out)
}

fn evaluate_path_pipeline_frames(
    pipeline: &Pipeline,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let mut current = vec![PathValueFrame {
        value: input.clone(),
        path: Vec::new(),
        bindings: bindings.clone(),
    }];
    for (index, stage) in pipeline.stages.iter().enumerate() {
        current = match apply_path_stage(stage, current.clone(), context) {
            Ok(next) => next,
            Err(AqError::Query(message))
                if message.starts_with("Invalid path expression")
                    && pipeline.stages.get(index + 1).is_some() =>
            {
                return invalid_path_pipeline_error(
                    stage,
                    pipeline
                        .stages
                        .get(index + 1)
                        .expect("next stage should exist"),
                    &current,
                    bindings,
                    context,
                );
            }
            Err(error) => return Err(error),
        };
    }
    Ok(current)
}

fn apply_path_stage(
    expr: &Expr,
    frames: Vec<PathValueFrame>,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let mut out = Vec::new();
    for frame in frames {
        out.extend(evaluate_path_expr(expr, frame, context)?);
    }
    Ok(out)
}

fn evaluate_path_expr(
    expr: &Expr,
    frame: PathValueFrame,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    match expr {
        Expr::Path(path) => {
            let scope_input = frame.value.clone();
            apply_path_segments(&path.segments, vec![frame], &scope_input, context)
        }
        Expr::Access { base, segments } => {
            let scope_input = frame.value.clone();
            let frame_bindings = frame.bindings.clone();
            let base_frames = match evaluate_path_expr(
                base,
                PathValueFrame {
                    value: scope_input.clone(),
                    path: Vec::new(),
                    bindings: frame_bindings.clone(),
                },
                context,
            ) {
                Ok(frames) => prefix_path_frames(&frame.path, frames),
                Err(AqError::Query(message))
                    if message == "path requires a path expression"
                        || message.starts_with("Invalid path expression") =>
                {
                    return invalid_path_access_error(
                        base,
                        segments,
                        &scope_input,
                        &frame_bindings,
                        context,
                    );
                }
                Err(error) => return Err(error),
            };
            apply_path_segments(segments, base_frames, &scope_input, context)
        }
        Expr::Subquery(query) => Ok(prefix_path_frames(
            &frame.path,
            evaluate_path_query_frames(query, &frame.value, &frame.bindings, context)?,
        )),
        Expr::FunctionCall { name, args } => {
            evaluate_path_function_call(name, args, frame, context)
        }
        Expr::Bind { expr, pattern } => evaluate_path_binding(expr, pattern, frame, context),
        Expr::Builtin(BuiltinExpr::Empty) => Ok(Vec::new()),
        Expr::Builtin(BuiltinExpr::Select(predicate)) => {
            let results = evaluate_expr(predicate, &frame.value, &frame.bindings, context)?;
            if results.iter().any(is_truthy) {
                Ok(vec![frame])
            } else {
                Ok(Vec::new())
            }
        }
        Expr::Builtin(BuiltinExpr::GetPath(query)) => {
            evaluate_getpath_path_frames(query, frame, context)
        }
        Expr::Builtin(BuiltinExpr::First) => path_first_frames(frame),
        Expr::Builtin(BuiltinExpr::Last) => path_last_frames(frame),
        Expr::Builtin(BuiltinExpr::Recurse { query, condition }) => {
            recurse_path_frames(query.as_deref(), condition.as_deref(), frame, context)
        }
        _ => invalid_path_expression_error(expr, &frame.value, &frame.bindings, context),
    }
}

fn path_first_frames(frame: PathValueFrame) -> Result<Vec<PathValueFrame>, AqError> {
    match frame.value.untagged() {
        Value::Array(values) => {
            let Some(value) = values.first().cloned() else {
                return Ok(Vec::new());
            };
            let mut path = frame.path;
            path.push(Value::Integer(0));
            Ok(vec![PathValueFrame {
                value,
                path,
                bindings: frame.bindings,
            }])
        }
        other => Err(AqError::Query(format!(
            "first is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn path_last_frames(frame: PathValueFrame) -> Result<Vec<PathValueFrame>, AqError> {
    match frame.value.untagged() {
        Value::Array(values) => {
            let Some(value) = values.last().cloned() else {
                return Ok(Vec::new());
            };
            let mut path = frame.path;
            path.push(Value::Integer(-1));
            Ok(vec![PathValueFrame {
                value,
                path,
                bindings: frame.bindings,
            }])
        }
        other => Err(AqError::Query(format!(
            "last is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn evaluate_path_binding(
    expr: &Expr,
    pattern: &BindingPattern,
    frame: PathValueFrame,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let bound_values = evaluate_expr(expr, &frame.value, &frame.bindings, context)?;
    let mut out = Vec::with_capacity(bound_values.len());
    for bound_value in bound_values {
        let mut bindings = frame.bindings.clone();
        bind_pattern(pattern, &bound_value, &mut bindings, context)?;
        out.push(PathValueFrame {
            value: frame.value.clone(),
            path: frame.path.clone(),
            bindings,
        });
    }
    Ok(out)
}

fn evaluate_path_function_call(
    name: &str,
    args: &[Query],
    frame: PathValueFrame,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let Some(binding) = frame.bindings.functions.lookup(name, args.len()) else {
        return Err(AqError::Query(format!(
            "{name}/{} is not defined",
            args.len()
        )));
    };

    match binding {
        FunctionBinding::Arg {
            query,
            captured_values,
            captured_scope,
        } => {
            if !args.is_empty() {
                return Err(AqError::Query(format!(
                    "{name}/{} is not defined",
                    args.len()
                )));
            }
            let call_bindings =
                Bindings::with_values(captured_values.clone(), Rc::clone(captured_scope));
            Ok(prefix_path_frames(
                &frame.path,
                evaluate_path_query_frames(
                    query,
                    &frame.value,
                    &call_bindings,
                    &context.with_functions(Rc::clone(captured_scope)),
                )?,
            ))
        }
        FunctionBinding::User {
            params,
            body,
            captured_values,
            captured_scope,
        } => {
            let captured_scope = captured_scope.upgrade().ok_or_else(|| {
                AqError::Query("internal error: function scope is no longer available".to_string())
            })?;
            let mut call_scope = Rc::clone(&captured_scope);
            for (param, arg_query) in params.iter().zip(args.iter()) {
                let mut functions = IndexMap::new();
                functions.insert(
                    FunctionKey {
                        name: param.clone(),
                        arity: 0,
                    },
                    FunctionBinding::Arg {
                        query: arg_query.clone(),
                        captured_values: frame.bindings.values.clone(),
                        captured_scope: Rc::clone(&frame.bindings.functions),
                    },
                );
                call_scope = Rc::new(FunctionScope {
                    parent: Some(call_scope),
                    bindings: functions,
                });
            }
            let call_bindings =
                Bindings::with_values(captured_values.clone(), Rc::clone(&call_scope));
            Ok(prefix_path_frames(
                &frame.path,
                evaluate_path_query_frames(
                    body,
                    &frame.value,
                    &call_bindings,
                    &context.with_functions(call_scope),
                )?,
            ))
        }
    }
}

fn recurse_path_frames(
    query: Option<&Query>,
    condition: Option<&Query>,
    frame: PathValueFrame,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let mut out = Vec::new();
    let mut pending = vec![frame];

    while let Some(current) = pending.pop() {
        out.push(current.clone());
        let mut next = match query {
            Some(query) => prefix_path_frames(
                &current.path,
                evaluate_path_query_frames(query, &current.value, &current.bindings, context)?,
            ),
            None => recurse_children_path_frames(&current),
        };
        if let Some(condition) = condition {
            let mut filtered = Vec::with_capacity(next.len());
            for frame in next {
                if query_is_truthy(condition, &frame.value, &frame.bindings, context)? {
                    filtered.push(frame);
                }
            }
            next = filtered;
        }
        for value in next.into_iter().rev() {
            pending.push(value);
        }
    }

    Ok(out)
}

fn recurse_children_path_frames(frame: &PathValueFrame) -> Vec<PathValueFrame> {
    match frame.value.untagged() {
        Value::Array(values) => values
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, value)| {
                let mut path = frame.path.clone();
                path.push(Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)));
                PathValueFrame {
                    value,
                    path,
                    bindings: frame.bindings.clone(),
                }
            })
            .collect(),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                let mut path = frame.path.clone();
                path.push(Value::String(key.clone()));
                PathValueFrame {
                    value: value.clone(),
                    path,
                    bindings: frame.bindings.clone(),
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn evaluate_getpath_path_frames(
    query: &Query,
    frame: PathValueFrame,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let path_value =
        evaluate_single_query_value("getpath", query, &frame.value, &frame.bindings, context)?;
    let path_components = setpath_path_of(&frame.value, &path_value)?;
    let value = getpath_value(&frame.value, &path_components)?;
    let mut path = frame.path;
    path.extend(path_values_from_components(&path_components));
    Ok(vec![PathValueFrame {
        value,
        path,
        bindings: frame.bindings,
    }])
}

fn path_values_from_components(path: &[PathComponent]) -> Vec<Value> {
    path.iter()
        .map(|component| match component {
            PathComponent::Field(name) => Value::String(name.clone()),
            PathComponent::Index(index) => Value::Integer(*index as i64),
            PathComponent::Slice { start, end } => slice_component_value(*start, *end),
        })
        .collect()
}

fn invalid_path_expression_error(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let rendered = rendered_path_expression_result(expr, input, bindings, context)?;
    Err(AqError::Query(format!(
        "Invalid path expression with result {rendered}"
    )))
}

fn invalid_path_access_error(
    base: &Expr,
    segments: &[Segment],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let value = path_expression_result_value(evaluate_expr(base, input, bindings, context)?);
    let rendered = typed_rendered_value(&value).1;
    let Some(segment) = segments.first() else {
        return Err(AqError::Query(format!(
            "Invalid path expression with result {rendered}"
        )));
    };
    let attempt = describe_invalid_path_attempt(segment, &value, bindings, context)?;
    Err(AqError::Query(format_invalid_path_attempt_message(
        &attempt, &rendered,
    )))
}

fn invalid_path_pipeline_error(
    expr: &Expr,
    next_stage: &Expr,
    frames: &[PathValueFrame],
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let Some(frame) = frames.first() else {
        return invalid_path_expression_error(expr, &Value::Null, bindings, context);
    };
    let value = path_expression_result_value(evaluate_expr(expr, &frame.value, bindings, context)?);
    let rendered = typed_rendered_value(&value).1;
    let attempt = describe_invalid_path_stage_attempt(next_stage, &value, bindings, context)?;
    Err(AqError::Query(format_invalid_path_attempt_message(
        &attempt, &rendered,
    )))
}

fn rendered_path_expression_result(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    Ok(
        typed_rendered_value(&path_expression_result_value(evaluate_expr(
            expr, input, bindings, context,
        )?))
        .1,
    )
}

fn path_expression_result_value(values: Vec<Value>) -> Value {
    match values.as_slice() {
        [value] => value.clone(),
        _ => Value::Array(values),
    }
}

fn describe_invalid_path_attempt(
    segment: &Segment,
    value: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    match segment {
        Segment::Field { name, .. } => Ok(format!(
            "access element {}",
            serde_json::to_string(name)
                .map_err(|error| AqError::Query(format!("failed to render path key: {error}")))?
        )),
        Segment::Lookup { expr, .. } => Ok(format!(
            "access element {}",
            rendered_path_expression_result(expr, value, bindings, context)?
        )),
        Segment::Index { index, .. } => Ok(format!("access element {index}")),
        Segment::Slice { start, end, .. } => Ok(format!(
            "access element {}",
            typed_rendered_value(&slice_component_value(*start, *end)).1
        )),
        Segment::Iterate { .. } => Ok("iterate through".to_string()),
    }
}

fn describe_invalid_path_stage_attempt(
    expr: &Expr,
    value: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    match expr {
        Expr::Path(path) => path
            .segments
            .first()
            .map(|segment| describe_invalid_path_attempt(segment, value, bindings, context))
            .unwrap_or_else(|| Ok("evaluate path expression".to_string())),
        Expr::Access { segments, .. } => segments
            .first()
            .map(|segment| describe_invalid_path_attempt(segment, value, bindings, context))
            .unwrap_or_else(|| Ok("evaluate path expression".to_string())),
        Expr::Builtin(BuiltinExpr::First) => Ok("access element 0".to_string()),
        Expr::Builtin(BuiltinExpr::Last) => Ok("access element -1".to_string()),
        _ => Ok("evaluate path expression".to_string()),
    }
}

fn format_invalid_path_attempt_message(attempt: &str, rendered: &str) -> String {
    if attempt == "iterate through" {
        format!("Invalid path expression near attempt to {attempt} {rendered}")
    } else {
        format!("Invalid path expression near attempt to {attempt} of {rendered}")
    }
}

fn prefix_path_frames(prefix: &[Value], frames: Vec<PathValueFrame>) -> Vec<PathValueFrame> {
    frames
        .into_iter()
        .map(|frame| {
            let mut path = prefix.to_vec();
            path.extend(frame.path);
            PathValueFrame {
                value: frame.value,
                path,
                bindings: frame.bindings,
            }
        })
        .collect()
}

fn number_value(value: f64) -> Value {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        Value::Integer(value as i64)
    } else {
        Value::Float(value)
    }
}

fn lossy_numeric_result(value: f64) -> Value {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        DecimalValue::from_lossy_f64(value)
            .map(Value::Decimal)
            .unwrap_or_else(|_| Value::Float(value))
    } else {
        Value::Float(value)
    }
}

fn integer_is_safe_in_f64(value: i64) -> bool {
    (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value)
}

fn numeric_to_f64_lossy(value: &Value) -> Option<f64> {
    match value.untagged() {
        Value::Integer(value) => Some(*value as f64),
        Value::Decimal(value) => Some(value.to_f64_lossy()),
        Value::Float(value) => Some(*value),
        _ => None,
    }
}

fn compare_numeric_order(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left.untagged(), right.untagged()) {
        (Value::Integer(left), Value::Integer(right)) => left.cmp(right),
        (Value::Decimal(left), Value::Decimal(right)) => {
            left.as_bigdecimal().cmp(right.as_bigdecimal())
        }
        (Value::Integer(left), Value::Decimal(right)) => {
            BigDecimal::from(*left).cmp(right.as_bigdecimal())
        }
        (Value::Decimal(left), Value::Integer(right)) => {
            left.as_bigdecimal().cmp(&BigDecimal::from(*right))
        }
        (left, right) => numeric_to_f64_lossy(left)
            .and_then(|left| numeric_to_f64_lossy(right).and_then(|right| left.partial_cmp(&right)))
            .unwrap_or(std::cmp::Ordering::Equal),
    }
}

fn nth_index_of(value: Value) -> Result<usize, AqError> {
    let index = match value {
        Value::Integer(value) => value as f64,
        Value::Decimal(value) => value.to_f64_lossy(),
        Value::Float(value) => value,
        _ => return Err(AqError::Query("nth requires numeric indices".to_string())),
    };
    if index < 0.0 {
        return Err(AqError::Query(
            "nth doesn't support negative indices".to_string(),
        ));
    }
    Ok(index.trunc() as usize)
}

fn evaluate_expr(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    match expr {
        Expr::Path(path) => evaluate_path_with_bindings(path, input, bindings, context),
        Expr::Literal(value) => Ok(vec![value.clone()]),
        Expr::FormatString { operator, parts } => {
            evaluate_format_string(*operator, parts, input, bindings, context)
        }
        Expr::FunctionCall { name, args } => {
            evaluate_function_call(name, args, input, bindings, context)
        }
        Expr::Variable(name) => bindings
            .get_value(name)
            .cloned()
            .or_else(|| (name == "ENV").then(env_of))
            .or_else(|| (name == "__loc__").then(location_value))
            .map(|value| vec![value])
            .ok_or_else(|| undefined_variable_error(name)),
        Expr::Access { base, segments } => {
            evaluate_access_expr(base, segments, input, bindings, context)
        }
        Expr::Array(items) => {
            if let Some(values) = try_simple_array_constructor(items, bindings) {
                return Ok(vec![Value::Array(values?)]);
            }
            if let [Expr::Subquery(query)] = items.as_slice() {
                return Ok(vec![Value::Array(evaluate_query_values(
                    query, input, bindings, context,
                )?)]);
            }
            let mut values = Vec::new();
            for item in items {
                values.extend(evaluate_expr(item, input, bindings, context)?);
            }
            Ok(vec![Value::Array(values)])
        }
        Expr::Object(fields) => {
            if let Some(object) = try_simple_object_constructor(fields, bindings) {
                return Ok(vec![Value::Object(object?)]);
            }
            if let Some(object) = try_direct_object_constructor(fields, input, bindings) {
                return Ok(vec![Value::Object(object?)]);
            }
            let mut partials = vec![IndexMap::new()];
            for (key_expr, value_expr) in fields {
                let keys = evaluate_object_keys(key_expr, input, bindings, context)?;
                if keys.is_empty() {
                    return Ok(Vec::new());
                }

                let values = evaluate_expr(value_expr, input, bindings, context)?;
                if values.is_empty() {
                    return Ok(Vec::new());
                }

                let mut next = Vec::new();
                for partial in &partials {
                    for key in &keys {
                        for value in &values {
                            let mut object = partial.clone();
                            object.insert(key.clone(), value.clone());
                            next.push(object);
                        }
                    }
                }
                partials = next;
            }

            Ok(partials.into_iter().map(Value::Object).collect())
        }
        Expr::Builtin(builtin) => evaluate_builtin(builtin, input, bindings, context),
        Expr::Subquery(query) => evaluate_query_values(query, input, bindings, context),
        Expr::Bind { .. } => Err(AqError::Query(
            "internal error: binding expression reached direct evaluator".to_string(),
        )),
        Expr::BindingAlt { .. } => Err(AqError::Query(
            "internal error: binding alternative reached direct evaluator".to_string(),
        )),
        Expr::Reduce {
            source,
            pattern,
            init,
            update,
        } => evaluate_reduce(source, pattern, init, update, input, bindings, context),
        Expr::ForEach {
            source,
            pattern,
            init,
            update,
            extract,
        } => evaluate_foreach(
            &ForeachSpec {
                source,
                pattern,
                init,
                update,
                extract,
            },
            input,
            bindings,
            context,
        ),
        Expr::If {
            branches,
            else_branch,
        } => evaluate_if(branches, else_branch, input, bindings, context),
        Expr::Try { body, catch } => evaluate_try(body, catch.as_deref(), input, bindings, context),
        Expr::Label { name, body } => {
            match evaluate_query_catching_label(body, input, bindings, context, name)? {
                LabelFlow::Continue(frames) | LabelFlow::Break(frames) => {
                    Ok(frames_to_values(frames))
                }
            }
        }
        Expr::Break(name) => Err(AqError::BreakLabel(name.clone())),
        Expr::Assign { path, op, value } => {
            evaluate_assign(path, *op, value, input, bindings, context)
        }
        Expr::Unary { op, expr } => evaluate_unary(*op, expr, input, bindings, context),
        Expr::Binary { left, op, right } => {
            evaluate_binary(left, *op, right, input, bindings, context)
        }
    }
}

fn try_simple_constructor_value(
    expr: &Expr,
    bindings: &Bindings,
) -> Option<Result<Value, AqError>> {
    match expr {
        Expr::Literal(value) => Some(Ok(value.clone())),
        Expr::Variable(name) => Some(
            bindings
                .get_value(name)
                .cloned()
                .or_else(|| (name == "ENV").then(env_of))
                .or_else(|| (name == "__loc__").then(location_value))
                .ok_or_else(|| undefined_variable_error(name)),
        ),
        _ => None,
    }
}

fn try_simple_array_constructor(
    items: &[Expr],
    bindings: &Bindings,
) -> Option<Result<Vec<Value>, AqError>> {
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        let value = try_simple_constructor_value(item, bindings)?;
        values.push(match value {
            Ok(value) => value,
            Err(error) => return Some(Err(error)),
        });
    }
    Some(Ok(values))
}

fn try_simple_object_constructor(
    fields: &[(ObjectKey, Expr)],
    bindings: &Bindings,
) -> Option<Result<IndexMap<String, Value>, AqError>> {
    let mut object = IndexMap::with_capacity(fields.len());
    for (key_expr, value_expr) in fields {
        let ObjectKey::Static(key) = key_expr else {
            return None;
        };
        let value = try_simple_constructor_value(value_expr, bindings)?;
        match value {
            Ok(value) => {
                object.insert(key.clone(), value);
            }
            Err(error) => return Some(Err(error)),
        }
    }
    Some(Ok(object))
}

fn try_direct_object_constructor(
    fields: &[(ObjectKey, Expr)],
    input: &Value,
    bindings: &Bindings,
) -> Option<Result<IndexMap<String, Value>, AqError>> {
    let mut object = IndexMap::with_capacity(fields.len());
    for (key_expr, value_expr) in fields {
        let key = match key_expr {
            ObjectKey::Static(key) => key.clone(),
            ObjectKey::Dynamic(expr) => {
                let value = direct_single_value_expr_value(expr, input, bindings)?;
                match value {
                    Value::String(key) => key,
                    other => return Some(Err(object_key_error(&other))),
                }
            }
        };
        let value = direct_single_value_expr_value(value_expr, input, bindings)?;
        object.insert(key, value);
    }
    Some(Ok(object))
}

fn evaluate_expr_catching_label(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<Value>>, AqError> {
    match expr {
        Expr::Subquery(query) => Ok(label_flow_values(evaluate_query_catching_label(
            query,
            input,
            bindings,
            context,
            catch_label,
        )?)),
        Expr::Label { name, body } => {
            match evaluate_query_catching_label(body, input, bindings, context, name)? {
                LabelFlow::Continue(frames) | LabelFlow::Break(frames) => {
                    Ok(LabelFlow::Continue(frames_to_values(frames)))
                }
            }
        }
        Expr::Break(name) => Err(AqError::BreakLabel(name.clone())),
        Expr::ForEach {
            source,
            pattern,
            init,
            update,
            extract,
        } => evaluate_foreach_catching_label(
            &ForeachSpec {
                source,
                pattern,
                init,
                update,
                extract,
            },
            input,
            bindings,
            context,
            catch_label,
        ),
        Expr::If {
            branches,
            else_branch,
        } => {
            evaluate_if_catching_label(branches, else_branch, input, bindings, context, catch_label)
        }
        Expr::Try { body, catch } => evaluate_try_catching_label(
            body,
            catch.as_deref(),
            input,
            bindings,
            context,
            catch_label,
        ),
        _ => Ok(LabelFlow::Continue(evaluate_expr(
            expr, input, bindings, context,
        )?)),
    }
}

fn evaluate_function_call(
    name: &str,
    args: &[Query],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let Some(binding) = bindings.functions.lookup(name, args.len()) else {
        return Err(AqError::Query(format!(
            "{name}/{} is not defined",
            args.len()
        )));
    };
    match binding {
        FunctionBinding::Arg {
            query,
            captured_values,
            captured_scope,
        } => {
            if !args.is_empty() {
                return Err(AqError::Query(format!(
                    "{name}/{} is not defined",
                    args.len()
                )));
            }
            let call_bindings =
                Bindings::with_values(captured_values.clone(), Rc::clone(captured_scope));
            Ok(evaluate_query(
                query,
                input,
                &call_bindings,
                &context.with_functions(Rc::clone(captured_scope)),
            )?
            .into_iter()
            .map(|frame| frame.value)
            .collect())
        }
        FunctionBinding::User {
            params,
            body,
            captured_values,
            captured_scope,
        } => {
            let captured_scope = captured_scope.upgrade().ok_or_else(|| {
                AqError::Query("internal error: function scope is no longer available".to_string())
            })?;
            let mut call_scope = Rc::clone(&captured_scope);
            for (param, arg_query) in params.iter().zip(args.iter()) {
                let mut functions = IndexMap::new();
                functions.insert(
                    FunctionKey {
                        name: param.clone(),
                        arity: 0,
                    },
                    FunctionBinding::Arg {
                        query: arg_query.clone(),
                        captured_values: bindings.values.clone(),
                        captured_scope: Rc::clone(&bindings.functions),
                    },
                );
                call_scope = Rc::new(FunctionScope {
                    parent: Some(call_scope),
                    bindings: functions,
                });
            }
            let call_bindings =
                Bindings::with_values(captured_values.clone(), Rc::clone(&call_scope));
            Ok(evaluate_query(
                body,
                input,
                &call_bindings,
                &context.with_functions(call_scope),
            )?
            .into_iter()
            .map(|frame| frame.value)
            .collect())
        }
    }
}

fn evaluate_builtin(
    builtin: &BuiltinExpr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    match builtin {
        BuiltinExpr::Input => context
            .pop_next_input()
            .map(|value| vec![value])
            .ok_or(AqError::Break),
        BuiltinExpr::Inputs => Ok(context.drain_inputs()),
        BuiltinExpr::ModuleMeta(options) => Ok(vec![module_meta_of(input, options)?]),
        BuiltinExpr::Length => Ok(vec![length_of(input)?]),
        BuiltinExpr::Utf8ByteLength => Ok(vec![Value::Integer(utf8_byte_length_of(input)?)]),
        BuiltinExpr::Keys => Ok(vec![keys_of(input)?]),
        BuiltinExpr::KeysUnsorted => Ok(vec![keys_unsorted_of(input)?]),
        BuiltinExpr::Type => Ok(vec![Value::String(type_name(input).to_string())]),
        BuiltinExpr::Builtins => Ok(vec![builtins_of()]),
        BuiltinExpr::Debug(expr) => debug_of(input, expr.as_deref(), bindings, context),
        BuiltinExpr::Del(query) => Ok(vec![del_of(input, query, bindings, context)?]),
        BuiltinExpr::Error(expr) => error_of(input, expr.as_deref(), bindings, context),
        BuiltinExpr::Env => Ok(vec![env_of()]),
        BuiltinExpr::Select(predicate) => {
            let selected = if let Some(result) = simple_truthy_expr(predicate, input) {
                result?
            } else {
                let values = evaluate_expr(predicate, input, bindings, context)?;
                values.iter().any(is_truthy)
            };
            if selected {
                Ok(vec![input.clone()])
            } else {
                Ok(Vec::new())
            }
        }
        BuiltinExpr::Add => Ok(vec![add_of(input)?]),
        BuiltinExpr::AddQuery(query) => Ok(vec![add_query_of(input, query, bindings, context)?]),
        BuiltinExpr::Avg => Ok(vec![avg_of(input)?]),
        BuiltinExpr::Median => Ok(vec![median_of(input)?]),
        BuiltinExpr::Stddev => Ok(vec![stddev_of(input)?]),
        BuiltinExpr::Percentile(percentile) => {
            Ok(vec![percentile_of(input, percentile, bindings, context)?])
        }
        BuiltinExpr::Histogram(bins) => Ok(vec![histogram_of(input, bins, bindings, context)?]),
        BuiltinExpr::Contains(expected) => Ok(vec![Value::Bool(contains_of(
            input, expected, bindings, context,
        )?)]),
        BuiltinExpr::Inside(container) => Ok(vec![Value::Bool(inside_of(
            input, container, bindings, context,
        )?)]),
        BuiltinExpr::First => Ok(vec![first_of(input)?]),
        BuiltinExpr::FirstQuery(query) => first_query_of(query, input, bindings, context),
        BuiltinExpr::Has(key) => Ok(vec![Value::Bool(has_of(input, key, bindings, context)?)]),
        BuiltinExpr::In(container) => Ok(vec![Value::Bool(in_of(
            input, container, bindings, context,
        )?)]),
        BuiltinExpr::InQuery(query) => Ok(vec![Value::Bool(in_query_of(
            input, query, bindings, context,
        )?)]),
        BuiltinExpr::InSource { source, stream } => Ok(vec![Value::Bool(in_source_of(
            input, source, stream, bindings, context,
        )?)]),
        BuiltinExpr::IsEmpty(expr) => Ok(vec![Value::Bool(isempty_of(
            input, expr, bindings, context,
        )?)]),
        BuiltinExpr::Last => Ok(vec![last_of(input)?]),
        BuiltinExpr::LastQuery(query) => last_query_of(query, input, bindings, context),
        BuiltinExpr::Limit { count, expr } => limit_of(count, expr, input, bindings, context),
        BuiltinExpr::Take(count) => Ok(vec![take_of(input, count, bindings, context)?]),
        BuiltinExpr::Skip(count) => Ok(vec![skip_of(input, count, bindings, context)?]),
        BuiltinExpr::SkipQuery { count, expr } => {
            skip_query_of(count, expr, input, bindings, context)
        }
        BuiltinExpr::Map(expr) => Ok(vec![map_of(input, expr, bindings, context)?]),
        BuiltinExpr::MapValues(expr) => Ok(vec![map_values_of(input, expr, bindings, context)?]),
        BuiltinExpr::Nth { indexes, expr } => nth_of(indexes, expr, input, bindings, context),
        BuiltinExpr::Empty => Ok(Vec::new()),
        BuiltinExpr::Range(args) => range_of(args, input, bindings, context),
        BuiltinExpr::Combinations(count) => {
            combinations_of(input, count.as_deref(), bindings, context)
        }
        BuiltinExpr::Bsearch(target) => bsearch_of(input, target, bindings, context),
        BuiltinExpr::Recurse { query, condition } => recurse_of(
            query.as_deref(),
            condition.as_deref(),
            input,
            bindings,
            context,
        ),
        BuiltinExpr::Repeat(query) => repeat_of(query, input, bindings, context),
        BuiltinExpr::Walk(expr) => walk_of(input, expr, bindings, context),
        BuiltinExpr::While { condition, update } => {
            while_of(condition, update, input, bindings, context)
        }
        BuiltinExpr::Until { condition, next } => {
            until_of(condition, next, input, bindings, context)
        }
        BuiltinExpr::Transpose => Ok(vec![transpose_of(input)?]),
        BuiltinExpr::Reverse => Ok(vec![reverse_of(input)?]),
        BuiltinExpr::Sort => Ok(vec![sort_of(input)?]),
        BuiltinExpr::Min => Ok(vec![min_of(input)?]),
        BuiltinExpr::Max => Ok(vec![max_of(input)?]),
        BuiltinExpr::Unique => Ok(vec![unique_of(input)?]),
        BuiltinExpr::Flatten => Ok(vec![flatten_of(input)?]),
        BuiltinExpr::FlattenDepth(depth) => flatten_with_depth_of(input, depth, bindings, context),
        BuiltinExpr::Floor => Ok(vec![floor_of(input)?]),
        BuiltinExpr::Ceil => Ok(vec![ceil_of(input)?]),
        BuiltinExpr::Round => Ok(vec![round_of(input)?]),
        BuiltinExpr::Abs => Ok(vec![abs_of(input)?]),
        BuiltinExpr::Fabs => Ok(vec![fabs_of(input)?]),
        BuiltinExpr::Sqrt => Ok(vec![sqrt_of(input)?]),
        BuiltinExpr::Log => Ok(vec![log_of(input)?]),
        BuiltinExpr::Log2 => Ok(vec![log2_of(input)?]),
        BuiltinExpr::Log10 => Ok(vec![log10_of(input)?]),
        BuiltinExpr::Exp => Ok(vec![exp_of(input)?]),
        BuiltinExpr::Exp2 => Ok(vec![exp2_of(input)?]),
        BuiltinExpr::Sin => Ok(vec![sin_of(input)?]),
        BuiltinExpr::Cos => Ok(vec![cos_of(input)?]),
        BuiltinExpr::Tan => Ok(vec![tan_of(input)?]),
        BuiltinExpr::Asin => Ok(vec![asin_of(input)?]),
        BuiltinExpr::Acos => Ok(vec![acos_of(input)?]),
        BuiltinExpr::Atan => Ok(vec![atan_of(input)?]),
        BuiltinExpr::Pow { base, exponent } => {
            Ok(vec![pow_of(base, exponent, input, bindings, context)?])
        }
        BuiltinExpr::Now => Ok(vec![now_of()?]),
        BuiltinExpr::ToDate => Ok(vec![todate_of(input)?]),
        BuiltinExpr::FromDate => Ok(vec![fromdate_of(input)?]),
        BuiltinExpr::ToDateTime => Ok(vec![to_datetime_of(input)?]),
        BuiltinExpr::GmTime => Ok(vec![gmtime_of(input)?]),
        BuiltinExpr::MkTime => Ok(vec![mktime_of(input)?]),
        BuiltinExpr::StrFTime(format) => strftime_of(input, format, bindings, context),
        BuiltinExpr::StrFLocalTime(format) => strflocaltime_of(input, format, bindings, context),
        BuiltinExpr::StrPTime(format) => strptime_of(input, format, bindings, context),
        BuiltinExpr::TypeFilter(filter) => filter_of(input, *filter),
        BuiltinExpr::ToString => Ok(vec![Value::String(to_string_of(input)?)]),
        BuiltinExpr::ToNumber => Ok(vec![to_number_of(input)?]),
        BuiltinExpr::ToBool => Ok(vec![to_bool_of(input)?]),
        BuiltinExpr::ToBoolean => Ok(vec![to_boolean_of(input)?]),
        BuiltinExpr::Infinite => Ok(vec![Value::Float(f64::MAX)]),
        BuiltinExpr::Nan => Ok(vec![Value::Float(f64::NAN)]),
        BuiltinExpr::IsNan => Ok(vec![Value::Bool(isnan_of(input))]),
        BuiltinExpr::Test { regex, flags } => Ok(vec![Value::Bool(test_of(
            input,
            regex,
            flags.as_deref(),
            bindings,
            context,
        )?)]),
        BuiltinExpr::Capture { regex, flags } => Ok(vec![capture_of(
            input,
            regex,
            flags.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::Match { regex, flags } => {
            match_of(input, regex, flags.as_deref(), bindings, context)
        }
        BuiltinExpr::Scan { regex, flags } => {
            scan_of(input, regex, flags.as_deref(), bindings, context)
        }
        BuiltinExpr::Format(operator) => Ok(vec![Value::String(format_of(input, *operator)?)]),
        BuiltinExpr::StartsWith(prefix) => Ok(vec![Value::Bool(starts_with_of(
            input, prefix, bindings, context,
        )?)]),
        BuiltinExpr::EndsWith(suffix) => Ok(vec![Value::Bool(ends_with_of(
            input, suffix, bindings, context,
        )?)]),
        BuiltinExpr::Split { pattern, flags } => Ok(vec![split_of(
            input,
            pattern,
            flags.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::Splits { pattern, flags } => {
            splits_of(input, pattern, flags.as_deref(), bindings, context)
        }
        BuiltinExpr::Sub {
            regex,
            replacement,
            flags,
        } => regex_replace_of(
            &RegexReplaceSpec {
                name: "sub",
                regex_query: regex,
                replacement_query: replacement,
                flags_query: flags.as_deref(),
                global: false,
            },
            input,
            bindings,
            context,
        ),
        BuiltinExpr::Gsub {
            regex,
            replacement,
            flags,
        } => regex_replace_of(
            &RegexReplaceSpec {
                name: "gsub",
                regex_query: regex,
                replacement_query: replacement,
                flags_query: flags.as_deref(),
                global: true,
            },
            input,
            bindings,
            context,
        ),
        BuiltinExpr::Any(predicate) => Ok(vec![Value::Bool(any_of(
            input,
            predicate.as_deref(),
            bindings,
            context,
        )?)]),
        BuiltinExpr::All(predicate) => Ok(vec![Value::Bool(all_of(
            input,
            predicate.as_deref(),
            bindings,
            context,
        )?)]),
        BuiltinExpr::AnyFrom { source, predicate } => Ok(vec![Value::Bool(any_from_of(
            input, source, predicate, bindings, context,
        )?)]),
        BuiltinExpr::AllFrom { source, predicate } => Ok(vec![Value::Bool(all_from_of(
            input, source, predicate, bindings, context,
        )?)]),
        BuiltinExpr::Join(separator) => join_of(input, separator, bindings, context),
        BuiltinExpr::JoinInput { index, key } => {
            Ok(vec![join_input_of(input, index, key, bindings, context)?])
        }
        BuiltinExpr::JoinStream {
            index,
            source,
            key,
            join,
        } => join_stream_of(
            input,
            index,
            source,
            key,
            join.as_deref(),
            bindings,
            context,
        ),
        BuiltinExpr::AsciiDowncase => Ok(vec![Value::String(ascii_downcase_of(input)?)]),
        BuiltinExpr::AsciiUpcase => Ok(vec![Value::String(ascii_upcase_of(input)?)]),
        BuiltinExpr::Trim => Ok(vec![Value::String(trim_of(input)?.to_string())]),
        BuiltinExpr::Ltrim => Ok(vec![Value::String(ltrim_of(input)?.to_string())]),
        BuiltinExpr::Rtrim => Ok(vec![Value::String(rtrim_of(input)?.to_string())]),
        BuiltinExpr::ToEntries => Ok(vec![to_entries_of(input)?]),
        BuiltinExpr::FromEntries => Ok(vec![from_entries_of(input)?]),
        BuiltinExpr::WithEntries(expr) => {
            Ok(vec![with_entries_of(input, expr, bindings, context)?])
        }
        BuiltinExpr::SortBy(expr) => Ok(vec![sort_by_of(input, expr, bindings, context)?]),
        BuiltinExpr::SortByDesc(expr) => Ok(vec![sort_by_desc_of(input, expr, bindings, context)?]),
        BuiltinExpr::GroupBy(expr) => Ok(vec![group_by_of(input, expr, bindings, context)?]),
        BuiltinExpr::UniqueBy(expr) => Ok(vec![unique_by_of(input, expr, bindings, context)?]),
        BuiltinExpr::CountBy(expr) => Ok(vec![count_by_of(input, expr, bindings, context)?]),
        BuiltinExpr::Columns => Ok(vec![columns_of(input)?]),
        BuiltinExpr::YamlTag(query) => Ok(vec![yaml_tag_of(
            input,
            query.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::XmlAttr(query) => Ok(vec![xml_attr_of(
            input,
            query.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::CsvHeader(query) => Ok(vec![csv_header_of(
            input,
            query.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::Merge { value, deep } => {
            merge_of(input, value, deep.as_deref(), bindings, context)
        }
        BuiltinExpr::MergeAll(query) => Ok(vec![merge_all_of(
            input,
            query.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::SortKeys(query) => Ok(vec![sort_keys_of(
            input,
            query.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::DropNulls(query) => Ok(vec![drop_nulls_of(
            input,
            query.as_deref(),
            bindings,
            context,
        )?]),
        BuiltinExpr::Pick(query) => Ok(vec![pick_of(input, query, bindings, context)?]),
        BuiltinExpr::Omit(query) => Ok(vec![omit_of(input, query, bindings, context)?]),
        BuiltinExpr::Rename { path, name } => {
            Ok(vec![rename_of(input, path, name, bindings, context)?])
        }
        BuiltinExpr::MinBy(expr) => Ok(vec![min_by_of(input, expr, bindings, context)?]),
        BuiltinExpr::MaxBy(expr) => Ok(vec![max_by_of(input, expr, bindings, context)?]),
        BuiltinExpr::GetPath(query) => getpath_of(input, query, bindings, context),
        BuiltinExpr::SetPath { path, value } => setpath_of(input, path, value, bindings, context),
        BuiltinExpr::DelPaths(query) => Ok(vec![delpaths_of(input, query, bindings, context)?]),
        BuiltinExpr::Path(query) => path_of_builtin(input, query, bindings, context),
        BuiltinExpr::Paths(query) => paths_of_builtin(input, query.as_deref(), bindings, context),
        BuiltinExpr::TruncateStream(query) => truncate_stream_of(input, query, bindings, context),
        BuiltinExpr::FromStream(query) => fromstream_of(input, query, bindings, context),
        BuiltinExpr::ToStream => tostream_of(input),
        BuiltinExpr::LeafPaths => leaf_paths_of_builtin(input),
        BuiltinExpr::Indices(expr) => indices_of(input, expr, bindings, context),
        BuiltinExpr::IndexInput(query) => {
            Ok(vec![index_input_of(input, query, bindings, context)?])
        }
        BuiltinExpr::IndexStream { source, key } => Ok(vec![index_stream_of(
            input, source, key, bindings, context,
        )?]),
        BuiltinExpr::Index(expr) => index_of(input, expr, bindings, context),
        BuiltinExpr::Rindex(expr) => rindex_of(input, expr, bindings, context),
        BuiltinExpr::TrimStr(expr) => Ok(vec![Value::String(trimstr_of(
            input, expr, bindings, context,
        )?)]),
        BuiltinExpr::LtrimStr(expr) => Ok(vec![Value::String(ltrimstr_of(
            input, expr, bindings, context,
        )?)]),
        BuiltinExpr::RtrimStr(expr) => Ok(vec![Value::String(rtrimstr_of(
            input, expr, bindings, context,
        )?)]),
        BuiltinExpr::ToJson => Ok(vec![Value::String(to_json_of(input)?)]),
        BuiltinExpr::FromJson => Ok(vec![from_json_of(input)?]),
        BuiltinExpr::Explode => Ok(vec![explode_of(input)?]),
        BuiltinExpr::Implode => Ok(vec![Value::String(implode_of(input)?)]),
    }
}

fn evaluate_if(
    branches: &[(Query, Query)],
    else_branch: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(value) = direct_single_value_if_value(branches, else_branch, input, bindings) {
        return Ok(vec![value]);
    }
    evaluate_if_branch_chain(branches, else_branch, input, bindings, context)
}

fn evaluate_if_catching_label(
    branches: &[(Query, Query)],
    else_branch: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<Value>>, AqError> {
    if let Some(value) = direct_single_value_if_value(branches, else_branch, input, bindings) {
        return Ok(LabelFlow::Continue(vec![value]));
    }
    evaluate_if_branch_chain_catching_label(
        branches,
        else_branch,
        input,
        bindings,
        context,
        catch_label,
    )
}

fn evaluate_if_branch_chain(
    branches: &[(Query, Query)],
    else_branch: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let Some((condition, branch)) = branches.first() else {
        return Ok(evaluate_query(else_branch, input, bindings, context)?
            .into_iter()
            .map(|frame| frame.value)
            .collect());
    };

    let condition_values = evaluate_query(condition, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
    if condition_values.is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for value in condition_values {
        if is_truthy(&value) {
            out.extend(
                evaluate_query(branch, input, bindings, context)?
                    .into_iter()
                    .map(|frame| frame.value),
            );
        } else {
            out.extend(evaluate_if_branch_chain(
                &branches[1..],
                else_branch,
                input,
                bindings,
                context,
            )?);
        }
    }
    Ok(out)
}

fn evaluate_if_branch_chain_catching_label(
    branches: &[(Query, Query)],
    else_branch: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<Value>>, AqError> {
    let Some((condition, branch)) = branches.first() else {
        return Ok(label_flow_values(evaluate_query_catching_label(
            else_branch,
            input,
            bindings,
            context,
            catch_label,
        )?));
    };

    let condition_values =
        match evaluate_query_catching_label(condition, input, bindings, context, catch_label)? {
            LabelFlow::Continue(frames) => frames_to_values(frames),
            LabelFlow::Break(_) => return Ok(LabelFlow::Break(Vec::new())),
        };
    if condition_values.is_empty() {
        return Ok(LabelFlow::Continue(Vec::new()));
    }

    let mut out = Vec::new();
    for value in condition_values {
        if is_truthy(&value) {
            match evaluate_query_catching_label(branch, input, bindings, context, catch_label)? {
                LabelFlow::Continue(frames) => out.extend(frames_to_values(frames)),
                LabelFlow::Break(frames) => {
                    out.extend(frames_to_values(frames));
                    return Ok(LabelFlow::Break(out));
                }
            }
        } else {
            match evaluate_if_branch_chain_catching_label(
                &branches[1..],
                else_branch,
                input,
                bindings,
                context,
                catch_label,
            )? {
                LabelFlow::Continue(values) => out.extend(values),
                LabelFlow::Break(values) => {
                    out.extend(values);
                    return Ok(LabelFlow::Break(out));
                }
            }
        }
    }
    Ok(LabelFlow::Continue(out))
}

fn evaluate_try(
    body: &Expr,
    catch: Option<&Expr>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Expr::Builtin(BuiltinExpr::Repeat(query)) = body {
        return evaluate_repeat_try(query, catch, input, bindings, context);
    }
    if let Expr::Subquery(query) = body {
        return match evaluate_query_preserving_partial(query, input, bindings, context) {
            Ok(frames) => Ok(frames_to_values(frames)),
            Err(PartialEvaluation { partial, error }) => {
                let mut values = frames_to_values(partial);
                match error {
                    AqError::BreakLabel(name) => Err(AqError::BreakLabel(name)),
                    other => match catch {
                        Some(catch) => {
                            let value = other.into_catch_value();
                            if expr_is_identity_path(catch) {
                                values.push(value);
                            } else {
                                values.extend(evaluate_expr(catch, &value, bindings, context)?);
                            }
                            Ok(values)
                        }
                        None => Ok(values),
                    },
                }
            }
        };
    }

    match evaluate_expr(body, input, bindings, context) {
        Ok(values) => Ok(values),
        Err(AqError::BreakLabel(name)) => Err(AqError::BreakLabel(name)),
        Err(error) => match catch {
            Some(catch) => {
                let value = error.into_catch_value();
                if expr_is_identity_path(catch) {
                    Ok(vec![value])
                } else {
                    evaluate_expr(catch, &value, bindings, context)
                }
            }
            None => Ok(Vec::new()),
        },
    }
}

fn evaluate_repeat_try(
    query: &Query,
    catch: Option<&Expr>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut values = Vec::new();
    loop {
        match evaluate_query_preserving_partial(query, input, bindings, context) {
            Ok(results) => values.extend(results.into_iter().map(|frame| frame.value)),
            Err(PartialEvaluation {
                partial,
                error: AqError::BreakLabel(name),
            }) => {
                values.extend(frames_to_values(partial));
                return Err(AqError::BreakLabel(name));
            }
            Err(PartialEvaluation { partial, error }) => {
                values.extend(frames_to_values(partial));
                match catch {
                    Some(catch) => {
                        let value = error.into_catch_value();
                        if expr_is_identity_path(catch) {
                            values.push(value);
                        } else {
                            values.extend(evaluate_expr(catch, &value, bindings, context)?);
                        }
                        return Ok(values);
                    }
                    None => return Ok(values),
                }
            }
        }
    }
}

fn evaluate_try_catching_label(
    body: &Expr,
    catch: Option<&Expr>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    catch_label: &str,
) -> Result<LabelFlow<Vec<Value>>, AqError> {
    match evaluate_expr_catching_label(body, input, bindings, context, catch_label) {
        Ok(values) => Ok(values),
        Err(AqError::BreakLabel(name)) => Err(AqError::BreakLabel(name)),
        Err(error) => match catch {
            Some(catch) => {
                let value = error.into_catch_value();
                if expr_is_identity_path(catch) {
                    Ok(LabelFlow::Continue(vec![value]))
                } else {
                    Ok(LabelFlow::Continue(evaluate_expr(
                        catch, &value, bindings, context,
                    )?))
                }
            }
            None => Ok(LabelFlow::Continue(Vec::new())),
        },
    }
}

fn evaluate_assign(
    path: &Query,
    op: AssignOp,
    value: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let paths = exact_paths_of("assignment", path, input, bindings, context)?;
    if paths.is_empty() {
        return Ok(vec![input.clone()]);
    }

    match op {
        AssignOp::Set => evaluate_set_assign(&paths, value, input, bindings, context),
        AssignOp::Update => evaluate_update_assign(&paths, value, input, bindings, context),
        AssignOp::UpdateWith(op) => {
            evaluate_compound_assign(&paths, op, value, input, bindings, context)
        }
    }
}

fn evaluate_assign_owned(
    path: &Query,
    op: AssignOp,
    value: &Expr,
    input: Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let paths = exact_paths_of("assignment", path, &input, bindings, context)?;
    if paths.is_empty() {
        return Ok(vec![input]);
    }

    match op {
        AssignOp::Set => evaluate_set_assign_owned(&paths, value, input, bindings, context),
        AssignOp::Update => evaluate_update_assign_owned(&paths, value, input, bindings, context),
        AssignOp::UpdateWith(op) => {
            evaluate_compound_assign(&paths, op, value, &input, bindings, context)
        }
    }
}

fn evaluate_set_assign(
    paths: &[Vec<PathComponent>],
    value: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let replacements = evaluate_expr(value, input, bindings, context)?;
    let mut out = Vec::new();
    for replacement in replacements {
        let mut updated = input.clone();
        for path in paths {
            setpath_value_in_place(&mut updated, path, &replacement)?;
        }
        out.push(updated);
    }
    Ok(out)
}

fn evaluate_set_assign_owned(
    paths: &[Vec<PathComponent>],
    value: &Expr,
    input: Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let replacements = evaluate_expr(value, &input, bindings, context)?;
    let replacement_count = replacements.len();
    let mut owned_input = Some(input);
    let mut out = Vec::with_capacity(replacement_count);
    for (index, replacement) in replacements.into_iter().enumerate() {
        let mut updated = if index + 1 == replacement_count {
            owned_input.take().unwrap_or(Value::Null)
        } else {
            owned_input.as_ref().cloned().unwrap_or(Value::Null)
        };
        for path in paths {
            setpath_value_in_place(&mut updated, path, &replacement)?;
        }
        out.push(updated);
    }
    Ok(out)
}

fn evaluate_update_assign(
    paths: &[Vec<PathComponent>],
    value: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut documents = vec![input.clone()];
    let mut ordered_paths = paths.to_vec();
    ordered_paths.sort_by(|left, right| compare_update_paths_desc(left, right));
    for path in &ordered_paths {
        let mut next = Vec::new();
        for document in documents {
            let current = getpath_value(&document, path)?;
            let replacement = evaluate_expr(value, &current, bindings, context)?
                .into_iter()
                .next();
            let Some(replacement) = replacement else {
                next.push(delpaths_value(&document, std::slice::from_ref(path))?);
                continue;
            };
            let mut updated = document.clone();
            setpath_value_in_place(&mut updated, path, &replacement)?;
            next.push(updated);
        }
        documents = next;
    }
    Ok(documents)
}

fn evaluate_update_assign_owned(
    paths: &[Vec<PathComponent>],
    value: &Expr,
    input: Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut documents = vec![input];
    let mut ordered_paths = paths.to_vec();
    ordered_paths.sort_by(|left, right| compare_update_paths_desc(left, right));
    for path in &ordered_paths {
        let mut next = Vec::new();
        for document in documents {
            let current = getpath_value(&document, path)?;
            let replacement = evaluate_expr(value, &current, bindings, context)?
                .into_iter()
                .next();
            let Some(replacement) = replacement else {
                next.push(delpaths_value(&document, std::slice::from_ref(path))?);
                continue;
            };
            let mut updated = document;
            setpath_value_in_place(&mut updated, path, &replacement)?;
            next.push(updated);
        }
        documents = next;
    }
    Ok(documents)
}

fn evaluate_compound_assign(
    paths: &[Vec<PathComponent>],
    op: BinaryOp,
    value: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut documents = vec![input.clone()];
    let mut ordered_paths = paths.to_vec();
    ordered_paths.sort_by(|left, right| compare_update_paths_desc(left, right));
    for path in &ordered_paths {
        let mut next = Vec::new();
        for document in documents {
            let current = getpath_value(&document, path)?;
            let replacements =
                compound_assignment_replacements(op, &current, value, input, bindings, context)?;
            if replacements.is_empty() {
                next.push(delpaths_value(&document, std::slice::from_ref(path))?);
                continue;
            }
            for replacement in replacements {
                let mut updated = document.clone();
                setpath_value_in_place(&mut updated, path, &replacement)?;
                next.push(updated);
            }
        }
        documents = next;
    }
    Ok(documents)
}

fn compare_update_paths_desc(
    left: &[PathComponent],
    right: &[PathComponent],
) -> std::cmp::Ordering {
    for (left_component, right_component) in left.iter().zip(right.iter()) {
        let ordering = compare_update_path_component_desc(left_component, right_component);
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    right.len().cmp(&left.len())
}

fn compare_update_path_component_desc(
    left: &PathComponent,
    right: &PathComponent,
) -> std::cmp::Ordering {
    match (left, right) {
        (PathComponent::Field(_), PathComponent::Field(_)) => std::cmp::Ordering::Equal,
        (PathComponent::Index(left), PathComponent::Index(right)) => right.cmp(left),
        (
            PathComponent::Slice {
                start: left_start,
                end: left_end,
            },
            PathComponent::Slice {
                start: right_start,
                end: right_end,
            },
        ) => right_start.cmp(left_start).then(right_end.cmp(left_end)),
        (left, right) => update_path_component_rank(right).cmp(&update_path_component_rank(left)),
    }
}

fn update_path_component_rank(component: &PathComponent) -> u8 {
    match component {
        PathComponent::Field(_) => 0,
        PathComponent::Index(_) => 1,
        PathComponent::Slice { .. } => 2,
    }
}

fn compound_assignment_replacements(
    op: BinaryOp,
    current: &Value,
    value: &Expr,
    scope_input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    match op {
        BinaryOp::Add => evaluate_expr(value, scope_input, bindings, context)?
            .into_iter()
            .map(|right| value_add(current, &right))
            .collect(),
        BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            evaluate_expr(value, scope_input, bindings, context)?
                .into_iter()
                .map(|right| value_math(current, op, &right))
                .collect()
        }
        BinaryOp::Alt => {
            if is_truthy(current) {
                Ok(vec![current.clone()])
            } else {
                evaluate_expr(value, scope_input, bindings, context)
            }
        }
        _ => Err(AqError::Query(
            "internal error: unsupported compound assignment operator".to_string(),
        )),
    }
}

fn evaluate_object_keys(
    key: &ObjectKey,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<String>, AqError> {
    match key {
        ObjectKey::Static(key) => Ok(vec![key.clone()]),
        ObjectKey::Dynamic(expr) => {
            if let Some(value) = direct_single_value_expr_value(expr, input, bindings) {
                return object_key_string(value).map(|key| vec![key]);
            }
            let values = evaluate_expr(expr, input, bindings, context)?;
            values.into_iter().map(object_key_string).collect()
        }
    }
}

fn object_key_string(value: Value) -> Result<String, AqError> {
    match value {
        Value::String(value) => Ok(value),
        other => Err(object_key_error(&other)),
    }
}

fn object_key_error(value: &Value) -> AqError {
    let (value_type, rendered) = typed_rendered_value(value);
    AqError::Query(format!(
        "Cannot use {value_type} ({rendered}) as object key"
    ))
}

fn undefined_variable_error(name: &str) -> AqError {
    AqError::Query(format!("${name} is not defined"))
}

fn string_expr_object_key(expr: Expr) -> ObjectKey {
    match expr {
        Expr::Literal(Value::String(key)) => ObjectKey::Static(key),
        other => ObjectKey::Dynamic(Box::new(other)),
    }
}

fn string_expr_lookup_segment(expr: Expr, optional: bool) -> Segment {
    match expr {
        Expr::Literal(Value::String(name)) => Segment::Field { name, optional },
        other => Segment::Lookup {
            expr: Box::new(other),
            optional,
        },
    }
}

fn string_expr_lookup_value(expr: &Expr) -> Expr {
    match expr {
        Expr::Literal(Value::String(name)) => Expr::Path(PathExpr {
            segments: vec![Segment::Field {
                name: name.clone(),
                optional: false,
            }],
        }),
        other => Expr::Access {
            base: Box::new(Expr::Path(PathExpr { segments: vec![] })),
            segments: vec![Segment::Lookup {
                expr: Box::new(other.clone()),
                optional: false,
            }],
        },
    }
}

fn evaluate_unary(
    op: UnaryOp,
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let values = evaluate_expr(expr, input, bindings, context)?;
    match op {
        UnaryOp::Not => Ok(values
            .into_iter()
            .map(|value| Value::Bool(!is_truthy(&value)))
            .collect()),
        UnaryOp::Neg => values.into_iter().map(value_neg).collect(),
    }
}

fn value_neg(value: Value) -> Result<Value, AqError> {
    match value {
        Value::Integer(value) => value
            .checked_neg()
            .map(Value::Integer)
            .ok_or_else(|| AqError::Query("integer negation overflow".to_string())),
        Value::Decimal(value) => Ok(Value::Decimal(value.negated())),
        Value::Float(value) => Ok(Value::Float(-value)),
        other => {
            let (value_type, rendered) = typed_rendered_value(&other);
            Err(AqError::Query(format!(
                "{value_type} ({rendered}) cannot be negated"
            )))
        }
    }
}

fn evaluate_binary(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if op == BinaryOp::Alt {
        return evaluate_alt(left, right, input, bindings, context);
    }
    if let Some(result) = try_evaluate_direct_single_value_binary(left, op, right, input, bindings)
    {
        return result;
    }

    let left_values = evaluate_expr(left, input, bindings, context)?;
    match op {
        BinaryOp::Add => evaluate_add(left_values, right, input, bindings, context),
        BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            evaluate_math_binary(left_values, op, right, input, bindings, context)
        }
        BinaryOp::And | BinaryOp::Or => {
            evaluate_boolean_binary(left_values, op, right, input, bindings, context)
        }
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            evaluate_comparison_binary(left_values, op, right, input, bindings, context)
        }
        BinaryOp::Alt => Err(AqError::Query(
            "internal error: alt should have been handled earlier".to_string(),
        )),
    }
}

fn try_evaluate_direct_single_value_binary(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    input: &Value,
    bindings: &Bindings,
) -> Option<Result<Vec<Value>, AqError>> {
    let left = direct_single_value_expr_value(left, input, bindings)?;
    match op {
        BinaryOp::And if !is_truthy(&left) => return Some(Ok(vec![Value::Bool(false)])),
        BinaryOp::Or if is_truthy(&left) => return Some(Ok(vec![Value::Bool(true)])),
        _ => {}
    }

    let right = direct_single_value_expr_value(right, input, bindings)?;
    Some(match op {
        BinaryOp::Add => value_add(&left, &right).map(|value| vec![value]),
        BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            value_math(&left, op, &right).map(|value| vec![value])
        }
        BinaryOp::And | BinaryOp::Or => Ok(vec![Value::Bool(is_truthy(&right))]),
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            apply_binary_op(&left, op, &right).map(|value| vec![Value::Bool(value)])
        }
        BinaryOp::Alt => Err(AqError::Query(
            "internal error: alt should have been handled earlier".to_string(),
        )),
    })
}

fn direct_single_value_expr_value(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
) -> Option<Value> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Variable(name) => bindings
            .get_value(name)
            .cloned()
            .or_else(|| (name == "ENV").then(env_of))
            .or_else(|| (name == "__loc__").then(location_value)),
        Expr::Path(path) => {
            let Ok(mut values) = evaluate_direct_static_path(path, input)? else {
                return None;
            };
            if values.len() == 1 {
                values.pop()
            } else {
                None
            }
        }
        Expr::Subquery(query) => {
            let stage = direct_stage_expr(query)?;
            direct_single_value_expr_value(stage, input, bindings)
        }
        Expr::Unary { op, expr } => {
            let value = direct_single_value_expr_value(expr, input, bindings)?;
            match op {
                UnaryOp::Not => Some(Value::Bool(!is_truthy(&value))),
                UnaryOp::Neg => value_neg(value).ok(),
            }
        }
        Expr::If {
            branches,
            else_branch,
        } => direct_single_value_if_value(branches, else_branch, input, bindings),
        _ => None,
    }
}

fn direct_single_value_if_value(
    branches: &[(Query, Query)],
    else_branch: &Query,
    input: &Value,
    bindings: &Bindings,
) -> Option<Value> {
    for (condition, branch) in branches {
        let condition = direct_single_value_query_value(condition, input, bindings)?;
        if is_truthy(&condition) {
            return direct_single_value_query_value(branch, input, bindings);
        }
    }
    direct_single_value_query_value(else_branch, input, bindings)
}

fn direct_single_value_query_value(
    query: &Query,
    input: &Value,
    bindings: &Bindings,
) -> Option<Value> {
    let stage = direct_stage_expr(query)?;
    direct_single_value_expr_value(stage, input, bindings)
}

fn evaluate_alt(
    left: &Expr,
    right: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(left_value) = direct_single_value_expr_value(left, input, bindings) {
        if is_truthy(&left_value) {
            return Ok(vec![left_value]);
        }
        if let Some(right_value) = direct_single_value_expr_value(right, input, bindings) {
            return Ok(vec![right_value]);
        }
    }

    let left_values = evaluate_expr(left, input, bindings, context)?;
    if left_values.is_empty() {
        return evaluate_expr(right, input, bindings, context);
    }
    if left_values.len() == 1 {
        let mut iter = left_values.into_iter();
        if let Some(value) = iter.next() {
            if is_truthy(&value) {
                return Ok(vec![value]);
            }
        }
        return evaluate_expr(right, input, bindings, context);
    }
    let truthy_values: Vec<Value> = left_values.into_iter().filter(is_truthy).collect();
    if truthy_values.is_empty() {
        evaluate_expr(right, input, bindings, context)
    } else {
        Ok(truthy_values)
    }
}

fn evaluate_add(
    left_values: Vec<Value>,
    right: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let right_values = evaluate_expr(right, input, bindings, context)?;
    let mut out = Vec::new();
    for right in &right_values {
        for left in &left_values {
            out.push(value_add(left, right)?);
        }
    }
    Ok(out)
}

fn evaluate_math_binary(
    left_values: Vec<Value>,
    op: BinaryOp,
    right: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let right_values = evaluate_expr(right, input, bindings, context)?;
    let mut out = Vec::new();
    for right in &right_values {
        for left in &left_values {
            out.push(value_math(left, op, right)?);
        }
    }
    Ok(out)
}

fn evaluate_boolean_binary(
    left_values: Vec<Value>,
    op: BinaryOp,
    right: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    for left in &left_values {
        match op {
            BinaryOp::And if !is_truthy(left) => out.push(Value::Bool(false)),
            BinaryOp::Or if is_truthy(left) => out.push(Value::Bool(true)),
            BinaryOp::And | BinaryOp::Or => {
                let right_values = evaluate_expr(right, input, bindings, context)?;
                for right in &right_values {
                    let value = match op {
                        BinaryOp::And => is_truthy(right),
                        BinaryOp::Or => is_truthy(right),
                        _ => {
                            return Err(AqError::Query(
                                "internal error: expected boolean operator".to_string(),
                            ));
                        }
                    };
                    out.push(Value::Bool(value));
                }
            }
            _ => {
                return Err(AqError::Query(
                    "internal error: expected boolean operator".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

fn evaluate_comparison_binary(
    left_values: Vec<Value>,
    op: BinaryOp,
    right: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let right_values = evaluate_expr(right, input, bindings, context)?;
    let mut out = Vec::new();
    for right in &right_values {
        for left in &left_values {
            out.push(Value::Bool(apply_binary_op(left, op, right)?));
        }
    }
    Ok(out)
}

fn evaluate_access_expr(
    base: &Expr,
    segments: &[Segment],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let values = evaluate_expr(base, input, bindings, context)?;
    apply_segments(segments, values, input, bindings, context)
}

fn apply_segments(
    segments: &[Segment],
    mut current: Vec<Value>,
    scope_input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    for segment in segments {
        current = apply_segment(segment, current, scope_input, bindings, context)?;
    }
    Ok(current)
}

fn evaluate_path_with_bindings(
    path: &PathExpr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(values) = evaluate_direct_static_path(path, input) {
        return values;
    }

    if path_supports_borrowed_value_fast_path(path) {
        return Ok(
            evaluate_exact_path_segments("path", &path.segments, input, bindings, context)?
                .into_iter()
                .map(|frame| frame.value.into_value())
                .collect(),
        );
    }

    apply_segments(
        &path.segments,
        vec![input.clone()],
        input,
        bindings,
        context,
    )
}

fn evaluate_direct_static_path(
    path: &PathExpr,
    input: &Value,
) -> Option<Result<Vec<Value>, AqError>> {
    evaluate_direct_static_path_up_to(path, input, usize::MAX)
}

fn evaluate_direct_static_path_up_to(
    path: &PathExpr,
    input: &Value,
    limit: usize,
) -> Option<Result<Vec<Value>, AqError>> {
    evaluate_direct_static_segments_up_to(&path.segments, input, limit)
}

fn evaluate_direct_static_segments_up_to(
    segments: &[Segment],
    input: &Value,
    limit: usize,
) -> Option<Result<Vec<Value>, AqError>> {
    let mut current = value_ref_for_direct_path(input);

    for (index, segment) in segments.iter().enumerate() {
        let is_last = index + 1 == segments.len();
        match segment {
            Segment::Field { name, optional } => match current {
                Some(value) => match value.untagged() {
                    Value::Object(fields) => {
                        current = fields.get(name).and_then(value_ref_for_direct_path);
                    }
                    other => {
                        if *optional {
                            return Some(Ok(Vec::new()));
                        }
                        return Some(Err(field_access_error(other, name)));
                    }
                },
                None => {
                    current = None;
                }
            },
            Segment::Index { index, optional } => match current {
                Some(value) => match value.untagged() {
                    Value::Array(items) => {
                        current = resolve_index(*index, items.len())
                            .and_then(|resolved| items.get(resolved))
                            .and_then(value_ref_for_direct_path);
                    }
                    other => {
                        if *optional {
                            return Some(Ok(Vec::new()));
                        }
                        return Some(Err(AqError::Query(format!(
                            "cannot index {} with [{}]",
                            kind_name(other),
                            index
                        ))));
                    }
                },
                None => {
                    current = None;
                }
            },
            Segment::Slice {
                start,
                end,
                optional,
            } if is_last => match current {
                Some(value) => match value.untagged() {
                    Value::Array(items) => {
                        let (start, end) = resolve_slice_bounds(*start, *end, items.len());
                        return Some(Ok(vec![Value::Array(items[start..end].to_vec())]));
                    }
                    Value::String(text) => {
                        return Some(Ok(vec![Value::String(slice_string(text, *start, *end))]));
                    }
                    other => {
                        if *optional {
                            return Some(Ok(Vec::new()));
                        }
                        return Some(Err(AqError::Query(format!(
                            "cannot slice {}",
                            kind_name(other)
                        ))));
                    }
                },
                None => return Some(Ok(vec![Value::Null])),
            },
            Segment::Lookup { expr, optional: _ } if is_last => {
                let (start, end) = literal_slice_lookup_bounds_expr(expr)?;
                match current {
                    Some(value) => match value.untagged() {
                        Value::Array(items) => {
                            let (start, end) = resolve_slice_bounds(start, end, items.len());
                            return Some(Ok(vec![Value::Array(items[start..end].to_vec())]));
                        }
                        Value::String(text) => {
                            return Some(Ok(vec![Value::String(slice_string(text, start, end))]));
                        }
                        Value::Null => return Some(Ok(vec![Value::Null])),
                        Value::Object(_) => {
                            return Some(Err(AqError::Query(
                                "cannot index object with object".to_string(),
                            )));
                        }
                        other => {
                            return Some(Err(AqError::Query(format!(
                                "cannot slice {}",
                                kind_name(other)
                            ))));
                        }
                    },
                    None => return Some(Ok(vec![Value::Null])),
                }
            }
            Segment::Iterate { optional } if is_last => match current {
                Some(value) => match value.untagged() {
                    Value::Array(items) => {
                        return Some(Ok(items.iter().take(limit).cloned().collect()));
                    }
                    Value::Object(fields) => {
                        return Some(Ok(fields.values().take(limit).cloned().collect()));
                    }
                    other => {
                        if *optional {
                            return Some(Ok(Vec::new()));
                        }
                        return Some(Err(iterate_error(other)));
                    }
                },
                None => {
                    if *optional {
                        return Some(Ok(Vec::new()));
                    }
                    return Some(Err(iterate_error(&Value::Null)));
                }
            },
            Segment::Lookup { .. } | Segment::Slice { .. } | Segment::Iterate { .. } => {
                return None;
            }
        }
    }

    Some(Ok(vec![current.cloned().unwrap_or(Value::Null)]))
}

fn literal_slice_lookup_bounds_expr(expr: &Expr) -> Option<(Option<isize>, Option<isize>)> {
    let Expr::Object(fields) = expr else {
        return None;
    };
    let mut start = None;
    let mut end = None;
    for (key, value_expr) in fields {
        match key {
            ObjectKey::Static(name) if name == "start" => {
                start = literal_slice_lookup_start_bound_expr(value_expr)?;
            }
            ObjectKey::Static(name) if name == "end" => {
                end = literal_slice_lookup_end_bound_expr(value_expr)?;
            }
            _ => return None,
        }
    }
    Some((start, end))
}

fn literal_slice_lookup_start_bound_expr(expr: &Expr) -> Option<Option<isize>> {
    let value = literal_slice_bound_expr_value(expr)?;
    lookup_slice_start_bound_of(&value)
}

fn literal_slice_lookup_end_bound_expr(expr: &Expr) -> Option<Option<isize>> {
    let value = literal_slice_bound_expr_value(expr)?;
    lookup_slice_end_bound_of(&value)
}

fn literal_slice_bound_expr_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Builtin(BuiltinExpr::Nan) => Some(Value::Float(f64::NAN)),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => value_neg(literal_slice_bound_expr_value(expr)?).ok(),
        Expr::Subquery(query) => literal_slice_bound_query_value(query),
        _ => None,
    }
}

fn literal_slice_bound_query_value(query: &Query) -> Option<Value> {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return None;
    }
    let [pipeline] = query.outputs.as_slice() else {
        return None;
    };
    let [expr] = pipeline.stages.as_slice() else {
        return None;
    };
    literal_slice_bound_expr_value(expr)
}

fn value_ref_for_direct_path(value: &Value) -> Option<&Value> {
    match value.untagged() {
        Value::Null => None,
        _ => Some(value),
    }
}

fn path_supports_borrowed_value_fast_path(path: &PathExpr) -> bool {
    path.segments
        .iter()
        .all(|segment| !matches!(segment, Segment::Lookup { .. }))
}

fn apply_path_segments(
    segments: &[Segment],
    mut current: Vec<PathValueFrame>,
    scope_input: &Value,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    for segment in segments {
        current = apply_path_segment(segment, current, scope_input, context)?;
    }
    Ok(current)
}

fn apply_segment(
    segment: &Segment,
    values: Vec<Value>,
    scope_input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    if let Segment::Lookup { expr, optional } = segment {
        let lookups = evaluate_expr(expr, scope_input, bindings, context)?;
        for value in values {
            for lookup in &lookups {
                apply_lookup(&value, lookup, *optional, &mut out)?;
            }
        }
        return Ok(out);
    }

    for value in values {
        match segment {
            Segment::Field { name, optional } => match value.untagged() {
                Value::Object(fields) => {
                    out.push(fields.get(name).cloned().unwrap_or(Value::Null));
                }
                Value::Null => out.push(Value::Null),
                other => {
                    if !*optional {
                        return Err(field_access_error(other, name));
                    }
                }
            },
            Segment::Lookup { .. } => unreachable!("lookup segments are handled above"),
            Segment::Index { index, optional } => match value.untagged() {
                Value::Array(items) => {
                    let resolved = resolve_index(*index, items.len());
                    out.push(
                        resolved
                            .and_then(|index| items.get(index).cloned())
                            .unwrap_or(Value::Null),
                    );
                }
                Value::Null => out.push(Value::Null),
                other => {
                    if !*optional {
                        return Err(AqError::Query(format!(
                            "cannot index {} with [{}]",
                            kind_name(other),
                            index
                        )));
                    }
                }
            },
            Segment::Slice {
                start,
                end,
                optional,
            } => match value.untagged() {
                Value::Array(items) => {
                    let (start, end) = resolve_slice_bounds(*start, *end, items.len());
                    out.push(Value::Array(items[start..end].to_vec()));
                }
                Value::String(text) => {
                    out.push(Value::String(slice_string(text, *start, *end)));
                }
                Value::Null => out.push(Value::Null),
                other => {
                    if !*optional {
                        return Err(AqError::Query(format!("cannot slice {}", kind_name(other))));
                    }
                }
            },
            Segment::Iterate { optional } => match value.untagged() {
                Value::Array(items) => out.extend(items.iter().cloned()),
                Value::Object(fields) => out.extend(fields.values().cloned()),
                other => {
                    if !*optional {
                        return Err(iterate_error(other));
                    }
                }
            },
        }
    }
    Ok(out)
}

fn apply_path_segment(
    segment: &Segment,
    frames: Vec<PathValueFrame>,
    scope_input: &Value,
    context: &EvaluationContext,
) -> Result<Vec<PathValueFrame>, AqError> {
    let mut out = Vec::new();
    for frame in frames {
        match segment {
            Segment::Field { name, optional } => match frame.value.untagged() {
                Value::Object(fields) => {
                    let mut path = frame.path;
                    path.push(Value::String(name.clone()));
                    out.push(PathValueFrame {
                        value: fields.get(name).cloned().unwrap_or(Value::Null),
                        path,
                        bindings: frame.bindings,
                    });
                }
                Value::Null => {
                    let mut path = frame.path;
                    path.push(Value::String(name.clone()));
                    out.push(PathValueFrame {
                        value: Value::Null,
                        path,
                        bindings: frame.bindings,
                    });
                }
                other => {
                    if !*optional {
                        return Err(field_access_error(other, name));
                    }
                }
            },
            Segment::Lookup { expr, optional } => {
                let lookups = evaluate_expr(expr, scope_input, &frame.bindings, context)?;
                for lookup in lookups {
                    apply_path_lookup(&frame, lookup, *optional, &mut out)?;
                }
            }
            Segment::Index { index, optional } => match frame.value.untagged() {
                Value::Array(items) => {
                    let mut path = frame.path;
                    path.push(Value::Integer(*index as i64));
                    let resolved = resolve_index(*index, items.len());
                    out.push(PathValueFrame {
                        value: resolved
                            .and_then(|index| items.get(index).cloned())
                            .unwrap_or(Value::Null),
                        path,
                        bindings: frame.bindings,
                    });
                }
                Value::Null => {
                    let mut path = frame.path;
                    path.push(Value::Integer(*index as i64));
                    out.push(PathValueFrame {
                        value: Value::Null,
                        path,
                        bindings: frame.bindings,
                    });
                }
                other => {
                    if !*optional {
                        return Err(AqError::Query(format!(
                            "cannot index {} with [{}]",
                            kind_name(other),
                            index
                        )));
                    }
                }
            },
            Segment::Slice {
                start,
                end,
                optional,
            } => match frame.value.untagged() {
                Value::Array(items) => {
                    let mut path = frame.path;
                    path.push(slice_component_value(*start, *end));
                    let (start, end) = resolve_slice_bounds(*start, *end, items.len());
                    out.push(PathValueFrame {
                        value: Value::Array(items[start..end].to_vec()),
                        path,
                        bindings: frame.bindings,
                    });
                }
                Value::String(text) => {
                    let mut path = frame.path;
                    path.push(slice_component_value(*start, *end));
                    out.push(PathValueFrame {
                        value: Value::String(slice_string(text, *start, *end)),
                        path,
                        bindings: frame.bindings,
                    });
                }
                Value::Null => {
                    let mut path = frame.path;
                    path.push(slice_component_value(*start, *end));
                    out.push(PathValueFrame {
                        value: Value::Null,
                        path,
                        bindings: frame.bindings,
                    });
                }
                other => {
                    if !*optional {
                        return Err(AqError::Query(format!("cannot slice {}", kind_name(other))));
                    }
                }
            },
            Segment::Iterate { optional } => match frame.value.untagged() {
                Value::Array(items) => {
                    for (index, value) in items.iter().cloned().enumerate() {
                        let mut path = frame.path.clone();
                        path.push(Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)));
                        out.push(PathValueFrame {
                            value,
                            path,
                            bindings: frame.bindings.clone(),
                        });
                    }
                }
                Value::Object(fields) => {
                    for (key, value) in fields {
                        let mut path = frame.path.clone();
                        path.push(Value::String(key.clone()));
                        out.push(PathValueFrame {
                            value: value.clone(),
                            path,
                            bindings: frame.bindings.clone(),
                        });
                    }
                }
                other => {
                    if !*optional {
                        return Err(iterate_error(other));
                    }
                }
            },
        }
    }
    Ok(out)
}

fn apply_lookup(
    value: &Value,
    lookup: &Value,
    optional: bool,
    out: &mut Vec<Value>,
) -> Result<(), AqError> {
    if let Some((start, end)) = lookup_slice_bounds(lookup) {
        match value.untagged() {
            Value::Array(items) => {
                let (start, end) = resolve_slice_bounds(start, end, items.len());
                out.push(Value::Array(items[start..end].to_vec()));
                return Ok(());
            }
            Value::String(text) => {
                out.push(Value::String(slice_string(text, start, end)));
                return Ok(());
            }
            Value::Null => {
                out.push(Value::Null);
                return Ok(());
            }
            Value::Object(_) => {
                return Err(AqError::Query(
                    "cannot index object with object".to_string(),
                ));
            }
            other => {
                return Err(AqError::Query(format!("cannot slice {}", kind_name(other))));
            }
        }
    }

    match (value.untagged(), lookup.untagged()) {
        (Value::Object(fields), Value::String(key)) => {
            out.push(fields.get(key).cloned().unwrap_or(Value::Null));
            Ok(())
        }
        (Value::Array(items), Value::Integer(index)) => {
            let resolved = resolve_index(*index as isize, items.len());
            out.push(
                resolved
                    .and_then(|index| items.get(index).cloned())
                    .unwrap_or(Value::Null),
            );
            Ok(())
        }
        (Value::Array(items), Value::Decimal(index)) => {
            let resolved = index
                .to_f64_lossy()
                .trunc()
                .to_string()
                .parse::<isize>()
                .ok()
                .and_then(|index| resolve_index(index, items.len()));
            out.push(
                resolved
                    .and_then(|index| items.get(index).cloned())
                    .unwrap_or(Value::Null),
            );
            Ok(())
        }
        (Value::Array(items), Value::Float(index)) if index.is_finite() => {
            let resolved = resolve_index(index.trunc() as isize, items.len());
            out.push(
                resolved
                    .and_then(|index| items.get(index).cloned())
                    .unwrap_or(Value::Null),
            );
            Ok(())
        }
        (Value::Array(_), Value::Float(index)) if !index.is_finite() => {
            out.push(Value::Null);
            Ok(())
        }
        (
            Value::Null,
            Value::String(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_),
        ) => {
            out.push(Value::Null);
            Ok(())
        }
        (Value::Object(_), other) => Err(index_lookup_error(value, other)),
        (Value::Array(_), other) => Err(index_lookup_error(value, other)),
        (Value::Null, _other) if optional => Ok(()),
        (Value::Null, other) => Err(index_lookup_error(value, other)),
        (_other, _) if optional => Ok(()),
        (other, lookup) => Err(index_lookup_error(other, lookup)),
    }
}

fn apply_path_lookup(
    frame: &PathValueFrame,
    lookup: Value,
    optional: bool,
    out: &mut Vec<PathValueFrame>,
) -> Result<(), AqError> {
    if let Some((start, end)) = lookup_slice_bounds(&lookup) {
        match frame.value.untagged() {
            Value::Array(items) => {
                let mut path = frame.path.clone();
                path.push(slice_component_value(start, end));
                let (start, end) = resolve_slice_bounds(start, end, items.len());
                out.push(PathValueFrame {
                    value: Value::Array(items[start..end].to_vec()),
                    path,
                    bindings: frame.bindings.clone(),
                });
                return Ok(());
            }
            Value::String(text) => {
                let mut path = frame.path.clone();
                path.push(slice_component_value(start, end));
                out.push(PathValueFrame {
                    value: Value::String(slice_string(text, start, end)),
                    path,
                    bindings: frame.bindings.clone(),
                });
                return Ok(());
            }
            Value::Null => {
                let mut path = frame.path.clone();
                path.push(slice_component_value(start, end));
                out.push(PathValueFrame {
                    value: Value::Null,
                    path,
                    bindings: frame.bindings.clone(),
                });
                return Ok(());
            }
            Value::Object(_) => {
                return Err(AqError::Query(
                    "cannot index object with object".to_string(),
                ));
            }
            other => {
                return Err(AqError::Query(format!("cannot slice {}", kind_name(other))));
            }
        }
    }

    match (frame.value.untagged(), lookup.untagged()) {
        (Value::Object(fields), Value::String(key)) => {
            let mut path = frame.path.clone();
            path.push(lookup.untagged().clone());
            out.push(PathValueFrame {
                value: fields.get(key).cloned().unwrap_or(Value::Null),
                path,
                bindings: frame.bindings.clone(),
            });
            Ok(())
        }
        (Value::Array(items), Value::Integer(index)) => {
            let mut path = frame.path.clone();
            path.push(lookup.untagged().clone());
            let resolved = resolve_index(*index as isize, items.len());
            out.push(PathValueFrame {
                value: resolved
                    .and_then(|index| items.get(index).cloned())
                    .unwrap_or(Value::Null),
                path,
                bindings: frame.bindings.clone(),
            });
            Ok(())
        }
        (Value::Array(items), Value::Decimal(index)) => {
            let mut path = frame.path.clone();
            path.push(lookup.untagged().clone());
            let resolved = index
                .to_f64_lossy()
                .trunc()
                .to_string()
                .parse::<isize>()
                .ok()
                .and_then(|index| resolve_index(index, items.len()));
            out.push(PathValueFrame {
                value: resolved
                    .and_then(|index| items.get(index).cloned())
                    .unwrap_or(Value::Null),
                path,
                bindings: frame.bindings.clone(),
            });
            Ok(())
        }
        (Value::Array(items), Value::Float(index)) if index.is_finite() => {
            let mut path = frame.path.clone();
            path.push(lookup.untagged().clone());
            let resolved = resolve_index(index.trunc() as isize, items.len());
            out.push(PathValueFrame {
                value: resolved
                    .and_then(|index| items.get(index).cloned())
                    .unwrap_or(Value::Null),
                path,
                bindings: frame.bindings.clone(),
            });
            Ok(())
        }
        (Value::Array(_), Value::Float(index)) if !index.is_finite() => {
            let mut path = frame.path.clone();
            path.push(lookup.untagged().clone());
            out.push(PathValueFrame {
                value: Value::Null,
                path,
                bindings: frame.bindings.clone(),
            });
            Ok(())
        }
        (
            Value::Null,
            Value::String(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_),
        ) => {
            let mut path = frame.path.clone();
            path.push(lookup.untagged().clone());
            out.push(PathValueFrame {
                value: Value::Null,
                path,
                bindings: frame.bindings.clone(),
            });
            Ok(())
        }
        (Value::Object(_), other) => Err(index_lookup_error(&frame.value, other)),
        (Value::Array(_), other) => Err(index_lookup_error(&frame.value, other)),
        (Value::Null, _other) if optional => Ok(()),
        (Value::Null, other) => Err(index_lookup_error(&frame.value, other)),
        (_other, _) if optional => Ok(()),
        (other, lookup) => Err(index_lookup_error(other, lookup)),
    }
}

fn slice_component_value(start: Option<isize>, end: Option<isize>) -> Value {
    let mut component = IndexMap::new();
    if let Some(start) = start {
        component.insert("start".to_string(), Value::Integer(start as i64));
    }
    if let Some(end) = end {
        component.insert("end".to_string(), Value::Integer(end as i64));
    }
    Value::Object(component)
}

fn index_lookup_error(base: &Value, lookup: &Value) -> AqError {
    let rendered = match lookup.untagged() {
        Value::String(value) => {
            serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
        }
        _ => typed_rendered_value(lookup).1,
    };
    AqError::Query(format!(
        "Cannot index {} with {} ({rendered})",
        index_kind_name(base),
        index_kind_name(lookup)
    ))
}

fn index_lookup_kind_error(base: &Value, lookup: &Value) -> AqError {
    AqError::Query(format!(
        "Cannot index {} with {}",
        index_kind_name(base),
        index_kind_name(lookup)
    ))
}

fn lookup_slice_bounds(lookup: &Value) -> Option<(Option<isize>, Option<isize>)> {
    let Value::Object(fields) = lookup.untagged() else {
        return None;
    };
    if fields.keys().any(|key| key != "start" && key != "end") {
        return None;
    }

    let start = match fields.get("start") {
        Some(value) => lookup_slice_start_bound_of(value)?,
        None => None,
    };
    let end = match fields.get("end") {
        Some(value) => lookup_slice_end_bound_of(value)?,
        None => None,
    };
    Some((start, end))
}

fn lookup_slice_start_bound_of(value: &Value) -> Option<Option<isize>> {
    match value.untagged() {
        Value::Integer(value) => isize::try_from(*value).ok().map(Some),
        Value::Decimal(value) => {
            let floored = value.to_f64_lossy().floor();
            if floored < isize::MIN as f64 || floored > isize::MAX as f64 {
                return None;
            }
            Some(Some(floored as isize))
        }
        Value::Float(value) if value.is_finite() => {
            let floored = value.floor();
            if floored < isize::MIN as f64 || floored > isize::MAX as f64 {
                return None;
            }
            Some(Some(floored as isize))
        }
        Value::Float(_) => Some(None),
        _ => None,
    }
}

fn lookup_slice_end_bound_of(value: &Value) -> Option<Option<isize>> {
    match value.untagged() {
        Value::Integer(value) => isize::try_from(*value).ok().map(Some),
        Value::Decimal(value) => {
            let ceiled = value.to_f64_lossy().ceil();
            if ceiled < isize::MIN as f64 || ceiled > isize::MAX as f64 {
                return None;
            }
            Some(Some(ceiled as isize))
        }
        Value::Float(value) if value.is_finite() => {
            let ceiled = value.ceil();
            if ceiled < isize::MIN as f64 || ceiled > isize::MAX as f64 {
                return None;
            }
            Some(Some(ceiled as isize))
        }
        Value::Float(_) => Some(None),
        _ => None,
    }
}

fn bind_pattern(
    pattern: &BindingPattern,
    value: &Value,
    bindings: &mut Bindings,
    context: &EvaluationContext,
) -> Result<(), AqError> {
    if let Some(result) = try_bind_simple_object_pattern(pattern, value, bindings) {
        return result;
    }

    match pattern {
        BindingPattern::Variable(name) => {
            bindings.insert_value(name.clone(), value.clone());
            Ok(())
        }
        BindingPattern::Array(patterns) => match value {
            Value::Array(items) => {
                for (index, pattern) in patterns.iter().enumerate() {
                    let item = items.get(index).unwrap_or(&Value::Null);
                    bind_pattern(pattern, item, bindings, context)?;
                }
                Ok(())
            }
            Value::Null => {
                for pattern in patterns {
                    bind_pattern(pattern, &Value::Null, bindings, context)?;
                }
                Ok(())
            }
            other => Err(AqError::Query(format!(
                "cannot index {} with number",
                kind_name(other)
            ))),
        },
        BindingPattern::Object(fields) => match value {
            Value::Object(object) => {
                for field in fields {
                    match &field.key {
                        ObjectKey::Static(key) => {
                            let field_value = object.get(key).unwrap_or(&Value::Null);
                            bind_object_field_pattern(
                                &field.pattern,
                                field.bind_name.as_deref(),
                                field_value,
                                bindings,
                                context,
                            )?;
                        }
                        ObjectKey::Dynamic(_) => {
                            for key in evaluate_object_keys(&field.key, value, bindings, context)? {
                                let field_value = object.get(&key).unwrap_or(&Value::Null);
                                bind_object_field_pattern(
                                    &field.pattern,
                                    field.bind_name.as_deref(),
                                    field_value,
                                    bindings,
                                    context,
                                )?;
                            }
                        }
                    }
                }
                Ok(())
            }
            Value::Null => {
                for field in fields {
                    match &field.key {
                        ObjectKey::Static(_) => {
                            bind_object_field_pattern(
                                &field.pattern,
                                field.bind_name.as_deref(),
                                &Value::Null,
                                bindings,
                                context,
                            )?;
                        }
                        ObjectKey::Dynamic(_) => {
                            for _ in evaluate_object_keys(&field.key, value, bindings, context)? {
                                bind_object_field_pattern(
                                    &field.pattern,
                                    field.bind_name.as_deref(),
                                    &Value::Null,
                                    bindings,
                                    context,
                                )?;
                            }
                        }
                    }
                }
                Ok(())
            }
            other => Err(AqError::Query(format!(
                "cannot index {} with string",
                kind_name(other)
            ))),
        },
    }
}

fn try_bind_simple_object_pattern(
    pattern: &BindingPattern,
    value: &Value,
    bindings: &mut Bindings,
) -> Option<Result<(), AqError>> {
    let BindingPattern::Object(fields) = pattern else {
        return None;
    };

    let simple_fields = fields
        .iter()
        .map(simple_object_binding_target)
        .collect::<Option<Vec<_>>>()?;

    Some(match value {
        Value::Object(object) => {
            for (key, name) in &simple_fields {
                let field_value = object.get(*key).cloned().unwrap_or(Value::Null);
                bindings.insert_value((*name).to_string(), field_value);
            }
            Ok(())
        }
        Value::Null => {
            for (_, name) in &simple_fields {
                bindings.insert_value((*name).to_string(), Value::Null);
            }
            Ok(())
        }
        other => Err(AqError::Query(format!(
            "cannot index {} with string",
            kind_name(other)
        ))),
    })
}

fn simple_object_binding_target(field: &ObjectBindingField) -> Option<(&str, &str)> {
    let ObjectKey::Static(key) = &field.key else {
        return None;
    };
    let BindingPattern::Variable(name) = &field.pattern else {
        return None;
    };
    if let Some(bind_name) = field.bind_name.as_deref() {
        if bind_name != name {
            return None;
        }
    }
    Some((key.as_str(), name.as_str()))
}

fn bind_object_field_pattern(
    pattern: &BindingPattern,
    bind_name: Option<&str>,
    field_value: &Value,
    bindings: &mut Bindings,
    context: &EvaluationContext,
) -> Result<(), AqError> {
    if let Some(name) = bind_name {
        bindings.insert_value(name.to_string(), field_value.clone());
        if matches!(pattern, BindingPattern::Variable(pattern_name) if pattern_name == name) {
            return Ok(());
        }
    }
    bind_pattern(pattern, field_value, bindings, context)
}

fn resolve_index(index: isize, len: usize) -> Option<usize> {
    if index >= 0 {
        usize::try_from(index).ok().filter(|index| *index < len)
    } else {
        let len = isize::try_from(len).ok()?;
        let resolved = len + index;
        usize::try_from(resolved)
            .ok()
            .filter(|index| *index < len as usize)
    }
}

fn resolve_slice_bounds(start: Option<isize>, end: Option<isize>, len: usize) -> (usize, usize) {
    let len_isize = isize::try_from(len).unwrap_or(isize::MAX);
    let start = resolve_slice_start(start, len_isize).clamp(0, len_isize);
    let end = resolve_slice_end(end, len_isize).clamp(0, len_isize);
    let start = usize::try_from(start).unwrap_or(0);
    let end = usize::try_from(end).unwrap_or(len);
    if end < start {
        (start, start)
    } else {
        (start, end)
    }
}

fn slice_string(value: &str, start: Option<isize>, end: Option<isize>) -> String {
    let chars: Vec<char> = value.chars().collect();
    let (start, end) = resolve_slice_bounds(start, end, chars.len());
    chars[start..end].iter().collect()
}

fn resolve_slice_start(bound: Option<isize>, len: isize) -> isize {
    match bound {
        Some(value) if value < 0 => len + value,
        Some(value) => value,
        None => 0,
    }
}

fn resolve_slice_end(bound: Option<isize>, len: isize) -> isize {
    match bound {
        Some(value) if value < 0 => len + value,
        Some(value) => value,
        None => len,
    }
}

fn kind_name(value: &Value) -> &'static str {
    match value.untagged() {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Integer(_) => "integer",
        Value::Decimal(_) => "number",
        Value::Float(_) => "float",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::Bytes(_) => "bytes",
        Value::DateTime(_) => "datetime",
        Value::Date(_) => "date",
        Value::Tagged { .. } => unreachable!("untagged values should not be tagged"),
    }
}

fn index_kind_name(value: &Value) -> &'static str {
    match value.untagged() {
        Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => "number",
        other => kind_name(other),
    }
}

fn field_access_error(value: &Value, name: &str) -> AqError {
    let rendered_name = serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""));
    AqError::Query(format!(
        "Cannot index {} with string ({rendered_name})",
        index_kind_name(value)
    ))
}

fn iterate_error(value: &Value) -> AqError {
    let (value_type, rendered) = typed_rendered_value(value);
    AqError::Query(format!("Cannot iterate over {value_type} ({rendered})"))
}

fn location_value() -> Value {
    let mut fields = IndexMap::new();
    fields.insert("file".to_string(), Value::String("<top-level>".to_string()));
    fields.insert("line".to_string(), Value::Integer(1));
    Value::Object(fields)
}

fn length_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Null => Ok(Value::Integer(0)),
        Value::String(value) => Ok(Value::Integer(
            i64::try_from(value.chars().count()).unwrap_or(i64::MAX),
        )),
        Value::Array(values) => Ok(Value::Integer(
            i64::try_from(values.len()).unwrap_or(i64::MAX),
        )),
        Value::Object(values) => Ok(Value::Integer(
            i64::try_from(values.len()).unwrap_or(i64::MAX),
        )),
        Value::Decimal(value) => Ok(Value::Decimal(value.abs())),
        Value::Integer(value) => match value.checked_abs() {
            Some(value) => Ok(Value::Integer(value)),
            None => Ok(Value::Float((*value as f64).abs())),
        },
        Value::Float(value) => Ok(normalize_number_value(value.abs())),
        other => Err(AqError::Query(format!(
            "length is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn utf8_byte_length_of(value: &Value) -> Result<i64, AqError> {
    match value.untagged() {
        Value::String(value) => Ok(i64::try_from(value.len()).unwrap_or(i64::MAX)),
        other => {
            let (value_type, rendered) = typed_rendered_value(other);
            Err(AqError::Query(format!(
                "{value_type} ({rendered}) only strings have UTF-8 byte length"
            )))
        }
    }
}

fn keys_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Object(values) => {
            let mut keys: Vec<String> = values.keys().cloned().collect();
            keys.sort();
            Ok(Value::Array(keys.into_iter().map(Value::String).collect()))
        }
        Value::Array(values) => {
            let mut keys = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                keys.push(Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)));
            }
            Ok(Value::Array(keys))
        }
        other => Err(AqError::Query(format!(
            "keys is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn keys_unsorted_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Object(values) => Ok(Value::Array(
            values.keys().cloned().map(Value::String).collect(),
        )),
        Value::Array(values) => {
            let mut keys = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                keys.push(Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)));
            }
            Ok(Value::Array(keys))
        }
        other => Err(AqError::Query(format!(
            "keys_unsorted is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn add_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Array(values) => {
            if let Some(result) = add_integer_array_fast_path(values)? {
                return Ok(result);
            }
            let mut iter = values.iter();
            let Some(first) = iter.next() else {
                return Ok(Value::Null);
            };

            let mut total = first.clone();
            for value in iter {
                total = value_add(&total, value)?;
            }
            Ok(total)
        }
        other => Err(AqError::Query(format!(
            "add is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn add_integer_array_fast_path(values: &[Value]) -> Result<Option<Value>, AqError> {
    let Some(first) = values.first() else {
        return Ok(None);
    };
    if !matches!(first.untagged(), Value::Integer(value) if integer_is_safe_in_f64(*value)) {
        return Ok(None);
    }

    let mut total = 0i64;
    for value in values {
        let Value::Integer(value) = value.untagged() else {
            return Ok(None);
        };
        if !integer_is_safe_in_f64(*value) {
            return Ok(None);
        }
        total = total
            .checked_add(*value)
            .ok_or_else(|| AqError::Query("integer addition overflow".to_string()))?;
    }
    Ok(Some(Value::Integer(total)))
}

fn add_query_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut values = evaluate_query_values(query, input, bindings, context)?.into_iter();
    let Some(mut total) = values.next() else {
        return Ok(Value::Null);
    };
    for value in values {
        total = value_add(&total, &value)?;
    }
    Ok(total)
}

fn avg_of(value: &Value) -> Result<Value, AqError> {
    let numbers = numeric_array_values("avg", value)?;
    if numbers.is_empty() {
        return Ok(Value::Null);
    }
    normalize_math_result(numbers.iter().sum::<f64>() / numbers.len() as f64)
}

fn median_of(value: &Value) -> Result<Value, AqError> {
    let mut numbers = numeric_array_values("median", value)?;
    if numbers.is_empty() {
        return Ok(Value::Null);
    }
    numbers.sort_by(f64::total_cmp);

    let middle = numbers.len() / 2;
    if numbers.len() % 2 == 1 {
        normalize_math_result(numbers[middle])
    } else {
        normalize_math_result((numbers[middle - 1] + numbers[middle]) / 2.0)
    }
}

fn stddev_of(value: &Value) -> Result<Value, AqError> {
    let numbers = numeric_array_values("stddev", value)?;
    if numbers.is_empty() {
        return Ok(Value::Null);
    }

    let mean = numbers.iter().sum::<f64>() / numbers.len() as f64;
    let variance = numbers
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / numbers.len() as f64;
    normalize_math_result(variance.sqrt())
}

fn percentile_of(
    input: &Value,
    percentile: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut numbers = numeric_array_values("percentile", input)?;
    if numbers.is_empty() {
        return Ok(Value::Null);
    }

    let percentile = evaluate_percentile_arg(percentile, input, bindings, context)?;
    numbers.sort_by(f64::total_cmp);
    let rank = (percentile / 100.0) * (numbers.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        normalize_math_result(numbers[lower])
    } else {
        let weight = rank - lower as f64;
        let value = numbers[lower] * (1.0 - weight) + numbers[upper] * weight;
        normalize_math_result(value)
    }
}

fn histogram_of(
    input: &Value,
    bins: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let numbers = numeric_array_values("histogram", input)?;
    if numbers.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    if numbers.iter().any(|value| !value.is_finite()) {
        return Err(AqError::Query(
            "histogram requires finite numeric values".to_string(),
        ));
    }

    let bins = evaluate_positive_count_arg("histogram", bins, input, bindings, "bins", context)?;
    let min = numbers
        .iter()
        .copied()
        .min_by(f64::total_cmp)
        .unwrap_or(0.0);
    let max = numbers
        .iter()
        .copied()
        .max_by(f64::total_cmp)
        .unwrap_or(0.0);

    if min.total_cmp(&max).is_eq() {
        return Ok(Value::Array(vec![histogram_bucket_value(
            min,
            max,
            numbers.len(),
        )]));
    }

    let width = (max - min) / bins as f64;
    if !width.is_finite() || width <= 0.0 {
        return Ok(Value::Array(vec![histogram_bucket_value(
            min,
            max,
            numbers.len(),
        )]));
    }

    let mut counts = vec![0usize; bins];
    for value in numbers {
        let index = if value.total_cmp(&max).is_eq() {
            bins - 1
        } else {
            let raw_index = ((value - min) / width).floor();
            if raw_index <= 0.0 {
                0
            } else if raw_index >= (bins - 1) as f64 {
                bins - 1
            } else {
                raw_index as usize
            }
        };
        counts[index] += 1;
    }

    let mut buckets = Vec::with_capacity(bins);
    for (index, count) in counts.into_iter().enumerate() {
        let start = min + width * index as f64;
        let end = if index + 1 == bins {
            max
        } else {
            min + width * (index + 1) as f64
        };
        buckets.push(histogram_bucket_value(start, end, count));
    }
    Ok(Value::Array(buckets))
}

fn builtins_of() -> Value {
    Value::Array(
        SUPPORTED_BUILTINS
            .iter()
            .map(|name| Value::String((*name).to_string()))
            .collect(),
    )
}

fn module_meta_of(input: &Value, options: &ParseOptions) -> Result<Value, AqError> {
    let module_name = match input.untagged() {
        Value::String(value) => value,
        other => {
            return Err(AqError::Query(format!(
                "modulemeta input must be a string, got {}",
                kind_name(other)
            )))
        }
    };
    let Some(module_dir) = options.module_dir.as_ref() else {
        return Err(AqError::Query(
            "modulemeta is unavailable without a base directory".to_string(),
        ));
    };
    let library_paths = effective_module_library_paths(module_dir, &options.library_paths);
    let loader = Rc::new(RefCell::new(ModuleLoader::default()));
    let path = resolve_module_path(module_dir, &library_paths, module_name, None, "jq")?;
    let query = load_module_query(&loader, path, &library_paths)?;
    Ok(query
        .module_info
        .unwrap_or(ModuleInfo {
            metadata: IndexMap::new(),
            deps: Vec::new(),
            defs: Vec::new(),
        })
        .to_value())
}

fn env_of() -> Value {
    let mut values = IndexMap::new();
    for (key, value) in std::env::vars() {
        values.insert(key, Value::String(value));
    }
    Value::Object(values)
}

fn debug_of(
    input: &Value,
    expr: Option<&Expr>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let payload = match expr {
        Some(expr) => evaluate_single_argument_value("debug", expr, input, bindings, context)?,
        None => input.clone(),
    };
    let payload_json = payload.to_json()?;
    let rendered = serde_json::to_string(&serde_json::Value::Array(vec![
        serde_json::Value::String("DEBUG:".to_string()),
        payload_json,
    ]))
    .map_err(|error| AqError::message(format!("failed to render debug payload: {error}")))?;
    let mut stderr = std::io::stderr();
    stderr
        .write_all(rendered.as_bytes())
        .map_err(|error| AqError::io(None, error))?;
    stderr
        .write_all(b"\n")
        .map_err(|error| AqError::io(None, error))?;
    Ok(vec![input.clone()])
}

fn error_of(
    input: &Value,
    expr: Option<&Expr>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let value = match expr {
        Some(expr) => evaluate_single_argument_value("error", expr, input, bindings, context)?,
        None => input.clone(),
    };
    Err(AqError::Thrown(value))
}

fn has_of(
    input: &Value,
    key_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    if let Some(key) = literal_expr_value(key_expr) {
        return value_has(input, key);
    }
    let keys = evaluate_expr(key_expr, input, bindings, context)?;
    let mut matched = false;
    for key in keys {
        matched = true;
        if !value_has(input, &key)? {
            return Ok(false);
        }
    }
    if matched {
        Ok(true)
    } else {
        Ok(false)
    }
}

fn in_of(
    input: &Value,
    container_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    if let Some(container) = literal_expr_value(container_expr) {
        return value_has(container, input);
    }
    let containers = evaluate_expr(container_expr, input, bindings, context)?;
    let mut matched = false;
    for container in containers {
        matched = true;
        if !value_has(&container, input)? {
            return Ok(false);
        }
    }
    if matched {
        Ok(true)
    } else {
        Ok(false)
    }
}

fn in_query_of(
    input: &Value,
    stream_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    Ok(evaluate_query(stream_query, input, bindings, context)?
        .into_iter()
        .any(|frame| frame.value == *input))
}

fn in_source_of(
    input: &Value,
    source_query: &Query,
    stream_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    let source_values: Vec<_> = evaluate_query(source_query, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect();
    if source_values.is_empty() {
        return Ok(false);
    }

    let stream_values: Vec<_> = evaluate_query(stream_query, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
        .collect();
    Ok(source_values
        .into_iter()
        .any(|source| stream_values.contains(&source)))
}

fn contains_of(
    input: &Value,
    expected_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    if let Some(expected) = literal_expr_value(expected_expr) {
        return contains_value(input, expected);
    }
    let expected_values = evaluate_expr(expected_expr, input, bindings, context)?;
    let mut matched = false;
    for expected in expected_values {
        matched = true;
        if !contains_value(input, &expected)? {
            return Ok(false);
        }
    }
    if matched {
        Ok(true)
    } else {
        Ok(false)
    }
}

fn first_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Array(values) => Ok(values.first().cloned().unwrap_or(Value::Null)),
        other => Err(AqError::Query(format!(
            "first is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn last_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Array(values) => Ok(values.last().cloned().unwrap_or(Value::Null)),
        other => Err(AqError::Query(format!(
            "last is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn reverse_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Array(values) => {
            let mut reversed = values.clone();
            reversed.reverse();
            Ok(Value::Array(reversed))
        }
        Value::String(value) => Ok(Value::String(value.chars().rev().collect())),
        other => Err(AqError::Query(format!(
            "reverse is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn transpose_of(value: &Value) -> Result<Value, AqError> {
    let rows = expect_array_input("transpose", value)?;
    let mut width = 0usize;
    for row in rows {
        match row.untagged() {
            Value::Array(values) => {
                width = width.max(values.len());
            }
            Value::Null => {}
            other => {
                return Err(AqError::Query(format!(
                    "cannot index {} with number",
                    kind_name(other)
                )));
            }
        }
    }

    let mut out = Vec::with_capacity(width);
    for column in 0..width {
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            match row.untagged() {
                Value::Array(items) => {
                    values.push(items.get(column).cloned().unwrap_or(Value::Null));
                }
                Value::Null => values.push(Value::Null),
                other => {
                    return Err(AqError::Query(format!(
                        "cannot index {} with number",
                        kind_name(other)
                    )));
                }
            }
        }
        out.push(Value::Array(values));
    }

    Ok(Value::Array(out))
}

fn sort_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Array(values) => {
            let mut sorted = values.clone();
            sorted.sort_by(compare_sort_values);
            Ok(Value::Array(sorted))
        }
        other => Err(AqError::Query(format!(
            "sort is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn min_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Array(values) => Ok(values
            .iter()
            .min_by(|left, right| compare_sort_values(left, right))
            .cloned()
            .unwrap_or(Value::Null)),
        other => Err(AqError::Query(format!(
            "min is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn max_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Array(values) => Ok(values
            .iter()
            .max_by(|left, right| compare_sort_values(left, right))
            .cloned()
            .unwrap_or(Value::Null)),
        other => Err(AqError::Query(format!(
            "max is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn unique_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::Array(values) => {
            let mut unique = values.clone();
            unique.sort_by(compare_sort_values);
            unique.dedup();
            Ok(Value::Array(unique))
        }
        other => Err(AqError::Query(format!(
            "unique is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn flatten_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::Array(values) => {
            let mut flattened = Vec::new();
            for value in values {
                flatten_value(value, &mut flattened);
            }
            Ok(Value::Array(flattened))
        }
        other => Err(AqError::Query(format!(
            "flatten is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn floor_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::Integer(_) => Ok(value.clone()),
        Value::Decimal(value) => Ok(normalize_number_value(value.to_f64_lossy().floor())),
        Value::Float(value) => Ok(normalize_number_value(value.floor())),
        other => Err(AqError::Query(format!(
            "floor is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn ceil_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::Integer(_) => Ok(value.clone()),
        Value::Decimal(value) => Ok(normalize_number_value(value.to_f64_lossy().ceil())),
        Value::Float(value) => Ok(normalize_number_value(value.ceil())),
        other => Err(AqError::Query(format!(
            "ceil is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn round_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::Integer(_) => Ok(value.clone()),
        Value::Decimal(value) => Ok(normalize_number_value(value.to_f64_lossy().round())),
        Value::Float(value) => Ok(normalize_number_value(value.round())),
        other => Err(AqError::Query(format!(
            "round is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn fabs_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::Decimal(value) => Ok(Value::Decimal(value.abs())),
        Value::Integer(value) => match value.checked_abs() {
            Some(value) => Ok(Value::Integer(value)),
            None => Ok(Value::Float((*value as f64).abs())),
        },
        Value::Float(value) => Ok(normalize_number_value(value.abs())),
        other => Err(AqError::Query(format!(
            "fabs is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn abs_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::String(_) => Ok(value.clone()),
        Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => fabs_of(value),
        other => Err(AqError::Query(format!(
            "abs is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn sqrt_of(value: &Value) -> Result<Value, AqError> {
    let value = match value {
        Value::Integer(value) => {
            if *value < 0 {
                return Ok(Value::Null);
            }
            let sqrt = (*value as f64).sqrt();
            return Ok(if sqrt.fract() == 0.0 && sqrt <= i64::MAX as f64 {
                Value::Integer(sqrt as i64)
            } else {
                normalize_number_value(sqrt)
            });
        }
        Value::Decimal(value) => value.to_f64_lossy(),
        Value::Float(value) => *value,
        other => {
            return Err(AqError::Query(format!(
                "sqrt is not defined for {}",
                kind_name(other)
            )));
        }
    };
    if value < 0.0 {
        return Ok(Value::Null);
    }
    Ok(normalize_number_value(value.sqrt()))
}

fn log_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("log", value, f64::ln)
}

fn log2_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("log2", value, f64::log2)
}

fn log10_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("log10", value, f64::log10)
}

fn exp_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("exp", value, f64::exp)
}

fn exp2_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("exp2", value, f64::exp2)
}

fn sin_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("sin", value, f64::sin)
}

fn cos_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("cos", value, f64::cos)
}

fn tan_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("tan", value, f64::tan)
}

fn asin_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("asin", value, f64::asin)
}

fn acos_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("acos", value, f64::acos)
}

fn atan_of(value: &Value) -> Result<Value, AqError> {
    unary_math_of("atan", value, f64::atan)
}

fn pow_of(
    base: &Query,
    exponent: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let base = evaluate_single_query_number("pow", base, input, bindings, context)?;
    let exponent = evaluate_single_query_number("pow", exponent, input, bindings, context)?;
    normalize_math_result(base.powf(exponent))
}

fn now_of() -> Result<Value, AqError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AqError::message(format!("system clock error: {error}")))?;
    Ok(Value::Float(elapsed.as_secs_f64()))
}

fn todate_of(value: &Value) -> Result<Value, AqError> {
    let seconds = numeric_input_value("todate", value)?.trunc();
    if !seconds.is_finite() || seconds < i64::MIN as f64 || seconds > i64::MAX as f64 {
        return Err(AqError::Query("todate input is out of range".to_string()));
    }
    let seconds = seconds as i64;
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
        .ok_or_else(|| AqError::Query("todate input is out of range".to_string()))?;
    Ok(Value::String(render_rfc3339_utc_seconds(&datetime)))
}

fn fromdate_of(value: &Value) -> Result<Value, AqError> {
    let timestamp = match value {
        Value::String(raw) => parse_rfc3339_utc_seconds(raw)
            .map(|value| value.timestamp())
            .ok_or_else(|| {
                AqError::Query(format!(
                    "date \"{raw}\" does not match format \"{RFC3339_UTC_SECONDS_FORMAT}\""
                ))
            })?,
        Value::DateTime(value) => value.timestamp(),
        Value::Date(value) => datetime_at_midnight(value)?.timestamp(),
        other => {
            return Err(AqError::Query(format!(
                "fromdate is not defined for {}",
                kind_name(other)
            )));
        }
    };
    if timestamp < 0 {
        return Err(AqError::Query("invalid gmtime representation".to_string()));
    }
    Ok(Value::Integer(timestamp))
}

fn to_datetime_of(value: &Value) -> Result<Value, AqError> {
    match value {
        Value::DateTime(_) => Ok(value.clone()),
        Value::Date(value) => Ok(Value::DateTime(datetime_at_midnight(value)?)),
        Value::String(raw) => parse_common_datetime_string(raw)
            .map(Value::DateTime)
            .ok_or_else(|| AqError::Query(format!("to_datetime cannot parse string \"{raw}\""))),
        other => Err(AqError::Query(format!(
            "to_datetime is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn gmtime_of(value: &Value) -> Result<Value, AqError> {
    let (datetime, nanos) = value_to_utc_datetime("gmtime", value)?;
    Ok(parsed_datetime_value(&datetime, nanos))
}

fn mktime_of(value: &Value) -> Result<Value, AqError> {
    let (datetime, nanos) = parsed_datetime_input("mktime", value)?;
    Ok(mktime_output_value(&datetime, nanos))
}

fn strptime_mktime_of(input: &Value, format: &str) -> Result<Value, AqError> {
    let Value::String(raw) = input.untagged() else {
        return Err(AqError::Query(
            "strptime/1 requires string inputs".to_string(),
        ));
    };
    let (datetime, nanos) = parse_strptime_datetime(raw, format)?;
    Ok(mktime_output_value(&datetime, nanos))
}

fn mktime_output_value(datetime: &chrono::DateTime<chrono::Utc>, nanos: u32) -> Value {
    if nanos == 0 {
        Value::Integer(datetime.timestamp())
    } else {
        Value::Float(datetime.timestamp() as f64 + f64::from(nanos) / 1_000_000_000.0)
    }
}

fn strftime_of(
    input: &Value,
    format_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(format) = literal_time_format_argument(format_expr) {
        if format == RFC3339_UTC_SECONDS_FORMAT {
            if let Some(rendered) =
                render_parsed_datetime_input_rfc3339_seconds("strftime/1", input)?
            {
                return Ok(vec![Value::String(rendered)]);
            }
        }

        let (datetime, _) = value_to_utc_datetime("strftime/1", input)?;
        return Ok(vec![if format == RFC3339_UTC_SECONDS_FORMAT {
            Value::String(render_rfc3339_utc_seconds(&datetime))
        } else {
            Value::String(datetime.format(format).to_string())
        }]);
    }

    let (datetime, _) = value_to_utc_datetime("strftime/1", input)?;
    Ok(
        evaluate_time_format_arguments("strftime/1", format_expr, input, bindings, context)?
            .into_iter()
            .map(|format| {
                if format == RFC3339_UTC_SECONDS_FORMAT {
                    Value::String(render_rfc3339_utc_seconds(&datetime))
                } else {
                    Value::String(datetime.format(&format).to_string())
                }
            })
            .collect(),
    )
}

fn render_parsed_datetime_input_rfc3339_seconds(
    name: &str,
    value: &Value,
) -> Result<Option<String>, AqError> {
    let Value::Array(values) = value.untagged() else {
        return Ok(None);
    };
    Ok(Some(render_parsed_datetime_array_rfc3339_seconds(
        values, name,
    )?))
}

fn render_parsed_datetime_array_rfc3339_seconds(
    values: &[Value],
    name: &str,
) -> Result<String, AqError> {
    if !(3..=8).contains(&values.len()) {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    }

    let year = parsed_datetime_int_component(values, 0, name)?;
    let month0 = parsed_datetime_int_component(values, 1, name)?;
    let day = parsed_datetime_int_component(values, 2, name)?;
    let hour = parsed_datetime_optional_int_component(values, 3, 0, name)?;
    let minute = parsed_datetime_optional_int_component(values, 4, 0, name)?;
    let (second, _) = parsed_datetime_second_component(values.get(5), name)?;

    if values.len() > 6 {
        let _ = parsed_datetime_int_component(values, 6, name)?;
    }
    if values.len() > 7 {
        let _ = parsed_datetime_int_component(values, 7, name)?;
    }

    let year = i32::try_from(year)
        .map_err(|_| AqError::Query(format!("{name} requires parsed datetime inputs")))?;
    let month = u32::try_from(month0.saturating_add(1))
        .map_err(|_| AqError::Query(format!("{name} requires parsed datetime inputs")))?;
    let day = u32::try_from(day)
        .map_err(|_| AqError::Query(format!("{name} requires parsed datetime inputs")))?;
    let hour = u32::try_from(hour)
        .map_err(|_| AqError::Query(format!("{name} requires parsed datetime inputs")))?;
    let minute = u32::try_from(minute)
        .map_err(|_| AqError::Query(format!("{name} requires parsed datetime inputs")))?;
    let second = u32::try_from(second)
        .map_err(|_| AqError::Query(format!("{name} requires parsed datetime inputs")))?;

    let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Some(time) = NaiveTime::from_hms_opt(hour, minute, second) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };

    let mut rendered = String::with_capacity(20);
    let _ = write!(
        rendered,
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        date.year(),
        date.month(),
        date.day(),
        time.hour(),
        time.minute(),
        time.second(),
    );
    Ok(rendered)
}

fn strflocaltime_of(
    input: &Value,
    format_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let (datetime, _) = value_to_utc_datetime("strflocaltime/1", input)?;
    let local = datetime.with_timezone(&chrono::Local);
    if let Some(format) = literal_time_format_argument(format_expr) {
        return Ok(vec![Value::String(local.format(format).to_string())]);
    }

    Ok(
        evaluate_time_format_arguments("strflocaltime/1", format_expr, input, bindings, context)?
            .into_iter()
            .map(|format| Value::String(local.format(&format).to_string()))
            .collect(),
    )
}

fn strptime_of(
    input: &Value,
    format_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let Value::String(raw) = input.untagged() else {
        return Err(AqError::Query(
            "strptime/1 requires string inputs".to_string(),
        ));
    };
    if let Some(format) = literal_time_format_argument(format_expr) {
        let (datetime, nanos) = parse_strptime_datetime(raw, format)?;
        return Ok(vec![parsed_datetime_value(&datetime, nanos)]);
    }

    let formats =
        evaluate_time_format_arguments("strptime/1", format_expr, input, bindings, context)?;
    let mut out = Vec::with_capacity(formats.len());
    for format in formats {
        let (datetime, nanos) = parse_strptime_datetime(raw, &format)?;
        out.push(parsed_datetime_value(&datetime, nanos));
    }
    Ok(out)
}

fn literal_time_format_argument(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(Value::String(value)) => Some(value.as_str()),
        Expr::Subquery(query) if query.functions.is_empty() && query.imported_values.is_empty() => {
            let [pipeline] = query.outputs.as_slice() else {
                return None;
            };
            let [stage] = pipeline.stages.as_slice() else {
                return None;
            };
            let Expr::Literal(Value::String(value)) = stage else {
                return None;
            };
            Some(value.as_str())
        }
        _ => None,
    }
}

fn value_to_utc_datetime(
    name: &str,
    value: &Value,
) -> Result<(chrono::DateTime<chrono::Utc>, u32), AqError> {
    match value.untagged() {
        Value::Integer(value) => {
            let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(*value, 0)
                .ok_or_else(|| AqError::Query(format!("{name} input is out of range")))?;
            Ok((datetime, 0))
        }
        Value::Decimal(value) => epoch_seconds_to_datetime(value.to_f64_lossy(), name),
        Value::Float(value) => epoch_seconds_to_datetime(*value, name),
        Value::Array(_) => parsed_datetime_input(name, value),
        Value::DateTime(value) => Ok((*value, value.timestamp_subsec_nanos())),
        Value::Date(value) => Ok((datetime_at_midnight(value)?, 0)),
        _ => Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        ))),
    }
}

fn evaluate_time_format_arguments(
    name: &str,
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<String>, AqError> {
    evaluate_expr(expr, input, bindings, context)?
        .into_iter()
        .map(|value| match value {
            Value::String(value) => Ok(value),
            _ => Err(AqError::Query(format!("{name} requires a string format"))),
        })
        .collect()
}

fn parse_strptime_datetime(
    raw: &str,
    format: &str,
) -> Result<(chrono::DateTime<chrono::Utc>, u32), AqError> {
    if format == RFC3339_UTC_SECONDS_FORMAT {
        if let Some(value) = parse_rfc3339_utc_seconds(raw) {
            return Ok((value, 0));
        }
    }
    if let Ok(value) = chrono::DateTime::parse_from_str(raw, format) {
        let value = value.with_timezone(&chrono::Utc);
        return Ok((value, value.timestamp_subsec_nanos()));
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(raw, format) {
        return Ok((
            chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(value, chrono::Utc),
            value.and_utc().timestamp_subsec_nanos(),
        ));
    }
    if let Ok(value) = NaiveDate::parse_from_str(raw, format) {
        return Ok((datetime_at_midnight(&value)?, 0));
    }
    Err(AqError::Query(format!(
        "date \"{raw}\" does not match format \"{format}\""
    )))
}

fn render_rfc3339_utc_seconds(datetime: &chrono::DateTime<chrono::Utc>) -> String {
    let mut rendered = String::with_capacity(20);
    let _ = write!(
        rendered,
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        datetime.year(),
        datetime.month(),
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        datetime.second(),
    );
    rendered
}

fn parse_rfc3339_utc_seconds(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let bytes = raw.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return None;
    }

    let year = parse_ascii_decimal_component(&bytes[0..4])? as i32;
    let month = parse_ascii_decimal_component(&bytes[5..7])?;
    let day = parse_ascii_decimal_component(&bytes[8..10])?;
    let hour = parse_ascii_decimal_component(&bytes[11..13])?;
    let minute = parse_ascii_decimal_component(&bytes[14..16])?;
    let second = parse_ascii_decimal_component(&bytes[17..19])?;

    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let time = NaiveTime::from_hms_opt(hour, minute, second)?;
    Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        NaiveDateTime::new(date, time),
        chrono::Utc,
    ))
}

fn parse_ascii_decimal_component(bytes: &[u8]) -> Option<u32> {
    let mut value = 0u32;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
    }
    Some(value)
}

fn epoch_seconds_to_datetime(
    seconds: f64,
    name: &str,
) -> Result<(chrono::DateTime<chrono::Utc>, u32), AqError> {
    if !seconds.is_finite() {
        return Err(AqError::Query(format!("{name} input is out of range")));
    }

    let whole_seconds = seconds.floor();
    if whole_seconds < i64::MIN as f64 || whole_seconds > i64::MAX as f64 {
        return Err(AqError::Query(format!("{name} input is out of range")));
    }

    let mut whole_seconds = whole_seconds as i64;
    let mut nanos = ((seconds - whole_seconds as f64) * 1_000_000_000.0).round();
    if nanos >= 1_000_000_000.0 {
        whole_seconds = whole_seconds.saturating_add(1);
        nanos = 0.0;
    }
    if nanos < 0.0 {
        whole_seconds = whole_seconds.saturating_sub(1);
        nanos += 1_000_000_000.0;
    }
    let nanos = nanos as u32;

    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(whole_seconds, nanos)
        .ok_or_else(|| AqError::Query(format!("{name} input is out of range")))?;
    Ok((datetime, nanos))
}

fn parsed_datetime_input(
    name: &str,
    value: &Value,
) -> Result<(chrono::DateTime<chrono::Utc>, u32), AqError> {
    match value.untagged() {
        Value::Array(values) => parsed_datetime_array(values, name),
        Value::DateTime(value) => Ok((*value, value.timestamp_subsec_nanos())),
        Value::Date(value) => Ok((datetime_at_midnight(value)?, 0)),
        _ => Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        ))),
    }
}

fn parsed_datetime_array(
    values: &[Value],
    name: &str,
) -> Result<(chrono::DateTime<chrono::Utc>, u32), AqError> {
    if !(3..=8).contains(&values.len()) {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    }

    let year = parsed_datetime_int_component(values, 0, name)?;
    let month0 = parsed_datetime_int_component(values, 1, name)?;
    let day = parsed_datetime_int_component(values, 2, name)?;
    let hour = parsed_datetime_optional_int_component(values, 3, 0, name)?;
    let minute = parsed_datetime_optional_int_component(values, 4, 0, name)?;
    let (second, nanos) = parsed_datetime_second_component(values.get(5), name)?;

    if values.len() > 6 {
        let _ = parsed_datetime_int_component(values, 6, name)?;
    }
    if values.len() > 7 {
        let _ = parsed_datetime_int_component(values, 7, name)?;
    }

    let Some(month) = month0.checked_add(1) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Ok(year) = i32::try_from(year) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Ok(month) = u32::try_from(month) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Ok(day) = u32::try_from(day) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Ok(hour) = u32::try_from(hour) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Ok(minute) = u32::try_from(minute) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Ok(second) = u32::try_from(second) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };

    let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let Some(time) = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos) else {
        return Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        )));
    };
    let datetime = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        NaiveDateTime::new(date, time),
        chrono::Utc,
    );
    Ok((datetime, nanos))
}

fn parsed_datetime_int_component(
    values: &[Value],
    index: usize,
    name: &str,
) -> Result<i64, AqError> {
    parsed_datetime_number_component(
        values
            .get(index)
            .ok_or_else(|| AqError::Query(format!("{name} requires parsed datetime inputs")))?,
        name,
    )
}

fn parsed_datetime_optional_int_component(
    values: &[Value],
    index: usize,
    default: i64,
    name: &str,
) -> Result<i64, AqError> {
    match values.get(index) {
        Some(value) => parsed_datetime_number_component(value, name),
        None => Ok(default),
    }
}

fn parsed_datetime_number_component(value: &Value, name: &str) -> Result<i64, AqError> {
    match value.untagged() {
        Value::Integer(value) => Ok(*value),
        Value::Decimal(value) if value.as_i64_exact().is_some() => {
            Ok(value.as_i64_exact().unwrap_or_default())
        }
        Value::Float(value)
            if value.is_finite()
                && value.fract() == 0.0
                && *value >= i64::MIN as f64
                && *value <= i64::MAX as f64 =>
        {
            Ok(*value as i64)
        }
        _ => Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        ))),
    }
}

fn parsed_datetime_second_component(
    value: Option<&Value>,
    name: &str,
) -> Result<(i64, u32), AqError> {
    let Some(value) = value else {
        return Ok((0, 0));
    };
    match value.untagged() {
        Value::Integer(value) => Ok((*value, 0)),
        Value::Decimal(value) => {
            let value = value.to_f64_lossy();
            let seconds = value.floor();
            if seconds < i64::MIN as f64 || seconds > i64::MAX as f64 {
                return Err(AqError::Query(format!(
                    "{name} requires parsed datetime inputs"
                )));
            }
            let mut seconds = seconds as i64;
            let mut nanos = ((value - seconds as f64) * 1_000_000_000.0).round();
            if nanos >= 1_000_000_000.0 {
                seconds = seconds.saturating_add(1);
                nanos = 0.0;
            }
            if nanos < 0.0 {
                return Err(AqError::Query(format!(
                    "{name} requires parsed datetime inputs"
                )));
            }
            Ok((seconds, nanos as u32))
        }
        Value::Float(value) if value.is_finite() => {
            let seconds = value.floor();
            if seconds < i64::MIN as f64 || seconds > i64::MAX as f64 {
                return Err(AqError::Query(format!(
                    "{name} requires parsed datetime inputs"
                )));
            }
            let mut seconds = seconds as i64;
            let mut nanos = ((value - seconds as f64) * 1_000_000_000.0).round();
            if nanos >= 1_000_000_000.0 {
                seconds = seconds.saturating_add(1);
                nanos = 0.0;
            }
            if nanos < 0.0 {
                return Err(AqError::Query(format!(
                    "{name} requires parsed datetime inputs"
                )));
            }
            Ok((seconds, nanos as u32))
        }
        _ => Err(AqError::Query(format!(
            "{name} requires parsed datetime inputs"
        ))),
    }
}

fn parsed_datetime_value(datetime: &chrono::DateTime<chrono::Utc>, nanos: u32) -> Value {
    let second = if nanos == 0 {
        Value::Integer(i64::from(datetime.second()))
    } else {
        Value::Float(f64::from(datetime.second()) + f64::from(nanos) / 1_000_000_000.0)
    };

    Value::Array(vec![
        Value::Integer(i64::from(datetime.year())),
        Value::Integer(i64::from(datetime.month0())),
        Value::Integer(i64::from(datetime.day())),
        Value::Integer(i64::from(datetime.hour())),
        Value::Integer(i64::from(datetime.minute())),
        second,
        Value::Integer(i64::from(datetime.weekday().num_days_from_sunday())),
        Value::Integer(i64::from(datetime.ordinal0())),
    ])
}

fn unary_math_of(name: &str, value: &Value, op: impl FnOnce(f64) -> f64) -> Result<Value, AqError> {
    let value = numeric_input_value(name, value)?;
    normalize_math_result(op(value))
}

fn numeric_input_value(name: &str, value: &Value) -> Result<f64, AqError> {
    match value {
        Value::Integer(value) => Ok(*value as f64),
        Value::Decimal(value) => Ok(value.to_f64_lossy()),
        Value::Float(value) => Ok(*value),
        other => Err(AqError::Query(format!(
            "{name} is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn normalize_math_result(value: f64) -> Result<Value, AqError> {
    if value.is_nan() {
        return Ok(Value::Null);
    }
    if value == f64::INFINITY {
        return Ok(Value::Float(f64::MAX));
    }
    if value == f64::NEG_INFINITY {
        return Ok(Value::Float(-f64::MAX));
    }
    Ok(normalize_number_value(value))
}

fn numeric_array_values(name: &str, value: &Value) -> Result<Vec<f64>, AqError> {
    expect_array_input(name, value)?
        .iter()
        .map(|value| numeric_input_value(name, value))
        .collect()
}

fn evaluate_percentile_arg(
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<f64, AqError> {
    let value = evaluate_single_argument_value("percentile", expr, input, bindings, context)?;
    let percentile = numeric_input_value("percentile", &value)?;
    if !percentile.is_finite() || !(0.0..=100.0).contains(&percentile) {
        return Err(AqError::Query(
            "percentile expects a percentile between 0 and 100".to_string(),
        ));
    }
    Ok(percentile)
}

fn filter_of(input: &Value, filter: TypeFilter) -> Result<Vec<Value>, AqError> {
    if matches_type_filter(input, filter) {
        Ok(vec![input.clone()])
    } else {
        Ok(Vec::new())
    }
}

fn matches_type_filter(value: &Value, filter: TypeFilter) -> bool {
    let value = value.untagged();
    match filter {
        TypeFilter::Values => !matches!(value, Value::Null),
        TypeFilter::Nulls => matches!(value, Value::Null),
        TypeFilter::Booleans => matches!(value, Value::Bool(_)),
        TypeFilter::Numbers => matches!(
            value,
            Value::Integer(_) | Value::Decimal(_) | Value::Float(_)
        ),
        TypeFilter::Strings => matches!(value, Value::String(_)),
        TypeFilter::Arrays => matches!(value, Value::Array(_)),
        TypeFilter::Objects => matches!(value, Value::Object(_)),
        TypeFilter::Iterables => matches!(value, Value::Array(_) | Value::Object(_)),
        TypeFilter::Scalars => !matches!(value, Value::Array(_) | Value::Object(_)),
    }
}

fn to_string_of(value: &Value) -> Result<String, AqError> {
    if let Some(value) = value.rendered_string() {
        Ok(value)
    } else {
        value.to_json_text()
    }
}

fn to_number_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => Ok(value.clone()),
        Value::String(raw) => parse_number_literal(raw),
        other => {
            let (value_type, rendered) = typed_rendered_value(other);
            Err(AqError::Query(format!(
                "{value_type} ({rendered}) cannot be parsed as a number"
            )))
        }
    }
}

fn to_bool_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Bool(_) => Ok(value.clone()),
        Value::Integer(0) => Ok(Value::Bool(false)),
        Value::Integer(1) => Ok(Value::Bool(true)),
        Value::Decimal(number) if number.rendered() == "0" => Ok(Value::Bool(false)),
        Value::Decimal(number) if number.rendered() == "1" => Ok(Value::Bool(true)),
        Value::Float(number) if *number == 0.0 => Ok(Value::Bool(false)),
        Value::Float(number) if *number == 1.0 => Ok(Value::Bool(true)),
        Value::String(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "false" | "0" => Ok(Value::Bool(false)),
            "true" | "1" => Ok(Value::Bool(true)),
            _ => Err(AqError::Query(format!(
                "to_bool cannot parse string \"{raw}\""
            ))),
        },
        other => Err(AqError::Query(format!(
            "to_bool is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn to_boolean_of(value: &Value) -> Result<Value, AqError> {
    match value.untagged() {
        Value::Bool(_) => Ok(value.clone()),
        Value::String(raw) if raw == "false" => Ok(Value::Bool(false)),
        Value::String(raw) if raw == "true" => Ok(Value::Bool(true)),
        other => {
            let rendered = match other.to_json() {
                Ok(json) => match serde_json::to_string(&json) {
                    Ok(rendered) => rendered,
                    Err(_) => type_name(other).to_string(),
                },
                Err(_) => type_name(other).to_string(),
            };
            Err(AqError::Query(format!(
                "{} ({rendered}) cannot be parsed as a boolean",
                type_name(other)
            )))
        }
    }
}

fn isnan_of(value: &Value) -> bool {
    matches!(value.untagged(), Value::Float(number) if number.is_nan())
}

fn starts_with_of(
    input: &Value,
    prefix_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    let value = expect_string_input("startswith", input)?;
    let prefix = evaluate_string_argument_cow("startswith", prefix_expr, input, bindings, context)?;
    Ok(value.starts_with(&*prefix))
}

fn ends_with_of(
    input: &Value,
    suffix_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    let value = expect_string_input("endswith", input)?;
    let suffix = evaluate_string_argument_cow("endswith", suffix_expr, input, bindings, context)?;
    Ok(value.ends_with(&*suffix))
}

fn split_of(
    input: &Value,
    pattern_query: &Query,
    flags_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let value = expect_string_input("split", input)?;
    if let Some(flags_query) = flags_query {
        let (pattern, flags) = evaluate_regex_pattern_and_flags(
            "split",
            pattern_query,
            Some(flags_query),
            input,
            bindings,
            context,
        )?;
        if let Some(parts) = simple_regex_split_values(value, &pattern, &flags) {
            return Ok(Value::Array(parts));
        }
        let (regex, behavior) =
            compile_regex_with_flags_cached("split", &pattern, &flags, context)?;
        return Ok(Value::Array(regex_split_values(
            value,
            &regex,
            behavior.no_empty,
        )));
    }
    let delimiter =
        evaluate_single_query_string_cow("split", pattern_query, input, bindings, context)?;
    Ok(split_literal_string_value(value, delimiter.as_ref()))
}

fn splits_of(
    input: &Value,
    pattern_query: &Query,
    flags_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let value = expect_string_input("splits", input)?;
    let (pattern, flags) = evaluate_regex_pattern_and_flags(
        "splits",
        pattern_query,
        flags_query,
        input,
        bindings,
        context,
    )?;
    if pattern.is_empty() && matches!(flags.as_str(), "" | "n") {
        return Ok(split_on_empty_pattern(value));
    }
    if let Some(parts) = simple_regex_split_values(value, &pattern, &flags) {
        return Ok(parts);
    }
    let (regex, behavior) = compile_regex_with_flags_cached("splits", &pattern, &flags, context)?;
    Ok(regex_split_values(value, &regex, behavior.no_empty))
}

fn split_on_empty_pattern(value: &str) -> Vec<Value> {
    let mut parts = Vec::with_capacity(value.chars().count().saturating_add(2));
    parts.push(Value::String(String::new()));
    for ch in value.chars() {
        parts.push(Value::String(ch.to_string()));
    }
    parts.push(Value::String(String::new()));
    parts
}

fn split_literal_string_value(value: &str, delimiter: &str) -> Value {
    if delimiter.is_empty() {
        return Value::Array(
            value
                .chars()
                .map(|ch| Value::String(ch.to_string()))
                .collect(),
        );
    }

    Value::Array(
        value
            .split(delimiter)
            .map(|part| Value::String(part.to_string()))
            .collect(),
    )
}

fn evaluate_regex_pattern_and_flags(
    name: &str,
    regex_query: &Query,
    flags_query: Option<&Query>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<(String, String), AqError> {
    let regex_value = evaluate_single_query_value(name, regex_query, input, bindings, context)?;
    let (pattern, inline_flags) = match regex_value {
        Value::String(pattern) => (pattern, None),
        Value::Array(values) if flags_query.is_none() => match values.as_slice() {
            [Value::String(pattern)] => (pattern.clone(), None),
            [Value::String(pattern), Value::String(flags)] => {
                (pattern.clone(), Some(flags.clone()))
            }
            [Value::String(pattern), Value::Null] => (pattern.clone(), Some(String::new())),
            _ => {
                return Err(AqError::Query(format!(
                    "{name} requires a string or [string, flags] argument"
                )))
            }
        },
        other => {
            return Err(AqError::Query(format!(
                "{name} requires a string argument, got {}",
                kind_name(&other)
            )))
        }
    };
    let flags = match flags_query {
        Some(query) => evaluate_regex_flags_argument(name, query, input, bindings, context)?,
        None => inline_flags.unwrap_or_default(),
    };
    Ok((pattern, flags))
}

fn evaluate_regex_argument(
    name: &str,
    regex_query: &Query,
    flags_query: Option<&Query>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<(Regex, RegexBehavior), AqError> {
    let (pattern, flags) =
        evaluate_regex_pattern_and_flags(name, regex_query, flags_query, input, bindings, context)?;
    compile_regex_with_flags_cached(name, &pattern, &flags, context)
}

fn evaluate_regex_flags_argument(
    name: &str,
    flags_query: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    match evaluate_single_query_value(name, flags_query, input, bindings, context)? {
        Value::String(flags) => Ok(flags),
        Value::Null => Ok(String::new()),
        other => Err(AqError::Query(format!(
            "{name} requires a string argument, got {}",
            kind_name(&other)
        ))),
    }
}

fn next_regex_search_start(input: &str, byte_offset: usize) -> Option<usize> {
    if byte_offset >= input.len() {
        return None;
    }
    input[byte_offset..]
        .chars()
        .next()
        .map(|ch| byte_offset + ch.len_utf8())
}

fn for_each_regex_capture<'a, F>(
    regex: &Regex,
    input: &'a str,
    no_empty: bool,
    mut visit: F,
) -> Result<(), AqError>
where
    F: FnMut(regex::Captures<'a>) -> Result<bool, AqError>,
{
    let mut search_start = 0usize;
    while let Some(captures) = regex.captures_at(input, search_start) {
        let matched = captures.get(0).ok_or_else(|| {
            AqError::Query("internal error: regex match missing full capture".to_string())
        })?;
        let is_empty = matched.start() == matched.end();
        if no_empty && is_empty {
            let Some(next_start) = next_regex_search_start(input, matched.end()) else {
                break;
            };
            search_start = next_start;
            continue;
        }
        if !visit(captures)? {
            break;
        }
        if is_empty {
            let Some(next_start) = next_regex_search_start(input, matched.end()) else {
                break;
            };
            search_start = next_start;
        } else {
            search_start = matched.end();
        }
    }
    Ok(())
}

fn regex_split_values(value: &str, regex: &Regex, no_empty: bool) -> Vec<Value> {
    let mut parts = Vec::new();
    let mut last_end = 0usize;
    for matched in regex.find_iter(value) {
        if no_empty && matched.start() == matched.end() {
            continue;
        }
        parts.push(Value::String(value[last_end..matched.start()].to_string()));
        last_end = matched.end();
    }
    parts.push(Value::String(value[last_end..].to_string()));
    parts
}

fn simple_regex_split_values(value: &str, pattern: &str, flags: &str) -> Option<Vec<Value>> {
    let case_insensitive = match flags {
        "" | "n" => false,
        "i" | "in" | "ni" => true,
        _ => return None,
    };
    if let Some(separator) = simple_ascii_delimiter_with_optional_spaces(pattern) {
        return Some(split_on_ascii_byte_with_optional_spaces(
            value,
            separator,
            case_insensitive,
        ));
    }
    let repeated = simple_repeated_ascii_pattern(pattern)?;
    Some(split_on_repeated_ascii_byte(
        value,
        repeated,
        case_insensitive,
    ))
}

fn simple_repeated_ascii_pattern(pattern: &str) -> Option<u8> {
    let repeated = pattern.strip_suffix('+')?;
    let [byte] = repeated.as_bytes() else {
        return None;
    };
    if !byte.is_ascii() || is_regex_metachar(*byte) {
        return None;
    }
    Some(*byte)
}

fn simple_ascii_delimiter_with_optional_spaces(pattern: &str) -> Option<u8> {
    let bytes = pattern.as_bytes();
    let [separator, b' ', b'*'] = bytes else {
        return None;
    };
    if !separator.is_ascii() || is_regex_metachar(*separator) {
        return None;
    }
    Some(*separator)
}

fn is_regex_metachar(byte: u8) -> bool {
    matches!(
        byte,
        b'.' | b'\\'
            | b'+'
            | b'*'
            | b'?'
            | b'('
            | b')'
            | b'['
            | b']'
            | b'{'
            | b'}'
            | b'^'
            | b'$'
            | b'|'
    )
}

fn split_on_repeated_ascii_byte(value: &str, repeated: u8, case_insensitive: bool) -> Vec<Value> {
    let bytes = value.as_bytes();
    let mut parts = Vec::new();
    let mut last_end = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if ascii_byte_matches_regex_repeated(bytes[index], repeated, case_insensitive) {
            let start = index;
            index += 1;
            while index < bytes.len()
                && ascii_byte_matches_regex_repeated(bytes[index], repeated, case_insensitive)
            {
                index += 1;
            }
            parts.push(Value::String(value[last_end..start].to_string()));
            last_end = index;
        } else {
            index += 1;
        }
    }
    parts.push(Value::String(value[last_end..].to_string()));
    parts
}

fn split_on_ascii_byte_with_optional_spaces(
    value: &str,
    separator: u8,
    case_insensitive: bool,
) -> Vec<Value> {
    let bytes = value.as_bytes();
    let mut parts = Vec::new();
    let mut last_end = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if ascii_byte_matches_regex_repeated(bytes[index], separator, case_insensitive) {
            let start = index;
            index += 1;
            while index < bytes.len() && bytes[index] == b' ' {
                index += 1;
            }
            parts.push(Value::String(value[last_end..start].to_string()));
            last_end = index;
        } else {
            index += 1;
        }
    }
    parts.push(Value::String(value[last_end..].to_string()));
    parts
}

fn ascii_byte_matches_regex_repeated(byte: u8, repeated: u8, case_insensitive: bool) -> bool {
    if case_insensitive {
        byte.eq_ignore_ascii_case(&repeated)
    } else {
        byte == repeated
    }
}

fn extract_positive_lookahead_pattern(pattern: &str) -> Option<&str> {
    pattern
        .strip_prefix("(?=")
        .and_then(|pattern| pattern.strip_suffix(')'))
}

fn named_capture_object(regex: &Regex, captures: &regex::Captures<'_>) -> Value {
    let mut object = IndexMap::with_capacity(regex.captures_len().saturating_sub(1));
    for (name, capture) in regex.capture_names().skip(1).zip(captures.iter().skip(1)) {
        let Some(name) = name else {
            continue;
        };
        let captured = capture
            .map(|value| Value::String(value.as_str().to_string()))
            .unwrap_or(Value::Null);
        object.insert(name.to_string(), captured);
    }
    Value::Object(object)
}

fn test_of(
    input: &Value,
    regex_query: &Query,
    flags_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    let value = expect_string_input("test", input)?;
    let (regex, behavior) =
        evaluate_regex_argument("test", regex_query, flags_query, input, bindings, context)?;
    if !behavior.no_empty {
        return Ok(regex.is_match(value));
    }
    let mut matched = false;
    for_each_regex_capture(&regex, value, behavior.no_empty, |_| {
        matched = true;
        Ok(false)
    })?;
    Ok(matched)
}

fn capture_of(
    input: &Value,
    regex_query: &Query,
    flags_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let value = expect_string_input("capture", input)?;
    let (regex, behavior) = evaluate_regex_argument(
        "capture",
        regex_query,
        flags_query,
        input,
        bindings,
        context,
    )?;
    if !behavior.no_empty {
        if let Some(captures) = regex.captures(value) {
            return Ok(named_capture_object(&regex, &captures));
        }
        return Err(AqError::Query("capture did not match input".to_string()));
    }
    let mut captured = None;
    for_each_regex_capture(&regex, value, behavior.no_empty, |captures| {
        captured = Some(named_capture_object(&regex, &captures));
        Ok(false)
    })?;
    captured.ok_or_else(|| AqError::Query("capture did not match input".to_string()))
}

fn match_of(
    input: &Value,
    regex_query: &Query,
    flags_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let value = expect_string_input("match", input)?;
    let (regex, behavior) =
        evaluate_regex_argument("match", regex_query, flags_query, input, bindings, context)?;
    if !behavior.global && !behavior.no_empty {
        return regex
            .captures(value)
            .map(|captures| regex_match_value(value, &regex, &captures))
            .transpose()
            .map(|value| value.into_iter().collect());
    }
    let mut out = Vec::new();
    for_each_regex_capture(&regex, value, behavior.no_empty, |captures| {
        out.push(regex_match_value(value, &regex, &captures)?);
        Ok(behavior.global)
    })?;
    Ok(out)
}

fn scan_of(
    input: &Value,
    regex_query: &Query,
    flags_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let value = expect_string_input("scan", input)?;
    let (regex, behavior) =
        evaluate_regex_argument("scan", regex_query, flags_query, input, bindings, context)?;
    let mut out = Vec::new();
    for_each_regex_capture(&regex, value, behavior.no_empty, |captures| {
        out.push(regex_scan_value(&captures));
        Ok(true)
    })?;
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
struct RegexBehavior {
    global: bool,
    no_empty: bool,
}

struct RegexReplaceSpec<'a> {
    name: &'a str,
    regex_query: &'a Query,
    replacement_query: &'a Query,
    flags_query: Option<&'a Query>,
    global: bool,
}

fn regex_positive_lookahead_replace_of(
    spec: &RegexReplaceSpec<'_>,
    value: &str,
    inner_pattern: &str,
    flags: &str,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let (regex, behavior) =
        compile_regex_with_flags_cached(spec.name, inner_pattern, flags, context)?;
    let replace_all = spec.global || behavior.global;
    let mut outputs = vec![String::new()];
    let mut matched_any = false;
    let mut any_replacement = false;
    let mut last_end = 0usize;
    let mut search_start = 0usize;

    while search_start <= value.len() {
        let Some(captures) = regex.captures_at(value, search_start) else {
            break;
        };
        let matched = captures.get(0).ok_or_else(|| {
            AqError::Query("internal error: regex replacement missing full capture".to_string())
        })?;
        if matched.start() != search_start {
            search_start = matched.start();
            continue;
        }
        if behavior.no_empty {
            let Some(next_start) = next_regex_search_start(value, search_start) else {
                break;
            };
            search_start = next_start;
            continue;
        }

        let literal = &value[last_end..search_start];
        for output in &mut outputs {
            output.push_str(literal);
        }

        let replacement_input = named_capture_object(&regex, &captures);
        let replacements = evaluate_query(
            spec.replacement_query,
            &replacement_input,
            bindings,
            context,
        )?
        .into_iter()
        .map(|frame| frame.value)
        .collect::<Vec<_>>();
        if !replacements.is_empty() {
            any_replacement = true;
        }

        let branch_count = outputs.len().max(replacements.len());
        let mut next_outputs = Vec::with_capacity(branch_count);
        for index in 0..branch_count {
            let base = outputs
                .get(index)
                .cloned()
                .unwrap_or_else(|| literal.to_string());
            if let Some(replacement) = replacements.get(index) {
                let combined = value_add(&Value::String(base), replacement)?;
                match combined {
                    Value::String(rendered) => next_outputs.push(rendered),
                    other => {
                        return Err(AqError::Query(format!(
                            "{} replacement produced {}, expected string-like output",
                            spec.name,
                            kind_name(&other)
                        )));
                    }
                }
            } else {
                next_outputs.push(base);
            }
        }
        outputs = next_outputs;
        last_end = search_start;
        matched_any = true;
        if !replace_all {
            break;
        }
        let Some(next_start) = next_regex_search_start(value, search_start) else {
            break;
        };
        search_start = next_start;
    }

    if matched_any && !any_replacement {
        return Ok(vec![Value::String(value.to_string())]);
    }

    let suffix = &value[last_end..];
    for output in &mut outputs {
        output.push_str(suffix);
    }
    Ok(outputs.into_iter().map(Value::String).collect())
}

fn regex_replace_of(
    spec: &RegexReplaceSpec<'_>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let value = expect_string_input(spec.name, input)?;
    let (pattern, flags) = evaluate_regex_pattern_and_flags(
        spec.name,
        spec.regex_query,
        spec.flags_query,
        input,
        bindings,
        context,
    )?;
    if let Some(replacement) =
        constant_string_query_value(spec.replacement_query, bindings, context)?
    {
        if let Some(replaced) =
            literal_regex_replace_fast_path(value, &pattern, &flags, &replacement, spec.global)
        {
            return Ok(vec![Value::String(replaced)]);
        }
    }
    let (regex, behavior) =
        match compile_regex_with_flags_cached(spec.name, &pattern, &flags, context) {
            Ok(compiled) => compiled,
            Err(error) => {
                let Some(inner_pattern) = extract_positive_lookahead_pattern(&pattern) else {
                    return Err(error);
                };
                return regex_positive_lookahead_replace_of(
                    spec,
                    value,
                    inner_pattern,
                    &flags,
                    bindings,
                    context,
                );
            }
        };
    let replace_all = spec.global || behavior.global;
    if let Some(replacement) =
        constant_string_query_value(spec.replacement_query, bindings, context)?
    {
        return Ok(vec![Value::String(regex_replace_constant_string(
            value,
            &regex,
            behavior.no_empty,
            &replacement,
            replace_all,
        ))]);
    }
    let mut outputs = vec![String::new()];
    let mut matched_any = false;
    let mut any_replacement = false;
    let mut last_end = 0usize;
    for_each_regex_capture(&regex, value, behavior.no_empty, |captures| {
        let matched = captures.get(0).ok_or_else(|| {
            AqError::Query("internal error: regex replacement missing full capture".to_string())
        })?;
        let literal = &value[last_end..matched.start()];
        for output in &mut outputs {
            output.push_str(literal);
        }

        let replacements = if let Some(replacements) =
            simple_regex_replacement_query_values(spec.replacement_query, &regex, &captures)?
        {
            replacements
        } else {
            let replacement_input = named_capture_object(&regex, &captures);
            evaluate_query(
                spec.replacement_query,
                &replacement_input,
                bindings,
                context,
            )?
            .into_iter()
            .map(|frame| frame.value)
            .collect::<Vec<_>>()
        };
        if !replacements.is_empty() {
            any_replacement = true;
        }

        let branch_count = outputs.len().max(replacements.len());
        let mut next_outputs = Vec::with_capacity(branch_count);
        for index in 0..branch_count {
            let base = outputs
                .get(index)
                .cloned()
                .unwrap_or_else(|| literal.to_string());
            if let Some(replacement) = replacements.get(index) {
                let combined = value_add(&Value::String(base), replacement)?;
                match combined {
                    Value::String(rendered) => next_outputs.push(rendered),
                    other => {
                        return Err(AqError::Query(format!(
                            "{} replacement produced {}, expected string-like output",
                            spec.name,
                            kind_name(&other)
                        )));
                    }
                }
            } else {
                next_outputs.push(base);
            }
        }
        outputs = next_outputs;
        last_end = matched.end();
        matched_any = true;
        Ok(replace_all)
    })?;

    if matched_any && !any_replacement {
        return Ok(vec![Value::String(value.to_string())]);
    }

    let suffix = &value[last_end..];
    for output in &mut outputs {
        output.push_str(suffix);
    }
    Ok(outputs.into_iter().map(Value::String).collect())
}

fn simple_regex_replacement_query_values(
    query: &Query,
    regex: &Regex,
    captures: &regex::Captures<'_>,
) -> Result<Option<Vec<Value>>, AqError> {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return Ok(None);
    }
    let mut outputs = Vec::with_capacity(query.outputs.len());
    for pipeline in &query.outputs {
        let [expr] = pipeline.stages.as_slice() else {
            return Ok(None);
        };
        let Some(output) = simple_regex_replacement_expr_value(expr, regex, captures)? else {
            return Ok(None);
        };
        outputs.push(Value::String(output));
    }
    Ok(Some(outputs))
}

fn simple_regex_replacement_expr_value(
    expr: &Expr,
    regex: &Regex,
    captures: &regex::Captures<'_>,
) -> Result<Option<String>, AqError> {
    match expr {
        Expr::Literal(Value::String(value)) => Ok(Some(value.clone())),
        Expr::FormatString {
            operator: FormatOperator::Text,
            parts,
        } => {
            let mut output = String::new();
            for part in parts {
                match part {
                    FormatStringPart::Literal(value) => output.push_str(value),
                    FormatStringPart::Query(query) => {
                        let Some(value) =
                            simple_regex_replacement_query_part_value(query, regex, captures)?
                        else {
                            return Ok(None);
                        };
                        output.push_str(&text_for_format(&value)?);
                    }
                }
            }
            Ok(Some(output))
        }
        _ => Ok(None),
    }
}

fn simple_regex_replacement_query_part_value(
    query: &Query,
    regex: &Regex,
    captures: &regex::Captures<'_>,
) -> Result<Option<Value>, AqError> {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return Ok(None);
    }
    let [pipeline] = query.outputs.as_slice() else {
        return Ok(None);
    };
    match pipeline.stages.as_slice() {
        [Expr::Path(path)] => Ok(simple_named_capture_path_value(path, regex, captures)),
        [Expr::Path(path), Expr::Builtin(BuiltinExpr::AsciiUpcase)] => {
            let Some(value) = simple_named_capture_path_value(path, regex, captures) else {
                return Ok(None);
            };
            Ok(Some(Value::String(ascii_upcase_of(&value)?)))
        }
        [Expr::Path(path), Expr::Builtin(BuiltinExpr::AsciiDowncase)] => {
            let Some(value) = simple_named_capture_path_value(path, regex, captures) else {
                return Ok(None);
            };
            Ok(Some(Value::String(ascii_downcase_of(&value)?)))
        }
        _ => Ok(None),
    }
}

fn simple_named_capture_path_value(
    path: &PathExpr,
    regex: &Regex,
    captures: &regex::Captures<'_>,
) -> Option<Value> {
    match path.segments.as_slice() {
        [Segment::Field { name, .. }]
            if regex.capture_names().flatten().any(|field| field == name) =>
        {
            Some(
                captures
                    .name(name)
                    .map(|value| Value::String(value.as_str().to_string()))
                    .unwrap_or(Value::Null),
            )
        }
        _ => None,
    }
}

fn regex_replace_constant_string(
    value: &str,
    regex: &Regex,
    no_empty: bool,
    replacement: &str,
    replace_all: bool,
) -> String {
    let mut matched_any = false;
    let mut last_end = 0usize;
    let mut output = String::with_capacity(
        value
            .len()
            .saturating_add(replacement.len().saturating_mul(2)),
    );
    for matched in regex.find_iter(value) {
        if no_empty && matched.start() == matched.end() {
            continue;
        }
        matched_any = true;
        output.push_str(&value[last_end..matched.start()]);
        output.push_str(replacement);
        last_end = matched.end();
        if !replace_all {
            break;
        }
    }
    if !matched_any {
        return value.to_string();
    }
    output.push_str(&value[last_end..]);
    output
}

fn literal_regex_replace_fast_path(
    value: &str,
    pattern: &str,
    flags: &str,
    replacement: &str,
    force_global: bool,
) -> Option<String> {
    if pattern.is_empty()
        || !is_plain_literal_regex_pattern(pattern)
        || !flags.chars().all(|flag| matches!(flag, 'g' | 'n'))
    {
        return None;
    }
    let replace_all = force_global || flags.contains('g');
    Some(if replace_all {
        value.replace(pattern, replacement)
    } else {
        value.replacen(pattern, replacement, 1)
    })
}

fn is_plain_literal_regex_pattern(pattern: &str) -> bool {
    !pattern.bytes().any(is_regex_meta_byte)
}

fn is_regex_meta_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'\\'
            | b'.'
            | b'^'
            | b'$'
            | b'|'
            | b'?'
            | b'*'
            | b'+'
            | b'('
            | b')'
            | b'['
            | b']'
            | b'{'
            | b'}'
    )
}

fn regex_match_value(
    input: &str,
    regex: &Regex,
    captures: &regex::Captures<'_>,
) -> Result<Value, AqError> {
    let ascii = input.is_ascii();
    let Some(matched) = captures.get(0) else {
        return Err(AqError::Query(
            "internal error: regex match missing full capture".to_string(),
        ));
    };
    let mut object = IndexMap::with_capacity(4);
    object.insert(
        "offset".to_string(),
        Value::Integer(if ascii {
            i64::try_from(matched.start())
                .map_err(|_| AqError::Query("character offset is out of range".to_string()))?
        } else {
            char_offset(input, matched.start())?
        }),
    );
    object.insert(
        "length".to_string(),
        Value::Integer(if ascii {
            i64::try_from(matched.as_str().len())
                .map_err(|_| AqError::Query("character length is out of range".to_string()))?
        } else {
            char_len(matched.as_str())?
        }),
    );
    object.insert(
        "string".to_string(),
        Value::String(matched.as_str().to_string()),
    );
    if captures.len() == 1 {
        object.insert("captures".to_string(), Value::Array(Vec::new()));
        return Ok(Value::Object(object));
    }

    let mut capture_values = Vec::with_capacity(captures.len().saturating_sub(1));
    for (capture_name, capture_match) in regex.capture_names().skip(1).zip(captures.iter().skip(1))
    {
        let mut capture = IndexMap::new();
        if let Some(matched) = capture_match {
            capture.insert(
                "offset".to_string(),
                Value::Integer(if ascii {
                    i64::try_from(matched.start()).map_err(|_| {
                        AqError::Query("character offset is out of range".to_string())
                    })?
                } else {
                    char_offset(input, matched.start())?
                }),
            );
            capture.insert(
                "length".to_string(),
                Value::Integer(if ascii {
                    i64::try_from(matched.as_str().len()).map_err(|_| {
                        AqError::Query("character length is out of range".to_string())
                    })?
                } else {
                    char_len(matched.as_str())?
                }),
            );
            capture.insert(
                "string".to_string(),
                Value::String(matched.as_str().to_string()),
            );
        } else {
            capture.insert("offset".to_string(), Value::Integer(-1));
            capture.insert("length".to_string(), Value::Integer(0));
            capture.insert("string".to_string(), Value::Null);
        }
        let name = capture_name
            .map(|name| Value::String(name.to_string()))
            .unwrap_or(Value::Null);
        capture.insert("name".to_string(), name);
        capture_values.push(Value::Object(capture));
    }
    object.insert("captures".to_string(), Value::Array(capture_values));
    Ok(Value::Object(object))
}

fn regex_scan_value(captures: &regex::Captures<'_>) -> Value {
    if captures.len() <= 1 {
        return captures
            .get(0)
            .map(|matched| Value::String(matched.as_str().to_string()))
            .unwrap_or(Value::Null);
    }
    Value::Array(
        (1..captures.len())
            .map(|index| {
                captures
                    .get(index)
                    .map(|matched| Value::String(matched.as_str().to_string()))
                    .unwrap_or(Value::Null)
            })
            .collect(),
    )
}

fn char_offset(input: &str, byte_offset: usize) -> Result<i64, AqError> {
    if input.is_ascii() {
        return i64::try_from(byte_offset)
            .map_err(|_| AqError::Query("character offset is out of range".to_string()));
    }
    i64::try_from(input[..byte_offset].chars().count())
        .map_err(|_| AqError::Query("character offset is out of range".to_string()))
}

fn char_len(value: &str) -> Result<i64, AqError> {
    if value.is_ascii() {
        return i64::try_from(value.len())
            .map_err(|_| AqError::Query("character length is out of range".to_string()));
    }
    i64::try_from(value.chars().count())
        .map_err(|_| AqError::Query("character length is out of range".to_string()))
}

fn any_of(
    input: &Value,
    predicate: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    let values = expect_array_input("any", input)?;
    evaluate_array_predicate(values, predicate, bindings, true, context)
}

fn all_of(
    input: &Value,
    predicate: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    let values = expect_array_input("all", input)?;
    evaluate_array_predicate(values, predicate, bindings, false, context)
}

fn evaluate_array_predicate(
    values: &[Value],
    predicate: Option<&Query>,
    bindings: &Bindings,
    is_any: bool,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    if let Some(predicate) = predicate {
        if is_any {
            for value in values {
                if query_produces_truthy(predicate, value, bindings, context)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }

        for value in values {
            if !query_produces_truthy(predicate, value, bindings, context)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    if is_any {
        Ok(values.iter().any(is_truthy))
    } else {
        Ok(values.iter().all(is_truthy))
    }
}

fn any_from_of(
    input: &Value,
    source: &Query,
    predicate: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    evaluate_query_predicate(source, predicate, input, bindings, context, true)
}

fn all_from_of(
    input: &Value,
    source: &Query,
    predicate: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    evaluate_query_predicate(source, predicate, input, bindings, context, false)
}

fn evaluate_query_predicate(
    source: &Query,
    predicate: &Query,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
    is_any: bool,
) -> Result<bool, AqError> {
    if query_is_simple_eager_predicate_source(source) {
        for frame in evaluate_query(source, input, bindings, context)? {
            let truthy = query_produces_truthy(predicate, &frame.value, &frame.bindings, context)?;
            if is_any && truthy {
                return Ok(true);
            }
            if !is_any && !truthy {
                return Ok(false);
            }
        }
        return Ok(!is_any);
    }

    let mut index = 0usize;

    loop {
        let frames =
            evaluate_query_up_to(source, input, bindings, context, index.saturating_add(1))?;
        let Some(frame) = frames.get(index) else {
            return Ok(!is_any);
        };
        let truthy = query_produces_truthy(predicate, &frame.value, &frame.bindings, context)?;
        if is_any && truthy {
            return Ok(true);
        }
        if !is_any && !truthy {
            return Ok(false);
        }
        index = index.saturating_add(1);
    }
}

fn query_is_simple_eager_predicate_source(query: &Query) -> bool {
    if !query.functions.is_empty() {
        return false;
    }
    query.outputs.iter().all(simple_eager_predicate_pipeline)
}

fn query_is_simple_constant(query: &Query) -> bool {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return false;
    }
    query.outputs.iter().all(simple_constant_pipeline)
}

fn constant_string_query_value(
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<String>, AqError> {
    if !query_is_simple_constant(query) {
        return Ok(None);
    }
    let frames = evaluate_query_up_to(query, &Value::Null, bindings, context, 2)?;
    let Some(frame) = frames.first() else {
        return Ok(None);
    };
    if frames.len() != 1 {
        return Ok(None);
    }
    match frame.value.untagged() {
        Value::String(value) => Ok(Some(value.clone())),
        _ => Ok(None),
    }
}

fn constant_path_query_components(
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Option<Vec<PathComponent>>, AqError> {
    if !query_is_simple_constant(query) {
        return Ok(None);
    }
    let cache_key = format!("{query:?}");
    if let Some(path) = context.constant_path_components(&cache_key) {
        return Ok(path);
    }
    let frames = evaluate_query_up_to(query, &Value::Null, bindings, context, 2)?;
    let path = match frames.as_slice() {
        [frame] => Some(path_components_of("getpath", &frame.value)?),
        _ => None,
    };
    context.cache_constant_path_components(cache_key, path.clone());
    Ok(path)
}

fn query_is_plain_empty(query: &Query) -> bool {
    if !query.functions.is_empty() || !query.imported_values.is_empty() {
        return false;
    }
    !query.outputs.is_empty()
        && query.outputs.iter().all(|pipeline| {
            matches!(
                pipeline.stages.as_slice(),
                [Expr::Builtin(BuiltinExpr::Empty)]
            )
        })
}

fn simple_constant_pipeline(pipeline: &Pipeline) -> bool {
    let Some((first, rest)) = pipeline.stages.split_first() else {
        return true;
    };
    simple_constant_seed_expr(first) && rest.iter().all(simple_local_only_expr)
}

fn query_is_simple_local_only(query: &Query) -> bool {
    if !query.functions.is_empty() {
        return false;
    }
    query
        .outputs
        .iter()
        .all(|pipeline| pipeline.stages.iter().all(simple_local_only_expr))
}

fn simple_constant_seed_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::FormatString { parts, .. } => parts.iter().all(|part| match part {
            FormatStringPart::Literal(_) => true,
            FormatStringPart::Query(query) => query_is_simple_constant(query),
        }),
        Expr::Array(values) => values.iter().all(simple_constant_seed_expr),
        Expr::Object(entries) => entries.iter().all(|(key, value)| {
            let key_is_constant = match key {
                ObjectKey::Static(_) => true,
                ObjectKey::Dynamic(expr) => simple_constant_seed_expr(expr),
            };
            key_is_constant && simple_constant_seed_expr(value)
        }),
        Expr::Builtin(builtin) => simple_constant_seed_builtin(builtin),
        Expr::Subquery(query) => query_is_simple_constant(query),
        Expr::Unary { expr, .. } => simple_constant_seed_expr(expr),
        Expr::Binary { left, right, .. } => {
            simple_constant_seed_expr(left) && simple_constant_seed_expr(right)
        }
        Expr::Path(_)
        | Expr::Variable(_)
        | Expr::Access { .. }
        | Expr::FunctionCall { .. }
        | Expr::Bind { .. }
        | Expr::BindingAlt { .. }
        | Expr::Reduce { .. }
        | Expr::ForEach { .. }
        | Expr::If { .. }
        | Expr::Try { .. }
        | Expr::Label { .. }
        | Expr::Break(_)
        | Expr::Assign { .. } => false,
    }
}

fn simple_constant_seed_builtin(builtin: &BuiltinExpr) -> bool {
    matches!(
        builtin,
        BuiltinExpr::Builtins | BuiltinExpr::Infinite | BuiltinExpr::Nan | BuiltinExpr::Empty
    )
}

fn simple_local_only_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Path(_) | Expr::Literal(_) => true,
        Expr::FormatString { parts, .. } => parts.iter().all(|part| match part {
            FormatStringPart::Literal(_) => true,
            FormatStringPart::Query(query) => query_is_simple_local_only(query),
        }),
        Expr::Access { base, segments } => {
            simple_local_only_expr(base)
                && segments.iter().all(|segment| match segment {
                    Segment::Lookup { expr, .. } => simple_local_only_expr(expr),
                    _ => true,
                })
        }
        Expr::Array(values) => values.iter().all(simple_local_only_expr),
        Expr::Object(entries) => entries.iter().all(|(key, value)| {
            let key_is_local = match key {
                ObjectKey::Static(_) => true,
                ObjectKey::Dynamic(expr) => simple_local_only_expr(expr),
            };
            key_is_local && simple_local_only_expr(value)
        }),
        Expr::Builtin(builtin) => simple_local_only_builtin(builtin),
        Expr::Subquery(query) => query_is_simple_local_only(query),
        Expr::Unary { expr, .. } => simple_local_only_expr(expr),
        Expr::Binary { left, right, .. } => {
            simple_local_only_expr(left) && simple_local_only_expr(right)
        }
        Expr::Variable(_)
        | Expr::FunctionCall { .. }
        | Expr::Bind { .. }
        | Expr::BindingAlt { .. }
        | Expr::Reduce { .. }
        | Expr::ForEach { .. }
        | Expr::If { .. }
        | Expr::Try { .. }
        | Expr::Label { .. }
        | Expr::Break(_)
        | Expr::Assign { .. } => false,
    }
}

fn simple_local_only_builtin(builtin: &BuiltinExpr) -> bool {
    matches!(
        builtin,
        BuiltinExpr::Length
            | BuiltinExpr::Utf8ByteLength
            | BuiltinExpr::Keys
            | BuiltinExpr::KeysUnsorted
            | BuiltinExpr::Type
            | BuiltinExpr::Builtins
            | BuiltinExpr::First
            | BuiltinExpr::Last
            | BuiltinExpr::Reverse
            | BuiltinExpr::Sort
            | BuiltinExpr::Min
            | BuiltinExpr::Max
            | BuiltinExpr::Unique
            | BuiltinExpr::Flatten
            | BuiltinExpr::Floor
            | BuiltinExpr::Ceil
            | BuiltinExpr::Round
            | BuiltinExpr::Abs
            | BuiltinExpr::Fabs
            | BuiltinExpr::Sqrt
            | BuiltinExpr::Log
            | BuiltinExpr::Log2
            | BuiltinExpr::Log10
            | BuiltinExpr::Exp
            | BuiltinExpr::Exp2
            | BuiltinExpr::Sin
            | BuiltinExpr::Cos
            | BuiltinExpr::Tan
            | BuiltinExpr::Asin
            | BuiltinExpr::Acos
            | BuiltinExpr::Atan
            | BuiltinExpr::ToDate
            | BuiltinExpr::FromDate
            | BuiltinExpr::ToDateTime
            | BuiltinExpr::GmTime
            | BuiltinExpr::MkTime
            | BuiltinExpr::ToString
            | BuiltinExpr::ToNumber
            | BuiltinExpr::ToBool
            | BuiltinExpr::ToBoolean
            | BuiltinExpr::Infinite
            | BuiltinExpr::Nan
            | BuiltinExpr::IsNan
            | BuiltinExpr::Empty
    ) || match builtin {
        BuiltinExpr::StartsWith(expr)
        | BuiltinExpr::EndsWith(expr)
        | BuiltinExpr::FlattenDepth(expr)
        | BuiltinExpr::TrimStr(expr)
        | BuiltinExpr::LtrimStr(expr)
        | BuiltinExpr::RtrimStr(expr)
        | BuiltinExpr::StrFTime(expr)
        | BuiltinExpr::StrFLocalTime(expr)
        | BuiltinExpr::StrPTime(expr) => simple_local_only_expr(expr),
        BuiltinExpr::Split { pattern, flags } | BuiltinExpr::Splits { pattern, flags } => {
            query_is_simple_local_only(pattern)
                && flags
                    .as_ref()
                    .is_none_or(|flags| query_is_simple_local_only(flags))
        }
        _ => false,
    }
}

fn simple_eager_predicate_pipeline(pipeline: &Pipeline) -> bool {
    pipeline.stages.iter().all(simple_eager_predicate_expr)
}

fn simple_eager_predicate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Path(_) | Expr::Literal(_) | Expr::Variable(_) => true,
        Expr::FormatString { parts, .. } => parts.iter().all(|part| match part {
            FormatStringPart::Literal(_) => true,
            FormatStringPart::Query(query) => query_is_simple_eager_predicate_source(query),
        }),
        Expr::Access { base, segments } => {
            simple_eager_predicate_expr(base)
                && segments.iter().all(|segment| match segment {
                    Segment::Lookup { expr, .. } => simple_eager_predicate_expr(expr),
                    _ => true,
                })
        }
        Expr::Array(values) => values.iter().all(simple_eager_predicate_expr),
        Expr::Object(entries) => entries.iter().all(|(key, value)| {
            let dynamic_key_safe = match key {
                ObjectKey::Static(_) => true,
                ObjectKey::Dynamic(expr) => simple_eager_predicate_expr(expr),
            };
            dynamic_key_safe && simple_eager_predicate_expr(value)
        }),
        Expr::Builtin(builtin) => simple_eager_predicate_builtin(builtin),
        Expr::Subquery(query) => query_is_simple_eager_predicate_source(query),
        Expr::Unary { expr, .. } => simple_eager_predicate_expr(expr),
        Expr::Binary { left, right, .. } => {
            simple_eager_predicate_expr(left) && simple_eager_predicate_expr(right)
        }
        Expr::FunctionCall { .. }
        | Expr::Bind { .. }
        | Expr::BindingAlt { .. }
        | Expr::Reduce { .. }
        | Expr::ForEach { .. }
        | Expr::If { .. }
        | Expr::Try { .. }
        | Expr::Label { .. }
        | Expr::Break(_)
        | Expr::Assign { .. } => false,
    }
}

fn simple_eager_predicate_builtin(builtin: &BuiltinExpr) -> bool {
    matches!(
        builtin,
        BuiltinExpr::Builtins
            | BuiltinExpr::Length
            | BuiltinExpr::Utf8ByteLength
            | BuiltinExpr::Type
            | BuiltinExpr::Keys
            | BuiltinExpr::KeysUnsorted
            | BuiltinExpr::ToString
            | BuiltinExpr::ToNumber
            | BuiltinExpr::ToBool
            | BuiltinExpr::ToBoolean
            | BuiltinExpr::Infinite
            | BuiltinExpr::Nan
            | BuiltinExpr::IsNan
            | BuiltinExpr::Empty
            | BuiltinExpr::First
            | BuiltinExpr::Last
            | BuiltinExpr::Reverse
            | BuiltinExpr::Sort
            | BuiltinExpr::Min
            | BuiltinExpr::Max
            | BuiltinExpr::Unique
            | BuiltinExpr::Flatten
            | BuiltinExpr::Floor
            | BuiltinExpr::Ceil
            | BuiltinExpr::Round
            | BuiltinExpr::Abs
            | BuiltinExpr::Fabs
            | BuiltinExpr::Sqrt
            | BuiltinExpr::Log
            | BuiltinExpr::Log2
            | BuiltinExpr::Log10
            | BuiltinExpr::Exp
            | BuiltinExpr::Exp2
            | BuiltinExpr::Sin
            | BuiltinExpr::Cos
            | BuiltinExpr::Tan
            | BuiltinExpr::Asin
            | BuiltinExpr::Acos
            | BuiltinExpr::Atan
            | BuiltinExpr::ToDate
            | BuiltinExpr::FromDate
            | BuiltinExpr::ToDateTime
            | BuiltinExpr::GmTime
            | BuiltinExpr::MkTime
            | BuiltinExpr::Env
    ) || match builtin {
        BuiltinExpr::StartsWith(expr)
        | BuiltinExpr::EndsWith(expr)
        | BuiltinExpr::FlattenDepth(expr)
        | BuiltinExpr::TrimStr(expr)
        | BuiltinExpr::LtrimStr(expr)
        | BuiltinExpr::RtrimStr(expr) => simple_eager_predicate_expr(expr),
        BuiltinExpr::Split { pattern, flags } | BuiltinExpr::Splits { pattern, flags } => {
            query_is_simple_eager_predicate_source(pattern)
                && flags
                    .as_ref()
                    .is_none_or(|flags| query_is_simple_eager_predicate_source(flags))
        }
        _ => false,
    }
}

fn query_produces_truthy(
    predicate: &Query,
    value: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    if let Some(result) = simple_truthy_query(predicate, value) {
        return result;
    }
    if query_has_no_binding_stages(predicate) {
        return Ok(evaluate_query_values(predicate, value, bindings, context)?
            .into_iter()
            .any(|value| is_truthy(&value)));
    }
    Ok(evaluate_query(predicate, value, bindings, context)?
        .into_iter()
        .any(|frame| is_truthy(&frame.value)))
}

fn simple_truthy_query(query: &Query, input: &Value) -> Option<Result<bool, AqError>> {
    let expr = direct_stage_expr(query)?;
    simple_truthy_expr(expr, input)
}

fn simple_truthy_expr(expr: &Expr, input: &Value) -> Option<Result<bool, AqError>> {
    match expr {
        Expr::Literal(value) => Some(Ok(is_truthy(value))),
        Expr::Path(path) => evaluate_direct_static_path(path, input)
            .map(|values| values.map(|values| values.iter().any(is_truthy))),
        Expr::Builtin(BuiltinExpr::Has(key_expr)) => {
            literal_expr_value(key_expr).map(|key| value_has(input, key))
        }
        Expr::Builtin(BuiltinExpr::Contains(expected_expr)) => {
            literal_expr_value(expected_expr).map(|expected| contains_value(input, expected))
        }
        Expr::Builtin(BuiltinExpr::Inside(container_expr)) => {
            literal_expr_value(container_expr).map(|container| contains_value(container, input))
        }
        Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } => simple_comparison_truthy(left, right, input, |left, right| left == right),
        Expr::Binary {
            left,
            op: BinaryOp::Ne,
            right,
        } => simple_comparison_truthy(left, right, input, |left, right| left != right),
        _ => None,
    }
}

fn simple_comparison_truthy<F>(
    left: &Expr,
    right: &Expr,
    input: &Value,
    compare: F,
) -> Option<Result<bool, AqError>>
where
    F: Fn(&Value, &Value) -> bool,
{
    if let (Expr::Path(path), Some(literal)) = (left, literal_expr_value(right)) {
        return evaluate_direct_static_path(path, input)
            .map(|values| values.map(|values| values.iter().any(|value| compare(value, literal))));
    }
    if let (Some(literal), Expr::Path(path)) = (literal_expr_value(left), right) {
        return evaluate_direct_static_path(path, input)
            .map(|values| values.map(|values| values.iter().any(|value| compare(literal, value))));
    }
    None
}

fn join_of(
    input: &Value,
    separator_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let values = expect_array_input("join", input)?;
    let separators = evaluate_expr(separator_expr, input, bindings, context)?;
    let mut out = Vec::new();
    for separator in separators {
        let Value::String(separator) = separator else {
            return Err(AqError::Query(format!(
                "join requires a string argument, got {}",
                kind_name(&separator)
            )));
        };
        let mut joined = String::with_capacity(
            separator
                .len()
                .saturating_mul(values.len().saturating_sub(1)),
        );
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                joined.push_str(&separator);
            }
            append_join_fragment(&mut joined, value)?;
        }
        out.push(Value::String(joined));
    }
    Ok(out)
}

fn join_input_of(
    input: &Value,
    index_query: &Query,
    key_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let index_storage;
    let index = if let Some(index_value) = borrowed_single_query_value(index_query, input, bindings)
    {
        expect_object_input("JOIN", index_value)?
    } else {
        index_storage = evaluate_single_query_value("JOIN", index_query, input, bindings, context)?;
        expect_object_input("JOIN", &index_storage)?
    };
    let mut joined = Vec::new();
    for item in iterate_input_values(input)? {
        joined.push(join_row_value(index, &item, key_query, bindings, context)?);
    }
    Ok(Value::Array(joined))
}

fn join_stream_of(
    input: &Value,
    index_query: &Query,
    source_query: &Query,
    key_query: &Query,
    join_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let index_storage;
    let index = if let Some(index_value) = borrowed_single_query_value(index_query, input, bindings)
    {
        expect_object_input("JOIN", index_value)?
    } else {
        index_storage = evaluate_single_query_value("JOIN", index_query, input, bindings, context)?;
        expect_object_input("JOIN", &index_storage)?
    };
    let mut out = Vec::new();
    for frame in evaluate_query(source_query, input, bindings, context)? {
        let joined = join_row_value(index, &frame.value, key_query, &frame.bindings, context)?;
        if let Some(join_query) = join_query {
            out.extend(
                evaluate_query(join_query, &joined, &frame.bindings, context)?
                    .into_iter()
                    .map(|frame| frame.value),
            );
        } else {
            out.push(joined);
        }
    }
    Ok(out)
}

fn join_row_value(
    index: &IndexMap<String, Value>,
    item: &Value,
    key_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let keys = if query_has_no_binding_stages(key_query) {
        evaluate_query_values(key_query, item, bindings, context)?
    } else {
        evaluate_query(key_query, item, bindings, context)?
            .into_iter()
            .map(|frame| frame.value)
            .collect()
    };
    let mut joined = Vec::with_capacity(keys.len().saturating_add(1));
    joined.push(item.clone());
    for key_value in keys {
        let key = to_string_of(&key_value)?;
        joined.push(index.get(&key).cloned().unwrap_or(Value::Null));
    }
    Ok(Value::Array(joined))
}

fn iterate_input_values(input: &Value) -> Result<Vec<Value>, AqError> {
    match input.untagged() {
        Value::Array(values) => Ok(values.clone()),
        Value::Object(values) => Ok(values.values().cloned().collect()),
        other => Err(iterate_error(other)),
    }
}

fn append_join_fragment(out: &mut String, value: &Value) -> Result<(), AqError> {
    match value.untagged() {
        Value::Null => Ok(()),
        Value::String(value) => {
            out.push_str(value);
            Ok(())
        }
        Value::Bool(value) => {
            out.push_str(if *value { "true" } else { "false" });
            Ok(())
        }
        Value::Integer(value) => {
            let _ = write!(out, "{value}");
            Ok(())
        }
        Value::Decimal(value) => {
            out.push_str(value.rendered());
            Ok(())
        }
        Value::Float(value) => {
            let _ = write!(out, "{value}");
            Ok(())
        }
        other => Err(binary_type_error(
            &Value::String(out.clone()),
            other,
            "cannot be added",
        )),
    }
}

fn ascii_downcase_of(input: &Value) -> Result<String, AqError> {
    let value = expect_string_input("ascii_downcase", input)?;
    Ok(value.to_ascii_lowercase())
}

fn ascii_upcase_of(input: &Value) -> Result<String, AqError> {
    let value = expect_string_input("ascii_upcase", input)?;
    Ok(value.to_ascii_uppercase())
}

fn trim_of(input: &Value) -> Result<&str, AqError> {
    let value = expect_trim_string_input(input)?;
    Ok(trim_ascii_or_unicode(value))
}

fn ltrim_of(input: &Value) -> Result<&str, AqError> {
    let value = expect_trim_string_input(input)?;
    Ok(trim_ascii_start_or_unicode(value))
}

fn rtrim_of(input: &Value) -> Result<&str, AqError> {
    let value = expect_trim_string_input(input)?;
    Ok(trim_ascii_end_or_unicode(value))
}

fn to_entries_of(input: &Value) -> Result<Value, AqError> {
    match input.untagged() {
        Value::Object(values) => Ok(Value::Array(
            values
                .iter()
                .map(|(key, value)| entry_value(Value::String(key.clone()), value.clone()))
                .collect(),
        )),
        Value::Array(values) => Ok(Value::Array(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    entry_value(
                        Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)),
                        value.clone(),
                    )
                })
                .collect(),
        )),
        other => Err(AqError::Query(format!(
            "to_entries is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn from_entries_of(input: &Value) -> Result<Value, AqError> {
    let entries = expect_array_input("from_entries", input)?;
    from_entry_values(entries)
}

fn with_entries_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let entries = match to_entries_of(input)? {
        Value::Array(entries) => entries,
        other => {
            return Err(AqError::Query(format!(
                "internal error: to_entries returned {}",
                kind_name(&other)
            )));
        }
    };

    let mut mapped = Vec::new();
    for entry in &entries {
        mapped.extend(evaluate_expr(expr, entry, bindings, context)?);
    }
    from_entry_values(&mapped)
}

fn sort_by_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut keyed = keyed_values_of(input, expr, bindings, "sort_by", context)?;
    keyed.sort_by(|(left_key, _), (right_key, _)| compare_sort_values(left_key, right_key));
    Ok(Value::Array(
        keyed.into_iter().map(|(_, value)| value).collect(),
    ))
}

fn sort_by_desc_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut keyed = keyed_values_of(input, expr, bindings, "sort_by_desc", context)?;
    keyed.sort_by(|(left_key, _), (right_key, _)| compare_sort_values(right_key, left_key));
    Ok(Value::Array(
        keyed.into_iter().map(|(_, value)| value).collect(),
    ))
}

fn group_by_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut keyed = keyed_values_of(input, expr, bindings, "group_by", context)?;
    keyed.sort_by(|(left_key, _), (right_key, _)| compare_sort_values(left_key, right_key));

    let mut groups: Vec<Value> = Vec::new();
    let mut current_key: Option<Value> = None;
    let mut current_group: Vec<Value> = Vec::new();

    for (key, value) in keyed {
        if current_key.as_ref().is_some_and(|current| current == &key) {
            current_group.push(value);
            continue;
        }

        if !current_group.is_empty() {
            groups.push(Value::Array(current_group));
            current_group = Vec::new();
        }
        current_key = Some(key);
        current_group.push(value);
    }

    if !current_group.is_empty() {
        groups.push(Value::Array(current_group));
    }

    Ok(Value::Array(groups))
}

fn unique_by_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut keyed = keyed_values_of(input, expr, bindings, "unique_by", context)?;
    keyed.sort_by(|(left_key, _), (right_key, _)| compare_sort_values(left_key, right_key));

    let mut unique = Vec::new();
    let mut previous_key: Option<Value> = None;
    for (key, value) in keyed {
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous == &key)
        {
            continue;
        }
        previous_key = Some(key);
        unique.push(value);
    }
    Ok(Value::Array(unique))
}

fn count_by_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut keyed = keyed_values_of(input, expr, bindings, "count_by", context)?;
    keyed.sort_by(|(left_key, _), (right_key, _)| compare_sort_values(left_key, right_key));

    let mut counts = Vec::new();
    let mut current_key: Option<Value> = None;
    let mut current_count = 0usize;

    for (key, _) in keyed {
        if current_key.as_ref().is_some_and(|current| current == &key) {
            current_count += 1;
            continue;
        }

        if let Some(previous_key) = current_key.take() {
            counts.push(count_entry_value(previous_key, current_count));
        }

        current_key = Some(key);
        current_count = 1;
    }

    if let Some(previous_key) = current_key {
        counts.push(count_entry_value(previous_key, current_count));
    }

    Ok(Value::Array(counts))
}

fn columns_of(input: &Value) -> Result<Value, AqError> {
    match input.untagged() {
        Value::Object(values) => Ok(Value::Array(
            values.keys().cloned().map(Value::String).collect(),
        )),
        Value::Array(rows) => {
            let mut columns = IndexMap::<String, ()>::new();
            for row in rows {
                match row {
                    Value::Object(values) => {
                        for key in values.keys() {
                            columns.entry(key.clone()).or_insert(());
                        }
                    }
                    Value::Null => {}
                    other => {
                        return Err(AqError::Query(format!(
                            "columns expects objects or arrays of objects, got array containing {}",
                            kind_name(other)
                        )));
                    }
                }
            }
            Ok(Value::Array(
                columns.into_keys().map(Value::String).collect(),
            ))
        }
        other => Err(AqError::Query(format!(
            "columns is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn yaml_tag_of(
    input: &Value,
    query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    match query {
        None => Ok(input
            .yaml_tag()
            .map(|tag| Value::String(tag.to_string()))
            .unwrap_or(Value::Null)),
        Some(query) => {
            let value = evaluate_single_query_value("yaml_tag", query, input, bindings, context)?;
            match value.untagged() {
                Value::Null => Ok(input.without_yaml_tag()),
                Value::String(tag) if tag.is_empty() => Err(AqError::Query(
                    "yaml_tag expects a non-empty string tag".to_string(),
                )),
                Value::String(tag) => Ok(input.with_yaml_tag(tag.clone())),
                other => Err(AqError::Query(format!(
                    "yaml_tag expects a string tag or null, got {}",
                    kind_name(other)
                ))),
            }
        }
    }
}

fn xml_attributes_of(input: &Value) -> Result<Option<&IndexMap<String, Value>>, AqError> {
    let fields = expect_object_input("xml_attr", input)?;
    match fields.get("attributes") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => match value.untagged() {
            Value::Object(attributes) => Ok(Some(attributes)),
            other => Err(AqError::Query(format!(
                "xml_attr expects `.attributes` to be an object, got {}",
                kind_name(other)
            ))),
        },
    }
}

fn xml_attr_of(
    input: &Value,
    query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let attributes = xml_attributes_of(input)?;
    match query {
        None => Ok(attributes
            .cloned()
            .map(Value::Object)
            .unwrap_or(Value::Null)),
        Some(query) => {
            let key = evaluate_single_query_value("xml_attr", query, input, bindings, context)?;
            match key.untagged() {
                Value::String(key) => Ok(attributes
                    .and_then(|attributes| attributes.get(key))
                    .cloned()
                    .unwrap_or(Value::Null)),
                other => Err(AqError::Query(format!(
                    "xml_attr expects a string attribute name, got {}",
                    kind_name(other)
                ))),
            }
        }
    }
}

fn csv_header_names_of(value: &Value) -> Result<Vec<String>, AqError> {
    let header = expect_array_input("csv_header", value)?;
    let mut names = Vec::with_capacity(header.len());
    let mut seen = BTreeSet::new();
    for field in header {
        match field.untagged() {
            Value::String(name) => {
                if !seen.insert(name.clone()) {
                    return Err(AqError::Query(format!(
                        "csv_header requires unique header names, found duplicate `{name}`"
                    )));
                }
                names.push(name.clone());
            }
            other => {
                return Err(AqError::Query(format!(
                    "csv_header expects header fields to be strings, got {}",
                    kind_name(other)
                )));
            }
        }
    }
    Ok(names)
}

fn csv_header_row_of(header: &[String], row: &Value) -> Result<Value, AqError> {
    let values = expect_array_input("csv_header", row)?;
    if values.len() != header.len() {
        return Err(AqError::Query(format!(
            "csv_header requires row width {} to match header width {}",
            values.len(),
            header.len()
        )));
    }
    let mut out = IndexMap::with_capacity(header.len());
    for (key, value) in header.iter().zip(values.iter()) {
        out.insert(key.clone(), value.clone());
    }
    Ok(Value::Object(out))
}

fn csv_header_of(
    input: &Value,
    query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    match query {
        Some(query) => {
            let header =
                evaluate_single_query_value("csv_header", query, input, bindings, context)?;
            let header = csv_header_names_of(&header)?;
            csv_header_row_of(&header, input)
        }
        None => {
            let rows = expect_array_input("csv_header", input)?;
            let Some((header_row, body)) = rows.split_first() else {
                return Ok(Value::Array(Vec::new()));
            };
            let header = csv_header_names_of(header_row)?;
            let mut out = Vec::with_capacity(body.len());
            for row in body {
                out.push(csv_header_row_of(&header, row)?);
            }
            Ok(Value::Array(out))
        }
    }
}

fn optional_flag_of(
    query: Option<&Query>,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    match query {
        Some(query) => query_is_truthy(query, input, bindings, context),
        None => Ok(false),
    }
}

fn merge_of(
    input: &Value,
    value_query: &Query,
    deep_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let deep = optional_flag_of(deep_query, input, bindings, context)?;
    Ok(evaluate_query(value_query, input, bindings, context)?
        .into_iter()
        .map(|frame| input.merged_with(&frame.value, deep))
        .collect())
}

fn merge_all_of(
    input: &Value,
    deep_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let deep = optional_flag_of(deep_query, input, bindings, context)?;
    let Value::Array(values) = input else {
        return Err(AqError::Query("merge_all expects an array".to_string()));
    };
    let mut values = values.iter();
    let Some(mut merged) = values.next().cloned() else {
        return Err(AqError::Query(
            "merge_all expects a non-empty array".to_string(),
        ));
    };
    for value in values {
        merged = merged.merged_with(value, deep);
    }
    Ok(merged)
}

fn sort_keys_of(
    input: &Value,
    recursive_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let recursive = optional_flag_of(recursive_query, input, bindings, context)?;
    Ok(input.sort_object_keys(recursive))
}

fn drop_nulls_of(
    input: &Value,
    recursive_query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let recursive = optional_flag_of(recursive_query, input, bindings, context)?;
    Ok(input.drop_nulls(recursive))
}

fn pick_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let paths = exact_paths_of("pick", query, input, bindings, context)?;
    let mut projected = Value::Null;
    for path in paths {
        let value = getpath_value(input, &path)?;
        projected = setpath_value(&projected, &path, &value)?;
    }
    Ok(projected)
}

fn omit_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let paths = exact_paths_of("omit", query, input, bindings, context)?;
    delpaths_value(input, &paths)
}

fn del_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let paths = exact_paths_of("del", query, input, bindings, context)?;
    delpaths_value(input, &paths)
}

fn rename_of(
    input: &Value,
    path_query: &Query,
    name_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let paths = exact_paths_of("rename", path_query, input, bindings, context)?;
    let name = rename_name_of(input, name_query, bindings, context)?;
    let mut renamed = input.clone();
    for path in paths {
        renamed = rename_path_value(&renamed, &path, &name)?;
    }
    Ok(renamed)
}

fn rename_name_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    match evaluate_single_query_value("rename", query, input, bindings, context)? {
        Value::String(name) => Ok(name),
        other => Err(AqError::Query(format!(
            "rename requires a string target name, got {}",
            kind_name(&other)
        ))),
    }
}

fn min_by_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    if let Some(value) = try_minmax_by_direct_key_of(input, expr, bindings, false)? {
        return Ok(value);
    }
    let keyed = keyed_values_of(input, expr, bindings, "min_by", context)?;
    Ok(keyed
        .into_iter()
        .min_by(|(left_key, _), (right_key, _)| compare_sort_values(left_key, right_key))
        .map(|(_, value)| value)
        .unwrap_or(Value::Null))
}

fn max_by_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    if let Some(value) = try_minmax_by_direct_key_of(input, expr, bindings, true)? {
        return Ok(value);
    }
    let keyed = keyed_values_of(input, expr, bindings, "max_by", context)?;
    Ok(keyed
        .into_iter()
        .max_by(|(left_key, _), (right_key, _)| compare_sort_values(left_key, right_key))
        .map(|(_, value)| value)
        .unwrap_or(Value::Null))
}

fn try_minmax_by_direct_key_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    choose_max: bool,
) -> Result<Option<Value>, AqError> {
    let values = match input.untagged() {
        Value::Array(values) => values,
        _ => return Ok(None),
    };
    let Some((first, rest)) = values.split_first() else {
        return Ok(Some(Value::Null));
    };
    let Some(mut best_key) = direct_single_value_expr_value(expr, first, bindings) else {
        return Ok(None);
    };
    let mut best_value = first.clone();
    for value in rest {
        let Some(key) = direct_single_value_expr_value(expr, value, bindings) else {
            return Ok(None);
        };
        let ordering = compare_sort_values(&key, &best_key);
        let should_replace = if choose_max {
            ordering != std::cmp::Ordering::Less
        } else {
            ordering == std::cmp::Ordering::Less
        };
        if should_replace {
            best_key = key;
            best_value = value.clone();
        }
    }
    Ok(Some(best_value))
}

fn getpath_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(path) = constant_path_query_components(query, bindings, context)? {
        return Ok(vec![getpath_value(input, &path)?]);
    }
    let path_values = evaluate_query(query, input, bindings, context)?;
    let mut values = Vec::with_capacity(path_values.len());
    for frame in path_values {
        let path = path_components_of("getpath", &frame.value)?;
        values.push(getpath_value(input, &path)?);
    }
    Ok(values)
}

fn getpath_value(input: &Value, path: &[PathComponent]) -> Result<Value, AqError> {
    enum PathLookupValue<'a> {
        Borrowed(&'a Value),
        Owned(Value),
        Null,
    }

    impl<'a> PathLookupValue<'a> {
        fn from_borrowed(value: &'a Value) -> Self {
            match value {
                Value::Tagged { value, .. } => Self::from_borrowed(value),
                Value::Null => Self::Null,
                other => Self::Borrowed(other),
            }
        }

        fn from_owned(value: Value) -> Self {
            match value {
                Value::Tagged { value, .. } => Self::from_owned(*value),
                Value::Null => Self::Null,
                other => Self::Owned(other),
            }
        }

        fn into_value(self) -> Value {
            match self {
                Self::Borrowed(value) => value.clone(),
                Self::Owned(value) => value,
                Self::Null => Value::Null,
            }
        }
    }

    let mut current = PathLookupValue::from_borrowed(input);
    for component in path {
        current = match component {
            PathComponent::Field(name) => match current {
                PathLookupValue::Borrowed(current) => match current.untagged() {
                    Value::Object(fields) => fields
                        .get(name)
                        .map(PathLookupValue::from_borrowed)
                        .unwrap_or(PathLookupValue::Null),
                    _other => {
                        return Err(index_lookup_error(current, &Value::String(name.clone())))
                    }
                },
                PathLookupValue::Owned(current) => match current.untagged() {
                    Value::Object(fields) => fields
                        .get(name)
                        .cloned()
                        .map(PathLookupValue::from_owned)
                        .unwrap_or(PathLookupValue::Null),
                    _other => {
                        return Err(index_lookup_error(&current, &Value::String(name.clone())))
                    }
                },
                PathLookupValue::Null => PathLookupValue::Null,
            },
            PathComponent::Index(index) => match current {
                PathLookupValue::Borrowed(current) => match current.untagged() {
                    Value::Array(items) => resolve_index(*index, items.len())
                        .and_then(|resolved| items.get(resolved))
                        .map(PathLookupValue::from_borrowed)
                        .unwrap_or(PathLookupValue::Null),
                    _other => {
                        return Err(index_lookup_error(current, &Value::Integer(*index as i64)));
                    }
                },
                PathLookupValue::Owned(current) => match current.untagged() {
                    Value::Array(items) => resolve_index(*index, items.len())
                        .and_then(|resolved| items.get(resolved))
                        .cloned()
                        .map(PathLookupValue::from_owned)
                        .unwrap_or(PathLookupValue::Null),
                    _other => {
                        return Err(index_lookup_error(&current, &Value::Integer(*index as i64)));
                    }
                },
                PathLookupValue::Null => PathLookupValue::Null,
            },
            PathComponent::Slice { start, end } => match current {
                PathLookupValue::Borrowed(current) => match current.untagged() {
                    Value::Array(items) => {
                        let (start, end) = resolve_slice_bounds(*start, *end, items.len());
                        PathLookupValue::from_owned(Value::Array(items[start..end].to_vec()))
                    }
                    Value::String(text) => {
                        PathLookupValue::from_owned(Value::String(slice_string(text, *start, *end)))
                    }
                    _other => {
                        return Err(index_lookup_error(
                            current,
                            &slice_component_value(*start, *end),
                        ));
                    }
                },
                PathLookupValue::Owned(current) => match current.untagged() {
                    Value::Array(items) => {
                        let (start, end) = resolve_slice_bounds(*start, *end, items.len());
                        PathLookupValue::from_owned(Value::Array(items[start..end].to_vec()))
                    }
                    Value::String(text) => {
                        PathLookupValue::from_owned(Value::String(slice_string(text, *start, *end)))
                    }
                    _other => {
                        return Err(index_lookup_error(
                            &current,
                            &slice_component_value(*start, *end),
                        ));
                    }
                },
                PathLookupValue::Null => PathLookupValue::Null,
            },
        };
    }
    Ok(current.into_value())
}

fn setpath_of(
    input: &Value,
    path_query: &Query,
    value_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let path_value = evaluate_single_query_value("setpath", path_query, input, bindings, context)?;
    let path = setpath_path_of(input, &path_value)?;
    let mut out = Vec::new();
    for value in evaluate_query(value_query, input, bindings, context)?
        .into_iter()
        .map(|frame| frame.value)
    {
        out.push(setpath_value(input, &path, &value)?);
    }
    Ok(out)
}

fn setpath_path_of(input: &Value, value: &Value) -> Result<Vec<PathComponent>, AqError> {
    let Value::Array(components) = value.untagged() else {
        return Err(AqError::Query("setpath expects an array path".to_string()));
    };

    let mut current = Some(input);
    let mut path = Vec::with_capacity(components.len());
    for component in components {
        match component.untagged() {
            Value::String(name) => {
                current = match current.map(Value::untagged) {
                    Some(Value::Object(fields)) => fields.get(name),
                    Some(Value::Null) | None => None,
                    Some(other) => {
                        return Err(index_lookup_error(other, &Value::String(name.clone())))
                    }
                };
                path.push(PathComponent::Field(name.clone()));
            }
            Value::Integer(index) => {
                let resolved = isize::try_from(*index).map_err(|_| {
                    AqError::Query("setpath path index is out of range".to_string())
                })?;
                current = match current.map(Value::untagged) {
                    Some(Value::Array(items)) => resolve_index(resolved, items.len())
                        .and_then(|resolved| items.get(resolved)),
                    Some(Value::Null) | None => None,
                    Some(other) => return Err(index_lookup_error(other, &Value::Integer(*index))),
                };
                path.push(PathComponent::Index(resolved));
            }
            Value::Array(_) => {
                return Err(match current.map(Value::untagged) {
                    Some(Value::Array(_)) => {
                        AqError::Query("Cannot update field at array index of array".to_string())
                    }
                    Some(other) => index_lookup_kind_error(other, component),
                    None => index_lookup_kind_error(&Value::Null, component),
                });
            }
            other => {
                return Err(AqError::Query(format!(
                    "setpath path components must be strings or integers, got {}",
                    kind_name(other)
                )));
            }
        }
    }

    Ok(path)
}

fn setpath_value(
    input: &Value,
    path: &[PathComponent],
    replacement: &Value,
) -> Result<Value, AqError> {
    let mut updated = input.clone();
    setpath_value_in_place(&mut updated, path, replacement)?;
    Ok(updated)
}

fn setpath_value_in_place(
    input: &mut Value,
    path: &[PathComponent],
    replacement: &Value,
) -> Result<(), AqError> {
    if let Value::Tagged { value, .. } = input {
        return setpath_value_in_place(value.as_mut(), path, replacement);
    }

    let Some((component, tail)) = path.split_first() else {
        *input = replacement.clone();
        return Ok(());
    };

    match component {
        PathComponent::Field(name) => setpath_field_in_place(input, name, tail, replacement),
        PathComponent::Index(index) => setpath_index_in_place(input, *index, tail, replacement),
        PathComponent::Slice { start, end } => {
            *input = setpath_slice(input, *start, *end, tail, replacement)?;
            Ok(())
        }
    }
}

fn setpath_field_in_place(
    input: &mut Value,
    name: &str,
    tail: &[PathComponent],
    replacement: &Value,
) -> Result<(), AqError> {
    match input {
        Value::Object(fields) => {
            if tail.is_empty() {
                fields.insert(name.to_string(), replacement.clone());
                return Ok(());
            }
            let child = fields.entry(name.to_string()).or_insert(Value::Null);
            setpath_value_in_place(child, tail, replacement)
        }
        Value::Null => {
            *input = Value::Object(IndexMap::new());
            setpath_field_in_place(input, name, tail, replacement)
        }
        other => Err(index_lookup_error(other, &Value::String(name.to_string()))),
    }
}

fn setpath_index_in_place(
    input: &mut Value,
    index: isize,
    tail: &[PathComponent],
    replacement: &Value,
) -> Result<(), AqError> {
    match input {
        Value::Array(items) => {
            let resolved = if index < 0 {
                resolve_index(index, items.len()).ok_or_else(|| {
                    AqError::Query("Out of bounds negative array index".to_string())
                })?
            } else {
                usize::try_from(index)
                    .map_err(|_| AqError::Query("array index is out of range".to_string()))?
            };
            if resolved >= items.len() {
                if resolved > MAX_AUTO_GROW_ARRAY_INDEX {
                    return Err(AqError::Query("Array index too large".to_string()));
                }
                if tail.is_empty() {
                    items.resize(resolved, Value::Null);
                    items.push(replacement.clone());
                    return Ok(());
                }
                items.resize(resolved + 1, Value::Null);
            }
            if tail.is_empty() {
                items[resolved] = replacement.clone();
                return Ok(());
            }
            setpath_value_in_place(&mut items[resolved], tail, replacement)
        }
        Value::Null => {
            if index < 0 {
                return Err(AqError::Query(
                    "Out of bounds negative array index".to_string(),
                ));
            }
            let resolved = usize::try_from(index)
                .map_err(|_| AqError::Query("array index is out of range".to_string()))?;
            if resolved > MAX_AUTO_GROW_ARRAY_INDEX {
                return Err(AqError::Query("Array index too large".to_string()));
            }
            if tail.is_empty() {
                let mut values = Vec::new();
                values.resize(resolved, Value::Null);
                values.push(replacement.clone());
                *input = Value::Array(values);
                return Ok(());
            }
            *input = Value::Array(Vec::new());
            setpath_index_in_place(input, index, tail, replacement)
        }
        other => Err(index_lookup_error(other, &Value::Integer(index as i64))),
    }
}

fn setpath_slice(
    input: &Value,
    start: Option<isize>,
    end: Option<isize>,
    tail: &[PathComponent],
    replacement: &Value,
) -> Result<Value, AqError> {
    match input {
        Value::Array(items) => {
            let (start, end) = resolve_slice_bounds(start, end, items.len());
            let child = Value::Array(items[start..end].to_vec());
            let updated_child = setpath_value(&child, tail, replacement)?;
            let Value::Array(updated_slice) = updated_child.untagged() else {
                return Err(AqError::Query(
                    "A slice of an array can only be assigned another array".to_string(),
                ));
            };

            let mut updated =
                Vec::with_capacity(start + updated_slice.len() + items.len().saturating_sub(end));
            updated.extend_from_slice(&items[..start]);
            updated.extend(updated_slice.iter().cloned());
            updated.extend_from_slice(&items[end..]);
            Ok(Value::Array(updated))
        }
        Value::Null => {
            let updated_child = setpath_value(&Value::Array(Vec::new()), tail, replacement)?;
            let Value::Array(updated_slice) = updated_child.untagged() else {
                return Err(AqError::Query(
                    "A slice of an array can only be assigned another array".to_string(),
                ));
            };
            Ok(Value::Array(updated_slice.clone()))
        }
        Value::String(_) => Err(AqError::Query("Cannot update string slices".to_string())),
        _other => Err(index_lookup_error(
            input,
            &slice_component_value(start, end),
        )),
    }
}

fn delpaths_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let paths = paths_of("delpaths", query, input, bindings, context)?;
    delpaths_value(input, &paths)
}

fn paths_of_builtin(
    input: &Value,
    query: Option<&Query>,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    collect_paths_builtin_values(input, &mut Vec::new(), query, bindings, &mut out, context)?;
    Ok(out)
}

fn leaf_paths_of_builtin(input: &Value) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    collect_leaf_paths_builtin_values(input, &mut Vec::new(), &mut out);
    Ok(out)
}

fn tostream_of(input: &Value) -> Result<Vec<Value>, AqError> {
    let mut out = Vec::new();
    append_tostream_entries(input, &mut Vec::new(), &mut out);
    Ok(out)
}

fn append_tostream_entries(input: &Value, path: &mut Vec<Value>, out: &mut Vec<Value>) {
    match input.untagged() {
        Value::Array(values) if !values.is_empty() => {
            for (index, value) in values.iter().enumerate() {
                path.push(Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)));
                append_tostream_entries(value, path, out);
                path.pop();
            }
            let mut close_path = path.clone();
            close_path.push(Value::Integer(
                i64::try_from(values.len().saturating_sub(1)).unwrap_or(i64::MAX),
            ));
            out.push(Value::Array(vec![Value::Array(close_path)]));
        }
        Value::Object(values) if !values.is_empty() => {
            let mut last_key = None;
            for (key, value) in values {
                path.push(Value::String(key.clone()));
                append_tostream_entries(value, path, out);
                path.pop();
                last_key = Some(key.clone());
            }
            if let Some(last_key) = last_key {
                let mut close_path = path.clone();
                close_path.push(Value::String(last_key));
                out.push(Value::Array(vec![Value::Array(close_path)]));
            }
        }
        _ => out.push(Value::Array(vec![
            Value::Array(path.clone()),
            input.clone(),
        ])),
    }
}

fn truncate_stream_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let count = non_negative_stream_count("truncate_stream", input)?;
    let mut out = Vec::new();
    for frame in evaluate_query(query, &Value::Null, bindings, context)? {
        let (path, value) = stream_entry_parts("truncate_stream", &frame.value)?;
        if path.len() <= count {
            continue;
        }
        let truncated_path = path_to_value(&path[count..]);
        out.push(match value {
            Some(value) => Value::Array(vec![truncated_path, value]),
            None => Value::Array(vec![truncated_path]),
        });
    }
    Ok(out)
}

fn fromstream_of(
    input: &Value,
    query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut state = Value::Null;
    let mut should_emit = false;
    let mut out = Vec::new();
    for frame in evaluate_query(query, input, bindings, context)? {
        if should_emit {
            state = Value::Null;
        }
        let (path, value) = stream_entry_parts("fromstream", &frame.value)?;
        match value {
            Some(value) => {
                should_emit = path.is_empty();
                state = setpath_value(&state, &path, &value)?;
            }
            None => {
                should_emit = path.len() == 1;
            }
        }
        if should_emit {
            out.push(state.clone());
        }
    }
    Ok(out)
}

fn non_negative_stream_count(name: &str, input: &Value) -> Result<usize, AqError> {
    match input.untagged() {
        Value::Integer(value) => usize::try_from(*value)
            .map_err(|_| AqError::Query(format!("{name} requires a non-negative integer input"))),
        Value::Decimal(value) => usize::try_from(value.as_i64_exact().ok_or_else(|| {
            AqError::Query(format!("{name} requires a non-negative integer input"))
        })?)
        .map_err(|_| AqError::Query(format!("{name} requires a non-negative integer input"))),
        Value::Float(value) if value.is_finite() && value.fract() == 0.0 && *value >= 0.0 => {
            if *value > usize::MAX as f64 {
                Ok(usize::MAX)
            } else {
                Ok(*value as usize)
            }
        }
        _ => Err(AqError::Query(format!(
            "{name} requires a non-negative integer input"
        ))),
    }
}

fn stream_entry_parts(
    name: &str,
    value: &Value,
) -> Result<(Vec<PathComponent>, Option<Value>), AqError> {
    let Value::Array(parts) = value.untagged() else {
        return Err(AqError::Query(format!(
            "{name} expects stream entries as arrays"
        )));
    };
    match parts.as_slice() {
        [path] => Ok((path_components_of(name, path)?, None)),
        [path, value] => Ok((path_components_of(name, path)?, Some(value.clone()))),
        _ => Err(AqError::Query(format!(
            "{name} expects stream entries with one path and an optional value"
        ))),
    }
}

fn collect_paths_builtin_values(
    input: &Value,
    path: &mut Vec<Value>,
    query: Option<&Query>,
    bindings: &Bindings,
    out: &mut Vec<Value>,
    context: &EvaluationContext,
) -> Result<(), AqError> {
    match input.untagged() {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                path.push(Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)));
                let should_include = match query {
                    Some(query) => query_is_truthy(query, value, bindings, context)?,
                    None => true,
                };
                if should_include {
                    out.push(Value::Array(path.clone()));
                }
                collect_paths_builtin_values(value, path, query, bindings, out, context)?;
                path.pop();
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                path.push(Value::String(key.clone()));
                let should_include = match query {
                    Some(query) => query_is_truthy(query, value, bindings, context)?,
                    None => true,
                };
                if should_include {
                    out.push(Value::Array(path.clone()));
                }
                collect_paths_builtin_values(value, path, query, bindings, out, context)?;
                path.pop();
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_leaf_paths_builtin_values(input: &Value, path: &mut Vec<Value>, out: &mut Vec<Value>) {
    match input.untagged() {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                path.push(Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)));
                if !matches!(value.untagged(), Value::Array(_) | Value::Object(_)) {
                    out.push(Value::Array(path.clone()));
                }
                collect_leaf_paths_builtin_values(value, path, out);
                path.pop();
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                path.push(Value::String(key.clone()));
                if !matches!(value.untagged(), Value::Array(_) | Value::Object(_)) {
                    out.push(Value::Array(path.clone()));
                }
                collect_leaf_paths_builtin_values(value, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

fn rename_path_value(
    input: &Value,
    path: &[PathComponent],
    new_name: &str,
) -> Result<Value, AqError> {
    let Some((last, prefix)) = path.split_last() else {
        return Ok(input.clone());
    };
    let PathComponent::Field(old_name) = last else {
        return Err(AqError::Query(
            "rename requires exact field paths".to_string(),
        ));
    };
    rename_field_at_prefix(input, prefix, old_name, new_name)
}

fn rename_field_at_prefix(
    input: &Value,
    prefix: &[PathComponent],
    old_name: &str,
    new_name: &str,
) -> Result<Value, AqError> {
    if let Value::Tagged { value, .. } = input {
        return Ok(input.retagged_like(rename_field_at_prefix(value, prefix, old_name, new_name)?));
    }

    let Some((head, tail)) = prefix.split_first() else {
        return rename_object_field(input, old_name, new_name);
    };

    match head {
        PathComponent::Field(name) => match input {
            Value::Object(values) => {
                let Some(child) = values.get(name) else {
                    return Ok(input.clone());
                };
                let renamed_child = rename_field_at_prefix(child, tail, old_name, new_name)?;
                let mut updated = values.clone();
                updated.insert(name.clone(), renamed_child);
                Ok(Value::Object(updated))
            }
            Value::Null => Ok(Value::Null),
            Value::Array(_) => Err(AqError::Query("cannot index array with string".to_string())),
            other => Err(AqError::Query(format!(
                "cannot index {} with string",
                kind_name(other)
            ))),
        },
        PathComponent::Index(index) => match input {
            Value::Array(values) => {
                let Some(index) = resolve_index(*index, values.len()) else {
                    return Ok(input.clone());
                };
                let mut updated = values.clone();
                updated[index] = rename_field_at_prefix(&updated[index], tail, old_name, new_name)?;
                Ok(Value::Array(updated))
            }
            Value::Null => Ok(Value::Null),
            Value::Object(_) => Err(AqError::Query(
                "cannot index object with number".to_string(),
            )),
            other => Err(AqError::Query(format!(
                "cannot index {} with number",
                kind_name(other)
            ))),
        },
        PathComponent::Slice { start, end } => match input {
            Value::Array(values) => {
                let (start, end) = resolve_slice_bounds(*start, *end, values.len());
                let child = Value::Array(values[start..end].to_vec());
                let renamed_child = rename_field_at_prefix(&child, tail, old_name, new_name)?;
                let Value::Array(renamed_slice) = renamed_child.untagged() else {
                    return Err(AqError::Query(
                        "A slice of an array can only be assigned another array".to_string(),
                    ));
                };
                let mut updated = Vec::with_capacity(
                    start + renamed_slice.len() + values.len().saturating_sub(end),
                );
                updated.extend_from_slice(&values[..start]);
                updated.extend(renamed_slice.iter().cloned());
                updated.extend_from_slice(&values[end..]);
                Ok(Value::Array(updated))
            }
            Value::Null => Ok(Value::Null),
            Value::String(_) => Err(AqError::Query("Cannot update string slices".to_string())),
            Value::Object(_) => Err(AqError::Query(
                "cannot index object with object".to_string(),
            )),
            other => Err(AqError::Query(format!(
                "cannot index {} with object",
                kind_name(other)
            ))),
        },
    }
}

fn rename_object_field(input: &Value, old_name: &str, new_name: &str) -> Result<Value, AqError> {
    if let Value::Tagged { value, .. } = input {
        return Ok(input.retagged_like(rename_object_field(value, old_name, new_name)?));
    }

    match input {
        Value::Object(values) => {
            if !values.contains_key(old_name) || old_name == new_name {
                return Ok(input.clone());
            }

            let mut renamed = IndexMap::new();
            let mut inserted = false;
            for (key, value) in values {
                if key == old_name {
                    renamed.insert(new_name.to_string(), value.clone());
                    inserted = true;
                } else if key == new_name && inserted {
                    continue;
                } else {
                    renamed.insert(key.clone(), value.clone());
                }
            }
            Ok(Value::Object(renamed))
        }
        Value::Null => Ok(Value::Null),
        Value::Array(_) => Err(AqError::Query("cannot index array with string".to_string())),
        other => Err(AqError::Query(format!(
            "cannot index {} with string",
            kind_name(other)
        ))),
    }
}

fn path_to_value(path: &[PathComponent]) -> Value {
    let mut values = Vec::with_capacity(path.len());
    for component in path {
        values.push(match component {
            PathComponent::Field(value) => Value::String(value.clone()),
            PathComponent::Index(value) => Value::Integer(*value as i64),
            PathComponent::Slice { start, end } => slice_component_value(*start, *end),
        });
    }
    Value::Array(values)
}

fn delpaths_value(input: &Value, paths: &[Vec<PathComponent>]) -> Result<Value, AqError> {
    if paths.iter().any(|path| path.is_empty()) {
        return Ok(Value::Null);
    }

    if let Value::Tagged { value, .. } = input {
        return Ok(input.retagged_like(delpaths_value(value, paths)?));
    }

    match input {
        Value::Object(fields) => delpaths_object(fields, paths),
        Value::Array(values) => delpaths_array(values, paths),
        Value::Null => Ok(Value::Null),
        other => Err(AqError::Query(format!(
            "cannot delete fields from {}",
            kind_name(other)
        ))),
    }
}

fn delpaths_object(
    fields: &IndexMap<String, Value>,
    paths: &[Vec<PathComponent>],
) -> Result<Value, AqError> {
    let mut direct = BTreeSet::new();
    let mut nested: BTreeMap<String, Vec<Vec<PathComponent>>> = BTreeMap::new();

    for path in paths {
        match &path[0] {
            PathComponent::Field(key) => {
                if path.len() == 1 {
                    direct.insert(key.clone());
                } else {
                    nested
                        .entry(key.clone())
                        .or_default()
                        .push(path[1..].to_vec());
                }
            }
            PathComponent::Index(_) => {
                return Err(AqError::Query(
                    "cannot delete number field of object".to_string(),
                ));
            }
            PathComponent::Slice { .. } => {
                return Err(AqError::Query(
                    "cannot index object with object".to_string(),
                ));
            }
        }
    }

    let mut updated = IndexMap::new();
    for (key, value) in fields {
        if direct.contains(key) {
            continue;
        }
        if let Some(child_paths) = nested.get(key) {
            updated.insert(key.clone(), delpaths_value(value, child_paths)?);
        } else {
            updated.insert(key.clone(), value.clone());
        }
    }
    Ok(Value::Object(updated))
}

fn delpaths_array(values: &[Value], paths: &[Vec<PathComponent>]) -> Result<Value, AqError> {
    if paths
        .iter()
        .all(|path| matches!(path.as_slice(), [PathComponent::Index(_)]))
    {
        let mut direct = vec![false; values.len()];
        for path in paths {
            let PathComponent::Index(index) = path[0] else {
                continue;
            };
            if let Some(resolved) = resolve_index(index, values.len()) {
                direct[resolved] = true;
            }
        }
        return Ok(Value::Array(
            values
                .iter()
                .enumerate()
                .filter(|(index, _)| !direct[*index])
                .map(|(_, value)| value.clone())
                .collect(),
        ));
    }

    let mut direct = BTreeSet::new();
    let mut nested: BTreeMap<usize, Vec<Vec<PathComponent>>> = BTreeMap::new();
    let mut sliced_nested: BTreeMap<(usize, usize), Vec<Vec<PathComponent>>> = BTreeMap::new();

    for path in paths {
        match &path[0] {
            PathComponent::Index(index) => {
                let Some(resolved) = resolve_index(*index, values.len()) else {
                    continue;
                };
                if path.len() == 1 {
                    direct.insert(resolved);
                } else {
                    nested.entry(resolved).or_default().push(path[1..].to_vec());
                }
            }
            PathComponent::Slice { start, end } => {
                let (start, end) = resolve_slice_bounds(*start, *end, values.len());
                if path.len() == 1 {
                    direct.extend(start..end);
                } else {
                    sliced_nested
                        .entry((start, end))
                        .or_default()
                        .push(path[1..].to_vec());
                }
            }
            PathComponent::Field(_) => {
                return Err(AqError::Query(
                    "cannot delete string element of array".to_string(),
                ));
            }
        }
    }

    let mut updated = Vec::new();
    let mut index = 0;
    while index < values.len() {
        if direct.contains(&index) {
            index += 1;
            continue;
        }

        if let Some(((start, end), child_paths)) =
            sliced_nested.iter().find(|((start, _), _)| *start == index)
        {
            let slice = Value::Array(values[*start..*end].to_vec());
            let updated_slice = delpaths_value(&slice, child_paths)?;
            let Value::Array(child_values) = updated_slice.untagged() else {
                return Err(AqError::Query(
                    "A slice of an array can only be assigned another array".to_string(),
                ));
            };
            updated.extend(child_values.iter().cloned());
            index = *end;
            continue;
        }

        let value = &values[index];
        if direct.contains(&index) {
            index += 1;
            continue;
        } else if let Some(child_paths) = nested.get(&index) {
            updated.push(delpaths_value(value, child_paths)?);
        } else {
            updated.push(value.clone());
        }
        index += 1;
    }
    Ok(Value::Array(updated))
}

fn indices_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(needle) = literal_expr_value(expr) {
        return Ok(vec![indices_value(input, needle)?]);
    }
    let needles = evaluate_expr(expr, input, bindings, context)?;
    let mut out = Vec::new();
    for needle in needles {
        out.push(indices_value(input, &needle)?);
    }
    Ok(out)
}

fn indices_value(input: &Value, needle: &Value) -> Result<Value, AqError> {
    match (input.untagged(), needle.untagged()) {
        (Value::String(value), Value::String(needle)) => Ok(Value::Array(
            string_indices(value, needle)
                .into_iter()
                .map(Value::Integer)
                .collect(),
        )),
        (Value::Array(values), Value::Array(needle)) => Ok(Value::Array(
            array_indices(values, needle)
                .into_iter()
                .map(|index| Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)))
                .collect(),
        )),
        (Value::Array(values), needle) => Ok(Value::Array(
            values
                .iter()
                .enumerate()
                .filter(|(_, value)| *value == needle)
                .map(|(index, _)| Value::Integer(i64::try_from(index).unwrap_or(i64::MAX)))
                .collect(),
        )),
        (Value::String(_), other) => Err(AqError::Query(format!(
            "indices on strings requires a string needle, got {}",
            kind_name(other)
        ))),
        (other, _) => Err(AqError::Query(format!(
            "indices is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn search_index_value(input: &Value, needle: &Value, reverse: bool) -> Result<Value, AqError> {
    match input {
        Value::String(value) => {
            let Value::String(needle) = needle.untagged() else {
                return Err(AqError::Query(format!(
                    "index/rindex on strings requires a string needle, got {}",
                    kind_name(needle)
                )));
            };
            let found = string_search_index(value, needle, reverse);
            Ok(found.map(Value::Integer).unwrap_or(Value::Null))
        }
        Value::Array(values) => {
            let found = match needle.untagged() {
                Value::Array(needle) => {
                    let matches = array_indices(values, needle);
                    if reverse {
                        matches.into_iter().last()
                    } else {
                        matches.into_iter().next()
                    }
                }
                needle => {
                    if reverse {
                        values.iter().rposition(|value| value == needle)
                    } else {
                        values.iter().position(|value| value == needle)
                    }
                }
            };
            Ok(found
                .and_then(|index| i64::try_from(index).ok())
                .map(Value::Integer)
                .unwrap_or(Value::Null))
        }
        other => Err(AqError::Query(format!(
            "index/rindex is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn string_search_index(value: &str, needle: &str, reverse: bool) -> Option<i64> {
    if needle.is_empty() {
        return None;
    }
    let byte_index = if reverse {
        value.rfind(needle)
    } else {
        value.find(needle)
    }?;
    if value.is_ascii() {
        return i64::try_from(byte_index).ok();
    }
    char_offset(value, byte_index).ok()
}

fn index_of(
    input: &Value,
    needle_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    search_index(input, needle_expr, bindings, false, context)
}

fn index_input_of(
    input: &Value,
    key_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut index = IndexMap::new();
    for item in iterate_input_values(input)? {
        append_index_entries(&mut index, &item, key_query, bindings, context)?;
    }
    Ok(Value::Object(index))
}

fn index_stream_of(
    input: &Value,
    source_query: &Query,
    key_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let mut index = IndexMap::new();
    for frame in evaluate_query(source_query, input, bindings, context)? {
        append_index_entries(
            &mut index,
            &frame.value,
            key_query,
            &frame.bindings,
            context,
        )?;
    }
    Ok(Value::Object(index))
}

fn rindex_of(
    input: &Value,
    needle_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    search_index(input, needle_expr, bindings, true, context)
}

fn search_index(
    input: &Value,
    needle_expr: &Expr,
    bindings: &Bindings,
    reverse: bool,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    if let Some(needle) = literal_expr_value(needle_expr) {
        return Ok(vec![search_index_value(input, needle, reverse)?]);
    }
    let needles = evaluate_expr(needle_expr, input, bindings, context)?;
    let mut out = Vec::new();
    for needle in needles {
        out.push(search_index_value(input, &needle, reverse)?);
    }
    Ok(out)
}

fn append_index_entries(
    index: &mut IndexMap<String, Value>,
    item: &Value,
    key_query: &Query,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<(), AqError> {
    for frame in evaluate_query(key_query, item, bindings, context)? {
        index.insert(to_string_of(&frame.value)?, item.clone());
    }
    Ok(())
}

fn flatten_depth_value(value: Value) -> Result<usize, AqError> {
    match value {
        Value::Integer(value) => usize::try_from(value)
            .map_err(|_| AqError::Query("flatten depth must not be negative".to_string())),
        Value::Float(value) if value.is_finite() && value.fract() == 0.0 => {
            if value < 0.0 {
                Err(AqError::Query(
                    "flatten depth must not be negative".to_string(),
                ))
            } else if value > usize::MAX as f64 {
                Ok(usize::MAX)
            } else {
                Ok(value as usize)
            }
        }
        Value::Float(_) => Err(AqError::Query(
            "flatten depth must be an integer".to_string(),
        )),
        _ => Err(AqError::Query("flatten depth must be a number".to_string())),
    }
}

fn flatten_with_depth_of(
    input: &Value,
    depth_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let depths = evaluate_expr(depth_expr, input, bindings, context)?;
    let values = expect_array_input("flatten", input)?;
    let mut out = Vec::new();
    for depth in depths {
        let depth = flatten_depth_value(depth)?;
        let mut flattened = Vec::new();
        for value in values {
            flatten_value_to_depth(value, depth, &mut flattened);
        }
        out.push(Value::Array(flattened));
    }
    Ok(out)
}

fn string_indices(value: &str, needle: &str) -> Vec<i64> {
    if needle.is_empty() {
        return Vec::new();
    }

    if value.is_ascii() && needle.is_ascii() {
        return ascii_string_indices(value.as_bytes(), needle.as_bytes());
    }

    let mut out = Vec::new();
    for (char_index, (byte_index, _)) in value.char_indices().enumerate() {
        if value[byte_index..].starts_with(needle) {
            out.push(i64::try_from(char_index).unwrap_or(i64::MAX));
        }
    }
    out
}

fn ascii_string_indices(value: &[u8], needle: &[u8]) -> Vec<i64> {
    if needle.is_empty() || needle.len() > value.len() {
        return Vec::new();
    }
    if needle.len() == 1 {
        let byte = needle[0];
        return value
            .iter()
            .enumerate()
            .filter(|(_, candidate)| **candidate == byte)
            .map(|(index, _)| i64::try_from(index).unwrap_or(i64::MAX))
            .collect();
    }
    let haystack = std::str::from_utf8(value).unwrap_or_default();
    let needle = std::str::from_utf8(needle).unwrap_or_default();
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < haystack.len() {
        let Some(found) = haystack[offset..].find(needle) else {
            break;
        };
        let index = offset + found;
        out.push(i64::try_from(index).unwrap_or(i64::MAX));
        offset = index.saturating_add(1);
    }
    out
}

fn array_indices(values: &[Value], needle: &[Value]) -> Vec<usize> {
    if needle.is_empty() || needle.len() > values.len() {
        return Vec::new();
    }

    let mut out = Vec::new();
    for start in 0..=values.len() - needle.len() {
        if values[start..start + needle.len()] == *needle {
            out.push(start);
        }
    }
    out
}

fn keyed_values_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    name: &str,
    context: &EvaluationContext,
) -> Result<Vec<(Value, Value)>, AqError> {
    let values = expect_array_input(name, input)?;
    if let Some(keyed) = try_simple_keyed_values_of(values, expr, bindings) {
        return keyed;
    }
    let mut keyed = Vec::with_capacity(values.len());
    for value in values {
        let key = Value::Array(evaluate_expr(expr, value, bindings, context)?);
        keyed.push((key, value.clone()));
    }
    Ok(keyed)
}

fn try_simple_keyed_values_of(
    values: &[Value],
    expr: &Expr,
    bindings: &Bindings,
) -> Option<Result<Vec<(Value, Value)>, AqError>> {
    match expr {
        Expr::Literal(value) => Some(Ok(values
            .iter()
            .cloned()
            .map(|entry| (Value::Array(vec![value.clone()]), entry))
            .collect())),
        Expr::Variable(name) => {
            let value = bindings.get_value(name)?.clone();
            Some(Ok(values
                .iter()
                .cloned()
                .map(|entry| (Value::Array(vec![value.clone()]), entry))
                .collect()))
        }
        Expr::Path(path) => {
            let mut keyed = Vec::with_capacity(values.len());
            for value in values {
                let key = match evaluate_direct_static_path(path, value)? {
                    Ok(key) => key,
                    Err(error) => return Some(Err(error)),
                };
                keyed.push((Value::Array(key), value.clone()));
            }
            Some(Ok(keyed))
        }
        _ => None,
    }
}

fn entry_value(key: Value, value: Value) -> Value {
    let mut entry = IndexMap::with_capacity(2);
    entry.insert("key".to_string(), key);
    entry.insert("value".to_string(), value);
    Value::Object(entry)
}

fn count_entry_value(key: Value, count: usize) -> Value {
    let mut entry = IndexMap::with_capacity(2);
    entry.insert("key".to_string(), key);
    entry.insert(
        "count".to_string(),
        Value::Integer(i64::try_from(count).unwrap_or(i64::MAX)),
    );
    Value::Object(entry)
}

fn histogram_bucket_value(start: f64, end: f64, count: usize) -> Value {
    let mut entry = IndexMap::new();
    entry.insert("start".to_string(), normalize_number_value(start));
    entry.insert("end".to_string(), normalize_number_value(end));
    entry.insert(
        "count".to_string(),
        match i64::try_from(count) {
            Ok(value) => Value::Integer(value),
            Err(_) => Value::Integer(i64::MAX),
        },
    );
    Value::Object(entry)
}

fn from_entry_values(entries: &[Value]) -> Result<Value, AqError> {
    let mut object = IndexMap::new();
    for entry in entries {
        let (key, value) = parse_entry_value(entry)?;
        object.insert(key, value);
    }
    Ok(Value::Object(object))
}

fn parse_entry_value(entry: &Value) -> Result<(String, Value), AqError> {
    let Value::Object(fields) = entry else {
        return Err(AqError::Query(format!(
            "from_entries expects objects, got {}",
            kind_name(entry)
        )));
    };

    let key = fields
        .get("key")
        .or_else(|| fields.get("Key"))
        .or_else(|| fields.get("name"))
        .or_else(|| fields.get("Name"))
        .ok_or_else(|| {
            AqError::Query("from_entries expects each object to contain a key".to_string())
        })?;
    let value = fields
        .get("value")
        .or_else(|| fields.get("Value"))
        .or_else(|| fields.get("value"))
        .cloned()
        .ok_or_else(|| {
            AqError::Query("from_entries expects each object to contain a value".to_string())
        })?;

    match key {
        Value::String(key) => Ok((key.clone(), value)),
        other => Err(AqError::Query(format!(
            "from_entries expects string keys, got {}",
            kind_name(other)
        ))),
    }
}

fn ltrimstr_of(
    input: &Value,
    prefix_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    let Value::String(value) = input else {
        return Err(AqError::Query(
            "startswith() requires string inputs".to_string(),
        ));
    };
    let prefix = if let Some(prefix) = literal_string_expr_value(prefix_expr) {
        Cow::Borrowed(prefix)
    } else {
        match evaluate_string_argument_cow("ltrimstr", prefix_expr, input, bindings, context) {
            Ok(prefix) => prefix,
            Err(AqError::Query(message))
                if message.starts_with("ltrimstr requires a string argument, got ") =>
            {
                return Err(AqError::Query(
                    "startswith() requires string inputs".to_string(),
                ))
            }
            Err(error) => return Err(error),
        }
    };
    Ok(value.strip_prefix(&*prefix).unwrap_or(value).to_string())
}

fn rtrimstr_of(
    input: &Value,
    suffix_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    let Value::String(value) = input else {
        return Err(AqError::Query(
            "endswith() requires string inputs".to_string(),
        ));
    };
    let suffix = if let Some(suffix) = literal_string_expr_value(suffix_expr) {
        Cow::Borrowed(suffix)
    } else {
        match evaluate_string_argument_cow("rtrimstr", suffix_expr, input, bindings, context) {
            Ok(suffix) => suffix,
            Err(AqError::Query(message))
                if message.starts_with("rtrimstr requires a string argument, got ") =>
            {
                return Err(AqError::Query(
                    "endswith() requires string inputs".to_string(),
                ))
            }
            Err(error) => return Err(error),
        }
    };
    Ok(value.strip_suffix(&*suffix).unwrap_or(value).to_string())
}

fn trimstr_of(
    input: &Value,
    trim_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    let value = expect_string_input("trimstr", input)?;
    let trim = if let Some(trim) = literal_string_expr_value(trim_expr) {
        Cow::Borrowed(trim)
    } else {
        evaluate_string_argument_cow("trimstr", trim_expr, input, bindings, context)?
    };
    if trim.is_empty() || trim.len() > value.len() {
        return Ok(value.to_string());
    }
    let mut start = 0usize;
    if value.starts_with(&*trim) {
        start = trim.len();
    }
    let mut end = value.len();
    if end.saturating_sub(start) >= trim.len() && value[start..].ends_with(&*trim) {
        end = end.saturating_sub(trim.len());
    }
    Ok(value[start..end].to_string())
}

fn to_json_of(input: &Value) -> Result<String, AqError> {
    input
        .to_json_text()
        .map_err(|error| AqError::Query(format!("failed to serialize JSON string: {error}")))
}

fn evaluate_format_string(
    operator: FormatOperator,
    parts: &[FormatStringPart],
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Vec<Value>, AqError> {
    let mut outputs = vec![String::new()];
    for part in parts {
        match part {
            FormatStringPart::Literal(value) => {
                for output in &mut outputs {
                    output.push_str(value);
                }
            }
            FormatStringPart::Query(query) => {
                let formatted = evaluate_query_values(query, input, bindings, context)?
                    .into_iter()
                    .map(|value| format_of(&value, operator))
                    .collect::<Result<Vec<_>, _>>()?;
                if formatted.is_empty() {
                    return Ok(Vec::new());
                }
                let mut next = Vec::with_capacity(outputs.len() * formatted.len());
                for value in &formatted {
                    for output in &outputs {
                        let mut combined = output.clone();
                        combined.push_str(value);
                        next.push(combined);
                    }
                }
                outputs = next;
            }
        }
    }
    Ok(outputs.into_iter().map(Value::String).collect())
}

fn format_of(input: &Value, operator: FormatOperator) -> Result<String, AqError> {
    match operator {
        FormatOperator::Json => to_json_of(input),
        FormatOperator::Text => text_for_format(input),
        FormatOperator::Csv => csv_row_of(input),
        FormatOperator::Tsv => tsv_row_of(input),
        FormatOperator::Html => html_escape_of(input),
        FormatOperator::Uri => uri_escape_of(input),
        FormatOperator::Urid => uri_decode_of(input),
        FormatOperator::Sh => shell_escape_of(input),
        FormatOperator::Base64 => base64_encode_of(input),
        FormatOperator::Base64d => base64_decode_of(input),
    }
}

fn csv_row_of(input: &Value) -> Result<String, AqError> {
    let values = expect_array_input("@csv", input)?;
    let mut out = String::new();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&csv_field(value)?);
    }
    Ok(out)
}

fn csv_field(value: &Value) -> Result<String, AqError> {
    match value.untagged() {
        Value::Null => Ok(String::new()),
        Value::String(_) | Value::DateTime(_) | Value::Date(_) => {
            let escaped = scalar_text_for_escape(value)?.replace('"', "\"\"");
            Ok(format!("\"{escaped}\""))
        }
        Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            scalar_text_for_escape(value)
        }
        other => Err(AqError::Query(format!(
            "{} is not valid in a csv row",
            kind_name(other)
        ))),
    }
}

fn tsv_row_of(input: &Value) -> Result<String, AqError> {
    let values = expect_array_input("@tsv", input)?;
    let mut out = String::new();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push('\t');
        }
        out.push_str(&tsv_field(value)?);
    }
    Ok(out)
}

fn tsv_field(value: &Value) -> Result<String, AqError> {
    match value.untagged() {
        Value::Null => Ok(String::new()),
        Value::String(_) | Value::DateTime(_) | Value::Date(_) => {
            let mut out = String::new();
            for ch in scalar_text_for_escape(value)?.chars() {
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
            scalar_text_for_escape(value)
        }
        other => Err(AqError::Query(format!(
            "{} is not valid in a tsv row",
            kind_name(other)
        ))),
    }
}

fn uri_escape_of(input: &Value) -> Result<String, AqError> {
    let value = text_for_format_cow(input)?;
    let bytes = value.as_bytes();
    let Some(mut index) = bytes.iter().position(|byte| !is_uri_unreserved(*byte)) else {
        return Ok(value.into_owned());
    };
    let mut out = String::with_capacity(bytes.len());
    out.push_str(&value[..index]);
    while index < bytes.len() {
        let byte = bytes[index];
        if is_uri_unreserved(byte) {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(HEX_DIGITS[(byte >> 4) as usize] as char);
            out.push(HEX_DIGITS[(byte & 0x0F) as usize] as char);
        }
        index += 1;
    }
    Ok(out)
}

const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

fn is_uri_unreserved(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~')
}

fn uri_decode_of(input: &Value) -> Result<String, AqError> {
    let value = text_for_format_cow(input)?;
    let bytes = value.as_bytes();
    let Some(mut index) = bytes.iter().position(|byte| *byte == b'%') else {
        return Ok(value.into_owned());
    };

    if value.is_ascii() {
        let mut ascii = String::with_capacity(bytes.len());
        ascii.push_str(&value[..index]);
        while index < bytes.len() {
            if bytes[index] == b'%' {
                if index + 2 >= bytes.len() {
                    return Err(AqError::Query(format!(
                        "string ({value:?}) is not a valid uri encoding"
                    )));
                }
                let decoded =
                    decode_hex_byte(bytes[index + 1], bytes[index + 2]).ok_or_else(|| {
                        AqError::Query(format!("string ({value:?}) is not a valid uri encoding"))
                    })?;
                if decoded.is_ascii() {
                    ascii.push(char::from(decoded));
                    index += 3;
                    continue;
                }

                let mut out = ascii.into_bytes();
                out.push(decoded);
                index += 3;
                while index < bytes.len() {
                    if bytes[index] == b'%' {
                        if index + 2 >= bytes.len() {
                            return Err(AqError::Query(format!(
                                "string ({value:?}) is not a valid uri encoding"
                            )));
                        }
                        let decoded = decode_hex_byte(bytes[index + 1], bytes[index + 2])
                            .ok_or_else(|| {
                                AqError::Query(format!(
                                    "string ({value:?}) is not a valid uri encoding"
                                ))
                            })?;
                        out.push(decoded);
                        index += 3;
                    } else {
                        out.push(bytes[index]);
                        index += 1;
                    }
                }
                return String::from_utf8(out).map_err(|_| {
                    AqError::Query(format!("string ({value:?}) is not a valid uri encoding"))
                });
            }

            ascii.push(char::from(bytes[index]));
            index += 1;
        }
        return Ok(ascii);
    }

    let mut out = Vec::with_capacity(bytes.len());
    out.extend_from_slice(&bytes[..index]);
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(AqError::Query(format!(
                    "string ({value:?}) is not a valid uri encoding"
                )));
            }
            let decoded = decode_hex_byte(bytes[index + 1], bytes[index + 2]).ok_or_else(|| {
                AqError::Query(format!("string ({value:?}) is not a valid uri encoding"))
            })?;
            out.push(decoded);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| AqError::Query(format!("string ({value:?}) is not a valid uri encoding")))
}

fn decode_hex_byte(high: u8, low: u8) -> Option<u8> {
    let high = decode_hex_nibble(high)?;
    let low = decode_hex_nibble(low)?;
    Some((high << 4) | low)
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn html_escape_of(input: &Value) -> Result<String, AqError> {
    let value = text_for_format_cow(input)?;
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    Ok(out)
}

fn base64_encode_of(input: &Value) -> Result<String, AqError> {
    let value = text_for_format_cow(input)?;
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(base64_encoded_len(bytes.len()));
    BASE64_STANDARD.encode_string(bytes, &mut out);
    Ok(out)
}

fn base64_decode_of(input: &Value) -> Result<String, AqError> {
    let encoded = text_for_format_cow(input)?;
    if encoded.bytes().all(|byte| byte == b'=') {
        return Ok(String::new());
    }
    let bytes = if looks_like_unpadded_base64(&encoded) {
        BASE64_STANDARD_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(|error| AqError::Query(base64_decode_error_message(&encoded, error)))?
    } else {
        match BASE64_STANDARD.decode(encoded.as_bytes()) {
            Ok(bytes) => bytes,
            Err(standard_error) => {
                BASE64_STANDARD_NO_PAD
                    .decode(encoded.as_bytes())
                    .map_err(|_| {
                        AqError::Query(base64_decode_error_message(&encoded, standard_error))
                    })?
            }
        }
    };
    match String::from_utf8(bytes) {
        Ok(value) => Ok(value),
        Err(error) => Ok(String::from_utf8_lossy(&error.into_bytes()).into_owned()),
    }
}

fn looks_like_unpadded_base64(encoded: &str) -> bool {
    !encoded.contains('=')
        && !encoded.is_empty()
        && !encoded.len().is_multiple_of(4)
        && encoded
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'-' | b'_'))
}

fn base64_encoded_len(bytes: usize) -> usize {
    bytes.div_ceil(3) * 4
}

fn base64_decode_error_message(encoded: &str, error: base64::DecodeError) -> String {
    match error {
        Base64DecodeError::InvalidLength(_)
            if encoded.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'-' | b'_')
            }) && encoded.len() % 4 == 1 =>
        {
            format!("string ({encoded:?}) trailing base64 byte found")
        }
        _ => format!("string ({encoded:?}) is not valid base64 data"),
    }
}

fn shell_escape_of(input: &Value) -> Result<String, AqError> {
    match input.untagged() {
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(shell_escape_scalar(value)?);
            }
            Ok(out.join(" "))
        }
        _ => shell_escape_scalar(input),
    }
}

fn shell_escape_scalar(value: &Value) -> Result<String, AqError> {
    match value.untagged() {
        Value::String(_) | Value::DateTime(_) | Value::Date(_) => {
            let escaped = scalar_text_for_escape(value)?.replace('\'', "'\\''");
            Ok(format!("'{escaped}'"))
        }
        Value::Null | Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            scalar_text_for_escape(value)
        }
        other => Err(AqError::Query(format!(
            "{} is not valid in a shell string",
            kind_name(other)
        ))),
    }
}

fn scalar_text_for_escape(value: &Value) -> Result<String, AqError> {
    scalar_text_for_escape_cow(value).map(Cow::into_owned)
}

fn scalar_text_for_escape_cow<'a>(value: &'a Value) -> Result<Cow<'a, str>, AqError> {
    if let Value::String(value) = value.untagged() {
        return Ok(Cow::Borrowed(value));
    }
    if let Some(value) = value.rendered_string() {
        return Ok(Cow::Owned(value));
    }
    match value.untagged() {
        Value::Null | Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            value
                .to_json_text()
                .map(Cow::Owned)
                .map_err(|error| AqError::Query(format!("failed to render scalar value: {error}")))
        }
        other => Err(AqError::Query(format!(
            "{} is not valid in a scalar string escape",
            kind_name(other)
        ))),
    }
}

fn text_for_format(value: &Value) -> Result<String, AqError> {
    text_for_format_cow(value).map(Cow::into_owned)
}

fn text_for_format_cow<'a>(value: &'a Value) -> Result<Cow<'a, str>, AqError> {
    match value.untagged() {
        Value::String(_) | Value::DateTime(_) | Value::Date(_) => scalar_text_for_escape_cow(value),
        Value::Null | Value::Bool(_) | Value::Integer(_) | Value::Decimal(_) | Value::Float(_) => {
            scalar_text_for_escape_cow(value)
        }
        _ => to_json_of(value).map(Cow::Owned),
    }
}

fn from_json_of(input: &Value) -> Result<Value, AqError> {
    let raw = expect_string_input("fromjson", input)?;
    crate::value::parse_json_str(raw).map_err(AqError::Query)
}

fn explode_of(input: &Value) -> Result<Value, AqError> {
    let value = expect_string_input("explode", input)?;
    if value.is_ascii() {
        let mut out = Vec::with_capacity(value.len());
        for byte in value.bytes() {
            out.push(Value::Integer(i64::from(byte)));
        }
        return Ok(Value::Array(out));
    }
    Ok(Value::Array(
        value
            .chars()
            .map(|ch| Value::Integer(i64::from(u32::from(ch))))
            .collect(),
    ))
}

fn implode_of(input: &Value) -> Result<String, AqError> {
    let values = match input.untagged() {
        Value::Array(values) => values,
        _ => return Err(AqError::Query("implode input must be an array".to_string())),
    };
    if let Some(out) = try_implode_ascii_string(values) {
        return out;
    }
    let mut out = String::with_capacity(values.len());
    for value in values {
        out.push(imploded_char_of(value)?);
    }
    Ok(out)
}

fn try_implode_ascii_string(values: &[Value]) -> Option<Result<String, AqError>> {
    let mut bytes = Vec::with_capacity(values.len());
    for value in values {
        let Value::Integer(codepoint) = value.untagged() else {
            return None;
        };
        let Ok(byte) = u8::try_from(*codepoint) else {
            return None;
        };
        if !byte.is_ascii() {
            return None;
        }
        bytes.push(byte);
    }
    Some(
        String::from_utf8(bytes)
            .map_err(|_| AqError::Query("internal error: invalid ascii implode".to_string())),
    )
}

fn imploded_char_of(value: &Value) -> Result<char, AqError> {
    let codepoint = match value.untagged() {
        Value::Integer(value) => *value,
        Value::Decimal(value) => value
            .as_i64_exact()
            .unwrap_or_else(|| value.to_f64_lossy().trunc() as i64),
        Value::Float(value) if value.is_finite() => value.trunc() as i64,
        other => {
            let (value_type, rendered) = typed_rendered_value(other);
            return Err(AqError::Query(format!(
                "{value_type} ({rendered}) can't be imploded, unicode codepoint needs to be numeric"
            )));
        }
    };
    Ok(replacement_char_for_codepoint(codepoint))
}

fn replacement_char_for_codepoint(codepoint: i64) -> char {
    const REPLACEMENT: char = '\u{FFFD}';
    let Ok(codepoint) = u32::try_from(codepoint) else {
        return REPLACEMENT;
    };
    if (0xD800..=0xDFFF).contains(&codepoint) {
        return REPLACEMENT;
    }
    char::from_u32(codepoint).unwrap_or(REPLACEMENT)
}

fn map_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    match input {
        Value::Array(values) => {
            if let Some(mapped) = try_simple_map_of(values, expr, bindings, context) {
                return mapped.map(Value::Array);
            }
            let mut mapped = Vec::new();
            for value in values {
                mapped.extend(evaluate_expr(expr, value, bindings, context)?);
            }
            Ok(Value::Array(mapped))
        }
        other => Err(AqError::Query(format!(
            "map is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn try_simple_map_of(
    values: &[Value],
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Option<Result<Vec<Value>, AqError>> {
    match expr {
        Expr::Builtin(BuiltinExpr::Abs) => Some(map_abs_values(values)),
        Expr::Builtin(BuiltinExpr::Sqrt) => Some(values.iter().map(sqrt_of).collect()),
        Expr::Builtin(BuiltinExpr::Length) => Some(values.iter().map(length_of).collect()),
        Expr::Builtin(BuiltinExpr::Has(key_expr)) => literal_expr_value(key_expr)
            .map(|key| map_has_literal(values, key))
            .or_else(|| {
                Some(
                    values
                        .iter()
                        .map(|value| has_of(value, key_expr, bindings, context).map(Value::Bool))
                        .collect(),
                )
            }),
        Expr::Builtin(BuiltinExpr::StartsWith(prefix_expr)) => {
            literal_string_expr_value(prefix_expr)
                .map(|prefix| map_starts_with_literal(values, prefix))
                .or_else(|| {
                    Some(
                        values
                            .iter()
                            .map(|value| {
                                starts_with_of(value, prefix_expr, bindings, context)
                                    .map(Value::Bool)
                            })
                            .collect(),
                    )
                })
        }
        Expr::Builtin(BuiltinExpr::EndsWith(suffix_expr)) => literal_string_expr_value(suffix_expr)
            .map(|suffix| map_ends_with_literal(values, suffix))
            .or_else(|| {
                Some(
                    values
                        .iter()
                        .map(|value| {
                            ends_with_of(value, suffix_expr, bindings, context).map(Value::Bool)
                        })
                        .collect(),
                )
            }),
        Expr::Builtin(BuiltinExpr::Contains(expected_expr)) => literal_expr_value(expected_expr)
            .map(|expected| map_contains_literal(values, expected))
            .or_else(|| {
                Some(
                    values
                        .iter()
                        .map(|value| {
                            contains_of(value, expected_expr, bindings, context).map(Value::Bool)
                        })
                        .collect(),
                )
            }),
        Expr::Builtin(BuiltinExpr::In(container_expr)) => literal_expr_value(container_expr)
            .map(|container| map_in_literal(values, container))
            .or_else(|| {
                Some(
                    values
                        .iter()
                        .map(|value| {
                            in_of(value, container_expr, bindings, context).map(Value::Bool)
                        })
                        .collect(),
                )
            }),
        Expr::Builtin(BuiltinExpr::Inside(container_expr)) => literal_expr_value(container_expr)
            .map(|container| map_inside_literal(values, container))
            .or_else(|| {
                Some(
                    values
                        .iter()
                        .map(|value| {
                            inside_of(value, container_expr, bindings, context).map(Value::Bool)
                        })
                        .collect(),
                )
            }),
        Expr::Builtin(BuiltinExpr::TypeFilter(filter)) => Some(Ok(values
            .iter()
            .filter(|value| matches_type_filter(value, *filter))
            .cloned()
            .collect())),
        _ => None,
    }
}

fn map_abs_values(values: &[Value]) -> Result<Vec<Value>, AqError> {
    let mut mapped = Vec::with_capacity(values.len());
    for value in values {
        mapped.push(match value {
            Value::String(_) => value.clone(),
            Value::Integer(value) => match value.checked_abs() {
                Some(value) => Value::Integer(value),
                None => Value::Float((*value as f64).abs()),
            },
            Value::Decimal(value) => Value::Decimal(value.abs()),
            Value::Float(value) => normalize_number_value(value.abs()),
            other => {
                return Err(AqError::Query(format!(
                    "abs is not defined for {}",
                    kind_name(other)
                )))
            }
        });
    }
    Ok(mapped)
}

fn map_has_literal(values: &[Value], key: &Value) -> Result<Vec<Value>, AqError> {
    let mut mapped = Vec::with_capacity(values.len());
    match key.untagged() {
        Value::String(key) => {
            for value in values {
                mapped.push(Value::Bool(match value.untagged() {
                    Value::Object(fields) => fields.contains_key(key),
                    Value::Array(_) => {
                        return Err(AqError::Query(format!(
                            "has expects integer indices for arrays, got {}",
                            "string"
                        )))
                    }
                    other => {
                        return Err(AqError::Query(format!(
                            "has is not defined for {}",
                            kind_name(other)
                        )))
                    }
                }));
            }
        }
        Value::Integer(index) => {
            for value in values {
                mapped.push(Value::Bool(match value.untagged() {
                    Value::Array(items) => resolve_index(*index as isize, items.len()).is_some(),
                    Value::Object(_) => {
                        return Err(AqError::Query("has is not defined for object".to_string()))
                    }
                    other => {
                        return Err(AqError::Query(format!(
                            "has is not defined for {}",
                            kind_name(other)
                        )))
                    }
                }));
            }
        }
        Value::Decimal(index) => {
            let resolved = decimal_index_value(index);
            for value in values {
                mapped.push(Value::Bool(match value.untagged() {
                    Value::Array(items) => resolved
                        .and_then(|index| isize::try_from(index).ok())
                        .and_then(|index| resolve_index(index, items.len()))
                        .is_some(),
                    Value::Object(_) => {
                        return Err(AqError::Query("has is not defined for object".to_string()))
                    }
                    other => {
                        return Err(AqError::Query(format!(
                            "has is not defined for {}",
                            kind_name(other)
                        )))
                    }
                }));
            }
        }
        Value::Float(index)
            if index.is_finite()
                && index.fract() == 0.0
                && *index >= isize::MIN as f64
                && *index <= isize::MAX as f64 =>
        {
            let index = *index as isize;
            for value in values {
                mapped.push(Value::Bool(match value.untagged() {
                    Value::Array(items) => resolve_index(index, items.len()).is_some(),
                    Value::Object(_) => {
                        return Err(AqError::Query("has is not defined for object".to_string()))
                    }
                    other => {
                        return Err(AqError::Query(format!(
                            "has is not defined for {}",
                            kind_name(other)
                        )))
                    }
                }));
            }
        }
        Value::Float(_) => {
            for value in values {
                mapped.push(Value::Bool(match value.untagged() {
                    Value::Array(_) => false,
                    Value::Object(_) => {
                        return Err(AqError::Query("has is not defined for object".to_string()))
                    }
                    other => {
                        return Err(AqError::Query(format!(
                            "has is not defined for {}",
                            kind_name(other)
                        )))
                    }
                }));
            }
        }
        _other => {
            for value in values {
                mapped.push(Value::Bool(value_has(value, key)?));
            }
        }
    }
    Ok(mapped)
}

fn map_contains_literal(values: &[Value], expected: &Value) -> Result<Vec<Value>, AqError> {
    let mut mapped = Vec::with_capacity(values.len());
    for value in values {
        mapped.push(Value::Bool(contains_value(value, expected)?));
    }
    Ok(mapped)
}

fn map_in_literal(values: &[Value], container: &Value) -> Result<Vec<Value>, AqError> {
    let mut mapped = Vec::with_capacity(values.len());
    for value in values {
        mapped.push(Value::Bool(value_has(container, value)?));
    }
    Ok(mapped)
}

fn map_inside_literal(values: &[Value], container: &Value) -> Result<Vec<Value>, AqError> {
    let mut mapped = Vec::with_capacity(values.len());
    for value in values {
        mapped.push(Value::Bool(contains_value(container, value)?));
    }
    Ok(mapped)
}

fn map_starts_with_literal(values: &[Value], prefix: &str) -> Result<Vec<Value>, AqError> {
    let mut mapped = Vec::with_capacity(values.len());
    for value in values {
        let Value::String(string) = value.untagged() else {
            return Err(AqError::Query(
                "startswith() requires string inputs".to_string(),
            ));
        };
        mapped.push(Value::Bool(string.starts_with(prefix)));
    }
    Ok(mapped)
}

fn map_ends_with_literal(values: &[Value], suffix: &str) -> Result<Vec<Value>, AqError> {
    let mut mapped = Vec::with_capacity(values.len());
    for value in values {
        let Value::String(string) = value.untagged() else {
            return Err(AqError::Query(
                "endswith() requires string inputs".to_string(),
            ));
        };
        mapped.push(Value::Bool(string.ends_with(suffix)));
    }
    Ok(mapped)
}

fn map_values_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    match input.untagged() {
        Value::Array(values) => {
            let mut mapped = Vec::new();
            for value in values {
                if let Some(first) = evaluate_expr(expr, value, bindings, context)?
                    .into_iter()
                    .next()
                {
                    mapped.push(first);
                }
            }
            Ok(Value::Array(mapped))
        }
        Value::Object(values) => {
            let mut mapped = IndexMap::new();
            for (key, value) in values {
                if let Some(first) = evaluate_expr(expr, value, bindings, context)?
                    .into_iter()
                    .next()
                {
                    mapped.insert(key.clone(), first);
                }
            }
            Ok(Value::Object(mapped))
        }
        other => Err(AqError::Query(format!(
            "map_values is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn flatten_value(value: &Value, out: &mut Vec<Value>) {
    match value.untagged() {
        Value::Array(values) => {
            for value in values {
                flatten_value(value, out);
            }
        }
        value => out.push(value.clone()),
    }
}

fn flatten_value_to_depth(value: &Value, depth: usize, out: &mut Vec<Value>) {
    if depth == 0 {
        out.push(value.clone());
        return;
    }

    match value.untagged() {
        Value::Array(values) => {
            for value in values {
                flatten_value_to_depth(value, depth.saturating_sub(1), out);
            }
        }
        value => out.push(value.clone()),
    }
}

fn normalize_number_value(value: f64) -> Value {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        Value::Integer(value as i64)
    } else {
        Value::Float(value)
    }
}

fn expect_string_input<'a>(name: &str, value: &'a Value) -> Result<&'a str, AqError> {
    match value.untagged() {
        Value::String(value) => Ok(value),
        other => Err(AqError::Query(format!(
            "{name} is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn expect_trim_string_input(value: &Value) -> Result<&str, AqError> {
    match value.untagged() {
        Value::String(value) => Ok(value),
        _ => Err(AqError::Query("trim input must be a string".to_string())),
    }
}

fn trim_ascii_or_unicode(value: &str) -> &str {
    if value.is_ascii() {
        let start = ascii_trim_start_index(value.as_bytes());
        let end = ascii_trim_end_index(value.as_bytes(), start);
        &value[start..end]
    } else {
        value.trim()
    }
}

fn trim_ascii_start_or_unicode(value: &str) -> &str {
    if value.is_ascii() {
        let start = ascii_trim_start_index(value.as_bytes());
        &value[start..]
    } else {
        value.trim_start()
    }
}

fn trim_ascii_end_or_unicode(value: &str) -> &str {
    if value.is_ascii() {
        let end = ascii_trim_end_index(value.as_bytes(), 0);
        &value[..end]
    } else {
        value.trim_end()
    }
}

fn ascii_trim_start_index(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .position(|byte| !is_ascii_trim_whitespace(*byte))
        .unwrap_or(bytes.len())
}

fn ascii_trim_end_index(bytes: &[u8], min_end: usize) -> usize {
    bytes
        .iter()
        .rposition(|byte| !is_ascii_trim_whitespace(*byte))
        .map_or(min_end, |index| index.saturating_add(1))
}

fn is_ascii_trim_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\n' | b'\r' | b'\t' | 0x0B | 0x0C)
}

fn expect_array_input<'a>(name: &str, value: &'a Value) -> Result<&'a [Value], AqError> {
    match value.untagged() {
        Value::Array(values) => Ok(values),
        other => Err(AqError::Query(format!(
            "{name} is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn expect_object_input<'a>(
    name: &str,
    value: &'a Value,
) -> Result<&'a IndexMap<String, Value>, AqError> {
    match value.untagged() {
        Value::Object(values) => Ok(values),
        other => Err(AqError::Query(format!(
            "{name} is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn evaluate_string_argument(
    name: &str,
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<String, AqError> {
    let value = evaluate_single_argument_value(name, expr, input, bindings, context)?;
    match value {
        Value::String(value) => Ok(value),
        other => Err(AqError::Query(format!(
            "{name} requires a string argument, got {}",
            kind_name(&other)
        ))),
    }
}

fn evaluate_string_argument_cow<'a>(
    name: &str,
    expr: &'a Expr,
    input: &'a Value,
    bindings: &'a Bindings,
    context: &EvaluationContext,
) -> Result<Cow<'a, str>, AqError> {
    if let Some(value) = borrowed_string_argument(expr, input, bindings) {
        return Ok(Cow::Borrowed(value));
    }
    evaluate_string_argument(name, expr, input, bindings, context).map(Cow::Owned)
}

fn borrowed_string_argument<'a>(
    expr: &'a Expr,
    input: &'a Value,
    bindings: &'a Bindings,
) -> Option<&'a str> {
    match borrowed_value_argument(expr, input, bindings)?.untagged() {
        Value::String(value) => Some(value),
        _ => None,
    }
}

fn borrowed_value_argument<'a>(
    expr: &'a Expr,
    input: &'a Value,
    bindings: &'a Bindings,
) -> Option<&'a Value> {
    match expr {
        Expr::Literal(value) => Some(value),
        Expr::Variable(name) => bindings.get_value(name),
        Expr::Path(path) => borrowed_value_path_value(path, input),
        _ => None,
    }
}

fn borrowed_value_path_value<'a>(path: &PathExpr, input: &'a Value) -> Option<&'a Value> {
    let mut current = Some(input);
    for segment in &path.segments {
        match segment {
            Segment::Field { name, optional } => {
                let value = current?;
                current = match value.untagged() {
                    Value::Object(fields) => fields.get(name),
                    Value::Null if *optional => return None,
                    Value::Null => return None,
                    _ if *optional => return None,
                    _ => return None,
                };
            }
            Segment::Index { index, optional } => {
                let value = current?;
                current = match value.untagged() {
                    Value::Array(items) => {
                        resolve_index(*index, items.len()).and_then(|i| items.get(i))
                    }
                    Value::Null if *optional => return None,
                    Value::Null => return None,
                    _ if *optional => return None,
                    _ => return None,
                };
            }
            Segment::Lookup { .. } | Segment::Slice { .. } | Segment::Iterate { .. } => {
                return None;
            }
        }
    }
    current
}

fn evaluate_single_argument_value(
    name: &str,
    expr: &Expr,
    input: &Value,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<Value, AqError> {
    let values = evaluate_expr(expr, input, bindings, context)?;
    let mut iter = values.into_iter();
    let Some(value) = iter.next() else {
        return Err(AqError::Query(format!(
            "{name} requires exactly one argument value"
        )));
    };
    if iter.next().is_some() {
        return Err(AqError::Query(format!(
            "{name} requires exactly one argument value"
        )));
    }
    Ok(value)
}

fn parse_number_literal(raw: &str) -> Result<Value, AqError> {
    if raw.trim() != raw {
        return Err(tonumber_parse_error(raw));
    }
    if !raw.contains('.') && !raw.contains('e') && !raw.contains('E') {
        if let Some(normalized) = normalize_integer_number_literal(raw) {
            if let Ok(value) = normalized.parse::<i64>() {
                return Ok(Value::Integer(value));
            }
            let parsed = serde_json::from_str::<serde_json::Value>(normalized)
                .map_err(|_| tonumber_parse_error(raw))?;
            return match parsed {
                serde_json::Value::Number(_) => Value::from_json(parsed),
                _ => Err(tonumber_parse_error(raw)),
            };
        }
        if raw
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            return Err(tonumber_parse_error(raw));
        }
    }
    let normalized = if let Some(rest) = raw.strip_prefix("+.") {
        format!("0.{rest}")
    } else if let Some(rest) = raw.strip_prefix("-.") {
        format!("-0.{rest}")
    } else if raw.starts_with('.') {
        format!("0{raw}")
    } else if let Some(rest) = raw.strip_prefix('+') {
        rest.to_string()
    } else {
        raw.to_string()
    };
    if normalized.contains('.') || normalized.contains('e') || normalized.contains('E') {
        return DecimalValue::parse(&normalized)
            .map(Value::Decimal)
            .map_err(|_| tonumber_parse_error(raw));
    }
    if let Ok(value) = normalized.parse::<i64>() {
        return Ok(Value::Integer(value));
    }
    let parsed = serde_json::from_str::<serde_json::Value>(&normalized)
        .map_err(|_| tonumber_parse_error(raw))?;
    match parsed {
        serde_json::Value::Number(_) => Value::from_json(parsed),
        _ => Err(tonumber_parse_error(raw)),
    }
}

fn tonumber_parse_error(raw: &str) -> AqError {
    let rendered = serde_json::to_string(raw).unwrap_or_else(|_| "\"\"".to_string());
    AqError::Query(format!("string ({rendered}) cannot be parsed as a number"))
}

fn normalize_integer_number_literal(raw: &str) -> Option<&str> {
    let normalized = raw.strip_prefix('+').unwrap_or(raw);
    let digits = normalized.strip_prefix('-').unwrap_or(normalized);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(normalized)
}

fn type_name(value: &Value) -> &'static str {
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

fn typed_rendered_value(value: &Value) -> (&'static str, String) {
    let value_type = type_name(value);
    let rendered = truncate_rendered_value_for_error(&rendered_value_for_error(value));
    (value_type, rendered)
}

fn rendered_value_for_error(value: &Value) -> String {
    match value.untagged() {
        Value::Null => return "null".to_string(),
        Value::Bool(value) => return value.to_string(),
        Value::Integer(value) => return value.to_string(),
        Value::Decimal(value) => return value.rendered().to_string(),
        Value::String(value) => {
            return serde_json::to_string(value).unwrap_or_else(|_| "string".to_string())
        }
        Value::Array(values) if values.is_empty() => return "[]".to_string(),
        Value::Object(values) if values.is_empty() => return "{}".to_string(),
        _ => {}
    }
    match value.to_json() {
        Ok(json) => match serde_json::to_string(&json) {
            Ok(rendered) => expand_scientific_notation(&rendered).unwrap_or(rendered),
            Err(_) => type_name(value).to_string(),
        },
        Err(_) => type_name(value).to_string(),
    }
}

fn truncate_rendered_value_for_error(rendered: &str) -> String {
    const BUFFER_SIZE: usize = 30;

    if rendered.len() <= BUFFER_SIZE.saturating_sub(1) || BUFFER_SIZE < 8 {
        return rendered
            .get(..rendered.len().min(BUFFER_SIZE.saturating_sub(1)))
            .unwrap_or(rendered)
            .to_string();
    }

    let delimiter = match rendered.as_bytes().first().copied() {
        Some(b'"') => Some('"'),
        Some(b'[') => Some(']'),
        Some(b'{') => Some('}'),
        _ => None,
    };
    let mut end = BUFFER_SIZE - if delimiter.is_some() { 5 } else { 4 };
    while end > 0 && !rendered.is_char_boundary(end) {
        end -= 1;
    }

    let mut out = String::with_capacity(BUFFER_SIZE.saturating_sub(1));
    out.push_str(&rendered[..end]);
    out.push_str("...");
    if let Some(delimiter) = delimiter {
        out.push(delimiter);
    }
    out
}

fn expand_scientific_notation(rendered: &str) -> Option<String> {
    let exponent_index = rendered.find(['e', 'E'])?;
    let mantissa = &rendered[..exponent_index];
    let exponent = rendered[exponent_index + 1..].parse::<i32>().ok()?;
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa
        .strip_prefix('-')
        .or_else(|| mantissa.strip_prefix('+'))
        .unwrap_or(mantissa);
    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if !whole.chars().all(|ch| ch.is_ascii_digit())
        || !fractional.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }

    let digits = format!("{whole}{fractional}");
    if digits.is_empty() {
        return None;
    }

    let decimal_index = whole.len() as i32 + exponent;
    let mut out = String::new();
    if negative {
        out.push('-');
    }

    if decimal_index <= 0 {
        out.push('0');
        out.push('.');
        out.push_str(&"0".repeat(decimal_index.unsigned_abs() as usize));
        out.push_str(&digits);
    } else if decimal_index as usize >= digits.len() {
        out.push_str(&digits);
        out.push_str(&"0".repeat(decimal_index as usize - digits.len()));
    } else {
        out.push_str(&digits[..decimal_index as usize]);
        out.push('.');
        out.push_str(&digits[decimal_index as usize..]);
    }

    if out.contains('.') {
        while out.ends_with('0') {
            out.pop();
        }
        if out.ends_with('.') {
            out.pop();
        }
    }

    Some(out)
}

fn binary_type_error(left: &Value, right: &Value, message: &str) -> AqError {
    let (left_type, left_rendered) = typed_rendered_value(left);
    let (right_type, right_rendered) = typed_rendered_value(right);
    AqError::Query(format!(
        "{left_type} ({left_rendered}) and {right_type} ({right_rendered}) {message}"
    ))
}

fn zero_division_error(left: &Value, right: &Value, remainder: bool) -> AqError {
    let message = if remainder {
        "cannot be divided (remainder) because the divisor is zero"
    } else {
        "cannot be divided because the divisor is zero"
    };
    binary_type_error(left, right, message)
}

fn is_truthy(value: &Value) -> bool {
    !matches!(value.untagged(), Value::Null | Value::Bool(false))
}

fn value_add(left: &Value, right: &Value) -> Result<Value, AqError> {
    let left = left.untagged();
    let right = right.untagged();
    match (left, right) {
        (Value::Null, value) => Ok(value.clone()),
        (value, Value::Null) => Ok(value.clone()),
        (left, right)
            if numeric_zero_identity_preserves_value(left)
                && numeric_zero_identity_preserves_value(right)
                && numeric_is_zero(left) =>
        {
            Ok(right.clone())
        }
        (left, right)
            if numeric_zero_identity_preserves_value(left)
                && numeric_zero_identity_preserves_value(right)
                && numeric_is_zero(right) =>
        {
            Ok(left.clone())
        }
        (Value::Decimal(left), Value::Decimal(right)) => Ok(lossy_numeric_result(
            left.to_f64_lossy() + right.to_f64_lossy(),
        )),
        (Value::Integer(left), Value::Decimal(right))
        | (Value::Decimal(right), Value::Integer(left)) => {
            Ok(lossy_numeric_result(*left as f64 + right.to_f64_lossy()))
        }
        (Value::Decimal(left), Value::Float(right))
        | (Value::Float(right), Value::Decimal(left)) => {
            Ok(lossy_numeric_result(left.to_f64_lossy() + *right))
        }
        (Value::Integer(left), Value::Integer(right))
            if !integer_is_safe_in_f64(*left) || !integer_is_safe_in_f64(*right) =>
        {
            Ok(lossy_numeric_result(*left as f64 + *right as f64))
        }
        (Value::Integer(left), Value::Integer(right)) => left
            .checked_add(*right)
            .map(Value::Integer)
            .ok_or_else(|| AqError::Query("integer addition overflow".to_string())),
        (Value::Float(left), Value::Float(right)) => Ok(Value::Float(left + right)),
        (Value::Integer(left), Value::Float(right)) => Ok(Value::Float(*left as f64 + right)),
        (Value::Float(left), Value::Integer(right)) => Ok(Value::Float(left + *right as f64)),
        (Value::String(left), Value::String(right)) => Ok(Value::String(format!("{left}{right}"))),
        (Value::Array(left), Value::Array(right)) => {
            let mut combined = left.clone();
            combined.extend(right.iter().cloned());
            Ok(Value::Array(combined))
        }
        (Value::Object(left), Value::Object(right)) => {
            let mut combined = left.clone();
            for (key, value) in right {
                combined.insert(key.clone(), value.clone());
            }
            Ok(Value::Object(combined))
        }
        _ => Err(binary_type_error(left, right, "cannot be added")),
    }
}

fn value_math(left: &Value, op: BinaryOp, right: &Value) -> Result<Value, AqError> {
    match op {
        BinaryOp::Sub => value_sub(left, right),
        BinaryOp::Mul => value_mul(left, right),
        BinaryOp::Div => value_div(left, right),
        BinaryOp::Mod => value_mod(left, right),
        _ => Err(AqError::Query(
            "internal error: non-math operator reached math evaluator".to_string(),
        )),
    }
}

fn value_sub(left: &Value, right: &Value) -> Result<Value, AqError> {
    let left = left.untagged();
    let right = right.untagged();
    match (left, right) {
        (left, right)
            if numeric_zero_identity_preserves_value(left)
                && numeric_zero_identity_preserves_value(right)
                && numeric_is_zero(right) =>
        {
            Ok(left.clone())
        }
        (Value::Decimal(left), Value::Decimal(right)) => Ok(lossy_numeric_result(
            left.to_f64_lossy() - right.to_f64_lossy(),
        )),
        (Value::Integer(left), Value::Decimal(right)) => {
            Ok(lossy_numeric_result(*left as f64 - right.to_f64_lossy()))
        }
        (Value::Decimal(left), Value::Integer(right)) => {
            Ok(lossy_numeric_result(left.to_f64_lossy() - *right as f64))
        }
        (Value::Decimal(left), Value::Float(right)) => {
            Ok(lossy_numeric_result(left.to_f64_lossy() - *right))
        }
        (Value::Float(left), Value::Decimal(right)) => {
            Ok(lossy_numeric_result(*left - right.to_f64_lossy()))
        }
        (Value::Integer(left), Value::Integer(right))
            if !integer_is_safe_in_f64(*left) || !integer_is_safe_in_f64(*right) =>
        {
            Ok(lossy_numeric_result(*left as f64 - *right as f64))
        }
        (Value::Integer(left), Value::Integer(right)) => left
            .checked_sub(*right)
            .map(Value::Integer)
            .ok_or_else(|| AqError::Query("integer subtraction overflow".to_string())),
        (Value::Float(left), Value::Float(right)) => Ok(Value::Float(left - right)),
        (Value::Integer(left), Value::Float(right)) => Ok(Value::Float(*left as f64 - right)),
        (Value::Float(left), Value::Integer(right)) => Ok(Value::Float(left - *right as f64)),
        (Value::Array(left), Value::Array(right)) => Ok(Value::Array(
            left.iter()
                .filter(|value| !right.contains(value))
                .cloned()
                .collect(),
        )),
        _ => Err(binary_type_error(left, right, "cannot be subtracted")),
    }
}

fn value_mul(left: &Value, right: &Value) -> Result<Value, AqError> {
    let left_value = left.untagged();
    let right_value = right.untagged();
    match (left_value, right_value) {
        (left, right)
            if numeric_one_identity_preserves_value(left)
                && numeric_one_identity_preserves_value(right)
                && numeric_is_one(left) =>
        {
            Ok(right.clone())
        }
        (left, right)
            if numeric_one_identity_preserves_value(left)
                && numeric_one_identity_preserves_value(right)
                && numeric_is_one(right) =>
        {
            Ok(left.clone())
        }
        (Value::Decimal(left), Value::Decimal(right)) => Ok(lossy_numeric_result(
            left.to_f64_lossy() * right.to_f64_lossy(),
        )),
        (Value::Integer(left), Value::Decimal(right))
        | (Value::Decimal(right), Value::Integer(left)) => {
            Ok(lossy_numeric_result(*left as f64 * right.to_f64_lossy()))
        }
        (Value::Decimal(left), Value::Float(right))
        | (Value::Float(right), Value::Decimal(left)) => {
            Ok(lossy_numeric_result(left.to_f64_lossy() * *right))
        }
        (Value::Integer(left), Value::Integer(right))
            if !integer_is_safe_in_f64(*left) || !integer_is_safe_in_f64(*right) =>
        {
            Ok(lossy_numeric_result(*left as f64 * *right as f64))
        }
        (Value::Integer(left), Value::Integer(right)) => left
            .checked_mul(*right)
            .map(Value::Integer)
            .ok_or_else(|| AqError::Query("integer multiplication overflow".to_string())),
        (Value::Float(left), Value::Float(right)) => Ok(Value::Float(left * right)),
        (Value::Integer(left), Value::Float(right)) => Ok(Value::Float(*left as f64 * right)),
        (Value::Float(left), Value::Integer(right)) => Ok(Value::Float(left * *right as f64)),
        (Value::String(value), Value::Integer(count)) => repeat_string(value, *count as f64),
        (Value::String(value), Value::Decimal(count)) => repeat_string(value, count.to_f64_lossy()),
        (Value::String(value), Value::Float(count)) => repeat_string(value, *count),
        (Value::Integer(count), Value::String(value)) => repeat_string(value, *count as f64),
        (Value::Decimal(count), Value::String(value)) => repeat_string(value, count.to_f64_lossy()),
        (Value::Float(count), Value::String(value)) => repeat_string(value, *count),
        (Value::Object(_), Value::Object(_)) => Ok(left.merged_with(right, true)),
        _ => Err(AqError::Query(format!(
            "cannot multiply {} and {}",
            kind_name(left_value),
            kind_name(right_value)
        ))),
    }
}

fn repeat_string(value: &str, count: f64) -> Result<Value, AqError> {
    if !count.is_finite() || count < 0.0 {
        return Ok(Value::Null);
    }

    let repeats = count.trunc();
    if repeats > usize::MAX as f64 {
        return Err(AqError::Query(
            "string repetition count is out of range".to_string(),
        ));
    }
    let repeats = repeats as usize;
    let total_bytes = value
        .len()
        .checked_mul(repeats)
        .ok_or_else(|| AqError::Query("Repeat string result too long".to_string()))?;
    if total_bytes > MAX_REPEATED_STRING_BYTES {
        return Err(AqError::Query("Repeat string result too long".to_string()));
    }
    Ok(Value::String(value.repeat(repeats)))
}

fn repeated_string_slice_array_value(
    value: &str,
    count: f64,
    slices: &[(Option<isize>, Option<isize>)],
) -> Result<Value, AqError> {
    let repeats = repeat_count_as_usize(count)?;
    let total_bytes = value
        .len()
        .checked_mul(repeats)
        .ok_or_else(|| AqError::Query("Repeat string result too long".to_string()))?;
    if total_bytes > MAX_REPEATED_STRING_BYTES {
        return Err(AqError::Query("Repeat string result too long".to_string()));
    }

    let values = if value.is_ascii() {
        let total_len = value
            .len()
            .checked_mul(repeats)
            .ok_or_else(|| AqError::Query("Repeat string result too long".to_string()))?;
        slices
            .iter()
            .map(|(start, end)| {
                Value::String(repeated_ascii_string_slice(value, total_len, *start, *end))
            })
            .collect::<Vec<_>>()
    } else {
        let chars: Vec<char> = value.chars().collect();
        let total_len = chars
            .len()
            .checked_mul(repeats)
            .ok_or_else(|| AqError::Query("Repeat string result too long".to_string()))?;
        slices
            .iter()
            .map(|(start, end)| {
                Value::String(repeated_unicode_string_slice(
                    &chars, total_len, *start, *end,
                ))
            })
            .collect::<Vec<_>>()
    };
    Ok(Value::Array(values))
}

fn repeat_count_as_usize(count: f64) -> Result<usize, AqError> {
    let repeats = count.trunc();
    if repeats > usize::MAX as f64 {
        return Err(AqError::Query(
            "string repetition count is out of range".to_string(),
        ));
    }
    Ok(repeats as usize)
}

fn repeated_ascii_string_slice(
    value: &str,
    total_len: usize,
    start: Option<isize>,
    end: Option<isize>,
) -> String {
    let bytes = value.as_bytes();
    let base_len = bytes.len();
    let (start, end) = resolve_slice_bounds(start, end, total_len);
    let slice_len = end.saturating_sub(start);
    if slice_len == 0 || base_len == 0 {
        return String::new();
    }

    let mut out = String::with_capacity(slice_len);
    let mut index = start;
    while index < end {
        let offset = index % base_len;
        let available = base_len - offset;
        let remaining = end - index;
        let chunk_len = available.min(remaining);
        out.push_str(&value[offset..offset + chunk_len]);
        index += chunk_len;
    }
    out
}

fn repeated_unicode_string_slice(
    chars: &[char],
    total_len: usize,
    start: Option<isize>,
    end: Option<isize>,
) -> String {
    let base_len = chars.len();
    let (start, end) = resolve_slice_bounds(start, end, total_len);
    let slice_len = end.saturating_sub(start);
    if slice_len == 0 || base_len == 0 {
        return String::new();
    }

    let mut out = String::new();
    for index in start..end {
        out.push(chars[index % base_len]);
    }
    out
}

fn value_div(left: &Value, right: &Value) -> Result<Value, AqError> {
    let left = left.untagged();
    let right = right.untagged();
    match (left, right) {
        (left, right)
            if numeric_one_identity_preserves_value(left)
                && numeric_one_identity_preserves_value(right)
                && numeric_is_one(right) =>
        {
            Ok(left.clone())
        }
        (Value::Decimal(left), Value::Decimal(right)) => {
            divide_floats(left.to_f64_lossy(), right.to_f64_lossy())
        }
        (Value::Integer(left), Value::Decimal(right)) => {
            divide_floats(*left as f64, right.to_f64_lossy())
        }
        (Value::Decimal(left), Value::Integer(right)) => {
            divide_floats(left.to_f64_lossy(), *right as f64)
        }
        (Value::Float(left), Value::Decimal(right)) => divide_floats(*left, right.to_f64_lossy()),
        (Value::Decimal(left), Value::Float(right)) => divide_floats(left.to_f64_lossy(), *right),
        (Value::Integer(left), Value::Integer(right)) => {
            if *right == 0 {
                return Err(zero_division_error(
                    &Value::Integer(*left),
                    &Value::Integer(*right),
                    false,
                ));
            }
            if left % right == 0 {
                Ok(Value::Integer(left / right))
            } else {
                Ok(Value::Float(*left as f64 / *right as f64))
            }
        }
        (Value::Float(left), Value::Float(right)) => divide_floats(*left, *right),
        (Value::Integer(left), Value::Float(right)) => divide_floats(*left as f64, *right),
        (Value::Float(left), Value::Integer(right)) => divide_floats(*left, *right as f64),
        (Value::String(left), Value::String(right)) => {
            if right.is_empty() {
                return Ok(Value::Array(
                    left.chars()
                        .map(|value| Value::String(value.to_string()))
                        .collect(),
                ));
            }
            Ok(Value::Array(
                left.split(right)
                    .map(|part| Value::String(part.to_string()))
                    .collect(),
            ))
        }
        _ => Err(binary_type_error(left, right, "cannot be divided")),
    }
}

fn divide_floats(left: f64, right: f64) -> Result<Value, AqError> {
    if right == 0.0 {
        return Err(zero_division_error(
            &Value::Float(left),
            &Value::Float(right),
            false,
        ));
    }
    Ok(Value::Float(left / right))
}

fn numeric_is_zero(value: &Value) -> bool {
    match value {
        Value::Integer(value) => *value == 0,
        Value::Decimal(value) => value.rendered() == "0",
        Value::Float(value) => *value == 0.0,
        _ => false,
    }
}

fn numeric_zero_identity_preserves_value(value: &Value) -> bool {
    match value {
        Value::Integer(value) => integer_is_safe_in_f64(*value),
        Value::Decimal(value) => value.is_lossy_float(),
        Value::Float(_) => true,
        _ => false,
    }
}

fn numeric_one_identity_preserves_value(value: &Value) -> bool {
    match value {
        Value::Integer(value) => integer_is_safe_in_f64(*value),
        Value::Decimal(value) => value.is_lossy_float(),
        Value::Float(_) => true,
        _ => false,
    }
}

fn numeric_is_one(value: &Value) -> bool {
    match value {
        Value::Integer(value) => *value == 1,
        Value::Decimal(value) => value.rendered() == "1",
        Value::Float(value) => *value == 1.0,
        _ => false,
    }
}

fn value_mod(left: &Value, right: &Value) -> Result<Value, AqError> {
    let left = left.untagged();
    let right = right.untagged();
    match (left, right) {
        (Value::Decimal(left), Value::Decimal(right)) => {
            let right_value = right.to_f64_lossy();
            if right_value == 0.0 {
                return Err(zero_division_error(
                    &Value::Decimal(left.clone()),
                    &Value::Decimal(right.clone()),
                    true,
                ));
            }
            Ok(modulo_floats(left.to_f64_lossy(), right_value))
        }
        (Value::Integer(left), Value::Decimal(right)) => {
            let right_value = right.to_f64_lossy();
            if right_value == 0.0 {
                return Err(zero_division_error(
                    &Value::Integer(*left),
                    &Value::Decimal(right.clone()),
                    true,
                ));
            }
            Ok(modulo_floats(*left as f64, right_value))
        }
        (Value::Decimal(left), Value::Integer(right)) => {
            if *right == 0 {
                return Err(zero_division_error(
                    &Value::Decimal(left.clone()),
                    &Value::Integer(*right),
                    true,
                ));
            }
            Ok(modulo_floats(left.to_f64_lossy(), *right as f64))
        }
        (Value::Float(left), Value::Decimal(right)) => {
            let right_value = right.to_f64_lossy();
            if right_value == 0.0 {
                return Err(zero_division_error(
                    &Value::Float(*left),
                    &Value::Decimal(right.clone()),
                    true,
                ));
            }
            Ok(modulo_floats(*left, right_value))
        }
        (Value::Decimal(left), Value::Float(right)) => {
            if *right == 0.0 {
                return Err(zero_division_error(
                    &Value::Decimal(left.clone()),
                    &Value::Float(*right),
                    true,
                ));
            }
            Ok(modulo_floats(left.to_f64_lossy(), *right))
        }
        (Value::Integer(left), Value::Integer(right)) => {
            if *right == 0 {
                return Err(zero_division_error(
                    &Value::Integer(*left),
                    &Value::Integer(*right),
                    true,
                ));
            }
            Ok(Value::Integer(left % right))
        }
        (Value::Float(left), Value::Float(right)) => {
            if *right == 0.0 {
                return Err(zero_division_error(
                    &Value::Float(*left),
                    &Value::Float(*right),
                    true,
                ));
            }
            Ok(modulo_floats(*left, *right))
        }
        (Value::Integer(left), Value::Float(right)) => {
            if *right == 0.0 {
                return Err(zero_division_error(
                    &Value::Integer(*left),
                    &Value::Float(*right),
                    true,
                ));
            }
            Ok(modulo_floats(*left as f64, *right))
        }
        (Value::Float(left), Value::Integer(right)) => {
            if *right == 0 {
                return Err(zero_division_error(
                    &Value::Float(*left),
                    &Value::Integer(*right),
                    true,
                ));
            }
            Ok(modulo_floats(*left, *right as f64))
        }
        _ => Err(binary_type_error(
            left,
            right,
            "cannot be divided (remainder)",
        )),
    }
}

fn modulo_floats(left: f64, right: f64) -> Value {
    // jq prints `infinite` as `f64::MAX` and carries a quirky remainder for `-infinite % infinite`.
    if left == -f64::MAX && right == f64::MAX {
        return Value::Integer(-1);
    }
    normalize_number_value(left % right)
}

fn value_has(input: &Value, key: &Value) -> Result<bool, AqError> {
    let input = input.untagged();
    let key = key.untagged();
    match (input, key) {
        (Value::Object(values), Value::String(key)) => Ok(values.contains_key(key)),
        (Value::Array(values), Value::Integer(index)) => {
            Ok(resolve_index(*index as isize, values.len()).is_some())
        }
        (Value::Array(values), Value::Decimal(index)) => Ok(decimal_index_value(index)
            .and_then(|index| isize::try_from(index).ok())
            .and_then(|index| resolve_index(index, values.len()))
            .is_some()),
        (Value::Array(values), Value::Float(index))
            if index.is_finite()
                && index.fract() == 0.0
                && *index >= isize::MIN as f64
                && *index <= isize::MAX as f64 =>
        {
            Ok(resolve_index(*index as isize, values.len()).is_some())
        }
        (Value::Array(_), Value::Float(_)) => Ok(false),
        (Value::Array(_), other) => Err(AqError::Query(format!(
            "has expects integer indices for arrays, got {}",
            kind_name(other)
        ))),
        (other, _) => Err(AqError::Query(format!(
            "has is not defined for {}",
            kind_name(other)
        ))),
    }
}

fn decimal_index_value(value: &DecimalValue) -> Option<i64> {
    value.as_i64_exact().or_else(|| {
        let (whole, fractional) = value.rendered().split_once('.')?;
        if fractional.chars().all(|digit| digit == '0') {
            whole.parse::<i64>().ok()
        } else {
            None
        }
    })
}

fn inside_of(
    input: &Value,
    container_expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    if let Some(container) = literal_expr_value(container_expr) {
        return contains_value(container, input);
    }
    let containers = evaluate_expr(container_expr, input, bindings, context)?;
    let mut matched = false;
    for container in containers {
        matched = true;
        if !contains_value(&container, input)? {
            return Ok(false);
        }
    }
    if matched {
        Ok(true)
    } else {
        Ok(false)
    }
}

fn isempty_of(
    input: &Value,
    expr: &Expr,
    bindings: &Bindings,
    context: &EvaluationContext,
) -> Result<bool, AqError> {
    match expr {
        Expr::Subquery(query) => {
            Ok(evaluate_query_up_to(query, input, bindings, context, 1)?.is_empty())
        }
        _ => Ok(evaluate_expr(expr, input, bindings, context)?.is_empty()),
    }
}

fn contains_value(input: &Value, expected: &Value) -> Result<bool, AqError> {
    let input = input.untagged();
    let expected = expected.untagged();
    if input == expected {
        return Ok(true);
    }
    match (input, expected) {
        (Value::String(input), Value::String(expected)) => Ok(input.contains(expected)),
        (_, Value::Array(expected)) if expected.is_empty() => Ok(true),
        (Value::Array(input), Value::Array(expected)) => {
            if input.iter().all(is_simple_contains_scalar)
                && expected.iter().all(is_simple_contains_scalar)
            {
                return Ok(expected.iter().all(|expected_item| {
                    input.iter().any(|input_item| {
                        contains_simple_scalar_value(input_item, expected_item).unwrap_or(false)
                    })
                }));
            }
            for expected_item in expected {
                let mut found = false;
                for input_item in input {
                    if contains_value(input_item, expected_item)? {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (_, Value::Object(expected)) if expected.is_empty() => Ok(true),
        (Value::Object(input), Value::Object(expected)) => {
            if input.len() < expected.len() {
                return Ok(false);
            }
            for (key, expected_value) in expected {
                let Some(input_value) = input.get(key) else {
                    return Ok(false);
                };
                if !contains_value(input_value, expected_value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(input == expected),
    }
}

fn is_simple_contains_scalar(value: &Value) -> bool {
    matches!(
        value.untagged(),
        Value::Null
            | Value::Bool(_)
            | Value::Integer(_)
            | Value::Decimal(_)
            | Value::Float(_)
            | Value::String(_)
    )
}

fn contains_simple_scalar_value(input: &Value, expected: &Value) -> Option<bool> {
    match (input.untagged(), expected.untagged()) {
        (Value::String(input), Value::String(expected)) => Some(input.contains(expected)),
        (Value::Null, Value::Null)
        | (Value::Bool(_), Value::Bool(_))
        | (Value::Integer(_), Value::Integer(_))
        | (Value::Integer(_), Value::Decimal(_))
        | (Value::Integer(_), Value::Float(_))
        | (Value::Decimal(_), Value::Integer(_))
        | (Value::Decimal(_), Value::Decimal(_))
        | (Value::Decimal(_), Value::Float(_))
        | (Value::Float(_), Value::Integer(_))
        | (Value::Float(_), Value::Decimal(_))
        | (Value::Float(_), Value::Float(_)) => Some(input == expected),
        _ => None,
    }
}

fn compare_sort_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let left = left.untagged();
    let right = right.untagged();
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Bool(_), _) => Ordering::Less,
        (_, Value::Bool(_)) => Ordering::Greater,
        (Value::Integer(_), Value::Integer(_))
        | (Value::Integer(_), Value::Decimal(_))
        | (Value::Decimal(_), Value::Integer(_))
        | (Value::Decimal(_), Value::Decimal(_))
        | (Value::Float(_), Value::Float(_))
        | (Value::Integer(_), Value::Float(_))
        | (Value::Float(_), Value::Integer(_))
        | (Value::Decimal(_), Value::Float(_))
        | (Value::Float(_), Value::Decimal(_)) => compare_numeric_order(left, right),
        (Value::Integer(_) | Value::Decimal(_) | Value::Float(_), _) => Ordering::Less,
        (_, Value::Integer(_) | Value::Decimal(_) | Value::Float(_)) => Ordering::Greater,
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::String(_), _) => Ordering::Less,
        (_, Value::String(_)) => Ordering::Greater,
        (Value::Array(left), Value::Array(right)) => compare_sort_arrays(left, right),
        (Value::Array(_), _) => Ordering::Less,
        (_, Value::Array(_)) => Ordering::Greater,
        (Value::Object(left), Value::Object(right)) => compare_sort_objects(left, right),
        (Value::Object(_), _) => Ordering::Less,
        (_, Value::Object(_)) => Ordering::Greater,
        (Value::Bytes(left), Value::Bytes(right)) => left.cmp(right),
        (Value::Bytes(_), _) => Ordering::Less,
        (_, Value::Bytes(_)) => Ordering::Greater,
        (Value::DateTime(left), Value::DateTime(right)) => left.cmp(right),
        (Value::DateTime(_), _) => Ordering::Less,
        (_, Value::DateTime(_)) => Ordering::Greater,
        (Value::Date(left), Value::Date(right)) => left.cmp(right),
        _ => unreachable!("untagged values should not be tagged"),
    }
}

fn compare_sort_arrays(left: &[Value], right: &[Value]) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let ordering = compare_sort_values(left_value, right_value);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    left.len().cmp(&right.len())
}

fn compare_sort_objects(
    left: &IndexMap<String, Value>,
    right: &IndexMap<String, Value>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let mut left_keys: Vec<&String> = left.keys().collect();
    let mut right_keys: Vec<&String> = right.keys().collect();
    left_keys.sort();
    right_keys.sort();

    for (left_key, right_key) in left_keys.iter().zip(right_keys.iter()) {
        let ordering = left_key.cmp(right_key);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    let key_ordering = left_keys.len().cmp(&right_keys.len());
    if key_ordering != Ordering::Equal {
        return key_ordering;
    }

    for key in left_keys {
        let Some(left_value) = left.get(key) else {
            continue;
        };
        let Some(right_value) = right.get(key) else {
            continue;
        };
        let ordering = compare_sort_values(left_value, right_value);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

fn apply_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<bool, AqError> {
    match op {
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Alt
        | BinaryOp::And
        | BinaryOp::Or => Err(AqError::Query(
            "internal error: non-comparison operator reached comparison evaluator".to_string(),
        )),
        BinaryOp::Eq => Ok(left == right),
        BinaryOp::Ne => Ok(left != right),
        BinaryOp::Lt => compare_values(left, right, |left, right| left < right),
        BinaryOp::Le => compare_values(left, right, |left, right| left <= right),
        BinaryOp::Gt => compare_values(left, right, |left, right| left > right),
        BinaryOp::Ge => compare_values(left, right, |left, right| left >= right),
    }
}

fn compare_values(
    left: &Value,
    right: &Value,
    cmp: impl Fn(std::cmp::Ordering, std::cmp::Ordering) -> bool,
) -> Result<bool, AqError> {
    match (left, right) {
        (Value::Integer(_), Value::Integer(_))
        | (Value::Integer(_), Value::Decimal(_))
        | (Value::Decimal(_), Value::Integer(_))
        | (Value::Decimal(_), Value::Decimal(_))
        | (Value::Float(_), Value::Float(_))
        | (Value::Integer(_), Value::Float(_))
        | (Value::Float(_), Value::Integer(_))
        | (Value::Decimal(_), Value::Float(_))
        | (Value::Float(_), Value::Decimal(_)) => Ok(cmp(
            compare_numeric_order(left, right),
            std::cmp::Ordering::Equal,
        )),
        (Value::String(left), Value::String(right)) => {
            Ok(cmp(left.cmp(right), std::cmp::Ordering::Equal))
        }
        _ => Err(AqError::Query(format!(
            "cannot compare {} with {}",
            kind_name(left),
            kind_name(right)
        ))),
    }
}

struct Parser<'a> {
    chars: Vec<char>,
    input: &'a str,
    index: usize,
    known_functions: IndexMap<FunctionKey, ()>,
    parameter_names: BTreeSet<String>,
    options: ParseOptions,
    module_loader: Rc<RefCell<ModuleLoader>>,
    allow_directives_only: bool,
}

impl<'a> Parser<'a> {
    fn with_options(
        input: &'a str,
        options: ParseOptions,
        module_loader: Rc<RefCell<ModuleLoader>>,
        allow_directives_only: bool,
    ) -> Self {
        Self {
            chars: input.chars().collect(),
            input,
            index: 0,
            known_functions: IndexMap::new(),
            parameter_names: BTreeSet::new(),
            options,
            module_loader,
            allow_directives_only,
        }
    }

    fn parse_query(mut self) -> Result<Query, AqError> {
        self.skip_ws();
        if self.is_eof() {
            return Ok(Query {
                functions: Vec::new(),
                outputs: vec![Pipeline {
                    stages: vec![Expr::Path(PathExpr {
                        segments: Vec::new(),
                    })],
                }],
                imported_values: IndexMap::new(),
                module_info: None,
            });
        }

        let query = self.parse_query_until(None, &[])?;
        self.skip_ws();
        if !self.is_eof() {
            return Err(AqError::InvalidExpression(format!(
                "unexpected token `{}` in expression",
                self.peek().unwrap_or_default()
            )));
        }
        Ok(query)
    }

    fn parse_query_until(
        &mut self,
        terminator: Option<char>,
        stop_keywords: &[&str],
    ) -> Result<Query, AqError> {
        self.parse_query_until_with_stops(terminator, stop_keywords, &[], true)
    }

    fn parse_query_until_with_stops(
        &mut self,
        terminator: Option<char>,
        stop_keywords: &[&str],
        stop_chars: &[char],
        allow_binding: bool,
    ) -> Result<Query, AqError> {
        let saved_functions = self.known_functions.clone();
        let mut functions = Vec::new();
        let mut imported_values = IndexMap::new();
        let mut module_metadata = IndexMap::new();
        let mut module_deps = Vec::new();
        let mut module_defs = Vec::new();
        loop {
            self.skip_ws();
            if terminator.is_some_and(|terminator| self.peek() == Some(terminator)) {
                break;
            }
            if self.peek().is_some_and(|value| stop_chars.contains(&value)) {
                break;
            }
            if stop_keywords
                .iter()
                .any(|keyword| self.starts_with_keyword(keyword))
            {
                break;
            }
            if self.starts_with_keyword("def") {
                let function = self.parse_function_def()?;
                module_defs.push(format!("{}/{}", function.name, function.params.len()));
                functions.push(function);
                self.skip_ws();
                self.expect(';')?;
                continue;
            }
            if self.starts_with_keyword("module") {
                module_metadata = self.parse_module_directive()?;
                self.skip_ws();
                self.expect(';')?;
                continue;
            }
            if self.starts_with_keyword("import") {
                let import = self.parse_import_directive()?;
                functions.extend(import.functions);
                imported_values.extend(import.imported_values);
                if let Some(dependency) = import.dependency {
                    module_deps.push(dependency);
                }
                self.skip_ws();
                self.expect(';')?;
                continue;
            }
            if self.starts_with_keyword("include") {
                let include = self.parse_include_directive()?;
                functions.extend(include.functions);
                if let Some(dependency) = include.dependency {
                    module_deps.push(dependency);
                }
                self.skip_ws();
                self.expect(';')?;
                continue;
            }
            break;
        }

        self.skip_ws();
        if terminator.is_some_and(|terminator| self.peek() == Some(terminator))
            || self.peek().is_some_and(|value| stop_chars.contains(&value))
            || stop_keywords
                .iter()
                .any(|keyword| self.starts_with_keyword(keyword))
            || self.is_eof()
        {
            self.known_functions = saved_functions;
            if self.allow_directives_only {
                return Ok(Query {
                    functions,
                    outputs: Vec::new(),
                    imported_values,
                    module_info: Some(ModuleInfo {
                        metadata: module_metadata,
                        deps: module_deps,
                        defs: module_defs,
                    }),
                });
            }
            return Err(AqError::InvalidExpression(
                "expected filter after definition".to_string(),
            ));
        }

        let mut outputs =
            vec![self.parse_pipeline(allow_binding, terminator, stop_keywords, stop_chars)?];
        loop {
            self.skip_ws();
            if terminator.is_some_and(|terminator| self.peek() == Some(terminator)) {
                break;
            }
            if self.peek().is_some_and(|value| stop_chars.contains(&value)) {
                break;
            }
            if stop_keywords
                .iter()
                .any(|keyword| self.starts_with_keyword(keyword))
            {
                break;
            }
            if !self.consume(',') {
                break;
            }
            self.skip_ws();
            outputs.push(self.parse_pipeline(
                allow_binding,
                terminator,
                stop_keywords,
                stop_chars,
            )?);
        }
        self.known_functions = saved_functions;
        Ok(Query {
            functions,
            outputs,
            imported_values,
            module_info: None,
        })
    }

    fn parse_function_def(&mut self) -> Result<FunctionDef, AqError> {
        self.expect_keyword("def")?;
        self.skip_ws();
        let name = self.parse_identifier_key()?;
        let params = self.parse_function_params()?;
        let param_names = params
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        self.skip_ws();
        self.expect(':')?;
        self.skip_ws();

        let key = FunctionKey {
            name: name.clone(),
            arity: param_names.len(),
        };
        self.known_functions.insert(key, ());
        let saved_params = self.parameter_names.clone();
        self.parameter_names.extend(param_names.iter().cloned());
        let mut body = self.parse_query_until_with_stops(None, &[], &[';'], true)?;
        self.parameter_names = saved_params;
        let variable_params = params
            .iter()
            .filter_map(|(name, is_variable)| is_variable.then_some(name.clone()))
            .collect::<Vec<_>>();
        if !variable_params.is_empty() {
            body = prepend_function_param_bindings(body, &variable_params);
        }
        Ok(FunctionDef {
            name,
            params: param_names,
            body,
            captured_values: IndexMap::new(),
        })
    }

    fn parse_function_params(&mut self) -> Result<Vec<(String, bool)>, AqError> {
        self.skip_ws();
        if !self.consume('(') {
            return Ok(Vec::new());
        }
        self.skip_ws();
        let mut params = Vec::new();
        if self.consume(')') {
            return Ok(params);
        }
        loop {
            params.push(if self.peek() == Some('$') {
                (self.parse_variable_name()?, true)
            } else {
                (self.parse_identifier_key()?, false)
            });
            self.skip_ws();
            if self.consume(')') {
                break;
            }
            self.expect(';')?;
            self.skip_ws();
        }
        Ok(params)
    }

    fn parse_module_directive(&mut self) -> Result<IndexMap<String, Value>, AqError> {
        self.expect_keyword("module")?;
        self.skip_ws();
        let metadata = self.parse_metadata_value()?;
        if !self.allow_directives_only {
            return Err(AqError::InvalidExpression(
                "module directives are only supported in jq module files".to_string(),
            ));
        }
        Ok(metadata)
    }

    fn parse_import_directive(&mut self) -> Result<DirectiveImport, AqError> {
        self.expect_keyword("import")?;
        self.skip_ws();
        let module_name = self.parse_module_path_literal()?;
        self.skip_ws();
        self.expect_keyword("as")?;
        self.skip_ws();
        let (alias, is_data) = if self.peek() == Some('$') {
            (self.parse_variable_name()?, true)
        } else {
            (self.parse_identifier_key()?, false)
        };
        self.skip_ws();
        let metadata = self.parse_optional_metadata_value()?;
        let search = metadata_search_path(metadata.as_ref())?;
        let dependency = Some(ModuleDependency {
            alias: Some(alias.clone()),
            is_data,
            relpath: module_name.clone(),
            search: search.clone(),
        });
        if is_data {
            let value = self.load_data_module_value(&module_name, search.as_deref())?;
            let mut imported_values = IndexMap::new();
            imported_values.insert(alias.clone(), value.clone());
            imported_values.insert(format!("{alias}::{alias}"), value);
            return Ok(DirectiveImport {
                functions: Vec::new(),
                imported_values,
                dependency,
            });
        }

        let query = self.load_module_query(&module_name, search.as_deref())?;
        let functions = namespace_module_functions(query.functions, &alias);
        self.register_function_defs(&functions);
        Ok(DirectiveImport {
            functions,
            imported_values: IndexMap::new(),
            dependency,
        })
    }

    fn parse_include_directive(&mut self) -> Result<DirectiveImport, AqError> {
        self.expect_keyword("include")?;
        self.skip_ws();
        let module_name = self.parse_module_path_literal()?;
        self.skip_ws();
        let metadata = self.parse_optional_metadata_value()?;
        let search = metadata_search_path(metadata.as_ref())?;
        let query = self.load_module_query(&module_name, search.as_deref())?;
        self.register_function_defs(&query.functions);
        Ok(DirectiveImport {
            functions: query.functions,
            imported_values: IndexMap::new(),
            dependency: Some(ModuleDependency {
                alias: None,
                is_data: false,
                relpath: module_name,
                search,
            }),
        })
    }

    fn load_module_query(&self, module_name: &str, search: Option<&str>) -> Result<Query, AqError> {
        let Some(module_dir) = self.options.module_dir.as_ref() else {
            return Err(AqError::InvalidExpression(format!(
                "module loading is unavailable without a base directory for `{module_name}`"
            )));
        };
        let library_paths = effective_module_library_paths(module_dir, &self.options.library_paths);
        let path = resolve_module_path(module_dir, &library_paths, module_name, search, "jq")?;
        load_module_query(&self.module_loader, path, &library_paths)
    }

    fn load_data_module_value(
        &self,
        module_name: &str,
        search: Option<&str>,
    ) -> Result<Value, AqError> {
        let Some(module_dir) = self.options.module_dir.as_ref() else {
            return Err(AqError::InvalidExpression(format!(
                "module loading is unavailable without a base directory for `{module_name}`"
            )));
        };
        let library_paths = effective_module_library_paths(module_dir, &self.options.library_paths);
        load_data_module_value(module_dir, &library_paths, module_name, search)
    }

    fn register_function_defs(&mut self, functions: &[FunctionDef]) {
        for function in functions {
            self.known_functions.insert(
                FunctionKey {
                    name: function.name.clone(),
                    arity: function.params.len(),
                },
                (),
            );
        }
    }

    fn parse_optional_metadata_value(
        &mut self,
    ) -> Result<Option<IndexMap<String, Value>>, AqError> {
        self.skip_ws();
        if self.peek().is_none() || self.peek() == Some(';') {
            Ok(None)
        } else {
            Ok(Some(self.parse_metadata_value()?))
        }
    }

    fn parse_metadata_value(&mut self) -> Result<IndexMap<String, Value>, AqError> {
        let raw = self.capture_metadata_expr()?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(AqError::InvalidExpression(
                "expected module metadata object".to_string(),
            ));
        }
        let query = parse(trimmed)?;
        if !query_is_simple_constant(&query) {
            return Err(AqError::InvalidExpression(
                "module metadata must be constant".to_string(),
            ));
        }
        let values = evaluate(&query, &Value::Null).map_err(|_| {
            AqError::InvalidExpression("module metadata must be constant".to_string())
        })?;
        let values = values.into_vec();
        let [value] = values.as_slice() else {
            return Err(AqError::InvalidExpression(
                "module metadata must be constant".to_string(),
            ));
        };
        if let Value::Object(object) = value {
            return Ok(object.clone());
        }
        Err(AqError::InvalidExpression(
            "module metadata must be an object".to_string(),
        ))
    }

    fn capture_metadata_expr(&mut self) -> Result<String, AqError> {
        let start = self.index;
        let mut brace_depth = 0usize;
        let mut bracket_depth = 0usize;
        let mut paren_depth = 0usize;
        let mut in_string = false;
        let mut escaped = false;
        while let Some(value) = self.peek() {
            self.index += 1;
            if in_string {
                if escaped {
                    escaped = false;
                    continue;
                }
                match value {
                    '\\' => escaped = true,
                    '"' => in_string = false,
                    _ => {}
                }
                continue;
            }

            match value {
                '"' => in_string = true,
                '{' => brace_depth += 1,
                '}' => brace_depth = brace_depth.saturating_sub(1),
                '[' => bracket_depth += 1,
                ']' => bracket_depth = bracket_depth.saturating_sub(1),
                '(' => paren_depth += 1,
                ')' => paren_depth = paren_depth.saturating_sub(1),
                ';' if brace_depth == 0 && bracket_depth == 0 && paren_depth == 0 => {
                    self.index = self.index.saturating_sub(1);
                    let raw: String = self.chars[start..self.index].iter().collect();
                    return Ok(raw);
                }
                _ => {}
            }
        }
        Err(AqError::InvalidExpression(
            "unterminated module metadata expression".to_string(),
        ))
    }

    fn parse_module_path_literal(&mut self) -> Result<String, AqError> {
        let start = self.index;
        self.expect('"')?;
        let mut escaped = false;
        while let Some(value) = self.peek() {
            self.index += 1;
            if escaped {
                if value == '(' {
                    return Err(AqError::InvalidExpression(
                        "import path must be constant".to_string(),
                    ));
                }
                escaped = false;
                continue;
            }
            match value {
                '\\' => escaped = true,
                '"' => {
                    let raw: String = self.chars[start..self.index].iter().collect();
                    return serde_json::from_str(&raw).map_err(|error| {
                        let message = bump_column_in_error_message(&error.to_string(), 1);
                        AqError::InvalidExpression(format!(
                            "invalid quoted field in expression `{}`: {message}",
                            self.input,
                        ))
                    });
                }
                _ => {}
            }
        }
        Err(AqError::InvalidExpression(
            "unterminated quoted field".to_string(),
        ))
    }

    fn parse_pipeline(
        &mut self,
        allow_binding: bool,
        terminator: Option<char>,
        stop_keywords: &[&str],
        stop_chars: &[char],
    ) -> Result<Pipeline, AqError> {
        let mut stages = vec![self.parse_pipeline_stage_query(
            allow_binding,
            terminator,
            stop_keywords,
            stop_chars,
            false,
        )?];
        loop {
            self.skip_ws();
            if !self.consume('|') {
                break;
            }
            self.skip_ws();
            if self.starts_with_keyword("def") {
                let query = self.parse_query_until_with_stops(
                    terminator,
                    stop_keywords,
                    stop_chars,
                    allow_binding,
                )?;
                stages.push(Expr::Subquery(Box::new(query)));
                break;
            }
            stages.push(self.parse_pipeline_stage_query(
                allow_binding,
                terminator,
                stop_keywords,
                stop_chars,
                true,
            )?);
        }
        Ok(Pipeline { stages })
    }

    fn parse_pipeline_stage_query(
        &mut self,
        allow_binding: bool,
        terminator: Option<char>,
        stop_keywords: &[&str],
        stop_chars: &[char],
        force_group_commas: bool,
    ) -> Result<Expr, AqError> {
        let first = self.parse_stage(allow_binding, terminator, stop_keywords, stop_chars)?;
        let mut outputs = vec![Pipeline {
            stages: vec![first],
        }];
        loop {
            self.skip_ws();
            if self.peek() != Some(',')
                || (!force_group_commas
                    && !self.has_top_level_pipe_ahead(terminator, stop_keywords, stop_chars))
            {
                break;
            }
            self.index += 1;
            self.skip_ws();
            outputs.push(Pipeline {
                stages: vec![self.parse_stage(
                    allow_binding,
                    terminator,
                    stop_keywords,
                    stop_chars,
                )?],
            });
        }
        if outputs.len() == 1 {
            let Some(pipeline) = outputs.pop() else {
                return Err(AqError::InvalidExpression(
                    "internal error: missing pipeline stage".to_string(),
                ));
            };
            let mut stages = pipeline.stages;
            return stages.pop().ok_or_else(|| {
                AqError::InvalidExpression("internal error: missing pipeline stage".to_string())
            });
        }
        Ok(Expr::Subquery(Box::new(Query {
            functions: Vec::new(),
            outputs,
            imported_values: IndexMap::new(),
            module_info: None,
        })))
    }

    fn has_top_level_pipe_ahead(
        &self,
        terminator: Option<char>,
        stop_keywords: &[&str],
        stop_chars: &[char],
    ) -> bool {
        let mut index = self.index;
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;
        let mut brace_depth = 0usize;
        let mut in_string = false;
        let mut escaped = false;

        while let Some(value) = self.chars.get(index).copied() {
            if in_string {
                index += 1;
                if escaped {
                    escaped = false;
                    continue;
                }
                match value {
                    '\\' => escaped = true,
                    '"' => in_string = false,
                    _ => {}
                }
                continue;
            }

            match value {
                '"' => in_string = true,
                '(' => paren_depth += 1,
                ')' => {
                    if paren_depth == 0 && terminator == Some(')') {
                        return false;
                    }
                    paren_depth = paren_depth.saturating_sub(1);
                }
                '[' => bracket_depth += 1,
                ']' => {
                    if bracket_depth == 0 && terminator == Some(']') {
                        return false;
                    }
                    bracket_depth = bracket_depth.saturating_sub(1);
                }
                '{' => brace_depth += 1,
                '}' => {
                    if brace_depth == 0 && terminator == Some('}') {
                        return false;
                    }
                    brace_depth = brace_depth.saturating_sub(1);
                }
                _ if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                    if terminator == Some(value) || stop_chars.contains(&value) {
                        return false;
                    }
                    if value == '|' && self.chars.get(index + 1) != Some(&'=') {
                        return true;
                    }
                    if value.is_ascii_alphabetic()
                        && stop_keywords.iter().any(|keyword| {
                            self.input[index..].starts_with(keyword)
                                && !matches!(
                                    self.chars.get(index + keyword.len()),
                                    Some(next) if is_identifier_continue(*next)
                                )
                        })
                    {
                        return false;
                    }
                }
                _ => {}
            }
            index += 1;
        }

        false
    }

    fn parse_stage(
        &mut self,
        allow_binding: bool,
        terminator: Option<char>,
        stop_keywords: &[&str],
        stop_chars: &[char],
    ) -> Result<Expr, AqError> {
        if self.starts_with_keyword("def") {
            return self.parse_local_def_expr(allow_binding, terminator, stop_keywords, stop_chars);
        }
        if self.starts_with_keyword("label") {
            return self.parse_label_expr(terminator, stop_keywords, stop_chars);
        }
        let expr = self.parse_expr()?;
        self.skip_ws();
        if allow_binding && self.consume_keyword("as") {
            self.skip_ws();
            let mut patterns = vec![self.parse_binding_pattern()?];
            loop {
                self.skip_ws();
                if !self.consume_str("?//") {
                    break;
                }
                self.skip_ws();
                patterns.push(self.parse_binding_pattern()?);
            }
            if patterns.len() == 1 {
                let Some(pattern) = patterns.pop() else {
                    return Err(AqError::Query(
                        "internal error: missing binding pattern".to_string(),
                    ));
                };
                Ok(Expr::Bind {
                    expr: Box::new(expr),
                    pattern,
                })
            } else {
                Ok(Expr::BindingAlt {
                    expr: Box::new(expr),
                    patterns,
                })
            }
        } else {
            Ok(expr)
        }
    }

    fn parse_local_def_expr(
        &mut self,
        allow_binding: bool,
        terminator: Option<char>,
        stop_keywords: &[&str],
        stop_chars: &[char],
    ) -> Result<Expr, AqError> {
        Ok(Expr::Subquery(Box::new(
            self.parse_query_until_with_stops(
                terminator,
                stop_keywords,
                stop_chars,
                allow_binding,
            )?,
        )))
    }

    fn parse_label_expr(
        &mut self,
        terminator: Option<char>,
        stop_keywords: &[&str],
        stop_chars: &[char],
    ) -> Result<Expr, AqError> {
        self.expect_keyword("label")?;
        self.skip_ws();
        let name = self.parse_variable_name()?;
        self.skip_ws();
        self.expect('|')?;
        self.skip_ws();
        let body =
            self.parse_query_until_with_stops(terminator, stop_keywords, stop_chars, true)?;
        Ok(Expr::Label {
            name,
            body: Box::new(body),
        })
    }

    fn parse_expr(&mut self) -> Result<Expr, AqError> {
        self.parse_assignment()
    }

    fn parse_assignment(&mut self) -> Result<Expr, AqError> {
        let left = self.parse_alt()?;
        self.skip_ws();
        let op = if self.consume_str("|=") {
            Some(AssignOp::Update)
        } else if self.consume_str("//=") {
            Some(AssignOp::UpdateWith(BinaryOp::Alt))
        } else if self.consume_str("+=") {
            Some(AssignOp::UpdateWith(BinaryOp::Add))
        } else if self.consume_str("-=") {
            Some(AssignOp::UpdateWith(BinaryOp::Sub))
        } else if self.consume_str("*=") {
            Some(AssignOp::UpdateWith(BinaryOp::Mul))
        } else if self.consume_str("/=") {
            Some(AssignOp::UpdateWith(BinaryOp::Div))
        } else if self.consume_str("%=") {
            Some(AssignOp::UpdateWith(BinaryOp::Mod))
        } else if self.peek() == Some('=') && self.peek_n(1) != Some('=') {
            self.index += 1;
            Some(AssignOp::Set)
        } else {
            None
        };

        let Some(op) = op else { return Ok(left) };
        self.skip_ws();
        let right = self.parse_assignment()?;
        Ok(Expr::Assign {
            path: Box::new(query_from_expr(left)),
            op,
            value: Box::new(right),
        })
    }

    fn parse_if_expr(&mut self) -> Result<Expr, AqError> {
        self.skip_ws();
        let mut branches = Vec::new();

        loop {
            let condition = self.parse_query_until(None, &["then"])?;
            self.skip_ws();
            self.expect_keyword("then")?;
            self.skip_ws();
            let branch = self.parse_query_until(None, &["elif", "else", "end"])?;
            branches.push((condition, branch));

            self.skip_ws();
            if self.consume_keyword("elif") {
                self.skip_ws();
                continue;
            }
            break;
        }

        self.skip_ws();
        let else_branch = if self.consume_keyword("else") {
            self.skip_ws();
            self.parse_query_until(None, &["end"])?
        } else {
            identity_query()
        };
        self.skip_ws();
        self.expect_keyword("end")?;

        Ok(Expr::If {
            branches,
            else_branch: Box::new(else_branch),
        })
    }

    fn parse_try_expr(&mut self) -> Result<Expr, AqError> {
        let body = self.parse_unary()?;
        self.skip_ws();
        let catch = if self.consume_keyword("catch") {
            self.skip_ws();
            Some(Box::new(self.parse_unary()?))
        } else {
            None
        };
        Ok(Expr::Try {
            body: Box::new(body),
            catch,
        })
    }

    fn parse_reduce_expr(&mut self) -> Result<Expr, AqError> {
        let source = self.parse_query_until_with_stops(None, &["as"], &[], false)?;
        self.skip_ws();
        self.expect_keyword("as")?;
        self.skip_ws();
        let pattern = self.parse_binding_pattern()?;
        self.skip_ws();
        self.expect('(')?;
        self.skip_ws();
        let init = self.parse_query_until_with_stops(Some(')'), &[], &[';'], true)?;
        self.skip_ws();
        self.expect(';')?;
        self.skip_ws();
        let update = self.parse_query_until(Some(')'), &[])?;
        self.skip_ws();
        self.expect(')')?;
        Ok(Expr::Reduce {
            source: Box::new(source),
            pattern,
            init: Box::new(init),
            update: Box::new(update),
        })
    }

    fn parse_foreach_expr(&mut self) -> Result<Expr, AqError> {
        let source = self.parse_query_until_with_stops(None, &["as"], &[], false)?;
        self.skip_ws();
        self.expect_keyword("as")?;
        self.skip_ws();
        let pattern = self.parse_binding_pattern()?;
        self.skip_ws();
        self.expect('(')?;
        self.skip_ws();
        let init = self.parse_query_until_with_stops(Some(')'), &[], &[';'], true)?;
        self.skip_ws();
        self.expect(';')?;
        self.skip_ws();
        let update = self.parse_query_until_with_stops(Some(')'), &[], &[';'], true)?;
        self.skip_ws();
        let extract = if self.consume(';') {
            self.skip_ws();
            self.parse_query_until(Some(')'), &[])?
        } else {
            identity_query()
        };
        self.skip_ws();
        self.expect(')')?;
        Ok(Expr::ForEach {
            source: Box::new(source),
            pattern,
            init: Box::new(init),
            update: Box::new(update),
            extract: Box::new(extract),
        })
    }

    fn parse_alt(&mut self) -> Result<Expr, AqError> {
        let mut expr = self.parse_or()?;
        loop {
            self.skip_ws();
            if self.peek() != Some('/')
                || self.peek_n(1) != Some('/')
                || self.peek_n(2) == Some('=')
            {
                break;
            }
            self.index += 2;
            self.skip_ws();
            let right = self.parse_or()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::Alt,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<Expr, AqError> {
        let mut expr = self.parse_and()?;
        loop {
            self.skip_ws();
            if !self.consume_keyword("or") {
                break;
            }
            self.skip_ws();
            let right = self.parse_and()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, AqError> {
        let mut expr = self.parse_equality()?;
        loop {
            self.skip_ws();
            if !self.consume_keyword("and") {
                break;
            }
            self.skip_ws();
            let right = self.parse_equality()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_equality(&mut self) -> Result<Expr, AqError> {
        let mut expr = self.parse_relational()?;
        loop {
            self.skip_ws();
            let op = if self.consume_str("==") {
                Some(BinaryOp::Eq)
            } else if self.consume_str("!=") {
                Some(BinaryOp::Ne)
            } else {
                None
            };
            let Some(op) = op else { break };
            self.skip_ws();
            let right = self.parse_relational()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_relational(&mut self) -> Result<Expr, AqError> {
        let mut expr = self.parse_additive()?;
        loop {
            self.skip_ws();
            let op = if self.consume_str(">=") {
                Some(BinaryOp::Ge)
            } else if self.consume_str("<=") {
                Some(BinaryOp::Le)
            } else if self.consume('>') {
                Some(BinaryOp::Gt)
            } else if self.consume('<') {
                Some(BinaryOp::Lt)
            } else {
                None
            };
            let Some(op) = op else { break };
            self.skip_ws();
            let right = self.parse_additive()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_additive(&mut self) -> Result<Expr, AqError> {
        let mut expr = self.parse_multiplicative()?;
        loop {
            self.skip_ws();
            let op = if self.peek() == Some('+') && self.peek_n(1) != Some('=') {
                self.index += 1;
                Some(BinaryOp::Add)
            } else if self.peek() == Some('-') && self.peek_n(1) != Some('=') {
                self.index += 1;
                Some(BinaryOp::Sub)
            } else {
                None
            };
            let Some(op) = op else { break };
            self.skip_ws();
            let right = self.parse_multiplicative()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, AqError> {
        let mut expr = self.parse_unary()?;
        loop {
            self.skip_ws();
            let op = if self.peek() == Some('*') && self.peek_n(1) != Some('=') {
                self.index += 1;
                Some(BinaryOp::Mul)
            } else if self.peek() == Some('/')
                && self.peek_n(1) != Some('/')
                && self.peek_n(1) != Some('=')
            {
                self.index += 1;
                Some(BinaryOp::Div)
            } else if self.peek() == Some('%') && self.peek_n(1) != Some('=') {
                self.index += 1;
                Some(BinaryOp::Mod)
            } else {
                None
            };
            let Some(op) = op else { break };
            self.skip_ws();
            let right = self.parse_unary()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, AqError> {
        self.skip_ws();
        if self.consume_keyword("not") {
            self.skip_ws();
            let expr = if self.peek().is_none_or(is_implicit_unary_operand_terminator) {
                Expr::Path(PathExpr {
                    segments: Vec::new(),
                })
            } else {
                self.parse_unary_operand()?
            };
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }
        if self.consume('-') {
            self.skip_ws();
            let expr = self.parse_unary_operand()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            });
        }
        self.parse_primary_expr()
    }

    fn parse_unary_operand(&mut self) -> Result<Expr, AqError> {
        self.skip_ws();
        if self.consume_keyword("if") {
            self.skip_ws();
            let expr = self.parse_if_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("foreach") {
            self.skip_ws();
            let expr = self.parse_foreach_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("reduce") {
            self.skip_ws();
            let expr = self.parse_reduce_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("try") {
            self.skip_ws();
            let expr = self.parse_try_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("break") {
            self.skip_ws();
            return Ok(Expr::Break(self.parse_variable_name()?));
        }
        self.parse_unary()
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, AqError> {
        self.skip_ws();
        if self.consume_keyword("if") {
            self.skip_ws();
            let expr = self.parse_if_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("foreach") {
            self.skip_ws();
            let expr = self.parse_foreach_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("reduce") {
            self.skip_ws();
            let expr = self.parse_reduce_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("try") {
            self.skip_ws();
            let expr = self.parse_try_expr()?;
            return self.parse_control_flow_postfix_expr(expr);
        }
        if self.consume_keyword("break") {
            self.skip_ws();
            return Ok(Expr::Break(self.parse_variable_name()?));
        }
        let expr = match self.peek() {
            Some('@') => self.parse_format_expr(),
            Some('.') if self.peek_n(1) == Some('.') => {
                self.index += 2;
                Ok(Expr::Builtin(BuiltinExpr::Recurse {
                    query: None,
                    condition: None,
                }))
            }
            Some('.') => Ok(Expr::Path(self.parse_path()?)),
            Some('$') => self.parse_variable_expr(),
            Some('[') => self.parse_array(),
            Some('{') => self.parse_object(),
            Some('(') => self.parse_parenthesized_expr(),
            Some('"') => self.parse_string_expr(),
            Some(value) if value.is_ascii_digit() => self.parse_number(),
            Some(value) if is_identifier_start(value) => self.parse_identifier_expr(),
            Some('%') => Err(AqError::InvalidExpression(
                "syntax error, unexpected `%`, expecting end of file".to_string(),
            )),
            Some('}') => Err(AqError::InvalidExpression(
                "syntax error, unexpected INVALID_CHARACTER, expecting end of file".to_string(),
            )),
            Some(value) => Err(AqError::InvalidExpression(format!(
                "unexpected token `{value}` in expression"
            ))),
            None => Err(AqError::InvalidExpression(
                "unexpected end of expression".to_string(),
            )),
        }?;
        self.parse_postfix_expr(expr)
    }

    fn parse_format_expr(&mut self) -> Result<Expr, AqError> {
        self.expect('@')?;
        let Some(value) = self.peek() else {
            return Err(AqError::InvalidExpression(
                "unexpected end of format expression".to_string(),
            ));
        };
        if !is_identifier_start(value) {
            return Err(AqError::InvalidExpression(format!(
                "unexpected token `{value}` in format expression"
            )));
        }
        let name = self.parse_identifier_key()?;
        let operator = match name.as_str() {
            "json" => FormatOperator::Json,
            "text" => FormatOperator::Text,
            "csv" => FormatOperator::Csv,
            "tsv" => FormatOperator::Tsv,
            "html" => FormatOperator::Html,
            "uri" => FormatOperator::Uri,
            "urid" => FormatOperator::Urid,
            "sh" => FormatOperator::Sh,
            "base64" => FormatOperator::Base64,
            "base64d" => FormatOperator::Base64d,
            _ => {
                return Err(AqError::InvalidExpression(format!(
                    "unknown format expression `@{name}`"
                )))
            }
        };
        self.skip_ws();
        if self.peek() == Some('"') {
            let parts = self.parse_format_string_parts()?;
            Ok(Expr::FormatString { operator, parts })
        } else {
            Ok(Expr::Builtin(BuiltinExpr::Format(operator)))
        }
    }

    fn parse_string_expr(&mut self) -> Result<Expr, AqError> {
        let parts = self.parse_interpolated_string_parts("string literal")?;
        if parts.is_empty() {
            return Ok(Expr::Literal(Value::String(String::new())));
        }
        if parts.len() == 1 {
            if let FormatStringPart::Literal(value) = &parts[0] {
                return Ok(Expr::Literal(Value::String(value.clone())));
            }
        }
        Ok(Expr::FormatString {
            operator: FormatOperator::Text,
            parts,
        })
    }

    fn parse_format_string_parts(&mut self) -> Result<Vec<FormatStringPart>, AqError> {
        self.parse_interpolated_string_parts("format string")
    }

    fn parse_interpolated_string_parts(
        &mut self,
        description: &str,
    ) -> Result<Vec<FormatStringPart>, AqError> {
        self.expect('"')?;
        let mut parts = Vec::new();
        let mut literal = String::from("\"");

        while let Some(value) = self.peek() {
            self.index += 1;
            match value {
                '"' => {
                    self.push_interpolated_literal_part(&mut parts, &literal, description)?;
                    return Ok(parts);
                }
                '\\' => {
                    let Some(next) = self.peek() else {
                        break;
                    };
                    if next == '(' {
                        self.index += 1;
                        self.push_interpolated_literal_part(&mut parts, &literal, description)?;
                        literal.clear();
                        literal.push('"');
                        let query = self.parse_query_until(Some(')'), &[])?;
                        self.expect(')')?;
                        parts.push(FormatStringPart::Query(Box::new(query)));
                    } else {
                        literal.push('\\');
                        literal.push(next);
                        self.index += 1;
                    }
                }
                other => literal.push(other),
            }
        }

        Err(AqError::InvalidExpression(format!(
            "unterminated quoted {description}"
        )))
    }

    fn push_interpolated_literal_part(
        &self,
        parts: &mut Vec<FormatStringPart>,
        literal: &str,
        description: &str,
    ) -> Result<(), AqError> {
        let value = serde_json::from_str::<String>(&format!("{literal}\"")).map_err(|error| {
            AqError::InvalidExpression(format!(
                "invalid quoted {description} in expression `{}`: {error}",
                self.input
            ))
        })?;
        if !value.is_empty() {
            parts.push(FormatStringPart::Literal(value));
        }
        Ok(())
    }

    fn parse_postfix_expr(&mut self, expr: Expr) -> Result<Expr, AqError> {
        let mut segments = Vec::new();
        loop {
            match self.peek() {
                Some('.') => {
                    let Some(next) = self.peek_n(1) else { break };
                    if next == '"' || next == '[' || is_identifier_start(next) {
                        self.index += 1;
                        if next == '"' {
                            segments.push(self.parse_quoted_field()?);
                        } else if next == '[' {
                            segments.push(self.parse_bracket_segment()?);
                        } else {
                            segments.push(self.parse_field()?);
                        }
                        continue;
                    }
                    break;
                }
                Some('[') => {
                    segments.push(self.parse_bracket_segment()?);
                }
                _ => break,
            }
        }

        let expr = if segments.is_empty() {
            expr
        } else {
            Expr::Access {
                base: Box::new(expr),
                segments,
            }
        };
        if self.consume('?') {
            Ok(Expr::Try {
                body: Box::new(expr),
                catch: None,
            })
        } else {
            Ok(expr)
        }
    }

    fn parse_control_flow_postfix_expr(&mut self, expr: Expr) -> Result<Expr, AqError> {
        self.skip_ws();
        self.parse_postfix_expr(expr)
    }

    fn parse_variable_expr(&mut self) -> Result<Expr, AqError> {
        let name = self.parse_variable_name()?;
        Ok(Expr::Variable(name))
    }

    fn parse_variable_name(&mut self) -> Result<String, AqError> {
        self.expect('$')?;
        let start = self.index;
        self.expect_identifier_segment("variable name")?;
        while self.consume_str("::") {
            self.expect_identifier_segment("variable name")?;
        }
        Ok(self.chars[start..self.index].iter().collect())
    }

    fn expect_identifier_segment(&mut self, context: &str) -> Result<(), AqError> {
        let Some(value) = self.peek() else {
            return Err(AqError::InvalidExpression(format!(
                "unexpected end of {context}"
            )));
        };
        if !is_identifier_start(value) {
            return Err(AqError::InvalidExpression(format!(
                "unexpected token `{value}` in {context}"
            )));
        }
        while let Some(value) = self.peek() {
            if is_identifier_continue(value) {
                self.index += 1;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn parse_binding_pattern(&mut self) -> Result<BindingPattern, AqError> {
        self.skip_ws();
        match self.peek() {
            Some('$') => self.parse_variable_name().map(BindingPattern::Variable),
            Some('[') => self.parse_array_binding_pattern(),
            Some('{') => self.parse_object_binding_pattern(),
            Some(value) => Err(AqError::InvalidExpression(format!(
                "unexpected token `{value}` in binding pattern"
            ))),
            None => Err(AqError::InvalidExpression(
                "unexpected end of binding pattern".to_string(),
            )),
        }
    }

    fn parse_array_binding_pattern(&mut self) -> Result<BindingPattern, AqError> {
        self.expect('[')?;
        self.skip_ws();
        let mut patterns = Vec::new();
        if self.consume(']') {
            return Err(AqError::InvalidExpression(
                "syntax error, unexpected `]`, expecting binding or `or`".to_string(),
            ));
        }

        loop {
            patterns.push(self.parse_binding_pattern()?);
            self.skip_ws();
            if self.consume(']') {
                break;
            }
            self.expect(',')?;
            self.skip_ws();
        }

        Ok(BindingPattern::Array(patterns))
    }

    fn parse_object_binding_pattern(&mut self) -> Result<BindingPattern, AqError> {
        self.expect('{')?;
        self.skip_ws();
        let mut fields = Vec::new();
        if self.consume('}') {
            return Err(AqError::InvalidExpression(
                "syntax error, unexpected `}`".to_string(),
            ));
        }

        loop {
            fields.push(self.parse_object_binding_field()?);
            self.skip_ws();
            if self.consume('}') {
                break;
            }
            self.expect(',')?;
            self.skip_ws();
        }

        Ok(BindingPattern::Object(fields))
    }

    fn parse_object_binding_field(&mut self) -> Result<ObjectBindingField, AqError> {
        match self.peek() {
            Some('$') => {
                let name = self.parse_variable_name()?;
                self.skip_ws();
                if self.consume(':') {
                    self.skip_ws();
                    let pattern = self.parse_binding_pattern()?;
                    Ok(ObjectBindingField {
                        key: ObjectKey::Static(name.clone()),
                        bind_name: Some(name),
                        pattern,
                    })
                } else {
                    Ok(ObjectBindingField {
                        key: ObjectKey::Static(name.clone()),
                        bind_name: None,
                        pattern: BindingPattern::Variable(name),
                    })
                }
            }
            Some('"') | Some('(') => {
                let key = self.parse_object_key()?;
                self.skip_ws();
                self.expect(':')?;
                self.skip_ws();
                let pattern = self.parse_binding_pattern()?;
                Ok(ObjectBindingField {
                    key,
                    bind_name: None,
                    pattern,
                })
            }
            Some(value) if is_identifier_start(value) => {
                let key = ObjectKey::Static(self.parse_identifier_key()?);
                self.skip_ws();
                self.expect(':')?;
                self.skip_ws();
                let pattern = self.parse_binding_pattern()?;
                Ok(ObjectBindingField {
                    key,
                    bind_name: None,
                    pattern,
                })
            }
            Some(value) => Err(AqError::InvalidExpression(format!(
                "unexpected token `{value}` in object binding pattern"
            ))),
            None => Err(AqError::InvalidExpression(
                "unexpected end of object binding pattern".to_string(),
            )),
        }
    }

    fn parse_parenthesized_expr(&mut self) -> Result<Expr, AqError> {
        self.expect('(')?;
        self.skip_ws();
        let query = self.parse_query_until(Some(')'), &[])?;
        self.skip_ws();
        self.expect(')')?;
        Ok(Expr::Subquery(Box::new(query)))
    }

    fn parse_path(&mut self) -> Result<PathExpr, AqError> {
        self.expect('.')?;
        if self.consume('?') {
            return Ok(PathExpr {
                segments: Vec::new(),
            });
        }
        let mut segments = Vec::new();

        loop {
            match self.peek() {
                None | Some(',') | Some(';') | Some('|') | Some(']') | Some('}') | Some(')')
                | Some(':') | Some('=') | Some('!') | Some('<') | Some('>') | Some('+')
                | Some('-') | Some('*') | Some('/') | Some('%') => break,
                Some(value) if value.is_whitespace() => break,
                Some('"') => segments.push(self.parse_quoted_field()?),
                Some('[') => segments.push(self.parse_bracket_segment()?),
                Some('.') => {
                    self.index += 1;
                    continue;
                }
                Some(value) if is_identifier_start(value) => segments.push(self.parse_field()?),
                Some(value) => {
                    return Err(AqError::InvalidExpression(format!(
                        "unexpected token `{value}` in path expression"
                    )));
                }
            }
        }

        Ok(PathExpr { segments })
    }

    fn parse_array(&mut self) -> Result<Expr, AqError> {
        self.expect('[')?;
        self.skip_ws();
        if self.consume(']') {
            return Ok(Expr::Array(Vec::new()));
        }
        let query = self.parse_query_until(Some(']'), &[])?;
        self.skip_ws();
        self.expect(']')?;
        Ok(Expr::Array(vec![Expr::Subquery(Box::new(query))]))
    }

    fn parse_object(&mut self) -> Result<Expr, AqError> {
        self.expect('{')?;
        self.skip_ws();
        let mut fields = Vec::new();
        if self.consume('}') {
            return Ok(Expr::Object(fields));
        }

        loop {
            fields.push(self.parse_object_field()?);
            self.skip_ws();
            if self.consume('}') {
                break;
            }
            self.expect(',')?;
            self.skip_ws();
        }

        Ok(Expr::Object(fields))
    }

    fn parse_object_field(&mut self) -> Result<(ObjectKey, Expr), AqError> {
        match self.peek() {
            Some('$') => {
                let name = self.parse_variable_name()?;
                self.skip_ws();
                if self.consume(':') {
                    self.skip_ws();
                    let value = self.parse_object_value_expr()?;
                    Ok((ObjectKey::Dynamic(Box::new(Expr::Variable(name))), value))
                } else {
                    Ok((ObjectKey::Static(name.clone()), Expr::Variable(name)))
                }
            }
            Some(value) if is_identifier_start(value) => {
                let key = self.parse_identifier_key()?;
                self.skip_ws();
                if self.consume(':') {
                    self.skip_ws();
                    let value = self.parse_object_value_expr()?;
                    Ok((ObjectKey::Static(key), value))
                } else {
                    let value = Expr::Path(PathExpr {
                        segments: vec![Segment::Field {
                            name: key.clone(),
                            optional: false,
                        }],
                    });
                    Ok((ObjectKey::Static(key), value))
                }
            }
            Some('"') => {
                let key = self.parse_string_expr()?;
                self.skip_ws();
                if self.consume(':') {
                    self.skip_ws();
                    let value = self.parse_object_value_expr()?;
                    Ok((string_expr_object_key(key), value))
                } else {
                    let value = string_expr_lookup_value(&key);
                    Ok((string_expr_object_key(key), value))
                }
            }
            _ => {
                let key = self.parse_object_key()?;
                self.skip_ws();
                self.expect(':')?;
                self.skip_ws();
                let value = self.parse_object_value_expr()?;
                Ok((key, value))
            }
        }
    }

    fn parse_object_value_expr(&mut self) -> Result<Expr, AqError> {
        Ok(Expr::Subquery(Box::new(
            self.parse_query_until_with_stops(None, &[], &[',', '}'], true)?,
        )))
    }

    fn parse_object_key(&mut self) -> Result<ObjectKey, AqError> {
        match self.peek() {
            Some('"') => self.parse_quoted_string().map(ObjectKey::Static),
            Some('(') => {
                self.expect('(')?;
                self.skip_ws();
                let query = self.parse_query_until(Some(')'), &[])?;
                self.skip_ws();
                self.expect(')')?;
                Ok(ObjectKey::Dynamic(Box::new(Expr::Subquery(Box::new(
                    query,
                )))))
            }
            Some(value) if is_identifier_start(value) => {
                self.parse_identifier_key().map(ObjectKey::Static)
            }
            Some(value)
                if value.is_ascii_digit() || matches!(value, '.' | '+' | '-' | '[' | '{') =>
            {
                Err(AqError::InvalidExpression(
                    "may need parentheses around object key expression".to_string(),
                ))
            }
            Some(value) => Err(AqError::InvalidExpression(format!(
                "unexpected token `{value}` in object key"
            ))),
            None => Err(AqError::InvalidExpression(
                "syntax error, unexpected end of file".to_string(),
            )),
        }
    }

    fn parse_identifier_key(&mut self) -> Result<String, AqError> {
        let start = self.index;
        while let Some(value) = self.peek() {
            if is_identifier_continue(value) {
                self.index += 1;
            } else {
                break;
            }
        }
        let key: String = self.chars[start..self.index].iter().collect();
        if key.is_empty() {
            Err(AqError::InvalidExpression(
                "empty identifier is not supported".to_string(),
            ))
        } else {
            Ok(key)
        }
    }

    fn parse_number(&mut self) -> Result<Expr, AqError> {
        let start = self.index;
        while matches!(self.peek(), Some(value) if value.is_ascii_digit()) {
            self.index += 1;
        }
        let mut is_float = false;
        if self.peek() == Some('.') {
            is_float = true;
            self.index += 1;
            while matches!(self.peek(), Some(value) if value.is_ascii_digit()) {
                self.index += 1;
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            is_float = true;
            self.index += 1;
            if matches!(self.peek(), Some('+' | '-')) {
                self.index += 1;
            }
            while matches!(self.peek(), Some(value) if value.is_ascii_digit()) {
                self.index += 1;
            }
        }
        let raw: String = self.chars[start..self.index].iter().collect();
        let value = if is_float {
            Value::Decimal(DecimalValue::parse(&raw).map_err(|error| {
                AqError::InvalidExpression(format!("invalid float literal `{raw}`: {error}"))
            })?)
        } else if let Ok(value) = raw.parse::<i64>() {
            Value::Integer(value)
        } else {
            Value::Decimal(DecimalValue::parse(&raw).map_err(|error| {
                AqError::InvalidExpression(format!("invalid integer literal `{raw}`: {error}"))
            })?)
        };
        Ok(Expr::Literal(value))
    }

    fn parse_identifier_expr(&mut self) -> Result<Expr, AqError> {
        let start = self.index;
        while let Some(value) = self.peek() {
            if is_identifier_continue(value) {
                self.index += 1;
            } else {
                break;
            }
        }
        while self.consume_str("::") {
            if !self.peek().is_some_and(is_identifier_start) {
                return Err(AqError::InvalidExpression(
                    "expected identifier after `::` in expression".to_string(),
                ));
            }
            while let Some(value) = self.peek() {
                if is_identifier_continue(value) {
                    self.index += 1;
                } else {
                    break;
                }
            }
        }
        let raw: String = self.chars[start..self.index].iter().collect();
        let saved_index = self.index;
        let args = self.parse_optional_function_call_queries()?;
        let arity = args.len();
        if self.parameter_names.contains(&raw) && arity == 0 {
            return Ok(Expr::FunctionCall { name: raw, args });
        }
        if self.known_functions.contains_key(&FunctionKey {
            name: raw.clone(),
            arity,
        }) {
            return Ok(Expr::FunctionCall { name: raw, args });
        }
        self.index = saved_index;
        match raw.as_str() {
            "null" => Ok(Expr::Literal(Value::Null)),
            "true" => Ok(Expr::Literal(Value::Bool(true))),
            "false" => Ok(Expr::Literal(Value::Bool(false))),
            "have_decnum" => Ok(Expr::Literal(Value::Bool(true))),
            "length" => Ok(Expr::Builtin(BuiltinExpr::Length)),
            "utf8bytelength" => Ok(Expr::Builtin(BuiltinExpr::Utf8ByteLength)),
            "keys" => Ok(Expr::Builtin(BuiltinExpr::Keys)),
            "keys_unsorted" => Ok(Expr::Builtin(BuiltinExpr::KeysUnsorted)),
            "type" => Ok(Expr::Builtin(BuiltinExpr::Type)),
            "builtins" => Ok(Expr::Builtin(BuiltinExpr::Builtins)),
            "modulemeta" => Ok(Expr::Builtin(BuiltinExpr::ModuleMeta(self.options.clone()))),
            "debug" => Ok(Expr::Builtin(BuiltinExpr::Debug(
                self.parse_optional_call_argument()?.map(Box::new),
            ))),
            "del" => match self.parse_required_call_argument()? {
                Expr::Subquery(query) => Ok(Expr::Builtin(BuiltinExpr::Del(query))),
                _ => Err(AqError::InvalidExpression(
                    "internal error: del call argument did not parse as query".to_string(),
                )),
            },
            "error" => Ok(Expr::Builtin(BuiltinExpr::Error(
                self.parse_optional_call_argument()?.map(Box::new),
            ))),
            "env" => Ok(Expr::Builtin(BuiltinExpr::Env)),
            "add" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => Ok(Expr::Builtin(BuiltinExpr::AddQuery(query))),
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: add call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::Add)),
            },
            "avg" => Ok(Expr::Builtin(BuiltinExpr::Avg)),
            "median" => Ok(Expr::Builtin(BuiltinExpr::Median)),
            "stddev" => Ok(Expr::Builtin(BuiltinExpr::Stddev)),
            "percentile" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Percentile(Box::new(expr))))
            }
            "histogram" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Histogram(Box::new(expr))))
            }
            "values" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Values))),
            "nulls" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Nulls))),
            "booleans" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Booleans))),
            "numbers" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Numbers))),
            "strings" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Strings))),
            "arrays" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Arrays))),
            "objects" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Objects))),
            "iterables" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(
                TypeFilter::Iterables,
            ))),
            "scalars" => Ok(Expr::Builtin(BuiltinExpr::TypeFilter(TypeFilter::Scalars))),
            "empty" => Ok(Expr::Builtin(BuiltinExpr::Empty)),
            "first" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => Ok(Expr::Builtin(BuiltinExpr::FirstQuery(query))),
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: first call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::First)),
            },
            "reverse" => Ok(Expr::Builtin(BuiltinExpr::Reverse)),
            "range" => {
                let args = self.parse_semicolon_call_queries()?;
                if !(1..=3).contains(&args.len()) {
                    return Err(AqError::InvalidExpression(
                        "range expects 1 to 3 arguments".to_string(),
                    ));
                }
                Ok(Expr::Builtin(BuiltinExpr::Range(args)))
            }
            "combinations" => Ok(Expr::Builtin(BuiltinExpr::Combinations(
                self.parse_optional_call_argument()?.map(Box::new),
            ))),
            "take" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Take(Box::new(expr))))
            }
            "skip" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => {
                        let count = expr_from_single_stage_query(
                            "skip",
                            args.pop().ok_or_else(|| {
                                AqError::InvalidExpression(
                                    "skip expects at least one argument".to_string(),
                                )
                            })?,
                        )?;
                        Ok(Expr::Builtin(BuiltinExpr::Skip(Box::new(count))))
                    }
                    2 => {
                        let expr = args.pop().ok_or_else(|| {
                            AqError::InvalidExpression(
                                "skip expects exactly 2 arguments".to_string(),
                            )
                        })?;
                        let count = args.pop().ok_or_else(|| {
                            AqError::InvalidExpression(
                                "skip expects exactly 2 arguments".to_string(),
                            )
                        })?;
                        Ok(Expr::Builtin(BuiltinExpr::SkipQuery {
                            count: Box::new(count),
                            expr: Box::new(expr),
                        }))
                    }
                    _ => Err(AqError::InvalidExpression(
                        "skip expects 1 or 2 arguments".to_string(),
                    )),
                }
            }
            "bsearch" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Bsearch(Box::new(expr))))
            }
            "recurse" => {
                let args = self.parse_optional_function_call_queries()?;
                match args.len() {
                    0 => Ok(Expr::Builtin(BuiltinExpr::Recurse {
                        query: None,
                        condition: None,
                    })),
                    1 => Ok(Expr::Builtin(BuiltinExpr::Recurse {
                        query: Some(Box::new(args.into_iter().next().ok_or_else(|| {
                            AqError::InvalidExpression(
                                "internal error: recurse should have a query".to_string(),
                            )
                        })?)),
                        condition: None,
                    })),
                    2 => {
                        let mut args = args;
                        let condition = Box::new(args.pop().ok_or_else(|| {
                            AqError::InvalidExpression(
                                "internal error: recurse should have a condition query".to_string(),
                            )
                        })?);
                        let query = Box::new(args.pop().ok_or_else(|| {
                            AqError::InvalidExpression(
                                "internal error: recurse should have a query".to_string(),
                            )
                        })?);
                        Ok(Expr::Builtin(BuiltinExpr::Recurse {
                            query: Some(query),
                            condition: Some(condition),
                        }))
                    }
                    _ => Err(AqError::InvalidExpression(
                        "recurse expects 0 to 2 arguments".to_string(),
                    )),
                }
            }
            "repeat" => match self.parse_required_call_argument()? {
                Expr::Subquery(query) => Ok(Expr::Builtin(BuiltinExpr::Repeat(query))),
                _ => Err(AqError::InvalidExpression(
                    "internal error: repeat call argument did not parse as query".to_string(),
                )),
            },
            "walk" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Walk(Box::new(expr))))
            }
            "transpose" => Ok(Expr::Builtin(BuiltinExpr::Transpose)),
            "while" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 2 {
                    return Err(AqError::InvalidExpression(
                        "while expects exactly 2 arguments".to_string(),
                    ));
                }
                let update = Box::new(args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: while should have an update query".to_string(),
                    )
                })?);
                let condition = Box::new(args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: while should have a condition query".to_string(),
                    )
                })?);
                Ok(Expr::Builtin(BuiltinExpr::While { condition, update }))
            }
            "until" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 2 {
                    return Err(AqError::InvalidExpression(
                        "until expects exactly 2 arguments".to_string(),
                    ));
                }
                let next = Box::new(args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: until should have a next query".to_string(),
                    )
                })?);
                let condition = Box::new(args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: until should have a condition query".to_string(),
                    )
                })?);
                Ok(Expr::Builtin(BuiltinExpr::Until { condition, next }))
            }
            "sort" => Ok(Expr::Builtin(BuiltinExpr::Sort)),
            "min" => Ok(Expr::Builtin(BuiltinExpr::Min)),
            "max" => Ok(Expr::Builtin(BuiltinExpr::Max)),
            "unique" => Ok(Expr::Builtin(BuiltinExpr::Unique)),
            "flatten" => match self.parse_optional_call_argument()? {
                Some(expr) => Ok(Expr::Builtin(BuiltinExpr::FlattenDepth(Box::new(expr)))),
                None => Ok(Expr::Builtin(BuiltinExpr::Flatten)),
            },
            "floor" => Ok(Expr::Builtin(BuiltinExpr::Floor)),
            "ceil" => Ok(Expr::Builtin(BuiltinExpr::Ceil)),
            "round" => Ok(Expr::Builtin(BuiltinExpr::Round)),
            "abs" => Ok(Expr::Builtin(BuiltinExpr::Abs)),
            "fabs" => Ok(Expr::Builtin(BuiltinExpr::Fabs)),
            "sqrt" => Ok(Expr::Builtin(BuiltinExpr::Sqrt)),
            "log" => Ok(Expr::Builtin(BuiltinExpr::Log)),
            "log2" => Ok(Expr::Builtin(BuiltinExpr::Log2)),
            "log10" => Ok(Expr::Builtin(BuiltinExpr::Log10)),
            "exp" => Ok(Expr::Builtin(BuiltinExpr::Exp)),
            "exp2" => Ok(Expr::Builtin(BuiltinExpr::Exp2)),
            "sin" => Ok(Expr::Builtin(BuiltinExpr::Sin)),
            "cos" => Ok(Expr::Builtin(BuiltinExpr::Cos)),
            "tan" => Ok(Expr::Builtin(BuiltinExpr::Tan)),
            "asin" => Ok(Expr::Builtin(BuiltinExpr::Asin)),
            "acos" => Ok(Expr::Builtin(BuiltinExpr::Acos)),
            "atan" => Ok(Expr::Builtin(BuiltinExpr::Atan)),
            "pow" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 2 {
                    return Err(AqError::InvalidExpression(
                        "pow expects exactly 2 arguments".to_string(),
                    ));
                }
                let exponent = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression("pow expects exactly 2 arguments".to_string())
                })?;
                let base = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression("pow expects exactly 2 arguments".to_string())
                })?;
                Ok(Expr::Builtin(BuiltinExpr::Pow {
                    base: Box::new(base),
                    exponent: Box::new(exponent),
                }))
            }
            "now" => Ok(Expr::Builtin(BuiltinExpr::Now)),
            "input" => Ok(Expr::Builtin(BuiltinExpr::Input)),
            "inputs" => Ok(Expr::Builtin(BuiltinExpr::Inputs)),
            "todate" => Ok(Expr::Builtin(BuiltinExpr::ToDate)),
            "fromdate" => Ok(Expr::Builtin(BuiltinExpr::FromDate)),
            "to_datetime" => Ok(Expr::Builtin(BuiltinExpr::ToDateTime)),
            "gmtime" => Ok(Expr::Builtin(BuiltinExpr::GmTime)),
            "mktime" => Ok(Expr::Builtin(BuiltinExpr::MkTime)),
            "strftime" => Ok(Expr::Builtin(BuiltinExpr::StrFTime(Box::new(
                self.parse_required_call_argument()?,
            )))),
            "strflocaltime" => Ok(Expr::Builtin(BuiltinExpr::StrFLocalTime(Box::new(
                self.parse_required_call_argument()?,
            )))),
            "strptime" => Ok(Expr::Builtin(BuiltinExpr::StrPTime(Box::new(
                self.parse_required_call_argument()?,
            )))),
            "last" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => Ok(Expr::Builtin(BuiltinExpr::LastQuery(query))),
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: last call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::Last)),
            },
            "limit" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 2 {
                    return Err(AqError::InvalidExpression(
                        "limit expects exactly 2 arguments".to_string(),
                    ));
                }
                let expr = args.pop().expect("limit should have expr");
                let count = args.pop().expect("limit should have count");
                Ok(Expr::Builtin(BuiltinExpr::Limit {
                    count: Box::new(count),
                    expr: Box::new(expr),
                }))
            }
            "nth" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Nth {
                        indexes: Box::new(args.pop().expect("nth should have indexes")),
                        expr: Box::new(identity_query()),
                    })),
                    2 => {
                        let expr = args.pop().expect("nth should have expr");
                        let indexes = args.pop().expect("nth should have indexes");
                        Ok(Expr::Builtin(BuiltinExpr::Nth {
                            indexes: Box::new(indexes),
                            expr: Box::new(expr),
                        }))
                    }
                    _ => Err(AqError::InvalidExpression(
                        "nth expects 1 or 2 arguments".to_string(),
                    )),
                }
            }
            "tostring" => Ok(Expr::Builtin(BuiltinExpr::ToString)),
            "tonumber" => Ok(Expr::Builtin(BuiltinExpr::ToNumber)),
            "to_number" => Ok(Expr::Builtin(BuiltinExpr::ToNumber)),
            "to_bool" => Ok(Expr::Builtin(BuiltinExpr::ToBool)),
            "toboolean" => Ok(Expr::Builtin(BuiltinExpr::ToBoolean)),
            "infinite" => Ok(Expr::Builtin(BuiltinExpr::Infinite)),
            "nan" => Ok(Expr::Builtin(BuiltinExpr::Nan)),
            "isnan" => Ok(Expr::Builtin(BuiltinExpr::IsNan)),
            "test" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Test {
                        regex: Box::new(args.remove(0)),
                        flags: None,
                    })),
                    2 => Ok(Expr::Builtin(BuiltinExpr::Test {
                        regex: Box::new(args.remove(0)),
                        flags: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "test expects one or two arguments".to_string(),
                    )),
                }
            }
            "capture" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Capture {
                        regex: Box::new(args.remove(0)),
                        flags: None,
                    })),
                    2 => Ok(Expr::Builtin(BuiltinExpr::Capture {
                        regex: Box::new(args.remove(0)),
                        flags: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "capture expects one or two arguments".to_string(),
                    )),
                }
            }
            "match" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Match {
                        regex: Box::new(args.remove(0)),
                        flags: None,
                    })),
                    2 => Ok(Expr::Builtin(BuiltinExpr::Match {
                        regex: Box::new(args.remove(0)),
                        flags: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "match expects one or two arguments".to_string(),
                    )),
                }
            }
            "splits" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Splits {
                        pattern: Box::new(args.remove(0)),
                        flags: None,
                    })),
                    2 => Ok(Expr::Builtin(BuiltinExpr::Splits {
                        pattern: Box::new(args.remove(0)),
                        flags: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "splits expects one or two arguments".to_string(),
                    )),
                }
            }
            "scan" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Scan {
                        regex: Box::new(args.remove(0)),
                        flags: None,
                    })),
                    2 => Ok(Expr::Builtin(BuiltinExpr::Scan {
                        regex: Box::new(args.remove(0)),
                        flags: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "scan expects one or two arguments".to_string(),
                    )),
                }
            }
            "ascii_downcase" => Ok(Expr::Builtin(BuiltinExpr::AsciiDowncase)),
            "ascii_upcase" => Ok(Expr::Builtin(BuiltinExpr::AsciiUpcase)),
            "trim" => Ok(Expr::Builtin(BuiltinExpr::Trim)),
            "ltrim" => Ok(Expr::Builtin(BuiltinExpr::Ltrim)),
            "rtrim" => Ok(Expr::Builtin(BuiltinExpr::Rtrim)),
            "to_entries" => Ok(Expr::Builtin(BuiltinExpr::ToEntries)),
            "from_entries" => Ok(Expr::Builtin(BuiltinExpr::FromEntries)),
            "tojson" => Ok(Expr::Builtin(BuiltinExpr::ToJson)),
            "fromjson" => Ok(Expr::Builtin(BuiltinExpr::FromJson)),
            "explode" => Ok(Expr::Builtin(BuiltinExpr::Explode)),
            "implode" => Ok(Expr::Builtin(BuiltinExpr::Implode)),
            "columns" => Ok(Expr::Builtin(BuiltinExpr::Columns)),
            "inside" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Inside(Box::new(expr))))
            }
            "group_by" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::GroupBy(Box::new(expr))))
            }
            "unique_by" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::UniqueBy(Box::new(expr))))
            }
            "uniq_by" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::UniqueBy(Box::new(expr))))
            }
            "sort_by_desc" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::SortByDesc(Box::new(expr))))
            }
            "count_by" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::CountBy(Box::new(expr))))
            }
            "yaml_tag" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => Ok(Expr::Builtin(BuiltinExpr::YamlTag(Some(query)))),
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: yaml_tag call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::YamlTag(None))),
            },
            "xml_attr" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => Ok(Expr::Builtin(BuiltinExpr::XmlAttr(Some(query)))),
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: xml_attr call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::XmlAttr(None))),
            },
            "csv_header" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => {
                    Ok(Expr::Builtin(BuiltinExpr::CsvHeader(Some(query))))
                }
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: csv_header call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::CsvHeader(None))),
            },
            "merge" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Merge {
                        value: Box::new(args.remove(0)),
                        deep: None,
                    })),
                    2 => Ok(Expr::Builtin(BuiltinExpr::Merge {
                        value: Box::new(args.remove(0)),
                        deep: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "merge expects one or two arguments".to_string(),
                    )),
                }
            }
            "merge_all" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => {
                    Ok(Expr::Builtin(BuiltinExpr::MergeAll(Some(query))))
                }
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: merge_all call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::MergeAll(None))),
            },
            "sort_keys" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => {
                    Ok(Expr::Builtin(BuiltinExpr::SortKeys(Some(query))))
                }
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: sort_keys call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::SortKeys(None))),
            },
            "drop_nulls" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => {
                    Ok(Expr::Builtin(BuiltinExpr::DropNulls(Some(query))))
                }
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: drop_nulls call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::DropNulls(None))),
            },
            "pick" => match self.parse_required_call_argument()? {
                Expr::Subquery(query) => Ok(Expr::Builtin(BuiltinExpr::Pick(query))),
                _ => Err(AqError::InvalidExpression(
                    "internal error: pick call argument did not parse as query".to_string(),
                )),
            },
            "omit" => match self.parse_required_call_argument()? {
                Expr::Subquery(query) => Ok(Expr::Builtin(BuiltinExpr::Omit(query))),
                _ => Err(AqError::InvalidExpression(
                    "internal error: omit call argument did not parse as query".to_string(),
                )),
            },
            "rename" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 2 {
                    return Err(AqError::InvalidExpression(
                        "rename expects exactly 2 arguments".to_string(),
                    ));
                }
                let name = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: rename should have a target name query".to_string(),
                    )
                })?;
                let path = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: rename should have a path query".to_string(),
                    )
                })?;
                Ok(Expr::Builtin(BuiltinExpr::Rename {
                    path: Box::new(path),
                    name: Box::new(name),
                }))
            }
            "min_by" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::MinBy(Box::new(expr))))
            }
            "max_by" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::MaxBy(Box::new(expr))))
            }
            "getpath" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 1 {
                    return Err(AqError::InvalidExpression(
                        "getpath expects exactly 1 argument".to_string(),
                    ));
                }
                let query = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: getpath should have a path query".to_string(),
                    )
                })?;
                Ok(Expr::Builtin(BuiltinExpr::GetPath(Box::new(query))))
            }
            "setpath" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 2 {
                    return Err(AqError::InvalidExpression(
                        "setpath expects exactly 2 arguments".to_string(),
                    ));
                }
                let value = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: setpath should have a value query".to_string(),
                    )
                })?;
                let path = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: setpath should have a path query".to_string(),
                    )
                })?;
                Ok(Expr::Builtin(BuiltinExpr::SetPath {
                    path: Box::new(path),
                    value: Box::new(value),
                }))
            }
            "delpaths" => {
                let mut args = self.parse_semicolon_call_queries()?;
                if args.len() != 1 {
                    return Err(AqError::InvalidExpression(
                        "delpaths expects exactly 1 argument".to_string(),
                    ));
                }
                let query = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: delpaths should have a paths query".to_string(),
                    )
                })?;
                Ok(Expr::Builtin(BuiltinExpr::DelPaths(Box::new(query))))
            }
            "path" => match self.parse_required_call_argument()? {
                Expr::Subquery(query) => Ok(Expr::Builtin(BuiltinExpr::Path(query))),
                _ => Err(AqError::InvalidExpression(
                    "internal error: path call argument did not parse as query".to_string(),
                )),
            },
            "paths" => match self.parse_optional_call_argument()? {
                Some(Expr::Subquery(query)) => Ok(Expr::Builtin(BuiltinExpr::Paths(Some(query)))),
                Some(_) => Err(AqError::InvalidExpression(
                    "internal error: paths call argument did not parse as query".to_string(),
                )),
                None => Ok(Expr::Builtin(BuiltinExpr::Paths(None))),
            },
            "truncate_stream" => match self.parse_required_call_argument()? {
                Expr::Subquery(query) => Ok(Expr::Builtin(BuiltinExpr::TruncateStream(query))),
                _ => Err(AqError::InvalidExpression(
                    "internal error: truncate_stream call argument did not parse as query"
                        .to_string(),
                )),
            },
            "fromstream" => match self.parse_required_call_argument()? {
                Expr::Subquery(query) => Ok(Expr::Builtin(BuiltinExpr::FromStream(query))),
                _ => Err(AqError::InvalidExpression(
                    "internal error: fromstream call argument did not parse as query".to_string(),
                )),
            },
            "tostream" => Ok(Expr::Builtin(BuiltinExpr::ToStream)),
            "leaf_paths" => Ok(Expr::Builtin(BuiltinExpr::LeafPaths)),
            "index" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Index(Box::new(expr))))
            }
            "rindex" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Rindex(Box::new(expr))))
            }
            "trimstr" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::TrimStr(Box::new(expr))))
            }
            "ltrimstr" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::LtrimStr(Box::new(expr))))
            }
            "rtrimstr" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::RtrimStr(Box::new(expr))))
            }
            "any" => self.parse_any_all_builtin(true),
            "all" => self.parse_any_all_builtin(false),
            "with_entries" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::WithEntries(Box::new(expr))))
            }
            "sort_by" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::SortBy(Box::new(expr))))
            }
            "contains" => {
                let expected = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Contains(Box::new(expected))))
            }
            "in" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::In(Box::new(expr))))
            }
            "isempty" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::IsEmpty(Box::new(expr))))
            }
            "has" => {
                let key = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Has(Box::new(key))))
            }
            "startswith" => {
                let prefix = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::StartsWith(Box::new(prefix))))
            }
            "endswith" => {
                let suffix = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::EndsWith(Box::new(suffix))))
            }
            "select" => {
                let predicate = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Select(Box::new(predicate))))
            }
            "map" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Map(Box::new(expr))))
            }
            "map_values" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::MapValues(Box::new(expr))))
            }
            "split" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::Split {
                        pattern: Box::new(args.remove(0)),
                        flags: None,
                    })),
                    2 => Ok(Expr::Builtin(BuiltinExpr::Split {
                        pattern: Box::new(args.remove(0)),
                        flags: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "split expects one or two arguments".to_string(),
                    )),
                }
            }
            "sub" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    2 => {
                        let replacement = args.pop().expect("sub should have replacement");
                        let regex = args.pop().expect("sub should have regex");
                        Ok(Expr::Builtin(BuiltinExpr::Sub {
                            regex: Box::new(regex),
                            replacement: Box::new(replacement),
                            flags: None,
                        }))
                    }
                    3 => {
                        let flags = args.pop().expect("sub should have flags");
                        let replacement = args.pop().expect("sub should have replacement");
                        let regex = args.pop().expect("sub should have regex");
                        Ok(Expr::Builtin(BuiltinExpr::Sub {
                            regex: Box::new(regex),
                            replacement: Box::new(replacement),
                            flags: Some(Box::new(flags)),
                        }))
                    }
                    _ => Err(AqError::InvalidExpression(
                        "sub expects two or three arguments".to_string(),
                    )),
                }
            }
            "gsub" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    2 => {
                        let replacement = args.pop().expect("gsub should have replacement");
                        let regex = args.pop().expect("gsub should have regex");
                        Ok(Expr::Builtin(BuiltinExpr::Gsub {
                            regex: Box::new(regex),
                            replacement: Box::new(replacement),
                            flags: None,
                        }))
                    }
                    3 => {
                        let flags = args.pop().expect("gsub should have flags");
                        let replacement = args.pop().expect("gsub should have replacement");
                        let regex = args.pop().expect("gsub should have regex");
                        Ok(Expr::Builtin(BuiltinExpr::Gsub {
                            regex: Box::new(regex),
                            replacement: Box::new(replacement),
                            flags: Some(Box::new(flags)),
                        }))
                    }
                    _ => Err(AqError::InvalidExpression(
                        "gsub expects two or three arguments".to_string(),
                    )),
                }
            }
            "INDEX" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::IndexInput(Box::new(
                        args.remove(0),
                    )))),
                    2 => Ok(Expr::Builtin(BuiltinExpr::IndexStream {
                        source: Box::new(args.remove(0)),
                        key: Box::new(args.remove(0)),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "INDEX expects 1 or 2 arguments".to_string(),
                    )),
                }
            }
            "JOIN" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    2 => Ok(Expr::Builtin(BuiltinExpr::JoinInput {
                        index: Box::new(args.remove(0)),
                        key: Box::new(args.remove(0)),
                    })),
                    3 => Ok(Expr::Builtin(BuiltinExpr::JoinStream {
                        index: Box::new(args.remove(0)),
                        source: Box::new(args.remove(0)),
                        key: Box::new(args.remove(0)),
                        join: None,
                    })),
                    4 => Ok(Expr::Builtin(BuiltinExpr::JoinStream {
                        index: Box::new(args.remove(0)),
                        source: Box::new(args.remove(0)),
                        key: Box::new(args.remove(0)),
                        join: Some(Box::new(args.remove(0))),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "JOIN expects 2 to 4 arguments".to_string(),
                    )),
                }
            }
            "IN" => {
                let mut args = self.parse_semicolon_call_queries()?;
                match args.len() {
                    1 => Ok(Expr::Builtin(BuiltinExpr::InQuery(Box::new(
                        args.remove(0),
                    )))),
                    2 => Ok(Expr::Builtin(BuiltinExpr::InSource {
                        source: Box::new(args.remove(0)),
                        stream: Box::new(args.remove(0)),
                    })),
                    _ => Err(AqError::InvalidExpression(
                        "IN expects 1 or 2 arguments".to_string(),
                    )),
                }
            }
            "join" => {
                let separator = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Join(Box::new(separator))))
            }
            "indices" => {
                let expr = self.parse_required_call_argument()?;
                Ok(Expr::Builtin(BuiltinExpr::Indices(Box::new(expr))))
            }
            _ => {
                let args = self.parse_optional_function_call_queries()?;
                let arity = args.len();
                if self.parameter_names.contains(&raw) && arity == 0 {
                    return Ok(Expr::FunctionCall { name: raw, args });
                }
                if self.known_functions.contains_key(&FunctionKey {
                    name: raw.clone(),
                    arity,
                }) {
                    return Ok(Expr::FunctionCall { name: raw, args });
                }
                Err(AqError::InvalidExpression(format!(
                    "unsupported identifier `{raw}` in expression"
                )))
            }
        }
    }

    fn parse_optional_function_call_queries(&mut self) -> Result<Vec<Query>, AqError> {
        self.skip_ws();
        if !self.consume('(') {
            return Ok(Vec::new());
        }
        self.skip_ws();
        if self.consume(')') {
            return Ok(Vec::new());
        }

        let mut args = Vec::new();
        loop {
            args.push(self.parse_query_until_with_stops(Some(')'), &[], &[';'], true)?);
            self.skip_ws();
            if self.consume(')') {
                break;
            }
            self.expect(';')?;
            self.skip_ws();
        }
        Ok(args)
    }

    fn parse_optional_call_argument(&mut self) -> Result<Option<Expr>, AqError> {
        self.skip_ws();
        if !self.consume('(') {
            return Ok(None);
        }
        self.skip_ws();
        let query = self.parse_query_until(Some(')'), &[])?;
        self.skip_ws();
        self.expect(')')?;
        Ok(Some(Expr::Subquery(Box::new(query))))
    }

    fn parse_required_call_argument(&mut self) -> Result<Expr, AqError> {
        self.skip_ws();
        self.expect('(')?;
        self.skip_ws();
        let query = self.parse_query_until(Some(')'), &[])?;
        self.skip_ws();
        self.expect(')')?;
        Ok(Expr::Subquery(Box::new(query)))
    }

    fn parse_any_all_builtin(&mut self, is_any: bool) -> Result<Expr, AqError> {
        self.skip_ws();
        if self.peek() != Some('(') {
            return Ok(Expr::Builtin(if is_any {
                BuiltinExpr::Any(None)
            } else {
                BuiltinExpr::All(None)
            }));
        }

        let mut args = self.parse_semicolon_call_queries()?;
        match args.len() {
            0 => Ok(Expr::Builtin(if is_any {
                BuiltinExpr::Any(None)
            } else {
                BuiltinExpr::All(None)
            })),
            1 => {
                let predicate = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: missing any/all predicate".to_string(),
                    )
                })?;
                Ok(Expr::Builtin(if is_any {
                    BuiltinExpr::Any(Some(Box::new(predicate)))
                } else {
                    BuiltinExpr::All(Some(Box::new(predicate)))
                }))
            }
            2 => {
                let predicate = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression(
                        "internal error: missing any/all predicate".to_string(),
                    )
                })?;
                let source = args.pop().ok_or_else(|| {
                    AqError::InvalidExpression("internal error: missing any/all source".to_string())
                })?;
                Ok(Expr::Builtin(if is_any {
                    BuiltinExpr::AnyFrom {
                        source: Box::new(source),
                        predicate: Box::new(predicate),
                    }
                } else {
                    BuiltinExpr::AllFrom {
                        source: Box::new(source),
                        predicate: Box::new(predicate),
                    }
                }))
            }
            _ => Err(AqError::InvalidExpression(
                "any/all expect at most 2 arguments".to_string(),
            )),
        }
    }

    fn parse_semicolon_call_queries(&mut self) -> Result<Vec<Query>, AqError> {
        self.skip_ws();
        self.expect('(')?;
        self.skip_ws();
        let mut args = Vec::new();
        if self.consume(')') {
            return Ok(args);
        }

        loop {
            args.push(self.parse_query_until_with_stops(Some(')'), &[], &[';'], true)?);
            self.skip_ws();
            if self.consume(')') {
                break;
            }
            self.expect(';')?;
            self.skip_ws();
        }

        Ok(args)
    }

    fn parse_field(&mut self) -> Result<Segment, AqError> {
        let start = self.index;
        while let Some(value) = self.peek() {
            if is_identifier_continue(value) {
                self.index += 1;
            } else {
                break;
            }
        }
        let name: String = self.chars[start..self.index].iter().collect();
        if name.is_empty() {
            return Err(AqError::InvalidExpression(
                "empty field access is not supported".to_string(),
            ));
        }
        let optional = self.consume('?');
        Ok(Segment::Field { name, optional })
    }

    fn parse_quoted_field(&mut self) -> Result<Segment, AqError> {
        let name = self.parse_string_expr()?;
        let optional = self.consume('?');
        Ok(string_expr_lookup_segment(name, optional))
    }

    fn parse_bracket_segment(&mut self) -> Result<Segment, AqError> {
        self.expect('[')?;
        if self.consume(']') {
            return Ok(Segment::Iterate {
                optional: self.consume('?'),
            });
        }

        let raw = self.parse_bracket_contents()?;
        let segment = if let Some((start, end)) = parse_slice_parts(&raw) {
            if let (Some(start), Some(end)) = (
                parse_static_optional_index(start),
                parse_static_optional_index(end),
            ) {
                Segment::Slice {
                    start,
                    end,
                    optional: self.consume('?'),
                }
            } else {
                Segment::Lookup {
                    expr: Box::new(self.parse_dynamic_slice_lookup_expr(start, end)?),
                    optional: self.consume('?'),
                }
            }
        } else if raw
            .chars()
            .all(|value| value == '-' || value.is_ascii_digit())
            && raw.chars().any(|value| value.is_ascii_digit())
        {
            let index = raw.parse::<isize>().map_err(|_| {
                AqError::InvalidExpression(format!("invalid bracket access `{raw}`"))
            })?;
            Segment::Index {
                index,
                optional: self.consume('?'),
            }
        } else {
            let query = parse(&raw)?;
            Segment::Lookup {
                expr: Box::new(Expr::Subquery(Box::new(query))),
                optional: self.consume('?'),
            }
        };
        Ok(segment)
    }

    fn parse_dynamic_slice_lookup_expr(&self, start: &str, end: &str) -> Result<Expr, AqError> {
        let mut fields = Vec::new();
        if !start.trim().is_empty() {
            fields.push((
                ObjectKey::Static("start".to_string()),
                Expr::Subquery(Box::new(self.parse_nested_query(start.trim())?)),
            ));
        }
        if !end.trim().is_empty() {
            fields.push((
                ObjectKey::Static("end".to_string()),
                Expr::Subquery(Box::new(self.parse_nested_query(end.trim())?)),
            ));
        }
        Ok(Expr::Object(fields))
    }

    fn parse_nested_query(&self, input: &str) -> Result<Query, AqError> {
        let mut parser = Parser::with_options(
            input,
            self.options.clone(),
            Rc::clone(&self.module_loader),
            false,
        );
        parser.known_functions = self.known_functions.clone();
        parser.parameter_names = self.parameter_names.clone();
        parser.parse_query()
    }

    fn parse_bracket_contents(&mut self) -> Result<String, AqError> {
        let start = self.index;
        let mut bracket_depth = 0usize;
        let mut paren_depth = 0usize;
        let mut brace_depth = 0usize;

        while let Some(value) = self.peek() {
            match value {
                '"' => self.skip_nested_string_literal()?,
                '[' => {
                    bracket_depth += 1;
                    self.index += 1;
                }
                ']' => {
                    if bracket_depth == 0 && paren_depth == 0 && brace_depth == 0 {
                        let raw: String = self.chars[start..self.index].iter().collect();
                        self.index += 1;
                        return Ok(raw);
                    }
                    bracket_depth = bracket_depth.saturating_sub(1);
                    self.index += 1;
                }
                '(' => {
                    paren_depth += 1;
                    self.index += 1;
                }
                ')' => {
                    paren_depth = paren_depth.saturating_sub(1);
                    self.index += 1;
                }
                '{' => {
                    brace_depth += 1;
                    self.index += 1;
                }
                '}' => {
                    brace_depth = brace_depth.saturating_sub(1);
                    self.index += 1;
                }
                _ => self.index += 1,
            }
        }

        Err(AqError::InvalidExpression(
            "unterminated index expression".to_string(),
        ))
    }

    fn skip_nested_string_literal(&mut self) -> Result<(), AqError> {
        self.expect('"')?;
        while let Some(value) = self.peek() {
            self.index += 1;
            match value {
                '"' => return Ok(()),
                '\\' => {
                    if self.peek().is_none() {
                        break;
                    }
                    self.index += 1;
                }
                _ => {}
            }
        }

        Err(AqError::InvalidExpression(
            "unterminated index expression".to_string(),
        ))
    }

    fn parse_quoted_string(&mut self) -> Result<String, AqError> {
        let start = self.index;
        self.expect('"')?;
        let mut escaped = false;
        while let Some(value) = self.peek() {
            self.index += 1;
            if escaped {
                escaped = false;
                continue;
            }
            match value {
                '\\' => escaped = true,
                '"' => {
                    let raw: String = self.chars[start..self.index].iter().collect();
                    return serde_json::from_str(&raw).map_err(|error| {
                        AqError::InvalidExpression(format!(
                            "invalid quoted field in expression `{}`: {error}",
                            self.input
                        ))
                    });
                }
                _ => {}
            }
        }
        Err(AqError::InvalidExpression(
            "unterminated quoted field".to_string(),
        ))
    }

    fn expect(&mut self, expected: char) -> Result<(), AqError> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(AqError::InvalidExpression(format!(
                "expected `{expected}` in expression"
            )))
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn consume_str(&mut self, expected: &str) -> bool {
        let expected_chars: Vec<char> = expected.chars().collect();
        if self.chars[self.index..].starts_with(&expected_chars) {
            self.index += expected_chars.len();
            true
        } else {
            false
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        if !self.starts_with_keyword(expected) {
            return false;
        }
        self.index += expected.chars().count();
        true
    }

    fn starts_with_keyword(&self, expected: &str) -> bool {
        let expected_chars: Vec<char> = expected.chars().collect();
        if !self.chars[self.index..].starts_with(&expected_chars) {
            return false;
        }
        !matches!(
            self.chars.get(self.index + expected_chars.len()),
            Some(value) if is_identifier_continue(*value)
        )
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<(), AqError> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            Err(AqError::InvalidExpression(format!(
                "expected `{expected}` in expression"
            )))
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\n' | '\r' | '\t')) {
            self.index += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.index).copied()
    }

    fn peek_n(&self, offset: usize) -> Option<char> {
        self.chars.get(self.index + offset).copied()
    }

    fn is_eof(&self) -> bool {
        self.index >= self.chars.len()
    }
}

fn is_identifier_start(value: char) -> bool {
    value == '_' || value.is_ascii_alphabetic()
}

fn is_identifier_continue(value: char) -> bool {
    value == '_' || value.is_ascii_alphanumeric()
}

fn parse_static_optional_index(raw: &str) -> Option<Option<isize>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Some(None);
    }
    raw.parse::<isize>().ok().map(Some)
}

fn parse_slice_parts(raw: &str) -> Option<(&str, &str)> {
    let mut bracket_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, value) in raw.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match value {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match value {
            '"' => in_string = true,
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '{' => brace_depth += 1,
            '}' => brace_depth = brace_depth.saturating_sub(1),
            ':' if bracket_depth == 0 && paren_depth == 0 && brace_depth == 0 => {
                return Some((&raw[..index], &raw[index + 1..]));
            }
            _ => {}
        }
    }

    None
}

fn is_implicit_unary_operand_terminator(value: char) -> bool {
    matches!(value, ')' | ']' | '}' | ',' | ';' | '|')
}

fn identity_query() -> Query {
    Query {
        functions: Vec::new(),
        outputs: vec![Pipeline {
            stages: vec![Expr::Path(PathExpr {
                segments: Vec::new(),
            })],
        }],
        imported_values: IndexMap::new(),
        module_info: None,
    }
}

const SUPPORTED_BUILTINS: &[&str] = &[
    "add/0",
    "add/1",
    "all/0",
    "all/1",
    "all/2",
    "any/0",
    "any/1",
    "any/2",
    "arrays/0",
    "ascii_downcase/0",
    "ascii_upcase/0",
    "abs/0",
    "avg/0",
    "bsearch/1",
    "booleans/0",
    "builtins/0",
    "ceil/0",
    "combinations/0",
    "combinations/1",
    "contains/1",
    "count_by/1",
    "csv_header/0",
    "csv_header/1",
    "columns/0",
    "debug/0",
    "debug/1",
    "del/1",
    "delpaths/1",
    "drop_nulls/0",
    "drop_nulls/1",
    "empty/0",
    "endswith/1",
    "env/0",
    "error/0",
    "error/1",
    "exp/0",
    "exp2/0",
    "explode/0",
    "fabs/0",
    "first/0",
    "first/1",
    "flatten/0",
    "flatten/1",
    "floor/0",
    "fromdate/0",
    "from_entries/0",
    "fromjson/0",
    "gmtime/0",
    "getpath/1",
    "group_by/1",
    "histogram/1",
    "has/1",
    "IN/1",
    "IN/2",
    "INDEX/1",
    "INDEX/2",
    "implode/0",
    "input/0",
    "inputs/0",
    "in/1",
    "index/1",
    "indices/1",
    "inside/1",
    "isempty/1",
    "iterables/0",
    "JOIN/2",
    "JOIN/3",
    "JOIN/4",
    "join/1",
    "keys/0",
    "keys_unsorted/0",
    "last/0",
    "last/1",
    "leaf_paths/0",
    "length/0",
    "limit/2",
    "log/0",
    "log10/0",
    "log2/0",
    "ltrim/0",
    "trimstr/1",
    "ltrimstr/1",
    "map/1",
    "map_values/1",
    "max/0",
    "max_by/1",
    "median/0",
    "merge/1",
    "merge/2",
    "merge_all/0",
    "merge_all/1",
    "modulemeta/0",
    "mktime/0",
    "match/1",
    "match/2",
    "min/0",
    "min_by/1",
    "percentile/1",
    "nth/1",
    "nth/2",
    "now/0",
    "nulls/0",
    "numbers/0",
    "objects/0",
    "rename/2",
    "path/1",
    "paths/0",
    "paths/1",
    "truncate_stream/1",
    "fromstream/1",
    "tostream/0",
    "pick/1",
    "pow/2",
    "range/1",
    "range/2",
    "range/3",
    "recurse/0",
    "recurse/1",
    "recurse/2",
    "repeat/1",
    "reverse/0",
    "rindex/1",
    "round/0",
    "rtrim/0",
    "rtrimstr/1",
    "scalars/0",
    "scan/1",
    "scan/2",
    "select/1",
    "setpath/2",
    "skip/1",
    "skip/2",
    "sort/0",
    "sort_by/1",
    "sort_by_desc/1",
    "sort_keys/0",
    "sort_keys/1",
    "split/1",
    "split/2",
    "splits/1",
    "splits/2",
    "strftime/1",
    "strflocaltime/1",
    "strptime/1",
    "sqrt/0",
    "startswith/1",
    "stddev/0",
    "strings/0",
    "sub/2",
    "sub/3",
    "sin/0",
    "test/1",
    "test/2",
    "omit/1",
    "take/1",
    "cos/0",
    "tan/0",
    "asin/0",
    "acos/0",
    "atan/0",
    "infinite/0",
    "isnan/0",
    "nan/0",
    "to_bool/0",
    "toboolean/0",
    "to_datetime/0",
    "to_entries/0",
    "to_number/0",
    "tojson/0",
    "todate/0",
    "tonumber/0",
    "transpose/0",
    "trim/0",
    "type/0",
    "utf8bytelength/0",
    "unique/0",
    "unique_by/1",
    "uniq_by/1",
    "until/2",
    "values/0",
    "walk/1",
    "while/2",
    "with_entries/1",
    "xml_attr/0",
    "xml_attr/1",
    "yaml_tag/0",
    "yaml_tag/1",
    "capture/1",
    "capture/2",
    "gsub/2",
    "gsub/3",
];

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use indexmap::IndexMap;

    use crate::error::AqError;
    use crate::query::{
        evaluate, evaluate_with_context, parse, parse_with_options, validate_streaming_query,
        BinaryOp, BindingPattern, EvaluationContext, Expr, ObjectBindingField, ObjectKey,
        ParseOptions, Pipeline, Query, Segment,
    };
    use crate::value::{DecimalValue, Value};

    fn temp_test_dir(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("aq-query-{unique}-{name}"));
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
        fs::write(root.join("c").join("d.jq"), "def meh: \"meh\";\n")
            .expect("d module should write");
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

    #[test]
    fn parses_simple_path() {
        let query = parse(".foo[0].bar").expect("query should parse");
        assert_eq!(
            query,
            Query {
                functions: Vec::new(),
                outputs: vec![Pipeline {
                    stages: vec![Expr::Path(crate::query::PathExpr {
                        segments: vec![
                            Segment::Field {
                                name: "foo".to_string(),
                                optional: false,
                            },
                            Segment::Index {
                                index: 0,
                                optional: false,
                            },
                            Segment::Field {
                                name: "bar".to_string(),
                                optional: false,
                            }
                        ]
                    })]
                }],
                imported_values: IndexMap::new(),
                module_info: None,
            }
        );
    }

    #[test]
    fn parses_pipe_and_comma() {
        let query = parse("(.users[] | .name), (.users[] | .id)").expect("query should parse");
        assert_eq!(query.outputs.len(), 2);
        assert_eq!(query.outputs[0].stages.len(), 1);
        assert_eq!(query.outputs[1].stages.len(), 1);
    }

    #[test]
    fn parses_quoted_and_optional_segments() {
        let query = parse(".\"foo-bar\"?.items[]?").expect("query should parse");
        assert_eq!(
            query.outputs[0].stages[0],
            Expr::Path(crate::query::PathExpr {
                segments: vec![
                    Segment::Field {
                        name: "foo-bar".to_string(),
                        optional: true,
                    },
                    Segment::Field {
                        name: "items".to_string(),
                        optional: false,
                    },
                    Segment::Iterate { optional: true }
                ]
            })
        );
    }

    #[test]
    fn parses_slice_segments() {
        let query = parse(".items[1:-1]").expect("query should parse");
        assert_eq!(
            query.outputs[0].stages[0],
            Expr::Path(crate::query::PathExpr {
                segments: vec![
                    Segment::Field {
                        name: "items".to_string(),
                        optional: false,
                    },
                    Segment::Slice {
                        start: Some(1),
                        end: Some(-1),
                        optional: false,
                    }
                ]
            })
        );
    }

    #[test]
    fn parses_dynamic_slice_segments() {
        let query = parse(".[:rindex(\"x\")]").expect("query should parse");
        let Expr::Path(path) = &query.outputs[0].stages[0] else {
            panic!("expected path expression");
        };
        let [Segment::Lookup { expr, optional }] = &path.segments[..] else {
            panic!("expected dynamic slice lookup segment");
        };
        assert!(!optional);
        assert!(matches!(expr.as_ref(), Expr::Object(_)));
    }

    #[test]
    fn parses_object_and_array_constructors() {
        let query = parse("{name: .name, tags: [.tags[]]}").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 1);
    }

    #[test]
    fn parses_query_pipelines_inside_array_constructors() {
        let query = parse("[.[] | .foo?]").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 1);
    }

    #[test]
    fn parses_object_shorthand_constructors() {
        let query = parse("{title, $name}").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 1);
    }

    #[test]
    fn parses_literal_outputs() {
        let query = parse("\"hello\", 42, true, null").expect("query should parse");
        assert_eq!(query.outputs.len(), 4);
    }

    #[test]
    fn parses_select_and_comparison() {
        let query = parse("select(.age >= 21)").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 1);
    }

    #[test]
    fn parses_map_and_boolean_operators() {
        let query =
            parse("map(.name) // [] | not (.active and .deleted)").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 2);
    }

    #[test]
    fn parses_additional_builtins() {
        let query = parse("has(\"name\"), contains({a: 1}), reverse, sort, empty")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 5);
    }

    #[test]
    fn parses_type_and_string_builtins() {
        let query =
            parse(
                "values, strings, numbers, tostring, tonumber, toboolean, startswith(\"a\"), split(\",\")",
            )
                .expect("query should parse");
        assert_eq!(query.outputs.len(), 8);
    }

    #[test]
    fn parses_error_builtin() {
        let query = parse("error(\"boom\")").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_builtins_builtin() {
        let query = parse("builtins").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_debug_builtin() {
        let query = parse("debug, debug(\"tag\")").expect("query should parse");
        assert_eq!(query.outputs.len(), 2);
    }

    #[test]
    fn parses_env_builtin() {
        let query = parse("env").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_collection_builtins() {
        let query = parse(
            "min, max, unique, flatten, flatten(1), avg, median, take(2), skip(1), combinations, combinations(2), bsearch(1)",
        )
        .expect("query should parse");
        assert_eq!(query.outputs.len(), 12);
    }

    #[test]
    fn parses_aq_extension_builtins() {
        let query = parse(
            "to_number, to_bool, to_datetime, columns, stddev, percentile(50), histogram(2), uniq_by(.a), sort_by_desc(.a), count_by(.a), yaml_tag, yaml_tag(\"!Thing\"), xml_attr, xml_attr(\"id\"), csv_header, csv_header([\"name\", \"role\"]), merge({extra: 1}), merge({nested: {b: 2}}; true), merge_all(true), drop_nulls(true), sort_keys(true), pick(.a, .b), omit(.c), rename(.old; \"new\")",
        )
        .expect("query should parse");
        assert_eq!(query.outputs.len(), 24);
    }

    #[test]
    fn parses_regex_builtins() {
        let query =
            parse(
                "test(\"x\"), test([\"x\", \"i\"]), test(\"x\"; \"i\"), capture(\"(?<x>.)\"), capture([\"(?<x>.)\", \"i\"]), capture(\"(?<x>.)\"; \"i\"), match(\"x\"), match([\"x\", \"ig\"]), match(\"x\"; \"ig\"), scan(\"x\"), scan([\"x\", \"i\"]), scan(\"x\"; \"i\"), split(\",\"), split(\", *\"; null), splits(\", *\"), splits(\", *\"; \"n\"), sub(\"x\"; \"y\"), sub(\"x\"; \"y\"; \"g\"), gsub(\"x\"; \"y\"), gsub(\"x\"; \"y\"; \"i\"), @json, @text, @csv, @tsv, @html, @uri, @urid, @sh, @base64, @base64d",
            )
                .expect("query should parse");
        assert_eq!(query.outputs.len(), 30);
    }

    #[test]
    fn parses_format_string_interpolation() {
        let query = parse("@uri \"x=\\(.x)&y=\\(.y)\", @text \"a\\(.), b\\(.+1)\"")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 2);
    }

    #[test]
    fn parses_jq_style_interpolated_string_literals() {
        let query = parse("\"inter\\(\"pol\" + \"ation\")\"").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_jq_style_unary_negation() {
        let query = parse("{x:-1},{x:-.},{x:-.|abs}").expect("query should parse");
        assert_eq!(query.outputs.len(), 3);
    }

    #[test]
    fn parses_quoted_string_object_shorthand() {
        let query = parse("{\"a\",b,\"a$\\(1+1)\"}").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_input_builtins() {
        let query = parse("input, inputs").expect("query should parse");
        assert_eq!(query.outputs.len(), 2);
    }

    #[test]
    fn validates_streaming_queries_reject_input_builtins() {
        let query = parse("input").expect("query should parse");
        let error = validate_streaming_query(&query).expect_err("stream validation should fail");
        assert!(error.to_string().contains("use `input`"));
    }

    #[test]
    fn validates_streaming_queries_reject_nested_inputs_usage() {
        let query = parse("def next: inputs; next").expect("query should parse");
        let error = validate_streaming_query(&query).expect_err("stream validation should fail");
        assert!(error.to_string().contains("use `inputs`"));
    }

    #[test]
    fn validates_streaming_queries_reject_nested_aq_builtin_input_usage() {
        let query = parse("merge(input)").expect("query should parse");
        let error = validate_streaming_query(&query).expect_err("stream validation should fail");
        assert!(error.to_string().contains("use `input`"));
    }

    #[test]
    fn parses_math_builtins() {
        let query = parse(
            "floor, ceil, round, fabs, sqrt, log, log2, log10, exp, exp2, sin, cos, tan, asin, acos, atan, pow(2; 3), now, todate, fromdate, infinite, nan, isnan",
        )
        .expect("query should parse");
        assert_eq!(query.outputs.len(), 23);
    }

    #[test]
    fn parses_range_and_limit_builtins() {
        let query = parse("range(3), range(1; 4), range(0; 1; 0.25), limit(2; .items[])")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 4);
    }

    #[test]
    fn parses_generator_selection_builtins() {
        let query =
            parse(
                "first(.items[]), last(.items[]), nth(2; .items[]), nth(range(3); range(10)), skip(3; .items[])",
            )
                .expect("query should parse");
        assert_eq!(query.outputs.len(), 5);
    }

    #[test]
    fn parses_loop_builtins() {
        let query = parse("while(. < 3; . + 1), until(. >= 5; . + 1)").expect("query should parse");
        assert_eq!(query.outputs.len(), 2);
    }

    #[test]
    fn parses_recurse_builtins() {
        let query = parse("recurse, recurse(.children[]), recurse(. * .; . < 20), ..")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 4);
    }

    #[test]
    fn parses_repeat_builtin() {
        let query = parse("[repeat(.*2, error)?]").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_map_values_and_indices_builtins() {
        let query = parse("map_values(. + 1), indices(\"an\")").expect("query should parse");
        assert_eq!(query.outputs.len(), 2);
    }

    #[test]
    fn parses_path_update_builtins() {
        let query = parse(
            "getpath([\"a\", 0]), setpath([\"a\", 0]; 1), del(.a, .b[0]), delpaths([[\"a\"], [\"b\", 0]])",
        )
        .expect("query should parse");
        assert_eq!(query.outputs.len(), 4);
    }

    #[test]
    fn parses_assignment_expressions() {
        let query = parse(
            ".a = 1, .items[] |= . + 1, .count += 1, .name //= \"unknown\", .count -= 1, .count *= 2, .count /= 2, .count %= 2",
        )
        .expect("query should parse");
        assert_eq!(query.outputs.len(), 8);
    }

    #[test]
    fn parses_paths_builtins() {
        let query = parse("path(.a), paths, paths(type == \"number\"), leaf_paths")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 4);
    }

    #[test]
    fn parses_walk_builtin() {
        let query =
            parse("walk(if type == \"number\" then . + 1 else . end)").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_transpose_builtin() {
        let query = parse("transpose").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_array_and_string_helper_builtins() {
        let query = parse(
            "any, all(.active), join(\",\"), ascii_downcase, ascii_upcase, trim, ltrim, rtrim",
        )
        .expect("query should parse");
        assert_eq!(query.outputs.len(), 8);
    }

    #[test]
    fn parses_arithmetic_operators_with_precedence() {
        let query = parse("2 + 3 * 4 - 5").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 1);
    }

    #[test]
    fn parses_structural_transform_builtins() {
        let query = parse("to_entries, from_entries, with_entries(.), sort_by(.name)")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 4);
    }

    #[test]
    fn parses_subqueries_and_dynamic_object_keys() {
        let query =
            parse("{(.key | ascii_upcase): (.value | tostring)}").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_search_and_trimstr_builtins() {
        let query =
            parse("index(\"a\"), rindex(\"a\"), trimstr(\"x\"), ltrimstr(\"a\"), rtrimstr(\"z\")")
                .expect("query should parse");
        assert_eq!(query.outputs.len(), 5);
    }

    #[test]
    fn parses_try_expressions() {
        let query = parse("try length catch .").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);

        let query = parse("try (1 / 0) catch \"fallback\"").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_json_and_unicode_builtins() {
        let query = parse("utf8bytelength, keys_unsorted, tojson, fromjson, explode, implode")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 6);
    }

    #[test]
    fn parses_by_collection_builtins() {
        let query = parse("group_by(.kind), unique_by(.kind), min_by(.score), max_by(.score)")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 4);
    }

    #[test]
    fn parses_membership_and_emptiness_builtins() {
        let query =
            parse("inside({a: 1}), in([1, 2]), isempty(.items[])").expect("query should parse");
        assert_eq!(query.outputs.len(), 3);
    }

    #[test]
    fn parses_variable_bindings() {
        let query = parse(".bar as $x | .foo + $x").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 2);
        assert_eq!(
            query.outputs[0].stages[0],
            Expr::Bind {
                expr: Box::new(Expr::Path(crate::query::PathExpr {
                    segments: vec![Segment::Field {
                        name: "bar".to_string(),
                        optional: false,
                    }],
                })),
                pattern: BindingPattern::Variable("x".to_string()),
            }
        );
        assert_eq!(
            query.outputs[0].stages[1],
            Expr::Binary {
                left: Box::new(Expr::Path(crate::query::PathExpr {
                    segments: vec![Segment::Field {
                        name: "foo".to_string(),
                        optional: false,
                    }],
                })),
                op: BinaryOp::Add,
                right: Box::new(Expr::Variable("x".to_string())),
            }
        );
    }

    #[test]
    fn parses_function_definitions() {
        let query = parse("def foo: . + 1; def pair(a; b): [a, b]; foo, pair(.; . + 1)")
            .expect("query should parse");
        assert_eq!(query.functions.len(), 2);
        assert_eq!(query.functions[0].name, "foo");
        assert!(query.functions[0].params.is_empty());
        assert_eq!(query.functions[1].name, "pair");
        assert_eq!(
            query.functions[1].params,
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(query.outputs.len(), 2);
        assert_eq!(
            query.outputs[0].stages[0],
            Expr::FunctionCall {
                name: "foo".to_string(),
                args: Vec::new(),
            }
        );
        match &query.outputs[1].stages[0] {
            Expr::FunctionCall { name, args } => {
                assert_eq!(name, "pair");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected function call, got {other:?}"),
        }
    }

    #[test]
    fn parses_function_definitions_with_variable_parameters() {
        let query = parse("def sum($a; $b): $a + $b; sum(.; . * 2)").expect("query should parse");
        assert_eq!(query.functions.len(), 1);
        assert_eq!(
            query.functions[0].params,
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn rejects_forward_function_references() {
        let error = parse("def a: b; def b: . + 1; 1 | a").expect_err("parse should fail");
        assert!(error.to_string().contains("unsupported identifier `b`"));
    }

    #[test]
    fn rejects_functions_capturing_late_variables() {
        let query = parse("def foo: $x; 1 as $x | 2 | foo").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error.to_string().contains("$x is not defined"));
    }

    #[test]
    fn parses_destructuring_binding_patterns() {
        let query = parse(". as {realnames: $names, posts: [$first, $second]}")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(
            query.outputs[0].stages[0],
            Expr::Bind {
                expr: Box::new(Expr::Path(crate::query::PathExpr { segments: vec![] })),
                pattern: BindingPattern::Object(vec![
                    ObjectBindingField {
                        key: ObjectKey::Static("realnames".to_string()),
                        bind_name: None,
                        pattern: BindingPattern::Variable("names".to_string()),
                    },
                    ObjectBindingField {
                        key: ObjectKey::Static("posts".to_string()),
                        bind_name: None,
                        pattern: BindingPattern::Array(vec![
                            BindingPattern::Variable("first".to_string()),
                            BindingPattern::Variable("second".to_string()),
                        ]),
                    },
                ]),
            }
        );
    }

    #[test]
    fn parses_dynamic_object_binding_fields() {
        let query = parse(". as {(\"e\"+\"x\"+\"p\"): $exp, $items: [$first, $second]}")
            .expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(
            query.outputs[0].stages[0],
            Expr::Bind {
                expr: Box::new(Expr::Path(crate::query::PathExpr { segments: vec![] })),
                pattern: BindingPattern::Object(vec![
                    ObjectBindingField {
                        key: ObjectKey::Dynamic(Box::new(Expr::Subquery(Box::new(Query {
                            functions: Vec::new(),
                            outputs: vec![Pipeline {
                                stages: vec![Expr::Binary {
                                    left: Box::new(Expr::Binary {
                                        left: Box::new(Expr::Literal(Value::String(
                                            "e".to_string(),
                                        ))),
                                        op: BinaryOp::Add,
                                        right: Box::new(Expr::Literal(Value::String(
                                            "x".to_string(),
                                        ))),
                                    }),
                                    op: BinaryOp::Add,
                                    right: Box::new(Expr::Literal(Value::String("p".to_string(),))),
                                }],
                            }],
                            imported_values: IndexMap::new(),
                            module_info: None,
                        })))),
                        bind_name: None,
                        pattern: BindingPattern::Variable("exp".to_string()),
                    },
                    ObjectBindingField {
                        key: ObjectKey::Static("items".to_string()),
                        bind_name: Some("items".to_string()),
                        pattern: BindingPattern::Array(vec![
                            BindingPattern::Variable("first".to_string()),
                            BindingPattern::Variable("second".to_string()),
                        ]),
                    },
                ]),
            }
        );
    }

    #[test]
    fn rejects_empty_binding_patterns() {
        let array_error = parse(". as [] | null").expect_err("parse should fail");
        assert!(array_error
            .to_string()
            .contains("syntax error, unexpected `]`, expecting binding or `or`"));

        let object_error = parse(". as {} | null").expect_err("parse should fail");
        assert!(object_error
            .to_string()
            .contains("syntax error, unexpected `}`"));
    }

    #[test]
    fn rejects_unparenthesized_expression_object_keys() {
        let error = parse("{1+2:3}").expect_err("parse should fail");
        assert!(error
            .to_string()
            .contains("may need parentheses around object key expression"));
    }

    #[test]
    fn rejects_non_string_dynamic_object_keys() {
        let query = parse("{(0):1}").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("Cannot use number (0) as object key"));

        let binding_query = parse(". as {(true):$foo} | $foo").expect("query should parse");
        let error = evaluate(&binding_query, &Value::Null).expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("Cannot use boolean (true) as object key"));
    }

    #[test]
    fn parses_destructuring_alternative_bindings() {
        let query = parse(". as {$a} ?// [$a] | $a").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
        assert_eq!(query.outputs[0].stages.len(), 2);
        assert!(matches!(
            &query.outputs[0].stages[0],
            Expr::BindingAlt { patterns, .. } if patterns.len() == 2
        ));
    }

    #[test]
    fn parses_postfix_access_on_arbitrary_expressions() {
        let query = parse("$names[.author], (.user).name").expect("query should parse");
        assert_eq!(query.outputs.len(), 2);
        assert_eq!(
            query.outputs[0].stages[0],
            Expr::Access {
                base: Box::new(Expr::Variable("names".to_string())),
                segments: vec![Segment::Lookup {
                    expr: Box::new(Expr::Subquery(Box::new(Query {
                        functions: Vec::new(),
                        outputs: vec![Pipeline {
                            stages: vec![Expr::Path(crate::query::PathExpr {
                                segments: vec![Segment::Field {
                                    name: "author".to_string(),
                                    optional: false,
                                }],
                            })],
                        }],
                        imported_values: IndexMap::new(),
                        module_info: None,
                    }))),
                    optional: false,
                }],
            }
        );
    }

    #[test]
    fn parses_if_expressions() {
        let query =
            parse("if .active then .name else \"inactive\" end").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);

        let query = parse(
            "if .kind == \"user\" then .name elif .kind == \"team\" then .team else \"unknown\" end",
        )
        .expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_reduce_expressions() {
        let query = parse("reduce .[] as $item (0; . + $item)").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);

        let query = parse("reduce .[] as [$x, $y] (0; . + $x + $y)").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn parses_foreach_expressions() {
        let query = parse("foreach .[] as $item (0; . + $item)").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);

        let query =
            parse("foreach .[] as [$x, $y] (0; . + $x + $y; . * 10)").expect("query should parse");
        assert_eq!(query.outputs.len(), 1);
    }

    #[test]
    fn evaluates_iterate_and_pipe() {
        let input =
            Value::from_json(serde_json::json!({"users": [{"name": "alice"}, {"name": "bob"}]}))
                .expect("value should parse");
        let query = parse(".users[] | .name").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("alice".to_string()),
                Value::String("bob".to_string())
            ]
        );
    }

    #[test]
    fn evaluates_comma_outputs() {
        let input = Value::from_json(serde_json::json!({"name": "alice", "age": 30}))
            .expect("value should parse");
        let query = parse(".name, .age").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::String("alice".to_string()), Value::Integer(30)]
        );
    }

    #[test]
    fn evaluates_array_constructor() {
        let input = Value::from_json(serde_json::json!({"name": "alice", "age": 30}))
            .expect("value should parse");
        let query = parse("[.name, .age]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::String("alice".to_string()),
                Value::Integer(30)
            ])]
        );
    }

    #[test]
    fn evaluates_query_pipelines_inside_array_constructors() {
        let input =
            Value::from_json(serde_json::json!([1, [2], {"foo": 3, "bar": 4}, {}, {"foo": 5}]))
                .expect("value should parse");
        let query = parse("[.[] | .foo?]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([3, null, 5])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_slice_segments() {
        let input = Value::from_json(serde_json::json!({"items": [1, 2, 3, 4]}))
            .expect("value should parse");
        let query = parse(".items[1:3]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![Value::Integer(2), Value::Integer(3)])]
        );
    }

    #[test]
    fn evaluates_negative_slice_bounds() {
        let input = Value::from_json(serde_json::json!([1, 2, 3, 4])).expect("value should parse");
        let query = parse(".[-2:]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![Value::Integer(3), Value::Integer(4)])]
        );
    }

    #[test]
    fn evaluates_dynamic_slice_segments() {
        let input = Value::from_json(serde_json::json!("正xyz")).expect("value should parse");
        let query = parse(".[:rindex(\"x\")]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("正".to_string())]);
    }

    #[test]
    fn evaluates_float_and_non_finite_slice_bounds() {
        let input =
            Value::from_json(serde_json::json!([0, 1, 2, 3, 4])).expect("value should parse");
        let query = parse(".[1.5:3.5], .[nan:2], .[2:nan]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
                Value::from_json(serde_json::json!([0, 1])).expect("value should parse"),
                Value::from_json(serde_json::json!([2, 3, 4])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_optional_string_slice_segments() {
        let input = Value::from_json(serde_json::json!([
            1,
            null,
            true,
            false,
            "abcdef",
            {},
            {"a": 1, "b": 2},
            [],
            [1, 2, 3, 4, 5],
            [1, 2]
        ]))
        .expect("value should parse");
        let query = parse("[.[] | .[1:3]?]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([null, "bc", [], [2, 3], [2]]))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_object_constructor() {
        let input = Value::from_json(serde_json::json!({"name": "alice", "age": 30}))
            .expect("value should parse");
        let query = parse("{name: .name, age: .age}").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        let expected = Value::from_json(serde_json::json!({"name": "alice", "age": 30}))
            .expect("expected value should parse");
        assert_eq!(values, vec![expected]);
    }

    #[test]
    fn evaluates_object_shorthand_and_variable_shorthand() {
        let input = Value::from_json(serde_json::json!({"title": "hello", "name": "alice"}))
            .expect("value should parse");
        let query = parse(".name as $name | {title, $name}").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"title": "hello", "name": "alice"}))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_object_constructor_cartesian_outputs() {
        let input =
            Value::from_json(serde_json::json!({"users": [{"name": "alice"}, {"name": "bob"}]}))
                .expect("value should parse");
        let query = parse(".users[] | {name: .name}").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"name": "alice"})).expect("value should parse"),
                Value::from_json(serde_json::json!({"name": "bob"})).expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_literal_outputs() {
        let query = parse("\"hello\", 42, true, null").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("hello".to_string()),
                Value::Integer(42),
                Value::Bool(true),
                Value::Null
            ]
        );
    }

    #[test]
    fn evaluates_have_decnum_as_true() {
        let query =
            parse("have_decnum, if have_decnum then 1 else 2 end").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true), Value::Integer(1)]);
    }

    #[test]
    fn evaluates_large_integer_literals_as_exact_decnums() {
        let input = Value::Decimal(
            DecimalValue::parse("12345678909876543212345").expect("value should parse"),
        );
        let query = parse(
            "[., tojson] == if have_decnum then [12345678909876543212345, \"12345678909876543212345\"] else [0, \"0\"] end",
        )
        .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_length_builtin() {
        let input = Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let query = parse("length").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(3)]);
    }

    #[test]
    fn evaluates_iterate_length_pipeline() {
        let input = Value::from_json(serde_json::json!([[1, 2], "string", {"a": 2}, null, -5]))
            .expect("value should parse");
        let query = parse(".[] | length").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(2),
                Value::Integer(6),
                Value::Integer(1),
                Value::Integer(0),
                Value::Integer(5),
            ]
        );

        let optional = evaluate(
            &parse(".[]? | length").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert!(optional.is_empty());
    }

    #[test]
    fn evaluates_keys_builtin() {
        let input =
            Value::from_json(serde_json::json!({"a": 1, "b": 2})).expect("value should parse");
        let query = parse("keys").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::String("a".to_string()),
                Value::String("b".to_string())
            ])]
        );
    }

    #[test]
    fn evaluates_keys_and_keys_unsorted_for_objects() {
        let input =
            Value::from_json(serde_json::json!({"b": 1, "a": 2})).expect("value should parse");
        let query = parse("keys, keys_unsorted").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("b".to_string())
                ]),
                Value::Array(vec![
                    Value::String("b".to_string()),
                    Value::String("a".to_string())
                ])
            ]
        );
    }

    #[test]
    fn evaluates_utf8bytelength_builtin() {
        let query = parse("utf8bytelength").expect("query should parse");
        let values = evaluate(&query, &Value::String("aé".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(3)]);
    }

    #[test]
    fn reports_jq_style_utf8bytelength_errors() {
        let query = parse("[.[] | try utf8bytelength catch .]").expect("query should parse");
        let input = Value::from_json(serde_json::json!([[], {}, [1, 2], 55, true, false]))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([
                "array ([]) only strings have UTF-8 byte length",
                "object ({}) only strings have UTF-8 byte length",
                "array ([1,2]) only strings have UTF-8 byte length",
                "number (55) only strings have UTF-8 byte length",
                "boolean (true) only strings have UTF-8 byte length",
                "boolean (false) only strings have UTF-8 byte length"
            ]))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_tojson_and_fromjson_builtins() {
        let tojson = parse("tojson").expect("query should parse");
        let tojson_values = evaluate(
            &tojson,
            &Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(tojson_values, vec![Value::String("{\"a\":1}".to_string())]);

        let fromjson = parse("fromjson").expect("query should parse");
        let fromjson_values = evaluate(&fromjson, &Value::String("{\"a\":1}".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            fromjson_values,
            vec![Value::from_json(serde_json::json!({"a": 1})).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_tojson_contains_skip_marker_for_string_inputs() {
        let query =
            parse("tojson | contains(\"<skipped: too deep>\")").expect("query should parse");
        let values = evaluate(&query, &Value::String("<skipped: too deep>".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_singleton_array_reduce_tojson_skip_marker_query() {
        let query = parse(
            "reduce range(10001) as $_ ([];[.]) | tojson | contains(\"<skipped: too deep>\")",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_explode_and_implode_builtins() {
        let explode = parse("explode").expect("query should parse");
        let explode_values = evaluate(&explode, &Value::String("Aé".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            explode_values,
            vec![Value::Array(vec![Value::Integer(65), Value::Integer(233)])]
        );

        let implode = parse("implode").expect("query should parse");
        let implode_values = evaluate(
            &implode,
            &Value::Array(vec![Value::Integer(65), Value::Integer(233)]),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(implode_values, vec![Value::String("Aé".to_string())]);
    }

    #[test]
    fn evaluates_add_operator() {
        let input = Value::from_json(serde_json::json!({"left": 2, "right": 3}))
            .expect("value should parse");
        let query = parse(".left + .right").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(5)]);
    }

    #[test]
    fn evaluates_arithmetic_operators() {
        let query = parse("2 + 3 * 4 - 5, 7 / 2, 7 % 3").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(9), Value::Float(3.5), Value::Integer(1)]
        );
    }

    #[test]
    fn preserves_numeric_identity_operations() {
        let input = Value::from_json(serde_json::json!([1, 1.25, 5])).expect("value should parse");
        let query = parse(".[] | [. + 0, . - 0, . * 1, . / 1]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Array(vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Integer(1),
                ]),
                Value::Array(vec![
                    Value::from_json(serde_json::json!(1.25)).expect("value should parse"),
                    Value::from_json(serde_json::json!(1.25)).expect("value should parse"),
                    Value::from_json(serde_json::json!(1.25)).expect("value should parse"),
                    Value::from_json(serde_json::json!(1.25)).expect("value should parse"),
                ]),
                Value::Array(vec![
                    Value::Integer(5),
                    Value::Integer(5),
                    Value::Integer(5),
                    Value::Integer(5),
                ]),
            ]
        );
    }

    #[test]
    fn evaluates_multi_output_binary_operators_in_jq_order() {
        let input = Value::from_json(serde_json::json!([1, 2])).expect("value should parse");
        let query = parse("[.[] / .[]]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([1, 2, 0.5, 1])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_direct_single_value_binary_operators() {
        let input =
            Value::from_json(serde_json::json!({"a": 1, "b": 2})).expect("value should parse");
        let query =
            parse(".a + .b, 2 - (-1), .a and .b, .missing or .b").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(3),
                Value::Integer(3),
                Value::Bool(true),
                Value::Bool(true),
            ]
        );
    }

    #[test]
    fn evaluates_array_difference_and_string_division() {
        let query = parse(".items - .drop, .csv / \",\"").expect("query should parse");
        let input = Value::from_json(serde_json::json!({
            "items": [1, 2, 3, 2],
            "drop": [2],
            "csv": "a,b,c"
        }))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Array(vec![Value::Integer(1), Value::Integer(3)]),
                Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                    Value::String("c".to_string())
                ])
            ]
        );
    }

    #[test]
    fn evaluates_to_entries_and_from_entries() {
        let query = parse("to_entries, (to_entries | from_entries)").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!({"a": 1, "b": 2})).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values[0],
            Value::Array(vec![
                Value::from_json(serde_json::json!({"key": "a", "value": 1}))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!({"key": "b", "value": 2}))
                    .expect("value should parse")
            ])
        );
        assert_eq!(values[1], input);
    }

    #[test]
    fn evaluates_with_entries_builtin() {
        let query = parse("with_entries({key: (.key | ascii_upcase), value: .value})")
            .expect("query should parse");
        let input =
            Value::from_json(serde_json::json!({"a": 1, "b": 2})).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({"A": 1, "B": 2})).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_sort_by_builtin() {
        let query = parse("sort_by(.name | ascii_downcase)").expect("query should parse");
        let input = Value::from_json(serde_json::json!([
            {"name": "Bob"},
            {"name": "alice"}
        ]))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([
                {"name": "alice"},
                {"name": "Bob"}
            ]))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_jq_style_sort_ordering() {
        let sorted = evaluate(
            &parse("sort").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                42,
                [2, 5, 3, 11],
                10,
                {"a": 42, "b": 2},
                {"a": 42},
                true,
                2,
                [2, 6],
                "hello",
                null,
                [2, 5, 6],
                {"a": [], "b": 1},
                "abc",
                "ab",
                [3, 10],
                {},
                false,
                "abcd",
                null
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            sorted,
            vec![Value::from_json(serde_json::json!([
                null,
                null,
                false,
                true,
                2,
                10,
                42,
                "ab",
                "abc",
                "abcd",
                "hello",
                [2, 5, 3, 11],
                [2, 5, 6],
                [2, 6],
                [3, 10],
                {},
                {"a": 42},
                {"a": 42, "b": 2},
                {"a": [], "b": 1}
            ]))
            .expect("value should parse")]
        );

        let extrema = evaluate(
            &parse("[min, max, min_by(.[1]), max_by(.[1]), min_by(.[2]), max_by(.[2])]")
                .expect("query should parse"),
            &Value::from_json(serde_json::json!([
                [4, 2, "a"],
                [3, 1, "a"],
                [2, 4, "a"],
                [1, 3, "a"]
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            extrema,
            vec![Value::from_json(serde_json::json!([
                [1, 3, "a"],
                [4, 2, "a"],
                [3, 1, "a"],
                [2, 4, "a"],
                [4, 2, "a"],
                [1, 3, "a"]
            ]))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_group_by_builtin() {
        let query = parse("group_by(.kind)").expect("query should parse");
        let input = Value::from_json(serde_json::json!([
            {"kind": "b", "name": "beta"},
            {"kind": "a", "name": "alpha"},
            {"kind": "b", "name": "bravo"}
        ]))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([
                [
                    {"kind": "a", "name": "alpha"}
                ],
                [
                    {"kind": "b", "name": "beta"},
                    {"kind": "b", "name": "bravo"}
                ]
            ]))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_grouping_builtins_with_multi_output_keys() {
        let input = Value::from_json(serde_json::json!([
            {"a": 1, "b": 4, "c": 14},
            {"a": 4, "b": 1, "c": 3},
            {"a": 1, "b": 4, "c": 3},
            {"a": 0, "b": 2, "c": 43}
        ]))
        .expect("value should parse");
        let values = evaluate(
            &parse(
                "(sort_by(.b) | sort_by(.a)), sort_by(.a, .b), sort_by(.b, .c), group_by(.b), group_by(.a + .b - .c == 2), count_by(.a, .b)",
            )
            .expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([
                    {"a": 0, "b": 2, "c": 43},
                    {"a": 1, "b": 4, "c": 14},
                    {"a": 1, "b": 4, "c": 3},
                    {"a": 4, "b": 1, "c": 3}
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    {"a": 0, "b": 2, "c": 43},
                    {"a": 1, "b": 4, "c": 14},
                    {"a": 1, "b": 4, "c": 3},
                    {"a": 4, "b": 1, "c": 3}
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    {"a": 4, "b": 1, "c": 3},
                    {"a": 0, "b": 2, "c": 43},
                    {"a": 1, "b": 4, "c": 3},
                    {"a": 1, "b": 4, "c": 14}
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    [{"a": 4, "b": 1, "c": 3}],
                    [{"a": 0, "b": 2, "c": 43}],
                    [{"a": 1, "b": 4, "c": 14}, {"a": 1, "b": 4, "c": 3}]
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    [{"a": 1, "b": 4, "c": 14}, {"a": 0, "b": 2, "c": 43}],
                    [{"a": 4, "b": 1, "c": 3}, {"a": 1, "b": 4, "c": 3}]
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    {"count": 1, "key": [0, 2]},
                    {"count": 2, "key": [1, 4]},
                    {"count": 1, "key": [4, 1]}
                ]))
                .expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_unique_by_builtin() {
        let query = parse("unique_by(.kind)").expect("query should parse");
        let input = Value::from_json(serde_json::json!([
            {"kind": "b", "name": "beta"},
            {"kind": "a", "name": "alpha"},
            {"kind": "b", "name": "bravo"}
        ]))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([
                {"kind": "a", "name": "alpha"},
                {"kind": "b", "name": "beta"}
            ]))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_min_by_and_max_by_builtins() {
        let query = parse("min_by(.score), max_by(.score)").expect("query should parse");
        let input = Value::from_json(serde_json::json!([
            {"name": "alice", "score": 7},
            {"name": "bob", "score": 3},
            {"name": "carol", "score": 9}
        ]))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"name": "bob", "score": 3}))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!({"name": "carol", "score": 9}))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_min_by_and_max_by_on_empty_arrays() {
        let query = parse("min_by(.score), max_by(.score)").expect("query should parse");
        let input = Value::Array(Vec::new());
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Null, Value::Null]);
    }

    #[test]
    fn evaluates_dynamic_object_keys() {
        let query = parse("{(.key | ascii_upcase): .value}").expect("query should parse");
        let input = Value::from_json(serde_json::json!({"key": "name", "value": 1}))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({"NAME": 1})).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_direct_object_constructor_with_dynamic_key_and_paths() {
        let query = parse("{a,b,(.d):.a,e:.b}").expect("query should parse");
        let input = Value::from_json(serde_json::json!({
            "a": 1,
            "b": 2,
            "c": 3,
            "d": "c"
        }))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({
                "a": 1,
                "b": 2,
                "c": 1,
                "e": 2
            }))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_direct_object_constructor_with_variable_and_if_subquery_keys() {
        let query = parse("1 as $x | \"3\" as $z | { $x, as, ($z): if false then 4 else 5 end }")
            .expect("query should parse");
        let input = Value::from_json(serde_json::json!({"as": 2})).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({
                "x": 1,
                "as": 2,
                "3": 5
            }))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_index_and_rindex() {
        let string_query = parse("index(\"na\"), rindex(\"na\")").expect("query should parse");
        let string_values = evaluate(&string_query, &Value::String("banana".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(string_values, vec![Value::Integer(2), Value::Integer(4)]);

        let array_query = parse("index(2), rindex(2), indices(2)").expect("query should parse");
        let array_input =
            Value::from_json(serde_json::json!([1, 2, 3, 2])).expect("value should parse");
        let array_values = evaluate(&array_query, &array_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            array_values,
            vec![
                Value::Integer(1),
                Value::Integer(3),
                Value::from_json(serde_json::json!([1, 3])).expect("value should parse"),
            ]
        );

        let unicode_query =
            parse("index(\"!\"), rindex(\"в\"), indices(\"в\")").expect("query should parse");
        let unicode_values = evaluate(
            &unicode_query,
            &Value::String("здравствуй мир!".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            unicode_values,
            vec![
                Value::Integer(14),
                Value::Integer(7),
                Value::from_json(serde_json::json!([4, 7])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_trimstr_variants() {
        let query = parse("trimstr(\"foo\"), ltrimstr(\"pre\"), rtrimstr(\"post\")")
            .expect("query should parse");
        let values = evaluate(&query, &Value::String("prefixpost".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("prefixpost".to_string()),
                Value::String("fixpost".to_string()),
                Value::String("prefix".to_string())
            ]
        );

        let trimmed = evaluate(
            &parse("trimstr(\"foo\")").expect("query should parse"),
            &Value::String("foobarfoo".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(trimmed, vec![Value::String("bar".to_string())]);
    }

    #[test]
    fn evaluates_if_then_else() {
        let query =
            parse("if .active then .name else \"inactive\" end").expect("query should parse");
        let active = Value::from_json(serde_json::json!({"active": true, "name": "alice"}))
            .expect("value should parse");
        let inactive = Value::from_json(serde_json::json!({"active": false, "name": "alice"}))
            .expect("value should parse");

        let active_values = evaluate(&query, &active)
            .expect("query should run")
            .into_vec();
        let inactive_values = evaluate(&query, &inactive)
            .expect("query should run")
            .into_vec();

        assert_eq!(active_values, vec![Value::String("alice".to_string())]);
        assert_eq!(inactive_values, vec![Value::String("inactive".to_string())]);
    }

    #[test]
    fn evaluates_if_elif_else() {
        let query = parse(
            "if .kind == \"user\" then .name elif .kind == \"team\" then .team else \"unknown\" end",
        )
        .expect("query should parse");
        let user = Value::from_json(serde_json::json!({"kind": "user", "name": "alice"}))
            .expect("value should parse");
        let team = Value::from_json(serde_json::json!({"kind": "team", "team": "ops"}))
            .expect("value should parse");
        let other =
            Value::from_json(serde_json::json!({"kind": "other"})).expect("value should parse");

        assert_eq!(
            evaluate(&query, &user)
                .expect("query should run")
                .into_vec(),
            vec![Value::String("alice".to_string())]
        );
        assert_eq!(
            evaluate(&query, &team)
                .expect("query should run")
                .into_vec(),
            vec![Value::String("ops".to_string())]
        );
        assert_eq!(
            evaluate(&query, &other)
                .expect("query should run")
                .into_vec(),
            vec![Value::String("unknown".to_string())]
        );
    }

    #[test]
    fn evaluates_if_branches_with_pipelines_and_multiple_outputs() {
        let query =
            parse("if .active then .items[] | .name else empty end").expect("query should parse");
        let input = Value::from_json(serde_json::json!({
            "active": true,
            "items": [{"name": "alice"}, {"name": "bob"}]
        }))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("alice".to_string()),
                Value::String("bob".to_string())
            ]
        );
    }

    #[test]
    fn evaluates_jq_style_if_semantics() {
        let query = parse(
            "[if 1,null,2 then 3 else 4 end], \
             [if empty then 3 else 4 end], \
             [if true then 3 end], \
             [if false then 3 end], \
             [if false then 3 elif false then 4 end], \
             [if false,false then 3 elif true then 4 else 5 end]",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([3, 4, 3])).expect("value should parse"),
                Value::from_json(serde_json::json!([])).expect("value should parse"),
                Value::from_json(serde_json::json!([3])).expect("value should parse"),
                Value::from_json(serde_json::json!([null])).expect("value should parse"),
                Value::from_json(serde_json::json!([null])).expect("value should parse"),
                Value::from_json(serde_json::json!([4, 4])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_postfix_after_control_flow_expressions() {
        let values = evaluate(
            &parse("if true then [.] else . end []").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Null]);
    }

    #[test]
    fn evaluates_try_precedence_and_optional_postfix() {
        let values = evaluate(
            &parse("try error(0) // 1, 1 + try 2 catch 3 + 4").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Integer(1), Value::Integer(7)]);

        let optional_values = evaluate(
            &parse("[.[]|(.a, .a)?], [[.[]|[.a,.a]]?], [if error then 1 else 2 end?]")
                .expect("query should parse"),
            &Value::from_json(serde_json::json!([null, true, {"a": 1}]))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            optional_values,
            vec![
                Value::from_json(serde_json::json!([null, null, 1, 1]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([])).expect("value should parse"),
                Value::from_json(serde_json::json!([])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn supports_optional_root_path() {
        let values = evaluate(
            &parse("try -.? catch .").expect("query should parse"),
            &Value::String("foo".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::String(
                "string (\"foo\") cannot be negated".to_string()
            )]
        );
    }

    #[test]
    fn truncates_rendered_values_in_jq_style_type_errors() {
        let string_values = evaluate(
            &parse("try -. catch .").expect("query should parse"),
            &Value::String("very-long-long-long-long-string".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            string_values,
            vec![Value::String(
                "string (\"very-long-long-long-long...\") cannot be negated".to_string(),
            )]
        );

        let unicode_values = evaluate(
            &parse("try -. catch .").expect("query should parse"),
            &Value::String("xxxx☆☆☆☆☆☆☆☆".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            unicode_values,
            vec![Value::String(
                "string (\"xxxx☆☆☆☆☆☆...\") cannot be negated".to_string(),
            )]
        );

        let number_values = evaluate(
            &parse("try (. + \"x\") catch .").expect("query should parse"),
            &Value::Float(1.2345678901234568e29),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            number_values,
            vec![Value::String(
                "number (12345678901234568000000000...) and string (\"x\") cannot be added"
                    .to_string(),
            )]
        );
    }

    #[test]
    fn trim_family_requires_strings_with_jq_errors() {
        let values = evaluate(
            &parse("try trim catch ., try ltrim catch ., try rtrim catch .")
                .expect("query should parse"),
            &Value::Integer(123),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("trim input must be a string".to_string()),
                Value::String("trim input must be a string".to_string()),
                Value::String("trim input must be a string".to_string()),
            ]
        );
    }

    #[test]
    fn evaluates_string_repetition() {
        let repeated = evaluate(
            &parse("[.[] * 3]").expect("query should parse"),
            &Value::from_json(serde_json::json!(["a", "ab", "abc"])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            repeated,
            vec![
                Value::from_json(serde_json::json!(["aaa", "ababab", "abcabcabc"]))
                    .expect("value should parse")
            ]
        );

        let numeric_repeat = evaluate(
            &parse("[.[] * \"abc\"]").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 3.7, 10.0
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            numeric_repeat,
            vec![Value::from_json(serde_json::json!([
                null,
                null,
                "",
                "",
                "abc",
                "abc",
                "abcabcabc",
                "abcabcabcabcabcabcabcabcabcabc"
            ]))
            .expect("value should parse")]
        );

        let nan_repeat = evaluate(
            &parse("[. * (nan,-nan)]").expect("query should parse"),
            &Value::String("abc".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            nan_repeat,
            vec![Value::from_json(serde_json::json!([null, null])).expect("value should parse")]
        );

        let big_repeat = evaluate(
            &parse(". * 100000 | [.[:10], .[-10:]]").expect("query should parse"),
            &Value::String("abc".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            big_repeat,
            vec![
                Value::from_json(serde_json::json!(["abcabcabca", "cabcabcabc"]))
                    .expect("value should parse")
            ]
        );

        let unicode_repeat = evaluate(
            &parse(". * 5 | [.[:4], .[-4:], .[1:7]]").expect("query should parse"),
            &Value::String("muμ".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            unicode_repeat,
            vec![
                Value::from_json(serde_json::json!(["muμm", "μmuμ", "uμmuμm"]))
                    .expect("value should parse")
            ]
        );

        let too_large = evaluate(
            &parse("try (. * 1000000000) catch .").expect("query should parse"),
            &Value::String("abc".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            too_large,
            vec![Value::String("Repeat string result too long".to_string())]
        );

        let too_large_sliced = evaluate(
            &parse("try (. * 1000000000 | [.[:10], .[-10:]]) catch .").expect("query should parse"),
            &Value::String("abc".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            too_large_sliced,
            vec![Value::String("Repeat string result too long".to_string())]
        );
    }

    #[test]
    fn reports_jq_style_field_access_errors() {
        let values = evaluate(
            &parse("try .a catch .").expect("query should parse"),
            &Value::Integer(1),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::String(
                "Cannot index number with string (\"a\")".to_string()
            )]
        );
    }

    #[test]
    fn evaluates_try_without_catch_as_empty_on_error() {
        let query = parse("try length").expect("query should parse");
        let values = evaluate(&query, &Value::Bool(true))
            .expect("query should run")
            .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn evaluates_try_catch_with_error_string_input() {
        let query = parse("try length catch .").expect("query should parse");
        let values = evaluate(&query, &Value::Bool(true))
            .expect("query should run")
            .into_vec();
        assert_eq!(values.len(), 1);
        assert!(
            matches!(&values[0], Value::String(message) if message.contains("length is not defined"))
        );
    }

    #[test]
    fn evaluates_try_catch_with_parenthesized_body() {
        let query = parse("try (1 / 0) catch \"fallback\"").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("fallback".to_string())]);
    }

    #[test]
    fn evaluates_try_subqueries_without_discarding_prior_outputs() {
        let query = parse(
            "try ([\"hi\",\"ho\"]|.[]|(try . catch (if .==\"ho\" then \"BROKEN\"|error else empty end)) \
             | if .==\"ho\" then error else \"\\(.) there!\" end) \
             catch \"caught outside \\(.)\"",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("hi there!".to_string()),
                Value::String("caught outside ho".to_string()),
            ]
        );
    }

    #[test]
    fn evaluates_error_builtin_with_try_catch() {
        let string_query =
            parse("try error(\"boom\") catch ., try error catch .").expect("query should parse");
        let string_values = evaluate(&string_query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            string_values,
            vec![Value::String("boom".to_string()), Value::Null]
        );

        let object_query = parse("try error({a: 1}) catch .").expect("query should parse");
        let object_values = evaluate(&object_query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            object_values,
            vec![Value::from_json(serde_json::json!({"a": 1})).expect("value should parse")]
        );
    }

    #[test]
    fn errors_on_error_builtin_with_non_scalar_payload() {
        let query = parse("error({a: 1})").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error.to_string().contains("(not a string): {\"a\":1}"));
    }

    #[test]
    fn evaluates_builtins_builtin() {
        let query = parse("builtins").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values.len(), 1);
        let Value::Array(names) = &values[0] else {
            panic!("builtins should return an array");
        };
        let names = names
            .iter()
            .map(|value| match value {
                Value::String(value) => value.as_str(),
                other => panic!("builtin entry should be a string, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(names.contains(&"builtins/0"));
        assert!(names.contains(&"error/0"));
        assert!(names.contains(&"error/1"));
        assert!(names.contains(&"flatten/0"));
        assert!(names.contains(&"flatten/1"));
        assert!(names.contains(&"histogram/1"));
        assert!(names.contains(&"IN/1"));
        assert!(names.contains(&"IN/2"));
        assert!(names.contains(&"INDEX/1"));
        assert!(names.contains(&"INDEX/2"));
        assert!(names.contains(&"infinite/0"));
        assert!(names.contains(&"isnan/0"));
        assert!(names.contains(&"JOIN/2"));
        assert!(names.contains(&"JOIN/3"));
        assert!(names.contains(&"JOIN/4"));
        assert!(names.contains(&"nan/0"));
        assert!(names.contains(&"range/1"));
        assert!(names.contains(&"range/2"));
        assert!(names.contains(&"range/3"));
        assert!(names.contains(&"test/1"));
        assert!(names.contains(&"test/2"));
        assert!(names.contains(&"capture/1"));
        assert!(names.contains(&"capture/2"));
        assert!(names.contains(&"match/1"));
        assert!(names.contains(&"match/2"));
        assert!(names.contains(&"scan/1"));
        assert!(names.contains(&"scan/2"));
        assert!(names.contains(&"split/1"));
        assert!(names.contains(&"split/2"));
        assert!(names.contains(&"splits/1"));
        assert!(names.contains(&"splits/2"));
        assert!(names.contains(&"gmtime/0"));
        assert!(names.contains(&"mktime/0"));
        assert!(names.contains(&"strftime/1"));
        assert!(names.contains(&"strflocaltime/1"));
        assert!(names.contains(&"strptime/1"));
        assert!(names.contains(&"sub/2"));
        assert!(names.contains(&"sub/3"));
        assert!(names.contains(&"toboolean/0"));
        assert!(names.contains(&"gsub/2"));
        assert!(names.contains(&"gsub/3"));
        assert!(names.contains(&"to_datetime/0"));
    }

    #[test]
    fn evaluates_debug_builtin_as_passthrough() {
        let query = parse("debug, debug(\"tag\")").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(1))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1), Value::Integer(1)]);
    }

    #[test]
    fn evaluates_env_builtin() {
        let query = parse("env").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values.len(), 1);
        let Value::Object(env) = &values[0] else {
            panic!("env should return an object");
        };
        if let Ok(path) = std::env::var("PATH") {
            assert_eq!(env.get("PATH"), Some(&Value::String(path)));
        }
        assert!(env.values().all(|value| matches!(value, Value::String(_))));
    }

    #[test]
    fn evaluates_alt_operator() {
        let input = Value::from_json(serde_json::json!({"nickname": null, "name": "alice"}))
            .expect("value should parse");
        let query = parse(".nickname // .name").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("alice".to_string())]);

        let stream_values = evaluate(
            &parse("[.[] | [.foo[] // .bar]]").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                {"foo": [1, 2], "bar": 42},
                {"foo": [1], "bar": null},
                {"foo": [null, false, 3], "bar": 18},
                {"foo": [], "bar": 42},
                {"foo": [null, false, null], "bar": 41}
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            stream_values,
            vec![
                Value::from_json(serde_json::json!([[1, 2], [1], [3], [42], [41]]))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_boolean_operators() {
        let input = Value::from_json(serde_json::json!({
            "active": true,
            "deleted": false,
            "disabled": false
        }))
        .expect("value should parse");
        let query =
            parse(".active and not .deleted and not .disabled").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_boolean_operators_with_jq_short_circuiting() {
        let input = Value::from_json(serde_json::json!([1, {"b": 3}])).expect("value should parse");

        let and_values = evaluate(
            &parse("type == \"object\" and has(\"b\")").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(and_values, vec![Value::Bool(false)]);

        let or_values = evaluate(
            &parse("type != \"object\" or has(\"b\")").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(or_values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_map_builtin() {
        let input =
            Value::from_json(serde_json::json!({"users": [{"name": "alice"}, {"name": "bob"}]}))
                .expect("value should parse");
        let query = parse(".users | map(.name)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::String("alice".to_string()),
                Value::String("bob".to_string())
            ])]
        );
    }

    #[test]
    fn evaluates_add_builtin() {
        let input = Value::from_json(serde_json::json!({"numbers": [1, 2, 3]}))
            .expect("value should parse");
        let query = parse(".numbers | add").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(6)]);
    }

    #[test]
    fn evaluates_add_builtin_with_query_arguments() {
        let values = evaluate(
            &parse("[add(null), add(range(range(10))), add(empty), add(10,range(10))]")
                .expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([null, 120, null, 55]))
                .expect("value should parse")]
        );

        let values = evaluate(
            &parse(".sum = add(.arr[])").expect("query should parse"),
            &Value::from_json(serde_json::json!({"arr": []})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"arr": [], "sum": null}))
                    .expect("value should parse")
            ]
        );

        let values = evaluate(
            &parse("add({(.[]):1}) | keys").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                "a", "a", "b", "a", "d", "b", "d", "a", "d"
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!(["a", "b", "d"])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_first_and_last_builtins() {
        let input = Value::from_json(serde_json::json!({"numbers": [1, 2, 3]}))
            .expect("value should parse");
        let query = parse("(.numbers | first), (.numbers | last)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1), Value::Integer(3)]);
    }

    #[test]
    fn evaluates_has_builtin() {
        let input =
            Value::from_json(serde_json::json!({"name": "alice"})).expect("value should parse");
        let query = parse("has(\"name\")").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);

        let array_values = evaluate(
            &parse("has(nan), has(1.0), has(1.5)").expect("query should parse"),
            &Value::from_json(serde_json::json!([0, 1, 2])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            array_values,
            vec![Value::Bool(false), Value::Bool(true), Value::Bool(false)]
        );
    }

    #[test]
    fn evaluates_contains_builtin() {
        let input = Value::from_json(serde_json::json!({
            "user": {"name": "alice", "active": true},
            "tags": ["ops", "prod"]
        }))
        .expect("value should parse");
        let query = parse("contains({user: {name: \"alice\"}})").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_inside_builtin() {
        let input =
            Value::from_json(serde_json::json!({"name": "alice"})).expect("value should parse");
        let query = parse("inside({name: \"alice\", active: true})").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_in_builtin() {
        let object_query = parse("in({\"name\": 1, \"age\": 2})").expect("query should parse");
        let object_values = evaluate(&object_query, &Value::String("name".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(object_values, vec![Value::Bool(true)]);

        let array_query = parse("in([10, 20])").expect("query should parse");
        let array_values = evaluate(&array_query, &Value::Integer(1))
            .expect("query should run")
            .into_vec();
        assert_eq!(array_values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_uppercase_in_builtin() {
        let values = evaluate(
            &parse("range(5;13)|IN(range(0;10;3))").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false),
            ]
        );

        let values = evaluate(
            &parse("IN(range(10;20); range(10)), IN(range(5;20); range(10))")
                .expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Bool(false), Value::Bool(true)]);

        let values = evaluate(
            &parse("walk(select(IN({}, []) | not))").expect("query should parse"),
            &Value::from_json(serde_json::json!({"a": 1, "b": []})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({"a": 1})).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_isempty_builtin() {
        let input = Value::from_json(serde_json::json!({
            "items": [{"active": true}, {"active": false}]
        }))
        .expect("value should parse");
        let query =
            parse("isempty(.items[] | select(.active)), isempty(.items[] | select(.missing))")
                .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(false), Value::Bool(true)]);
    }

    #[test]
    fn evaluates_variable_binding_with_original_input() {
        let input = Value::from_json(serde_json::json!({"foo": 10, "bar": 200}))
            .expect("value should parse");
        let query = parse(".bar as $x | .foo | . + $x").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(210)]);
    }

    #[test]
    fn evaluates_variable_shadowing_lexically() {
        let query = parse(". as $x | (1 as $x | $x) + $x").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(5))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(6)]);
    }

    #[test]
    fn evaluates_user_defined_functions() {
        let query = parse("def inc: . + 1; 1 | inc").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(2)]);
    }

    #[test]
    fn evaluates_user_defined_functions_with_filter_parameters() {
        let query = parse("def apply_each(f): .[] | f; [1, 2] | apply_each(. + 1)")
            .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(2), Value::Integer(3)]);
    }

    #[test]
    fn evaluates_user_defined_functions_with_variable_parameters() {
        let query = parse(
            "def x(a;b): a as $a | b as $b | $a + $b; def y($a;$b): $a + $b; def check(a;b): [x(a;b)] == [y(a;b)]; check(.[];.[]*2)",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn evaluates_local_function_definitions_in_expression_contexts() {
        let query = parse(
            "def f: 1; def g: f, def f: 2; def g: 3; f, def f: g; f, g; def f: 4; [f, def f: g; def g: 5; f, g]+[f,g]",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([4, 1, 2, 3, 3, 5, 4, 1, 2, 3, 3]))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_user_defined_functions_shadowing_builtins() {
        let query = parse("def capture: 1; capture").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1)]);
    }

    #[test]
    fn evaluates_include_and_import_directives() {
        let dir = temp_test_dir("jq-modules");
        fs::write(
            dir.join("math.jq"),
            "def inc: . + 1; def twice_inc: inc | inc;",
        )
        .expect("module should write");
        let query = parse_with_options(
            "include \"math\"; import \"math\" as m; (1 | inc), (1 | m::twice_inc)",
            &ParseOptions::with_module_dir(dir),
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(2), Value::Integer(3)]);
    }

    #[test]
    fn evaluates_library_path_search_and_module_metadata_syntax() {
        let root = temp_test_dir("jq-library-path");
        let lib = root.join("lib");
        fs::create_dir_all(&lib).expect("lib dir should create");
        fs::write(
            lib.join("math.jq"),
            "module {kind: \"math\"}; def inc: . + 1; def twice_inc: inc | inc;",
        )
        .expect("module should write");
        let query = parse_with_options(
            "include \"math\" {search: \"lib\"}; import \"math\" as m {search: \"lib\"}; (1 | inc), (1 | m::twice_inc)",
            &ParseOptions::with_module_search_paths(root, vec![lib]),
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(2), Value::Integer(3)]);
    }

    #[test]
    fn evaluates_upstream_style_module_imports_and_data_imports() {
        let root = temp_test_dir("jq-upstream-modules");
        write_upstream_module_fixtures(&root);
        let query = parse_with_options(
            "import \"c\" as foo; [foo::a, foo::c]",
            &ParseOptions::with_module_dir(root.clone()),
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([0, "acmehbah"])).expect("value should parse")]
        );

        let query = parse_with_options(
            "import \"data\" as $e; import \"data\" as $d; [$d[].this,$e[].that,$d::d[].this,$e::e[].that]|join(\";\")",
            &ParseOptions::with_module_dir(root),
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::String(
                "is a test;is too;is a test;is too".to_string()
            )]
        );
    }

    #[test]
    fn evaluates_modulemeta_builtin_for_upstream_module_fixtures() {
        let root = temp_test_dir("jq-modulemeta");
        write_upstream_module_fixtures(&root);
        let query = parse_with_options("modulemeta", &ParseOptions::with_module_dir(root))
            .expect("query should parse");
        let values = evaluate(&query, &Value::String("c".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({
                "whatever": null,
                "deps": [
                    {"as":"foo","is_data":false,"relpath":"a"},
                    {"search":"./","as":"d","is_data":false,"relpath":"d"},
                    {"search":"./","as":"d2","is_data":false,"relpath":"d"},
                    {"search":"./../lib/jq","as":"e","is_data":false,"relpath":"e"},
                    {"search":"./../lib/jq","as":"f","is_data":false,"relpath":"f"},
                    {"as":"d","is_data":true,"relpath":"data"}
                ],
                "defs": ["a/0", "c/0"]
            }))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_label_and_break_over_generators() {
        let query = parse("[ label $if | range(10) | ., (select(. == 5) | break $if) ]")
            .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([0, 1, 2, 3, 4, 5]))
                .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_label_and_break_inside_foreach() {
        let query = parse(
            "[label $out | foreach .[] as $item ([3, null]; if .[0] < 1 then break $out else [.[0] -1, $item] end; .[1])]",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!([11, 22, 33, 44, 55, 66, 77, 88, 99]))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([11, 22, 33])).expect("value should parse")]
        );
    }

    #[test]
    fn reports_missing_top_level_labels() {
        let query = parse(". as $foo | break $foo").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error.to_string().contains("label foo is not defined"));
    }

    #[test]
    fn rejects_modules_with_filters() {
        let dir = temp_test_dir("jq-bad-module");
        fs::write(dir.join("bad.jq"), "def inc: . + 1; .").expect("module should write");
        let error = parse_with_options("include \"bad\"; 1", &ParseOptions::with_module_dir(dir))
            .expect_err("parse should fail");
        assert!(error
            .to_string()
            .contains("must only contain definitions, imports, or includes"));
    }

    #[test]
    fn rejects_cyclic_module_imports() {
        let dir = temp_test_dir("jq-cyclic-module");
        fs::write(dir.join("a.jq"), "include \"b\"; def a: 1;").expect("module should write");
        fs::write(dir.join("b.jq"), "include \"a\"; def b: 1;").expect("module should write");
        let error = parse_with_options("include \"a\"; 1", &ParseOptions::with_module_dir(dir))
            .expect_err("parse should fail");
        assert!(error.to_string().contains("cyclic module import"));
    }

    #[test]
    fn rejects_module_directives_in_main_queries() {
        let error = parse("module {kind: \"main\"}; .").expect_err("parse should fail");
        assert!(error
            .to_string()
            .contains("module directives are only supported in jq module files"));
    }

    #[test]
    fn rejects_invalid_module_metadata_before_main_query_directive_errors() {
        let constant_error = parse("module (.+1); 0").expect_err("parse should fail");
        assert!(constant_error
            .to_string()
            .contains("module metadata must be constant"));

        let object_error = parse("module []; 0").expect_err("parse should fail");
        assert!(object_error
            .to_string()
            .contains("module metadata must be an object"));
    }

    #[test]
    fn rejects_invalid_include_metadata_and_dynamic_import_paths() {
        let constant_error = parse("include \"a\" (.+1); 0").expect_err("parse should fail");
        assert!(constant_error
            .to_string()
            .contains("module metadata must be constant"));

        let object_error = parse("include \"a\" []; 0").expect_err("parse should fail");
        assert!(object_error
            .to_string()
            .contains("module metadata must be an object"));

        let path_error = parse("include \"\\(a)\"; 0").expect_err("parse should fail");
        assert!(path_error
            .to_string()
            .contains("import path must be constant"));
    }

    #[test]
    fn evaluates_user_defined_functions_with_lexical_captures() {
        let query = parse("1 as $x | def foo: $x; 2 | foo").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1)]);
    }

    #[test]
    fn errors_on_undefined_variables() {
        let query = parse("$missing").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error.to_string().contains("$missing is not defined"));
    }

    #[test]
    fn evaluates_array_destructuring_bindings() {
        let query = parse(". as [$a, $b, {c: $c}] | $a + $b + $c").expect("query should parse");
        let input = Value::from_json(serde_json::json!([2, 3, {"c": 4, "d": 5}]))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(9)]);
    }

    #[test]
    fn evaluates_jq_object_binding_compat_cases() {
        let query =
            parse(". as {as: $kw, \"str\": $str, (\"e\"+\"x\"+\"p\"): $exp} | [$kw, $str, $exp]")
                .expect("query should parse");
        let input = Value::from_json(serde_json::json!({"as": 1, "str": 2, "exp": 3}))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse")]
        );

        let query = parse(". as {$a, $b:[$c, $d]}| [$a, $b, $c, $d]").expect("query should parse");
        let input = Value::from_json(serde_json::json!({"a": 1, "b": [2, {"d": 3}]}))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1, [2, {"d": 3}], 2, {"d": 3}]))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_jq_object_construction_abs_length_and_location_compat() {
        let query = parse(
            "1 as $x | \"2\" as $y | \"3\" as $z | { $x, as, $y: 4, ($z): 5, if: 6, foo: 7 }",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!({"as": 8})).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({
                "x": 1,
                "as": 8,
                "2": 4,
                "3": 5,
                "if": 6,
                "foo": 7
            }))
            .expect("value should parse")]
        );

        let query = parse("{ a, $__loc__, c }").expect("query should parse");
        let input = Value::from_json(serde_json::json!({
            "a": [1, 2, 3],
            "b": "foo",
            "c": {"hi": "hey"}
        }))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({
                "a": [1, 2, 3],
                "__loc__": {"file": "<top-level>", "line": 1},
                "c": {"hi": "hey"}
            }))
            .expect("value should parse")]
        );

        let query = parse("abs, length").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([-10, "abc", -1.1])).expect("value should parse"),
        )
        .expect_err("mixed input should fail per current query shape");
        assert!(values.to_string().contains("abs is not defined"));

        let query = parse("abs").expect("query should parse");
        let values = evaluate(&query, &Value::String("abc".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("abc".to_string())]);

        let query = parse("length").expect("query should parse");
        let values = evaluate(&query, &Value::Float(-1.1))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Float(1.1)]);
    }

    #[test]
    fn evaluates_missing_destructured_values_as_null() {
        let query = parse(". as {a: [$x], b: $b} | [$x, $b]").expect("query should parse");
        let input = Value::from_json(serde_json::json!({"b": 1})).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![Value::Null, Value::Integer(1)])]
        );
    }

    #[test]
    fn evaluates_destructuring_alternative_bindings() {
        let query = parse(
            ".[] as {$a, b: [$c, {$d}]} ?// [$a, {$b}, $e] ?// $f | [$a, $b, $c, $d, $e, $f]",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!([
            {"a": 1, "b": [2, {"d": 3}]},
            [4, {"b": 5, "c": 6}, 7, 8, 9],
            "foo"
        ]))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Array(vec![
                    Value::Integer(1),
                    Value::Null,
                    Value::Integer(2),
                    Value::Integer(3),
                    Value::Null,
                    Value::Null,
                ]),
                Value::Array(vec![
                    Value::Integer(4),
                    Value::Integer(5),
                    Value::Null,
                    Value::Null,
                    Value::Integer(7),
                    Value::Null,
                ]),
                Value::Array(vec![
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::String("foo".to_string()),
                ]),
            ]
        );
    }

    #[test]
    fn evaluates_destructuring_alternative_fallback_on_later_errors() {
        let query =
            parse(".[] as [$a] ?// [$b] | if $a != null then error(\"boom\") else {$a, $b} end")
                .expect("query should parse");
        let input = Value::from_json(serde_json::json!([[3]])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({"a": null, "b": 3}))
                .expect("value should parse")]
        );
    }

    #[test]
    fn reports_final_destructuring_alternative_errors() {
        let query = parse(
            ".[] as [$a] ?// [$b] | if $a != null then error(\"boom\") else error(\"fallback boom\") end",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!([[3]])).expect("value should parse");
        let error = evaluate(&query, &input).expect_err("query should fail");
        assert!(error.to_string().contains("fallback boom"));
    }

    #[test]
    fn errors_on_invalid_non_null_destructuring_shape() {
        let query = parse(". as [$a] | $a").expect("query should parse");
        let input = Value::from_json(serde_json::json!({"a": 1})).expect("value should parse");
        let error = evaluate(&query, &input).expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("cannot index object with number"));
    }

    #[test]
    fn evaluates_postfix_dynamic_lookup_with_stage_scope() {
        let input = Value::from_json(serde_json::json!({
            "posts": [
                {"title": "First post", "author": "anon"},
                {"title": "A well-written article", "author": "person1"}
            ],
            "realnames": {
                "anon": "Anonymous Coward",
                "person1": "Person McPherson"
            }
        }))
        .expect("value should parse");
        let query =
            parse(".realnames as $names | .posts[] | {title: .title, author: $names[.author]}")
                .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(
                    serde_json::json!({"title": "First post", "author": "Anonymous Coward"})
                )
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "title": "A well-written article",
                    "author": "Person McPherson"
                }))
                .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_postfix_field_access_on_parenthesized_queries() {
        let input = Value::from_json(serde_json::json!({"user": {"name": "alice"}}))
            .expect("value should parse");
        let query = parse("(.user).name").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("alice".to_string())]);
    }

    #[test]
    fn evaluates_path_builtin() {
        let exact_query =
            parse("path(.a), path(.a[0]), path(.a | .b), path(.a?), path(.a[1:2]), path(.[1,2])")
                .expect("query should parse");
        let exact_values = evaluate(&exact_query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            exact_values,
            vec![
                Value::from_json(serde_json::json!(["a"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 0])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", "b"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", {"start": 1, "end": 2}]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([1])).expect("value should parse"),
                Value::from_json(serde_json::json!([2])).expect("value should parse"),
            ]
        );

        let pattern_query = parse("path(.a[].b)").expect("query should parse");
        let pattern_input = Value::from_json(serde_json::json!({
            "a": [{"b": 1}, {"b": 2}]
        }))
        .expect("value should parse");
        let pattern_values = evaluate(&pattern_query, &pattern_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            pattern_values,
            vec![
                Value::from_json(serde_json::json!(["a", 0, "b"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 1, "b"])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn errors_on_invalid_path_builtin_expressions() {
        let input =
            Value::from_json(serde_json::json!({"a": [{"b": 0}]})).expect("value should parse");
        let query = parse(
            "try path(length) catch ., \
             try path(.a | map(select(.b == 0))) catch ., \
             try path(.a | map(select(.b == 0)) | .[0]) catch ., \
             try path(.a | map(select(.b == 0)) | .c) catch ., \
             try path(.a | map(select(.b == 0)) | .[]) catch .",
        )
        .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("Invalid path expression with result 1".to_string()),
                Value::String("Invalid path expression with result [{\"b\":0}]".to_string()),
                Value::String(
                    "Invalid path expression near attempt to access element 0 of [{\"b\":0}]"
                        .to_string()
                ),
                Value::String(
                    "Invalid path expression near attempt to access element \"c\" of [{\"b\":0}]"
                        .to_string()
                ),
                Value::String(
                    "Invalid path expression near attempt to iterate through [{\"b\":0}]"
                        .to_string()
                ),
            ]
        );
    }

    #[test]
    fn evaluates_nested_dynamic_path_builtin_queries() {
        let query = parse("path(.a[path(.b)[0]])").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!({"a": {"b": 0}})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!(["a", "b"])).expect("value should parse"),]
        );
    }

    #[test]
    fn path_last_uses_negative_indices() {
        let query = parse("path(last), try pick(last) catch .").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([-1])).expect("value should parse"),
                Value::String("Out of bounds negative array index".to_string()),
            ]
        );
    }

    #[test]
    fn evaluates_reduce_sum() {
        let input = Value::from_json(serde_json::json!([1, 2, 3, 4])).expect("value should parse");
        let query = parse("reduce .[] as $item (0; . + $item)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(10)]);
    }

    #[test]
    fn evaluates_reduce_with_destructuring_pattern() {
        let input = Value::from_json(serde_json::json!([[1, 2], [3, 4], [5, 6]]))
            .expect("value should parse");
        let query = parse("reduce .[] as [$x, $y] (0; . + $x + $y)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(21)]);
    }

    #[test]
    fn evaluates_reduce_with_outer_bindings() {
        let input = Value::from_json(serde_json::json!({"factor": 10, "items": [1, 2, 3]}))
            .expect("value should parse");
        let query = parse(".factor as $f | reduce .items[] as $item (0; . + ($item * $f))")
            .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(60)]);
    }

    #[test]
    fn evaluates_reduce_with_multi_output_init() {
        let query = parse("reduce [1, 2][] as $x (0, 10; . + $x)").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(3), Value::Integer(13)]);
    }

    #[test]
    fn evaluates_unary_minus_before_reduce_expression() {
        let input = Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let query = parse("[-reduce -.[] as $x (0; . + $x)]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([6])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_range_builtin() {
        let query = parse("range(3), range(1; 4), range(0; 10; 3), range(0; 1; 0.25)")
            .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(0),
                Value::Integer(3),
                Value::Integer(6),
                Value::Integer(9),
                Value::Integer(0),
                Value::Float(0.25),
                Value::Float(0.5),
                Value::Float(0.75),
            ]
        );
    }

    #[test]
    fn evaluates_range_with_multi_output_arguments() {
        let query =
            parse("[range(0,1;3,4)], [range(0,1;4,5;1,2)], [range(0,1,2;4,3,2;2,3)], [range(3,5)]")
                .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([0, 1, 2, 0, 1, 2, 3, 1, 2, 1, 2, 3]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    0, 1, 2, 3, 0, 2, 0, 1, 2, 3, 4, 0, 2, 4, 1, 2, 3, 1, 3, 1, 2, 3, 4, 1, 3
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    0, 2, 0, 3, 0, 2, 0, 0, 0, 1, 3, 1, 1, 1, 1, 1, 2, 2, 2, 2
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([0, 1, 2, 0, 1, 2, 3, 4]))
                    .expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_range_with_zero_step_as_empty() {
        let query = parse("range(0; 3; 0)").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn evaluates_limit_builtin() {
        let input = Value::from_json(serde_json::json!({"n": 2, "items": [1, 2, 3]}))
            .expect("value should parse");
        let query = parse("limit(.n; .items[]), limit(1.1; .items[])").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(2),
            ]
        );
    }

    #[test]
    fn evaluates_limit_builtin_on_direct_ranges() {
        let query = parse("[limit(5,7; range(9))]").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6]))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn limit_short_circuits_after_requested_results() {
        let query = parse("[limit(0; error)], [limit(1; 1, error)]").expect("query should parse");
        let values = evaluate(&query, &Value::String("badness".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Array(Vec::new()),
                Value::Array(vec![Value::Integer(1)]),
            ]
        );
    }

    #[test]
    fn errors_on_negative_limit_count() {
        let query = parse("limit(-1; .items[])").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!({"items": [1, 2, 3]})).expect("value should parse");
        let error = evaluate(&query, &input).expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("limit doesn't support negative count"));
    }

    #[test]
    fn evaluates_generator_selection_builtins() {
        let query =
            parse("first(range(5)), last(range(5)), nth(2; range(5)), nth(range(3); range(10))")
                .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(0),
                Value::Integer(4),
                Value::Integer(2),
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(2),
            ]
        );
    }

    #[test]
    fn evaluates_unary_nth_builtin_on_arrays() {
        let query = parse("[range(10)] | nth(5)").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(5)]);
    }

    #[test]
    fn nth_short_circuits_after_requested_index() {
        let query = parse("nth(1; 0,1,error(\"foo\"))").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1)]);
    }

    #[test]
    fn evaluates_skip_generator_builtin() {
        let skip_query = parse("[skip(3; .[])]").expect("query should parse");
        let skip_input = Value::from_json(serde_json::json!([1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .expect("value should parse");
        let skip_values = evaluate(&skip_query, &skip_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            skip_values,
            vec![Value::from_json(serde_json::json!([4, 5, 6, 7, 8, 9]))
                .expect("value should parse")]
        );

        let multi_query = parse("[skip(0,2,3,4; .[])]").expect("query should parse");
        let multi_input =
            Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let multi_values = evaluate(&multi_query, &multi_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            multi_values,
            vec![Value::from_json(serde_json::json!([1, 2, 3, 3])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_generator_selection_empty_cases() {
        let query =
            parse("first(empty), last(empty), nth(10; range(5))").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn errors_on_negative_nth_indices() {
        let query = parse("nth(-1; range(5))").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("nth doesn't support negative indices"));
    }

    #[test]
    fn evaluates_while_builtin() {
        let query = parse("while(. < 3; . + 1)").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(0))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(0), Value::Integer(1), Value::Integer(2)]
        );
    }

    #[test]
    fn evaluates_while_with_branching_updates() {
        let query = parse("while(. < 2; . + 1, 10)").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(0))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(0), Value::Integer(1)]);
    }

    #[test]
    fn evaluates_repeat_builtin() {
        let query = parse("[repeat(.*2, error)?]").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(1))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([2])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_until_builtin() {
        let query = parse("until(. >= 5; . + 1)").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(0))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(5)]);
    }

    #[test]
    fn evaluates_until_with_branching_updates() {
        let query = parse("until(. >= 2; . + 1, 10)").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(0))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(2), Value::Integer(10), Value::Integer(10)]
        );
    }

    #[test]
    fn evaluates_recurse_builtin() {
        let query = parse("recurse").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!([1, [2, [3]], 4])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1, [2, [3]], 4])).expect("value should parse"),
                Value::Integer(1),
                Value::from_json(serde_json::json!([2, [3]])).expect("value should parse"),
                Value::Integer(2),
                Value::from_json(serde_json::json!([3])).expect("value should parse"),
                Value::Integer(3),
                Value::Integer(4),
            ]
        );
    }

    #[test]
    fn evaluates_recurse_with_custom_query() {
        let query = parse("recurse(.children[]) | .name").expect("query should parse");
        let input = Value::from_json(serde_json::json!({
            "name": "root",
            "children": [
                {
                    "name": "a",
                    "children": [
                        {"name": "a1", "children": []}
                    ]
                },
                {
                    "name": "b",
                    "children": []
                }
            ]
        }))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("root".to_string()),
                Value::String("a".to_string()),
                Value::String("a1".to_string()),
                Value::String("b".to_string()),
            ]
        );
    }

    #[test]
    fn evaluates_recurse_with_condition() {
        let query = parse("recurse(. * .; . < 20)").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(2))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(2), Value::Integer(4), Value::Integer(16)]
        );
    }

    #[test]
    fn evaluates_recurse_with_terminating_custom_query() {
        let query =
            parse("recurse(if . < 2 then . + 1 else empty end)").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(0))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(0), Value::Integer(1), Value::Integer(2)]
        );
    }

    #[test]
    fn evaluates_nested_definitions_with_outer_parameters() {
        let query = parse(
            "def range(init; upto; by): def _range: if (by > 0 and . < upto) or (by < 0 and . > upto) then ., ((.+by)|_range) else empty end; if init == upto then empty elif by == 0 then init else init|_range end; def while(cond; update): def _while: if cond then ., (update | _while) else empty end; _while; [range(0; 10; 3)], [1 | while(.<100; .*2)]",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([0, 3, 6, 9])).expect("value should parse"),
                Value::from_json(serde_json::json!([1, 2, 4, 8, 16, 32, 64]))
                    .expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_recursive_descent_alias() {
        let query = parse(".. | numbers").expect("query should parse");
        let input = Value::from_json(serde_json::json!({"a": 1, "b": {"c": 2}, "d": [3]}))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        );
    }

    #[test]
    fn evaluates_map_values_builtin() {
        let array_query = parse("map_values(. + 1)").expect("query should parse");
        let array_input = Value::from_json(serde_json::json!([1, 2])).expect("value should parse");
        let array_values = evaluate(&array_query, &array_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            array_values,
            vec![Value::from_json(serde_json::json!([2, 3])).expect("value should parse")]
        );

        let object_query = parse("map_values(empty)").expect("query should parse");
        let object_input =
            Value::from_json(serde_json::json!({"a": 1, "b": 2})).expect("value should parse");
        let object_values = evaluate(&object_query, &object_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            object_values,
            vec![Value::from_json(serde_json::json!({})).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_map_values_using_first_result_only() {
        let query = parse("map_values(., . + 10)").expect("query should parse");
        let input = Value::from_json(serde_json::json!([1, 2])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([1, 2])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_indices_builtin() {
        let string_query = parse("indices(\"aa\")").expect("query should parse");
        let string_values = evaluate(&string_query, &Value::String("aaaa".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            string_values,
            vec![Value::from_json(serde_json::json!([0, 1, 2])).expect("value should parse")]
        );

        let array_query = parse("indices([1, 1])").expect("query should parse");
        let array_input =
            Value::from_json(serde_json::json!([1, 1, 1])).expect("value should parse");
        let array_values = evaluate(&array_query, &array_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            array_values,
            vec![Value::from_json(serde_json::json!([0, 1])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_multi_output_index_builtins() {
        let query = parse("[(index(\",\",\"|\"), rindex(\",\",\"|\")), indices(\",\",\"|\")]")
            .expect("query should parse");
        let values = evaluate(
            &query,
            &Value::String("a,b|c,d,e||f,g,h,|,|,i,j".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([
                1,
                3,
                22,
                19,
                [1, 5, 7, 12, 14, 16, 18, 20, 22],
                [3, 9, 10, 17, 19]
            ]))
            .expect("value should parse")]
        );

        let subarray_values = evaluate(
            &parse("index([1,2]), rindex([1,2])").expect("query should parse"),
            &Value::from_json(serde_json::json!([0, 1, 2, 3, 1, 4, 2, 5, 1, 2, 6, 7]))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(subarray_values, vec![Value::Integer(1), Value::Integer(8)]);
    }

    #[test]
    fn evaluates_uppercase_index_builtins() {
        let values = evaluate(
            &parse("INDEX(.id)").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
                {"id": 1, "name": "c"}
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({
                "1": {"id": 1, "name": "c"},
                "2": {"id": 2, "name": "b"}
            }))
            .expect("value should parse")]
        );

        let values = evaluate(
            &parse("INDEX(range(3); ., . + 10)").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({
                "0": 0,
                "10": 0,
                "1": 1,
                "11": 1,
                "2": 2,
                "12": 2
            }))
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_getpath_builtin() {
        let input = Value::from_json(serde_json::json!({"a": {"b": 1}, "items": [10, 20, 30]}))
            .expect("value should parse");
        let query =
            parse("getpath([\"a\", \"b\"]), getpath([\"items\", -1]), getpath([\"missing\", 0])")
                .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(1), Value::Integer(30), Value::Null]
        );

        let values = evaluate(
            &parse("[getpath([\"a\", \"b\"], [\"a\", \"c\"])]").expect("query should parse"),
            &Value::from_json(serde_json::json!({"a":{"b":0,"c":1}})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([0, 1])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_env_special_variable() {
        let query = parse("$ENV.TEST_AQ_ENV").expect("query should parse");
        let value = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(value, vec![Value::Null]);
    }

    #[test]
    fn evaluates_setpath_builtin() {
        let query = parse(
            "(null | setpath([\"a\", \"b\"]; 1)), ([] | setpath([2]; 7)), ([1,2,3] | setpath([-1]; 9))",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"a": {"b": 1}})).expect("value should parse"),
                Value::from_json(serde_json::json!([null, null, 7])).expect("value should parse"),
                Value::from_json(serde_json::json!([1, 2, 9])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_setpath_with_multi_output_values() {
        let query = parse("setpath([\"a\"]; 1, 2)").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
                Value::from_json(serde_json::json!({"a": 2})).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn reports_contextual_setpath_path_component_errors() {
        let query = parse("setpath([[1]]; 1)").expect("query should parse");

        let array_error =
            evaluate(&query, &Value::Array(Vec::new())).expect_err("query should fail");
        assert_eq!(
            array_error.to_string(),
            "query error: Cannot update field at array index of array"
        );

        let object_error = evaluate(
            &query,
            &Value::from_json(serde_json::json!({})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert_eq!(
            object_error.to_string(),
            "query error: Cannot index object with array"
        );
    }

    #[test]
    fn evaluates_delpaths_builtin() {
        let query = parse(
            "({a:1,b:2,c:3} | delpaths([[\"b\"],[\"c\"]])), ([0,1,2,3] | delpaths([[1],[2]])), ({a:{b:1,c:2}} | delpaths([[\"a\",\"b\"]])), (1 | delpaths([[]]))",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
                Value::from_json(serde_json::json!([0, 3])).expect("value should parse"),
                Value::from_json(serde_json::json!({"a": {"c": 2}})).expect("value should parse"),
                Value::Null,
            ]
        );
    }

    #[test]
    fn evaluates_del_builtin() {
        let query = parse(
            "({a:1,b:2,c:3} | del(.b, .c)), ({items:[0,1,2,3]} | del(.items[1,2])), ({a:{b:1,c:2}} | del(.a.b)), ([0,1,2] | del(.[1]))",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
                Value::from_json(serde_json::json!({"items": [0, 3]})).expect("value should parse"),
                Value::from_json(serde_json::json!({"a": {"c": 2}})).expect("value should parse"),
                Value::from_json(serde_json::json!([0, 2])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_del_builtin_with_slice_paths() {
        let query =
            parse("[0,1,2,3,4,5,6,7] | del(.[2:4], .[0], .[-2:])").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([1, 4, 5])).expect("value should parse")]
        );
    }

    #[test]
    fn ignores_non_finite_del_paths() {
        let query = parse("del(.[nan], .[nan, nan])").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_assignment_expressions() {
        let query = parse(
            "({service:{port:8080},features:[\"alpha\",\"beta\"]} | .service.port = 8443 | .features[] |= ascii_upcase), ({a:1,b:2} | .a |= empty), (null | .metadata.labels.env = \"staging\"), ({count:2,name:null,items:[1,2,3]} | .count += 3 | .name //= \"unknown\" | .items[1] *= 5), ({enabled:false} | .enabled //= true), ({quota:10} | .quota /= 4), ({quota:10} | .quota %= 4), ({foo:2} | .foo += .foo)",
        )
        .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({
                    "service": {"port": 8443},
                    "features": ["ALPHA", "BETA"]
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({"b": 2})).expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "metadata": {"labels": {"env": "staging"}}
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "count": 5,
                    "name": "unknown",
                    "items": [1, 10, 3]
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({"enabled": true})).expect("value should parse"),
                Value::from_json(serde_json::json!({"quota": 2.5})).expect("value should parse"),
                Value::from_json(serde_json::json!({"quota": 2})).expect("value should parse"),
                Value::from_json(serde_json::json!({"foo": 4})).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_update_assignments_using_only_the_first_rhs_result() {
        let query = parse("(null | .a |= range(3)), (null | (.a, .b) |= range(3))")
            .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"a": 0})).expect("value should parse"),
                Value::from_json(serde_json::json!({"a": 0, "b": 0})).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_sparse_array_reduce_assignments_without_semantic_changes() {
        let query =
            parse("reduce range(5;2;-1) as $i ([]; .[$i] = $i)").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([null, null, null, 3, 4, 5]))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn sparse_array_reduce_assignments_preserve_negative_index_errors() {
        let query =
            parse("reduce range(-1;-4;-1) as $i ([]; .[$i] = $i)").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert_eq!(
            error.to_string(),
            "query error: Out of bounds negative array index"
        );
    }

    #[test]
    fn evaluates_sparse_array_reduce_assignments_with_tail_slice_fusion() {
        let query = parse("reduce range(6;2;-1) as $i ([]; .[$i] = $i) | .[3:]")
            .expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([3, 4, 5, 6])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_update_assignments_with_descending_array_paths() {
        let removed = evaluate(
            &parse("(.[] | select(. >= 2)) |= empty").expect("query should parse"),
            &Value::from_json(serde_json::json!([1, 5, 3, 0, 7])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            removed,
            vec![Value::from_json(serde_json::json!([1, 0])).expect("value should parse")]
        );

        let evens = evaluate(
            &parse(".[] |= select(. % 2 == 0)").expect("query should parse"),
            &Value::from_json(serde_json::json!([0, 1, 2, 3, 4, 5])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            evens,
            vec![Value::from_json(serde_json::json!([0, 2, 4])).expect("value should parse")]
        );

        let object = evaluate(
            &parse(".foo[1,4,2,3] |= empty").expect("query should parse"),
            &Value::from_json(serde_json::json!({"foo": [0, 1, 2, 3, 4, 5]}))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            object,
            vec![Value::from_json(serde_json::json!({"foo": [0, 5]})).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_array_slice_assignment_expressions() {
        let base = Value::from_json(serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7]))
            .expect("value should parse");

        let removed = evaluate(&parse(".[2:4] = []").expect("query should parse"), &base)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            removed,
            vec![Value::from_json(serde_json::json!([0, 1, 4, 5, 6, 7]))
                .expect("value should parse")]
        );

        let replaced = evaluate(
            &parse(".[2:4] = [\"a\",\"b\"], .[2:4] = [\"a\",\"b\",\"c\"]")
                .expect("query should parse"),
            &base,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            replaced,
            vec![
                Value::from_json(serde_json::json!([0, 1, "a", "b", 4, 5, 6, 7]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([0, 1, "a", "b", "c", 4, 5, 6, 7]))
                    .expect("value should parse"),
            ]
        );

        let nested = evaluate(
            &parse(".[2:4][1] = 9").expect("query should parse"),
            &Value::from_json(serde_json::json!([0, 1, 2, 3, 4, 5])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            nested,
            vec![Value::from_json(serde_json::json!([0, 1, 2, 9, 4, 5]))
                .expect("value should parse")]
        );

        let null_update = evaluate(
            &parse(".[1:3] = [\"a\",\"b\"]").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            null_update,
            vec![Value::from_json(serde_json::json!(["a", "b"])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_dynamic_array_slice_assignment_expressions() {
        let input =
            Value::from_json(serde_json::json!([0, 1, 2, 3, 4, 5])).expect("value should parse");
        let query = parse(".[1.5:3.5] = ([\"x\"], [\"x\", \"y\"])").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([0, "x", 4, 5])).expect("value should parse"),
                Value::from_json(serde_json::json!([0, "x", "y", 4, 5]))
                    .expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_recursive_update_assignments_through_recurse_paths() {
        let query = parse(
            "(.. | select(type == \"object\" and has(\"b\") and (.b | type) == \"array\") | .b) |= .[0]",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!({"a": {"b": [1, {"b": 3}]}}))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({"a": {"b": 1}})).expect("value should parse")]
        );
    }

    #[test]
    fn errors_on_invalid_path_updates() {
        let getpath_query = parse("getpath([0])").expect("query should parse");
        let getpath_error =
            evaluate(&getpath_query, &Value::Integer(1)).expect_err("query should fail");
        assert!(getpath_error
            .to_string()
            .contains("Cannot index number with number (0)"));

        let setpath_query = parse("setpath([-1]; 9)").expect("query should parse");
        let setpath_error = evaluate(&setpath_query, &Value::Null).expect_err("query should fail");
        assert!(setpath_error
            .to_string()
            .contains("Out of bounds negative array index"));

        let nan_assign_query = parse(".[nan] = 9").expect("query should parse");
        let nan_assign_error = evaluate(&nan_assign_query, &Value::Array(vec![Value::Integer(1)]))
            .expect_err("query should fail");
        assert!(nan_assign_error
            .to_string()
            .contains("Cannot set array element at NaN index"));

        let delpaths_query = parse("delpaths([[\"a\", \"x\"]])").expect("query should parse");
        let delpaths_error = evaluate(
            &delpaths_query,
            &Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(delpaths_error
            .to_string()
            .contains("cannot delete fields from integer"));
    }

    #[test]
    fn evaluates_paths_builtin() {
        let input = Value::from_json(serde_json::json!({"a": [1, {"b": 2}], "c": 3}))
            .expect("value should parse");
        let query =
            parse("paths, paths(type == \"number\"), leaf_paths").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!(["a"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 0])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 1])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 1, "b"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["c"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 0])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 1, "b"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["c"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 0])).expect("value should parse"),
                Value::from_json(serde_json::json!(["a", 1, "b"])).expect("value should parse"),
                Value::from_json(serde_json::json!(["c"])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_paths_on_scalars_as_empty() {
        let query =
            parse("paths, paths(type == \"number\"), leaf_paths").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(1))
            .expect("query should run")
            .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn evaluates_walk_builtin() {
        let query =
            parse("walk(if type == \"number\" then . + 1 else . end)").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!([1, {"a": 2}, [3]])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([2, {"a": 3}, [4]]))
                .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_walk_with_object_key_transforms() {
        let query = parse(
            "walk(if type == \"object\" then with_entries({key: (.key | ascii_upcase), value: .value}) else . end)",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!({"a": 1, "b": {"c": 2}}))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!({"A": 1, "B": {"C": 2}}))
                .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_walk_with_multi_output_children() {
        let query = parse("walk(if type == \"number\" then ., . + 10 else . end)")
            .expect("query should parse");
        let array_values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            array_values,
            vec![Value::from_json(serde_json::json!([1, 11, 2, 12])).expect("value should parse")]
        );

        let object_values = evaluate(
            &query,
            &Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            object_values,
            vec![Value::from_json(serde_json::json!({"a": 1})).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_walk_empty_results() {
        let query = parse("walk(empty)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn evaluates_transpose_builtin() {
        let query = parse("transpose").expect("query should parse");
        let rectangular = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[1, 2, 3], [4, 5, 6]]))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            rectangular,
            vec![
                Value::from_json(serde_json::json!([[1, 4], [2, 5], [3, 6]]))
                    .expect("value should parse")
            ]
        );

        let ragged = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[1, 2], [3], null])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            ragged,
            vec![
                Value::from_json(serde_json::json!([[1, 3, null], [2, null, null]]))
                    .expect("value should parse")
            ]
        );

        let empty = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[], []])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            empty,
            vec![Value::from_json(serde_json::json!([])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_avg_and_median_builtins() {
        let query = parse("avg, median").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2, 3, 4])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Float(2.5), Value::Float(2.5)]);

        let odd_query = parse("median").expect("query should parse");
        let odd_values = evaluate(
            &odd_query,
            &Value::from_json(serde_json::json!([3, 1, 2])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(odd_values, vec![Value::Integer(2)]);

        let empty_values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(empty_values, vec![Value::Null, Value::Null]);
    }

    #[test]
    fn evaluates_histogram_builtin() {
        let query = parse("histogram(2)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2, 3, 4])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([
                {"start": 1, "end": 2.5, "count": 2},
                {"start": 2.5, "end": 4, "count": 2}
            ]))
            .expect("value should parse")]
        );

        let constant_values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([5, 5, 5])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            constant_values,
            vec![Value::from_json(serde_json::json!([
                {"start": 5, "end": 5, "count": 3}
            ]))
            .expect("value should parse")]
        );

        let empty_values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            empty_values,
            vec![Value::from_json(serde_json::json!([])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_take_and_skip_builtins() {
        let query = parse("take(2), skip(2), take(10), skip(10)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2, 3, 4])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
                Value::from_json(serde_json::json!([3, 4])).expect("value should parse"),
                Value::from_json(serde_json::json!([1, 2, 3, 4])).expect("value should parse"),
                Value::from_json(serde_json::json!([])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn errors_on_invalid_avg_median_take_and_skip_inputs() {
        let avg_query = parse("avg").expect("query should parse");
        let avg_error =
            evaluate(&avg_query, &Value::String("x".to_string())).expect_err("query should fail");
        assert!(avg_error
            .to_string()
            .contains("avg is not defined for string"));

        let median_query = parse("median").expect("query should parse");
        let median_error = evaluate(
            &median_query,
            &Value::from_json(serde_json::json!([1, "x"])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(median_error
            .to_string()
            .contains("median is not defined for string"));

        let take_query = parse("take(1.5)").expect("query should parse");
        let take_error = evaluate(
            &take_query,
            &Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(take_error
            .to_string()
            .contains("take count must be a non-negative integer"));

        let skip_query = parse("skip(-1)").expect("query should parse");
        let skip_error = evaluate(
            &skip_query,
            &Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(skip_error
            .to_string()
            .contains("skip doesn't support negative count"));
    }

    #[test]
    fn errors_on_invalid_histogram_inputs() {
        let histogram_query = parse("histogram(0)").expect("query should parse");
        let zero_bins_error = evaluate(
            &histogram_query,
            &Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(zero_bins_error
            .to_string()
            .contains("histogram bins must be a positive integer"));

        let fractional_bins_query = parse("histogram(1.5)").expect("query should parse");
        let fractional_bins_error = evaluate(
            &fractional_bins_query,
            &Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(fractional_bins_error
            .to_string()
            .contains("histogram count must be a non-negative integer"));

        let value_error = evaluate(
            &parse("histogram(2)").expect("query should parse"),
            &Value::from_json(serde_json::json!([1, "x"])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(value_error
            .to_string()
            .contains("histogram is not defined for string"));
    }

    #[test]
    fn evaluates_aq_extension_aliases_and_grouping_helpers() {
        let to_number_query = parse("to_number").expect("query should parse");
        let to_number_values = evaluate(&to_number_query, &Value::String("42".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(to_number_values, vec![Value::Integer(42)]);

        let to_bool_query = parse("to_bool").expect("query should parse");
        let to_bool_values = evaluate(
            &to_bool_query,
            &Value::from_json(serde_json::json!(["true", " FALSE ", "1", 0, true]))
                .expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(to_bool_values
            .to_string()
            .contains("to_bool is not defined for array"));

        let scalar_bool_values = evaluate(&to_bool_query, &Value::String(" FALSE ".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(scalar_bool_values, vec![Value::Bool(false)]);

        let toboolean_query = parse("toboolean").expect("query should parse");
        let jq_bool_values = evaluate(&toboolean_query, &Value::String("false".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(jq_bool_values, vec![Value::Bool(false)]);

        let columns_query = parse("columns").expect("query should parse");
        let columns_values = evaluate(
            &columns_query,
            &Value::from_json(serde_json::json!([
                {"b": 1, "a": 2},
                {"c": 3, "a": 4},
                null
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            columns_values,
            vec![Value::from_json(serde_json::json!(["b", "a", "c"])).expect("value should parse")]
        );

        let stats_query = parse("stddev, percentile(0), percentile(50), percentile(100)")
            .expect("query should parse");
        let stats_values = evaluate(
            &stats_query,
            &Value::from_json(serde_json::json!([1, 2, 3, 4])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            stats_values,
            vec![
                Value::Float((1.25_f64).sqrt()),
                Value::Integer(1),
                Value::Float(2.5),
                Value::Integer(4),
            ]
        );

        let input = Value::from_json(serde_json::json!([
            {"a": 2, "name": "x"},
            {"a": 1, "name": "y"},
            {"a": 2, "name": "z"}
        ]))
        .expect("value should parse");
        let query =
            parse("uniq_by(.a), sort_by_desc(.a), count_by(.a)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([
                    {"a": 1, "name": "y"},
                    {"a": 2, "name": "x"}
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    {"a": 2, "name": "x"},
                    {"a": 2, "name": "z"},
                    {"a": 1, "name": "y"}
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!([
                    {"key": [1], "count": 1},
                    {"key": [2], "count": 2}
                ]))
                .expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_rename_extension() {
        let input = Value::from_json(serde_json::json!({
            "old": 1,
            "new": 2,
            "keep": 3,
            "items": [{"old": 4, "keep": 5}, {"keep": 6}]
        }))
        .expect("value should parse");
        let query = parse(
            "rename(.old; \"renamed\"), rename(.items[].old; \"renamed\"), rename(.missing; \"x\")",
        )
        .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({
                    "renamed": 1,
                    "new": 2,
                    "keep": 3,
                    "items": [{"old": 4, "keep": 5}, {"keep": 6}]
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "old": 1,
                    "new": 2,
                    "keep": 3,
                    "items": [{"renamed": 4, "keep": 5}, {"keep": 6}]
                }))
                .expect("value should parse"),
                input,
            ]
        );
    }

    #[test]
    fn evaluates_pick_and_omit_extensions() {
        let input = Value::from_json(serde_json::json!({
            "a": 1,
            "b": 2,
            "c": {"d": 3, "e": 4},
            "items": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]
        }))
        .expect("value should parse");
        let query =
            parse("pick(.a, .c.d, .items[].x), omit(.b, .c.e, .items[].y), ({} | pick(.missing))")
                .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({
                    "a": 1,
                    "c": {"d": 3},
                    "items": [{"x": 1}, {"x": 3}]
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "a": 1,
                    "c": {"d": 3},
                    "items": [{"x": 1}, {"x": 3}]
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({"missing": null})).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_structural_aq_extension_builtins() {
        let input = Value::from_json(serde_json::json!({
            "base": {
                "service": {"name": "api", "port": 8080},
                "flags": [1, null, 2],
                "meta": {"owner": null}
            },
            "overlay": {
                "service": {"port": 8443},
                "meta": {"team": "platform"},
                "extra": null
            },
            "merge_inputs": [
                {"a": {"x": 1}, "b": null},
                {"a": {"y": 2}, "c": 3}
            ],
            "clean": {
                "z": null,
                "items": [1, null, {"keep": 1, "drop": null}],
                "obj": {"keep": {"nested": 1, "drop": null}, "drop": null}
            }
        }))
        .expect("value should parse");
        let query = parse(
            "(. as $doc | $doc.base | merge($doc.overlay)), (. as $doc | $doc.base | merge($doc.overlay; true)), (.merge_inputs | merge_all(true)), (.clean | drop_nulls), (.clean | drop_nulls(true))",
        )
        .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({
                    "service": {"port": 8443},
                    "flags": [1, null, 2],
                    "meta": {"team": "platform"},
                    "extra": null
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "service": {"name": "api", "port": 8443},
                    "flags": [1, null, 2],
                    "meta": {"owner": null, "team": "platform"},
                    "extra": null
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "a": {"x": 1, "y": 2},
                    "b": null,
                    "c": 3
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "items": [1, null, {"keep": 1, "drop": null}],
                    "obj": {"keep": {"nested": 1, "drop": null}, "drop": null}
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "items": [1, {"keep": 1}],
                    "obj": {"keep": {"nested": 1}}
                }))
                .expect("value should parse"),
            ]
        );

        let mut nested_unsorted = IndexMap::new();
        nested_unsorted.insert("d".to_string(), Value::Integer(4));
        nested_unsorted.insert("c".to_string(), Value::Integer(3));
        let mut unsorted = IndexMap::new();
        unsorted.insert("b".to_string(), Value::Object(nested_unsorted));
        unsorted.insert("a".to_string(), Value::Integer(1));
        let input = Value::Object(unsorted);

        let query = parse("sort_keys, sort_keys(true)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        let mut shallow_nested = IndexMap::new();
        shallow_nested.insert("d".to_string(), Value::Integer(4));
        shallow_nested.insert("c".to_string(), Value::Integer(3));
        let mut shallow_sorted = IndexMap::new();
        shallow_sorted.insert("a".to_string(), Value::Integer(1));
        shallow_sorted.insert("b".to_string(), Value::Object(shallow_nested));
        let mut recursive_nested = IndexMap::new();
        recursive_nested.insert("c".to_string(), Value::Integer(3));
        recursive_nested.insert("d".to_string(), Value::Integer(4));
        let mut recursive_sorted = IndexMap::new();
        recursive_sorted.insert("a".to_string(), Value::Integer(1));
        recursive_sorted.insert("b".to_string(), Value::Object(recursive_nested));
        assert_eq!(
            values,
            vec![
                Value::Object(shallow_sorted),
                Value::Object(recursive_sorted),
            ]
        );
    }

    #[test]
    fn evaluates_metadata_aq_extension_builtins() {
        let input = Value::Tagged {
            tag: "!Old".to_string(),
            value: Box::new(
                Value::from_json(serde_json::json!({
                    "name": "alice",
                    "attributes": {"id": "42", "role": "admin"}
                }))
                .expect("value should parse"),
            ),
        };
        let query =
            parse("yaml_tag, yaml_tag(\"!Thing\"), yaml_tag(null), xml_attr, xml_attr(\"id\")")
                .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("!Old".to_string()),
                Value::Tagged {
                    tag: "!Thing".to_string(),
                    value: Box::new(
                        Value::from_json(serde_json::json!({
                            "name": "alice",
                            "attributes": {"id": "42", "role": "admin"}
                        }))
                        .expect("value should parse"),
                    ),
                },
                Value::from_json(serde_json::json!({
                    "name": "alice",
                    "attributes": {"id": "42", "role": "admin"}
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({"id": "42", "role": "admin"}))
                    .expect("value should parse"),
                Value::String("42".to_string()),
            ]
        );

        let csv_query =
            parse("(.rows | csv_header), (. as $doc | $doc.rows[1] | csv_header($doc.rows[0]))")
                .expect("query should parse");
        let csv_input = Value::from_json(serde_json::json!({
            "rows": [
                ["name", "role"],
                ["alice", "admin"],
                ["bob", "ops"]
            ]
        }))
        .expect("value should parse");
        let values = evaluate(&csv_query, &csv_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([
                    {"name": "alice", "role": "admin"},
                    {"name": "bob", "role": "ops"}
                ]))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({"name": "alice", "role": "admin"}))
                    .expect("value should parse"),
            ]
        );
    }

    #[test]
    fn errors_on_invalid_pick_and_omit_paths() {
        let pick_query = parse("pick(length)").expect("query should parse");
        let pick_error = evaluate(
            &pick_query,
            &Value::from_json(serde_json::json!({"items": [1, 2]})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(pick_error.to_string().contains("Invalid path expression"));

        let omit_query = parse("omit(length)").expect("query should parse");
        let omit_error = evaluate(&omit_query, &Value::Null).expect_err("query should fail");
        assert!(omit_error.to_string().contains("Invalid path expression"));
    }

    #[test]
    fn errors_on_invalid_aq_extension_grouping_inputs() {
        let sort_by_desc_query = parse("sort_by_desc(.a)").expect("query should parse");
        let sort_by_desc_error = evaluate(
            &sort_by_desc_query,
            &Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(sort_by_desc_error
            .to_string()
            .contains("sort_by_desc is not defined for object"));

        let to_bool_query = parse("to_bool").expect("query should parse");
        let to_bool_error = evaluate(&to_bool_query, &Value::String("maybe".to_string()))
            .expect_err("query should fail");
        assert!(to_bool_error
            .to_string()
            .contains("to_bool cannot parse string"));

        let toboolean_query = parse("toboolean").expect("query should parse");
        let toboolean_error = evaluate(&toboolean_query, &Value::String(" FALSE ".to_string()))
            .expect_err("query should fail");
        assert!(toboolean_error
            .to_string()
            .contains("string (\" FALSE \") cannot be parsed as a boolean"));

        let columns_query = parse("columns").expect("query should parse");
        let columns_error = evaluate(
            &columns_query,
            &Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(columns_error
            .to_string()
            .contains("columns expects objects or arrays of objects"));

        let merge_all_query = parse("merge_all").expect("query should parse");
        let merge_all_error = evaluate(
            &merge_all_query,
            &Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(merge_all_error
            .to_string()
            .contains("merge_all expects an array"));

        let merge_all_empty_error =
            evaluate(&merge_all_query, &Value::Array(Vec::new())).expect_err("query should fail");
        assert!(merge_all_empty_error
            .to_string()
            .contains("merge_all expects a non-empty array"));

        let yaml_tag_query = parse("yaml_tag(1)").expect("query should parse");
        let yaml_tag_error =
            evaluate(&yaml_tag_query, &Value::Integer(1)).expect_err("query should fail");
        assert!(yaml_tag_error
            .to_string()
            .contains("yaml_tag expects a string tag or null"));

        let xml_attr_query = parse("xml_attr(\"id\")").expect("query should parse");
        let xml_attr_error = evaluate(
            &xml_attr_query,
            &Value::from_json(serde_json::json!({"attributes": []})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(xml_attr_error
            .to_string()
            .contains("xml_attr expects `.attributes` to be an object"));

        let csv_header_query = parse("csv_header").expect("query should parse");
        let csv_header_error = evaluate(
            &csv_header_query,
            &Value::from_json(serde_json::json!([["name", "name"], ["alice", "admin"]]))
                .expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(csv_header_error
            .to_string()
            .contains("csv_header requires unique header names"));

        let csv_width_error = evaluate(
            &parse("csv_header([\"name\"])").expect("query should parse"),
            &Value::from_json(serde_json::json!(["alice", "admin"])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(csv_width_error
            .to_string()
            .contains("csv_header requires row width 2 to match header width 1"));

        let rename_query = parse("rename(.items[0]; \"x\")").expect("query should parse");
        let rename_error = evaluate(
            &rename_query,
            &Value::from_json(serde_json::json!({"items": [1, 2]})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(rename_error
            .to_string()
            .contains("rename requires exact field paths"));

        let stddev_query = parse("stddev").expect("query should parse");
        let stddev_error = evaluate(
            &stddev_query,
            &Value::from_json(serde_json::json!([1, "x"])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(stddev_error
            .to_string()
            .contains("stddev is not defined for string"));

        let percentile_query = parse("percentile(101)").expect("query should parse");
        let percentile_error = evaluate(
            &percentile_query,
            &Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(percentile_error
            .to_string()
            .contains("percentile expects a percentile between 0 and 100"));
    }

    #[test]
    fn errors_on_invalid_transpose_inputs() {
        let query = parse("transpose").expect("query should parse");

        let null_error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(null_error
            .to_string()
            .contains("transpose is not defined for null"));

        let row_error = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[1], {"a": 2}])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(row_error
            .to_string()
            .contains("cannot index object with number"));
    }

    #[test]
    fn evaluates_flatten_with_depth() {
        let query = parse("flatten(1), flatten(2), flatten(0)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[[[1]]], 2])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([[[1]], 2])).expect("value should parse"),
                Value::from_json(serde_json::json!([[1], 2])).expect("value should parse"),
                Value::from_json(serde_json::json!([[[[1]]], 2])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_flatten_with_multi_output_depths() {
        let query = parse("flatten(3,2,1)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([0, [1], [[2]], [[[3]]]]))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([0, 1, 2, 3])).expect("value should parse"),
                Value::from_json(serde_json::json!([0, 1, 2, [3]])).expect("value should parse"),
                Value::from_json(serde_json::json!([0, 1, [2], [[3]]]))
                    .expect("value should parse"),
            ]
        );
    }

    #[test]
    fn errors_on_invalid_flatten_depth() {
        let query = parse("flatten(-1)").expect("query should parse");
        let error = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[[1]]])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("flatten depth must not be negative"));

        let query = parse("flatten(\"x\")").expect("query should parse");
        let error = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[[1]]])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("flatten depth must be a number"));
    }

    #[test]
    fn evaluates_math_builtins() {
        let query = parse(
            "floor, ceil, round, fabs, sqrt, log, log2, log10, exp, exp2, sin, cos, tan, asin, acos, atan",
        )
        .expect("query should parse");
        let float_values = evaluate(&query, &Value::Float(1.2))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            float_values,
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(1),
                Value::Float(1.2),
                Value::Float(1.2_f64.sqrt()),
                Value::Float(1.2_f64.ln()),
                Value::Float(1.2_f64.log2()),
                Value::Float(1.2_f64.log10()),
                Value::Float(1.2_f64.exp()),
                Value::Float(1.2_f64.exp2()),
                Value::Float(1.2_f64.sin()),
                Value::Float(1.2_f64.cos()),
                Value::Float(1.2_f64.tan()),
                Value::Null,
                Value::Null,
                Value::Float(1.2_f64.atan()),
            ]
        );

        let negative_values = evaluate(&query, &Value::Float(-1.2))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            negative_values,
            vec![
                Value::Integer(-2),
                Value::Integer(-1),
                Value::Integer(-1),
                Value::Float(1.2),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Float((-1.2_f64).exp()),
                Value::Float((-1.2_f64).exp2()),
                Value::Float((-1.2_f64).sin()),
                Value::Float((-1.2_f64).cos()),
                Value::Float((-1.2_f64).tan()),
                Value::Null,
                Value::Null,
                Value::Float((-1.2_f64).atan()),
            ]
        );

        let object_products = evaluate(
            &parse(
                "{\"k\": {\"a\": 1, \"b\": 2}} * ., {\"k\": {\"a\": 1, \"b\": 2}, \"hello\": {\"x\": 1}} * ., {\"k\": {\"a\": 1, \"b\": 2}, \"hello\": 1} * .",
            )
            .expect("query should parse"),
            &Value::from_json(serde_json::json!({
                "k": {"a": 0, "c": 3},
                "hello": 1
            }))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            object_products,
            vec![
                Value::from_json(serde_json::json!({"k": {"a": 0, "b": 2, "c": 3}, "hello": 1}),)
                    .expect("value should parse"),
                Value::from_json(serde_json::json!({"k": {"a": 0, "b": 2, "c": 3}, "hello": 1}),)
                    .expect("value should parse"),
                Value::from_json(serde_json::json!({"k": {"a": 0, "b": 2, "c": 3}, "hello": 1}),)
                    .expect("value should parse"),
            ]
        );

        let nested_product = evaluate(
            &parse("{\"a\": {\"b\": 1}, \"c\": {\"d\": 2}, \"e\": 5} * .")
                .expect("query should parse"),
            &Value::from_json(serde_json::json!({
                "a": {"b": 2},
                "c": {"d": 3, "f": 9}
            }))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            nested_product,
            vec![Value::from_json(
                serde_json::json!({"a": {"b": 2}, "c": {"d": 3, "f": 9}, "e": 5}),
            )
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_jq_boolean_and_float_constant_builtins() {
        let toboolean_query = parse("map(toboolean)").expect("query should parse");
        let toboolean_values = evaluate(
            &toboolean_query,
            &Value::from_json(serde_json::json!(["false", "true", false, true]))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            toboolean_values,
            vec![
                Value::from_json(serde_json::json!([false, true, false, true]))
                    .expect("value should parse")
            ]
        );

        let toboolean_errors = evaluate(
            &parse(".[] | try toboolean catch .").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                null,
                0,
                "tru",
                "truee",
                "fals",
                "falsee",
                [],
                {}
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            toboolean_errors,
            vec![
                Value::String("null (null) cannot be parsed as a boolean".to_string()),
                Value::String("number (0) cannot be parsed as a boolean".to_string()),
                Value::String("string (\"tru\") cannot be parsed as a boolean".to_string()),
                Value::String("string (\"truee\") cannot be parsed as a boolean".to_string()),
                Value::String("string (\"fals\") cannot be parsed as a boolean".to_string()),
                Value::String("string (\"falsee\") cannot be parsed as a boolean".to_string()),
                Value::String("array ([]) cannot be parsed as a boolean".to_string()),
                Value::String("object ({}) cannot be parsed as a boolean".to_string()),
            ]
        );

        let values = evaluate(
            &parse("[(infinite, -infinite) % (1, -1, infinite)], [nan % 1, 1 % nan | isnan]")
                .expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([0, 0, 0, 0, 0, -1]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([true, true])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn errors_on_invalid_math_inputs() {
        let query = parse("floor").expect("query should parse");
        let error =
            evaluate(&query, &Value::String("x".to_string())).expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("floor is not defined for string"));

        let query = parse("log").expect("query should parse");
        let error =
            evaluate(&query, &Value::String("x".to_string())).expect_err("query should fail");
        assert!(error.to_string().contains("log is not defined for string"));
    }

    #[test]
    fn evaluates_pow_builtin() {
        let query =
            parse("pow(2; 3), pow(2; 0.5), pow(-1; 0.5), pow(0; -1)").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(8),
                Value::Float(2_f64.sqrt()),
                Value::Null,
                Value::Float(f64::MAX),
            ]
        );
    }

    #[test]
    fn evaluates_pow2_log2_round_without_semantic_changes() {
        let query = parse("[range(-5;6;1) | pow(2;.) | log2 | round]").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn pow2_log2_round_preserves_large_exponent_fallback_behavior() {
        let query = parse("pow(2;.) | log2 | round").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(2000))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1024)]);
    }

    #[test]
    fn clamps_or_nulls_domain_edge_math_results() {
        let query = parse("log, asin, acos").expect("query should parse");
        let zero_log = evaluate(&query, &Value::Integer(0))
            .expect("query should run")
            .into_vec();
        assert_eq!(zero_log[0], Value::Float(-f64::MAX));

        let out_of_domain = evaluate(&query, &Value::Integer(2))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            out_of_domain,
            vec![Value::Float(2_f64.ln()), Value::Null, Value::Null]
        );
    }

    #[test]
    fn evaluates_date_builtins() {
        let todate_query = parse("todate").expect("query should parse");
        let todate_values = evaluate(&todate_query, &Value::Float(1.9))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            todate_values,
            vec![Value::String("1970-01-01T00:00:01Z".to_string())]
        );

        let fromdate_query = parse("fromdate").expect("query should parse");
        let fromdate_values = evaluate(
            &fromdate_query,
            &Value::String("1970-01-01T00:00:00Z".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(fromdate_values, vec![Value::Integer(0)]);

        let to_datetime_query = parse("to_datetime").expect("query should parse");
        let to_datetime_values = evaluate(
            &to_datetime_query,
            &Value::String("1970-01-01 01:02:03".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            to_datetime_values,
            vec![Value::DateTime(
                chrono::DateTime::<chrono::Utc>::from_timestamp(3723, 0)
                    .expect("timestamp should be in range"),
            )]
        );

        let to_datetime_date_values =
            evaluate(&to_datetime_query, &Value::String("1970-01-02".to_string()))
                .expect("query should run")
                .into_vec();
        assert_eq!(
            to_datetime_date_values,
            vec![Value::DateTime(
                chrono::DateTime::<chrono::Utc>::from_timestamp(86_400, 0)
                    .expect("timestamp should be in range"),
            )]
        );

        let to_datetime_fromdate_query =
            parse("to_datetime | fromdate").expect("query should parse");
        let to_datetime_fromdate_values = evaluate(
            &to_datetime_fromdate_query,
            &Value::String("1970-01-01T01:00:00+01:00".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(to_datetime_fromdate_values, vec![Value::Integer(0)]);

        let gmtime_query = parse("gmtime, gmtime[5]").expect("query should parse");
        let gmtime_values = evaluate(&gmtime_query, &Value::Float(1_425_599_507.25))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            gmtime_values,
            vec![
                Value::from_json(serde_json::json!([2015, 2, 5, 23, 51, 47.25, 4, 63]))
                    .expect("value should parse"),
                Value::Float(47.25),
            ]
        );

        let strftime_query = parse("strftime(\"%Y-%m-%dT%H:%M:%SZ\")").expect("query should parse");
        let strftime_values = evaluate(
            &strftime_query,
            &Value::from_json(serde_json::json!([2015, 2, 5, 23, 51, 47, 4, 63]))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            strftime_values,
            vec![Value::String("2015-03-05T23:51:47Z".to_string())]
        );

        let numeric_strftime_values = evaluate(
            &parse("strftime(\"%A, %B %d, %Y\")").expect("query should parse"),
            &Value::Float(1_435_677_542.822_351),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            numeric_strftime_values,
            vec![Value::String("Tuesday, June 30, 2015".to_string())]
        );

        let short_strftime_values = evaluate(
            &strftime_query,
            &Value::from_json(serde_json::json!([2024, 2, 15])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            short_strftime_values,
            vec![Value::String("2024-03-15T00:00:00Z".to_string())]
        );

        let mktime_values = evaluate(
            &parse("mktime").expect("query should parse"),
            &Value::from_json(serde_json::json!([2024, 8, 21])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(mktime_values, vec![Value::Integer(1_726_876_800)]);

        let strptime_values = evaluate(
            &parse(
                "(strptime(\"%Y-%m-%dT%H:%M:%SZ\")), (strptime(\"%Y-%m-%dT%H:%M:%SZ\") | mktime)",
            )
            .expect("query should parse"),
            &Value::String("2015-03-05T23:51:47Z".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            strptime_values,
            vec![
                Value::from_json(serde_json::json!([2015, 2, 5, 23, 51, 47, 4, 63]))
                    .expect("value should parse"),
                Value::Integer(1_425_599_507),
            ]
        );

        let strflocaltime_values = evaluate(
            &parse("strflocaltime(\"\" | ., @uri)").expect("query should parse"),
            &Value::Integer(0),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            strflocaltime_values,
            vec![Value::String(String::new()), Value::String(String::new())]
        );
    }

    #[test]
    fn evaluates_now_builtin_in_current_range() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_secs_f64();
        let query = parse("now").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_secs_f64();
        let [Value::Float(value)] = values.as_slice() else {
            panic!("now should return one float")
        };
        assert!(*value >= before);
        assert!(*value <= after);
    }

    #[test]
    fn errors_on_invalid_date_inputs() {
        let todate_query = parse("todate").expect("query should parse");
        let todate_error = evaluate(&todate_query, &Value::Null).expect_err("query should fail");
        assert!(todate_error
            .to_string()
            .contains("todate is not defined for null"));

        let fromdate_query = parse("fromdate").expect("query should parse");
        let invalid_format_error = evaluate(&fromdate_query, &Value::String("x".to_string()))
            .expect_err("query should fail");
        assert!(invalid_format_error
            .to_string()
            .contains("date \"x\" does not match format"));

        let negative_time_error = evaluate(
            &fromdate_query,
            &Value::String("1969-12-31T23:59:59Z".to_string()),
        )
        .expect_err("query should fail");
        assert!(negative_time_error
            .to_string()
            .contains("invalid gmtime representation"));

        let to_datetime_query = parse("to_datetime").expect("query should parse");
        let invalid_format_error = evaluate(&to_datetime_query, &Value::String("x".to_string()))
            .expect_err("query should fail");
        assert!(invalid_format_error
            .to_string()
            .contains("to_datetime cannot parse string \"x\""));

        let invalid_type_error =
            evaluate(&to_datetime_query, &Value::Integer(1)).expect_err("query should fail");
        assert!(invalid_type_error
            .to_string()
            .contains("to_datetime is not defined for integer"));

        let strftime_error = evaluate(
            &parse("strftime(\"%Y-%m-%dT%H:%M:%SZ\")").expect("query should parse"),
            &Value::from_json(serde_json::json!(["a", 1, 2, 3, 4, 5, 6, 7]))
                .expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(strftime_error
            .to_string()
            .contains("strftime/1 requires parsed datetime inputs"));

        let strftime_format_error = evaluate(
            &parse("strftime([])").expect("query should parse"),
            &Value::Integer(0),
        )
        .expect_err("query should fail");
        assert!(strftime_format_error
            .to_string()
            .contains("strftime/1 requires a string format"));

        let mktime_error = evaluate(
            &parse("mktime").expect("query should parse"),
            &Value::from_json(serde_json::json!(["a", 1, 2, 3, 4, 5, 6, 7]))
                .expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(mktime_error
            .to_string()
            .contains("mktime requires parsed datetime inputs"));

        let strptime_error = evaluate(
            &parse("strptime(\"%Y-%m-%d\")").expect("query should parse"),
            &Value::Integer(1),
        )
        .expect_err("query should fail");
        assert!(strptime_error
            .to_string()
            .contains("strptime/1 requires string inputs"));

        let strflocaltime_error = evaluate(
            &parse("strflocaltime(\"%Y-%m-%dT%H:%M:%SZ\")").expect("query should parse"),
            &Value::from_json(serde_json::json!(["a", 1, 2, 3, 4, 5, 6, 7]))
                .expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(strflocaltime_error
            .to_string()
            .contains("strflocaltime/1 requires parsed datetime inputs"));

        let strflocaltime_format_error = evaluate(
            &parse("strflocaltime({})").expect("query should parse"),
            &Value::Integer(0),
        )
        .expect_err("query should fail");
        assert!(strflocaltime_format_error
            .to_string()
            .contains("strflocaltime/1 requires a string format"));
    }

    #[test]
    fn evaluates_combinations_builtin() {
        let query =
            parse("combinations, combinations(2), combinations(0)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([[1, 2], {"a": 3, "b": 4}]))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1, 3])).expect("value should parse"),
                Value::from_json(serde_json::json!([1, 4])).expect("value should parse"),
                Value::from_json(serde_json::json!([2, 3])).expect("value should parse"),
                Value::from_json(serde_json::json!([2, 4])).expect("value should parse"),
                Value::from_json(serde_json::json!([[1, 2], [1, 2]])).expect("value should parse"),
                Value::from_json(serde_json::json!([[1, 2], {"a": 3, "b": 4}]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([{"a": 3, "b": 4}, [1, 2]]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([{"a": 3, "b": 4}, {"a": 3, "b": 4}]))
                    .expect("value should parse"),
                Value::from_json(serde_json::json!([])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_combinations_edge_cases() {
        let query = parse("combinations(0.5), combinations(-1)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1])).expect("value should parse"),
                Value::from_json(serde_json::json!([2])).expect("value should parse"),
                Value::from_json(serde_json::json!([])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn errors_on_invalid_combinations_inputs() {
        let query = parse("combinations").expect("query should parse");
        let error = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("Cannot iterate over number (1)"));

        let query = parse("combinations(null)").expect("query should parse");
        let error = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("combinations count must be numeric"));
    }

    #[test]
    fn evaluates_bsearch_builtin() {
        let query =
            parse("bsearch(3), bsearch(4), bsearch(0), bsearch(6)").expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 3, 5])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(1),
                Value::Integer(-3),
                Value::Integer(-1),
                Value::Integer(-4),
            ]
        );

        let multi_values = evaluate(
            &parse("bsearch(0,1,2,3,4)").expect("query should parse"),
            &Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            multi_values,
            vec![
                Value::Integer(-1),
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(-4),
            ]
        );

        let duplicate_query = parse("bsearch(2)").expect("query should parse");
        let duplicate_values = evaluate(
            &duplicate_query,
            &Value::from_json(serde_json::json!([1, 2, 2, 2, 3])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(duplicate_values, vec![Value::Integer(2)]);

        let string_error = evaluate(
            &parse("try [\"OK\", bsearch(0)] catch [\"KO\", .]").expect("query should parse"),
            &Value::String("aa".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            string_error,
            vec![Value::from_json(serde_json::json!([
                "KO",
                "string (\"aa\") cannot be searched from"
            ]),)
            .expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_foreach_with_default_extract() {
        let input = Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let query = parse("foreach .[] as $x (0; . + $x)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(1), Value::Integer(3), Value::Integer(6)]
        );
    }

    #[test]
    fn evaluates_foreach_with_explicit_extract() {
        let input = Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let query = parse("foreach .[] as $x (0; . + $x; . * 10)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(10), Value::Integer(30), Value::Integer(60)]
        );
    }

    #[test]
    fn evaluates_foreach_with_multi_output_init() {
        let query = parse("foreach [1, 2][] as $x (0, 10; . + $x)").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Integer(1),
                Value::Integer(3),
                Value::Integer(11),
                Value::Integer(13)
            ]
        );
    }

    #[test]
    fn evaluates_foreach_with_destructuring_and_outer_bindings() {
        let input = Value::from_json(serde_json::json!({
            "factor": 10,
            "pairs": [[1, 2], [3, 4]]
        }))
        .expect("value should parse");
        let query = parse(".factor as $f | foreach .pairs[] as [$x, $y] (0; . + $x + $y; . * $f)")
            .expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(30), Value::Integer(100)]);
    }

    #[test]
    fn evaluates_unary_minus_before_foreach_expression() {
        let input = Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let query = parse("[-foreach -.[] as $x (0; . + $x)]").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([1, 3, 6])).expect("value should parse")]
        );
    }

    #[test]
    fn evaluates_reverse_builtin() {
        let input = Value::from_json(serde_json::json!([1, 2, 3])).expect("value should parse");
        let query = parse("reverse").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::Integer(3),
                Value::Integer(2),
                Value::Integer(1)
            ])]
        );
    }

    #[test]
    fn evaluates_sort_builtin() {
        let input = Value::from_json(serde_json::json!([3, 1, 2])).expect("value should parse");
        let query = parse("sort").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3)
            ])]
        );
    }

    #[test]
    fn evaluates_empty_builtin() {
        let query = parse("empty").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn evaluates_values_filter() {
        let query = parse(".[] | values").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!([1, null, false])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1), Value::Bool(false)]);
    }

    #[test]
    fn evaluates_string_and_number_filters() {
        let query = parse("(.[] | strings), (.[] | numbers)").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!(["alice", 2, true])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::String("alice".to_string()), Value::Integer(2)]
        );
    }

    #[test]
    fn evaluates_tostring_builtin() {
        let query = parse("tostring").expect("query should parse");
        let input = Value::from_json(serde_json::json!({"a": 1})).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("{\"a\":1}".to_string())]);
    }

    #[test]
    fn evaluates_tonumber_builtin() {
        let query = parse("tonumber").expect("query should parse");
        let input = Value::String("3.5".to_string());
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Float(3.5)]);
    }

    #[test]
    fn evaluates_tonumber_builtin_for_plain_integers() {
        let query = parse("tonumber").expect("query should parse");
        let input = Value::String("10".to_string());
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(10)]);
    }

    #[test]
    fn reports_jq_style_tonumber_errors() {
        let query = parse("try tonumber catch .").expect("query should parse");
        let string_values = evaluate(&query, &Value::String("123\u{0000}456".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            string_values,
            vec![Value::String(
                "string (\"123\\u0000456\") cannot be parsed as a number".to_string()
            )]
        );

        let array_values = evaluate(
            &query,
            &Value::from_json(serde_json::json!([1, 2])).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            array_values,
            vec![Value::String(
                "array ([1,2]) cannot be parsed as a number".to_string()
            )]
        );
    }

    #[test]
    fn compares_integer_and_float_values_as_equal() {
        let simple_values = evaluate(
            &parse("10 == 10.0, [10] == [10.0], {value:10} == {value:10.0}")
                .expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            simple_values,
            vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)]
        );

        let query = parse(
            "[{\"a\":42},.object,10,.num,false,true,null,\"b\",[1,4]] | .[] as $x | [$x == .[]]",
        )
        .expect("query should parse");
        let input = Value::from_json(serde_json::json!({"object": {"a": 42}, "num": 10.0}))
            .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values[0],
            Value::from_json(serde_json::json!([
                true, true, false, false, false, false, false, false, false
            ]))
            .expect("value should parse")
        );
        assert_eq!(
            values[1],
            Value::from_json(serde_json::json!([
                true, true, false, false, false, false, false, false, false
            ]))
            .expect("value should parse")
        );
        assert_eq!(
            values[2],
            Value::from_json(serde_json::json!([
                false, false, true, true, false, false, false, false, false
            ]))
            .expect("value should parse")
        );
        assert_eq!(
            values[3],
            Value::from_json(serde_json::json!([
                false, false, true, true, false, false, false, false, false
            ]))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_startswith_and_endswith_builtins() {
        let query = parse("startswith(\"ali\"), endswith(\"ice\")").expect("query should parse");
        let input = Value::String("alice".to_string());
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true), Value::Bool(true)]);
    }

    #[test]
    fn evaluates_split_builtin() {
        let query = parse(
            "split(\",\"), split(\", *\"; null), [splits(\", *\")], [splits(\",? *\"; \"n\")]",
        )
        .expect("query should parse");
        let input = Value::String("ab,cd,   ef, gh".to_string());
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Array(vec![
                    Value::String("ab".to_string()),
                    Value::String("cd".to_string()),
                    Value::String("   ef".to_string()),
                    Value::String(" gh".to_string())
                ]),
                Value::Array(vec![
                    Value::String("ab".to_string()),
                    Value::String("cd".to_string()),
                    Value::String("ef".to_string()),
                    Value::String("gh".to_string())
                ]),
                Value::Array(vec![
                    Value::String("ab".to_string()),
                    Value::String("cd".to_string()),
                    Value::String("ef".to_string()),
                    Value::String("gh".to_string())
                ]),
                Value::Array(vec![
                    Value::String("ab".to_string()),
                    Value::String("cd".to_string()),
                    Value::String("ef".to_string()),
                    Value::String("gh".to_string())
                ]),
            ]
        );

        let query = parse("[splits(\"\")]").expect("query should parse");
        let input = Value::String("a,b,c".to_string());
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::String(String::new()),
                Value::String("a".to_string()),
                Value::String(",".to_string()),
                Value::String("b".to_string()),
                Value::String(",".to_string()),
                Value::String("c".to_string()),
                Value::String(String::new())
            ])]
        );
    }

    #[test]
    fn evaluates_regex_builtins() {
        let regex_query = parse(
            "test(\"^(?<name>[a-z]+)-(?<id>[0-9]+)$\"), capture(\"^(?<name>[a-z]+)-(?<id>[0-9]+)$\")",
        )
        .expect("query should parse");
        let input = Value::String("alice-42".to_string());
        let values = evaluate(&regex_query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values[0], Value::Bool(true));
        assert_eq!(
            values[1],
            Value::from_json(serde_json::json!({
                "name": "alice",
                "id": "42"
            }))
            .expect("value should parse")
        );

        let replace_query =
            parse("(.value | sub(\"cat\"; \"dog\")), (.numbers | gsub(\"[0-9]+\"; \"#\"))")
                .expect("query should parse");
        let replace_input = Value::from_json(serde_json::json!({
            "value": "catapult cat cat",
            "numbers": "a1 b22 c333"
        }))
        .expect("value should parse");
        let replaced = evaluate(&replace_query, &replace_input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            replaced,
            vec![
                Value::String("dogapult cat cat".to_string()),
                Value::String("a# b# c#".to_string())
            ]
        );

        let match_query = parse("match(\"([a-z]+)([0-9]+)\"), match(\"abc\"; \"ig\")")
            .expect("query should parse");
        let matched = evaluate(&match_query, &Value::String("ABCabc123".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            matched[0],
            Value::from_json(serde_json::json!({
                "offset": 3,
                "length": 6,
                "string": "abc123",
                "captures": [
                    {
                        "offset": 3,
                        "length": 3,
                        "string": "abc",
                        "name": null
                    },
                    {
                        "offset": 6,
                        "length": 3,
                        "string": "123",
                        "name": null
                    }
                ]
            }))
            .expect("value should parse")
        );
        assert_eq!(
            matched[1..],
            [
                Value::from_json(serde_json::json!({
                    "offset": 0,
                    "length": 3,
                    "string": "ABC",
                    "captures": []
                }))
                .expect("value should parse"),
                Value::from_json(serde_json::json!({
                    "offset": 3,
                    "length": 3,
                    "string": "abc",
                    "captures": []
                }))
                .expect("value should parse")
            ]
        );

        let scan_query =
            parse("scan(\"([a-z]+)([0-9]+)\"), scan(\"abc\"; \"i\")").expect("query should parse");
        let scanned = evaluate(&scan_query, &Value::String("ABCabc123def456".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            scanned,
            vec![
                Value::Array(vec![
                    Value::String("abc".to_string()),
                    Value::String("123".to_string())
                ]),
                Value::Array(vec![
                    Value::String("def".to_string()),
                    Value::String("456".to_string())
                ]),
                Value::String("ABC".to_string()),
                Value::String("abc".to_string()),
            ]
        );

        let jq_style_query = parse(
            "match([\"foo\", \"ig\"]), [match(\"( )*\"; \"gn\")], [test(\"( )*\"; \"gn\")], gsub(\"[^a-z]*(?<x>[a-z]*)\"; \"Z\\(.x)\")",
        )
        .expect("query should parse");
        let jq_style_values = evaluate(&jq_style_query, &Value::String("123foo456bar".to_string()))
            .expect("query should run")
            .into_vec();
        assert_eq!(
            jq_style_values[0],
            Value::from_json(serde_json::json!({
                "offset": 3,
                "length": 3,
                "string": "foo",
                "captures": []
            }))
            .expect("value should parse")
        );
        assert_eq!(jq_style_values[1], Value::Array(Vec::new()));
        assert_eq!(jq_style_values[2], Value::Array(vec![Value::Bool(false)]));
        assert_eq!(jq_style_values[3], Value::String("ZfooZbarZ".to_string()));

        let lookahead_values = evaluate(
            &parse("gsub(\"(?=u)\"; \"u\")").expect("query should parse"),
            &Value::String("qux".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(lookahead_values, vec![Value::String("quux".to_string())]);
    }

    #[test]
    fn evaluates_regex_flags_and_replacement_streams() {
        let query = parse(
            "(.mixed | test(\"^(?<letters>[a-z]+)(?<digits>[0-9]+)$\"; \"i\")), (.mixed | capture(\"^(?<letters>[a-z]+)(?<digits>[0-9]+)$\"; \"i\")), (.replace_once | sub(\"ab\"; \"X\"; \"ig\")), (.replace_all | gsub(\"(?<letters>[a-z]+)(?<digits>[0-9]+)\"; if .letters == \"ab\" then \"A\" else \"C\", \"D\" end))",
        )
        .expect("query should parse");
        let values = evaluate(
            &query,
            &Value::from_json(serde_json::json!({
                "mixed": "ABC123",
                "replace_once": "abABab",
                "replace_all": "ab12--cd34"
            }))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Bool(true),
                Value::from_json(serde_json::json!({
                    "letters": "ABC",
                    "digits": "123"
                }))
                .expect("value should parse"),
                Value::String("XXX".to_string()),
                Value::String("A--C".to_string()),
                Value::String("--D".to_string()),
            ]
        );
    }

    #[test]
    fn evaluates_format_operators() {
        let json_value = evaluate(
            &parse("@json").expect("query should parse"),
            &Value::from_json(serde_json::json!({"name":"alice","roles":["admin"]}))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            json_value,
            vec![Value::String(
                "{\"name\":\"alice\",\"roles\":[\"admin\"]}".to_string()
            )]
        );

        let text_value = evaluate(
            &parse("@text").expect("query should parse"),
            &Value::from_json(serde_json::json!({"name":"alice","roles":["admin"]}))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            text_value,
            vec![Value::String(
                "{\"name\":\"alice\",\"roles\":[\"admin\"]}".to_string()
            )]
        );

        let decimal_text_value = evaluate(
            &parse("@text").expect("query should parse"),
            &Value::from_json(serde_json::json!(1.25)).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(decimal_text_value, vec![Value::String("1.25".to_string())]);

        let csv_value = evaluate(
            &parse("@csv").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                "abc", "a,b", "a\"b", "c\nd", null, 1, 1.25, true
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            csv_value,
            vec![Value::String(
                "\"abc\",\"a,b\",\"a\"\"b\",\"c\nd\",,1,1.25,true".to_string()
            )]
        );

        let tsv_value = evaluate(
            &parse("@tsv").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                "abc", "a\tb", "c\nd", "e\\f", null, 1, 1.25, true
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            tsv_value,
            vec![Value::String(
                "abc\ta\\tb\tc\\nd\te\\\\f\t\t1\t1.25\ttrue".to_string()
            )]
        );

        let html_value = evaluate(
            &parse("@html").expect("query should parse"),
            &Value::String("a&b<c>d\"e'f".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            html_value,
            vec![Value::String("a&amp;b&lt;c&gt;d&quot;e&apos;f".to_string())]
        );

        let uri_value = evaluate(
            &parse("@uri").expect("query should parse"),
            &Value::String("a b/c?d=e&f".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            uri_value,
            vec![Value::String("a%20b%2Fc%3Fd%3De%26f".to_string())]
        );

        let urid_value = evaluate(
            &parse("@urid").expect("query should parse"),
            &Value::String("a%20b%2Fc%3Fd%3De%26f".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(urid_value, vec![Value::String("a b/c?d=e&f".to_string())]);

        let sh_value = evaluate(
            &parse("@sh").expect("query should parse"),
            &Value::from_json(serde_json::json!([
                "abc", "a b", "c'd", 3, 1.25, true, null
            ]))
            .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            sh_value,
            vec![Value::String(
                "'abc' 'a b' 'c'\\''d' 3 1.25 true null".to_string()
            )]
        );

        let base64_value = evaluate(
            &parse("@base64").expect("query should parse"),
            &Value::from_json(serde_json::json!({"a":1})).expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            base64_value,
            vec![Value::String("eyJhIjoxfQ==".to_string())]
        );

        let base64d_value = evaluate(
            &parse("@base64d").expect("query should parse"),
            &Value::String("/w==".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(base64d_value, vec![Value::String("\u{FFFD}".to_string())]);
    }

    #[test]
    fn evaluates_format_string_interpolation() {
        let uri_values = evaluate(
            &parse("@uri \"x=\\(.x)&y=\\(.y)\"").expect("query should parse"),
            &Value::from_json(serde_json::json!({"x":"a b","y":"c/d"}))
                .expect("value should parse"),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            uri_values,
            vec![Value::String("x=a%20b&y=c%2Fd".to_string())]
        );

        let product_values = evaluate(
            &parse("@text \"\\(.,.+1)-\\(.+10,.+20)\"").expect("query should parse"),
            &Value::Integer(1),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            product_values,
            vec![
                Value::String("1-11".to_string()),
                Value::String("2-11".to_string()),
                Value::String("1-21".to_string()),
                Value::String("2-21".to_string()),
            ]
        );

        let empty_values = evaluate(
            &parse("@text \"a\\(empty)b\"").expect("query should parse"),
            &Value::Integer(1),
        )
        .expect("query should run")
        .into_vec();
        assert!(empty_values.is_empty());
    }

    #[test]
    fn evaluates_jq_style_interpolated_string_literals() {
        let values = evaluate(
            &parse("\"inter\\(\"pol\" + \"ation\")\"").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::String("interpolation".to_string())]);
    }

    #[test]
    fn evaluates_jq_style_unary_negation() {
        let values = evaluate(
            &parse("{x:-1},{x:-.},{x:-.|abs}").expect("query should parse"),
            &Value::Integer(1),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"x": -1})).expect("value should parse"),
                Value::from_json(serde_json::json!({"x": -1})).expect("value should parse"),
                Value::from_json(serde_json::json!({"x": 1})).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_jq_pipe_precedence_with_comma_generators() {
        let values = evaluate(
            &parse("{x:(1,2)},{x:3} | .x").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        );

        let values = evaluate(
            &parse("[nan % 1, 1 % nan | isnan]").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([true, true])).expect("value should parse"),]
        );
    }

    #[test]
    fn evaluates_quoted_string_object_shorthand() {
        let input = Value::from_json(serde_json::json!({"a": 1, "b": 2, "a$2": 4}))
            .expect("value should parse");
        let values = evaluate(
            &parse("{\"a\",b,\"a$\\(1+1)\"}").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!({"a": 1, "b": 2, "a$2": 4}))
                    .expect("value should parse")
            ]
        );
    }

    #[test]
    fn evaluates_interpolated_string_lookup_segments() {
        let input =
            Value::from_json(serde_json::json!({"prefix-2": "match"})).expect("value should parse");
        let values = evaluate(
            &parse(".[\"prefix-\\(1+1)\"], .\"prefix-\\(1+1)\"").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("match".to_string()),
                Value::String("match".to_string()),
            ]
        );
    }

    #[test]
    fn evaluates_input_and_inputs_builtins() {
        let query = parse("input, inputs").expect("query should parse");
        let context = EvaluationContext::from_remaining_inputs(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let values = evaluate_with_context(&query, &Value::Null, &context)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        );
        assert_eq!(context.remaining_inputs_len(), 0);
    }

    #[test]
    fn input_builtin_returns_break_at_eof() {
        let query = parse("input").expect("query should parse");
        let error = evaluate_with_context(&query, &Value::Null, &EvaluationContext::empty())
            .expect_err("query should fail");
        assert!(matches!(error, AqError::Break));
    }

    #[test]
    fn errors_on_invalid_regex_inputs() {
        let error = evaluate(
            &parse("test(\"[\")").expect("query should parse"),
            &Value::String("alice".to_string()),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("test failed to compile regex"));

        let error = evaluate(
            &parse("capture(\"^(?<name>[a-z]+)$\")").expect("query should parse"),
            &Value::String("42".to_string()),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("capture did not match input"));

        let error = evaluate(
            &parse("match(\"abc\"; \"z\")").expect("query should parse"),
            &Value::String("abc".to_string()),
        )
        .expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("match does not support regex flag `z`"));

        let error = evaluate(
            &parse("sub(\"abc\"; \"x\"; \"z\")").expect("query should parse"),
            &Value::String("abc".to_string()),
        )
        .expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("sub does not support regex flag `z`"));
    }

    #[test]
    fn errors_on_invalid_format_operator_inputs() {
        let error = evaluate(
            &parse("@csv").expect("query should parse"),
            &Value::from_json(serde_json::json!([{"a": 1}])).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("is not valid in a csv row"));

        let error = evaluate(
            &parse("@sh").expect("query should parse"),
            &Value::from_json(serde_json::json!({"a": 1})).expect("value should parse"),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("is not valid in a shell string"));

        let error = evaluate(
            &parse("@base64d").expect("query should parse"),
            &Value::String("%%%%".to_string()),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("is not valid base64 data"));

        let decoded = evaluate(
            &parse("@base64d").expect("query should parse"),
            &Value::String("=".to_string()),
        )
        .expect("query should run");
        assert_eq!(decoded.into_vec(), vec![Value::String(String::new())]);

        let output = evaluate(
            &parse("try @base64d catch .").expect("query should parse"),
            &Value::String("QUJDa".to_string()),
        )
        .expect("query should run");
        assert_eq!(
            output.into_vec(),
            vec![Value::String(
                "string (\"QUJDa\") trailing base64 byte found".to_string()
            )]
        );

        let error = evaluate(
            &parse("@urid").expect("query should parse"),
            &Value::String("abc%ZZ".to_string()),
        )
        .expect_err("query should fail");
        assert!(error.to_string().contains("is not a valid uri encoding"));
    }

    #[test]
    fn evaluates_min_and_max_builtins() {
        let query = parse("min, max").expect("query should parse");
        let input = Value::from_json(serde_json::json!([3, 1, 2])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Integer(1), Value::Integer(3)]);
    }

    #[test]
    fn evaluates_unique_builtin() {
        let query = parse("unique").expect("query should parse");
        let input = Value::from_json(serde_json::json!([3, 1, 2, 1])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3)
            ])]
        );
    }

    #[test]
    fn evaluates_flatten_builtin() {
        let query = parse("flatten").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!([1, [2, [3]], 4])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4)
            ])]
        );
    }

    #[test]
    fn evaluates_any_and_all_builtins() {
        let query = parse("any, all").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!([true, false, true])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true), Value::Bool(false)]);
    }

    #[test]
    fn evaluates_any_and_all_predicates() {
        let query = parse("any(.active), all(.active)").expect("query should parse");
        let input = Value::from_json(serde_json::json!([
            {"active": true},
            {"active": false}
        ]))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true), Value::Bool(false)]);
    }

    #[test]
    fn evaluates_any_and_all_query_forms() {
        let input = Value::from_json(serde_json::json!([1, 2, 3, 4, true, false, 1, 2, 3, 4, 5]))
            .expect("value should parse");
        let values = evaluate(
            &parse(". as $dot | any($dot[]; not)").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);

        let values = evaluate(
            &parse(". as $dot | all($dot[]; .)").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Bool(false)]);

        let values = evaluate(
            &parse("[false] | any(not)").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);

        let values = evaluate(
            &parse("[] | all(not)").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);

        let values = evaluate(
            &parse("[false] | any(not)").expect("query should parse"),
            &Value::Null,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);

        let values = evaluate(
            &parse("any(true, error; .), all(false, error; .)").expect("query should parse"),
            &Value::String("badness".to_string()),
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(values, vec![Value::Bool(true), Value::Bool(false)]);
    }

    #[test]
    fn evaluates_any_and_all_query_predicates_with_explicit_separation() {
        let query = parse("(any(.active)), (all(.active))").expect("query should parse");
        let input = Value::from_json(serde_json::json!([
            {"active": true},
            {"active": false}
        ]))
        .expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true), Value::Bool(false)]);
    }

    #[test]
    fn evaluates_join_builtin() {
        let query = parse("join(\",\")").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!(["a", 2, true, null])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("a,2,true,".to_string())]);
    }

    #[test]
    fn evaluates_uppercase_join_builtins() {
        let input = Value::from_json(serde_json::json!([1, 2])).expect("value should parse");

        let values = evaluate(
            &parse("JOIN({\"1\":\"a\",\"2\":\"b\"}; tostring)").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![Value::from_json(serde_json::json!([[1, "a"], [2, "b"]]))
                .expect("value should parse")]
        );

        let values = evaluate(
            &parse("JOIN({\"1\":\"a\",\"2\":\"b\"}; .[]; tostring)").expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1, "a"])).expect("value should parse"),
                Value::from_json(serde_json::json!([2, "b"])).expect("value should parse"),
            ]
        );

        let values = evaluate(
            &parse("JOIN({\"1\":\"a\",\"2\":\"b\"}; .[]; tostring; [.[0], .[1], .[0] + 10])")
                .expect("query should parse"),
            &input,
        )
        .expect("query should run")
        .into_vec();
        assert_eq!(
            values,
            vec![
                Value::from_json(serde_json::json!([1, "a", 11])).expect("value should parse"),
                Value::from_json(serde_json::json!([2, "b", 12])).expect("value should parse"),
            ]
        );
    }

    #[test]
    fn evaluates_join_with_multi_output_separators() {
        let query = parse("join(\",\",\"/\")").expect("query should parse");
        let input =
            Value::from_json(serde_json::json!(["a", "b", "c", "d"])).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("a,b,c,d".to_string()),
                Value::String("a/b/c/d".to_string()),
            ]
        );
    }

    #[test]
    fn evaluates_ascii_case_and_trim_builtins() {
        let query =
            parse("ascii_downcase, ascii_upcase, trim, ltrim, rtrim").expect("query should parse");
        let input = Value::String("  AlIce  ".to_string());
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::String("  alice  ".to_string()),
                Value::String("  ALICE  ".to_string()),
                Value::String("AlIce".to_string()),
                Value::String("AlIce  ".to_string()),
                Value::String("  AlIce".to_string())
            ]
        );
    }

    #[test]
    fn evaluates_type_builtin() {
        let query = parse("type").expect("query should parse");
        let values = evaluate(&query, &Value::Integer(1))
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::String("number".to_string())]);
    }

    #[test]
    fn evaluates_select_builtin() {
        let input =
            Value::from_json(serde_json::json!({"active": true})).expect("value should parse");
        let query = parse("select(.active == true)").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![input]);
    }

    #[test]
    fn evaluates_comparisons() {
        let query = parse(".age >= 21").expect("query should parse");
        let input = Value::from_json(serde_json::json!({"age": 30})).expect("value should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Bool(true)]);
    }

    #[test]
    fn optional_field_omits_results_on_type_errors() {
        let input = Value::Integer(1);
        let query = parse(".foo?").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn optional_field_preserves_null_inputs() {
        let query = parse(".foo?").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(values, vec![Value::Null]);
    }

    #[test]
    fn optional_iterate_suppresses_type_errors() {
        let input = Value::Integer(1);
        let query = parse(".[]?").expect("query should parse");
        let values = evaluate(&query, &input)
            .expect("query should run")
            .into_vec();
        assert!(values.is_empty());
    }

    #[test]
    fn iterate_over_null_errors_without_optional() {
        let query = parse(".[]").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error
            .to_string()
            .contains("Cannot iterate over null (null)"));
    }

    #[test]
    fn evaluates_scientific_notation_literals() {
        let query =
            parse("1e+0+0.001e3, 1e-19 + 1e-20 - 5e-21, 1 / 1e-17").expect("query should parse");
        let values = evaluate(&query, &Value::Null)
            .expect("query should run")
            .into_vec();
        assert_eq!(
            values,
            vec![
                Value::Float(2.0),
                Value::Float(1.05e-19),
                Value::Float(1e17),
            ]
        );
    }

    #[test]
    fn rejects_excessive_array_growth_in_assignment() {
        let query = parse(".[999999999] = 0").expect("query should parse");
        let error = evaluate(&query, &Value::Null).expect_err("query should fail");
        assert!(error.to_string().contains("Array index too large"));
    }
}
