use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::hash::Hash;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use allocative::Allocative;
use base64::engine::general_purpose::{
    STANDARD as BASE64_STANDARD, STANDARD_NO_PAD as BASE64_STANDARD_NO_PAD,
    URL_SAFE as BASE64_URL_SAFE, URL_SAFE_NO_PAD as BASE64_URL_SAFE_NO_PAD,
};
use base64::Engine as _;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use indexmap::IndexMap;
use regex::Regex;
use sha1::Sha1;
use sha2::Digest as _;
use sha2::{Sha256, Sha512};
use starlark::collections::SmallMap;
use starlark::collections::StarlarkHasher;
use starlark::environment::{
    FrozenModule, Globals, GlobalsBuilder, Methods, MethodsBuilder, MethodsStatic, Module,
};
use starlark::eval::{Evaluator, ReturnFileLoader};
use starlark::starlark_module;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::dict::DictRef;
use starlark::values::list::ListRef;
use starlark::values::none::NoneOr;
use starlark::values::tuple::TupleRef;
use starlark::values::{
    Heap, NoSerialize, ProvidesStaticType, UnpackValue, Value as StarlarkValue, ValueError,
};

use crate::error::AqError;
use crate::format::{
    parse_text, read_path, render, DetectConflictPolicy, Format, InputDocument, RenderOptions,
};
use crate::inplace::write_atomically;
use crate::query::{evaluate, parse};
use crate::value::{parse_common_datetime_string, parse_date_string, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StarlarkCapabilities {
    pub filesystem: bool,
    pub environment: bool,
    pub time: bool,
}

impl StarlarkCapabilities {
    pub fn from_flags(filesystem: bool, environment: bool, time: bool, unsafe_all: bool) -> Self {
        if unsafe_all {
            return Self {
                filesystem: true,
                environment: true,
                time: true,
            };
        }

        Self {
            filesystem,
            environment,
            time,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarlarkContext {
    pub capabilities: StarlarkCapabilities,
    pub detect_conflicts: DetectConflictPolicy,
    pub current_format_name: Option<String>,
    pub base_dir: PathBuf,
}

impl StarlarkContext {
    pub fn new(
        capabilities: StarlarkCapabilities,
        detect_conflicts: DetectConflictPolicy,
        current_format_name: Option<String>,
        base_dir: PathBuf,
    ) -> Self {
        Self {
            capabilities,
            detect_conflicts,
            current_format_name,
            base_dir,
        }
    }
}

#[derive(Debug, Clone, ProvidesStaticType, NoSerialize, Allocative)]
struct AqDate {
    #[allocative(skip)]
    date: NaiveDate,
}

impl AqDate {
    fn new(date: NaiveDate) -> Self {
        Self { date }
    }
}

starlark::starlark_simple_value!(AqDate);

impl fmt::Display for AqDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.date.to_string())
    }
}

#[starlark::values::starlark_value(type = "date")]
impl<'v> starlark::values::StarlarkValue<'v> for AqDate {
    fn get_methods() -> Option<&'static Methods> {
        static RES: MethodsStatic = MethodsStatic::new();
        RES.methods(aq_date_methods)
    }

    fn add(
        &self,
        rhs: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> Option<starlark::Result<StarlarkValue<'v>>> {
        AqTimedelta::from_value(rhs).map(|rhs| add_duration_to_date(self.date, rhs.duration, heap))
    }

    fn sub(&self, other: StarlarkValue<'v>, heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        if let Some(other) = AqTimedelta::from_value(other) {
            return subtract_duration_from_date(self.date, other.duration, heap);
        }
        if let Some(other) = AqDate::from_value(other) {
            return Ok(heap.alloc(AqTimedelta::new(
                self.date.signed_duration_since(other.date),
            )));
        }
        ValueError::unsupported_with(self, "-", other)
    }

    fn compare(&self, other: StarlarkValue<'v>) -> starlark::Result<Ordering> {
        let Some(other) = AqDate::from_value(other) else {
            return ValueError::unsupported_with(self, "compare", other);
        };
        Ok(self.date.cmp(&other.date))
    }

    fn equals(&self, other: StarlarkValue<'v>) -> starlark::Result<bool> {
        Ok(AqDate::from_value(other).is_some_and(|other| self.date == other.date))
    }

    fn write_hash(&self, hasher: &mut StarlarkHasher) -> starlark::Result<()> {
        "aq.date".hash(hasher);
        self.date.num_days_from_ce().hash(hasher);
        Ok(())
    }
}

#[derive(Debug, Clone, ProvidesStaticType, NoSerialize, Allocative)]
struct AqDateTime {
    #[allocative(skip)]
    datetime: DateTime<Utc>,
}

impl AqDateTime {
    fn new(datetime: DateTime<Utc>) -> Self {
        Self { datetime }
    }
}

starlark::starlark_simple_value!(AqDateTime);

impl fmt::Display for AqDateTime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&render_datetime(&self.datetime))
    }
}

#[starlark::values::starlark_value(type = "datetime")]
impl<'v> starlark::values::StarlarkValue<'v> for AqDateTime {
    fn get_methods() -> Option<&'static Methods> {
        static RES: MethodsStatic = MethodsStatic::new();
        RES.methods(aq_datetime_methods)
    }

    fn add(
        &self,
        rhs: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> Option<starlark::Result<StarlarkValue<'v>>> {
        AqTimedelta::from_value(rhs)
            .map(|rhs| add_duration_to_datetime(&self.datetime, rhs.duration, heap))
    }

    fn sub(&self, other: StarlarkValue<'v>, heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        if let Some(other) = AqTimedelta::from_value(other) {
            return subtract_duration_from_datetime(&self.datetime, other.duration, heap);
        }
        if let Some(other) = AqDateTime::from_value(other) {
            return Ok(heap.alloc(AqTimedelta::new(
                self.datetime.signed_duration_since(other.datetime),
            )));
        }
        ValueError::unsupported_with(self, "-", other)
    }

    fn compare(&self, other: StarlarkValue<'v>) -> starlark::Result<Ordering> {
        let Some(other) = AqDateTime::from_value(other) else {
            return ValueError::unsupported_with(self, "compare", other);
        };
        Ok(self.datetime.cmp(&other.datetime))
    }

    fn equals(&self, other: StarlarkValue<'v>) -> starlark::Result<bool> {
        Ok(AqDateTime::from_value(other).is_some_and(|other| self.datetime == other.datetime))
    }

    fn write_hash(&self, hasher: &mut StarlarkHasher) -> starlark::Result<()> {
        "aq.datetime".hash(hasher);
        self.datetime.timestamp().hash(hasher);
        self.datetime.timestamp_subsec_nanos().hash(hasher);
        Ok(())
    }
}

#[derive(Debug, Clone, ProvidesStaticType, NoSerialize, Allocative)]
struct AqTimedelta {
    #[allocative(skip)]
    duration: Duration,
}

impl AqTimedelta {
    fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

starlark::starlark_simple_value!(AqTimedelta);

impl fmt::Display for AqTimedelta {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&render_timedelta(self.duration))
    }
}

#[starlark::values::starlark_value(type = "timedelta")]
impl<'v> starlark::values::StarlarkValue<'v> for AqTimedelta {
    fn get_methods() -> Option<&'static Methods> {
        static RES: MethodsStatic = MethodsStatic::new();
        RES.methods(aq_timedelta_methods)
    }

    fn add(
        &self,
        rhs: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> Option<starlark::Result<StarlarkValue<'v>>> {
        if let Some(rhs) = AqTimedelta::from_value(rhs) {
            return Some(add_durations(self.duration, rhs.duration, heap));
        }
        if let Some(rhs) = AqDate::from_value(rhs) {
            return Some(add_duration_to_date(rhs.date, self.duration, heap));
        }
        if let Some(rhs) = AqDateTime::from_value(rhs) {
            return Some(add_duration_to_datetime(&rhs.datetime, self.duration, heap));
        }
        None
    }

    fn sub(&self, other: StarlarkValue<'v>, heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        let Some(other) = AqTimedelta::from_value(other) else {
            return ValueError::unsupported_with(self, "-", other);
        };
        subtract_durations(self.duration, other.duration, heap)
    }

    fn compare(&self, other: StarlarkValue<'v>) -> starlark::Result<Ordering> {
        let Some(other) = AqTimedelta::from_value(other) else {
            return ValueError::unsupported_with(self, "compare", other);
        };
        Ok(self.duration.cmp(&other.duration))
    }

    fn equals(&self, other: StarlarkValue<'v>) -> starlark::Result<bool> {
        Ok(AqTimedelta::from_value(other).is_some_and(|other| self.duration == other.duration))
    }

    fn write_hash(&self, hasher: &mut StarlarkHasher) -> starlark::Result<()> {
        "aq.timedelta".hash(hasher);
        self.duration.num_seconds().hash(hasher);
        let remainder = (self.duration - Duration::seconds(self.duration.num_seconds()))
            .num_nanoseconds()
            .unwrap_or(0);
        remainder.hash(hasher);
        Ok(())
    }
}

#[starlark_module]
fn aq_date_methods(builder: &mut MethodsBuilder) {
    #[starlark(attribute)]
    fn year(this: &AqDate) -> starlark::Result<i32> {
        Ok(this.date.year())
    }

    #[starlark(attribute)]
    fn month(this: &AqDate) -> starlark::Result<u32> {
        Ok(this.date.month())
    }

    #[starlark(attribute)]
    fn day(this: &AqDate) -> starlark::Result<u32> {
        Ok(this.date.day())
    }

    #[starlark(attribute)]
    fn ordinal(this: &AqDate) -> starlark::Result<u32> {
        Ok(this.date.ordinal())
    }

    fn weekday(this: &AqDate) -> starlark::Result<u32> {
        Ok(this.date.weekday().num_days_from_monday())
    }

    fn isoformat(this: &AqDate) -> starlark::Result<String> {
        Ok(this.date.to_string())
    }

    fn replace<'v>(
        this: &AqDate,
        #[starlark(require = named, default = NoneOr::None)] year: NoneOr<i32>,
        #[starlark(require = named, default = NoneOr::None)] month: NoneOr<u32>,
        #[starlark(require = named, default = NoneOr::None)] day: NoneOr<u32>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let date = replace_date_fields(
            this.date,
            year.into_option(),
            month.into_option(),
            day.into_option(),
        )
        .map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(AqDate::new(date)))
    }

    fn at<'v>(
        this: &AqDate,
        #[starlark(require = named, default = 0)] hour: u32,
        #[starlark(require = named, default = 0)] minute: u32,
        #[starlark(require = named, default = 0)] second: u32,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let datetime = build_datetime(this.date, hour, minute, second, 0)
            .map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(AqDateTime::new(datetime)))
    }
}

#[starlark_module]
fn aq_datetime_methods(builder: &mut MethodsBuilder) {
    #[starlark(attribute)]
    fn year(this: &AqDateTime) -> starlark::Result<i32> {
        Ok(this.datetime.year())
    }

    #[starlark(attribute)]
    fn month(this: &AqDateTime) -> starlark::Result<u32> {
        Ok(this.datetime.month())
    }

    #[starlark(attribute)]
    fn day(this: &AqDateTime) -> starlark::Result<u32> {
        Ok(this.datetime.day())
    }

    #[starlark(attribute)]
    fn hour(this: &AqDateTime) -> starlark::Result<u32> {
        Ok(this.datetime.hour())
    }

    #[starlark(attribute)]
    fn minute(this: &AqDateTime) -> starlark::Result<u32> {
        Ok(this.datetime.minute())
    }

    #[starlark(attribute)]
    fn second(this: &AqDateTime) -> starlark::Result<u32> {
        Ok(this.datetime.second())
    }

    #[starlark(attribute)]
    fn ordinal(this: &AqDateTime) -> starlark::Result<u32> {
        Ok(this.datetime.ordinal())
    }

    fn weekday(this: &AqDateTime) -> starlark::Result<u32> {
        Ok(this.datetime.weekday().num_days_from_monday())
    }

    fn isoformat(this: &AqDateTime) -> starlark::Result<String> {
        Ok(render_datetime(&this.datetime))
    }

    fn date<'v>(this: &AqDateTime, heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(AqDate::new(this.datetime.date_naive())))
    }

    fn timestamp(this: &AqDateTime) -> starlark::Result<f64> {
        let nanos = this.datetime.timestamp_nanos_opt().ok_or_else(|| {
            starlark::Error::new_other(StarlarkBuiltinError::new(
                "datetime timestamp is out of range",
            ))
        })?;
        Ok(nanos as f64 / 1_000_000_000.0)
    }

    #[allow(clippy::too_many_arguments)]
    fn replace<'v>(
        this: &AqDateTime,
        #[starlark(require = named, default = NoneOr::None)] year: NoneOr<i32>,
        #[starlark(require = named, default = NoneOr::None)] month: NoneOr<u32>,
        #[starlark(require = named, default = NoneOr::None)] day: NoneOr<u32>,
        #[starlark(require = named, default = NoneOr::None)] hour: NoneOr<u32>,
        #[starlark(require = named, default = NoneOr::None)] minute: NoneOr<u32>,
        #[starlark(require = named, default = NoneOr::None)] second: NoneOr<u32>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let datetime = replace_datetime_fields(
            &this.datetime,
            year.into_option(),
            month.into_option(),
            day.into_option(),
            hour.into_option(),
            minute.into_option(),
            second.into_option(),
        )
        .map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(AqDateTime::new(datetime)))
    }
}

#[starlark_module]
fn aq_timedelta_methods(builder: &mut MethodsBuilder) {
    fn total_seconds(this: &AqTimedelta) -> starlark::Result<f64> {
        Ok(duration_total_seconds(this.duration))
    }
}

fn render_datetime(value: &DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

fn render_timedelta(value: Duration) -> String {
    let seconds = value.num_seconds();
    let remainder = value - Duration::seconds(seconds);
    if remainder == Duration::zero() {
        format!("timedelta(seconds = {seconds})")
    } else {
        let nanos = remainder.num_nanoseconds().unwrap_or(0);
        format!("timedelta(seconds = {seconds}, nanoseconds = {nanos})")
    }
}

fn duration_total_seconds(value: Duration) -> f64 {
    let seconds = value.num_seconds();
    let remainder = value - Duration::seconds(seconds);
    let nanos = remainder.num_nanoseconds().unwrap_or(0);
    seconds as f64 + nanos as f64 / 1_000_000_000.0
}

fn duration_component(
    name: &str,
    value: i64,
    constructor: impl FnOnce(i64) -> Option<Duration>,
) -> Result<Duration, StarlarkBuiltinError> {
    constructor(value).ok_or_else(|| {
        StarlarkBuiltinError::new(format!("aq.timedelta {name} component is out of range"))
    })
}

fn add_duration(total: Duration, component: Duration) -> Result<Duration, StarlarkBuiltinError> {
    total
        .checked_add(&component)
        .ok_or_else(|| StarlarkBuiltinError::new("aq.timedelta result is out of range"))
}

#[allow(clippy::too_many_arguments)]
fn build_timedelta(
    weeks: i64,
    days: i64,
    hours: i64,
    minutes: i64,
    seconds: i64,
    milliseconds: i64,
    microseconds: i64,
    nanoseconds: i64,
) -> Result<AqTimedelta, StarlarkBuiltinError> {
    let mut duration = Duration::zero();
    duration = add_duration(
        duration,
        duration_component("weeks", weeks, Duration::try_weeks)?,
    )?;
    duration = add_duration(
        duration,
        duration_component("days", days, Duration::try_days)?,
    )?;
    duration = add_duration(
        duration,
        duration_component("hours", hours, Duration::try_hours)?,
    )?;
    duration = add_duration(
        duration,
        duration_component("minutes", minutes, Duration::try_minutes)?,
    )?;
    duration = add_duration(
        duration,
        duration_component("seconds", seconds, Duration::try_seconds)?,
    )?;
    duration = add_duration(
        duration,
        duration_component("milliseconds", milliseconds, Duration::try_milliseconds)?,
    )?;
    duration = add_duration(
        duration,
        duration_component("microseconds", microseconds, |value| {
            Some(Duration::microseconds(value))
        })?,
    )?;
    duration = add_duration(
        duration,
        duration_component("nanoseconds", nanoseconds, |value| {
            Some(Duration::nanoseconds(value))
        })?,
    )?;
    Ok(AqTimedelta::new(duration))
}

fn build_date(year: i32, month: u32, day: u32) -> Result<NaiveDate, StarlarkBuiltinError> {
    NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
        StarlarkBuiltinError::new(format!(
            "invalid date components year={year} month={month} day={day}"
        ))
    })
}

fn build_datetime(
    date: NaiveDate,
    hour: u32,
    minute: u32,
    second: u32,
    nanosecond: u32,
) -> Result<DateTime<Utc>, StarlarkBuiltinError> {
    let naive = date
        .and_hms_nano_opt(hour, minute, second, nanosecond)
        .ok_or_else(|| {
            StarlarkBuiltinError::new(format!(
                "invalid time components hour={hour} minute={minute} second={second}"
            ))
        })?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn replace_date_fields(
    date: NaiveDate,
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
) -> Result<NaiveDate, StarlarkBuiltinError> {
    build_date(
        year.unwrap_or(date.year()),
        month.unwrap_or(date.month()),
        day.unwrap_or(date.day()),
    )
}

fn replace_datetime_fields(
    datetime: &DateTime<Utc>,
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<u32>,
) -> Result<DateTime<Utc>, StarlarkBuiltinError> {
    let date = build_date(
        year.unwrap_or(datetime.year()),
        month.unwrap_or(datetime.month()),
        day.unwrap_or(datetime.day()),
    )?;
    build_datetime(
        date,
        hour.unwrap_or(datetime.hour()),
        minute.unwrap_or(datetime.minute()),
        second.unwrap_or(datetime.second()),
        datetime.nanosecond(),
    )
}

fn whole_day_duration(value: Duration) -> Result<Duration, StarlarkBuiltinError> {
    let days = value.num_days();
    let whole_days = Duration::try_days(days)
        .ok_or_else(|| StarlarkBuiltinError::new("date arithmetic overflowed"))?;
    if value == whole_days {
        Ok(whole_days)
    } else {
        Err(StarlarkBuiltinError::new(
            "date arithmetic requires a whole-day timedelta",
        ))
    }
}

fn add_duration_to_date<'v>(
    date: NaiveDate,
    duration: Duration,
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    let duration = whole_day_duration(duration).map_err(starlark::Error::new_other)?;
    let date = date.checked_add_signed(duration).ok_or_else(|| {
        starlark::Error::new_other(StarlarkBuiltinError::new("date arithmetic overflowed"))
    })?;
    Ok(heap.alloc(AqDate::new(date)))
}

fn subtract_duration_from_date<'v>(
    date: NaiveDate,
    duration: Duration,
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    let duration = whole_day_duration(duration).map_err(starlark::Error::new_other)?;
    let date = date.checked_sub_signed(duration).ok_or_else(|| {
        starlark::Error::new_other(StarlarkBuiltinError::new("date arithmetic overflowed"))
    })?;
    Ok(heap.alloc(AqDate::new(date)))
}

fn add_duration_to_datetime<'v>(
    datetime: &DateTime<Utc>,
    duration: Duration,
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    let datetime = datetime.checked_add_signed(duration).ok_or_else(|| {
        starlark::Error::new_other(StarlarkBuiltinError::new("datetime arithmetic overflowed"))
    })?;
    Ok(heap.alloc(AqDateTime::new(datetime)))
}

fn subtract_duration_from_datetime<'v>(
    datetime: &DateTime<Utc>,
    duration: Duration,
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    let datetime = datetime.checked_sub_signed(duration).ok_or_else(|| {
        starlark::Error::new_other(StarlarkBuiltinError::new("datetime arithmetic overflowed"))
    })?;
    Ok(heap.alloc(AqDateTime::new(datetime)))
}

fn add_durations<'v>(
    left: Duration,
    right: Duration,
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    let duration = left.checked_add(&right).ok_or_else(|| {
        starlark::Error::new_other(StarlarkBuiltinError::new("timedelta arithmetic overflowed"))
    })?;
    Ok(heap.alloc(AqTimedelta::new(duration)))
}

fn subtract_durations<'v>(
    left: Duration,
    right: Duration,
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    let duration = left.checked_sub(&right).ok_or_else(|| {
        starlark::Error::new_other(StarlarkBuiltinError::new("timedelta arithmetic overflowed"))
    })?;
    Ok(heap.alloc(AqTimedelta::new(duration)))
}

pub fn evaluate_inline(
    source: &str,
    input: &Value,
    context: &StarlarkContext,
) -> Result<Value, AqError> {
    let globals = globals_for(context.capabilities);
    let module = Module::new();
    let heap = module.heap();
    module.set(
        "data",
        to_starlark_value(input, heap).map_err(|error| AqError::Starlark(error.to_string()))?,
    );
    let ast = AstModule::parse("<expr.star>", source.to_owned(), &Dialect::Standard)
        .map_err(|error| AqError::InvalidStarlark(error.to_string()))?;
    install_runtime_context(
        &module,
        heap,
        context.detect_conflicts,
        context.current_format_name.as_deref(),
        &context.base_dir,
    );
    let result = evaluate_ast_in_module(&module, &globals, ast, context)?;
    from_starlark_value(result)
}

pub fn evaluate_file(
    path: &Path,
    input: &Value,
    context: &StarlarkContext,
) -> Result<Value, AqError> {
    let source = std::fs::read_to_string(path)
        .map_err(|error| AqError::io(Some(path.to_path_buf()), error))?;
    let globals = globals_for(context.capabilities);
    let module = Module::new();
    let heap = module.heap();
    let data_value =
        to_starlark_value(input, heap).map_err(|error| AqError::Starlark(error.to_string()))?;
    module.set("data", data_value);

    let filename = path.to_string_lossy().into_owned();
    let ast = AstModule::parse(&filename, source, &Dialect::Standard)
        .map_err(|error| AqError::InvalidStarlark(error.to_string()))?;
    install_runtime_context(
        &module,
        heap,
        context.detect_conflicts,
        context.current_format_name.as_deref(),
        &context.base_dir,
    );
    evaluate_ast_in_module(&module, &globals, ast, context)?;
    let main = module
        .get("main")
        .ok_or_else(|| AqError::Starlark("starlark file must define main(data)".to_string()))?;
    let mut evaluator = Evaluator::new(&module);
    let result = evaluator
        .eval_function(main, &[data_value], &[])
        .map_err(|error| AqError::Starlark(error.to_string()))?;
    from_starlark_value(result)
}

pub enum StarlarkReplValue {
    Aq(Value),
    Starlark(String),
}

pub struct StarlarkReplSession {
    context: StarlarkContext,
    globals: Globals,
    module: Module,
    aq_names: Vec<String>,
}

impl StarlarkReplSession {
    pub fn new(input: &Value, context: StarlarkContext) -> Result<Self, AqError> {
        let globals = globals_for(context.capabilities);
        let module = Module::new();
        let heap = module.heap();
        let data_value =
            to_starlark_value(input, heap).map_err(|error| AqError::Starlark(error.to_string()))?;
        module.set("data", data_value);
        install_runtime_context(
            &module,
            heap,
            context.detect_conflicts,
            context.current_format_name.as_deref(),
            &context.base_dir,
        );
        let aq_names = repl_namespace_names(&module, &globals, &context, "aq")?;
        Ok(Self {
            context,
            globals,
            module,
            aq_names,
        })
    }

    pub fn reset(&mut self, input: &Value) -> Result<(), AqError> {
        *self = Self::new(input, self.context.clone())?;
        Ok(())
    }

    pub fn evaluate(&self, source: &str) -> Result<Option<StarlarkReplValue>, AqError> {
        let result = self.evaluate_value("<repl.star>", source)?;
        Ok(self.record_result(result))
    }

    pub fn evaluate_file_in_session(
        &self,
        path: &Path,
    ) -> Result<Option<StarlarkReplValue>, AqError> {
        let resolved = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.context.base_dir.join(path)
        };
        let source = std::fs::read_to_string(&resolved)
            .map_err(|error| AqError::io(Some(resolved.clone()), error))?;
        let filename = resolved.to_string_lossy().into_owned();
        let result = self.evaluate_value(&filename, &source)?;
        Ok(self.record_result(result))
    }

    pub fn set_data_from_source(&self, source: &str) -> Result<StarlarkReplValue, AqError> {
        let result = self.evaluate_value("<repl-data.star>", source)?;
        self.module.set("data", result);
        Ok(self
            .record_result(result)
            .unwrap_or(StarlarkReplValue::Aq(Value::Null)))
    }

    pub fn current_data(&self) -> Result<StarlarkReplValue, AqError> {
        let value = self
            .module
            .get("data")
            .ok_or_else(|| AqError::Starlark("starlark repl lost top-level data".to_string()))?;
        Ok(repl_value_from_starlark_or_null(value))
    }

    pub fn current_format_name(&self) -> Option<&str> {
        self.context.current_format_name.as_deref()
    }

    pub fn capabilities(&self) -> StarlarkCapabilities {
        self.context.capabilities
    }

    pub fn names(&self) -> Vec<String> {
        let mut names = self
            .module
            .names()
            .map(|name| name.as_str().to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn base_dir(&self) -> &Path {
        &self.context.base_dir
    }

    pub fn aq_names(&self) -> &[String] {
        &self.aq_names
    }

    pub fn name_type(&self, name: &str) -> Option<String> {
        self.module
            .get(name)
            .map(|value| value.get_type().to_owned())
    }

    pub fn evaluate_type(&self, source: &str) -> Result<String, AqError> {
        let value = self.evaluate_value("<repl-type.star>", source)?;
        Ok(value.get_type().to_owned())
    }

    fn evaluate_value<'v>(
        &'v self,
        filename: &str,
        source: &str,
    ) -> Result<StarlarkValue<'v>, AqError> {
        let ast = AstModule::parse(filename, source.to_owned(), &Dialect::Standard)
            .map_err(|error| AqError::InvalidStarlark(error.to_string()))?;
        evaluate_ast_in_module(&self.module, &self.globals, ast, &self.context)
    }

    fn record_result<'v>(&'v self, value: StarlarkValue<'v>) -> Option<StarlarkReplValue> {
        if value.is_none() {
            return None;
        }
        if let Some(previous) = self.module.get("ans") {
            self.module.set("prev", previous);
        }
        self.module.set("ans", value);
        self.module.set("_", value);
        Some(repl_value_from_starlark_or_null(value))
    }
}

fn repl_value_from_starlark_or_null(value: StarlarkValue) -> StarlarkReplValue {
    match from_starlark_value(value) {
        Ok(value) => StarlarkReplValue::Aq(value),
        Err(_) => StarlarkReplValue::Starlark(value.to_repr()),
    }
}

fn repl_namespace_names(
    module: &Module,
    globals: &Globals,
    context: &StarlarkContext,
    name: &str,
) -> Result<Vec<String>, AqError> {
    let source = format!("dir({name})");
    let ast = AstModule::parse("<repl-introspect.star>", source, &Dialect::Standard)
        .map_err(|error| AqError::InvalidStarlark(error.to_string()))?;
    let value = evaluate_ast_in_module(module, globals, ast, context)?;
    let value = from_starlark_value(value)?;
    let Value::Array(values) = value else {
        return Err(AqError::Starlark(format!(
            "expected dir({name}) to produce an array"
        )));
    };
    let mut names = Vec::with_capacity(values.len());
    for value in values {
        let Value::String(name) = value else {
            return Err(AqError::Starlark(format!(
                "expected dir({name}) to contain only strings"
            )));
        };
        names.push(name);
    }
    names.sort();
    Ok(names)
}

pub fn aq_helper_description(name: &str) -> Option<&'static str> {
    match name {
        "base64_decode" => Some("decode UTF-8 text from base64"),
        "base64_encode" => Some("encode UTF-8 text as base64"),
        "base_dir" => Some("return the current base directory, requires filesystem"),
        "blake3" => Some("hash text with BLAKE3"),
        "camel_case" => Some("normalize text to camelCase"),
        "clean_k8s_metadata" => {
            Some("keep portable Kubernetes manifest metadata and drop live-object fields")
        }
        "collect_paths" => Some("collect transformed values from matching paths"),
        "copy" => Some("copy a file or directory path, requires filesystem"),
        "date" => Some("parse a string into aq.date"),
        "datetime" => Some("parse a string into aq.datetime"),
        "delete_path" => Some("delete one deep path from a value"),
        "delete_paths" => Some("delete multiple deep paths from a value"),
        "drop_nulls" => Some("recursively remove nulls from arrays and objects"),
        "env" => Some("read an environment variable, requires environment"),
        "exists" => Some("test whether a path exists, requires filesystem"),
        "find_paths" => Some("return paths selected by a predicate"),
        "format" => Some("return the current input format name"),
        "get_path" => Some("read one deep path from a value"),
        "glob" => Some("glob paths relative to the base dir, requires filesystem"),
        "hash" => Some("hash text with sha1, sha256, sha512, or blake3"),
        "hash_file" => Some("hash a file's contents, requires filesystem"),
        "is_dir" => Some("test whether a path is a directory, requires filesystem"),
        "is_file" => Some("test whether a path is a file, requires filesystem"),
        "kebab_case" => Some("normalize text to kebab-case"),
        "list_dir" => Some("list directory entries, requires filesystem"),
        "merge" => Some("deep-merge two values"),
        "merge_all" => Some("deep-merge an array of values"),
        "mkdir" => Some("create a directory, requires filesystem"),
        "now" => Some("return the current UTC datetime, requires time"),
        "omit_paths" => Some("remove selected deep paths from a value"),
        "omit_where" => Some("remove values selected by a path predicate"),
        "parse" => Some("parse text in a specific data format"),
        "parse_all" => Some("parse text in a format and always return an array"),
        "paths" => Some("list paths in a value"),
        "pick_paths" => Some("project a value down to selected paths"),
        "pick_where" => Some("project values selected by a path predicate"),
        "query_all" => Some("run an aq query and return all results"),
        "query_one" => Some("run an aq query and require exactly one result"),
        "read" => Some("read and parse one file with format detection, requires filesystem"),
        "read_all" => Some("read and parse all documents from one file, requires filesystem"),
        "read_all_as" => {
            Some("read all documents from one file as an explicit format, requires filesystem")
        }
        "read_as" => Some("read one file as an explicit format, requires filesystem"),
        "read_glob" => Some("read matching files with format detection, requires filesystem"),
        "read_glob_all" => Some("read all documents from matching files, requires filesystem"),
        "read_glob_all_as" => Some(
            "read matching files as an explicit format and keep all documents, requires filesystem",
        ),
        "read_glob_as" => Some("read matching files as an explicit format, requires filesystem"),
        "read_text" => Some("read a UTF-8 text file, requires filesystem"),
        "read_text_glob" => Some("read matching UTF-8 text files, requires filesystem"),
        "regex_capture" => Some("capture named and positional regex groups"),
        "regex_capture_all" => Some("capture regex groups for every match"),
        "regex_escape" => Some("escape text for literal use in a regex"),
        "regex_find" => Some("return the first regex match"),
        "regex_find_all" => Some("return all regex matches"),
        "regex_is_match" => Some("test whether a regex matches"),
        "regex_replace" => Some("replace the first regex match"),
        "regex_replace_all" => Some("replace every regex match"),
        "regex_split" => Some("split text with a regex pattern"),
        "relative_path" => Some("compute a relative path, requires filesystem"),
        "remove" => Some("remove a file or directory path, requires filesystem"),
        "rename" => Some("rename or move a path, requires filesystem"),
        "render" => Some("render one value in a specific format"),
        "render_all" => Some("render many values with aq output semantics"),
        "resolve_path" => Some("resolve a path against the base dir, requires filesystem"),
        "rewrite_text" => Some("rewrite one text file via callback, requires filesystem"),
        "rewrite_text_glob" => {
            Some("rewrite matching text files via callback, requires filesystem")
        }
        "semver_bump" => Some("bump a semantic version"),
        "semver_compare" => Some("compare two semantic versions"),
        "semver_parse" => Some("parse a semantic version into structured parts"),
        "set_path" => Some("set one deep path in a value"),
        "sha1" => Some("hash text with SHA-1"),
        "sha256" => Some("hash text with SHA-256"),
        "sha512" => Some("hash text with SHA-512"),
        "shell_escape" => Some("quote text for a POSIX shell"),
        "slug" => Some("normalize text to a URL-safe slug"),
        "snake_case" => Some("normalize text to snake_case"),
        "sort_keys" => Some("recursively sort object keys"),
        "stat" => Some("stat a filesystem path, requires filesystem"),
        "timedelta" => Some("construct an aq.timedelta duration"),
        "timestamp" => Some("return the current UNIX timestamp, requires time"),
        "title_case" => Some("normalize text to Title Case"),
        "today" => Some("return the current UTC date, requires time"),
        "trim_prefix" => Some("remove a prefix if present"),
        "trim_suffix" => Some("remove a suffix if present"),
        "url_decode_component" => Some("percent-decode one URL component"),
        "url_encode_component" => Some("percent-encode one URL component"),
        "walk" => Some("recursively rewrite every value"),
        "walk_files" => Some("recursively list files, requires filesystem"),
        "walk_paths" => Some("recursively rewrite values with path context"),
        "write" => Some("render one value to a file, requires filesystem"),
        "write_all" => Some("render many values to one file, requires filesystem"),
        "write_batch" => Some("render one value per entry to many files, requires filesystem"),
        "write_batch_all" => {
            Some("render many values per entry to many files, requires filesystem")
        }
        "write_text" => Some("write UTF-8 text to a file, requires filesystem"),
        "write_text_batch" => Some("write UTF-8 text to many files, requires filesystem"),
        _ => None,
    }
}

pub fn aq_helper_signature(name: &str) -> Option<&'static str> {
    match name {
        "base64_decode" => Some("aq.base64_decode(text, urlsafe = False)"),
        "base64_encode" => Some("aq.base64_encode(text, urlsafe = False, pad = True)"),
        "base_dir" => Some("aq.base_dir()"),
        "blake3" => Some("aq.blake3(text, encoding = \"hex\")"),
        "camel_case" => Some("aq.camel_case(text)"),
        "clean_k8s_metadata" => Some("aq.clean_k8s_metadata(value)"),
        "collect_paths" => Some("aq.collect_paths(value, function, leaves_only = False)"),
        "copy" => Some("aq.copy(source, destination, overwrite = False)"),
        "date" => Some("aq.date(text)"),
        "datetime" => Some("aq.datetime(text)"),
        "delete_path" => Some("aq.delete_path(value, path)"),
        "delete_paths" => Some("aq.delete_paths(value, paths)"),
        "drop_nulls" => Some("aq.drop_nulls(value, recursive = False)"),
        "env" => Some("aq.env(name)"),
        "exists" => Some("aq.exists(path)"),
        "find_paths" => Some("aq.find_paths(value, function, leaves_only = False)"),
        "format" => Some("aq.format()"),
        "get_path" => Some("aq.get_path(value, path)"),
        "glob" => Some("aq.glob(pattern, include_dirs = False)"),
        "hash" => Some("aq.hash(text, algorithm = \"sha256\", encoding = \"hex\")"),
        "hash_file" => Some("aq.hash_file(path, algorithm = \"sha256\", encoding = \"hex\")"),
        "is_dir" => Some("aq.is_dir(path)"),
        "is_file" => Some("aq.is_file(path)"),
        "kebab_case" => Some("aq.kebab_case(text)"),
        "list_dir" => Some("aq.list_dir(path = \".\")"),
        "merge" => Some("aq.merge(left, right, deep = False)"),
        "merge_all" => Some("aq.merge_all(values, deep = False)"),
        "mkdir" => Some("aq.mkdir(path, parents = False)"),
        "now" => Some("aq.now()"),
        "omit_paths" => Some("aq.omit_paths(value, paths)"),
        "omit_where" => Some("aq.omit_where(value, function, leaves_only = False)"),
        "parse" => Some("aq.parse(text, format)"),
        "parse_all" => Some("aq.parse_all(text, format)"),
        "paths" => Some("aq.paths(value, leaves_only = False)"),
        "pick_paths" => Some("aq.pick_paths(value, paths)"),
        "pick_where" => Some("aq.pick_where(value, function, leaves_only = False)"),
        "query_all" => Some("aq.query_all(expr, input)"),
        "query_one" => Some("aq.query_one(expr, input)"),
        "read" => Some("aq.read(path)"),
        "read_all" => Some("aq.read_all(path)"),
        "read_all_as" => Some("aq.read_all_as(path, format)"),
        "read_as" => Some("aq.read_as(path, format)"),
        "read_glob" => Some("aq.read_glob(pattern)"),
        "read_glob_all" => Some("aq.read_glob_all(pattern)"),
        "read_glob_all_as" => Some("aq.read_glob_all_as(pattern, format)"),
        "read_glob_as" => Some("aq.read_glob_as(pattern, format)"),
        "read_text" => Some("aq.read_text(path)"),
        "read_text_glob" => Some("aq.read_text_glob(pattern)"),
        "regex_capture" => Some("aq.regex_capture(pattern, text)"),
        "regex_capture_all" => Some("aq.regex_capture_all(pattern, text)"),
        "regex_escape" => Some("aq.regex_escape(text)"),
        "regex_find" => Some("aq.regex_find(pattern, text)"),
        "regex_find_all" => Some("aq.regex_find_all(pattern, text)"),
        "regex_is_match" => Some("aq.regex_is_match(pattern, text)"),
        "regex_replace" => Some("aq.regex_replace(pattern, replacement, text)"),
        "regex_replace_all" => Some("aq.regex_replace_all(pattern, replacement, text)"),
        "regex_split" => Some("aq.regex_split(pattern, text)"),
        "relative_path" => Some("aq.relative_path(path, start = \".\")"),
        "remove" => Some("aq.remove(path, recursive = False, missing_ok = False)"),
        "rename" => Some("aq.rename(source, destination, overwrite = False)"),
        "render" => Some("aq.render(value, format, compact = False)"),
        "render_all" => Some("aq.render_all(values, format, compact = False)"),
        "resolve_path" => Some("aq.resolve_path(path)"),
        "rewrite_text" => Some("aq.rewrite_text(path, function)"),
        "rewrite_text_glob" => Some("aq.rewrite_text_glob(pattern, function)"),
        "semver_bump" => Some("aq.semver_bump(text, part, prerelease_label = \"rc\")"),
        "semver_compare" => Some("aq.semver_compare(left, right)"),
        "semver_parse" => Some("aq.semver_parse(text)"),
        "set_path" => Some("aq.set_path(value, path, replacement)"),
        "sha1" => Some("aq.sha1(text, encoding = \"hex\")"),
        "sha256" => Some("aq.sha256(text, encoding = \"hex\")"),
        "sha512" => Some("aq.sha512(text, encoding = \"hex\")"),
        "shell_escape" => Some("aq.shell_escape(text)"),
        "slug" => Some("aq.slug(text)"),
        "snake_case" => Some("aq.snake_case(text)"),
        "sort_keys" => Some("aq.sort_keys(value, recursive = False)"),
        "stat" => Some("aq.stat(path)"),
        "timedelta" => Some(
            "aq.timedelta(weeks = 0, days = 0, hours = 0, minutes = 0, seconds = 0, milliseconds = 0, microseconds = 0, nanoseconds = 0)",
        ),
        "timestamp" => Some("aq.timestamp()"),
        "title_case" => Some("aq.title_case(text)"),
        "today" => Some("aq.today()"),
        "trim_prefix" => Some("aq.trim_prefix(text, prefix)"),
        "trim_suffix" => Some("aq.trim_suffix(text, suffix)"),
        "url_decode_component" => Some("aq.url_decode_component(text)"),
        "url_encode_component" => Some("aq.url_encode_component(text)"),
        "walk" => Some("aq.walk(value, function)"),
        "walk_files" => Some("aq.walk_files(path = \".\", include_dirs = False)"),
        "walk_paths" => Some("aq.walk_paths(value, function)"),
        "write" => Some("aq.write(path, value, format, compact = False, parents = False)"),
        "write_all" => Some("aq.write_all(path, values, format, compact = False, parents = False)"),
        "write_batch" => {
            Some("aq.write_batch(entries, format, compact = False, parents = False)")
        }
        "write_batch_all" => {
            Some("aq.write_batch_all(entries, format, compact = False, parents = False)")
        }
        "write_text" => Some("aq.write_text(path, text, parents = False)"),
        "write_text_batch" => Some("aq.write_text_batch(entries, parents = False)"),
        _ => None,
    }
}

pub fn aq_helper_completion_detail(name: &str) -> Option<String> {
    match (aq_helper_signature(name), aq_helper_description(name)) {
        (Some(signature), Some(description)) => Some(format!("{signature}\n{description}")),
        (Some(signature), None) => Some(signature.to_owned()),
        (None, Some(description)) => Some(description.to_owned()),
        (None, None) => None,
    }
}

pub fn starlark_top_level_builtin_description(name: &str) -> Option<&'static str> {
    match name {
        "log" => Some("write one Starlark value to stderr and return None"),
        _ => None,
    }
}

pub fn starlark_top_level_builtin_signature(name: &str) -> Option<&'static str> {
    match name {
        "log" => Some("log(value)"),
        _ => None,
    }
}

pub fn starlark_top_level_builtin_completion_detail(name: &str) -> Option<String> {
    match (
        starlark_top_level_builtin_signature(name),
        starlark_top_level_builtin_description(name),
    ) {
        (Some(signature), Some(description)) => Some(format!("{signature}\n{description}")),
        (Some(signature), None) => Some(signature.to_owned()),
        (None, Some(description)) => Some(description.to_owned()),
        (None, None) => None,
    }
}

fn evaluate_ast_in_module<'v>(
    module: &'v Module,
    globals: &'v Globals,
    ast: AstModule,
    context: &StarlarkContext,
) -> Result<StarlarkValue<'v>, AqError> {
    let loads = load_dependencies(&ast, globals, context, &context.base_dir)?;
    let load_map = loads
        .iter()
        .map(|(module_id, module)| (module_id.as_str(), module))
        .collect::<HashMap<_, _>>();
    let loader = (!load_map.is_empty()).then_some(ReturnFileLoader { modules: &load_map });
    let mut evaluator = Evaluator::new(module);
    if let Some(loader) = &loader {
        evaluator.set_loader(loader);
    }
    evaluator
        .eval_module(ast, globals)
        .map_err(|error| AqError::Starlark(error.to_string()))
}

fn install_runtime_context(
    module: &Module,
    heap: &Heap,
    detect_conflicts: DetectConflictPolicy,
    current_format_name: Option<&str>,
    base_dir: &Path,
) {
    module.set_extra_value(heap.alloc((
        detect_conflicts.to_string(),
        current_format_name.unwrap_or_default().to_owned(),
        base_dir.to_string_lossy().into_owned(),
    )));
}

fn globals_for(capabilities: StarlarkCapabilities) -> Globals {
    let mut builder = GlobalsBuilder::standard();
    starlark_top_level(&mut builder);
    builder.namespace("aq", |aq| {
        aq_core(aq);

        if capabilities.filesystem {
            aq_filesystem_enabled(aq);
        } else {
            aq_filesystem_disabled(aq);
        }

        if capabilities.environment {
            aq_env_enabled(aq);
        } else {
            aq_env_disabled(aq);
        }

        if capabilities.time {
            aq_time_enabled(aq);
        } else {
            aq_time_disabled(aq);
        }
    });
    builder.build()
}

#[starlark_module]
fn starlark_top_level(builder: &mut GlobalsBuilder) {
    fn log<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let rendered = value.to_repr();
        let mut stderr = std::io::stderr().lock();
        stderr.write_all(rendered.as_bytes()).map_err(|error| {
            starlark::Error::new_other(StarlarkBuiltinError::new(format!(
                "log failed to write to stderr: {error}"
            )))
        })?;
        stderr.write_all(b"\n").map_err(|error| {
            starlark::Error::new_other(StarlarkBuiltinError::new(format!(
                "log failed to write to stderr: {error}"
            )))
        })?;
        stderr.flush().map_err(|error| {
            starlark::Error::new_other(StarlarkBuiltinError::new(format!(
                "log failed to flush stderr: {error}"
            )))
        })?;
        Ok(StarlarkValue::new_none())
    }
}

fn from_starlark_value(value: StarlarkValue) -> Result<Value, AqError> {
    if value.is_none() {
        return Ok(Value::Null);
    }
    if let Some(value) = value.unpack_bool() {
        return Ok(Value::Bool(value));
    }
    if let Some(value) = i64::unpack_value(value).map_err(|error| {
        AqError::Starlark(format!("starlark integer conversion failed: {error}"))
    })? {
        return Ok(Value::Integer(value));
    }
    if let Some(value) = value.unpack_str() {
        return Ok(Value::String(value.to_owned()));
    }
    if let Some(value) = AqDate::from_value(value) {
        return Ok(Value::Date(value.date));
    }
    if let Some(value) = AqDateTime::from_value(value) {
        return Ok(Value::DateTime(value.datetime));
    }
    if AqTimedelta::from_value(value).is_some() {
        return Err(AqError::Starlark(
            "starlark results cannot contain timedelta values".to_string(),
        ));
    }
    if let Some(list) = ListRef::from_value(value) {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(from_starlark_value(item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Some(tuple) = TupleRef::from_value(value) {
        let mut out = Vec::with_capacity(tuple.len());
        for item in tuple.content() {
            out.push(from_starlark_value(*item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Some(dict) = DictRef::from_value(value) {
        let mut out = IndexMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let Some(key) = key.unpack_str() else {
                return Err(AqError::Starlark(
                    "starlark objects must use string keys".to_string(),
                ));
            };
            out.insert(key.to_owned(), from_starlark_value(value)?);
        }
        return Ok(Value::Object(out));
    }
    let json = value.to_json_value().map_err(|error| {
        AqError::Starlark(format!(
            "starlark results must be aq-compatible values: {error}"
        ))
    })?;
    Value::from_json(json)
}

fn runtime_context(
    eval: &mut Evaluator<'_, '_, '_>,
) -> Result<(DetectConflictPolicy, Option<String>, PathBuf), StarlarkBuiltinError> {
    let extra = eval.module().extra_value().ok_or_else(|| {
        StarlarkBuiltinError::new("internal error: missing starlark runtime context")
    })?;
    let tuple = TupleRef::from_value(extra).ok_or_else(|| {
        StarlarkBuiltinError::new("internal error: invalid starlark runtime context")
    })?;
    if tuple.len() != 3 {
        return Err(StarlarkBuiltinError::new(
            "internal error: malformed starlark runtime context",
        ));
    }

    let detect_conflicts = tuple.content()[0].unpack_str().ok_or_else(|| {
        StarlarkBuiltinError::new("internal error: malformed starlark detect-conflicts context")
    })?;
    let current_format_name = tuple.content()[1].unpack_str().ok_or_else(|| {
        StarlarkBuiltinError::new("internal error: malformed starlark format context")
    })?;
    let base_dir = tuple.content()[2].unpack_str().ok_or_else(|| {
        StarlarkBuiltinError::new("internal error: malformed starlark base-dir context")
    })?;

    let detect_conflicts = parse_detect_conflicts_name(detect_conflicts)?;
    let current_format_name = if current_format_name.is_empty() {
        None
    } else {
        Some(current_format_name.to_owned())
    };
    Ok((
        detect_conflicts,
        current_format_name,
        PathBuf::from(base_dir),
    ))
}

fn parse_detect_conflicts_name(raw: &str) -> Result<DetectConflictPolicy, StarlarkBuiltinError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "warn-fallback" | "warn_fallback" => Ok(DetectConflictPolicy::WarnFallback),
        "extension" => Ok(DetectConflictPolicy::Extension),
        "sniff" => Ok(DetectConflictPolicy::Sniff),
        other => Err(StarlarkBuiltinError::new(format!(
            "invalid detect-conflicts policy `{other}`"
        ))),
    }
}

fn parse_format_name(raw: &str) -> Result<Format, StarlarkBuiltinError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "json" => Ok(Format::Json),
        "jsonl" | "ndjson" => Ok(Format::Jsonl),
        "toml" => Ok(Format::Toml),
        "yaml" | "yml" => Ok(Format::Yaml),
        "csv" => Ok(Format::Csv),
        "tsv" => Ok(Format::Tsv),
        "table" => Ok(Format::Table),
        other => Err(StarlarkBuiltinError::new(format!(
            "unsupported format `{other}`, expected one of: json, jsonl, toml, yaml, csv, tsv, table"
        ))),
    }
}

fn to_starlark_value<'v>(value: &Value, heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
    match value.untagged() {
        Value::Null => Ok(StarlarkValue::new_none()),
        Value::Bool(value) => Ok(StarlarkValue::new_bool(*value)),
        Value::Integer(value) => Ok(heap.alloc(*value)),
        Value::Decimal(value) => {
            if let Some(value) = value.as_i64_exact() {
                Ok(heap.alloc(value))
            } else {
                Ok(heap.alloc(value.to_f64_lossy()))
            }
        }
        Value::Float(value) => Ok(heap.alloc(*value)),
        Value::String(value) => Ok(heap.alloc(value.as_str())),
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(to_starlark_value(value, heap)?);
            }
            Ok(heap.alloc(out))
        }
        Value::Object(values) => {
            let mut out = SmallMap::with_capacity(values.len());
            for (key, value) in values {
                out.insert(key.clone(), to_starlark_value(value, heap)?);
            }
            Ok(heap.alloc(out))
        }
        Value::Bytes(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(heap.alloc(i64::from(*value)));
            }
            Ok(heap.alloc(out))
        }
        Value::DateTime(value) => Ok(heap.alloc(AqDateTime::new(*value))),
        Value::Date(value) => Ok(heap.alloc(AqDate::new(*value))),
        Value::Tagged { .. } => unreachable!("untagged values should not be tagged"),
    }
}

fn to_starlark_array<'v>(values: &[Value], heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(to_starlark_value(value, heap)?);
    }
    Ok(heap.alloc(out))
}

fn to_starlark_json_value<'v>(
    value: &Value,
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    to_starlark_value(value, heap)
}

fn to_starlark_json_array<'v>(
    values: &[Value],
    heap: &'v Heap,
) -> starlark::Result<StarlarkValue<'v>> {
    to_starlark_array(values, heap)
}

fn call_starlark_transform<'v>(
    eval: &mut Evaluator<'v, '_, '_>,
    function: StarlarkValue<'v>,
    args: &[Value],
) -> Result<Value, StarlarkBuiltinError> {
    let mut starlark_args = Vec::with_capacity(args.len());
    for arg in args {
        starlark_args.push(
            to_starlark_value(arg, eval.heap())
                .map_err(|error| StarlarkBuiltinError::new(error.to_string()))?,
        );
    }
    let result = eval
        .eval_function(function, &starlark_args, &[])
        .map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
    from_starlark_value(result).map_err(|error| StarlarkBuiltinError::new(error.to_string()))
}

fn collapse_documents(documents: Vec<InputDocument>) -> Value {
    let values = documents
        .into_iter()
        .map(|document| document.value)
        .collect::<Vec<_>>();
    collapse_values(values)
}

fn all_documents(documents: Vec<InputDocument>) -> Vec<Value> {
    documents
        .into_iter()
        .map(|document| document.value)
        .collect()
}

fn collapse_values(values: Vec<Value>) -> Value {
    let mut values = values.into_iter();
    match (values.next(), values.next()) {
        (None, _) => Value::Array(Vec::new()),
        (Some(first), None) => first,
        (Some(first), Some(second)) => {
            let mut out = vec![first, second];
            out.extend(values);
            Value::Array(out)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StarlarkPathComponent {
    Field(String),
    Index(isize),
}

fn starlark_kind_name(value: &Value) -> &'static str {
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

fn resolve_array_index(index: isize, len: usize) -> Option<usize> {
    if index >= 0 {
        usize::try_from(index).ok().filter(|index| *index < len)
    } else {
        len.checked_sub(index.unsigned_abs())
    }
}

fn starlark_path_component_of(
    name: &str,
    value: &Value,
) -> Result<StarlarkPathComponent, StarlarkBuiltinError> {
    match value.untagged() {
        Value::String(value) => Ok(StarlarkPathComponent::Field(value.clone())),
        Value::Integer(value) => Ok(StarlarkPathComponent::Index(
            isize::try_from(*value).map_err(|_| {
                StarlarkBuiltinError::new(format!("{name} path index is out of range"))
            })?,
        )),
        other => Err(StarlarkBuiltinError::new(format!(
            "{name} path components must be strings or integers, got {}",
            starlark_kind_name(other)
        ))),
    }
}

fn starlark_path_of(
    name: &str,
    value: &Value,
) -> Result<Vec<StarlarkPathComponent>, StarlarkBuiltinError> {
    let Value::Array(components) = value.untagged() else {
        return Err(StarlarkBuiltinError::new(format!(
            "{name} expects an array path"
        )));
    };

    components
        .iter()
        .map(|component| starlark_path_component_of(name, component))
        .collect()
}

fn starlark_paths_of(
    name: &str,
    value: &Value,
) -> Result<Vec<Vec<StarlarkPathComponent>>, StarlarkBuiltinError> {
    let Value::Array(paths) = value.untagged() else {
        return Err(StarlarkBuiltinError::new(format!(
            "{name} expects an array of paths"
        )));
    };

    paths
        .iter()
        .map(|path| starlark_path_of(name, path))
        .collect()
}

fn get_path_value(
    input: &Value,
    path: &[StarlarkPathComponent],
) -> Result<Value, StarlarkBuiltinError> {
    let mut current = input.clone();
    for component in path {
        current = match component {
            StarlarkPathComponent::Field(name) => match current.untagged() {
                Value::Object(fields) => fields.get(name).cloned().unwrap_or(Value::Null),
                Value::Null => Value::Null,
                Value::Array(_) => {
                    return Err(StarlarkBuiltinError::new("cannot index array with string"));
                }
                other => {
                    return Err(StarlarkBuiltinError::new(format!(
                        "cannot index {} with string",
                        starlark_kind_name(other)
                    )));
                }
            },
            StarlarkPathComponent::Index(index) => match current.untagged() {
                Value::Array(items) => resolve_array_index(*index, items.len())
                    .and_then(|index| items.get(index).cloned())
                    .unwrap_or(Value::Null),
                Value::Null => Value::Null,
                Value::Object(_) => {
                    return Err(StarlarkBuiltinError::new("cannot index object with number"));
                }
                other => {
                    return Err(StarlarkBuiltinError::new(format!(
                        "cannot index {} with number",
                        starlark_kind_name(other)
                    )));
                }
            },
        };
    }
    Ok(current)
}

fn set_path_value(
    input: &Value,
    path: &[StarlarkPathComponent],
    replacement: &Value,
) -> Result<Value, StarlarkBuiltinError> {
    if let Value::Tagged { value, .. } = input {
        return Ok(input.retagged_like(set_path_value(value, path, replacement)?));
    }

    let Some((component, tail)) = path.split_first() else {
        return Ok(replacement.clone());
    };

    match component {
        StarlarkPathComponent::Field(name) => match input {
            Value::Object(fields) => {
                let child = fields.get(name).cloned().unwrap_or(Value::Null);
                let updated_child = set_path_value(&child, tail, replacement)?;
                let mut updated = fields.clone();
                updated.insert(name.clone(), updated_child);
                Ok(Value::Object(updated))
            }
            Value::Null => {
                let updated_child = set_path_value(&Value::Null, tail, replacement)?;
                let mut updated = IndexMap::new();
                updated.insert(name.clone(), updated_child);
                Ok(Value::Object(updated))
            }
            Value::Array(_) => Err(StarlarkBuiltinError::new("cannot index array with string")),
            other => Err(StarlarkBuiltinError::new(format!(
                "cannot index {} with string",
                starlark_kind_name(other)
            ))),
        },
        StarlarkPathComponent::Index(index) => set_path_index(input, *index, tail, replacement),
    }
}

fn set_path_index(
    input: &Value,
    index: isize,
    tail: &[StarlarkPathComponent],
    replacement: &Value,
) -> Result<Value, StarlarkBuiltinError> {
    match input {
        Value::Array(items) => {
            let resolved = if index < 0 {
                resolve_array_index(index, items.len()).ok_or_else(|| {
                    StarlarkBuiltinError::new("out of bounds negative array index")
                })?
            } else {
                usize::try_from(index)
                    .map_err(|_| StarlarkBuiltinError::new("array index is out of range"))?
            };
            let child = items.get(resolved).cloned().unwrap_or(Value::Null);
            let updated_child = set_path_value(&child, tail, replacement)?;
            let mut updated = items.clone();
            if resolved >= updated.len() {
                updated.resize(resolved + 1, Value::Null);
            }
            updated[resolved] = updated_child;
            Ok(Value::Array(updated))
        }
        Value::Null => {
            if index < 0 {
                return Err(StarlarkBuiltinError::new(
                    "out of bounds negative array index",
                ));
            }
            let resolved = usize::try_from(index)
                .map_err(|_| StarlarkBuiltinError::new("array index is out of range"))?;
            let updated_child = set_path_value(&Value::Null, tail, replacement)?;
            let mut updated = Vec::new();
            updated.resize(resolved + 1, Value::Null);
            updated[resolved] = updated_child;
            Ok(Value::Array(updated))
        }
        Value::Object(_) => Err(StarlarkBuiltinError::new("cannot index object with number")),
        other => Err(StarlarkBuiltinError::new(format!(
            "cannot index {} with number",
            starlark_kind_name(other)
        ))),
    }
}

fn delete_paths_value(
    input: &Value,
    paths: &[Vec<StarlarkPathComponent>],
) -> Result<Value, StarlarkBuiltinError> {
    if paths.iter().any(|path| path.is_empty()) {
        return Ok(Value::Null);
    }

    if let Value::Tagged { value, .. } = input {
        return Ok(input.retagged_like(delete_paths_value(value, paths)?));
    }

    match input {
        Value::Object(fields) => delete_paths_object(fields, paths),
        Value::Array(values) => delete_paths_array(values, paths),
        Value::Null => Ok(Value::Null),
        other => Err(StarlarkBuiltinError::new(format!(
            "cannot delete fields from {}",
            starlark_kind_name(other)
        ))),
    }
}

fn clean_k8s_metadata_object(input: &Value) -> Value {
    const PORTABLE_METADATA_FIELDS: &[&str] =
        &["annotations", "generateName", "labels", "name", "namespace"];

    if let Value::Tagged { value, .. } = input {
        return input.retagged_like(clean_k8s_metadata_object(value));
    }

    let Value::Object(fields) = input else {
        return input.clone();
    };

    let mut cleaned = IndexMap::new();
    for field in PORTABLE_METADATA_FIELDS {
        if let Some(value) = fields.get(*field) {
            cleaned.insert((*field).to_string(), value.clone());
        }
    }
    Value::Object(cleaned)
}

fn clean_k8s_metadata_value(input: &Value) -> Result<Value, StarlarkBuiltinError> {
    if let Value::Tagged { value, .. } = input {
        return Ok(input.retagged_like(clean_k8s_metadata_value(value)?));
    }

    match input {
        Value::Array(values) => {
            let mut cleaned = Vec::with_capacity(values.len());
            for value in values {
                cleaned.push(clean_k8s_metadata_value(value)?);
            }
            Ok(Value::Array(cleaned))
        }
        Value::Object(fields) => {
            let mut cleaned = fields.clone();
            if let Some(metadata) = fields.get("metadata") {
                cleaned.insert("metadata".to_string(), clean_k8s_metadata_object(metadata));
            }
            if let Some(items) = fields.get("items") {
                cleaned.insert("items".to_string(), clean_k8s_metadata_value(items)?);
            }
            Ok(Value::Object(cleaned))
        }
        other => Ok(other.clone()),
    }
}

fn path_components_to_value(path: &[StarlarkPathComponent]) -> Value {
    Value::Array(
        path.iter()
            .map(|component| match component {
                StarlarkPathComponent::Field(value) => Value::String(value.clone()),
                StarlarkPathComponent::Index(value) => Value::Integer(*value as i64),
            })
            .collect(),
    )
}

fn value_truthy(value: &Value) -> bool {
    match value.untagged() {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Integer(value) => *value != 0,
        Value::Decimal(value) => value.rendered() != "0",
        Value::Float(value) => *value != 0.0,
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
        Value::Bytes(value) => !value.is_empty(),
        Value::DateTime(_) | Value::Date(_) => true,
        Value::Tagged { .. } => unreachable!("untagged values should not be tagged"),
    }
}

fn collect_paths(
    value: &Value,
    leaves_only: bool,
) -> Result<Vec<Vec<StarlarkPathComponent>>, StarlarkBuiltinError> {
    let mut out = Vec::new();
    let mut path = Vec::new();
    collect_paths_inner(value, &mut path, leaves_only, &mut out)?;
    Ok(out)
}

fn collect_paths_inner(
    value: &Value,
    path: &mut Vec<StarlarkPathComponent>,
    leaves_only: bool,
    out: &mut Vec<Vec<StarlarkPathComponent>>,
) -> Result<(), StarlarkBuiltinError> {
    match value.untagged() {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                path.push(StarlarkPathComponent::Index(
                    isize::try_from(index)
                        .map_err(|_| StarlarkBuiltinError::new("array index is out of range"))?,
                ));
                if !leaves_only || !matches!(value.untagged(), Value::Array(_) | Value::Object(_)) {
                    out.push(path.clone());
                }
                collect_paths_inner(value, path, leaves_only, out)?;
                path.pop();
            }
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                path.push(StarlarkPathComponent::Field(key.clone()));
                if !leaves_only || !matches!(value.untagged(), Value::Array(_) | Value::Object(_)) {
                    out.push(path.clone());
                }
                collect_paths_inner(value, path, leaves_only, out)?;
                path.pop();
            }
        }
        _ => {}
    }
    Ok(())
}

fn find_matching_paths<'v>(
    value: &Value,
    function: StarlarkValue<'v>,
    eval: &mut Evaluator<'v, '_, '_>,
    leaves_only: bool,
) -> Result<Vec<Vec<StarlarkPathComponent>>, StarlarkBuiltinError> {
    let mut matches = Vec::new();
    for path in collect_paths(value, leaves_only)? {
        let current = get_path_value(value, &path)?;
        let keep =
            call_starlark_transform(eval, function, &[path_components_to_value(&path), current])?;
        if value_truthy(&keep) {
            matches.push(path);
        }
    }
    Ok(matches)
}

fn collect_path_values<'v>(
    value: &Value,
    function: StarlarkValue<'v>,
    eval: &mut Evaluator<'v, '_, '_>,
    leaves_only: bool,
) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let mut out = Vec::new();
    for path in collect_paths(value, leaves_only)? {
        let current = get_path_value(value, &path)?;
        out.push(call_starlark_transform(
            eval,
            function,
            &[path_components_to_value(&path), current],
        )?);
    }
    Ok(out)
}

fn pick_paths_value(
    value: &Value,
    paths: &[Vec<StarlarkPathComponent>],
) -> Result<Value, StarlarkBuiltinError> {
    let mut projected = Value::Null;
    for path in paths {
        let current = get_path_value(value, path)?;
        projected = set_path_value(&projected, path, &current)?;
    }
    Ok(projected)
}

fn omit_paths_value(
    value: &Value,
    paths: &[Vec<StarlarkPathComponent>],
) -> Result<Value, StarlarkBuiltinError> {
    delete_paths_value(value, paths)
}

fn walk_value<'v>(
    value: &Value,
    function: StarlarkValue<'v>,
    eval: &mut Evaluator<'v, '_, '_>,
) -> Result<Value, StarlarkBuiltinError> {
    walk_value_with_path(value, &[], function, eval, false)
}

fn walk_value_with_path<'v>(
    value: &Value,
    path: &[StarlarkPathComponent],
    function: StarlarkValue<'v>,
    eval: &mut Evaluator<'v, '_, '_>,
    include_path: bool,
) -> Result<Value, StarlarkBuiltinError> {
    let walked = match value.untagged() {
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for (index, value) in values.iter().enumerate() {
                let mut child_path = path.to_vec();
                child_path.push(StarlarkPathComponent::Index(
                    isize::try_from(index)
                        .map_err(|_| StarlarkBuiltinError::new("array index is out of range"))?,
                ));
                out.push(walk_value_with_path(
                    value,
                    &child_path,
                    function,
                    eval,
                    include_path,
                )?);
            }
            Value::Array(out)
        }
        Value::Object(fields) => {
            let mut out = IndexMap::with_capacity(fields.len());
            for (key, value) in fields {
                let mut child_path = path.to_vec();
                child_path.push(StarlarkPathComponent::Field(key.clone()));
                out.insert(
                    key.clone(),
                    walk_value_with_path(value, &child_path, function, eval, include_path)?,
                );
            }
            Value::Object(out)
        }
        _ => value.clone(),
    };

    let transformed = if include_path {
        call_starlark_transform(
            eval,
            function,
            &[path_components_to_value(path), walked.clone()],
        )?
    } else {
        call_starlark_transform(eval, function, std::slice::from_ref(&walked))?
    };
    Ok(transformed)
}

fn delete_paths_object(
    fields: &IndexMap<String, Value>,
    paths: &[Vec<StarlarkPathComponent>],
) -> Result<Value, StarlarkBuiltinError> {
    let mut direct = std::collections::BTreeSet::new();
    let mut nested: std::collections::BTreeMap<String, Vec<Vec<StarlarkPathComponent>>> =
        std::collections::BTreeMap::new();

    for path in paths {
        match &path[0] {
            StarlarkPathComponent::Field(key) => {
                if path.len() == 1 {
                    direct.insert(key.clone());
                } else {
                    nested
                        .entry(key.clone())
                        .or_default()
                        .push(path[1..].to_vec());
                }
            }
            StarlarkPathComponent::Index(_) => {
                return Err(StarlarkBuiltinError::new(
                    "cannot delete number field of object",
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
            updated.insert(key.clone(), delete_paths_value(value, child_paths)?);
        } else {
            updated.insert(key.clone(), value.clone());
        }
    }
    Ok(Value::Object(updated))
}

fn delete_paths_array(
    values: &[Value],
    paths: &[Vec<StarlarkPathComponent>],
) -> Result<Value, StarlarkBuiltinError> {
    let mut direct = std::collections::BTreeSet::new();
    let mut nested: std::collections::BTreeMap<usize, Vec<Vec<StarlarkPathComponent>>> =
        std::collections::BTreeMap::new();

    for path in paths {
        match &path[0] {
            StarlarkPathComponent::Index(index) => {
                let Some(resolved) = resolve_array_index(*index, values.len()) else {
                    continue;
                };
                if path.len() == 1 {
                    direct.insert(resolved);
                } else {
                    nested.entry(resolved).or_default().push(path[1..].to_vec());
                }
            }
            StarlarkPathComponent::Field(_) => {
                return Err(StarlarkBuiltinError::new(
                    "cannot delete string element of array",
                ));
            }
        }
    }

    let mut updated = Vec::new();
    for (index, value) in values.iter().enumerate() {
        if direct.contains(&index) {
            continue;
        }
        if let Some(child_paths) = nested.get(&index) {
            updated.push(delete_paths_value(value, child_paths)?);
        } else {
            updated.push(value.clone());
        }
    }
    Ok(Value::Array(updated))
}

fn starlark_read(
    path: &str,
    override_format: Option<Format>,
    return_all_documents: bool,
    base_dir: &Path,
    detect_conflicts: DetectConflictPolicy,
) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let path = resolve_runtime_path(base_dir, path);
    let documents = read_path(&path, override_format, false, detect_conflicts)
        .map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
    if return_all_documents {
        Ok(all_documents(documents))
    } else {
        Ok(vec![collapse_documents(documents)])
    }
}

fn starlark_glob_record(
    path: &str,
    value: Value,
    index: Option<usize>,
) -> Result<Value, StarlarkBuiltinError> {
    let mut fields = IndexMap::new();
    fields.insert("path".to_string(), Value::String(path.to_owned()));
    if let Some(index) = index {
        fields.insert(
            "index".to_string(),
            Value::Integer(
                i64::try_from(index)
                    .map_err(|_| StarlarkBuiltinError::new("document index is out of range"))?,
            ),
        );
    }
    fields.insert("value".to_string(), value);
    Ok(Value::Object(fields))
}

fn starlark_read_glob(
    pattern: &str,
    override_format: Option<Format>,
    return_all_documents: bool,
    base_dir: &Path,
    detect_conflicts: DetectConflictPolicy,
) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let mut values = Vec::new();
    for path in starlark_glob(pattern, false, base_dir)? {
        let resolved = resolve_runtime_path(base_dir, &path);
        let documents = read_path(&resolved, override_format, false, detect_conflicts)
            .map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
        if return_all_documents {
            for (index, document) in documents.into_iter().enumerate() {
                values.push(starlark_glob_record(&path, document.value, Some(index))?);
            }
        } else {
            values.push(starlark_glob_record(
                &path,
                collapse_documents(documents),
                None,
            )?);
        }
    }
    Ok(values)
}

fn starlark_query(expr: &str, input: StarlarkValue) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let query = parse(expr).map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
    let input =
        from_starlark_value(input).map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
    evaluate(&query, &input)
        .map(|stream| stream.into_vec())
        .map_err(|error| StarlarkBuiltinError::new(error.to_string()))
}

fn starlark_parse(text: &str, format: &str) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let format = parse_format_name(format)?;
    parse_text(text, format).map_err(|error| StarlarkBuiltinError::new(error.to_string()))
}

fn starlark_render_value(
    value: Value,
    format: &str,
    compact: bool,
    stream: bool,
) -> Result<String, StarlarkBuiltinError> {
    let format = parse_format_name(format)?;
    let values = if stream {
        match value {
            Value::Array(values) => values,
            other => vec![other],
        }
    } else {
        vec![value]
    };
    render(
        &values,
        format,
        RenderOptions {
            compact,
            ..RenderOptions::default()
        },
    )
    .map_err(|error| StarlarkBuiltinError::new(error.to_string()))
}

fn starlark_render(
    input: StarlarkValue,
    format: &str,
    compact: bool,
    stream: bool,
) -> Result<String, StarlarkBuiltinError> {
    let value =
        from_starlark_value(input).map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
    starlark_render_value(value, format, compact, stream)
}

fn starlark_read_text(path: &str, base_dir: &Path) -> Result<String, StarlarkBuiltinError> {
    let path = resolve_runtime_path(base_dir, path);
    std::fs::read_to_string(&path)
        .map_err(|error| StarlarkBuiltinError::new(AqError::io(Some(path), error).to_string()))
}

fn starlark_read_text_glob(
    pattern: &str,
    base_dir: &Path,
) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let mut values = Vec::new();
    for path in starlark_glob(pattern, false, base_dir)? {
        let text = starlark_read_text(&path, base_dir)?;
        let mut record = IndexMap::new();
        record.insert("path".to_string(), Value::String(path));
        record.insert("text".to_string(), Value::String(text));
        values.push(Value::Object(record));
    }
    Ok(values)
}

fn call_starlark_text_transform<'v>(
    eval: &mut Evaluator<'v, '_, '_>,
    function: StarlarkValue<'v>,
    path: &str,
    text: &str,
) -> Result<String, StarlarkBuiltinError> {
    let result = call_starlark_transform(
        eval,
        function,
        &[
            Value::String(path.to_owned()),
            Value::String(text.to_owned()),
        ],
    )?;
    match result {
        Value::String(text) => Ok(text),
        other => Err(StarlarkBuiltinError::new(format!(
            "text transform callbacks must return a string, got {}",
            starlark_kind_name(&other)
        ))),
    }
}

fn ensure_parent_directory(path: &Path) -> Result<(), StarlarkBuiltinError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(parent.to_path_buf()), error).to_string())
    })
}

fn starlark_write_text(
    path: &str,
    text: &str,
    parents: bool,
    base_dir: &Path,
) -> Result<i64, StarlarkBuiltinError> {
    let path = resolve_runtime_path(base_dir, path);
    if parents {
        ensure_parent_directory(&path)?;
    }
    write_atomically(&path, text).map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
    i64::try_from(text.len())
        .map_err(|_| StarlarkBuiltinError::new("text length does not fit into i64"))
}

fn starlark_write_rendered(
    path: &str,
    input: StarlarkValue,
    format: &str,
    compact: bool,
    stream: bool,
    parents: bool,
    base_dir: &Path,
) -> Result<i64, StarlarkBuiltinError> {
    let rendered = starlark_render(input, format, compact, stream)?;
    starlark_write_text(path, &rendered, parents, base_dir)
}

fn starlark_batch_entries(
    name: &str,
    entries: Value,
    value_field: &str,
) -> Result<Vec<(String, Value)>, StarlarkBuiltinError> {
    let Value::Array(entries) = entries else {
        return Err(StarlarkBuiltinError::new(format!(
            "{name} expects an array of entry objects"
        )));
    };

    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let Value::Object(fields) = entry else {
            return Err(StarlarkBuiltinError::new(format!(
                "{name} entries must be objects"
            )));
        };
        let path = match fields.get("path") {
            Some(Value::String(path)) => path.clone(),
            Some(other) => {
                return Err(StarlarkBuiltinError::new(format!(
                    "{name} entry path must be a string, got {}",
                    starlark_kind_name(other)
                )))
            }
            None => {
                return Err(StarlarkBuiltinError::new(format!(
                    "{name} entries must include a `path` field"
                )))
            }
        };
        let value = fields.get(value_field).cloned().ok_or_else(|| {
            StarlarkBuiltinError::new(format!(
                "{name} entries must include a `{value_field}` field"
            ))
        })?;
        out.push((path, value));
    }
    Ok(out)
}

fn starlark_write_text_batch(
    entries: Value,
    parents: bool,
    base_dir: &Path,
) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let entries = starlark_batch_entries("aq.write_text_batch", entries, "text")?;
    let mut results = Vec::with_capacity(entries.len());
    for (path, value) in entries {
        let Value::String(text) = value else {
            return Err(StarlarkBuiltinError::new(format!(
                "aq.write_text_batch entry text must be a string, got {}",
                starlark_kind_name(&value)
            )));
        };
        let bytes = starlark_write_text(&path, &text, parents, base_dir)?;
        let mut record = IndexMap::new();
        record.insert("path".to_string(), Value::String(path));
        record.insert("bytes".to_string(), Value::Integer(bytes));
        results.push(Value::Object(record));
    }
    Ok(results)
}

fn starlark_rewrite_text<'v>(
    path: &str,
    function: StarlarkValue<'v>,
    eval: &mut Evaluator<'v, '_, '_>,
    base_dir: &Path,
) -> Result<i64, StarlarkBuiltinError> {
    let text = starlark_read_text(path, base_dir)?;
    let rewritten = call_starlark_text_transform(eval, function, path, &text)?;
    starlark_write_text(path, &rewritten, false, base_dir)
}

fn starlark_rewrite_text_glob<'v>(
    pattern: &str,
    function: StarlarkValue<'v>,
    eval: &mut Evaluator<'v, '_, '_>,
    base_dir: &Path,
) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let mut results = Vec::new();
    for path in starlark_glob(pattern, false, base_dir)? {
        let bytes = starlark_rewrite_text(&path, function, eval, base_dir)?;
        let mut record = IndexMap::new();
        record.insert("path".to_string(), Value::String(path));
        record.insert("bytes".to_string(), Value::Integer(bytes));
        results.push(Value::Object(record));
    }
    Ok(results)
}

fn starlark_write_batch(
    entries: Value,
    value_field: &str,
    format: &str,
    compact: bool,
    stream: bool,
    parents: bool,
    base_dir: &Path,
) -> Result<Vec<Value>, StarlarkBuiltinError> {
    let entries = starlark_batch_entries(
        if stream {
            "aq.write_batch_all"
        } else {
            "aq.write_batch"
        },
        entries,
        value_field,
    )?;
    let mut results = Vec::with_capacity(entries.len());
    for (path, value) in entries {
        let rendered = starlark_render_value(value, format, compact, stream)?;
        let bytes = starlark_write_text(&path, &rendered, parents, base_dir)?;
        let mut record = IndexMap::new();
        record.insert("path".to_string(), Value::String(path));
        record.insert("bytes".to_string(), Value::Integer(bytes));
        results.push(Value::Object(record));
    }
    Ok(results)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedPathParts {
    prefix: Option<OsString>,
    has_root: bool,
    parts: Vec<OsString>,
}

impl NormalizedPathParts {
    fn from_path(path: &Path) -> Self {
        let mut prefix = None;
        let mut has_root = false;
        let mut parts = Vec::new();

        for component in path.components() {
            match component {
                Component::Prefix(value) => {
                    prefix = Some(value.as_os_str().to_os_string());
                }
                Component::RootDir => {
                    has_root = true;
                }
                Component::CurDir => {}
                Component::ParentDir => match parts.last() {
                    Some(last) if last != OsStr::new("..") => {
                        parts.pop();
                    }
                    _ if has_root => {}
                    _ => parts.push(OsString::from("..")),
                },
                Component::Normal(value) => {
                    parts.push(value.to_os_string());
                }
            }
        }

        Self {
            prefix,
            has_root,
            parts,
        }
    }

    fn to_path_buf(&self) -> PathBuf {
        let mut path = match &self.prefix {
            Some(prefix) => PathBuf::from(prefix),
            None => PathBuf::new(),
        };
        if self.has_root {
            path.push(std::path::MAIN_SEPARATOR.to_string());
        }
        for part in &self.parts {
            path.push(part);
        }
        if path.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            path
        }
    }
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    NormalizedPathParts::from_path(path).to_path_buf()
}

fn starlark_resolve_path(path: &str, base_dir: &Path) -> String {
    normalize_lexical_path(&resolve_runtime_path(base_dir, path))
        .to_string_lossy()
        .into_owned()
}

fn starlark_relative_path(path: &str, start: Option<&str>, base_dir: &Path) -> String {
    let target = NormalizedPathParts::from_path(&resolve_runtime_path(base_dir, path));
    let start =
        NormalizedPathParts::from_path(&resolve_runtime_path(base_dir, start.unwrap_or(".")));

    if target.prefix != start.prefix || target.has_root != start.has_root {
        return target.to_path_buf().to_string_lossy().into_owned();
    }

    let mut common = 0;
    while common < target.parts.len()
        && common < start.parts.len()
        && target.parts[common] == start.parts[common]
    {
        common += 1;
    }

    let mut relative = PathBuf::new();
    for _ in common..start.parts.len() {
        relative.push("..");
    }
    for part in &target.parts[common..] {
        relative.push(part);
    }

    if relative.as_os_str().is_empty() {
        ".".to_string()
    } else {
        relative.to_string_lossy().into_owned()
    }
}

fn starlark_path_exists(path: &str, base_dir: &Path) -> bool {
    resolve_runtime_path(base_dir, path).exists()
}

fn starlark_path_is_file(path: &str, base_dir: &Path) -> bool {
    resolve_runtime_path(base_dir, path).is_file()
}

fn starlark_path_is_dir(path: &str, base_dir: &Path) -> bool {
    resolve_runtime_path(base_dir, path).is_dir()
}

fn starlark_list_dir(
    path: Option<&str>,
    base_dir: &Path,
) -> Result<Vec<String>, StarlarkBuiltinError> {
    let path = match path {
        Some(path) => resolve_runtime_path(base_dir, path),
        None => base_dir.to_path_buf(),
    };
    let entries = std::fs::read_dir(&path).map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(path.clone()), error).to_string())
    })?;
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            StarlarkBuiltinError::new(AqError::io(Some(path.clone()), error).to_string())
        })?;
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    Ok(names)
}

fn starlark_mkdir(
    path: &str,
    parents: bool,
    base_dir: &Path,
) -> Result<String, StarlarkBuiltinError> {
    let path = resolve_runtime_path(base_dir, path);
    let create = if parents {
        std::fs::create_dir_all(&path)
    } else {
        std::fs::create_dir(&path)
    };
    create.map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(path.clone()), error).to_string())
    })?;
    Ok(path.to_string_lossy().into_owned())
}

fn starlark_walk_files(
    path: Option<&str>,
    include_dirs: bool,
    base_dir: &Path,
) -> Result<Vec<String>, StarlarkBuiltinError> {
    let requested = path.unwrap_or(".");
    let root = resolve_runtime_path(base_dir, requested);
    if root.is_file() {
        let name = Path::new(requested)
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| StarlarkBuiltinError::new(format!("cannot walk path `{requested}`")))?;
        return Ok(vec![name.to_owned()]);
    }
    if !root.is_dir() {
        return Err(StarlarkBuiltinError::new(format!(
            "path `{}` is not a directory",
            root.display()
        )));
    }

    let mut out = Vec::new();
    walk_directory_entries(&root, &root, include_dirs, &mut out)?;
    Ok(out)
}

fn walk_directory_entries(
    current: &Path,
    root: &Path,
    include_dirs: bool,
    out: &mut Vec<String>,
) -> Result<(), StarlarkBuiltinError> {
    let entries = std::fs::read_dir(current).map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(current.to_path_buf()), error).to_string())
    })?;
    let mut entries = entries.collect::<Result<Vec<_>, _>>().map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(current.to_path_buf()), error).to_string())
    })?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
        let relative = relative.to_string_lossy().into_owned();
        let file_type = entry.file_type().map_err(|error| {
            StarlarkBuiltinError::new(AqError::io(Some(path.clone()), error).to_string())
        })?;
        if file_type.is_dir() {
            if include_dirs {
                out.push(relative.clone());
            }
            walk_directory_entries(&path, root, include_dirs, out)?;
        } else {
            out.push(relative);
        }
    }

    Ok(())
}

fn normalize_glob_text(input: &str) -> String {
    input.replace('\\', "/")
}

fn compile_glob_pattern(pattern: &str) -> Result<Regex, StarlarkBuiltinError> {
    let pattern = normalize_glob_text(pattern);
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '*' => {
                if matches!(chars.peek(), Some('*')) {
                    chars.next();
                    if matches!(chars.peek(), Some('/')) {
                        chars.next();
                        regex.push_str("(?:.*/)?");
                    } else {
                        regex.push_str(".*");
                    }
                } else {
                    regex.push_str("[^/]*");
                }
            }
            '?' => regex.push_str("[^/]"),
            _ => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }
    regex.push('$');
    Regex::new(&regex).map_err(|error| StarlarkBuiltinError::new(error.to_string()))
}

fn starlark_glob(
    pattern: &str,
    include_dirs: bool,
    base_dir: &Path,
) -> Result<Vec<String>, StarlarkBuiltinError> {
    if Path::new(pattern).is_absolute() {
        return Err(StarlarkBuiltinError::new(
            "aq.glob only supports patterns relative to the starlark base dir",
        ));
    }

    let regex = compile_glob_pattern(pattern)?;
    let mut entries = Vec::new();
    walk_directory_entries(base_dir, base_dir, include_dirs, &mut entries)?;
    Ok(entries
        .into_iter()
        .filter(|entry| regex.is_match(&normalize_glob_text(entry)))
        .collect())
}

fn compile_regex(pattern: &str) -> Result<Regex, StarlarkBuiltinError> {
    Regex::new(pattern).map_err(|error| StarlarkBuiltinError::new(error.to_string()))
}

fn starlark_regex_is_match(pattern: &str, text: &str) -> Result<bool, StarlarkBuiltinError> {
    Ok(compile_regex(pattern)?.is_match(text))
}

fn starlark_regex_find(pattern: &str, text: &str) -> Result<Option<String>, StarlarkBuiltinError> {
    Ok(compile_regex(pattern)?
        .find(text)
        .map(|capture| capture.as_str().to_owned()))
}

fn starlark_regex_find_all(pattern: &str, text: &str) -> Result<Vec<String>, StarlarkBuiltinError> {
    Ok(compile_regex(pattern)?
        .find_iter(text)
        .map(|capture| capture.as_str().to_owned())
        .collect())
}

fn captures_to_json(regex: &Regex, captures: &regex::Captures<'_>) -> serde_json::Value {
    let groups = captures
        .iter()
        .skip(1)
        .map(|capture| capture.map(|capture| capture.as_str().to_owned()))
        .collect::<Vec<_>>();
    let mut named = serde_json::Map::new();
    for name in regex.capture_names().flatten() {
        named.insert(
            name.to_owned(),
            captures
                .name(name)
                .map(|capture| serde_json::Value::String(capture.as_str().to_owned()))
                .unwrap_or(serde_json::Value::Null),
        );
    }

    serde_json::json!({
        "match": captures
            .get(0)
            .map(|capture| capture.as_str().to_owned())
            .unwrap_or_default(),
        "groups": groups,
        "named": named,
    })
}

fn starlark_regex_capture(
    pattern: &str,
    text: &str,
) -> Result<Option<serde_json::Value>, StarlarkBuiltinError> {
    let regex = compile_regex(pattern)?;
    let captures = match regex.captures(text) {
        Some(captures) => captures,
        None => return Ok(None),
    };

    Ok(Some(captures_to_json(&regex, &captures)))
}

fn starlark_regex_capture_all(
    pattern: &str,
    text: &str,
) -> Result<Vec<serde_json::Value>, StarlarkBuiltinError> {
    let regex = compile_regex(pattern)?;
    Ok(regex
        .captures_iter(text)
        .map(|captures| captures_to_json(&regex, &captures))
        .collect())
}

fn starlark_regex_split(pattern: &str, text: &str) -> Result<Vec<String>, StarlarkBuiltinError> {
    Ok(compile_regex(pattern)?
        .split(text)
        .map(ToOwned::to_owned)
        .collect())
}

fn starlark_regex_replace(
    pattern: &str,
    replacement: &str,
    text: &str,
    all: bool,
) -> Result<String, StarlarkBuiltinError> {
    let regex = compile_regex(pattern)?;
    let rendered = if all {
        regex.replace_all(text, replacement)
    } else {
        regex.replace(text, replacement)
    };
    Ok(rendered.into_owned())
}

fn base64_engine(urlsafe: bool, pad: bool) -> &'static base64::engine::GeneralPurpose {
    match (urlsafe, pad) {
        (false, true) => &BASE64_STANDARD,
        (false, false) => &BASE64_STANDARD_NO_PAD,
        (true, true) => &BASE64_URL_SAFE,
        (true, false) => &BASE64_URL_SAFE_NO_PAD,
    }
}

fn starlark_base64_encode(text: &str, urlsafe: bool, pad: bool) -> String {
    base64_engine(urlsafe, pad).encode(text.as_bytes())
}

fn starlark_base64_decode(text: &str, urlsafe: bool) -> Result<String, StarlarkBuiltinError> {
    let bytes = base64_engine(urlsafe, true)
        .decode(text)
        .or_else(|_| base64_engine(urlsafe, false).decode(text))
        .map_err(|error| StarlarkBuiltinError::new(error.to_string()))?;
    String::from_utf8(bytes).map_err(|error| {
        StarlarkBuiltinError::new(format!("decoded base64 is not valid UTF-8: {error}"))
    })
}

fn lowercase_text(text: &str) -> String {
    text.chars().flat_map(char::to_lowercase).collect()
}

fn uppercase_first_lowercase_rest(text: &str) -> String {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.extend(first.to_uppercase());
    for ch in chars {
        out.extend(ch.to_lowercase());
    }
    out
}

fn should_split_word(previous: char, current: char, next: Option<char>) -> bool {
    if previous.is_ascii_digit() != current.is_ascii_digit() {
        return true;
    }
    if previous.is_lowercase() && current.is_uppercase() {
        return true;
    }
    previous.is_uppercase() && current.is_uppercase() && next.is_some_and(char::is_lowercase)
}

fn split_normalized_words(text: &str) -> Vec<String> {
    let chars = text.chars().collect::<Vec<_>>();
    let mut words = Vec::new();
    let mut current = String::new();

    for (index, ch) in chars.iter().copied().enumerate() {
        if !ch.is_alphanumeric() {
            if !current.is_empty() {
                words.push(current);
                current = String::new();
            }
            continue;
        }

        if let Some(previous) = current.chars().last() {
            if should_split_word(previous, ch, chars.get(index + 1).copied()) {
                words.push(current);
                current = String::new();
            }
        }

        current.push(ch);
    }

    if !current.is_empty() {
        words.push(current);
    }

    words
}

fn join_normalized_words(text: &str, separator: &str) -> String {
    split_normalized_words(text)
        .into_iter()
        .map(|word| lowercase_text(&word))
        .collect::<Vec<_>>()
        .join(separator)
}

fn starlark_slug(text: &str) -> String {
    join_normalized_words(text, "-")
}

fn starlark_snake_case(text: &str) -> String {
    join_normalized_words(text, "_")
}

fn starlark_kebab_case(text: &str) -> String {
    join_normalized_words(text, "-")
}

fn starlark_camel_case(text: &str) -> String {
    let mut words = split_normalized_words(text).into_iter();
    let Some(first) = words.next() else {
        return String::new();
    };

    let mut out = lowercase_text(&first);
    for word in words {
        out.push_str(&uppercase_first_lowercase_rest(&word));
    }
    out
}

fn starlark_title_case(text: &str) -> String {
    split_normalized_words(text)
        .into_iter()
        .map(|word| uppercase_first_lowercase_rest(&word))
        .collect::<Vec<_>>()
        .join(" ")
}

fn starlark_trim_prefix(text: &str, prefix: &str) -> String {
    text.strip_prefix(prefix).unwrap_or(text).to_owned()
}

fn starlark_trim_suffix(text: &str, suffix: &str) -> String {
    text.strip_suffix(suffix).unwrap_or(text).to_owned()
}

fn starlark_regex_escape(text: &str) -> String {
    regex::escape(text)
}

fn is_shell_safe_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric()
        || matches!(
            ch,
            '@' | '%' | '_' | '-' | '+' | '=' | ':' | ',' | '.' | '/'
        )
}

fn starlark_shell_escape(text: &str) -> String {
    if text.is_empty() {
        return "''".to_owned();
    }
    if text.chars().all(is_shell_safe_char) {
        return text.to_owned();
    }

    let mut out = String::with_capacity(text.len() + 2);
    out.push('\'');
    for ch in text.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn is_url_component_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn push_hex_byte(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push(char::from(HEX[(byte >> 4) as usize]));
    out.push(char::from(HEX[(byte & 0x0f) as usize]));
}

fn starlark_url_encode_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if is_url_component_unreserved(byte) {
            out.push(char::from(byte));
        } else {
            out.push('%');
            push_hex_byte(&mut out, byte);
        }
    }
    out
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn starlark_url_decode_component(text: &str) -> Result<String, StarlarkBuiltinError> {
    let bytes = text.as_bytes();
    if !bytes.contains(&b'%') {
        return Ok(text.to_owned());
    }

    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            out.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            return Err(StarlarkBuiltinError::new(format!(
                "aq.url_decode_component found incomplete percent escape at byte {}",
                index
            )));
        }
        let high = decode_hex_nibble(bytes[index + 1]).ok_or_else(|| {
            StarlarkBuiltinError::new(format!(
                "aq.url_decode_component found invalid percent escape `%{}{}' at byte {}",
                char::from(bytes[index + 1]),
                char::from(bytes[index + 2]),
                index
            ))
        })?;
        let low = decode_hex_nibble(bytes[index + 2]).ok_or_else(|| {
            StarlarkBuiltinError::new(format!(
                "aq.url_decode_component found invalid percent escape `%{}{}' at byte {}",
                char::from(bytes[index + 1]),
                char::from(bytes[index + 2]),
                index
            ))
        })?;
        out.push((high << 4) | low);
        index += 3;
    }

    String::from_utf8(out).map_err(|error| {
        StarlarkBuiltinError::new(format!(
            "aq.url_decode_component decoded bytes are not valid UTF-8: {error}"
        ))
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        push_hex_byte(&mut out, *byte);
    }
    lowercase_text(&out)
}

fn starlark_hash_bytes(
    bytes: &[u8],
    algorithm: &str,
    encoding: &str,
) -> Result<String, StarlarkBuiltinError> {
    let digest = match algorithm.trim().to_ascii_lowercase().as_str() {
        "sha1" => Sha1::digest(bytes).to_vec(),
        "sha256" => Sha256::digest(bytes).to_vec(),
        "sha512" => Sha512::digest(bytes).to_vec(),
        "blake3" => blake3::hash(bytes).as_bytes().to_vec(),
        other => {
            return Err(StarlarkBuiltinError::new(format!(
            "unsupported hash algorithm `{other}`, expected one of: sha1, sha256, sha512, blake3"
        )))
        }
    };

    match encoding.trim().to_ascii_lowercase().as_str() {
        "hex" => Ok(encode_hex(&digest)),
        "base64" => Ok(BASE64_STANDARD.encode(digest)),
        other => Err(StarlarkBuiltinError::new(format!(
            "unsupported hash encoding `{other}`, expected one of: hex, base64"
        ))),
    }
}

fn starlark_hash(
    text: &str,
    algorithm: &str,
    encoding: &str,
) -> Result<String, StarlarkBuiltinError> {
    starlark_hash_bytes(text.as_bytes(), algorithm, encoding)
}

fn starlark_hash_file(
    path: &str,
    algorithm: &str,
    encoding: &str,
    base_dir: &Path,
) -> Result<String, StarlarkBuiltinError> {
    let path = resolve_runtime_path(base_dir, path);
    let bytes = std::fs::read(&path)
        .map_err(|error| StarlarkBuiltinError::new(AqError::io(Some(path), error).to_string()))?;
    starlark_hash_bytes(&bytes, algorithm, encoding)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SemverIdentifier {
    Numeric(u64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Semver {
    major: u64,
    minor: u64,
    patch: u64,
    prerelease: Vec<SemverIdentifier>,
    build: Vec<String>,
}

impl Semver {
    fn render(&self) -> String {
        let mut out = format!("{}.{}.{}", self.major, self.minor, self.patch);
        if !self.prerelease.is_empty() {
            out.push('-');
            out.push_str(
                &self
                    .prerelease
                    .iter()
                    .map(|identifier| match identifier {
                        SemverIdentifier::Numeric(value) => value.to_string(),
                        SemverIdentifier::Text(value) => value.clone(),
                    })
                    .collect::<Vec<_>>()
                    .join("."),
            );
        }
        if !self.build.is_empty() {
            out.push('+');
            out.push_str(&self.build.join("."));
        }
        out
    }
}

fn validate_semver_identifier(identifier: &str, field: &str) -> Result<(), StarlarkBuiltinError> {
    if identifier.is_empty() {
        return Err(StarlarkBuiltinError::new(format!(
            "aq.semver_parse found an empty {field} identifier"
        )));
    }
    if !identifier
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(StarlarkBuiltinError::new(format!(
            "aq.semver_parse found invalid {field} identifier `{identifier}`"
        )));
    }
    Ok(())
}

fn parse_semver_numeric_identifier(
    identifier: &str,
    field: &str,
) -> Result<u64, StarlarkBuiltinError> {
    validate_semver_identifier(identifier, field)?;
    if identifier.len() > 1 && identifier.starts_with('0') {
        return Err(StarlarkBuiltinError::new(format!(
            "aq.semver_parse found leading zeroes in {field} identifier `{identifier}`"
        )));
    }
    identifier.parse::<u64>().map_err(|error| {
        StarlarkBuiltinError::new(format!(
            "aq.semver_parse could not parse {field} identifier `{identifier}`: {error}"
        ))
    })
}

fn parse_semver_prerelease_identifiers(
    prerelease: &str,
) -> Result<Vec<SemverIdentifier>, StarlarkBuiltinError> {
    prerelease
        .split('.')
        .map(|identifier| {
            validate_semver_identifier(identifier, "prerelease")?;
            if identifier.bytes().all(|byte| byte.is_ascii_digit()) {
                Ok(SemverIdentifier::Numeric(parse_semver_numeric_identifier(
                    identifier,
                    "prerelease",
                )?))
            } else {
                Ok(SemverIdentifier::Text(identifier.to_owned()))
            }
        })
        .collect()
}

fn parse_semver_build_identifiers(build: &str) -> Result<Vec<String>, StarlarkBuiltinError> {
    build
        .split('.')
        .map(|identifier| {
            validate_semver_identifier(identifier, "build")?;
            Ok(identifier.to_owned())
        })
        .collect()
}

fn parse_semver(text: &str) -> Result<Semver, StarlarkBuiltinError> {
    let (without_build, build) = match text.split_once('+') {
        Some((version, build)) => {
            if build.contains('+') {
                return Err(StarlarkBuiltinError::new(format!(
                    "aq.semver_parse found multiple `+` markers in `{text}`"
                )));
            }
            (version, parse_semver_build_identifiers(build)?)
        }
        None => (text, Vec::new()),
    };

    let (core, prerelease) = match without_build.split_once('-') {
        Some((core, prerelease)) => {
            if prerelease.contains('-') {
                return Err(StarlarkBuiltinError::new(format!(
                    "aq.semver_parse found multiple `-` markers in `{text}`"
                )));
            }
            (core, parse_semver_prerelease_identifiers(prerelease)?)
        }
        None => (without_build, Vec::new()),
    };

    let mut parts = core.split('.');
    let Some(major) = parts.next() else {
        return Err(StarlarkBuiltinError::new(
            "aq.semver_parse expected MAJOR.MINOR.PATCH",
        ));
    };
    let Some(minor) = parts.next() else {
        return Err(StarlarkBuiltinError::new(
            "aq.semver_parse expected MAJOR.MINOR.PATCH",
        ));
    };
    let Some(patch) = parts.next() else {
        return Err(StarlarkBuiltinError::new(
            "aq.semver_parse expected MAJOR.MINOR.PATCH",
        ));
    };
    if parts.next().is_some() {
        return Err(StarlarkBuiltinError::new(
            "aq.semver_parse expected MAJOR.MINOR.PATCH",
        ));
    }

    Ok(Semver {
        major: parse_semver_numeric_identifier(major, "major")?,
        minor: parse_semver_numeric_identifier(minor, "minor")?,
        patch: parse_semver_numeric_identifier(patch, "patch")?,
        prerelease,
        build,
    })
}

fn semver_identifier_to_json(identifier: &SemverIdentifier) -> serde_json::Value {
    match identifier {
        SemverIdentifier::Numeric(value) => serde_json::json!(value),
        SemverIdentifier::Text(value) => serde_json::json!(value),
    }
}

fn semver_to_json(version: &Semver) -> serde_json::Value {
    serde_json::json!({
        "major": version.major,
        "minor": version.minor,
        "patch": version.patch,
        "prerelease": version.prerelease.iter().map(semver_identifier_to_json).collect::<Vec<_>>(),
        "build": version.build,
        "is_prerelease": !version.prerelease.is_empty(),
        "version": version.render(),
    })
}

fn compare_semver_identifier(left: &SemverIdentifier, right: &SemverIdentifier) -> Ordering {
    match (left, right) {
        (SemverIdentifier::Numeric(left), SemverIdentifier::Numeric(right)) => left.cmp(right),
        (SemverIdentifier::Numeric(_), SemverIdentifier::Text(_)) => Ordering::Less,
        (SemverIdentifier::Text(_), SemverIdentifier::Numeric(_)) => Ordering::Greater,
        (SemverIdentifier::Text(left), SemverIdentifier::Text(right)) => left.cmp(right),
    }
}

fn starlark_semver_compare(left: &str, right: &str) -> Result<i64, StarlarkBuiltinError> {
    let left = parse_semver(left)?;
    let right = parse_semver(right)?;

    let ordering = left
        .major
        .cmp(&right.major)
        .then_with(|| left.minor.cmp(&right.minor))
        .then_with(|| left.patch.cmp(&right.patch))
        .then_with(
            || match (left.prerelease.is_empty(), right.prerelease.is_empty()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => {
                    for (left_identifier, right_identifier) in
                        left.prerelease.iter().zip(right.prerelease.iter())
                    {
                        let ordering = compare_semver_identifier(left_identifier, right_identifier);
                        if ordering != Ordering::Equal {
                            return ordering;
                        }
                    }
                    left.prerelease.len().cmp(&right.prerelease.len())
                }
            },
        );

    Ok(match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    })
}

fn checked_increment_semver(value: u64, field: &str) -> Result<u64, StarlarkBuiltinError> {
    value.checked_add(1).ok_or_else(|| {
        StarlarkBuiltinError::new(format!(
            "aq.semver_bump cannot increment {field}, value would overflow"
        ))
    })
}

fn validate_prerelease_label(label: &str) -> Result<(), StarlarkBuiltinError> {
    validate_semver_identifier(label, "prerelease label")?;
    if label.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(StarlarkBuiltinError::new(
            "aq.semver_bump prerelease labels cannot be purely numeric",
        ));
    }
    Ok(())
}

fn starlark_semver_bump(
    text: &str,
    part: &str,
    prerelease_label: &str,
) -> Result<String, StarlarkBuiltinError> {
    let mut version = parse_semver(text)?;
    match part.trim().to_ascii_lowercase().as_str() {
        "major" => {
            version.major = checked_increment_semver(version.major, "major")?;
            version.minor = 0;
            version.patch = 0;
            version.prerelease.clear();
            version.build.clear();
        }
        "minor" => {
            version.minor = checked_increment_semver(version.minor, "minor")?;
            version.patch = 0;
            version.prerelease.clear();
            version.build.clear();
        }
        "patch" => {
            version.patch = checked_increment_semver(version.patch, "patch")?;
            version.prerelease.clear();
            version.build.clear();
        }
        "prerelease" => {
            validate_prerelease_label(prerelease_label)?;
            version.build.clear();
            if version.prerelease.is_empty() {
                version.prerelease = vec![
                    SemverIdentifier::Text(prerelease_label.to_owned()),
                    SemverIdentifier::Numeric(1),
                ];
            } else if matches!(
                version.prerelease.first(),
                Some(SemverIdentifier::Text(existing)) if existing == prerelease_label
            ) {
                match version.prerelease.last_mut() {
                    Some(SemverIdentifier::Numeric(value)) => {
                        *value = checked_increment_semver(*value, "prerelease numeric suffix")?;
                    }
                    _ => version.prerelease.push(SemverIdentifier::Numeric(1)),
                }
            } else {
                version.prerelease = vec![
                    SemverIdentifier::Text(prerelease_label.to_owned()),
                    SemverIdentifier::Numeric(1),
                ];
            }
        }
        "release" => {
            version.prerelease.clear();
            version.build.clear();
        }
        other => {
            return Err(StarlarkBuiltinError::new(format!(
                "aq.semver_bump does not support part `{other}`, expected one of: major, minor, patch, prerelease, release"
            )))
        }
    }

    Ok(version.render())
}

fn starlark_stat(
    path: &str,
    base_dir: &Path,
) -> Result<Option<serde_json::Value>, StarlarkBuiltinError> {
    let path = resolve_runtime_path(base_dir, path);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(StarlarkBuiltinError::new(
                AqError::io(Some(path.clone()), error).to_string(),
            ))
        }
    };

    let file_type = metadata.file_type();
    let kind = if file_type.is_symlink() {
        "symlink"
    } else if file_type.is_dir() {
        "dir"
    } else if file_type.is_file() {
        "file"
    } else {
        "other"
    };
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| i64::try_from(duration.as_secs()).ok());

    Ok(Some(serde_json::json!({
        "path": path.to_string_lossy(),
        "type": kind,
        "size": metadata.len(),
        "modified": modified,
    })))
}

fn starlark_copy(
    source: &str,
    destination: &str,
    overwrite: bool,
    base_dir: &Path,
) -> Result<i64, StarlarkBuiltinError> {
    let source = resolve_runtime_path(base_dir, source);
    let destination = resolve_runtime_path(base_dir, destination);
    if destination.exists() && !overwrite {
        return Err(StarlarkBuiltinError::new(format!(
            "destination `{}` already exists, pass overwrite = True to replace it",
            destination.display()
        )));
    }
    if destination.exists() && overwrite {
        starlark_remove_path(&destination, true, false)?;
    }
    let bytes = std::fs::copy(&source, &destination).map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(destination.clone()), error).to_string())
    })?;
    i64::try_from(bytes)
        .map_err(|_| StarlarkBuiltinError::new("copied byte count does not fit into i64"))
}

fn starlark_rename(
    source: &str,
    destination: &str,
    overwrite: bool,
    base_dir: &Path,
) -> Result<String, StarlarkBuiltinError> {
    let source = resolve_runtime_path(base_dir, source);
    let destination = resolve_runtime_path(base_dir, destination);
    if destination.exists() && !overwrite {
        return Err(StarlarkBuiltinError::new(format!(
            "destination `{}` already exists, pass overwrite = True to replace it",
            destination.display()
        )));
    }
    if destination.exists() && overwrite {
        starlark_remove_path(&destination, true, false)?;
    }
    std::fs::rename(&source, &destination).map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(destination.clone()), error).to_string())
    })?;
    Ok(destination.to_string_lossy().into_owned())
}

fn starlark_remove(
    path: &str,
    recursive: bool,
    missing_ok: bool,
    base_dir: &Path,
) -> Result<bool, StarlarkBuiltinError> {
    let path = resolve_runtime_path(base_dir, path);
    starlark_remove_path(&path, recursive, missing_ok)
}

fn starlark_remove_path(
    path: &Path,
    recursive: bool,
    missing_ok: bool,
) -> Result<bool, StarlarkBuiltinError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && missing_ok => {
            return Ok(false)
        }
        Err(error) => {
            return Err(StarlarkBuiltinError::new(
                AqError::io(Some(path.to_path_buf()), error).to_string(),
            ))
        }
    };

    let file_type = metadata.file_type();
    let remove_result = if file_type.is_dir() {
        if recursive {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_dir(path)
        }
    } else {
        std::fs::remove_file(path)
    };
    remove_result.map_err(|error| {
        StarlarkBuiltinError::new(AqError::io(Some(path.to_path_buf()), error).to_string())
    })?;
    Ok(true)
}

#[derive(Debug)]
struct StarlarkBuiltinError {
    message: String,
}

impl StarlarkBuiltinError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for StarlarkBuiltinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StarlarkBuiltinError {}

fn resolve_runtime_path(base_dir: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

fn canonicalize_starlark_path(path: &Path) -> Result<PathBuf, AqError> {
    std::fs::canonicalize(path).map_err(|error| AqError::io(Some(path.to_path_buf()), error))
}

fn load_dependencies(
    ast: &AstModule,
    globals: &Globals,
    context: &StarlarkContext,
    base_dir: &Path,
) -> Result<Vec<(String, FrozenModule)>, AqError> {
    let mut cache = HashMap::new();
    let mut loading = Vec::new();
    load_dependencies_with_state(ast, globals, context, base_dir, &mut cache, &mut loading)
}

fn load_dependencies_with_state(
    ast: &AstModule,
    globals: &Globals,
    context: &StarlarkContext,
    base_dir: &Path,
    cache: &mut HashMap<PathBuf, FrozenModule>,
    loading: &mut Vec<PathBuf>,
) -> Result<Vec<(String, FrozenModule)>, AqError> {
    let mut modules = Vec::new();
    for load in ast.loads() {
        if !context.capabilities.filesystem {
            return Err(AqError::Starlark(
                "starlark load() is disabled, pass --starlark-filesystem or --starlark-unsafe"
                    .to_string(),
            ));
        }
        let requested = load.module_id;
        let resolved = canonicalize_starlark_path(&resolve_runtime_path(base_dir, requested))?;
        let module = load_module_from_path(&resolved, globals, context, cache, loading)?;
        modules.push((requested.to_owned(), module));
    }
    Ok(modules)
}

fn load_module_from_path(
    path: &Path,
    globals: &Globals,
    context: &StarlarkContext,
    cache: &mut HashMap<PathBuf, FrozenModule>,
    loading: &mut Vec<PathBuf>,
) -> Result<FrozenModule, AqError> {
    let path = canonicalize_starlark_path(path)?;
    if let Some(module) = cache.get(&path) {
        return Ok(module.clone());
    }
    if loading.contains(&path) {
        return Err(AqError::Starlark(format!(
            "cyclic starlark load detected for `{}`",
            path.display()
        )));
    }

    loading.push(path.clone());
    let result = load_module_uncached(&path, globals, context, cache, loading);
    loading.pop();

    let module = result?;
    cache.insert(path.clone(), module.clone());
    Ok(module)
}

fn load_module_uncached(
    path: &Path,
    globals: &Globals,
    context: &StarlarkContext,
    cache: &mut HashMap<PathBuf, FrozenModule>,
    loading: &mut Vec<PathBuf>,
) -> Result<FrozenModule, AqError> {
    let source = std::fs::read_to_string(path)
        .map_err(|error| AqError::io(Some(path.to_path_buf()), error))?;
    let filename = path.to_string_lossy().into_owned();
    let ast = AstModule::parse(&filename, source, &Dialect::Standard)
        .map_err(|error| AqError::InvalidStarlark(error.to_string()))?;
    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let loads = load_dependencies_with_state(&ast, globals, context, base_dir, cache, loading)?;
    let load_map = loads
        .iter()
        .map(|(module_id, module)| (module_id.as_str(), module))
        .collect::<HashMap<_, _>>();

    let module = Module::new();
    install_runtime_context(
        &module,
        module.heap(),
        context.detect_conflicts,
        context.current_format_name.as_deref(),
        base_dir,
    );

    {
        let loader = (!load_map.is_empty()).then_some(ReturnFileLoader { modules: &load_map });
        let mut evaluator = Evaluator::new(&module);
        if let Some(loader) = &loader {
            evaluator.set_loader(loader);
        }
        evaluator
            .eval_module(ast, globals)
            .map_err(|error| AqError::Starlark(error.to_string()))?;
    }
    module
        .freeze()
        .map_err(|error| AqError::Starlark(format!("{error:?}")))
}

#[starlark_module]
fn aq_core(builder: &mut GlobalsBuilder) {
    fn format<'v>(eval: &mut Evaluator<'v, '_, '_>) -> starlark::Result<StarlarkValue<'v>> {
        let (_, current_format_name, _) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        match current_format_name {
            Some(current_format_name) => Ok(eval.heap().alloc(current_format_name)),
            None => Ok(StarlarkValue::new_none()),
        }
    }

    fn date<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let date = parse_date_string(text).ok_or_else(|| {
            starlark::Error::new_other(StarlarkBuiltinError::new(format!(
                "aq.date cannot parse string \"{text}\""
            )))
        })?;
        Ok(heap.alloc(AqDate::new(date)))
    }

    fn datetime<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let datetime = parse_common_datetime_string(text).ok_or_else(|| {
            starlark::Error::new_other(StarlarkBuiltinError::new(format!(
                "aq.datetime cannot parse string \"{text}\""
            )))
        })?;
        Ok(heap.alloc(AqDateTime::new(datetime)))
    }

    #[allow(clippy::too_many_arguments)]
    fn timedelta<'v>(
        #[starlark(require = named, default = 0)] weeks: i64,
        #[starlark(require = named, default = 0)] days: i64,
        #[starlark(require = named, default = 0)] hours: i64,
        #[starlark(require = named, default = 0)] minutes: i64,
        #[starlark(require = named, default = 0)] seconds: i64,
        #[starlark(require = named, default = 0)] milliseconds: i64,
        #[starlark(require = named, default = 0)] microseconds: i64,
        #[starlark(require = named, default = 0)] nanoseconds: i64,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let duration = build_timedelta(
            weeks,
            days,
            hours,
            minutes,
            seconds,
            milliseconds,
            microseconds,
            nanoseconds,
        )
        .map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(duration))
    }

    fn query_all<'v>(
        #[starlark(require = pos)] expr: &str,
        #[starlark(require = pos)] input: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let values = starlark_query(expr, input).map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, heap)
    }

    fn query_one<'v>(
        #[starlark(require = pos)] expr: &str,
        #[starlark(require = pos)] input: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let values = starlark_query(expr, input).map_err(starlark::Error::new_other)?;
        match values.as_slice() {
            [value] => to_starlark_json_value(value, heap),
            [] => Err(starlark::Error::new_other(StarlarkBuiltinError::new(
                format!("aq.query_one expected exactly one result for `{expr}`, got 0"),
            ))),
            _ => Err(starlark::Error::new_other(StarlarkBuiltinError::new(
                format!(
                    "aq.query_one expected exactly one result for `{expr}`, got {}",
                    values.len()
                ),
            ))),
        }
    }

    fn parse<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = pos)] format: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value =
            collapse_values(starlark_parse(text, format).map_err(starlark::Error::new_other)?);
        to_starlark_json_value(&value, heap)
    }

    fn parse_all<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = pos)] format: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let values = starlark_parse(text, format).map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, heap)
    }

    fn render<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] format: &str,
        #[starlark(require = named, default = false)] compact: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let rendered =
            starlark_render(value, format, compact, false).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(rendered))
    }

    fn render_all<'v>(
        #[starlark(require = pos)] values: StarlarkValue<'v>,
        #[starlark(require = pos)] format: &str,
        #[starlark(require = named, default = false)] compact: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let rendered =
            starlark_render(values, format, compact, true).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(rendered))
    }

    fn regex_is_match<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let matched = starlark_regex_is_match(pattern, text).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(matched))
    }

    fn regex_find<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        match starlark_regex_find(pattern, text).map_err(starlark::Error::new_other)? {
            Some(value) => Ok(heap.alloc(value)),
            None => Ok(StarlarkValue::new_none()),
        }
    }

    fn regex_find_all<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(serde_json::json!(
            starlark_regex_find_all(pattern, text).map_err(starlark::Error::new_other)?
        )))
    }

    fn regex_capture<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        match starlark_regex_capture(pattern, text).map_err(starlark::Error::new_other)? {
            Some(value) => Ok(heap.alloc(value)),
            None => Ok(StarlarkValue::new_none()),
        }
    }

    fn regex_capture_all<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(serde_json::json!(
            starlark_regex_capture_all(pattern, text).map_err(starlark::Error::new_other)?
        )))
    }

    fn regex_split<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(serde_json::json!(
            starlark_regex_split(pattern, text).map_err(starlark::Error::new_other)?
        )))
    }

    fn regex_replace<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] replacement: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_regex_replace(pattern, replacement, text, false)
            .map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn regex_replace_all<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] replacement: &str,
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_regex_replace(pattern, replacement, text, true)
            .map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn base64_encode<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = false)] urlsafe: bool,
        #[starlark(require = named, default = true)] pad: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_base64_encode(text, urlsafe, pad)))
    }

    fn base64_decode<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = false)] urlsafe: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_base64_decode(text, urlsafe).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn slug<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_slug(text)))
    }

    fn snake_case<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_snake_case(text)))
    }

    fn kebab_case<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_kebab_case(text)))
    }

    fn camel_case<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_camel_case(text)))
    }

    fn title_case<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_title_case(text)))
    }

    fn trim_prefix<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = pos)] prefix: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_trim_prefix(text, prefix)))
    }

    fn trim_suffix<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = pos)] suffix: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_trim_suffix(text, suffix)))
    }

    fn regex_escape<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_regex_escape(text)))
    }

    fn shell_escape<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_shell_escape(text)))
    }

    fn url_encode_component<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(starlark_url_encode_component(text)))
    }

    fn url_decode_component<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_url_decode_component(text).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn hash<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = "sha256")] algorithm: &str,
        #[starlark(require = named, default = "hex")] encoding: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_hash(text, algorithm, encoding).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn sha1<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = "hex")] encoding: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_hash(text, "sha1", encoding).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn sha256<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = "hex")] encoding: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_hash(text, "sha256", encoding).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn sha512<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = "hex")] encoding: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_hash(text, "sha512", encoding).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn blake3<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = "hex")] encoding: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = starlark_hash(text, "blake3", encoding).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(value))
    }

    fn semver_parse<'v>(
        #[starlark(require = pos)] text: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let parsed = parse_semver(text).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(semver_to_json(&parsed)))
    }

    fn semver_compare<'v>(
        #[starlark(require = pos)] left: &str,
        #[starlark(require = pos)] right: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let ordering = starlark_semver_compare(left, right).map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(ordering))
    }

    fn semver_bump<'v>(
        #[starlark(require = pos)] text: &str,
        #[starlark(require = pos)] part: &str,
        #[starlark(require = named, default = "rc")] prerelease_label: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let bumped = starlark_semver_bump(text, part, prerelease_label)
            .map_err(starlark::Error::new_other)?;
        Ok(heap.alloc(bumped))
    }

    fn merge<'v>(
        #[starlark(require = pos)] left: StarlarkValue<'v>,
        #[starlark(require = pos)] right: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] deep: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let left = from_starlark_value(left).map_err(starlark::Error::new_other)?;
        let right = from_starlark_value(right).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(&left.merged_with(&right, deep), heap)
    }

    fn merge_all<'v>(
        #[starlark(require = pos)] values: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] deep: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let values = from_starlark_value(values).map_err(starlark::Error::new_other)?;
        let Value::Array(values) = values else {
            return Err(starlark::Error::new_other(StarlarkBuiltinError::new(
                "aq.merge_all expects an array of values",
            )));
        };
        let mut values = values.into_iter();
        let Some(mut merged) = values.next() else {
            return Err(starlark::Error::new_other(StarlarkBuiltinError::new(
                "aq.merge_all expects a non-empty array",
            )));
        };
        for value in values {
            merged = merged.merged_with(&value, deep);
        }
        to_starlark_json_value(&merged, heap)
    }

    fn sort_keys<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] recursive: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(&value.sort_object_keys(recursive), heap)
    }

    fn drop_nulls<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] recursive: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(&value.drop_nulls(recursive), heap)
    }

    fn clean_k8s_metadata<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(
            &clean_k8s_metadata_value(&value).map_err(starlark::Error::new_other)?,
            heap,
        )
    }

    fn get_path<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] path: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let path = from_starlark_value(path).map_err(starlark::Error::new_other)?;
        let path = starlark_path_of("aq.get_path", &path).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(
            &get_path_value(&value, &path).map_err(starlark::Error::new_other)?,
            heap,
        )
    }

    fn set_path<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] path: StarlarkValue<'v>,
        #[starlark(require = pos)] replacement: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let path = from_starlark_value(path).map_err(starlark::Error::new_other)?;
        let replacement = from_starlark_value(replacement).map_err(starlark::Error::new_other)?;
        let path = starlark_path_of("aq.set_path", &path).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(
            &set_path_value(&value, &path, &replacement).map_err(starlark::Error::new_other)?,
            heap,
        )
    }

    fn delete_path<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] path: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let path = from_starlark_value(path).map_err(starlark::Error::new_other)?;
        let path = starlark_path_of("aq.delete_path", &path).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(
            &delete_paths_value(&value, &[path]).map_err(starlark::Error::new_other)?,
            heap,
        )
    }

    fn delete_paths<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] paths: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let paths = from_starlark_value(paths).map_err(starlark::Error::new_other)?;
        let paths =
            starlark_paths_of("aq.delete_paths", &paths).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(
            &delete_paths_value(&value, &paths).map_err(starlark::Error::new_other)?,
            heap,
        )
    }

    fn walk<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let transformed = walk_value(&value, function, eval).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(&transformed, eval.heap())
    }

    fn walk_paths<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let transformed = walk_value_with_path(&value, &[], function, eval, true)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_value(&transformed, eval.heap())
    }

    fn paths<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] leaves_only: bool,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let paths = collect_paths(&value, leaves_only).map_err(starlark::Error::new_other)?;
        let values = paths
            .iter()
            .map(|path| path_components_to_value(path))
            .collect::<Vec<_>>();
        to_starlark_json_array(&values, heap)
    }

    fn find_paths<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] leaves_only: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let paths = find_matching_paths(&value, function, eval, leaves_only)
            .map_err(starlark::Error::new_other)?;
        let values = paths
            .iter()
            .map(|path| path_components_to_value(path))
            .collect::<Vec<_>>();
        to_starlark_json_array(&values, eval.heap())
    }

    fn collect_paths<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] leaves_only: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let values = collect_path_values(&value, function, eval, leaves_only)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn pick_paths<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] paths: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let paths = from_starlark_value(paths).map_err(starlark::Error::new_other)?;
        let paths =
            starlark_paths_of("aq.pick_paths", &paths).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(
            &pick_paths_value(&value, &paths).map_err(starlark::Error::new_other)?,
            heap,
        )
    }

    fn omit_paths<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] paths: StarlarkValue<'v>,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let paths = from_starlark_value(paths).map_err(starlark::Error::new_other)?;
        let paths =
            starlark_paths_of("aq.omit_paths", &paths).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(
            &omit_paths_value(&value, &paths).map_err(starlark::Error::new_other)?,
            heap,
        )
    }

    fn pick_where<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] leaves_only: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let paths = find_matching_paths(&value, function, eval, leaves_only)
            .map_err(starlark::Error::new_other)?;
        let projected = pick_paths_value(&value, &paths).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(&projected, eval.heap())
    }

    fn omit_where<'v>(
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] leaves_only: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let value = from_starlark_value(value).map_err(starlark::Error::new_other)?;
        let paths = find_matching_paths(&value, function, eval, leaves_only)
            .map_err(starlark::Error::new_other)?;
        let projected = omit_paths_value(&value, &paths).map_err(starlark::Error::new_other)?;
        to_starlark_json_value(&projected, eval.heap())
    }
}

#[starlark_module]
fn aq_filesystem_enabled(builder: &mut GlobalsBuilder) {
    fn base_dir<'v>(eval: &mut Evaluator<'v, '_, '_>) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(base_dir.to_string_lossy().into_owned()))
    }

    fn resolve_path<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(starlark_resolve_path(path, &base_dir)))
    }

    fn relative_path<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = named)] start: Option<&str>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        Ok(eval
            .heap()
            .alloc(starlark_relative_path(path, start, &base_dir)))
    }

    fn exists<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(starlark_path_exists(path, &base_dir)))
    }

    fn is_file<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(starlark_path_is_file(path, &base_dir)))
    }

    fn is_dir<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(starlark_path_is_dir(path, &base_dir)))
    }

    fn list_dir<'v>(
        #[starlark(require = named)] path: Option<&str>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values = starlark_list_dir(path, &base_dir).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(serde_json::json!(values)))
    }

    fn walk_files<'v>(
        #[starlark(require = named)] path: Option<&str>,
        #[starlark(require = named, default = false)] include_dirs: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values = starlark_walk_files(path, include_dirs, &base_dir)
            .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(serde_json::json!(values)))
    }

    fn glob<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = named, default = false)] include_dirs: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values =
            starlark_glob(pattern, include_dirs, &base_dir).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(serde_json::json!(values)))
    }

    fn mkdir<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = named, default = false)] parents: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let path = starlark_mkdir(path, parents, &base_dir).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(path))
    }

    fn stat<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        match starlark_stat(path, &base_dir).map_err(starlark::Error::new_other)? {
            Some(value) => Ok(eval.heap().alloc(value)),
            None => Ok(StarlarkValue::new_none()),
        }
    }

    fn read_text<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let text = starlark_read_text(path, &base_dir).map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(text))
    }

    fn read_text_glob<'v>(
        #[starlark(require = pos)] pattern: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values =
            starlark_read_text_glob(pattern, &base_dir).map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn rewrite_text<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let bytes = starlark_rewrite_text(path, function, eval, &base_dir)
            .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(bytes))
    }

    fn rewrite_text_glob<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] function: StarlarkValue<'v>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values = starlark_rewrite_text_glob(pattern, function, eval, &base_dir)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn hash_file<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = named, default = "sha256")] algorithm: &str,
        #[starlark(require = named, default = "hex")] encoding: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let digest = starlark_hash_file(path, algorithm, encoding, &base_dir)
            .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(digest))
    }

    fn read<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values = starlark_read(path, None, false, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        match values.as_slice() {
            [value] => to_starlark_json_value(value, eval.heap()),
            _ => Err(starlark::Error::new_other(StarlarkBuiltinError::new(
                "internal error: aq.read did not collapse documents",
            ))),
        }
    }

    fn read_as<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = pos)] format: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let format = parse_format_name(format).map_err(starlark::Error::new_other)?;
        let values = starlark_read(path, Some(format), false, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        match values.as_slice() {
            [value] => to_starlark_json_value(value, eval.heap()),
            _ => Err(starlark::Error::new_other(StarlarkBuiltinError::new(
                "internal error: aq.read_as did not collapse documents",
            ))),
        }
    }

    fn read_all<'v>(
        #[starlark(require = pos)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values = starlark_read(path, None, true, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn read_all_as<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = pos)] format: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let format = parse_format_name(format).map_err(starlark::Error::new_other)?;
        let values = starlark_read(path, Some(format), true, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn read_glob<'v>(
        #[starlark(require = pos)] pattern: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values = starlark_read_glob(pattern, None, false, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn read_glob_as<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] format: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let format = parse_format_name(format).map_err(starlark::Error::new_other)?;
        let values = starlark_read_glob(pattern, Some(format), false, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn read_glob_all<'v>(
        #[starlark(require = pos)] pattern: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let values = starlark_read_glob(pattern, None, true, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn read_glob_all_as<'v>(
        #[starlark(require = pos)] pattern: &str,
        #[starlark(require = pos)] format: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (detect_conflicts, _, base_dir) =
            runtime_context(eval).map_err(starlark::Error::new_other)?;
        let format = parse_format_name(format).map_err(starlark::Error::new_other)?;
        let values = starlark_read_glob(pattern, Some(format), true, &base_dir, detect_conflicts)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn write_text<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = pos)] text: &str,
        #[starlark(require = named, default = false)] parents: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let bytes = starlark_write_text(path, text, parents, &base_dir)
            .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(bytes))
    }

    fn write_text_batch<'v>(
        #[starlark(require = pos)] entries: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] parents: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let entries = from_starlark_value(entries).map_err(starlark::Error::new_other)?;
        let values = starlark_write_text_batch(entries, parents, &base_dir)
            .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn copy<'v>(
        #[starlark(require = pos)] source: &str,
        #[starlark(require = pos)] destination: &str,
        #[starlark(require = named, default = false)] overwrite: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let bytes = starlark_copy(source, destination, overwrite, &base_dir)
            .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(bytes))
    }

    fn rename<'v>(
        #[starlark(require = pos)] source: &str,
        #[starlark(require = pos)] destination: &str,
        #[starlark(require = named, default = false)] overwrite: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let path = starlark_rename(source, destination, overwrite, &base_dir)
            .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(path))
    }

    fn remove<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = named, default = false)] recursive: bool,
        #[starlark(require = named, default = false)] missing_ok: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let removed = starlark_remove(path, recursive, missing_ok, &base_dir)
            .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(removed))
    }

    fn write<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = pos)] value: StarlarkValue<'v>,
        #[starlark(require = pos)] format: &str,
        #[starlark(require = named, default = false)] compact: bool,
        #[starlark(require = named, default = false)] parents: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let bytes =
            starlark_write_rendered(path, value, format, compact, false, parents, &base_dir)
                .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(bytes))
    }

    fn write_all<'v>(
        #[starlark(require = pos)] path: &str,
        #[starlark(require = pos)] values: StarlarkValue<'v>,
        #[starlark(require = pos)] format: &str,
        #[starlark(require = named, default = false)] compact: bool,
        #[starlark(require = named, default = false)] parents: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let bytes =
            starlark_write_rendered(path, values, format, compact, true, parents, &base_dir)
                .map_err(starlark::Error::new_other)?;
        Ok(eval.heap().alloc(bytes))
    }

    fn write_batch<'v>(
        #[starlark(require = pos)] entries: StarlarkValue<'v>,
        #[starlark(require = pos)] format: &str,
        #[starlark(require = named, default = false)] compact: bool,
        #[starlark(require = named, default = false)] parents: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let entries = from_starlark_value(entries).map_err(starlark::Error::new_other)?;
        let values =
            starlark_write_batch(entries, "value", format, compact, false, parents, &base_dir)
                .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }

    fn write_batch_all<'v>(
        #[starlark(require = pos)] entries: StarlarkValue<'v>,
        #[starlark(require = pos)] format: &str,
        #[starlark(require = named, default = false)] compact: bool,
        #[starlark(require = named, default = false)] parents: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        let (_, _, base_dir) = runtime_context(eval).map_err(starlark::Error::new_other)?;
        let entries = from_starlark_value(entries).map_err(starlark::Error::new_other)?;
        let values =
            starlark_write_batch(entries, "values", format, compact, true, parents, &base_dir)
                .map_err(starlark::Error::new_other)?;
        to_starlark_json_array(&values, eval.heap())
    }
}

#[starlark_module]
fn aq_filesystem_disabled(builder: &mut GlobalsBuilder) {
    fn base_dir<'v>(_eval: &mut Evaluator<'v, '_, '_>) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.base_dir is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn resolve_path<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.resolve_path is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn relative_path<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = named)] _start: Option<&str>,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.relative_path is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn exists<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.exists is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn is_file<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.is_file is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn is_dir<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.is_dir is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn list_dir<'v>(
        #[starlark(require = named)] _path: Option<&str>,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.list_dir is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn walk_files<'v>(
        #[starlark(require = named)] _path: Option<&str>,
        #[starlark(require = named, default = false)] _include_dirs: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.walk_files is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn glob<'v>(
        #[starlark(require = pos)] _pattern: &str,
        #[starlark(require = named, default = false)] _include_dirs: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.glob is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn mkdir<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = named, default = false)] _parents: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.mkdir is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn stat<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.stat is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_text<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_text is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_text_glob<'v>(
        #[starlark(require = pos)] _pattern: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_text_glob is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn rewrite_text<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = pos)] _function: StarlarkValue<'v>,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.rewrite_text is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn rewrite_text_glob<'v>(
        #[starlark(require = pos)] _pattern: &str,
        #[starlark(require = pos)] _function: StarlarkValue<'v>,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.rewrite_text_glob is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn hash_file<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = named, default = "sha256")] _algorithm: &str,
        #[starlark(require = named, default = "hex")] _encoding: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.hash_file is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_as<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = pos)] _format: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_as is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_all<'v>(
        #[starlark(require = pos)] _path: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_all is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_all_as<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = pos)] _format: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_all_as is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_glob<'v>(
        #[starlark(require = pos)] _pattern: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_glob is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_glob_as<'v>(
        #[starlark(require = pos)] _pattern: &str,
        #[starlark(require = pos)] _format: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_glob_as is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_glob_all<'v>(
        #[starlark(require = pos)] _pattern: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_glob_all is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn read_glob_all_as<'v>(
        #[starlark(require = pos)] _pattern: &str,
        #[starlark(require = pos)] _format: &str,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.read_glob_all_as is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn write_text<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = pos)] _text: &str,
        #[starlark(require = named, default = false)] _parents: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.write_text is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn write_text_batch<'v>(
        #[starlark(require = pos)] _entries: StarlarkValue<'v>,
        #[starlark(require = named, default = false)] _parents: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.write_text_batch is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn copy<'v>(
        #[starlark(require = pos)] _source: &str,
        #[starlark(require = pos)] _destination: &str,
        #[starlark(require = named, default = false)] _overwrite: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.copy is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn rename<'v>(
        #[starlark(require = pos)] _source: &str,
        #[starlark(require = pos)] _destination: &str,
        #[starlark(require = named, default = false)] _overwrite: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.rename is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn remove<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = named, default = false)] _recursive: bool,
        #[starlark(require = named, default = false)] _missing_ok: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.remove is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn write<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = pos)] _value: StarlarkValue<'v>,
        #[starlark(require = pos)] _format: &str,
        #[starlark(require = named, default = false)] _compact: bool,
        #[starlark(require = named, default = false)] _parents: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.write is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn write_all<'v>(
        #[starlark(require = pos)] _path: &str,
        #[starlark(require = pos)] _values: StarlarkValue<'v>,
        #[starlark(require = pos)] _format: &str,
        #[starlark(require = named, default = false)] _compact: bool,
        #[starlark(require = named, default = false)] _parents: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.write_all is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn write_batch<'v>(
        #[starlark(require = pos)] _entries: StarlarkValue<'v>,
        #[starlark(require = pos)] _format: &str,
        #[starlark(require = named, default = false)] _compact: bool,
        #[starlark(require = named, default = false)] _parents: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.write_batch is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }

    fn write_batch_all<'v>(
        #[starlark(require = pos)] _entries: StarlarkValue<'v>,
        #[starlark(require = pos)] _format: &str,
        #[starlark(require = named, default = false)] _compact: bool,
        #[starlark(require = named, default = false)] _parents: bool,
        _eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.write_batch_all is disabled, pass --starlark-filesystem or --starlark-unsafe",
        )))
    }
}

#[starlark_module]
fn aq_env_enabled(builder: &mut GlobalsBuilder) {
    fn env<'v>(
        #[starlark(require = pos)] name: &str,
        heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        match std::env::var(name) {
            Ok(value) => Ok(heap.alloc(value)),
            Err(std::env::VarError::NotPresent) => Ok(StarlarkValue::new_none()),
            Err(error) => Err(starlark::Error::new_other(StarlarkBuiltinError::new(
                format!("failed to read environment variable `{name}`: {error}"),
            ))),
        }
    }
}

#[starlark_module]
fn aq_env_disabled(builder: &mut GlobalsBuilder) {
    fn env<'v>(
        #[starlark(require = pos)] _name: &str,
        _heap: &'v Heap,
    ) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.env is disabled, pass --starlark-environment or --starlark-unsafe",
        )))
    }
}

#[starlark_module]
fn aq_time_enabled(builder: &mut GlobalsBuilder) {
    fn timestamp<'v>(heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(Utc::now().timestamp()))
    }

    fn now<'v>(heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(AqDateTime::new(Utc::now())))
    }

    fn today<'v>(heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        Ok(heap.alloc(AqDate::new(Utc::now().date_naive())))
    }
}

#[starlark_module]
fn aq_time_disabled(builder: &mut GlobalsBuilder) {
    fn timestamp<'v>(_heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.timestamp is disabled, pass --starlark-time or --starlark-unsafe",
        )))
    }

    fn now<'v>(_heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.now is disabled, pass --starlark-time or --starlark-unsafe",
        )))
    }

    fn today<'v>(_heap: &'v Heap) -> starlark::Result<StarlarkValue<'v>> {
        Err(starlark::Error::new_other(StarlarkBuiltinError::new(
            "aq.today is disabled, pass --starlark-time or --starlark-unsafe",
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::format::{DetectConflictPolicy, Format};
    use crate::starlark::{
        collapse_documents, evaluate_inline, StarlarkCapabilities, StarlarkContext,
    };
    use crate::value::Value;

    fn context(
        capabilities: StarlarkCapabilities,
        detect_conflicts: DetectConflictPolicy,
        current_format_name: Option<&str>,
    ) -> StarlarkContext {
        StarlarkContext::new(
            capabilities,
            detect_conflicts,
            current_format_name.map(str::to_owned),
            Path::new(".").to_path_buf(),
        )
    }

    #[test]
    fn collapses_multiple_documents_into_an_array() {
        let value = collapse_documents(vec![
            crate::format::InputDocument {
                value: Value::Integer(1),
                format: Format::Json,
            },
            crate::format::InputDocument {
                value: Value::Integer(2),
                format: Format::Json,
            },
        ]);
        assert_eq!(
            value,
            Value::Array(vec![Value::Integer(1), Value::Integer(2)])
        );
    }

    #[test]
    fn starlark_unsafe_enables_all_capabilities() {
        let capabilities = StarlarkCapabilities::from_flags(false, false, false, true);
        assert!(capabilities.filesystem);
        assert!(capabilities.environment);
        assert!(capabilities.time);
    }

    #[test]
    fn evaluates_query_all_helper_inline() {
        let input = Value::from_json(serde_json::json!({
            "users": [
                {"name": "alice", "active": true},
                {"name": "bob", "active": false},
                {"name": "carol", "active": true}
            ]
        }))
        .expect("value should parse");
        let value = evaluate_inline(
            "[user[\"name\"] for user in aq.query_all(\".users[] | select(.active)\", data)]",
            &input,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::Array(vec![
                Value::String("alice".to_string()),
                Value::String("carol".to_string()),
            ])
        );
    }

    #[test]
    fn evaluates_query_one_and_format_helpers_inline() {
        let input = Value::from_json(serde_json::json!({
            "service": {"port": 8080}
        }))
        .expect("value should parse");
        let value = evaluate_inline(
            "{\"format\": aq.format(), \"port\": aq.query_one(\".service.port\", data)}",
            &input,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("yaml"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({"format": "yaml", "port": 8080}))
                .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_parse_and_render_helpers_inline() {
        let value = evaluate_inline(
            "aq.render({\"name\": aq.parse(\"name: alice\\nage: 30\\n\", \"yaml\")[\"name\"]}, \"json\", compact = True)",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(value, Value::String("{\"name\":\"alice\"}\n".to_string()));
    }

    #[test]
    fn evaluates_table_render_helper_inline() {
        let value = evaluate_inline(
            "aq.render([{\"name\": \"alice\", \"role\": \"admin\"}, {\"name\": \"bob\", \"role\": \"ops\"}], \"table\")",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::String("name   role\n-----  -----\nalice  admin\nbob    ops\n".to_string())
        );
    }

    #[test]
    fn evaluates_csv_and_tsv_parse_helpers_inline() {
        let value = evaluate_inline(
            "{\"csv\": aq.parse_all('alice,\"a,b\"\\n', \"csv\"), \"tsv\": aq.parse('alice\\ta\\\\tb\\n', \"tsv\")}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "csv": [["alice", "a,b"]],
                "tsv": ["alice", "a\tb"]
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_typed_date_and_datetime_helpers_inline() {
        let value = evaluate_inline(
            "{\"date\": (aq.date(\"2026-03-30\") + aq.timedelta(days = 1)).isoformat(), \"datetime\": (aq.datetime(\"2026-03-30T12:30:00Z\") + aq.timedelta(hours = 2)).isoformat(), \"delta_seconds\": (aq.datetime(\"2026-03-30T14:30:00Z\") - aq.datetime(\"2026-03-30T12:30:00Z\")).total_seconds()}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "date": "2026-03-31",
                "datetime": "2026-03-30T14:30:00Z",
                "delta_seconds": 7200.0
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_temporal_methods_inline() {
        let value = evaluate_inline(
            "{\"replaced_date\": aq.date(\"2026-03-30\").replace(day = 31).isoformat(), \"weekday\": aq.date(\"2026-03-30\").weekday(), \"ordinal\": aq.date(\"2026-03-30\").ordinal, \"at\": aq.date(\"2026-03-30\").at(hour = 9, minute = 15).isoformat(), \"from_datetime\": aq.datetime(\"2026-03-30T12:30:45Z\").date().isoformat(), \"replaced_datetime\": aq.datetime(\"2026-03-30T12:30:45Z\").replace(day = 31, hour = 8, minute = 0, second = 0).isoformat(), \"epoch\": aq.datetime(\"1970-01-01T00:00:01Z\").timestamp()}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "replaced_date": "2026-03-31",
                "weekday": 0,
                "ordinal": 89,
                "at": "2026-03-30T09:15:00Z",
                "from_datetime": "2026-03-30",
                "replaced_datetime": "2026-03-31T08:00:00Z",
                "epoch": 1.0
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_time_helpers_and_extended_timedelta_inline() {
        let value = evaluate_inline(
            "{\"same_day\": aq.now().date() == aq.today(), \"shifted\": (aq.datetime(\"2026-03-30T12:30:00Z\") + aq.timedelta(weeks = 1, milliseconds = 250)).isoformat(), \"fractional\": aq.timedelta(milliseconds = 250, microseconds = 500, nanoseconds = 600).total_seconds()}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, true, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "same_day": true,
                "shifted": "2026-04-06T12:30:00.250Z",
                "fractional": 0.2505006
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_temporal_equality_and_hashing_inline() {
        let value = evaluate_inline(
            "{\"date_equal\": aq.date(\"2026-03-30\") == aq.date(\"2026-03-30\"), \"datetime_equal\": aq.datetime(\"2026-03-30T12:30:00Z\") == aq.datetime(\"2026-03-30T12:30:00Z\"), \"delta_equal\": aq.timedelta(days = 1) == aq.timedelta(hours = 24), \"date_lookup\": {aq.date(\"2026-03-30\"): \"ok\"}[aq.date(\"2026-03-30\")], \"datetime_lookup\": {aq.datetime(\"2026-03-30T12:30:00Z\"): \"ok\"}[aq.datetime(\"2026-03-30T12:30:00Z\")], \"delta_lookup\": {aq.timedelta(days = 1): \"ok\"}[aq.timedelta(hours = 24)]}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "date_equal": true,
                "datetime_equal": true,
                "delta_equal": true,
                "date_lookup": "ok",
                "datetime_lookup": "ok",
                "delta_lookup": "ok"
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn rejects_invalid_temporal_replace_components() {
        let error = evaluate_inline(
            "aq.date(\"2026-03-30\").replace(month = 2, day = 31)",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect_err("invalid replace should fail");
        assert!(error
            .to_string()
            .contains("invalid date components year=2026 month=2 day=31"));
    }

    #[test]
    fn preserves_toml_date_types_inside_starlark() {
        let value = evaluate_inline(
            "aq.parse(\"day = 2026-03-30\\nat = 2026-03-30T12:30:00Z\\n\", \"toml\")[\"day\"] + aq.timedelta(days = 1)",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("toml"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::Date(
                chrono::NaiveDate::from_ymd_opt(2026, 3, 31).expect("date should be valid"),
            )
        );
    }

    #[test]
    fn evaluates_parse_all_and_render_all_helpers_inline() {
        let value = evaluate_inline(
            "aq.render_all(aq.parse_all('{\"name\":\"alice\"}\\n{\"name\":\"bob\"}\\n', \"jsonl\"), \"yaml\")",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::String("name: alice\n---\nname: bob\n".to_string())
        );
    }

    #[test]
    fn read_as_uses_explicit_format_override() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("aq-starlark-{unique}.txt"));
        fs::write(&path, "name: alice\nage: 30\n").expect("temp file should write");
        let source = format!("aq.read_as({path:?}, \"yaml\")[\"name\"]");
        let value = evaluate_inline(
            &source,
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(value, Value::String("alice".to_string()));
        fs::remove_file(path).expect("temp file should clean up");
    }

    #[test]
    fn evaluates_read_all_helper_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-read-all-{unique}"));
        fs::create_dir_all(&directory).expect("temp dir should create");
        let path = directory.join("docs.yaml");
        fs::write(&path, "---\nname: alice\n---\nname: bob\n").expect("temp file should write");
        let source = "aq.read_all(\"docs.yaml\")";
        let value = evaluate_inline(
            source,
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!([
                {"name": "alice"},
                {"name": "bob"}
            ]))
            .expect("value should parse")
        );
        fs::remove_file(path).expect("temp file should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_read_glob_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-read-glob-{unique}"));
        fs::create_dir_all(directory.join("configs")).expect("configs dir should create");
        fs::write(
            directory.join("configs").join("app.yaml"),
            "kind: ConfigMap\nmetadata:\n  name: app-config\n",
        )
        .expect("yaml should write");
        fs::write(
            directory.join("configs").join("bundle.txt"),
            "---\nkind: Service\nmetadata:\n  name: app-service\n---\nkind: ConfigMap\nmetadata:\n  name: extra-config\n",
        )
        .expect("bundle should write");
        let value = evaluate_inline(
            "{\"files\": aq.read_glob(\"configs/*.yaml\"), \"docs\": aq.read_glob_all_as(\"configs/*.txt\", \"yaml\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "files": [
                    {
                        "path": "configs/app.yaml",
                        "value": {
                            "kind": "ConfigMap",
                            "metadata": {
                                "name": "app-config"
                            }
                        }
                    }
                ],
                "docs": [
                    {
                        "path": "configs/bundle.txt",
                        "index": 0,
                        "value": {
                            "kind": "Service",
                            "metadata": {
                                "name": "app-service"
                            }
                        }
                    },
                    {
                        "path": "configs/bundle.txt",
                        "index": 1,
                        "value": {
                            "kind": "ConfigMap",
                            "metadata": {
                                "name": "extra-config"
                            }
                        }
                    }
                ]
            }))
            .expect("value should parse")
        );
        fs::remove_file(directory.join("configs").join("app.yaml")).expect("yaml should clean up");
        fs::remove_file(directory.join("configs").join("bundle.txt"))
            .expect("bundle should clean up");
        fs::remove_dir(directory.join("configs")).expect("configs dir should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_filesystem_path_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-paths-{unique}"));
        fs::create_dir_all(&directory).expect("temp dir should create");
        fs::write(directory.join("alpha.txt"), "alpha").expect("file should write");
        fs::create_dir(directory.join("nested")).expect("nested dir should create");
        let value = evaluate_inline(
            "{\"base\": aq.base_dir(), \"entries\": aq.list_dir(), \"file\": aq.is_file(\"alpha.txt\"), \"dir\": aq.is_dir(\"nested\"), \"exists\": aq.exists(\"nested\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "base": directory.to_string_lossy(),
                "entries": ["alpha.txt", "nested"],
                "file": true,
                "dir": true,
                "exists": true
            }))
            .expect("value should parse")
        );
        fs::remove_file(directory.join("alpha.txt")).expect("file should clean up");
        fs::remove_dir(directory.join("nested")).expect("nested dir should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_glob_and_relative_path_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-glob-{unique}"));
        fs::create_dir_all(directory.join("configs")).expect("configs should create");
        fs::create_dir_all(directory.join("nested").join("deeper"))
            .expect("nested dirs should create");
        fs::write(directory.join("configs").join("app.yaml"), "name: api\n")
            .expect("yaml should write");
        fs::write(
            directory.join("configs").join("app.json"),
            "{\"name\":\"api\"}\n",
        )
        .expect("json should write");
        fs::write(
            directory.join("nested").join("deeper").join("service.yaml"),
            "kind: Service\n",
        )
        .expect("service yaml should write");
        fs::write(directory.join("nested").join("x1.txt"), "xray").expect("text should write");

        let value = evaluate_inline(
            "{\"yaml\": aq.glob(\"**/*.yaml\"), \"txt\": aq.glob(\"nested/?1.txt\"), \"absolute\": aq.resolve_path(\"nested/../configs/app.yaml\"), \"relative\": aq.relative_path(\"configs/app.yaml\", start = \"nested/deeper\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");

        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "yaml": ["configs/app.yaml", "nested/deeper/service.yaml"],
                "txt": ["nested/x1.txt"],
                "absolute": directory.join("configs/app.yaml").to_string_lossy(),
                "relative": "../../configs/app.yaml"
            }))
            .expect("value should parse")
        );

        fs::remove_file(directory.join("configs").join("app.yaml")).expect("yaml should clean up");
        fs::remove_file(directory.join("configs").join("app.json")).expect("json should clean up");
        fs::remove_file(directory.join("nested").join("deeper").join("service.yaml"))
            .expect("service should clean up");
        fs::remove_file(directory.join("nested").join("x1.txt")).expect("text should clean up");
        fs::remove_dir(directory.join("nested").join("deeper")).expect("deeper should clean up");
        fs::remove_dir(directory.join("nested")).expect("nested should clean up");
        fs::remove_dir(directory.join("configs")).expect("configs should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_write_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-write-{unique}"));
        fs::create_dir_all(&directory).expect("temp dir should create");
        let value = evaluate_inline(
            "written = aq.write_text(\"note.txt\", \"hello\\n\"); payload = aq.write(\"data.json\", {\"name\": \"alice\"}, \"json\", compact = True); {\"written\": written, \"payload\": payload, \"note\": aq.read_text(\"note.txt\"), \"data\": aq.read_as(\"data.json\", \"json\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "written": 6,
                "payload": 17,
                "note": "hello\n",
                "data": {"name": "alice"}
            }))
            .expect("value should parse")
        );
        fs::remove_file(directory.join("note.txt")).expect("note should clean up");
        fs::remove_file(directory.join("data.json")).expect("data should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_write_batch_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-write-batch-{unique}"));
        fs::create_dir_all(&directory).expect("temp dir should create");
        let value = evaluate_inline(
            "single = aq.write_batch([{\"path\": \"out/one.json\", \"value\": {\"name\": \"alice\"}}], \"json\", compact = True, parents = True)\nmulti = aq.write_batch_all([{\"path\": \"out/two.yaml\", \"values\": [{\"name\": \"alice\"}, {\"name\": \"bob\"}]}], \"yaml\", parents = True)\n{\"single\": single, \"multi\": multi, \"one\": aq.read_as(\"out/one.json\", \"json\"), \"two\": aq.read_all_as(\"out/two.yaml\", \"yaml\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "single": [
                    {
                        "path": "out/one.json",
                        "bytes": 17
                    }
                ],
                "multi": [
                    {
                        "path": "out/two.yaml",
                        "bytes": 26
                    }
                ],
                "one": {
                    "name": "alice"
                },
                "two": [
                    {
                        "name": "alice"
                    },
                    {
                        "name": "bob"
                    }
                ]
            }))
            .expect("value should parse")
        );
        fs::remove_file(directory.join("out").join("one.json")).expect("one should clean up");
        fs::remove_file(directory.join("out").join("two.yaml")).expect("two should clean up");
        fs::remove_dir(directory.join("out")).expect("out should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_text_glob_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-text-glob-{unique}"));
        fs::create_dir_all(directory.join("notes").join("nested")).expect("dirs should create");
        fs::write(directory.join("notes").join("alpha.txt"), "alpha\n")
            .expect("alpha should write");
        fs::write(
            directory.join("notes").join("nested").join("beta.txt"),
            "beta\n",
        )
        .expect("beta should write");
        let value = evaluate_inline(
            "entries = aq.read_text_glob(\"notes/**/*.txt\")\nwrites = aq.write_text_batch([{\"path\": \"out/\" + entry[\"path\"], \"text\": \"# Source: \" + entry[\"path\"] + \"\\n\\n\" + entry[\"text\"]} for entry in entries], parents = True)\n{\"entries\": entries, \"writes\": writes, \"rendered\": aq.read_text(\"out/notes/alpha.txt\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "entries": [
                    {
                        "path": "notes/alpha.txt",
                        "text": "alpha\n"
                    },
                    {
                        "path": "notes/nested/beta.txt",
                        "text": "beta\n"
                    }
                ],
                "writes": [
                    {
                        "path": "out/notes/alpha.txt",
                        "bytes": 33
                    },
                    {
                        "path": "out/notes/nested/beta.txt",
                        "bytes": 38
                    }
                ],
                "rendered": "# Source: notes/alpha.txt\n\nalpha\n"
            }))
            .expect("value should parse")
        );
        fs::remove_file(directory.join("notes").join("alpha.txt")).expect("alpha should clean up");
        fs::remove_file(directory.join("notes").join("nested").join("beta.txt"))
            .expect("beta should clean up");
        fs::remove_file(directory.join("out").join("notes").join("alpha.txt"))
            .expect("rendered alpha should clean up");
        fs::remove_file(
            directory
                .join("out")
                .join("notes")
                .join("nested")
                .join("beta.txt"),
        )
        .expect("rendered beta should clean up");
        fs::remove_dir(directory.join("out").join("notes").join("nested"))
            .expect("rendered nested should clean up");
        fs::remove_dir(directory.join("out").join("notes"))
            .expect("rendered notes should clean up");
        fs::remove_dir(directory.join("out")).expect("out should clean up");
        fs::remove_dir(directory.join("notes").join("nested")).expect("nested should clean up");
        fs::remove_dir(directory.join("notes")).expect("notes should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_rewrite_text_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-rewrite-text-{unique}"));
        fs::create_dir_all(directory.join("notes").join("nested")).expect("dirs should create");
        fs::write(directory.join("notes").join("alpha.txt"), "alpha\n")
            .expect("alpha should write");
        fs::write(
            directory.join("notes").join("nested").join("beta.txt"),
            "beta\n",
        )
        .expect("beta should write");
        let value = evaluate_inline(
            "def annotate(path, text):\n    return \"# Source: \" + path + \"\\n\\n\" + text.upper()\n\n{\"single\": aq.rewrite_text(\"notes/alpha.txt\", annotate), \"batch\": aq.rewrite_text_glob(\"notes/nested/**/*.txt\", annotate), \"alpha\": aq.read_text(\"notes/alpha.txt\"), \"beta\": aq.read_text(\"notes/nested/beta.txt\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        let json = value.to_json().expect("value should serialize");
        assert_eq!(json["alpha"], "# Source: notes/alpha.txt\n\nALPHA\n");
        assert_eq!(json["beta"], "# Source: notes/nested/beta.txt\n\nBETA\n");
        assert!(json["single"].as_i64().expect("single should be integer") > 0);
        assert_eq!(
            json["batch"],
            serde_json::json!([{
                "path": "notes/nested/beta.txt",
                "bytes": 38
            }])
        );
        fs::remove_file(directory.join("notes").join("alpha.txt")).expect("alpha should clean up");
        fs::remove_file(directory.join("notes").join("nested").join("beta.txt"))
            .expect("beta should clean up");
        fs::remove_dir(directory.join("notes").join("nested")).expect("nested should clean up");
        fs::remove_dir(directory.join("notes")).expect("notes should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_walk_and_mkdir_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-walk-{unique}"));
        fs::create_dir_all(&directory).expect("temp dir should create");
        fs::create_dir(directory.join("nested")).expect("nested should create");
        fs::write(directory.join("nested").join("a.txt"), "alpha").expect("file should write");
        fs::write(directory.join("b.txt"), "bravo").expect("file should write");
        let value = evaluate_inline(
            "created = aq.mkdir(\"out/deeper\", parents = True); {\"created\": created, \"files\": aq.walk_files(include_dirs = True), \"nested\": aq.walk_files(path = \"nested\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "created": directory.join("out/deeper").to_string_lossy(),
                "files": ["b.txt", "nested", "nested/a.txt", "out", "out/deeper"],
                "nested": ["a.txt"]
            }))
            .expect("value should parse")
        );
        fs::remove_file(directory.join("nested").join("a.txt")).expect("file should clean up");
        fs::remove_file(directory.join("b.txt")).expect("file should clean up");
        fs::remove_dir(directory.join("out").join("deeper")).expect("dir should clean up");
        fs::remove_dir(directory.join("out")).expect("dir should clean up");
        fs::remove_dir(directory.join("nested")).expect("dir should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_stat_copy_rename_and_remove_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-mutate-{unique}"));
        fs::create_dir_all(&directory).expect("temp dir should create");
        fs::write(directory.join("source.txt"), "alpha").expect("file should write");
        let value = evaluate_inline(
            "copied = aq.copy(\"source.txt\", \"copy.txt\"); renamed = aq.rename(\"copy.txt\", \"final.txt\"); removed = aq.remove(\"source.txt\"); {\"copied\": copied, \"renamed\": renamed, \"removed\": removed, \"stat\": aq.stat(\"final.txt\"), \"missing\": aq.remove(\"missing.txt\", missing_ok = True)}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        let stat = value.to_json().expect("value should serialize");
        assert_eq!(stat["copied"], 5);
        assert_eq!(stat["removed"], true);
        assert_eq!(stat["missing"], false);
        assert_eq!(
            stat["renamed"],
            serde_json::Value::String(directory.join("final.txt").to_string_lossy().into_owned())
        );
        assert_eq!(stat["stat"]["type"], "file");
        assert_eq!(stat["stat"]["size"], 5);
        assert_eq!(
            fs::read_to_string(directory.join("final.txt")).expect("file should read"),
            "alpha"
        );
        fs::remove_file(directory.join("final.txt")).expect("file should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_regex_base64_and_hash_helpers_inline() {
        let value = evaluate_inline(
            "{\"matched\": aq.regex_is_match(\"user-[0-9]+\", \"user-42\"), \"found\": aq.regex_find(\"[0-9]+\", \"user-42\"), \"all\": aq.regex_find_all(\"[a-z]+\", \"alpha 42 beta\"), \"capture\": aq.regex_capture(\"(?P<name>[a-z]+)-(?P<id>[0-9]+)\", \"user-42\"), \"replaced\": aq.regex_replace_all(\"[0-9]+\", \"XX\", \"user-42 id-7\"), \"encoded\": aq.base64_encode(\"hello\"), \"decoded\": aq.base64_decode(\"aGVsbG8=\"), \"hash\": aq.hash(\"hello\", algorithm = \"sha256\")}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        let json = value.to_json().expect("value should serialize");
        assert_eq!(json["matched"], true);
        assert_eq!(json["found"], "42");
        assert_eq!(json["all"], serde_json::json!(["alpha", "beta"]));
        assert_eq!(json["capture"]["match"], "user-42");
        assert_eq!(json["capture"]["groups"], serde_json::json!(["user", "42"]));
        assert_eq!(json["capture"]["named"]["name"], "user");
        assert_eq!(json["capture"]["named"]["id"], "42");
        assert_eq!(json["replaced"], "user-XX id-XX");
        assert_eq!(json["encoded"], "aGVsbG8=");
        assert_eq!(json["decoded"], "hello");
        assert_eq!(
            json["hash"],
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn evaluates_regex_capture_all_split_and_hash_file_helpers_inline() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("aq-starlark-hash-file-{unique}"));
        fs::create_dir_all(&directory).expect("temp dir should create");
        fs::write(directory.join("payload.bin"), [0_u8, 255_u8, 16_u8])
            .expect("payload should write");
        let value = evaluate_inline(
            "{\"captures\": aq.regex_capture_all(\"(?P<word>[a-z]+)-(?P<id>[0-9]+)\", \"user-42 admin-7\"), \"split\": aq.regex_split(\"[,;]\", \"alpha,beta;gamma\"), \"digest\": aq.hash_file(\"payload.bin\")}",
            &Value::Null,
            &StarlarkContext::new(
                StarlarkCapabilities::from_flags(true, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json".to_string()),
                directory.clone(),
            ),
        )
        .expect("starlark should run");
        let json = value.to_json().expect("value should serialize");
        assert_eq!(json["captures"][0]["match"], "user-42");
        assert_eq!(json["captures"][0]["named"]["word"], "user");
        assert_eq!(json["captures"][1]["named"]["id"], "7");
        assert_eq!(json["split"], serde_json::json!(["alpha", "beta", "gamma"]));
        assert_eq!(
            json["digest"],
            "2da45f2cd1f9c8e69a67abf7a6b26c282533d0a7686787a9533265418680d4d2"
        );
        fs::remove_file(directory.join("payload.bin")).expect("payload should clean up");
        fs::remove_dir(directory).expect("temp dir should clean up");
    }

    #[test]
    fn evaluates_string_normalization_escape_and_hash_convenience_helpers_inline() {
        let value = evaluate_inline(
            "{\"slug\": aq.slug(\"HTTPServer v2\"), \"snake\": aq.snake_case(\"HTTPServer v2\"), \"kebab\": aq.kebab_case(\"user_profile ID\"), \"camel\": aq.camel_case(\"user profile_id\"), \"title\": aq.title_case(\"http_server v2\"), \"trimmed\": aq.trim_suffix(aq.trim_prefix(\"refs/tags/v1.2.3\", \"refs/tags/\"), \".3\"), \"regex\": aq.regex_escape(\"a+b?(c)\"), \"shell\": aq.shell_escape(\"hello 'quoted' world\"), \"encoded\": aq.url_encode_component(\"a b/c?d=e&f\"), \"decoded\": aq.url_decode_component(\"a%20b%2Fc%3Fd%3De%26f\"), \"sha1\": aq.sha1(\"hello\"), \"sha256_b64\": aq.sha256(\"hello\", encoding = \"base64\"), \"sha512\": aq.sha512(\"hello\"), \"blake3\": aq.blake3(\"hello\")}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        let json = value.to_json().expect("value should serialize");
        assert_eq!(json["slug"], "http-server-v-2");
        assert_eq!(json["snake"], "http_server_v_2");
        assert_eq!(json["kebab"], "user-profile-id");
        assert_eq!(json["camel"], "userProfileId");
        assert_eq!(json["title"], "Http Server V 2");
        assert_eq!(json["trimmed"], "v1.2");
        assert_eq!(json["regex"], "a\\+b\\?\\(c\\)");
        assert_eq!(json["shell"], "'hello '\\''quoted'\\'' world'");
        assert_eq!(json["encoded"], "a%20b%2Fc%3Fd%3De%26f");
        assert_eq!(json["decoded"], "a b/c?d=e&f");
        assert_eq!(json["sha1"], "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
        assert_eq!(
            json["sha256_b64"],
            "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ="
        );
        assert_eq!(
            json["sha512"],
            "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
        );
        assert_eq!(
            json["blake3"],
            "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f"
        );
    }

    #[test]
    fn evaluates_semver_helpers_inline() {
        let value = evaluate_inline(
            "{\"parsed\": aq.semver_parse(\"1.2.3-rc.4+git.7\"), \"cmp_release\": aq.semver_compare(\"1.2.3-rc.1\", \"1.2.3\"), \"cmp_build\": aq.semver_compare(\"1.2.3\", \"1.2.3+build.9\"), \"minor\": aq.semver_bump(\"1.2.3\", \"minor\"), \"pre\": aq.semver_bump(\"1.2.3\", \"prerelease\"), \"pre_next\": aq.semver_bump(\"1.2.3-rc.4+git.7\", \"prerelease\"), \"release\": aq.semver_bump(\"1.2.3-rc.4+git.7\", \"release\")}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        let json = value.to_json().expect("value should serialize");
        assert_eq!(
            json["parsed"],
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
        assert_eq!(json["cmp_release"], -1);
        assert_eq!(json["cmp_build"], 0);
        assert_eq!(json["minor"], "1.3.0");
        assert_eq!(json["pre"], "1.2.3-rc.1");
        assert_eq!(json["pre_next"], "1.2.3-rc.5");
        assert_eq!(json["release"], "1.2.3");
    }

    #[test]
    fn evaluates_merge_drop_nulls_and_sort_keys_helpers_inline() {
        let value = evaluate_inline(
            "aq.sort_keys(aq.drop_nulls(aq.merge_all([{\"service\": {\"port\": 8080, \"name\": \"api\"}, \"flags\": [1, None, 2], \"meta\": {\"owner\": None}}, {\"service\": {\"port\": 8443}, \"meta\": {\"team\": \"platform\"}, \"extra\": None}], deep = True), recursive = True), recursive = True)",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "flags": [1, 2],
                "meta": {
                    "team": "platform"
                },
                "service": {
                    "name": "api",
                    "port": 8443
                }
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_get_set_and_delete_path_helpers_inline() {
        let value = evaluate_inline(
            "{\"port\": aq.get_path({\"service\": {\"port\": 8080}}, [\"service\", \"port\"]), \"missing\": aq.get_path({\"service\": {\"port\": 8080}}, [\"service\", \"host\"]), \"created\": aq.set_path(None, [\"meta\", \"labels\", \"env\"], \"prod\"), \"rewritten\": aq.delete_paths(aq.set_path({\"items\": [1, 2], \"meta\": {\"uid\": \"x\", \"name\": \"api\"}}, [\"items\", -1], 9), [[\"meta\", \"uid\"], [\"items\", 0]])}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "port": 8080,
                "missing": null,
                "created": {
                    "meta": {
                        "labels": {
                            "env": "prod"
                        }
                    }
                },
                "rewritten": {
                    "items": [9],
                    "meta": {
                        "name": "api"
                    }
                }
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_clean_k8s_metadata_helper_inline() {
        let value = evaluate_inline(
            "aq.clean_k8s_metadata({\"apiVersion\": \"v1\", \"items\": [{\"kind\": \"ConfigMap\", \"metadata\": {\"name\": \"one\", \"uid\": \"a\", \"namespace\": \"apps\", \"ownerReferences\": [{\"name\": \"parent\"}], \"labels\": {\"tier\": \"backend\"}}, \"spec\": {\"template\": {\"metadata\": {\"annotations\": {\"checksum/config\": \"abc\"}}}}}, {\"kind\": \"Secret\", \"metadata\": {\"name\": \"two\", \"annotations\": {\"note\": \"keep\"}, \"resourceVersion\": \"7\"}}], \"metadata\": {\"resourceVersion\": \"1\", \"annotations\": {\"team\": \"platform\"}}})",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "apiVersion": "v1",
                "items": [
                    {
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "one",
                            "namespace": "apps",
                            "labels": {
                                "tier": "backend"
                            }
                        },
                        "spec": {
                            "template": {
                                "metadata": {
                                    "annotations": {
                                        "checksum/config": "abc"
                                    }
                                }
                            }
                        },
                    },
                    {
                        "kind": "Secret",
                        "metadata": {
                            "name": "two",
                            "annotations": {
                                "note": "keep"
                            }
                        }
                    }
                ],
                "metadata": {
                    "annotations": {
                        "team": "platform"
                    }
                }
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_walk_and_walk_paths_helpers_inline() {
        let value = evaluate_inline(
            "def normalize(value):\n    if type(value) == \"string\":\n        return value.strip()\n    return value\n\ndef patch(path, value):\n    if path == [\"metadata\", \"labels\", \"tier\"]:\n        return value.upper()\n    return value\n\n{\"trimmed\": aq.walk({\"name\": \"  api  \", \"items\": [\" one \", 2]}, normalize), \"patched\": aq.walk_paths({\"metadata\": {\"labels\": {\"tier\": \"backend\", \"name\": \"api\"}}}, patch)}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "trimmed": {
                    "name": "api",
                    "items": ["one", 2]
                },
                "patched": {
                    "metadata": {
                        "labels": {
                            "tier": "BACKEND",
                            "name": "api"
                        }
                    }
                }
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_paths_find_paths_and_collect_paths_helpers_inline() {
        let value = evaluate_inline(
            "def is_secret(path, value):\n    leaf = path[len(path) - 1]\n    return type(leaf) == \"string\" and leaf in [\"password\", \"token\"]\n\ndef describe(path, value):\n    return {\"path\": path, \"value\": value}\n\n{\"all\": aq.paths({\"auth\": {\"password\": \"secret\"}, \"nested\": [{\"token\": \"abc\"}, {\"name\": \"api\"}]}, leaves_only = True), \"matches\": aq.find_paths({\"auth\": {\"password\": \"secret\"}, \"nested\": [{\"token\": \"abc\"}, {\"name\": \"api\"}]}, is_secret, leaves_only = True), \"described\": aq.collect_paths({\"auth\": {\"password\": \"secret\"}, \"nested\": [{\"token\": \"abc\"}, {\"name\": \"api\"}]}, describe, leaves_only = True)}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "all": [
                    ["auth", "password"],
                    ["nested", 0, "token"],
                    ["nested", 1, "name"]
                ],
                "matches": [
                    ["auth", "password"],
                    ["nested", 0, "token"]
                ],
                "described": [
                    {"path": ["auth", "password"], "value": "secret"},
                    {"path": ["nested", 0, "token"], "value": "abc"},
                    {"path": ["nested", 1, "name"], "value": "api"}
                ]
            }))
            .expect("value should parse")
        );
    }

    #[test]
    fn evaluates_pick_and_omit_helpers_inline() {
        let value = evaluate_inline(
            "def is_secret(path, value):\n    leaf = path[len(path) - 1]\n    return type(leaf) == \"string\" and leaf in [\"password\", \"token\"]\n\nsource = {\"service\": {\"port\": 8080}, \"metadata\": {\"labels\": {\"tier\": \"backend\"}, \"annotations\": {\"note\": \"remove\"}}, \"auth\": {\"token\": \"abc\"}, \"items\": [{\"name\": \"api\", \"password\": \"secret\"}, {\"name\": \"worker\"}]}\n\n{\"picked\": aq.pick_paths(source, [[\"service\", \"port\"], [\"metadata\", \"labels\", \"missing\"], [\"items\", 0, \"name\"]]), \"omitted\": aq.omit_paths(source, [[\"metadata\", \"annotations\"], [\"auth\", \"token\"]]), \"picked_where\": aq.pick_where(source, is_secret, leaves_only = True), \"omitted_where\": aq.omit_where(source, is_secret, leaves_only = True)}",
            &Value::Null,
            &context(
                StarlarkCapabilities::from_flags(false, false, false, false),
                DetectConflictPolicy::WarnFallback,
                Some("json"),
            ),
        )
        .expect("starlark should run");
        assert_eq!(
            value,
            Value::from_json(serde_json::json!({
                "picked": {
                    "service": {
                        "port": 8080
                    },
                    "metadata": {
                        "labels": {
                            "missing": null
                        }
                    },
                    "items": [
                        {
                            "name": "api"
                        }
                    ]
                },
                "omitted": {
                    "service": {
                        "port": 8080
                    },
                    "metadata": {
                        "labels": {
                            "tier": "backend"
                        }
                    },
                    "auth": {},
                    "items": [
                        {
                            "name": "api",
                            "password": "secret"
                        },
                        {
                            "name": "worker"
                        }
                    ]
                },
                "picked_where": {
                    "auth": {
                        "token": "abc"
                    },
                    "items": [
                        {
                            "password": "secret"
                        }
                    ]
                },
                "omitted_where": {
                    "service": {
                        "port": 8080
                    },
                    "metadata": {
                        "labels": {
                            "tier": "backend"
                        },
                        "annotations": {
                            "note": "remove"
                        }
                    },
                    "auth": {},
                    "items": [
                        {
                            "name": "api"
                        },
                        {
                            "name": "worker"
                        }
                    ]
                }
            }))
            .expect("value should parse")
        );
    }
}
