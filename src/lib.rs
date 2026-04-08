mod app;
mod color;
mod error;
mod format;
mod inplace;
mod query;
#[cfg(feature = "starlark")]
mod starlark;
mod value;

pub fn run_cli() -> i32 {
    crate::app::run_cli()
}
