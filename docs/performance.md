# Performance

This document summarizes the current local `aq` vs `jq` benchmark results for
the harvested upstream jq direct-success test corpus.

The raw machine-generated artifacts live in the repository at:

- `benchmarks/jq-upstream-benchmark.md`
- `benchmarks/jq-upstream-benchmark.json`

## Setup

- Sources: jq upstream `tests/base64.test`, `tests/jq.test`, `tests/man.test`,
  `tests/manonig.test`, `tests/onig.test`, `tests/optional.test`, and
  `tests/uri.test`
- Harvested direct-success cases: `836`
- Comparable cases in this workspace: `833`
- `aq` binary: `target/release/aq`
- `jq` binary: local jq master build at
  `tmp/jq-master/build/install-pure/bin/jq`
- jq version: `jq-master-69785bf-dirty`
- Warmup runs per case: `1`
- Measured runs per case: `3`
- Per-run timeout: `10.0s`

The benchmark command is:

```bash
python3 scripts/jq_upstream_benchmark.py \
  --jq-binary "$PWD/tmp/jq-master/build/install-pure/bin/jq"
```

Use an absolute jq path here. The harness changes working directories while it
runs module and fixture cases.

## Headline

On the latest local jq-master run, `aq` is broadly at parity with jq and
slightly ahead on aggregate.

Aggregate results from the saved report:

- Compared cases: `833`
- `aq` faster cases: `411`
- `jq` faster cases: `185`
- Roughly equal cases: `237`
- Uncomparable cases: `3`
- Sum of jq medians: `1.343s`
- Sum of aq medians: `1.261s`
- Median `aq/jq` ratio: `0.95x`
- Geometric mean `aq/jq` ratio: `0.96x`

Interpretation:

- Ratios below `1.00x` mean `aq` is faster.
- Ratios above `1.00x` mean `aq` is slower.

The current release build is in the same performance tier as jq master on this
corpus, with a small overall aggregate edge to `aq`.

## Startup

Focused local one-shot measurements against the same jq-master build came out
to:

- `aq -n null`: about `1.30ms`
- `jq -n null`: about `1.50ms`
- `aq . --compact` on `{"a":1}`: about `1.32ms`
- `jq . -c` on `{"a":1}`: about `1.35ms`

So startup is also essentially at parity, with `aq` slightly ahead in these
small local checks.

## Notable Wins

The biggest speedups in the current run were:

- jq case `#391`, datetime roundtrip pipeline:
  jq `27.34ms`, aq `2.21ms`, `0.08x`
- jq case `#512`, deep `tojson` / `fromjson` / `flatten` case:
  jq `11.36ms`, aq `2.71ms`, `0.24x`
- jq case `#513`, deep `tojson` and `fromjson` depth-limit case:
  jq `5.21ms`, aq `2.86ms`, `0.55x`

These are real wins, but they are no longer representative of the whole story.
Against jq master, most of the suite is clustered much closer to parity.

## Notable Slowdowns

The largest remaining slowdowns in the current run were:

- jq case `#104`, destructuring swap:
  jq `1.46ms`, aq `2.23ms`, `1.52x`
- `manonig.test` case `#8`, regex global match:
  jq `1.38ms`, aq `1.93ms`, `1.40x`
- jq case `#468`, `try input catch .`:
  jq `1.57ms`, aq `2.16ms`, `1.38x`

These are still small absolute differences, but they are the main places where
plain jq master is currently tighter.

## Heaviest Cases

The heaviest cases called out in the saved report were:

1. jq case `#512`
   `reduce range(9999) as $_ ([];[.]) | tojson | fromjson | flatten`
   jq `11.36ms`, aq `2.71ms`, `0.24x`

2. jq case `#513`
   `reduce range(10000) as $_ ([];[.]) | tojson | try (fromjson) catch . | (contains("<skipped: too deep>") | not) and contains("Exceeds depth limit for parsing")`
   jq `5.21ms`, aq `2.86ms`, `0.55x`

3. jq case `#391`
   `last(range(365 * 67)|("1970-03-01T01:02:03Z"|strptime("%Y-%m-%dT%H:%M:%SZ")|mktime) + (86400 * .)|strftime("%Y-%m-%dT%H:%M:%SZ")|strptime("%Y-%m-%dT%H:%M:%SZ"))`
   jq `27.34ms`, aq `2.21ms`, `0.08x`

The deep JSON cases are still where most of the absolute time is, even though
two of the three are already wins.

## Caveats

This benchmark compares `aq` against a local jq-master build, not against the
older local `jq 1.8.1` baseline used earlier in the project.

There are `3` uncomparable cases in this workspace. In those cases, the local
jq-master build failed while `aq` produced the expected upstream result:

- `modulemeta`
- `modulemeta | .deps | length`
- `modulemeta | .defs | length`

The saved benchmark therefore represents:

- a strict local `aq` vs local jq-master performance comparison
- on the harvested upstream direct-success corpus
- with three jq-master local-build exceptions in the module metadata slice

## Current Read

Current assessment:

- `aq` is competitive with a current jq-master build on the harvested upstream
  corpus
- overall performance is near parity, with a modest aggregate edge to `aq`
- remaining performance work is targeted rather than structural

The best remaining optimization targets are:

- deep `tojson` and `fromjson` workloads, especially jq case `#512`
- low-millisecond overhead in simple map and arithmetic pipelines
- module metadata behavior, if we want cleaner apples-to-apples comparison on
  the last uncomparable cases
