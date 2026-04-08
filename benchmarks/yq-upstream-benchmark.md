## yq vs aq Upstream Benchmark

- Sources: `https://raw.githubusercontent.com/jqlang/jq/master/tests/base64.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/manonig.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/onig.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/optional.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/uri.test`
- Harvested direct success cases: `836`
- Warmup runs: `1`
- Measured runs: `3`
- Per-run timeout: `10.0s`
- yq binary: `yq`
- yq version: `yq (https://github.com/mikefarah/yq/) version v4.52.5`
- aq binary: `target/release/aq`
- aq version: `aq 0.1.0`
- Benchmark wall time: `26.4s`
- aq success cases: `836`
- yq success cases: `230`
- Compared overlap cases: `230`
- aq faster cases: `230`
- yq faster cases: `0`
- Roughly equal cases: `0`
- yq-incompatible or otherwise uncomparable cases: `606`
- Sum of yq medians: `1.194s`
- Sum of aq medians: `0.372s`
- Median aq/yq ratio: `0.31x`
- Geometric mean aq/yq ratio: `0.31x`

Interpretation: aq/yq ratios above `1.00x` mean aq is slower, below `1.00x` mean aq is faster.

Important: mikefarah/yq is not jq-compatible. This report benchmarks the overlap where `yq` happens to satisfy the jq upstream expected stdout. It is not a whole-language comparison.

### Biggest aq Speedups

- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#275` `[10 > 0, 10 > 10, 10 > 20, 10 < 0, 10 < 10, 10 < 20]`, yq `5.36ms`, aq `1.29ms`, aq/yq `0.24x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#45` `{"k": {"a": 1, "b": 2}} * {"k": {"a": 0,"c": 3}}`, yq `7.85ms`, aq `1.90ms`, aq/yq `0.24x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#64` `del(.foo)`, yq `6.14ms`, aq `1.51ms`, aq/yq `0.25x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#36` `.a + 1`, yq `5.92ms`, aq `1.46ms`, aq/yq `0.25x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#40` `.[-2] = 5`, yq `5.62ms`, aq `1.39ms`, aq/yq `0.25x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#34` `{(.user): .titles}`, yq `5.86ms`, aq `1.45ms`, aq/yq `0.25x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#15` `@base64d`, yq `5.84ms`, aq `1.45ms`, aq/yq `0.25x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#47` `[{}]`, yq `5.44ms`, aq `1.37ms`, aq/yq `0.25x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#157` `([1,2] + [4,5])`, yq `5.32ms`, aq `1.36ms`, aq/yq `0.26x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#324` `[.[]|split(",")]`, yq `5.20ms`, aq `1.33ms`, aq/yq `0.26x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#450` `.[] as $n | $n+0 | [., tostring, . == $n]`, yq `5.83ms`, aq `1.49ms`, aq/yq `0.26x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/base64.test#4` `@base64`, yq `5.34ms`, aq `1.37ms`, aq/yq `0.26x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#357` `{"k": {"a": 1, "b": 2}} * .`, yq `5.62ms`, aq `1.45ms`, aq/yq `0.26x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#14` `@base64`, yq `5.49ms`, aq `1.42ms`, aq/yq `0.26x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#251` `.foo[2].bar = 1`, yq `5.38ms`, aq `1.40ms`, aq/yq `0.26x`

### Heaviest Overlap Cases

- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#45` `{"k": {"a": 1, "b": 2}} * {"k": {"a": 0,"c": 3}}`, yq `7.85ms`, aq `1.90ms`, aq/yq `0.24x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#360` `{"a": {"b": 1}, "c": {"d": 2}, "e": 5} * .`, yq `7.14ms`, aq `1.93ms`, aq/yq `0.27x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#198` `.bar as $x | .foo | . + $x`, yq `6.68ms`, aq `1.95ms`, aq/yq `0.29x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#28` `.[4,2]`, yq `6.09ms`, aq `2.09ms`, aq/yq `0.34x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#27` `.["foo"].bar`, yq `6.33ms`, aq `1.82ms`, aq/yq `0.29x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#89` `any`, yq `6.06ms`, aq `2.04ms`, aq/yq `0.34x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#160` `[1,2,3]`, yq `6.24ms`, aq `1.85ms`, aq/yq `0.30x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#31` `[.user, .projects[]]`, yq `6.13ms`, aq `1.96ms`, aq/yq `0.32x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#123` `contains("bar")`, yq `6.23ms`, aq `1.84ms`, aq/yq `0.29x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#196` `[.[] | .a?]`, yq `5.91ms`, aq `2.15ms`, aq/yq `0.36x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#210` `[any,all]`, yq `5.99ms`, aq `1.95ms`, aq/yq `0.33x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#368` `flatten`, yq `5.94ms`, aq `1.86ms`, aq/yq `0.31x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#371` `flatten(2)`, yq `5.75ms`, aq `1.95ms`, aq/yq `0.34x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#359` `{"k": {"a": 1, "b": 2}, "hello": 1} * .`, yq `5.96ms`, aq `1.73ms`, aq/yq `0.29x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#65` `del(.[1, 2])`, yq `5.88ms`, aq `1.80ms`, aq/yq `0.31x`

### Sample yq Incompatibilities

- `https://raw.githubusercontent.com/jqlang/jq/master/tests/base64.test#6` `@base64d`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/base64.test#9` `. | try @base64d catch .`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/base64.test#10` `. | try @base64d catch .`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#5` `-1`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#8` `{x:-1},{x:-.},{x:-.|abs}`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#9` `.`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#10` `"Aa\r\n\t\b\f\u03bc"`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#12` `"inter\("pol" + "ation")"`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#13` `@text,@json,([1,.]|@csv,@tsv),@html,(@uri|.,@urid),@sh,(@base64|.,@base64d)`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#18` `@html "<b>\(.)</b>"`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#20` `{a: 1}`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#21` `{a,b,(.d):.a,e:.b}`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#22` `{"a",b,"a$\(1+1)"}`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#29` `.e0, .E1, .E-1, .E+1`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#34` `[.[]|.[1:3]?]`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#35` `map(try .a[] catch ., try .a.[] catch ., .a[]?, .a.[]?)`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#36` `try ["OK", (.[] | error)] catch ["KO", .]`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#37` `try (.foo[-1] = 0) catch .`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#38` `try (.foo[-2] = 0) catch .`, yq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#41` `try (.[999999999] = 0) catch .`, yq `failed`, aq `passed`
