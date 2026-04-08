## jq vs aq Upstream Benchmark

- Sources: `https://raw.githubusercontent.com/jqlang/jq/master/tests/base64.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/manonig.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/onig.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/optional.test, https://raw.githubusercontent.com/jqlang/jq/master/tests/uri.test`
- Harvested direct success cases: `836`
- Warmup runs: `1`
- Measured runs: `3`
- Per-run timeout: `10.0s`
- jq binary: `tmp/jq-master/build/install-pure/bin/jq`
- jq version: `jq-master-69785bf-dirty`
- aq binary: `target/release/aq`
- aq version: `aq 0.1.0`
- Benchmark wall time: `13.4s`
- Compared cases: `833`
- aq faster cases: `411`
- jq faster cases: `185`
- Roughly equal cases: `237`
- Uncomparable cases: `3`
- Sum of jq medians: `1.343s`
- Sum of aq medians: `1.261s`
- Median aq/jq ratio: `0.95x`
- Geometric mean aq/jq ratio: `0.96x`

Interpretation: aq/jq ratios above `1.00x` mean aq is slower, below `1.00x` mean aq is faster.

### Biggest aq Slowdowns

- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#104` `.[] as [$a, $b] | [$b, $a]`, jq `1.46ms`, aq `2.23ms`, aq/jq `1.52x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/manonig.test#8` `match("foo (?<bar123>bar)? foo"; "ig")`, jq `1.38ms`, aq `1.93ms`, aq/jq `1.40x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#468` `try input catch .`, jq `1.57ms`, aq `2.16ms`, aq/jq `1.38x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#124` `1 + 2 * 2 + 10 / 2`, jq `1.72ms`, aq `2.36ms`, aq/jq `1.37x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#141` `inside({"foo": 12, "bar":[1,2,{"barp":12, "blip":13}]})`, jq `1.47ms`, aq `1.97ms`, aq/jq `1.34x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#41` `try (.[999999999] = 0) catch .`, jq `1.60ms`, aq `2.10ms`, aq/jq `1.32x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#55` `[range(0;10;3)]`, jq `1.50ms`, aq `1.98ms`, aq/jq `1.32x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#215` `reduce .[] as $item (0; . + $item)`, jq `1.36ms`, aq `1.80ms`, aq/jq `1.32x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/onig.test#18` `sub("^(?<head>.)"; "Head=\(.head) Tail=")`, jq `1.56ms`, aq `2.03ms`, aq/jq `1.30x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#441` `.x | tojson | . == if have_decnum then "13911860366432393" else "13911860366432392" end`, jq `1.44ms`, aq `1.87ms`, aq/jq `1.30x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#29` `.[] | .name`, jq `1.51ms`, aq `1.95ms`, aq/jq `1.29x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#336` `[({foo: 12, bar:13} | contains({foo: 12})), ({foo: 12} | contains({})), ({foo: 12, bar:13} | contains({baz:14}))]`, jq `1.51ms`, aq `1.94ms`, aq/jq `1.29x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#208` `all(not)`, jq `1.50ms`, aq `1.93ms`, aq/jq `1.29x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#88` `join(",","/")`, jq `1.38ms`, aq `1.77ms`, aq/jq `1.28x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#226` `["foo",1] as $p | getpath($p), setpath($p; 20), delpaths([$p])`, jq `1.49ms`, aq `1.91ms`, aq/jq `1.28x`

### Biggest aq Speedups

- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#391` `last(range(365 * 67)|("1970-03-01T01:02:03Z"|strptime("%Y-%m-%dT%H:%M:%SZ")|mktime) + (86400 * .)|strftime("%Y-%m-%dT%H:%M:%SZ")|strptime("%Y-%m-%dT%H:%M:%SZ"))`, jq `27.34ms`, aq `2.21ms`, aq/jq `0.08x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#512` `reduce range(9999) as $_ ([];[.]) | tojson | fromjson | flatten`, jq `11.36ms`, aq `2.71ms`, aq/jq `0.24x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#513` `reduce range(10000) as $_ ([];[.]) | tojson | try (fromjson) catch . | (contains("<skipped: too deep>") | not) and contains("Exceeds depth limit for parsing")`, jq `5.21ms`, aq `2.86ms`, aq/jq `0.55x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#514` `reduce range(10001) as $_ ([];[.]) | tojson | contains("<skipped: too deep>")`, jq `4.37ms`, aq `2.60ms`, aq/jq `0.60x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#158` `recurse(.foo[])`, jq `2.14ms`, aq `1.39ms`, aq/jq `0.65x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#4` `1`, jq `2.06ms`, aq `1.38ms`, aq/jq `0.67x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#329` `. * 100000 | [.[:10],.[-10:]]`, jq `2.49ms`, aq `1.67ms`, aq/jq `0.67x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#5` `-1`, jq `1.93ms`, aq `1.31ms`, aq/jq `0.68x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#250` `.[2][3] = 1`, jq `1.94ms`, aq `1.34ms`, aq/jq `0.69x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/uri.test#8` `@urid`, jq `1.96ms`, aq `1.35ms`, aq/jq `0.69x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/onig.test#10` `"a","b","c" | capture("(?<x>a)?b?")`, jq `1.91ms`, aq `1.35ms`, aq/jq `0.71x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test#204` `.[] as [$a] ?// [$b] | if $a != null then error("err: \($a)") else {$a,$b} end`, jq `1.99ms`, aq `1.43ms`, aq/jq `0.72x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#103` `. as {as: $kw, "str": $str, ("e"+"x"+"p"): $exp} | [$kw, $str, $exp]`, jq `1.80ms`, aq `1.32ms`, aq/jq `0.74x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#202` `. as $dot|all($dot[];.)`, jq `1.67ms`, aq `1.23ms`, aq/jq `0.74x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#313` `map(trim), map(ltrim), map(rtrim)`, jq `1.89ms`, aq `1.40ms`, aq/jq `0.74x`

### Heaviest Cases

- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#391` `last(range(365 * 67)|("1970-03-01T01:02:03Z"|strptime("%Y-%m-%dT%H:%M:%SZ")|mktime) + (86400 * .)|strftime("%Y-%m-%dT%H:%M:%SZ")|strptime("%Y-%m-%dT%H:%M:%SZ"))`, jq `27.34ms`, aq `2.21ms`, aq/jq `0.08x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#512` `reduce range(9999) as $_ ([];[.]) | tojson | fromjson | flatten`, jq `11.36ms`, aq `2.71ms`, aq/jq `0.24x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#513` `reduce range(10000) as $_ ([];[.]) | tojson | try (fromjson) catch . | (contains("<skipped: too deep>") | not) and contains("Exceeds depth limit for parsing")`, jq `5.21ms`, aq `2.86ms`, aq/jq `0.55x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#514` `reduce range(10001) as $_ ([];[.]) | tojson | contains("<skipped: too deep>")`, jq `4.37ms`, aq `2.60ms`, aq/jq `0.60x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#123` `[10 * 20, 20 / .]`, jq `1.91ms`, aq `2.40ms`, aq/jq `1.26x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#329` `. * 100000 | [.[:10],.[-10:]]`, jq `2.49ms`, aq `1.67ms`, aq/jq `0.67x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#42` `.[]`, jq `2.01ms`, aq `2.10ms`, aq/jq `1.04x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#126` `1e-19 + 1e-20 - 5e-21`, jq `2.08ms`, aq `2.03ms`, aq/jq `0.98x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#124` `1 + 2 * 2 + 10 / 2`, jq `1.72ms`, aq `2.36ms`, aq/jq `1.37x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/onig.test#40` `sub("(?<x>.)"; "\(.x)!")`, jq `1.87ms`, aq `2.20ms`, aq/jq `1.18x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#125` `[16 / 4 / 2, 16 / 4 * 2, 16 - 4 - 2, 16 - 4 + 2]`, jq `1.96ms`, aq `2.11ms`, aq/jq `1.08x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#340` `(sort_by(.b) | sort_by(.a)), sort_by(.a, .b), sort_by(.b, .c), group_by(.b), group_by(.a + .b - .c == 2)`, jq `1.74ms`, aq `2.21ms`, aq/jq `1.27x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#393` `import "c" as foo; [foo::a, foo::c]`, jq `1.79ms`, aq `2.11ms`, aq/jq `1.18x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/onig.test#33` `[.[]|[[sub(", *";":")], [gsub(", *";":")], [scan(", *")]]]`, jq `1.91ms`, aq `1.97ms`, aq/jq `1.03x`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#77` `try skip(-1; error) catch .`, jq `1.85ms`, aq `2.03ms`, aq/jq `1.09x`

### Uncomparable Cases

- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#400` `modulemeta`, jq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#401` `modulemeta | .deps | length`, jq `failed`, aq `passed`
- `https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test#402` `modulemeta | .defs | length`, jq `failed`, aq `passed`
