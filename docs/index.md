---
layout: home

hero:
  name: aq
  text: Universal data query tool
  tagline: jq-style queries for JSON, JSONL, YAML, TOML, CSV, TSV, and human-readable tables.
  actions:
    - theme: brand
      text: Performance
      link: /performance
    - theme: alt
      text: jq Compatibility
      link: /jq-compatibility
    - theme: alt
      text: Starlark
      link: /starlark

features:
  - title: jq-style query language
    details: aq follows jq-style result streams and expression semantics rather than inventing a second query model.
  - title: More than JSON
    details: Read and write JSON, JSONL, YAML, TOML, CSV, and TSV, plus render output-only tables for inspection.
  - title: Scriptable when needed
    details: Optional Starlark integration adds structured file I/O, transforms, automation helpers, and a persistent REPL without changing the core CLI model.
  - title: Performance-focused
    details: Current jq-master benchmarking puts aq in the same performance tier, with the exact numbers published in the generated benchmark docs.
---

## What `aq` is

`aq` is a jq-style command-line tool for querying and transforming structured data across multiple file formats. The CLI is the stable v1 product surface.

## Quick examples

```bash
aq '.users[] | select(.age >= 30) | .name' users.json
aq -f yaml -o json '.services[].port' compose.yaml
aq -o table '.items[] | {name, status, owner}' data.json
aq --stream 'select(.status >= 500)' logs.jsonl
aq --in-place '.version = "2.0"' config.toml
```

## Where to go next

- [Performance](/performance)
- [jq Compatibility](/jq-compatibility)
- [Starlark](/starlark)
