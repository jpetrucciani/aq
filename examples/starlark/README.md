# Starlark Examples

Run these from the repository root. File-based examples resolve relative paths from the script location, so the commands below work as written.

## Worked Runs

Clean Kubernetes metadata copied from `kubectl get -o yaml`:

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

Summarize config maps from the bundled manifest fixtures:

```bash
aq -n --starlark-file examples/starlark/config_map_names.star --starlark-filesystem \
  --output-format json --compact examples/starlark/data/manifests/app.yaml \
  examples/starlark/data/manifests/nested/bundle.yaml
```

```json
["app-config","extra-config"]
```

Sanitize contacts with regexes and digests:

```bash
printf '%s\n' '{"users":[{"name":"Alice","email":"alice@example.com"},{"name":"Bob","email":"bob@example.com"}]}' \
  | aq --starlark-file examples/starlark/sanitize_contacts.star --output-format json --compact
```

```json
[{"name":"Alice","email":"***@example.com","token":"YWxpY2VAZXhhbXBsZS5jb20=","fingerprint":"b0592e381d5b6b5b8e14a53e089e693779525183161310286e48597d54a062b9"},{"name":"Bob","email":"***@example.com","token":"Ym9iQGV4YW1wbGUuY29t","fingerprint":"520593f928475d27316cfd9cebad542a9f82cd891e5409669807a6bde2e4660a"}]
```

Generate a TOML application config from a script with no input:

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

## Catalog

- `calendar_rollup.star`, build a small typed-date schedule summary
- `clean_k8s_metadata.star`, keep portable Kubernetes manifest metadata while dropping live-object fields
- `config_map_names.star`, load a local helper module and list ConfigMap names
- `embedded_config.star`, parse embedded YAML text and re-render it
- `emit_manifest_summaries.star`, write one JSON summary per bundled manifest
- `emit_note_copies.star`, batch-copy and annotate note text files
- `find_secretish_fields.star`, locate and describe suspicious secret-like fields
- `fingerprint_library.star`, hash bundled Starlark library files
- `generate_app_toml.star`, emit a complete TOML config without input
- `library_index.star`, inspect bundled library files and relative path helpers
- `manifest_summary.star`, summarize manifest bundles with `aq.query_all(...)`
- `manifest_tree_summary.star`, read bundled manifest trees with filesystem helpers
- `merge_overlay.star`, deep-merge two values and drop nulls
- `normalize_note_files.star`, rewrite bundled note files in place
- `normalize_strings.star`, trim and patch nested strings with `aq.walk_paths(...)`
- `redact_secretish_fields.star`, omit secret-like values by path predicate
- `sanitize_contacts.star`, mask emails and compute fingerprints
- `self_inventory.star`, inspect bundled library files from the filesystem
- `shift_release_window.star`, do typed datetime arithmetic
- `stage_report.star`, write a generated JSON report to disk
- `time_snapshot.star`, show typed `now`, `today`, and `timedelta` helpers
- `users_over_30.star`, filter a JSON document with `aq.query_all(...)`
