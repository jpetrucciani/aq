Drop representative source documents in this tree when you want them turned into regression tests.

Suggested workflow:

1. Put raw or lightly sanitized examples in `tests/sources/inbox/`.
2. Add a short note next to them describing:
   - the command you run today with `jq` or `yq`
   - the expected stdout or in-place rewrite
   - any details that matter, such as multi-doc YAML, ordering, comments, or fields that must not change
3. I will turn those into minimized committed fixtures under `tests/fixtures/` and add explicit tests in `tests/cli.rs`.

This directory is not loaded automatically by the test suite. It is a staging area for real-world examples.
