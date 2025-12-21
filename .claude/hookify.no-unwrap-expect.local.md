---
name: no-unwrap-expect
enabled: true
event: file
conditions:
  - field: file_path
    operator: regex_match
    pattern: \.rs$
  - field: new_text
    operator: regex_match
    pattern: \.(unwrap|expect)\s*\(
action: warn
---

**Avoid unwrap/expect in production code**

Your coding standards specify: "Avoid `unwrap/expect` in prod; propagate errors with context."

**Instead of:**
- `.unwrap()` → use `?` operator or `.ok_or_else(|| ...)?`
- `.expect("msg")` → use `.context("msg")?` (with anyhow) or map to custom error

**Better patterns:**
```rust
// Instead of: let value = result.unwrap();
let value = result?;

// Instead of: let value = option.expect("should exist");
let value = option.ok_or_else(|| anyhow!("should exist"))?;

// With context:
let value = result.context("failed to parse config")?;
```

**Exceptions:**
- Test code (files ending in `_test.rs` or in `tests/` directory)
- Build scripts (`build.rs`)
- Examples (`examples/` directory)

Consider if this is production code that needs proper error handling.
