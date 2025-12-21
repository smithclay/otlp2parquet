---
name: no-println
enabled: true
event: file
conditions:
  - field: file_path
    operator: regex_match
    pattern: \.rs$
  - field: new_text
    operator: regex_match
    pattern: (println!|eprintln!)\s*\(
action: warn
---

**Use tracing macros instead of println!/eprintln!**

Your coding standards specify: "Use `tracing::*` macros only; no `println!/eprintln!` in production paths."

**Instead of:**
- `println!("message")` → use `tracing::info!("message")`
- `eprintln!("error")` → use `tracing::error!("error")`

**Available tracing macros:**
- `tracing::trace!()` - Trace-level logging
- `tracing::debug!()` - Debug-level logging
- `tracing::info!()` - Info-level logging
- `tracing::warn!()` - Warning-level logging
- `tracing::error!()` - Error-level logging

This ensures proper structured logging throughout the application.
