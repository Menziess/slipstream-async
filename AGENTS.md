# AGENTS.md

## Verification

```bash
pyright slipstream tests
ruff check && ruff format --check
pytest slipstream tests
```

## Code style

- Single quotes for strings. Double quotes for docstrings only.
- Google-style docstrings. 79-char line limit for Python. Not for `.md` / `.rst`.
- Assign exception messages to a variable before `raise` (TRY003).
- When wrapping a long string in parentheses, do **not** add a
  trailing comma. That makes a 1-tuple and fails Ruff ISC004.
