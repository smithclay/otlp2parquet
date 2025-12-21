---
name: use-uv-python
enabled: true
event: bash
pattern: ^(python3?|pip3?)\s+
action: warn
---

**Use uv/uvx for Python instead of direct python/pip**

Your project guidelines specify: "always use the uv tool to run python or uvx to run python executable scripts/CLIs"

**Instead of:**
- `python script.py` → use `uv run script.py`
- `pip install package` → use `uv pip install package`
- `python -m module` → use `uv run python -m module`
- Running CLI tools → use `uvx tool-name`

This ensures consistent Python environment management across the project.
