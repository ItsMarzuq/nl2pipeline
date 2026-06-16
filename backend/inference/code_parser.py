from __future__ import annotations

import re

_CODE_FENCE_RE = re.compile(r"```python\s*\n(.*?)```", re.DOTALL)


class ParseError(ValueError):
    """Raised when model output contains no ```python fence."""


def extract_python(text: str) -> str:
    """
    Return the body of the first ```python ... ``` fence in *text*.

    Raises ParseError if no fence is found so the correction loop can feed
    the failure back to the model rather than silently passing prose.
    """
    match = _CODE_FENCE_RE.search(text)
    if match:
        return match.group(1).rstrip()
    raise ParseError(
        "Model output did not contain a ```python code block.\n"
        f"Raw output (first 400 chars):\n{text[:400]}"
    )
