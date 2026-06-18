from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

SYSTEM_PROMPT = (
    "You are an expert PySpark developer for the NL2Pipeline project. "
    "Given a Big Data environment description and a natural-language pipeline "
    "request, generate complete, self-contained PySpark code that fulfils the "
    "request exactly. "
    "Wrap your answer in a single ```python ... ``` code block. "
    "Do not include any explanation, commentary, or text outside that block."
)


def load_env_yaml(path: str | Path) -> str:
    """
    Load *path*, strip any surrounding markdown code fences, parse with
    yaml.safe_load, then re-serialise with yaml.dump for stable normalised output.
    """
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()

    raw = raw.strip()
    if raw.startswith("```yaml"):
        raw = raw[len("```yaml"):].lstrip("\n")
    elif raw.startswith("```"):
        raw = raw[3:].lstrip("\n")
    if raw.endswith("```"):
        raw = raw[:-3].rstrip()

    data: Any = yaml.safe_load(raw)
    return yaml.dump(data, sort_keys=False)


def build_messages(
    env_yaml_text: str,
    user_request: str,
    correction_history: list[dict] | None = None,
) -> list[dict]:
    """
    Build the messages list for tokenizer.apply_chat_template.

    First call:  [system, user]
    Retries:     [system, user, assistant(attempt-N), user(error+retry), ...]
    """
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"{env_yaml_text}\n\n{user_request}"},
    ]
    if correction_history:
        messages.extend(correction_history)
    return messages


def apply_template(tokenizer: Any, messages: list[dict]) -> str:
    """Apply the Llama-3 chat template and return the formatted prompt string."""
    return tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )
