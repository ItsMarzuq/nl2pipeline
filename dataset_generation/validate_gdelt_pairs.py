import ast
import csv
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd
from tqdm import tqdm


PAIRS_FILE = Path("gdelt_pairs.jsonl")
GDELT_CSV = Path("data/gdelt/gdelt_events_sample.csv")

OUTPUT_DIR = Path("validation_output")
OUTPUT_DIR.mkdir(exist_ok=True)

REPORT_FILE = OUTPUT_DIR / "validation_report.csv"
VALID_FILE = OUTPUT_DIR / "gdelt_valid_pairs.jsonl"
FAILED_FILE = OUTPUT_DIR / "gdelt_failed_pairs.jsonl"

REQUIRED_FIELDS = [
    "id",
    "category",
    "difficulty",
    "source_dataset",
    "user",
    "assistant",
]

EXPECTED_GDELT_PATH = "data/gdelt/gdelt_events_sample.csv"


def load_allowed_columns() -> List[str]:
    if not GDELT_CSV.exists():
        raise FileNotFoundError(f"Missing GDELT CSV file: {GDELT_CSV}")

    df = pd.read_csv(GDELT_CSV, nrows=1)
    return list(df.columns)


def load_jsonl(path: Path) -> List[Dict]:
    records = []

    with open(path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)
                record["_line_number"] = line_number
                records.append(record)

            except json.JSONDecodeError as e:
                records.append(
                    {
                        "_line_number": line_number,
                        "_json_error": str(e),
                    }
                )

    return records


def clean_code(code: str) -> str:
    code = code.strip()

    if code.startswith("```python"):
        code = code.replace("```python", "", 1).strip()

    if code.startswith("```"):
        code = code.replace("```", "", 1).strip()

    if code.endswith("```"):
        code = code[:-3].strip()

    return code


def validate_json_schema(
    record: Dict,
    seen_ids: Set[str],
    seen_prompts: Set[str],
) -> Tuple[bool, List[str]]:
    errors = []

    if "_json_error" in record:
        return False, [f"Invalid JSON: {record['_json_error']}"]

    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"Missing field: {field}")

    if errors:
        return False, errors

    if record["id"] in seen_ids:
        errors.append("Duplicate id")

    seen_ids.add(record["id"])

    normalized_prompt = " ".join(record["user"].lower().split())

    if normalized_prompt in seen_prompts:
        errors.append("Duplicate user prompt")

    seen_prompts.add(normalized_prompt)

    if record.get("source_dataset") != "gdelt":
        errors.append("source_dataset must be 'gdelt'")

    if not isinstance(record.get("user"), str) or len(record["user"].strip()) < 20:
        errors.append("User prompt is too short")

    if not isinstance(record.get("assistant"), str) or len(record["assistant"].strip()) < 50:
        errors.append("Assistant code is too short")

    return len(errors) == 0, errors


def validate_python_syntax(code: str) -> Tuple[bool, List[str]]:
    try:
        ast.parse(code)
        return True, []
    except SyntaxError as e:
        return False, [f"Python syntax error: {e}"]


def extract_string_literals_from_call(node: ast.Call) -> List[str]:
    values = []

    for arg in node.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            values.append(arg.value)

        elif isinstance(arg, ast.List):
            for item in arg.elts:
                if isinstance(item, ast.Constant) and isinstance(item.value, str):
                    values.append(item.value)

    for keyword in node.keywords:
        if keyword.arg == "subset":
            if isinstance(keyword.value, ast.List):
                for item in keyword.value.elts:
                    if isinstance(item, ast.Constant) and isinstance(item.value, str):
                        values.append(item.value)

    return values


def get_call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id

    if isinstance(node.func, ast.Attribute):
        return node.func.attr

    return ""


def extract_referenced_columns(code: str) -> Set[str]:
    """
    Extracts common PySpark column references.

    It checks examples like:
    col("avg_tone")
    df["avg_tone"]
    groupBy("action_country")
    select("event_date", "avg_tone")
    avg("avg_tone")
    count("event_code")
    dropna(subset=["actor1_country"])
    """

    referenced_columns = set()

    try:
        tree = ast.parse(code)
    except SyntaxError:
        return referenced_columns

    column_functions = {
        "col",
        "groupBy",
        "select",
        "orderBy",
        "sort",
        "partitionBy",
        "drop",
        "dropDuplicates",
        "avg",
        "sum",
        "count",
        "min",
        "max",
        "first",
        "last",
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            call_name = get_call_name(node)

            if call_name in column_functions:
                values = extract_string_literals_from_call(node)

                for value in values:
                    if value != "*":
                        referenced_columns.add(value)

            if call_name in {"dropna", "fillna"}:
                values = extract_string_literals_from_call(node)

                for value in values:
                    referenced_columns.add(value)

        if isinstance(node, ast.Subscript):
            if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                referenced_columns.add(node.slice.value)

    return referenced_columns


def validate_gdelt_columns(code: str, allowed_columns: List[str]) -> Tuple[bool, List[str]]:
    errors = []

    if EXPECTED_GDELT_PATH not in code:
        errors.append(f"Code does not use expected GDELT path: {EXPECTED_GDELT_PATH}")

    referenced_columns = extract_referenced_columns(code)
    allowed_set = set(allowed_columns)

    invalid_columns = sorted(
        column for column in referenced_columns if column not in allowed_set
    )

    if invalid_columns:
        errors.append(f"Invalid or invented GDELT columns used: {invalid_columns}")

    return len(errors) == 0, errors


def run_ruff(code: str) -> Tuple[bool, List[str]]:
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "candidate.py"
        script_path.write_text(code, encoding="utf-8")

        try:
            result = subprocess.run(
                [sys.executable, "-m", "ruff", "check", str(script_path)],
                capture_output=True,
                text=True,
                timeout=30,
            )

        except subprocess.TimeoutExpired:
            return False, ["Ruff timed out"]

        if result.returncode != 0:
            output = result.stdout.strip() or result.stderr.strip()
            return False, [output]

    return True, []


def write_jsonl(path: Path, records: List[Dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            clean_record = {
                k: v for k, v in record.items() if not k.startswith("_")
            }
            f.write(json.dumps(clean_record, ensure_ascii=False) + "\n")


def main():
    allowed_columns = load_allowed_columns()
    records = load_jsonl(PAIRS_FILE)

    print(f"Loaded {len(records)} prompt-code pairs")
    print(f"Detected GDELT columns: {allowed_columns}")

    seen_ids = set()
    seen_prompts = set()

    report_rows = []
    valid_records = []
    failed_records = []

    for record in tqdm(records, desc="Validating pairs"):
        pair_id = record.get("id", f"line_{record.get('_line_number', 'unknown')}")
        all_errors = []

        json_ok, json_errors = validate_json_schema(
            record,
            seen_ids,
            seen_prompts,
        )
        all_errors.extend(json_errors)

        syntax_ok = False
        gdelt_columns_ok = False
        ruff_ok = False

        if json_ok:
            code = clean_code(record["assistant"])
            record["assistant"] = code

            syntax_ok, syntax_errors = validate_python_syntax(code)
            all_errors.extend(syntax_errors)

            if syntax_ok:
                gdelt_columns_ok, gdelt_errors = validate_gdelt_columns(
                    code,
                    allowed_columns,
                )
                all_errors.extend(gdelt_errors)

                ruff_ok, ruff_errors = run_ruff(code)
                all_errors.extend([f"Ruff: {error}" for error in ruff_errors])

        passed = json_ok and syntax_ok and gdelt_columns_ok and ruff_ok

        report_rows.append(
            {
                "id": pair_id,
                "line_number": record.get("_line_number", ""),
                "json_ok": json_ok,
                "syntax_ok": syntax_ok,
                "gdelt_columns_ok": gdelt_columns_ok,
                "ruff_ok": ruff_ok,
                "passed": passed,
                "errors": " | ".join(all_errors),
            }
        )

        if passed:
            valid_records.append(record)
        else:
            failed_record = dict(record)
            failed_record["validation_errors"] = all_errors
            failed_records.append(failed_record)

    with open(REPORT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "line_number",
                "json_ok",
                "syntax_ok",
                "gdelt_columns_ok",
                "ruff_ok",
                "passed",
                "errors",
            ],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    write_jsonl(VALID_FILE, valid_records)
    write_jsonl(FAILED_FILE, failed_records)

    print("\nValidation complete")
    print(f"Total pairs: {len(records)}")
    print(f"Valid pairs: {len(valid_records)}")
    print(f"Failed pairs: {len(failed_records)}")
    print(f"Report saved to: {REPORT_FILE}")
    print(f"Valid pairs saved to: {VALID_FILE}")
    print(f"Failed pairs saved to: {FAILED_FILE}")


if __name__ == "__main__":
    main()