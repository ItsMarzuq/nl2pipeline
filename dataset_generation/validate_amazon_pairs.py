import ast
import csv
import json
import shutil
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml
from tqdm import tqdm


SCRIPT_DIR = Path(__file__).resolve().parent

PAIRS_FILE = SCRIPT_DIR / "amazon_pairs.jsonl"
ENV_FILE = SCRIPT_DIR / "amazon_environment.yaml"
RUFF_CONFIG = SCRIPT_DIR / "ruff.toml"

OUTPUT_DIR = SCRIPT_DIR / "validation_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = OUTPUT_DIR / "amazon_validation_report.csv"
VALID_FILE = OUTPUT_DIR / "amazon_valid_pairs.jsonl"
FAILED_FILE = OUTPUT_DIR / "amazon_failed_pairs.jsonl"

REQUIRED_FIELDS = [
    "id",
    "category",
    "difficulty",
    "source_dataset",
    "user",
    "assistant",
]

VALID_CATEGORIES = {
    "filtering",
    "aggregation",
    "sentiment_analysis",
    "data_quality",
    "spark_to_parquet",
    "spark_to_cassandra",
}

VALID_DIFFICULTIES = {
    "easy",
    "medium",
    "hard",
}


def load_allowed_columns() -> List[str]:
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"Missing environment metadata file: {ENV_FILE}"
        )

    with open(ENV_FILE, "r", encoding="utf-8") as file:
        environment_data = yaml.safe_load(file) or {}

    if "environment" in environment_data:
        allowed_columns = (
            environment_data
            .get("environment", {})
            .get("available_columns", [])
        )

    elif "input_data" in environment_data:
        allowed_columns = (
            environment_data
            .get("input_data", {})
            .get("columns", [])
        )

    else:
        raise ValueError(
            "amazon_environment.yaml must contain either "
            "'environment' or 'input_data'."
        )

    if not isinstance(allowed_columns, list):
        raise ValueError(
            "The available columns value must be a list."
        )

    cleaned_columns = [
        column.strip()
        for column in allowed_columns
        if isinstance(column, str) and column.strip()
    ]

    if not cleaned_columns:
        raise ValueError(
            "No allowed columns were found in amazon_environment.yaml"
        )

    return cleaned_columns


def load_expected_input_path() -> str:
    with open(ENV_FILE, "r", encoding="utf-8") as file:
        environment_data = yaml.safe_load(file) or {}

    if "environment" in environment_data:
        return str(
            environment_data
            .get("environment", {})
            .get("input_path", "")
        ).strip()

    if "input_data" in environment_data:
        return str(
            environment_data
            .get("input_data", {})
            .get("path", "")
        ).strip()

    return ""


def load_jsonl(path: Path) -> List[Dict]:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing JSONL pairs file: {path}"
        )

    records = []

    with open(path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)

                if not isinstance(record, dict):
                    records.append(
                        {
                            "_line_number": line_number,
                            "_json_error": (
                                "Each JSONL line must contain a JSON object."
                            ),
                        }
                    )
                    continue

                record["_line_number"] = line_number
                records.append(record)

            except json.JSONDecodeError as error:
                records.append(
                    {
                        "_line_number": line_number,
                        "_json_error": str(error),
                    }
                )

    return records


def clean_code(code: str) -> str:
    if not isinstance(code, str):
        return ""

    code = code.strip()

    if code.startswith("```python"):
        code = code[len("```python"):].strip()

    elif code.startswith("```py"):
        code = code[len("```py"):].strip()

    elif code.startswith("```"):
        code = code[len("```"):].strip()

    if code.endswith("```"):
        code = code[:-3].strip()

    return code


def validate_json_schema(
    record: Dict,
    seen_ids: Set[str],
    seen_prompts: Set[str],
    expected_input_path: str,
) -> Tuple[bool, List[str]]:
    errors = []

    if "_json_error" in record:
        return False, [
            f"Invalid JSON: {record['_json_error']}"
        ]

    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"Missing field: {field}")

    if errors:
        return False, errors

    pair_id = record.get("id")
    category = record.get("category")
    difficulty = record.get("difficulty")
    source_dataset = record.get("source_dataset")
    user_prompt = record.get("user")
    assistant_code = record.get("assistant")

    if not isinstance(pair_id, str) or not pair_id.strip():
        errors.append("id must be a non-empty string")

    else:
        pair_id = pair_id.strip()

        if pair_id in seen_ids:
            errors.append("Duplicate id")
        else:
            seen_ids.add(pair_id)

        if not pair_id.startswith("amazon_"):
            errors.append("id must start with 'amazon_'")

    if not isinstance(category, str):
        errors.append("category must be a string")

    elif category not in VALID_CATEGORIES:
        errors.append(f"Unknown category: {category}")

    if not isinstance(difficulty, str):
        errors.append("difficulty must be a string")

    elif difficulty not in VALID_DIFFICULTIES:
        errors.append(f"Unknown difficulty: {difficulty}")

    if source_dataset != "amazon":
        errors.append("source_dataset must be 'amazon'")

    if not isinstance(user_prompt, str):
        errors.append("user must be a string")

    else:
        stripped_prompt = user_prompt.strip()

        if len(stripped_prompt) < 20:
            errors.append("User prompt is too short")

        normalized_prompt = " ".join(
            stripped_prompt.lower().split()
        )

        if normalized_prompt:
            if normalized_prompt in seen_prompts:
                errors.append("Duplicate user prompt")
            else:
                seen_prompts.add(normalized_prompt)

    if not isinstance(assistant_code, str):
        errors.append("assistant must be a string")

    elif len(assistant_code.strip()) < 30:
        errors.append("Assistant code is too short")

    metadata = record.get("metadata")

    if metadata is not None:
        if not isinstance(metadata, dict):
            errors.append("metadata must be a JSON object")

        else:
            metadata_path = metadata.get("input_path")

            if (
                expected_input_path
                and metadata_path is not None
                and metadata_path != expected_input_path
            ):
                errors.append(
                    "metadata.input_path does not match "
                    "amazon_environment.yaml"
                )

            processing_engine = metadata.get("processing_engine")

            if processing_engine not in {
                None,
                "spark",
                "apache_spark",
            }:
                errors.append(
                    "metadata.processing_engine must be 'spark'"
                )

            main_operation = metadata.get("main_operation")

            if (
                main_operation is not None
                and main_operation != category
            ):
                errors.append(
                    "metadata.main_operation does not match category"
                )

    return len(errors) == 0, errors


def validate_python_syntax(
    code: str,
) -> Tuple[bool, List[str]]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            ast.parse(code)

        return True, []

    except SyntaxError as error:
        location = ""

        if error.lineno is not None:
            location = f" at line {error.lineno}"

        return False, [
            f"Python syntax error{location}: {error.msg}"
        ]


def get_call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id

    if isinstance(node.func, ast.Attribute):
        return node.func.attr

    return ""


def extract_string_values(node: ast.AST) -> List[str]:
    values = []

    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            values.append(node.value)

    elif isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        for item in node.elts:
            values.extend(extract_string_values(item))

    elif isinstance(node, ast.Dict):
        for key in node.keys:
            if key is not None:
                values.extend(extract_string_values(key))

    return values


def extract_string_literals_from_call(
    node: ast.Call,
) -> List[str]:
    values = []

    for argument in node.args:
        values.extend(extract_string_values(argument))

    for keyword in node.keywords:
        if keyword.arg in {
            "subset",
            "cols",
            "columns",
        }:
            values.extend(
                extract_string_values(keyword.value)
            )

    return values


def extract_referenced_columns(
    code: str,
) -> Tuple[Set[str], Set[str]]:
    referenced_columns = set()
    defined_columns = set()

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(code)

    except SyntaxError:
        return referenced_columns, defined_columns

    column_functions = {
        "col",
        "column",
        "groupBy",
        "groupby",
        "select",
        "orderBy",
        "sort",
        "partitionBy",
        "drop",
        "dropDuplicates",
        "avg",
        "mean",
        "sum",
        "count",
        "countDistinct",
        "min",
        "max",
        "first",
        "last",
        "collect_set",
        "collect_list",
        "approx_count_distinct",
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            call_name = get_call_name(node)

            if call_name == "withColumn" and node.args:
                defined_columns.update(
                    extract_string_values(node.args[0])
                )

            elif call_name == "withColumnRenamed":
                if len(node.args) >= 2:
                    referenced_columns.update(
                        extract_string_values(node.args[0])
                    )

                    defined_columns.update(
                        extract_string_values(node.args[1])
                    )

            elif call_name == "alias" and node.args:
                defined_columns.update(
                    extract_string_values(node.args[0])
                )

            if call_name in column_functions:
                values = extract_string_literals_from_call(node)

                for value in values:
                    if value != "*":
                        referenced_columns.add(value)

            if call_name in {
                "dropna",
                "fillna",
                "replace",
                "fill",
            }:
                for argument in node.args:
                    if isinstance(argument, ast.Dict):
                        referenced_columns.update(
                            extract_string_values(argument)
                        )

                for keyword in node.keywords:
                    if keyword.arg == "subset":
                        referenced_columns.update(
                            extract_string_values(
                                keyword.value
                            )
                        )

        if isinstance(node, ast.Subscript):
            slice_node = node.slice

            if (
                isinstance(slice_node, ast.Constant)
                and isinstance(slice_node.value, str)
            ):
                referenced_columns.add(slice_node.value)

    return referenced_columns, defined_columns


def validate_amazon_columns(
    code: str,
    allowed_columns: List[str],
) -> Tuple[bool, List[str]]:
    errors = []

    referenced_columns, defined_columns = (
        extract_referenced_columns(code)
    )

    allowed_set = set(allowed_columns) | defined_columns

    invalid_columns = sorted(
        column
        for column in referenced_columns
        if column not in allowed_set
    )

    if invalid_columns:
        errors.append(
            "Invalid or invented Amazon columns used: "
            f"{invalid_columns}"
        )

    return len(errors) == 0, errors


def find_ruff_command() -> Optional[List[str]]:
    ruff_executable = shutil.which("ruff")

    if ruff_executable:
        return [ruff_executable]

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "ruff",
                "--version",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        if result.returncode == 0:
            return [
                sys.executable,
                "-m",
                "ruff",
            ]

    except (
        subprocess.SubprocessError,
        FileNotFoundError,
    ):
        pass

    return None


def run_ruff(
    code: str,
    ruff_command: List[str],
) -> Tuple[bool, List[str]]:
    with tempfile.TemporaryDirectory() as temporary_directory:
        script_path = (
            Path(temporary_directory) / "candidate.py"
        )

        script_path.write_text(
            code,
            encoding="utf-8",
        )

        command = [
            *ruff_command,
            "check",
            "--config",
            str(RUFF_CONFIG),
            "--output-format",
            "concise",
            str(script_path),
        ]

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

        except subprocess.TimeoutExpired:
            return False, [
                "Ruff timed out after 30 seconds"
            ]

        except FileNotFoundError as error:
            return False, [
                f"Ruff could not be started: {error}"
            ]

        if result.returncode != 0:
            output = (
                result.stdout.strip()
                or result.stderr.strip()
                or "Unknown Ruff error"
            )

            return False, [output]

    return True, []


def write_jsonl(
    path: Path,
    records: List[Dict],
) -> None:
    with open(path, "w", encoding="utf-8") as file:
        for record in records:
            clean_record = {
                key: value
                for key, value in record.items()
                if not key.startswith("_")
            }

            file.write(
                json.dumps(
                    clean_record,
                    ensure_ascii=False,
                )
                + "\n"
            )


def write_validation_report(
    report_rows: List[Dict],
) -> None:
    fieldnames = [
        "id",
        "line_number",
        "json_ok",
        "syntax_ok",
        "amazon_columns_ok",
        "ruff_ok",
        "passed",
        "errors",
    ]

    with open(
        REPORT_FILE,
        "w",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(report_rows)


def main() -> None:
    print("=" * 70)
    print("AMAZON JSONL DATASET VALIDATOR")
    print("=" * 70)

    if not RUFF_CONFIG.exists():
        raise FileNotFoundError(
            f"Missing Ruff configuration file: {RUFF_CONFIG}"
        )

    allowed_columns = load_allowed_columns()
    expected_input_path = load_expected_input_path()
    records = load_jsonl(PAIRS_FILE)
    ruff_command = find_ruff_command()

    if ruff_command is None:
        raise RuntimeError(
            "Ruff is required but could not be found in PATH."
        )

    print(f"\nPairs file: {PAIRS_FILE}")
    print(f"Environment file: {ENV_FILE}")
    print(f"Loaded pairs: {len(records)}")
    print(f"Allowed Amazon columns: {len(allowed_columns)}")
    print(f"Expected input path: {expected_input_path}")
    print(f"Ruff command: {' '.join(ruff_command)}")

    seen_ids: Set[str] = set()
    seen_prompts: Set[str] = set()

    report_rows = []
    valid_records = []
    failed_records = []

    for record in tqdm(
        records,
        desc="Validating pairs",
    ):
        pair_id = record.get(
            "id",
            f"line_{record.get('_line_number', 'unknown')}",
        )

        all_errors = []

        json_ok, json_errors = validate_json_schema(
            record=record,
            seen_ids=seen_ids,
            seen_prompts=seen_prompts,
            expected_input_path=expected_input_path,
        )

        all_errors.extend(json_errors)

        syntax_ok = False
        amazon_columns_ok = False
        ruff_ok = False

        if json_ok:
            code = clean_code(
                record.get("assistant", "")
            )

            record["assistant"] = code

            syntax_ok, syntax_errors = (
                validate_python_syntax(code)
            )

            all_errors.extend(syntax_errors)

            if syntax_ok:
                (
                    amazon_columns_ok,
                    amazon_errors,
                ) = validate_amazon_columns(
                    code=code,
                    allowed_columns=allowed_columns,
                )

                all_errors.extend(amazon_errors)

                ruff_ok, ruff_errors = run_ruff(
                    code=code,
                    ruff_command=ruff_command,
                )

                all_errors.extend(
                    f"Ruff: {error}"
                    for error in ruff_errors
                )

        passed = (
            json_ok
            and syntax_ok
            and amazon_columns_ok
            and ruff_ok
        )

        report_rows.append(
            {
                "id": pair_id,
                "line_number": record.get(
                    "_line_number",
                    "",
                ),
                "json_ok": json_ok,
                "syntax_ok": syntax_ok,
                "amazon_columns_ok": amazon_columns_ok,
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

    write_validation_report(report_rows)
    write_jsonl(VALID_FILE, valid_records)
    write_jsonl(FAILED_FILE, failed_records)

    print("\n" + "=" * 70)
    print("VALIDATION COMPLETE")
    print("=" * 70)

    print(f"Total pairs:  {len(records)}")
    print(f"Valid pairs:  {len(valid_records)}")
    print(f"Failed pairs: {len(failed_records)}")

    if records:
        pass_rate = (
            len(valid_records) / len(records)
        ) * 100

        print(f"Pass rate:    {pass_rate:.2f}%")

    print(f"\nReport:       {REPORT_FILE}")
    print(f"Valid JSONL:  {VALID_FILE}")
    print(f"Failed JSONL: {FAILED_FILE}")

    if len(valid_records) >= 250:
        print(
            "\nTarget reached: at least 250 valid "
            "Amazon pairs are available."
        )

    else:
        remaining = 250 - len(valid_records)

        print(
            f"\nTarget not reached. You need at least "
            f"{remaining} more valid Amazon pairs."
        )


if __name__ == "__main__":
    main()