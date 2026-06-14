# NL2Pipeline Dataset Generation

This folder contains the GDELT dataset generation workflow for creating natural-language prompt and PySpark code pairs.

## Files

* `download_gdelt_real_data.py` — Downloads and cleans real GDELT event data.
* `gdelt_environment.yaml` — Defines the GDELT dataset path, columns, and allowed outputs.
* `generate_gdelt_pairs.py` — Generates prompt-code pairs using GPT-4o.
* `validate_gdelt_pairs.py` — Validates generated pairs for JSON format, Python syntax, GDELT column usage, and Ruff linting.
* `gdelt_pairs.jsonl` — Stores all generated prompt-code pairs.
* `validation_output/gdelt_valid_pairs.jsonl` — Stores pairs that passed validation.
* `validation_output/gdelt_failed_pairs.jsonl` — Stores pairs that failed validation.
* `validation_output/validation_report.csv` — Contains the validation summary for each pair.

## Setup

Create and activate a virtual environment:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install pandas tqdm ruff python-dotenv openai pyyaml requests
```

Create a `.env` file and add your OpenAI API key:

```env
OPENAI_API_KEY=your_api_key_here
```

## Run Steps

Download real GDELT data:

```bash
python dataset_generation/download_gdelt_real_data.py
```

Generate prompt-code pairs:

```bash
python dataset_generation/generate_gdelt_pairs.py
```

Validate generated pairs:

```bash
python dataset_generation/validate_gdelt_pairs.py
```

The valid dataset will be saved at:

```text
dataset_generation/validation_output/gdelt_valid_pairs.jsonl
```


