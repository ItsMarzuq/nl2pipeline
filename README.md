# NL2Pipeline Dataset Generation

This repository contains the initial dataset generation workflow for the **NL2Pipeline** project. The goal is to generate natural-language-to-code training pairs where a user describes a big-data pipeline task and the assistant returns executable PySpark code.

For the interim stage, the dataset generation focuses on **real GDELT event data**. Other datasets such as Amazon Customer Reviews can be added later.

---

## Project Scope

The dataset generation module currently supports:

* Downloading and preparing real GDELT event data
* Creating a GDELT environment metadata file
* Generating natural-language prompt and PySpark code pairs using an LLM-as-a-Teacher approach
* Validating generated pairs using:

  * JSON format checks
  * Python syntax checks
  * Real GDELT column checks
  * Ruff linting
* Saving valid and failed pairs separately for later analysis

---

## Folder Structure

```text
dataset_generation/
│
├── download_gdelt_real_data.py
├── generate_gdelt_pairs.py
├── validate_gdelt_pairs.py
├── gdelt_environment.yaml
├── gdelt_pairs.jsonl
│
└── validation_output/
    ├── gdelt_valid_pairs.jsonl
    ├── gdelt_failed_pairs.jsonl
    └── validation_report.csv
```

---

## Files

### `download_gdelt_real_data.py`

Downloads real GDELT 2.0 Events data, extracts useful fields, cleans the dataset, and saves it locally as a CSV file.

Expected output:

```text
data/gdelt/gdelt_events_sample.csv
```

### `gdelt_environment.yaml`

Defines the available dataset metadata, including:

* Input dataset path
* Allowed GDELT columns
* Processing engine
* Allowed output locations

This file is injected into the generation prompt so that the LLM generates code grounded in the real dataset schema.

### `generate_gdelt_pairs.py`

Generates natural-language prompt and PySpark code pairs using GPT-4o as the teacher model.

Each generated pair is saved in JSONL format.

### `validate_gdelt_pairs.py`

Validates the generated pairs without executing Spark. It checks:

* JSON structure
* Required fields
* Duplicate IDs and prompts
* Python syntax
* Whether only real GDELT columns are used
* Ruff linting

### `validation_output/gdelt_valid_pairs.jsonl`

Contains only prompt-code pairs that passed validation.

### `validation_output/gdelt_failed_pairs.jsonl`

Contains failed pairs along with validation errors.

### `validation_output/validation_report.csv`

CSV report showing pass/fail status for every generated pair.

---

## Setup

Create and activate a virtual environment:

```bash
python -m venv .venv
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install pandas tqdm ruff python-dotenv openai pyyaml requests
```

---

## Environment Variables

Create a `.env` file in your local environment:

```env
OPENAI_API_KEY=your_api_key_here
```

Do not commit `.env` to GitHub.

---

## Step 1: Download Real GDELT Data

Run:

```bash
python dataset_generation/download_gdelt_real_data.py
```

This creates a cleaned real GDELT CSV at:

```text
data/gdelt/gdelt_events_sample.csv
```

The cleaned dataset contains columns such as:

```text
global_event_id
event_date
actor1_country
actor2_country
event_code
event_base_code
event_root_code
quad_class
goldstein_scale
num_mentions
num_sources
num_articles
avg_tone
action_country
action_location
action_lat
action_long
source_url
```

---

## Step 2: Generate Prompt-Code Pairs

Run:

```bash
python dataset_generation/generate_gdelt_pairs.py
```

This generates records in JSONL format.

Example record:

```json
{
  "id": "gdelt_000001",
  "category": "aggregation",
  "difficulty": "easy",
  "source_dataset": "gdelt",
  "user": "Create a Spark pipeline that reads real GDELT event data and counts events by action country.",
  "assistant": "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import count\n\nspark = SparkSession.builder.appName('gdelt_country_counts').getOrCreate()\n...",
  "metadata": {
    "input_path": "data/gdelt/gdelt_events_sample.csv",
    "processing_engine": "spark",
    "output_type": "parquet",
    "main_operation": "aggregation"
  }
}
```

---

## Step 3: Validate Generated Pairs

Run:

```bash
python dataset_generation/validate_gdelt_pairs.py
```

The validator checks the generated dataset and creates:

```text
validation_output/validation_report.csv
validation_output/gdelt_valid_pairs.jsonl
validation_output/gdelt_failed_pairs.jsonl
```

For the interim stage, validation does not execute the PySpark code. It only checks format, syntax, schema usage, and linting.

---

## Dataset Format

The generated dataset uses JSONL format, where each line is one training example.

Required fields:

```json
{
  "id": "unique pair id",
  "category": "pipeline category",
  "difficulty": "easy | medium | hard",
  "source_dataset": "gdelt",
  "user": "natural language pipeline request",
  "assistant": "PySpark code answer"
}
```

---

## Current Pipeline Categories

The current GDELT dataset generation supports tasks such as:

* Filtering GDELT events by country, event type, tone, or mentions
* Aggregating event counts by country, event code, or date
* Analysing tone and Goldstein score trends
* Cleaning missing or invalid rows
* Saving processed results as local CSV or Parquet

---

## Git Notes

Do not commit:

```text
.env
.venv/
.ruff_cache/
large raw data files
temporary outputs
```

Recommended workflow:

```bash
git checkout -b dataset-generation
git add .
git commit -m "Add GDELT dataset generation and validation workflow"
git push -u origin dataset-generation
```

Then open a Pull Request into `main`.

---

## Future Work

Planned improvements include:

* Adding PySpark execution validation
* Adding Docker-based validation
* Adding Cassandra or Iceberg output validation
* Adding GPT-based repair for failed generations
* Expanding the dataset beyond GDELT
* Adding Amazon Customer Reviews for streaming-style workloads
* Creating a held-out hand-authored evaluation set
* Converting validated pairs into chat fine-tuning format for a local SLM

---

## Team Usage

This module is intended to be one component of the larger NL2Pipeline project. Teammates should create separate branches for their own work, such as:

```text
frontend-ui
backend-api
model-training
pipeline-validation
dataset-generation
```

All changes should be merged through Pull Requests to reduce conflicts.
