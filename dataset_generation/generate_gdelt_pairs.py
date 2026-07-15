import os
import json
import time
import random
from pathlib import Path

import yaml
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI


load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

OUTPUT_FILE = Path("gdelt_pairs.jsonl")
ENV_FILE = Path("gdelt_environment.yaml")


CATEGORIES = [
    "filtering",
    "aggregation",
    "trend_analysis",
    "data_quality",
    "spark_to_parquet",
    "spark_to_cassandra"
]

DIFFICULTIES = ["easy", "medium", "hard"]


SEED_IDEAS = {
    "filtering": [
        "Filter for sporting events, athletic competitions, or Olympic coverage",
        "Extract reports concerning terrorist activity, rebel conflicts, or militia insurgencies",
        "Isolate media and entertainment coverage, movie releases, film festivals, or celebrity news",
        "Find news involving medical outbreaks, public health campaigns, or hospital updates",
        "Filter for corporate announcements, business mergers, or financial market movements",
        "Extract reports involving religious leaders, local clergy, or faith-based movements",
        "Isolate academic research, university studies, or scientific discoveries"
    ],
    "aggregation": [
        "Find which societal sectors (Sports, Business, Medical) get the most media attention",
        "Calculate the total volume of violent versus non-violent rebel events",
        "Compare average sentiment tone between medical coverage and commercial corporate news",
        "Count how many times entertainers or media personnel are mentioned by region",
        "Determine which industry sectors have the highest total count of recorded events"
    ],
    "trend_analysis": [
        "Track the daily frequency of sports-related coverage over the course of the year",
        "Monitor sudden spikes in reports concerning rebel activity or insurgent attacks",
        "Analyze whether coverage of movie, theater, and entertainment events rises on weekends",
        "Track weekly public sentiment shifts in articles covering scientific or medical updates",
        "Identify anomalous spikes in financial and corporate business news"
    ],
    "data_quality": [
        "Cleanse the dataset by removing empty or null entries in the Actor Type classifications",
        "Drop records where the actor identifier represents an invalid or corrupt category",
        "Clean and standardize the Actor Type roles to uniform uppercase values",
        "Filter out events where the recorded date falls outside the valid tracking parameters"
    ],
    "spark_to_parquet": [
        "Save our processed athletic and sports event database as a clean Parquet table",
        "Export compiled reports on scientific innovations and educational programs to Parquet",
        "Archive filtered rebel and conflict records to partitioned security directories in Parquet"
    ],
    "spark_to_cassandra": [
        "Export the aggregated business sentiment statistics directly to Cassandra",
        "Store the daily volume of global entertainment and media updates into Cassandra",
        "Load daily calculated health and medical alerts into our Cassandra pipeline"
    ]
}


def load_environment_text() -> str:
    with open(ENV_FILE, "r", encoding="utf-8") as f:
        env = yaml.safe_load(f)

    return yaml.dump(env, sort_keys=False)


def build_teacher_prompt(pair_id: int, category: str, difficulty: str, environment_text: str) -> str:
    seed_task = random.choice(SEED_IDEAS[category])

    return f"""
You are generating a highly diverse synthetic training pair (User Request -> PySpark Code) for an NL2Pipeline platform targeting GDELT.

The project environment schema details are as follows:
{environment_text}

---

### 🚨 CONCEPTUAL ARCHITECTURAL RULE (THE TRANSLATION LAYER)
You must translate real-world human concepts spanning sports, entertainment, business, science, religion, and conflict into the correct database codes. 

1. THE "user" FIELD MUST ONLY CONTAIN HUMAN-CENTRIC LANGUAGE:
- Do not mention technical column names (e.g., 'Actor1Type1Code', 'event_code', 'avg_tone').
- Do not mention programming syntax or code.
- Instead of using technical jargon, the analyst will describe a normal social, corporate, or athletic domain (e.g., "Find news about major sporting games" or "Track terrorist threat spikes").

2. TRANSLATION DIRECTORY FOR THE "assistant" PYSPARK CODE:
Map the analyst's domain concepts to these specific GDELT CAMEO Actor Type codes:
- "Sports / Athletes / Teams / Olympic Games" -> Use `Actor1Type1Code == 'ATH'` (or Actor2Type1Code)
- "Terrorism / Rebels / Militias / Insurgencies" -> Use `Actor1Type1Code.isin('REB', 'INS')`
- "Movies / Entertainment / Celebrities / TV / Journalism / News" -> Use `Actor1Type1Code.isin('MED', 'ENT')`
- "Medicine / Health / Public Healthcare / Diseases / Doctors" -> Use `Actor1Type1Code == 'HLH'`
- "Business / Corporations / Markets / Companies / Executives" -> Use `Actor1Type1Code == 'BUS'`
- "Religions / Clergy / Churches / Spiritual Leaders" -> Use `Actor1Type1Code == 'REL'`
- "Education / Universities / Scientific Research / Academics" -> Use `Actor1Type1Code.isin('EDU', 'SCI')`

Always construct realistic filtering logic in PySpark using the exact column names present in the environment schema (e.g., if the schema uses 'actor1_type1' instead of 'Actor1Type1Code', use the schema's exact case).

---

### GENERATION CONSTRAINTS:
- Dataset must be GDELT only.
- Use only columns listed in the environment metadata.
- Use only paths listed in the environment metadata.
- Generate Python PySpark code.
- The assistant code should be complete and self-contained.
- Do not include markdown in the assistant code.
- Do not invent unavailable datasets.
- Do not invent unavailable columns.
- Do not include explanations inside the assistant field.
- Return valid JSON only.

Pair details:
- id: gdelt_{pair_id:06d}
- category: {category}
- difficulty: {difficulty}
- seed task: {seed_task}

Return JSON in exactly this structure:

{{
  "id": "gdelt_{pair_id:06d}",
  "category": "{category}",
  "difficulty": "{difficulty}",
  "source_dataset": "gdelt",
  "user": "A completely natural, realistic, domain-level query.",
  "assistant": "complete PySpark code here translating the user's human concepts into database codes",
  "metadata": {{
    "input_path": "data/gdelt/gdelt_events_sample.csv",
    "processing_engine": "spark",
    "output_type": "parquet, csv, or cassandra",
    "main_operation": "{category}"
  }}
}}
"""


def clean_json_response(text: str) -> dict:
    """
    Converts the model response into a Python dictionary.
    Handles cases where the model accidentally wraps JSON in markdown fences.
    """
    text = text.strip()

    if text.startswith("```json"):
        text = text.replace("```json", "", 1).strip()

    if text.startswith("```"):
        text = text.replace("```", "", 1).strip()

    if text.endswith("```"):
        text = text[:-3].strip()

    return json.loads(text)


def generate_pair(pair_id: int, environment_text: str) -> dict:
    category = random.choice(CATEGORIES)
    difficulty = random.choice(DIFFICULTIES)

    prompt = build_teacher_prompt(
        pair_id=pair_id,
        category=category,
        difficulty=difficulty,
        environment_text=environment_text
    )

    response = client.responses.create(
        model="gpt-4o",
        instructions="You are a strict JSON generator for synthetic NL2Pipeline training data. You never let human user queries leak raw database column names or numerical code details.",
        input=prompt,
        temperature=0.7
    )

    pair = clean_json_response(response.output_text)

    return pair


def append_jsonl(record: dict, path: Path) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    target_pairs = 200  # change this to 200, 500, etc.

    environment_text = load_environment_text()

    existing_count = 0
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            existing_count = sum(1 for _ in f)

    print(f"Existing pairs: {existing_count}")
    print(f"Generating until total pairs = {target_pairs}")

    for pair_id in tqdm(range(existing_count + 1, target_pairs + 1)):
        try:
            pair = generate_pair(pair_id, environment_text)
            append_jsonl(pair, OUTPUT_FILE)
            time.sleep(0.5)

        except Exception as e:
            print(f"Failed to generate pair {pair_id}: {e}")
            time.sleep(2)

    print(f"Done. Saved pairs to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()