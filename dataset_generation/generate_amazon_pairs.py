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

OUTPUT_FILE = Path("amazon_pairs.jsonl")
ENV_FILE = Path("amazon_environment.yaml")

CATEGORIES = [
    "filtering",
    "aggregation",
    "sentiment_analysis",
    "data_quality",
    "spark_to_parquet",
    "spark_to_cassandra"
]

DIFFICULTIES = ["easy", "medium", "hard"]

SEED_IDEAS = {
    "filtering": [
        "Filter Amazon reviews by specific product_id",
        "Filter reviews with high number of helpful_votes",
        "Filter reviews that are verified purchases only",
        "Filter reviews by 5-star ratings or 1-star ratings"
    ],
    "aggregation": [
        "Count reviews per product_category",
        "Calculate the average star_rating per product",
        "Find the total number of helpful_votes per category",
        "Find the top 5 most reviewed products based on review volume"
    ],
    "sentiment_analysis": [
        "Identify review text containing positive keywords like great or love using lower functions",
        "Flag review text containing negative keywords like broken or disappointed",
        "Calculate average star rating for reviews containing specific words",
        "Filter reviews where review_headline indicates high satisfaction"
    ],
    "data_quality": [
        "Remove rows with missing review_body text",
        "Filter out invalid review_date format structures using cast or to_date",
        "Drop records with zero total_votes",
        "Clean and standardise product_title strings"
    ],
    "spark_to_parquet": [
        "Read Amazon reviews sample and write filtered output to Parquet",
        "Aggregate review metrics and save results as Parquet partitioned by category",
        "Generate product-level summaries and save to local Parquet storage"
    ],
    "spark_to_cassandra": [
        "Write category review counts to Cassandra storage environment",
        "Write high helpful vote summaries directly to Cassandra targets",
        "Write verified purchase records straight to Cassandra clusters"
    ]
}


def load_environment_text() -> str:
    if not ENV_FILE.exists():
        default_env = {
            "environment": {
                "engine": "spark",
                "input_path": "data/amazon/amazon_reviews_sample.csv",
                "available_columns": [
                    "marketplace", "customer_id", "review_id", "product_id", 
                    "product_parent", "product_title", "product_category", 
                    "star_rating", "helpful_votes", "total_votes", "vine", 
                    "verified_purchase", "review_headline", "review_body", "review_date"
                ]
            }
        }
        with open(ENV_FILE, "w", encoding="utf-8") as f:
            yaml.dump(default_env, f, sort_keys=False)

    with open(ENV_FILE, "r", encoding="utf-8") as f:
        env = yaml.safe_load(f)

    return yaml.dump(env, sort_keys=False)


def build_teacher_prompt(pair_id: int, category: str, difficulty: str, environment_text: str) -> str:
    seed_task = random.choice(SEED_IDEAS[category])

    return f"""
You are generating a synthetic training example for an NL2Pipeline system.

The goal is to create one natural-language prompt and one corresponding executable PySpark code answer.

The project uses the following environment metadata:

{environment_text}

Generation constraints:
- Dataset must be Amazon reviews only.
- Use only columns listed in the environment metadata.
- Use only paths listed in the environment metadata.
- Generate Python PySpark code.
- Assume the SparkSession is available as `spark` and the DataFrame is already loaded as a variable named `df`.
- Use PySpark functions (from pyspark.sql.functions) elegantly where needed.
- The assistant code should be complete and self-contained.
- Do not include markdown in the assistant code.
- Do not invent unavailable datasets.
- Do not invent unavailable columns.
- Do not include explanations inside the assistant field.
- Return valid JSON only.

Pair details:
- id: amazon_{pair_id:06d}
- category: {category}
- difficulty: {difficulty}
- seed task: {seed_task}

Return JSON in exactly this structure:

{{
  "id": "amazon_{pair_id:06d}",
  "category": "{category}",
  "difficulty": "{difficulty}",
  "source_dataset": "amazon",
  "user": "natural language pipeline request here",
  "assistant": "complete PySpark code here",
  "metadata": {{
    "input_path": "data/amazon/amazon_reviews_sample.csv",
    "processing_engine": "spark",
    "output_type": "parquet, csv, or cassandra",
    "main_operation": "{category}"
  }}
}}
"""


def clean_json_response(text: str) -> dict:
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
        instructions="You are a strict JSON generator for synthetic NL2Pipeline training data.",
        input=prompt,
        temperature=0.7
    )

    pair = clean_json_response(response.output_text)

    return pair


def append_jsonl(record: dict, path: Path) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    target_pairs = 200

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
