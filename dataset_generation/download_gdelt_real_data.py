import zipfile
import requests
from pathlib import Path
from io import BytesIO

import pandas as pd
from tqdm import tqdm


OUTPUT_DIR = Path("data/gdelt")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "gdelt_events_sample.csv"

# GDELT 2.0 master file list
MASTER_FILE_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


GDELT_EVENT_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]


def get_latest_event_file_urls(limit: int = 4) -> list[str]:
    """
    Gets latest GDELT 2.0 event export files from the master file list.
    Each GDELT 2.0 update usually has three files:
    - export.CSV.zip
    - mentions.CSV.zip
    - gkg.csv.zip

    For this project, we only need export.CSV.zip event files.
    """
    response = requests.get(MASTER_FILE_LIST_URL, timeout=30)
    response.raise_for_status()

    lines = response.text.strip().splitlines()

    event_urls = []

    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue

        url = parts[-1]

        if url.endswith(".export.CSV.zip"):
            event_urls.append(url)

    return event_urls[-limit:]


def download_and_read_gdelt_zip(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as z:
        csv_filename = z.namelist()[0]

        with z.open(csv_filename) as f:
            df = pd.read_csv(
                f,
                sep="\t",
                header=None,
                names=GDELT_EVENT_COLUMNS,
                dtype=str,
                low_memory=False,
            )

    return df


def clean_gdelt_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only columns that are useful for your NL2Pipeline tasks.
    Rename them into simpler names for generated Spark code.
    """
    selected = df[
        [
            "GLOBALEVENTID",
            "SQLDATE",
            "Actor1CountryCode",
            "Actor2CountryCode",
            "EventCode",
            "EventBaseCode",
            "EventRootCode",
            "QuadClass",
            "GoldsteinScale",
            "NumMentions",
            "NumSources",
            "NumArticles",
            "AvgTone",
            "ActionGeo_CountryCode",
            "ActionGeo_FullName",
            "ActionGeo_Lat",
            "ActionGeo_Long",
            "SOURCEURL",
        ]
    ].copy()

    selected = selected.rename(
        columns={
            "GLOBALEVENTID": "global_event_id",
            "SQLDATE": "event_date",
            "Actor1CountryCode": "actor1_country",
            "Actor2CountryCode": "actor2_country",
            "EventCode": "event_code",
            "EventBaseCode": "event_base_code",
            "EventRootCode": "event_root_code",
            "QuadClass": "quad_class",
            "GoldsteinScale": "goldstein_scale",
            "NumMentions": "num_mentions",
            "NumSources": "num_sources",
            "NumArticles": "num_articles",
            "AvgTone": "avg_tone",
            "ActionGeo_CountryCode": "action_country",
            "ActionGeo_FullName": "action_location",
            "ActionGeo_Lat": "action_lat",
            "ActionGeo_Long": "action_long",
            "SOURCEURL": "source_url",
        }
    )

    numeric_columns = [
        "goldstein_scale",
        "num_mentions",
        "num_sources",
        "num_articles",
        "avg_tone",
        "action_lat",
        "action_long",
    ]

    for col in numeric_columns:
        selected[col] = pd.to_numeric(selected[col], errors="coerce")

    selected["event_date"] = pd.to_datetime(
        selected["event_date"], format="%Y%m%d", errors="coerce"
    )

    selected = selected.dropna(subset=["event_date"])

    return selected


def main():
    file_limit = 4

    print("Fetching latest GDELT event file URLs...")
    urls = get_latest_event_file_urls(limit=file_limit)

    print("Files selected:")
    for url in urls:
        print(url)

    all_frames = []

    for url in tqdm(urls, desc="Downloading GDELT files"):
        df = download_and_read_gdelt_zip(url)
        cleaned = clean_gdelt_dataframe(df)
        all_frames.append(cleaned)

    final_df = pd.concat(all_frames, ignore_index=True)

    final_df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved real GDELT sample to: {OUTPUT_FILE}")
    print(f"Rows: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    print("\nPreview:")
    print(final_df.head())


if __name__ == "__main__":
    main()