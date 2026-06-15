# GDELT event schema used by the NL2Pipeline (from dataset_generation/download_gdelt_real_data.py)

GDELT_SCHEMA = {
    "dataset": "gdelt_events",
    "description": "Cleaned GDELT 2.0 event export fields used by NL2Pipeline",
    "columns": [
        {"name": "global_event_id", "type": "string", "source": "GLOBALEVENTID"},
        {"name": "event_date", "type": "datetime", "source": "SQLDATE"},
        {"name": "actor1_country", "type": "string", "source": "Actor1CountryCode"},
        {"name": "actor2_country", "type": "string", "source": "Actor2CountryCode"},
        {"name": "event_code", "type": "string", "source": "EventCode"},
        {"name": "event_base_code", "type": "string", "source": "EventBaseCode"},
        {"name": "event_root_code", "type": "string", "source": "EventRootCode"},
        {"name": "quad_class", "type": "string", "source": "QuadClass"},
        {"name": "goldstein_scale", "type": "float", "source": "GoldsteinScale"},
        {"name": "num_mentions", "type": "integer", "source": "NumMentions"},
        {"name": "num_sources", "type": "integer", "source": "NumSources"},
        {"name": "num_articles", "type": "integer", "source": "NumArticles"},
        {"name": "avg_tone", "type": "float", "source": "AvgTone"},
        {"name": "action_country", "type": "string", "source": "ActionGeo_CountryCode"},
        {"name": "action_location", "type": "string", "source": "ActionGeo_FullName"},
        {"name": "action_lat", "type": "float", "source": "ActionGeo_Lat"},
        {"name": "action_long", "type": "float", "source": "ActionGeo_Long"},
        {"name": "source_url", "type": "string", "source": "SOURCEURL"},
    ],
}
