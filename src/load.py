import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

# ============================================================
# Config
# ============================================================
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
TABLE = "raw_price_snapshots"
DATA_DIR = "data"


# ============================================================
# 1. Load one JSONL file into BigQuery (append)
# ============================================================
def load_jsonl_to_bq(client, file_path, table_id):
    """Batch-load one JSONL file into a BigQuery table using append.

    Args:
        client: An authenticated bigquery.Client.
        file_path: Path to the local .jsonl file.
        table_id: Fully-qualified table id "project.dataset.table".

    Returns:
        int: Total row count in the table after the load.
    """
    # Three decisions: JSONL format, append (never truncate), use the table's
    # existing schema instead of guessing.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )

    # "rb" = read binary; load_table_from_file expects a binary file object.
    with open(file_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)

    # Block until the job finishes. If the load fails (schema mismatch, bad
    # type, REQUIRED column is null...) this line raises with the reason.
    job.result()

    table = client.get_table(table_id)
    return table.num_rows


# ============================================================
# 2. Orchestration
# ============================================================
def main():
    # Fail loud if config is missing -- nothing to do without a target table.
    if not PROJECT_ID or not DATASET:
        sys.exit("Missing GCP_PROJECT_ID or BQ_DATASET in .env")

    client = bigquery.Client(project=PROJECT_ID)

    # Python client uses dots: project.dataset.table  (bq CLI uses a colon!)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    # Default to today's file; allow an explicit path as the first CLI arg
    # so we can reload old files later (useful for Airflow backfills).
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        today = datetime.now(timezone.utc).date().isoformat()
        file_path = f"{DATA_DIR}/snapshots_{today}.jsonl"

    if not os.path.exists(file_path):
        sys.exit(f"File not found: {file_path} (run extract.py first?)")

    n = load_jsonl_to_bq(client, file_path, table_id)
    print(f"Loaded {file_path} -> {table_id}")
    print(f"Table now has {n} rows")


if __name__ == "__main__":
    main()