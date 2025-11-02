import dlt
from dlt.sources.helpers import requests
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.sources.helpers.rest_client import RESTClient
from pydantic import BaseModel
from dlt.common.libs.pydantic import DltConfig
from typing import Optional, List, ClassVar
from dlt.common.runtime.collector import TqdmCollector
import logging
import sys
import os
from datetime import datetime
import json

def log_pipeline_summary(pipeline, load_info):
    """Log and pretty print a readable summary of the pipeline run."""

    logger.info("🚀 PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 70)

    started_at = (
        load_info.started_at.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(load_info.started_at, datetime)
        else load_info.started_at
    )

    logger.info(f"🕒 Started at: {started_at}")
    logger.info(f"📦 Pipeline name: {pipeline.pipeline_name}")
    logger.info(f"💾 Destination: {pipeline.destination}")
    logger.info("=" * 70)

    # Extract info
    logger.info("🧩 Extract Information")
    logger.info(pipeline.last_trace.last_extract_info)

    # Normalize info
    logger.info("🧮 Normalization Information")
    logger.info(pipeline.last_trace.last_normalize_info)
    logger.info("📈 Row counts by table:")
    logger.info(json.dumps(pipeline.last_trace.last_normalize_info.row_counts, indent=4))

    # Load info
    logger.info("🚚 Load Information")
    logger.info(pipeline.last_trace.last_load_info)

    logger.info("✅ Pipeline completed successfully")
    logger.info("=" * 70 + "\n")

def pretty_print_pipeline_info(pipeline, load_info):
    print("\n" + "=" * 80)
    print("🚀 PIPELINE EXECUTION SUMMARY")
    print("=" * 80)

    print(f"🕒 Started at: {load_info.started_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(load_info.started_at, datetime) else load_info.started_at}")
    print(f"📦 Pipeline name: {pipeline.pipeline_name}")
    print(f"💾 Destination: {pipeline.destination}")
    print()

    # # --- Trace summary ---
    # print("📊 TRACE SUMMARY")
    # print("-" * 80)
    # print(json.dumps(pipeline.last_trace.asdict(), indent=4, default=str))
    # print()

    # --- Extract info ---
    print("🧩 EXTRACT INFORMATION")
    print("-" * 80)
    print(pipeline.last_trace.last_extract_info)
    print()

    # --- Normalize info ---
    print("🧮 NORMALIZATION INFORMATION")
    print("-" * 80)
    print(pipeline.last_trace.last_normalize_info)
    print()
    print("📈 Row counts by table:")
    print(json.dumps(pipeline.last_trace.last_normalize_info.row_counts, indent=4))
    print()

    # --- Load info ---
    print("🚚 LOAD INFORMATION")
    print("-" * 80)
    print(pipeline.last_trace.last_load_info)
    print()

    print("✅ Pipeline completed successfully!")
    print("=" * 80 + "\n")


run_params = {
    "BASE_URL": "https://www.ebi.ac.uk/ols4/api/ontologies/efo",
    "TERMS_PATH": "/terms",
    "TERMS_TABLE_NAME": "terms",
    "TERMS_PARENTS_TABLE_NAME": "terms_parents",
    "TERMS_PARENTS_REFERENCES": [
        {
            "referenced_table": "terms",
            "columns": ["child_iri"],
            "referenced_columns": ["iri"],
        }
    ],
    "WRITE_DISPOSITION": "merge",
    "MAX_NESTING": 1,
    "SCHEMA_CONTRACT": {"tables": "evolve", "columns": "evolve", "data_type": "evolve"},
    "MAX_ITEMS": 1000,
    "MAX_TIME": 120,
    "LIMIT": None,
    "PIPELINE_NAME": "ols_efo_parents",
    "DATASET_NAME": "efo",
    "DESTINATION": "postgres",
    "REFRESH": "drop_sources",
    "PIPELINE_DIR": os.path.join(get_dlt_pipelines_dir(), "dev"),
}


ols_client = RESTClient(
    base_url=run_params["BASE_URL"],
    paginator=JSONLinkPaginator(next_url_path="_links.next.href"),  # (1)
    data_selector="_embedded.terms",  # (2)
)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# # --- 1️⃣ Configure logging ---
# logger = logging.getLogger("ols_efo_pipeline")
# logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

# File handler
file_handler = logging.FileHandler("./.logs/ols_efo_pipeline.log")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# --- 2️⃣ Define schemas via Pydantic ---
class Term(BaseModel):
    iri: str
    label: Optional[str]
    short_form: Optional[str]
    ontology_name: Optional[str]
    synonyms: Optional[List[str]]
    parent_url: Optional[str]
    mesh_ref: Optional[List[str]]


class TermWithNesting(Term):
    dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}


@dlt.source(name="ols_efo_source")
def efo_source(term_limit=None):
    """
    Defines the EFO source consisting of:
    - efo_terms: root resource that fetches ontology terms
    - efo_terms_parents: transformer that fetches parent terms for each term
    """

    # --- 3️⃣ Root resource: terms ---
    @dlt.resource(
        name=run_params["TERMS_TABLE_NAME"],
        write_disposition=run_params["WRITE_DISPOSITION"],
        max_table_nesting=run_params["MAX_NESTING"],
        primary_key="iri",
        columns=TermWithNesting,
        schema_contract=run_params["SCHEMA_CONTRACT"],
    )
    def efo_terms(limit=term_limit):
        logger.info("Fetching terms from OLS")
        pages = ols_client.paginate(path=run_params["TERMS_PATH"])

        count = 0  # track how many terms have been yielded

        for page in pages:
            for term in page:
                if limit is not None and count >= limit:
                    logger.info(f"Reached limit of {limit} terms. Stopping fetch.")
                    return  # stop the generator

                database_cross_reference = term.get("annotation", {}).get(
                    "database_cross_reference", []
                )

                record = Term(
                    iri=term.get("iri"),
                    label=term.get("label"),
                    short_form=term.get("short_form"),
                    ontology_name=term.get("ontology_name"),
                    synonyms=term.get("synonyms"),
                    parent_url=term.get("_links", {}).get("parents", {}).get("href"),
                    mesh_ref=[ref for ref in database_cross_reference if "MESH" in ref],
                )

                yield record.model_dump()
                count += 1

    @dlt.transformer(
        data_from=efo_terms,
        name=run_params["TERMS_PARENTS_TABLE_NAME"],
        write_disposition=run_params["WRITE_DISPOSITION"],
        max_table_nesting=run_params["MAX_NESTING"],
        primary_key="iri",
        columns=TermWithNesting,
        schema_contract=run_params["SCHEMA_CONTRACT"],
        parallelized=True,
        references=run_params["TERMS_PARENTS_REFERENCES"],
    )
    def efo_terms_parents(term):
        if not term.get("parent_url"):
            return
        response = requests.get(term["parent_url"]).json()
        for parent in response.get("_embedded", {}).get("terms", []):
            yield {
                "iri": parent.get("iri"),
                "label": parent.get("label"),
                "short_form": parent.get("short_form"),
                "ontology_name": parent.get("ontology_name"),
                "child_iri": term["iri"],
            }

    return efo_terms, efo_terms_parents


# --- 7️⃣ Pipeline runner with progress bar ---
if __name__ == "__main__":
    logger.info("Starting OLS EFO ingestion pipeline")
    # collector = TqdmCollector(single_bar=True, desc="OLS EFO direct load")

    pipeline = dlt.pipeline(
        pipeline_name=run_params["PIPELINE_NAME"],
        destination=run_params["DESTINATION"],
        dataset_name=run_params["DATASET_NAME"],
        progress="tqdm",
        pipelines_dir=run_params["PIPELINE_DIR"],
    )

    load_info = pipeline.run(
        efo_source(term_limit=run_params["LIMIT"]),
        refresh=run_params["REFRESH"],
    )
    logger.info("Pipeline finished successfully")
    logger.info(load_info)


    pretty_print_pipeline_info(pipeline, load_info)
    # log_pipeline_summary(pipeline, load_info)