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

run_params = {
    "BASE_URL": "https://www.ebi.ac.uk/ols4/api/ontologies/efo",
    "TERMS_PATH": "/terms",
    "TERMS_TABLE_NAME": "terms",
    "TERMS_PARENTS_TABLE_NAME": "terms_parents",
    "WRITE_DISPOSITION": "merge",
    "MAX_NESTING": 1,
    "SCHEMA_CONTRACT": {"tables": "evolve", "columns": "evolve", "data_type": "evolve"},
    "MAX_ITEMS": 1000,
    "MAX_TIME": 120,
    "PIPELINE_NAME": "ols_efo_nesting",
    "DATASET_NAME": "test_nesting",
    "DESTINATION": "postgres",
    "REFRESH": "drop_sources",
    "PIPELINE_DIR": os.path.join(get_dlt_pipelines_dir(), "dev")

}


ols_client = RESTClient(
    base_url=run_params["BASE_URL"],
    paginator=JSONLinkPaginator(next_url_path="_links.next.href"),   # (1)
    data_selector="_embedded.terms"                             # (2)
)


# --- 1️⃣ Configure logging ---
logger = logging.getLogger("ols_efo_pipeline")
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# File handler
file_handler = logging.FileHandler("ols_efo_pipeline.log")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
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

# --- 3️⃣ Root resource: terms ---
@dlt.resource(
    name=run_params["TERMS_TABLE_NAME"],
    write_disposition=run_params["WRITE_DISPOSITION"],
    max_table_nesting=run_params["MAX_NESTING"],
    primary_key="iri",
    columns=TermWithNesting,
    schema_contract=run_params["SCHEMA_CONTRACT"]
)
def efo_terms():
    logger.info("Fetching terms from OLS")
    pages = ols_client.paginate(
        path=run_params["TERMS_PATH"]
    )
    for page in pages:
        for term in page:
            database_cross_reference = term.get("annotation", {}).get("database_cross_reference", [])
            record = Term(
                iri=term.get("iri"),
                label=term.get("label"),
                short_form=term.get("short_form"),
                ontology_name=term.get("ontology_name"),
                synonyms=term.get("synonyms"),
                parent_url=term.get("_links", {}).get("parents", {}).get("href"),
                mesh_ref=[ref for ref in database_cross_reference if "MESH" in ref]
            )
            yield record.model_dump()

@dlt.transformer(
        data_from=efo_terms,
        name=run_params["TERMS_PARENTS_TABLE_NAME"],
        write_disposition=run_params["WRITE_DISPOSITION"],
        max_table_nesting=run_params["MAX_NESTING"],
        primary_key="iri",
        columns=Term,
        schema_contract=run_params["SCHEMA_CONTRACT"],
        parallelized=True
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
            "synonyms": parent.get("synonyms"),
            "child_iri": term["iri"]
        }



# --- 7️⃣ Pipeline runner with progress bar ---
if __name__ == "__main__":
    logger.info("Starting OLS EFO ingestion pipeline")
    # collector = TqdmCollector(single_bar=True, desc="OLS EFO direct load")

    pipeline = dlt.pipeline(
        pipeline_name=run_params["PIPELINE_NAME"],
        destination=run_params["DESTINATION"],
        dataset_name=run_params["DATASET_NAME"],
        progress="tqdm",
        pipelines_dir=run_params["PIPELINE_DIR"]
    )

    load_info = pipeline.run(
                              efo_terms().add_limit(max_items=run_params["MAX_ITEMS"]
                                                    , max_time=run_params["MAX_TIME"]
                                                    ) 
                             , refresh=run_params["REFRESH"]
                             )
    logger.info("Pipeline finished successfully")
    logger.info(load_info)
