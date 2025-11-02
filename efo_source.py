import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.sources.helpers.rest_client import RESTClient
from pydantic import BaseModel
from dlt.common.libs.pydantic import DltConfig
from typing import Optional, List, ClassVar
import logging
import sys
import os
from datetime import datetime
import json
from efo_source_config import (
    EfoBaseConfig,
    OlsApiClientConfig,
    EfoTermsConfig,
    EfoTermsParentsConfig,
)

# --- 1️⃣ Configure logging ---
logger = logging.getLogger("ols_efo_pipeline")
logger.setLevel(logging.INFO)

ols_client = OlsApiClientConfig()
efo_source_config = EfoBaseConfig()
efo_terms_config = EfoTermsConfig()
efo_terms_parents_config = EfoTermsParentsConfig()

ols_client = RESTClient(
    base_url=ols_client.BASE_URL,
    paginator=JSONLinkPaginator(
        next_url_path=ols_client.PAGINATOR_NEXT_URL_PATH
    ),  # (1)
    data_selector=ols_client.DATA_SELECTOR,  # (2)
)


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


@dlt.source(name=efo_source_config.SOURCE_NAME)
def efo_source(term_limit=efo_source_config.LIMIT):
    """
    Defines the EFO source consisting of:
    - efo_terms: root resource that fetches ontology terms
    - efo_terms_parents: transformer that fetches parent terms for each term
    """

    # --- 3️⃣ Root resource: terms ---
    @dlt.resource(
        name=efo_terms_config.TERMS_TABLE_NAME,
        write_disposition=efo_terms_config.WRITE_DISPOSITION,
        max_table_nesting=efo_terms_config.MAX_NESTING,
        primary_key=efo_terms_config.TERMS_PRIMARY_KEY,
        columns=TermWithNesting,
        schema_contract=efo_terms_config.SCHEMA_CONTRACT,
    )
    def efo_terms(limit=term_limit):
        logger.info("Fetching terms from OLS")
        pages = ols_client.paginate(path=efo_terms_config.TERMS_PATH)

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
        name=efo_terms_parents_config.TERMS_PARENTS_TABLE_NAME,
        write_disposition=efo_terms_parents_config.WRITE_DISPOSITION,
        max_table_nesting=efo_terms_parents_config.MAX_NESTING,
        primary_key=efo_terms_parents_config.TERMS_PARENTS_PRIMARY_KEY,
        columns=TermWithNesting,
        schema_contract=efo_terms_parents_config.SCHEMA_CONTRACT,
        parallelized=efo_terms_parents_config.PARALLELIZED,
        references=efo_terms_parents_config.TERMS_PARENTS_REFERENCES,
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
