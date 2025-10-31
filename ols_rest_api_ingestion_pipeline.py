import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from urllib.parse import quote


@dlt.source
def efo_terms_source():
    """
    dlt source for fetching efo ontology terms from the EBI OLS4 API.
    It automatically paginates through results and extracts the list of terms.
    """

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.ebi.ac.uk/ols4/api/ontologies/efo/",
            # Pagination: follow "_links.next.href" if present in JSON response
            "paginator": {
                "type": "json_link",
                "next_url_path": "_links.next.href"
            },
        },
        "resource_defaults": {
            # Each ontology term has a unique IRI — use it as the PK
            "primary_key": "iri",
            "write_disposition": "merge",
            "max_table_nesting": 2
        },
        "resources": [
            {
                "name": "efo_terms",
                "endpoint": {
                    "path": "terms",
                    "params": {
                        "lang": "en"
                    },
                    # OLS wraps data in _embedded.terms, so extract from there
                    "data_selector": "_embedded.terms"
                    # "columns": ["iri", "label", "ontology_name", "ontology_iri", "is_obsolete", "short_form", "obo_id", "_links__parents__href"],
                }
            },
            # {
            #     "name": "efo_terms_parents",
            #     "endpoint": {
            #         "path": "terms/{resources.efo_terms.short_form}/parents"
            #         # quote(quote({resources.efo_terms.short_form}, safe=""), safe="")
            #     }
            # },            
        ]
    }

    # Generate dlt resources from config
    yield from rest_api_resources(config)


def load_efo_terms():
    """
    Creates and runs the dlt pipeline to load efo ontology terms
    into a DuckDB database (default: .dlt/efo_terms_pipeline.duckdb)
    """
    pipeline = dlt.pipeline(
        pipeline_name="efo_terms_pipeline",
        destination="postgres",        # or 'bigquery', 'snowflake', etc.
        dataset_name="efo_terms_dataset"
    )

    load_info = pipeline.run(efo_terms_source())
    print(load_info)


if __name__ == "__main__":
    load_efo_terms()
