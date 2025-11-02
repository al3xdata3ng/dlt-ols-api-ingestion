from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from abc import ABC

# --- Abstract Base Class ---
@dataclass
class EfoBaseConfig(ABC):
    SOURCE_NAME: str = "ols_efo_source"
    BASE_URL: str = "https://www.ebi.ac.uk/ols4/api/ontologies/efo"
    WRITE_DISPOSITION: str = "merge"
    MAX_NESTING: int = 1
    SCHEMA_CONTRACT: Dict[str, str] = field(default_factory=lambda: {
        "tables": "evolve",
        "columns": "evolve",
        "data_type": "evolve"
    })
    LIMIT: Optional[int] = 1000

# --- Subclass 1: EFO Terms ---
@dataclass
class EfoTermsConfig(EfoBaseConfig):
    TERMS_PATH: str = "/terms"
    TERMS_TABLE_NAME: str = "terms"
    TERMS_PRIMARY_KEY: str = "iri"

# --- Subclass 2: EFO Terms Parents ---
@dataclass
class EfoTermsParentsConfig(EfoBaseConfig):
    TERMS_PARENTS_TABLE_NAME: str = "terms_parents"
    TERMS_PARENTS_REFERENCES: List[Dict[str, Any]] = field(default_factory=lambda: [
        {
            "referenced_table": "terms",
            "columns": ["child_iri"],
            "referenced_columns": ["iri"],
        }
    ])
    TERMS_PARENTS_PRIMARY_KEY: str = "iri"
    PARALLELIZED: bool = True


# --- Subclass 3: OLS API client ---
@dataclass
class OlsApiClientConfig(EfoBaseConfig):
    DATA_SELECTOR: str = "_embedded.terms"
    PAGINATOR_NEXT_URL_PATH: str = "_links.next.href"
