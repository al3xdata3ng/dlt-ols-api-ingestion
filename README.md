# 🧬 OLS EFO Data Pipeline

This project implements a data pipeline using the [DLT (Data Load Tool) framework](https://dlthub.com/) to retrieve EFO (Experimental Factor Ontology) terms from the Ontology Lookup Service (OLS) and store them in a PostgreSQL database. DLT provides efficient data loading capabilities with built-in support for incremental updates, schema management, and data integrity checks.

## 🔄 Pipeline Design

The pipeline follows an [Extract, Normalize & Load](https://dlthub.com/docs/reference/explainers/how-dlt-works) process:

1. **Extract**: Retrieve data from the OLS API
   - Fetch EFO terms using paginated API endpoints
   - Extract term details, synonyms, and parent relationships
   - Collect MeSH references from cross-references

2. **Normalize**: Normalize the data
   - Flatten nested JSON structures
   - Split terms and their relationships into separate tables
   - Normalize synonyms and MeSH references into dedicated tables
   - Generate unique identifiers for linking related data

3. **Load**: Store in PostgreSQL
   - Load data into normalized tables
   - Maintain referential integrity between related tables
   - Support incremental updates through merge operations

## ✨ Features

- 🔍 Retrieves EFO terms, synonyms, and parent-child relationships
- 🔗 Extracts MeSH term references
- 🔄 Supports incremental updates through merge write disposition
- 📦 Uses efficient bulk data movement with pagination
- 🗃️ Implements a normalized database schema

## 🛠️ DLT Framework Features

The implementation leverages several out-of-the-box features provided by the DLT framework:
- [Built-in pagination](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic#pagination) support through `JSONLinkPaginator` for efficient data retrieval
- Request client wrapper with [automatic retry mechanisms](https://dlthub.com/docs/general-usage/http/requests#customizing-retry-settings) (using default configurations)
- [Schema contract management with Pydantic](https://dlthub.com/docs/general-usage/resource#define-a-schema-with-pydantic) allowing schema evolution in current implementation
- Use of [dlt.transformer](https://dlthub.com/docs/general-usage/resource#process-resources-with-dlttransformer) decorator for terms' parents extraction.
- [Parallel processing](https://dlthub.com/docs/general-usage/resource#declare-parallel-and-async-resources) capabilities:
  - Concurrent extraction of parent terms
- [Postgres destination](https://dlthub.com/docs/dlt-ecosystem/destinations/postgres#install-dlt-with-postgresql)
- [Merge write disposition](https://dlthub.com/docs/general-usage/merge-loading) loading data to postgres (using default `delete-insert` strategy)


## 📊 Database Schema

- **terms**: Stores EFO terms
  - `iri` (Primary Key): Term identifier
  - `label`: Term label
  - `short_form`: Short form identifier
  - `ontology_name`: Name of the ontology
  - `synonyms`: List of term synonyms
  - `parent_url`: URL to parent terms
  - `mesh_ref`: List of MeSH references
  - `_dlt_id`: DLT generated unique hash

- **terms_parents**: Stores parent-child relationships
  - `iri` (Primary Key): Parent term identifier
  - `label`: Parent term label
  - `short_form`: Parent term short form
  - `ontology_name`: Parent term ontology name
  - `child_iri` (Foreign Key): Reference to child term in `terms` table

- **terms__mesh_ref**: Stores parent-child relationships
  - `value`: Term synonym
  - `_dlt_parent_id`: (Foreign Key): Reference to `_dlt_id` in `terms` table

- **terms__synonyms**: Stores parent-child relationships
  - `value`: Term mesh database reference
  - `_dlt_parent_id`: (Foreign Key): Reference to `_dlt_id` in `terms` table

## ⚙️ Configuration

The pipeline can be configured through the following files:

- `efo_pipeline_config.py`: Pipeline-level configuration
- `efo_source_config.py`: Source-specific configuration including API endpoints and table names

Key configuration options:
- `LIMIT`: Number of terms to retrieve (set to `None` for all terms - default value: `1000`)
- `WRITE_DISPOSITION`: Set to "merge" for incremental updates
- `PARALLELIZED`: Enable/disable parallel processing of parent terms

## 📈 Monitoring

- Pipeline progress is displayed using a progress bar
- Logs are written to `./.log/efo_ingestion_pipeline.log`
- Pipeline metadata is stored in the DLT pipeline directory

## 🚀 Future Work/Improvements

- **🔍 Data Quality Monitoring**
  - Implement [alerting for schema drifts](https://dlthub.com/docs/general-usage/schema-evolution#alert-schema-changes-to-curate-new-data) to catch API changes early
  
- **Data Lifecycle Management**
  - Explore hard/soft [deletes](https://dlthub.com/docs/general-usage/merge-loading#delete-records)
  - Implement [SCD Type 2](https://dlthub.com/docs/general-usage/merge-loading#scd2-strategy) for tracking historical changes on `is_obsolete` field of the API repsonse or any other field.
  
- **Performance Optimization**
  - Explore and implement strategies for incremental extraction to reduce API load and processing time

## 🚀 Setup and Usage Guide

### 📋 Prerequisites

1. **🐳 Docker & Docker Compose**
   - Install Docker: [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - Install Docker Compose: [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

2. **uv (Python Package Installer)**
   - Install uv for faster package installation: [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/)

3. **Environment Setup**
   Create a `.env` file in the root directory with your PostgreSQL configuration:
   ```env
   POSTGRES_USER=your_user
   POSTGRES_PASSWORD=your_password
   POSTGRES_DB=your_database
   POSTGRES_PORT=5432
   ```

### Running the Pipeline

After installing the prerequisites and setting up the environment file:

```bash
./run_pipeline.sh
```

## 📋 Expected Output

The pipeline creates the following database structure:

- Database: `olsdb`
- Schema: `efo`
- Tables:
  - `terms`: Main table containing EFO terms
  - `terms_parents`: Parent-child relationships between terms
  - `terms__mesh_ref`: MeSH references for terms
  - `terms__synonyms`: Synonyms for terms

### Example Query

Connect to the database using your SQL query editor of choice and run this query to explore the relational hierarchy generated:

```sql
select distinct 
    t.iri as term_iri
    , t.short_form as term_short_form
    , t."label"  as term_label
    , tp.short_form as parent_short_form
    , tp."label"  as parent_label
    , ts.value as term_synonyms
    , tmr.value as term_mesh_reference
from 
    efo.terms t 
left join 
    efo.terms_parents tp 
        on tp.child_iri = t.iri 
left join
    efo.terms__synonyms ts 
        on t._dlt_id = ts._dlt_parent_id 
left join 
    efo.terms__mesh_ref tmr 
        on t._dlt_id = tmr._dlt_parent_id 
where 
--  tp.iri is not null
    t.short_form = 'CHEBI_16113'
    and tmr._dlt_parent_id is not null
order by 
    1,2,3,4,5,6,7
limit 100;
```

This query demonstrates how to:
- Join the normalized tables to get a complete view of a term
- Access parent-child relationships
- Retrieve synonyms and MeSH references
- Filter for specific terms using their short form
