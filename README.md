# OLS EFO Data Pipeline

This project implements a data pipeline using the DLT (Data Load Tool) framework to retrieve EFO (Experimental Factor Ontology) terms from the Ontology Lookup Service (OLS) and store them in a PostgreSQL database. DLT provides efficient data loading capabilities with built-in support for incremental updates, schema management, and data integrity checks.

## Prerequisites

1. **Docker & Docker Compose**
   - Install Docker: [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - Install Docker Compose: [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

2. **uv (Python Package Installer)**
   - Install uv for faster package installation: [uv installation guide] (https://docs.astral.sh/uv/getting-started/installation/)

3. **Environment Setup**
   Create a `.env` file in the root directory with your PostgreSQL configuration:
   ```env
   POSTGRES_USER=your_user
   POSTGRES_PASSWORD=your_password
   POSTGRES_DB=your_database
   POSTGRES_PORT=5432
   ```

## Features

- Retrieves EFO terms, synonyms, and parent-child relationships
- Extracts MeSH term references
- Supports incremental updates through merge write disposition
- Uses efficient bulk data movement with pagination
- Implements a normalized database schema

## Database Schema

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

## Running the Pipeline

After installing the prerequisites and setting up the environment file:

```bash
./run_pipeline.sh
```

## Configuration

The pipeline can be configured through the following files:

- `efo_pipeline_config.py`: Pipeline-level configuration
- `efo_source_config.py`: Source-specific configuration including API endpoints and table names

Key configuration options:
- `LIMIT`: Number of terms to retrieve (set to `None` for all terms)
- `WRITE_DISPOSITION`: Set to "merge" for incremental updates
- `PARALLELIZED`: Enable/disable parallel processing of parent terms

## Monitoring

- Pipeline progress is displayed using a progress bar
- Logs are written to `./.log/efo_ingestion_pipeline.log`
- Pipeline metadata is stored in the DLT pipeline directory
