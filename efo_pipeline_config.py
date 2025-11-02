from dlt.common.pipeline import get_dlt_pipelines_dir
from dataclasses import dataclass
import os

@dataclass
class EfoPipelineConfig():
    PIPELINE_NAME: str = "efo_ingestion_pipeline"
    DESTINATION: str = "postgres" 
    DATASET_NAME: str = "efo"
    PIPELINE_DIR: str = os.path.join(get_dlt_pipelines_dir(), "dev")
    PROGRESS: str = "tqdm"
    REFRESH: str = "drop_sources"