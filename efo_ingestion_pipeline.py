import dlt
from dlt.common.runtime.collector import TqdmCollector
import logging
import sys
from efo_pipeline_config import EfoPipelineConfig
from efo_source import efo_source

efo_pipeline_config = EfoPipelineConfig()

# --- 1️⃣ Configure logging ---
logger = logging.getLogger(efo_pipeline_config.PIPELINE_NAME)
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

# File handler
file_handler = logging.FileHandler(f"./.log/{efo_pipeline_config.PIPELINE_NAME}.log")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# --- 7️⃣ Pipeline runner with progress bar ---
if __name__ == "__main__":
    logger.info("Starting OLS EFO ingestion pipeline")
    # collector = TqdmCollector(single_bar=True, desc="OLS EFO direct load")

    pipeline = dlt.pipeline(
        pipeline_name=efo_pipeline_config.PIPELINE_NAME,
        destination=efo_pipeline_config.DESTINATION,
        dataset_name=efo_pipeline_config.DATASET_NAME,
        progress=efo_pipeline_config.PROGRESS,
        pipelines_dir=efo_pipeline_config.PIPELINE_DIR,
    )

    load_info = pipeline.run(
        efo_source(),
        refresh=efo_pipeline_config.REFRESH,
    )
    logger.info("Pipeline finished successfully")
    logger.info(load_info)
