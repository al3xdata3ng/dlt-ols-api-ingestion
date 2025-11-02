from datetime import datetime
import json


def pretty_print_pipeline_info(pipeline, load_info):
    print("\n" + "=" * 80)
    print("🚀 PIPELINE EXECUTION SUMMARY")
    print("=" * 80)

    print(
        f"🕒 Started at: {load_info.started_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(load_info.started_at, datetime) else load_info.started_at}"
    )
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
