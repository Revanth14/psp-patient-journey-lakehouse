#!/usr/bin/env python3
"""
Bronze Layer Ingestion
Reads Parquet files and writes to Delta Lake with audit columns
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil


def ingest_source_to_bronze(source_name, partition_col=None):
    """Ingest using Pandas (simpler, no Spark issues)"""
    print(f"\n{'='*60}")
    print(f"INGESTING: {source_name}")
    print(f"{'='*60}")
    
    start_time = datetime.now()
    
    # Paths
    raw_path = f"data/raw_samples/{source_name}.parquet"
    bronze_path = f"data/bronze/{source_name}.parquet"
    
    # Check if source exists
    if not Path(raw_path).exists():
        print(f"Source not found: {raw_path}")
        return
    
    # Read with Pandas
    print(f"Reading: {raw_path}")
    df = pd.read_parquet(raw_path)
    
    row_count = len(df)
    print(f"Rows: {row_count:,}")
    
    # Add audit columns
    df['_bronze_loaded_at'] = datetime.now()
    df['_bronze_source'] = f"{source_name}.parquet"
    
    # Write to Bronze
    print(f"Writing to: {bronze_path}")
    
    # Create bronze directory
    Path("data/bronze").mkdir(parents=True, exist_ok=True)
    
    # Write parquet
    df.to_parquet(
        bronze_path,
        compression='snappy',
        index=False
    )
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"Complete in {elapsed:.1f}s")
    
    # Verify
    verify_df = pd.read_parquet(bronze_path)
    verify_count = len(verify_df)
    
    if verify_count == row_count:
        print(f"Verification passed: {verify_count:,} rows")
    else:
        print(f"Row count mismatch: {row_count:,} → {verify_count:,}")


def main():
    """Main ingestion pipeline"""
    print("="*80)
    print("PSP LAKEHOUSE - BRONZE LAYER INGESTION (Pandas)")
    print("="*80)
    
    overall_start = datetime.now()
    
    # Define sources
    sources = [
        "psp_enrollments",
        "psp_cases",
        "psp_status_history",
        "specialty_pharmacy_shipments",
        "claims",
    ]
    
    # Ingest each source
    for source_name in sources:
        ingest_source_to_bronze(source_name)
    
    # Summary
    overall_elapsed = (datetime.now() - overall_start).total_seconds()
    
    print("\n" + "="*80)
    print("BRONZE INGESTION COMPLETE")
    print("="*80)
    print(f"Total Time: {overall_elapsed:.1f}s")
    
    # List bronze tables
    bronze_dir = Path("data/bronze")
    if bronze_dir.exists():
        files = list(bronze_dir.glob("*.parquet"))
        print(f"\n📊 Bronze Tables Created ({len(files)}):")
        for file in sorted(files):
            size_mb = file.stat().st_size / 1024 / 1024
            print(f"   • {file.stem:40} {size_mb:>6.2f} MB")
    
    print(f"\nBronze layer ready!")
    print(f"Next: Silver layer cleaning & validation")


if __name__ == "__main__":
    main()