#!/usr/bin/env python3
"""
Inspect Bronze layer Delta tables
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path


def create_spark_session():
    """Create Spark session"""
    builder = (
        SparkSession.builder
        .appName("Inspect Bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    spark = create_spark_session()
    
    print("="*80)
    print("BRONZE LAYER INSPECTION")
    print("="*80)
    
    # Inspect enrollments
    print("\nPSP ENROLLMENTS (Sample):")
    enrollments = spark.read.format("delta").load("data/bronze/psp_enrollments")
    
    print(f"Total Rows: {enrollments.count():,}")
    print(f"Columns: {len(enrollments.columns)}")
    
    print("\nSample Data (3 rows):")
    enrollments.select(
        "enrollment_id", "program_name", "enrolled_ts", 
        "payer_name", "enrollment_channel", "_bronze_loaded_at"
    ).show(3, truncate=False)
    
    # Partition info
    print("\nPARTITION DISTRIBUTION (First 10):")
    enrollments.groupBy("enrolled_month").count().orderBy("enrolled_month").show(10)
    
    # Claims inspection
    print("\nCLAIMS (Sample):")
    claims = spark.read.format("delta").load("data/bronze/claims")
    
    print(f"Total Claims: {claims.count():,}")
    claims.select(
        "claim_id", "claim_type", "claim_date", "claim_status", 
        "paid_amount", "_bronze_loaded_at"
    ).show(3, truncate=False)
    
    # Elevance Health check
    print("\nELEVANCE HEALTH:")
    elevance_count = enrollments.filter("payer_name = 'Elevance Health'").count()
    print(f"   Enrollments: {elevance_count:,}")
    
    elevance_claims = claims.filter("payer_id = 'ELEV-001'").count()
    print(f"   Claims:      {elevance_claims:,}")
    
    # Check audit columns
    print("\nAUDIT TRAIL CHECK:")
    print("Bronze load timestamps (should all be recent):")
    enrollments.select("_bronze_loaded_at").distinct().show(5, truncate=False)
    
    spark.stop()
    print("\nInspection complete!")


if __name__ == "__main__":
    main()
