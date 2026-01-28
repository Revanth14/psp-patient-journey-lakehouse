#!/usr/bin/env python3
"""
Main data generation orchestrator
Generates all Phase 1 sources
"""
import yaml
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.generators.enrollments import generate_enrollments
from scripts.generators.cases import generate_cases
from scripts.generators.status_history import generate_status_history
from scripts.generators.shipments import generate_shipments
from scripts.generators.claims import generate_claims


def main():
    """Generate all Phase 1 data"""
    
    print("="*80)
    print("PSP PATIENT JOURNEY LAKEHOUSE - DATA GENERATION")
    print("="*80)
    
    overall_start = datetime.now()
    
    # Load config
    config_path = Path(__file__).parent.parent / 'config' / 'data_generation_config.yaml'
    
    print(f"\n📂 Loading config: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Show configuration
    active_scale = config['active_scale']
    scale_config = config['scales'][active_scale]
    
    print(f"\n⚙️  Configuration:")
    print(f"   Scale: {active_scale}")
    print(f"   Label: {scale_config['label']}")
    print(f"   Enrollments: {scale_config['enrollments']:,}")
    print(f"   Period: {scale_config['start_date']} to {scale_config['end_date']}")
    print(f"   Random Seed: {config['project']['random_seed']}")
    
    # Check which phases are enabled
    phase1_enabled = config['phases']['phase1']['enabled']
    
    if not phase1_enabled:
        print("\n❌ Phase 1 is disabled in config. Enable it to generate data.")
        return
    
    print(f"\n🎯 Phase 1 Sources: {', '.join(config['phases']['phase1']['sources'])}")
    
    # Output directory
    output_dir = Path(config['output']['base_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n💾 Output directory: {output_dir}")
    
    # ========================================
    # GENERATE DATA
    # ========================================
    
    # 1. Enrollments
    enrollments_df = generate_enrollments(config)
    enrollments_file = output_dir / 'psp_enrollments.parquet'
    enrollments_df.to_parquet(enrollments_file, compression='snappy')
    print(f"   Saved: {enrollments_file}")
    
    # 2. Cases
    cases_df = generate_cases(enrollments_df, config)
    cases_file = output_dir / 'psp_cases.parquet'
    cases_df.to_parquet(cases_file, compression='snappy')
    print(f"   Saved: {cases_file}")
    
    # 3. Status History
    status_history_df = generate_status_history(cases_df, enrollments_df, config)
    status_file = output_dir / 'psp_status_history.parquet'
    status_history_df.to_parquet(status_file, compression='snappy')
    print(f"   Saved: {status_file}")
    
    # 4. Shipments
    shipments_df = generate_shipments(enrollments_df, config)
    shipments_file = output_dir / 'specialty_pharmacy_shipments.parquet'
    shipments_df.to_parquet(shipments_file, compression='snappy')
    print(f"   Saved: {shipments_file}")
    
    # 5. Claims
    claims_df = generate_claims(enrollments_df, shipments_df, config)
    claims_file = output_dir / 'claims.parquet'
    claims_df.to_parquet(claims_file, compression='snappy')
    print(f"   Saved: {claims_file}")
    
    # ========================================
    # SUMMARY
    # ========================================
    
    overall_elapsed = (datetime.now() - overall_start).total_seconds()
    
    print("\n" + "="*80)
    print("GENERATION COMPLETE!")
    print("="*80)
    
    print(f"\n📊 Data Summary:")
    print(f"   Enrollments:       {len(enrollments_df):>10,}")
    print(f"   Cases:             {len(cases_df):>10,}")
    print(f"   Status History:    {len(status_history_df):>10,}")
    print(f"   Shipments:         {len(shipments_df):>10,}")
    print(f"   Claims:            {len(claims_df):>10,}")
    print(f"   " + "-"*40)
    print(f"   TOTAL ROWS:        {len(enrollments_df) + len(cases_df) + len(status_history_df) + len(shipments_df) + len(claims_df):>10,}")
    
    # Calculate file sizes
    total_size = sum([
        enrollments_file.stat().st_size,
        cases_file.stat().st_size,
        status_file.stat().st_size,
        shipments_file.stat().st_size,
        claims_file.stat().st_size
    ])
    
    print(f"\n💾 Storage:")
    print(f"   Total Size:        {total_size / 1024 / 1024:>10.2f} MB")
    
    print(f"\n⏱️  Performance:")
    print(f"   Total Time:        {overall_elapsed:>10.1f}s")
    print(f"   Rows/Second:       {(len(enrollments_df) + len(cases_df) + len(status_history_df) + len(shipments_df) + len(claims_df)) / overall_elapsed:>10,.0f}")
    
    print(f"\n✅ All data files saved to: {output_dir}")
    print(f"\n🎉 Stage 3 Complete! Ready for Stage 4 (Bronze Layer Ingestion)")


if __name__ == "__main__":
    main()
