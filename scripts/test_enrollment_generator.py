#!/usr/bin/env python3
"""
Test enrollment generator
"""
import yaml
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.generators.enrollments import generate_enrollments


def main():
    """Test enrollment generation"""
    
    print("="*80)
    print("PSP ENROLLMENT GENERATOR - TEST MODE")
    print("="*80)
    
    # Load config
    config_path = Path(__file__).parent.parent / 'config' / 'data_generation_config.yaml'
    
    print(f"\n📂 Loading config: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    print(f"✅ Config loaded successfully!")
    
    # Show available scales
    print(f"\n📊 Available scales:")
    for scale_name, scale_config in config['scales'].items():
        is_active = "⭐ ACTIVE" if scale_name == config['active_scale'] else ""
        print(f"  - {scale_name}: {scale_config['enrollments']:,} enrollments, {scale_config['years_of_data']} year(s) {is_active}")
    
    # Override for test mode
    print("\n" + "="*80)
    print("🧪 TEST MODE: Generating 100 enrollments (overriding config)")
    print("="*80)
    
    # Create test scale
    config['active_scale'] = 'test'
    config['scales']['test'] = {
        'label': 'Test Mode',
        'enrollments': 100,
        'years_of_data': 1,
        'start_date': '2024-01-01',
        'end_date': '2024-12-31'
    }
    
    # Generate enrollments
    df = generate_enrollments(config)
    
    # Display results
    print("\n" + "="*80)
    print("RESULTS")
    print("="*80)
    
    print(f"\n📊 Generated {len(df):,} rows")
    
    print(f"\n📋 Sample records (first 3):")
    print(df[['enrollment_id', 'program_name', 'enrolled_ts', 'enrollment_channel', 'payer_name', 'plan_type']].head(3).to_string())
    
    print(f"\n📈 Distributions:")
    
    print(f"\n  Programs:")
    print(df['program_name'].value_counts(normalize=True).apply(lambda x: f"{x*100:.1f}%"))
    
    print(f"\n  Channels:")
    print(df['enrollment_channel'].value_counts(normalize=True).apply(lambda x: f"{x*100:.1f}%"))
    
    print(f"\n  Plan Types:")
    print(df['plan_type'].value_counts(normalize=True).apply(lambda x: f"{x*100:.1f}%"))
    
    print(f"\n  Top 5 Payers:")
    print(df['payer_name'].value_counts().head(5))
    
    print(f"\n🔍 Data Quality Check:")
    print(f"  - Unique enrollments: {df['enrollment_id'].nunique():,}")
    print(f"  - Duplicates: {len(df) - df['enrollment_id'].nunique():,}")
    print(f"  - Null payers (cash pay): {df['payer_id'].isna().sum():,} ({df['payer_id'].isna().sum()/len(df)*100:.1f}%)")
    print(f"  - Null hub vendors: {df['hub_vendor'].isna().sum():,} ({df['hub_vendor'].isna().sum()/len(df)*100:.1f}%)")
    
    # Save test output
    output_dir = Path(__file__).parent.parent / 'data' / 'raw_samples'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / 'test_enrollments.parquet'
    df.to_parquet(output_file, compression='snappy')
    
    print(f"\n💾 Saved to: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")
    
    print("\n" + "="*80)
    print("✅ TEST PASSED!")
    print("="*80)
    
    print(f"\n💡 Next steps:")
    print(f"  1. To test with DEV scale (5K):   Change active_scale: dev in config")
    print(f"  2. To test with DEMO scale (25K): Change active_scale: demo in config")
    print(f"  3. Build remaining generators:    cases, status_history, shipments, claims")


if __name__ == "__main__":
    main()
