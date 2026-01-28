"""
Generate PSP Enrollments
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict
import random

from scripts.utils.helpers import (
    hash_patient_id,
    generate_npi,
    random_date_between,
    weighted_choice,
    generate_us_states_weighted,
    format_month_partition,
    inject_data_quality_issues,
    print_generation_summary
)


def generate_enrollments(config: Dict) -> pd.DataFrame:
    """
    Generate PSP enrollment records
    """
    print("\n" + "="*60)
    print("GENERATING: PSP Enrollments")
    print("="*60)
    
    start_time = datetime.now()
    
    # Get active scale config
    active_scale = config['active_scale']
    scale_config = config['scales'][active_scale]
    
    print(f"Scale: {active_scale} - {scale_config['label']}")
    print(f"Target: {scale_config['enrollments']:,} enrollments")
    print(f"Period: {scale_config['start_date']} to {scale_config['end_date']}")
    
    n_enrollments = scale_config['enrollments']
    start_date = datetime.fromisoformat(scale_config['start_date'])
    end_date = datetime.fromisoformat(scale_config['end_date'])
    
    # Get dimensions
    channels = config['dimensions']['channels']
    hub_vendors = config['dimensions']['hub_vendors']
    program_types = config['dimensions']['program_types']
    products = config['dimensions']['products']
    payers = config['dimensions']['payers']
    plan_types = config['dimensions']['plan_types']
    specialties = config['dimensions']['prescriber_specialties']
    
    # Set random seed
    random_seed = config['project'].get('random_seed', 42)
    random.seed(random_seed)
    np.random.seed(random_seed)
    
    # Generate enrollments
    enrollments = []
    
    print(f"\nGenerating {n_enrollments:,} enrollments...")
    
    for i in range(n_enrollments):
        # Select product (weighted)
        product = weighted_choice(products, [p['weight'] for p in products])
        
        # Select program type (weighted)
        program_type = weighted_choice(program_types, [pt['weight'] for pt in program_types])['name']
        
        # Generate enrollment timestamp
        enrolled_ts = random_date_between(start_date, end_date)
        
        # Inquiry typically 0-14 days before enrollment
        inquiry_days_before = random.randint(0, 14)
        inquiry_ts = enrolled_ts - timedelta(days=inquiry_days_before) if random.random() > 0.1 else None
        
        # Select plan type
        plan_type_obj = weighted_choice(plan_types, [pt['weight'] for pt in plan_types])
        plan_type = plan_type_obj['name']
        
        # Select payer (unless cash pay)
        if plan_type == 'CASH_PAY':
            payer_id = None
            payer_name = None
        else:
            payer = weighted_choice(payers, [p['weight'] for p in payers])
            payer_id = payer['payer_id']
            payer_name = payer['payer_name']
        
        # Select channel
        channel = weighted_choice(channels, [c['weight'] for c in channels])['name']
        
        # Select hub vendor (90% have one)
        if random.random() < 0.90:
            hub_vendor = weighted_choice(hub_vendors, [hv['weight'] for hv in hub_vendors])['name']
        else:
            hub_vendor = None
        
        # Select prescriber specialty
        specialty = weighted_choice(specialties, [s['weight'] for s in specialties])['name']
        
        enrollment = {
            'enrollment_id': f"PSP-{enrolled_ts.year}-{i+1:06d}",
            'patient_id_hash': hash_patient_id(i),
            'program_id': product['product_id'],
            'program_name': product['product_name'],
            'program_type': program_type,
            'indication': product['indication'],
            'ndc_code': product['ndc'],
            'enrolled_ts': enrolled_ts,
            'enrolled_month': format_month_partition(enrolled_ts),
            'inquiry_ts': inquiry_ts,
            'enrollment_channel': channel,
            'hub_vendor': hub_vendor,
            'payer_id': payer_id,
            'payer_name': payer_name,
            'plan_type': plan_type,
            'prescriber_npi': generate_npi(),
            'prescriber_specialty': specialty,
            'patient_state': generate_us_states_weighted(),
            'patient_zip3': f"{random.randint(100, 999)}",
            'created_at': datetime.now()
        }
        
        enrollments.append(enrollment)
        
        # Progress indicator
        if (i + 1) % 5000 == 0:
            print(f"  Generated {i+1:,} / {n_enrollments:,} enrollments...")
    
    df = pd.DataFrame(enrollments)
    
    # Inject data quality issues
    print("\nInjecting data quality issues...")
    df = inject_data_quality_issues(
        df,
        config['data_quality'],
        date_columns=['enrolled_ts', 'inquiry_ts'],
        nullable_columns=config['data_quality']['nullable_fields']
    )
    
    print_generation_summary("PSP Enrollments", len(df), start_time)
    
    return df
