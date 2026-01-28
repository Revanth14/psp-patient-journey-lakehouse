"""
Generate Claims (OPTIMIZED - Vectorized version)
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict
import random

from scripts.utils.helpers import (
    generate_npi,
    format_month_partition,
    inject_data_quality_issues,
    print_generation_summary
)


def generate_claims(enrollments_df: pd.DataFrame, shipments_df: pd.DataFrame, 
                   config: Dict) -> pd.DataFrame:
    """
    Generate medical and pharmacy claims (OPTIMIZED)
    """
    print("\n" + "="*60)
    print("GENERATING: Claims (Vectorized)")
    print("="*60)
    
    start_time = datetime.now()
    
    # Get config
    random_seed = config['project'].get('random_seed', 42)
    np.random.seed(random_seed)
    
    claims_per_patient_year = config['multipliers']['claims_per_patient_year']
    scale_config = config['scales'][config['active_scale']]
    years = scale_config['years_of_data']
    
    # Get patients with shipments
    shipped_patients = enrollments_df[
        enrollments_df['enrollment_id'].isin(shipments_df['enrollment_id'].unique())
    ].copy()
    
    print(f"Generating claims for {len(shipped_patients):,} patients...")
    
    # Pre-compute shipment date ranges per patient (vectorized)
    shipment_ranges = shipments_df.copy()
    shipment_ranges['ship_date'] = pd.to_datetime(shipment_ranges['ship_date'])
    
    patient_date_ranges = shipment_ranges.groupby('enrollment_id')['ship_date'].agg(
        first_ship='min',
        last_ship='max'
    ).reset_index()
    
    # Merge with enrollments
    shipped_patients = shipped_patients.merge(
        patient_date_ranges,
        on='enrollment_id',
        how='left'
    )
    
    # Calculate number of claims per patient (vectorized)
    base_claims = int(claims_per_patient_year * years)
    n_patients = len(shipped_patients)
    
    # Vectorized: number of claims per patient (with variance)
    claims_per_patient = np.random.randint(
        int(base_claims * 0.7),
        int(base_claims * 1.3) + 1,
        size=n_patients
    )
    
    total_claims = claims_per_patient.sum()
    print(f"  Target: ~{total_claims:,} claims total")
    
    # Pre-allocate arrays for all claims
    claim_ids = [f"CLM-{i+1:08d}" for i in range(total_claims)]
    
    # Build arrays
    enrollment_ids = []
    patient_hashes = []
    claim_dates = []
    ndc_codes = []
    payer_ids = []
    prescriber_npis = []
    
    # Vectorized generation
    claim_counter = 0
    
    for idx, patient in shipped_patients.iterrows():
        n_claims = claims_per_patient[idx]
        
        # Generate all claim dates for this patient at once (vectorized)
        first_ship = patient['first_ship']
        last_ship = patient['last_ship']
        
        # Create date range with buffer
        start_range = first_ship - timedelta(days=30)
        end_range = last_ship + timedelta(days=30)
        
        # Vectorized: random dates between start and end
        date_range_seconds = (end_range - start_range).total_seconds()
        random_offsets = np.random.uniform(0, date_range_seconds, size=n_claims)
        patient_claim_dates = [
            start_range + timedelta(seconds=offset)
            for offset in random_offsets
        ]
        
        # Append to arrays
        enrollment_ids.extend([patient['enrollment_id']] * n_claims)
        patient_hashes.extend([patient['patient_id_hash']] * n_claims)
        claim_dates.extend(patient_claim_dates)
        ndc_codes.extend([patient['ndc_code']] * n_claims)
        payer_ids.extend([patient['payer_id']] * n_claims)
        prescriber_npis.extend([patient['prescriber_npi']] * n_claims)
        
        claim_counter += n_claims
        
        # Progress indicator
        if (idx + 1) % 2000 == 0:
            print(f"  Generated claims for {idx+1:,} / {n_patients:,} patients...")
    
    print(f"  Generated {claim_counter:,} total claims")
    
    # Build DataFrame at once (much faster than appending)
    df = pd.DataFrame({
        'claim_id': claim_ids[:claim_counter],
        'enrollment_id': enrollment_ids,
        'patient_id_hash': patient_hashes,
        'claim_date_raw': claim_dates,
        'ndc_code': ndc_codes,
        'payer_id': payer_ids,
        'provider_npi': prescriber_npis
    })
    
    # Vectorized: claim type (60% pharmacy, 40% medical)
    df['claim_type'] = np.random.choice(
        ['PHARMACY', 'MEDICAL'],
        size=len(df),
        p=[0.60, 0.40]
    )
    
    # Vectorized: procedure codes for medical claims
    procedure_codes = ['99213', '99214', '96372', 'J1234']
    df['procedure_code'] = np.where(
        df['claim_type'] == 'MEDICAL',
        np.random.choice(procedure_codes, size=len(df)),
        None
    )
    
    # Vectorized: clear ndc_code for medical claims
    df.loc[df['claim_type'] == 'MEDICAL', 'ndc_code'] = None
    
    # Vectorized: claim status
    df['claim_status'] = np.random.choice(
        ['PAID', 'DENIED', 'PENDING'],
        size=len(df),
        p=[0.85, 0.10, 0.05]
    )
    
    # Vectorized: amounts (conditional on status and type)
    df['paid_amount'] = 0.0
    df['patient_paid'] = 0.0
    
    # Paid pharmacy claims
    paid_pharmacy = (df['claim_status'] == 'PAID') & (df['claim_type'] == 'PHARMACY')
    df.loc[paid_pharmacy, 'paid_amount'] = np.random.uniform(
        5000, 15000, 
        size=paid_pharmacy.sum()
    )
    df.loc[paid_pharmacy, 'patient_paid'] = np.random.uniform(
        0, 500,
        size=paid_pharmacy.sum()
    )
    
    # Paid medical claims
    paid_medical = (df['claim_status'] == 'PAID') & (df['claim_type'] == 'MEDICAL')
    df.loc[paid_medical, 'paid_amount'] = np.random.uniform(
        100, 500,
        size=paid_medical.sum()
    )
    df.loc[paid_medical, 'patient_paid'] = np.random.uniform(
        0, 50,
        size=paid_medical.sum()
    )
    
    # Round amounts
    df['paid_amount'] = df['paid_amount'].round(2)
    df['patient_paid'] = df['patient_paid'].round(2)
    
    # Set to None where amount is 0
    df.loc[df['paid_amount'] == 0, 'paid_amount'] = None
    df.loc[df['patient_paid'] == 0, 'patient_paid'] = None
    
    # Format dates
    df['claim_date'] = pd.to_datetime(df['claim_date_raw']).dt.date
    df['claim_month'] = df['claim_date_raw'].apply(format_month_partition)
    df['created_at'] = datetime.now()
    
    # Drop temporary column
    df = df.drop('claim_date_raw', axis=1)
    
    # Reorder columns
    df = df[[
        'claim_id', 'enrollment_id', 'patient_id_hash', 'claim_date', 
        'claim_month', 'claim_type', 'procedure_code', 'ndc_code',
        'payer_id', 'provider_npi', 'claim_status', 
        'paid_amount', 'patient_paid', 'created_at'
    ]]
    
    # Inject data quality issues
    print("\nInjecting data quality issues...")
    df = inject_data_quality_issues(
        df,
        config['data_quality'],
        date_columns=['claim_date'],
        nullable_columns=['procedure_code', 'ndc_code', 'paid_amount', 'patient_paid']
    )
    
    print_generation_summary("Claims (Optimized)", len(df), start_time)
    
    return df
