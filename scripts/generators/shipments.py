"""
Generate Specialty Pharmacy Shipments
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict
import random

from scripts.utils.helpers import (
    format_month_partition,
    weighted_choice,
    inject_data_quality_issues,
    print_generation_summary
)


def generate_shipments(enrollments_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """
    Generate prescription shipments for successfully enrolled patients
    """
    print("\n" + "="*60)
    print("GENERATING: Specialty Pharmacy Shipments")
    print("="*60)
    
    start_time = datetime.now()
    
    # Get config
    random_seed = config['project'].get('random_seed', 42)
    random.seed(random_seed)
    np.random.seed(random_seed)
    
    shipment_rate = config['funnel_rates']['first_shipment']
    avg_shipments_per_patient = config['multipliers']['shipments_per_shipped_patient']
    
    # Get refill cadence from config
    refill_options = config['timing']['refill_cadence']
    days_supply_options = [r['days_supply'] for r in refill_options]
    days_supply_weights = [r['weight'] for r in refill_options]
    
    # Filter to patients who received shipments (~45%)
    shipped_enrollments = enrollments_df.sample(frac=shipment_rate, random_state=random_seed)
    
    print(f"Generating shipments for {len(shipped_enrollments):,} patients "
          f"(~{avg_shipments_per_patient} shipments each)...")
    
    shipments = []
    shipment_counter = 0
    
    patient_counter = 0
    for idx, enrollment in shipped_enrollments.iterrows():
        patient_counter += 1
        # Determine number of shipments for this patient (with variance)
        variance = 0.3  # ±30%
        n_shipments = int(avg_shipments_per_patient * random.uniform(1 - variance, 1 + variance))
        n_shipments = max(1, n_shipments)  # At least 1 shipment
        
        # First shipment is 5-15 days after enrollment
        first_ship_date = enrollment['enrolled_ts'] + timedelta(days=random.randint(5, 15))
        
        current_ship_date = first_ship_date
        
        # Get product info from enrollment
        product_ndc = enrollment['ndc_code']
        product_name = enrollment['program_name']
        
        for refill_num in range(n_shipments):
            # Select days supply (weighted)
            days_supply = weighted_choice(days_supply_options, days_supply_weights)
            
            # Fill date is typically 1-2 days before ship date
            fill_date = current_ship_date - timedelta(days=random.randint(0, 2))
            
            # Quantity based on days supply
            if days_supply == 90:
                quantity = 3.0
            else:
                quantity = 1.0
            
            # Claim status (90% paid, 8% denied, 2% reversed)
            claim_status = weighted_choice(
                ['PAID', 'DENIED', 'REVERSED'],
                [0.90, 0.08, 0.02]
            )
            
            # Copay amount (if paid)
            if claim_status == 'PAID':
                if enrollment['plan_type'] == 'COMMERCIAL':
                    copay = random.choice([0, 10, 25, 50, 100, 150])
                elif enrollment['plan_type'] == 'MEDICARE':
                    copay = random.choice([0, 5, 15, 30, 75])
                else:
                    copay = 0
            else:
                copay = 0
            
            # Pharmacy (50 specialty pharmacies)
            pharmacy_id = f"PHARM-{random.randint(1, 50):03d}"
            
            shipment_counter += 1
            
            shipment = {
                'shipment_id': f"SHIP-{shipment_counter:08d}",
                'enrollment_id': enrollment['enrollment_id'],
                'patient_id_hash': enrollment['patient_id_hash'],
                'prescription_id': f"RX-{enrollment['enrollment_id'].split('-')[2]}-{refill_num:03d}",
                'fill_date': fill_date.date(),
                'ship_date': current_ship_date.date(),
                'shipment_month': format_month_partition(current_ship_date),
                'ndc_code': product_ndc,
                'product_name': product_name,
                'days_supply': days_supply,
                'quantity': quantity,
                'refill_number': refill_num,
                'pharmacy_id': pharmacy_id,
                'claim_status': claim_status,
                'copay_amount': copay,
                'created_at': datetime.now()
            }
            
            shipments.append(shipment)
            
            # Calculate next shipment date (with some variance)
            # Add days_supply + some early/late refills
            refill_variance = random.randint(-3, 5)  # Can refill 3 days early or 5 days late
            current_ship_date = current_ship_date + timedelta(days=days_supply + refill_variance)
            
            # Stop if we've gone past end date
            scale_config = config['scales'][config['active_scale']]
            end_date = datetime.fromisoformat(scale_config['end_date'])
            if current_ship_date > end_date:
                break
        
        # Progress indicator
        if patient_counter % 1000 == 0:
            print(f"  Generated shipments for {patient_counter:,} / {len(shipped_enrollments):,} patients...")
    
    df = pd.DataFrame(shipments)
    
    # Inject data quality issues
    print("\nInjecting data quality issues...")
    df = inject_data_quality_issues(
        df,
        config['data_quality'],
        date_columns=['fill_date', 'ship_date'],
        nullable_columns=['copay_amount']
    )
    
    print_generation_summary("Specialty Pharmacy Shipments", len(df), start_time)
    
    return df
