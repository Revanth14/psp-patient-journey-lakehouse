"""
Generate PSP Cases (1 per enrollment)
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict
import random

from scripts.utils.helpers import (
    format_month_partition,
    inject_data_quality_issues,
    print_generation_summary
)


def generate_cases(enrollments_df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    """
    Generate PSP cases (1:1 with enrollments)
    """
    print("\n" + "="*60)
    print("GENERATING: PSP Cases")
    print("="*60)
    
    start_time = datetime.now()
    
    # Get config
    random_seed = config['project'].get('random_seed', 42)
    random.seed(random_seed)
    np.random.seed(random_seed)
    
    cases = []
    
    print(f"Generating {len(enrollments_df):,} cases (1 per enrollment)...")
    
    for idx, enrollment in enrollments_df.iterrows():
        # Case opened typically same day as enrollment (±1 day)
        case_opened_ts = enrollment['enrolled_ts'] + timedelta(
            hours=random.randint(-12, 12)
        )
        
        # Determine case status based on enrollment journey
        # We'll use enrollment status as proxy (will be refined in status_history)
        case_statuses = ['ACTIVE', 'CLOSED', 'ON_HOLD']
        case_weights = [0.60, 0.35, 0.05]  # 60% active, 35% closed, 5% on hold
        
        current_status = random.choices(case_statuses, weights=case_weights)[0]
        
        # If closed, set closed timestamp
        if current_status == 'CLOSED':
            # Closed 30-180 days after opening
            days_to_close = random.randint(30, 180)
            closed_ts = case_opened_ts + timedelta(days=days_to_close)
            
            # Closure reasons
            closure_reasons = [
                'COMPLETED_THERAPY',
                'ABANDONED',
                'LOST_TO_FOLLOWUP',
                'PATIENT_DECLINED',
                'TRANSFERRED'
            ]
            closure_weights = [0.50, 0.25, 0.15, 0.07, 0.03]
            closure_reason = random.choices(closure_reasons, weights=closure_weights)[0]
        else:
            closed_ts = None
            closure_reason = None
        
        # Assign case manager (20 case managers)
        case_manager_id = f"CM-{random.randint(1, 20):03d}"
        
        case = {
            'case_id': f"CASE-{enrollment['enrollment_id'].split('-')[1]}-{enrollment['enrollment_id'].split('-')[2]}",
            'enrollment_id': enrollment['enrollment_id'],
            'patient_id_hash': enrollment['patient_id_hash'],
            'case_opened_ts': case_opened_ts,
            'opened_month': format_month_partition(case_opened_ts),
            'case_manager_id': case_manager_id,
            'current_status': current_status,
            'closed_ts': closed_ts,
            'closure_reason': closure_reason,
            'created_at': datetime.now()
        }
        
        cases.append(case)
        
        # Progress indicator
        if (idx + 1) % 5000 == 0:
            print(f"  Generated {idx+1:,} / {len(enrollments_df):,} cases...")
    
    df = pd.DataFrame(cases)
    
    # Inject data quality issues
    print("\nInjecting data quality issues...")
    df = inject_data_quality_issues(
        df,
        config['data_quality'],
        date_columns=['case_opened_ts', 'closed_ts'],
        nullable_columns=['closed_ts', 'closure_reason']
    )
    
    print_generation_summary("PSP Cases", len(df), start_time)
    
    return df
