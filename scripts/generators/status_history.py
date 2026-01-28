"""
Generate PSP Status History (multiple statuses per case)
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List
import random

from scripts.utils.helpers import (
    format_month_partition,
    inject_data_quality_issues,
    print_generation_summary
)


def generate_status_history(cases_df: pd.DataFrame, enrollments_df: pd.DataFrame, 
                            config: Dict) -> pd.DataFrame:
    """
    Generate status history showing case progression
    """
    print("\n" + "="*60)
    print("GENERATING: PSP Status History")
    print("="*60)
    
    start_time = datetime.now()
    
    # Get config
    random_seed = config['project'].get('random_seed', 42)
    random.seed(random_seed)
    np.random.seed(random_seed)
    
    avg_statuses = config['multipliers']['status_changes_per_case']
    
    # Define status progression paths
    status_paths = {
        'successful': [
            'INQUIRY',
            'ENROLLED',
            'BV_PENDING',
            'BV_COMPLETE',
            'PA_PENDING',
            'PA_APPROVED',
            'SHIPPED',
            'ACTIVE'
        ],
        'abandoned_at_bv': [
            'INQUIRY',
            'ENROLLED',
            'BV_PENDING',
            'ABANDONED'
        ],
        'abandoned_at_pa': [
            'INQUIRY',
            'ENROLLED',
            'BV_PENDING',
            'BV_COMPLETE',
            'PA_PENDING',
            'PA_DENIED',
            'ABANDONED'
        ],
        'abandoned_early': [
            'INQUIRY',
            'ENROLLED',
            'ABANDONED'
        ]
    }
    
    status_history = []
    
    print(f"Generating status history for {len(cases_df):,} cases...")
    
    # Merge cases with enrollments to get timing info
    cases_with_enrollments = cases_df.merge(
        enrollments_df[['enrollment_id', 'enrolled_ts', 'inquiry_ts']],
        on='enrollment_id',
        how='left'
    )
    
    for idx, case in cases_with_enrollments.iterrows():
        # Choose status path based on case outcome
        if case['current_status'] == 'ACTIVE':
            path = status_paths['successful']
        elif case['current_status'] == 'CLOSED':
            if case['closure_reason'] == 'COMPLETED_THERAPY':
                path = status_paths['successful'] + ['CLOSED']
            else:
                # Random abandonment point
                path_choice = random.choice(['abandoned_early', 'abandoned_at_bv', 'abandoned_at_pa'])
                path = status_paths[path_choice]
        else:
            # ON_HOLD cases
            path = status_paths['successful'][:random.randint(3, 6)]
        
        # Generate timestamps for each status
        current_ts = case['inquiry_ts'] if pd.notna(case['inquiry_ts']) else case['enrolled_ts']
        
        for i, status in enumerate(path):
            # Calculate status duration (1-7 days typically)
            if i == 0:
                # First status starts at inquiry/enrollment
                status_start_ts = current_ts
            else:
                # Subsequent statuses have delays
                if status in ['BV_COMPLETE', 'PA_APPROVED']:
                    # Longer waits for completions
                    days_delay = random.randint(1, 10)
                elif status in ['SHIPPED']:
                    days_delay = random.randint(3, 14)
                else:
                    days_delay = random.randint(1, 5)
                
                status_start_ts = current_ts + timedelta(days=days_delay)
            
            # Status end time (NULL for current status)
            if i < len(path) - 1:
                # Not the last status
                status_end_ts = status_start_ts + timedelta(days=random.randint(1, 3))
            else:
                # Last status (current) has no end time
                status_end_ts = None
            
            status_reason = None
            if status == 'PA_DENIED':
                status_reason = random.choice([
                    'NOT_MEDICALLY_NECESSARY',
                    'MISSING_DOCUMENTATION',
                    'COVERAGE_ISSUE'
                ])
            elif status == 'ABANDONED':
                status_reason = case['closure_reason']
            
            status_record = {
                'status_id': f"STAT-{case['case_id'].split('-')[1]}-{case['case_id'].split('-')[2]}-{i+1:02d}",
                'case_id': case['case_id'],
                'enrollment_id': case['enrollment_id'],
                'status_start_ts': status_start_ts,
                'status_start_month': format_month_partition(status_start_ts),
                'status_end_ts': status_end_ts,
                'status': status,
                'status_reason': status_reason,
                'created_at': datetime.now()
            }
            
            status_history.append(status_record)
            
            # Update current timestamp for next status
            if status_end_ts:
                current_ts = status_end_ts
        
        # Progress indicator
        if (idx + 1) % 5000 == 0:
            print(f"  Generated history for {idx+1:,} / {len(cases_df):,} cases...")
    
    df = pd.DataFrame(status_history)
    
    # Inject data quality issues
    print("\nInjecting data quality issues...")
    df = inject_data_quality_issues(
        df,
        config['data_quality'],
        date_columns=['status_start_ts', 'status_end_ts'],
        nullable_columns=['status_end_ts', 'status_reason']
    )
    
    print_generation_summary("PSP Status History", len(df), start_time)
    
    return df
