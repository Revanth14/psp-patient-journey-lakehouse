
#  Helper functions for data generation
import hashlib
import random
from datetime import datetime, timedelta
from typing import List
import numpy as np


def hash_patient_id(patient_number: int) -> str:
    """Generate SHA-256 hashed patient ID (16 chars)"""
    patient_str = f"patient_{patient_number:08d}"
    return hashlib.sha256(patient_str.encode()).hexdigest()[:16]


def generate_npi() -> str:
    """Generate valid-looking 10-digit NPI"""
    return f"{random.randint(1000000000, 9999999999)}"


def random_date_between(start_date: datetime, end_date: datetime) -> datetime:
    """Generate random datetime between two dates"""
    time_delta = end_date - start_date
    random_days = random.randint(0, time_delta.days)
    random_seconds = random.randint(0, 86400)
    return start_date + timedelta(days=random_days, seconds=random_seconds)


def weighted_choice(choices: List, weights: List):
    """Make weighted random choice"""
    return random.choices(choices, weights=weights, k=1)[0]


def generate_us_states_weighted() -> str:
    """Generate US state with population weighting"""
    top_states = ['CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI']
    top_weights = [0.15, 0.12, 0.10, 0.08, 0.06, 0.05, 0.05, 0.04, 0.04, 0.04]
    
    remaining_states = [
        'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI',
        'CO', 'MN', 'SC', 'AL', 'LA', 'KY', 'OR', 'OK', 'CT', 'UT',
        'IA', 'NV', 'AR', 'MS', 'KS', 'NM', 'NE', 'WV', 'ID', 'HI',
        'NH', 'ME', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY'
    ]
    remaining_weight = 0.27
    remaining_weights = [remaining_weight / len(remaining_states)] * len(remaining_states)
    
    all_states = top_states + remaining_states
    all_weights = top_weights + remaining_weights
    
    return weighted_choice(all_states, all_weights)


def format_month_partition(date: datetime) -> str:
    """Format date as YYYY-MM for monthly partitioning"""
    return date.strftime('%Y-%m')


def inject_data_quality_issues(df, config: dict, date_columns: List[str] = None, 
                                nullable_columns: List[str] = None):
    """Inject intentional data quality issues for testing"""
    import pandas as pd
    
    if not config.get('inject_issues', False):
        return df
    
    n_rows = len(df)
    
    # 1. Inject duplicates
    dup_rate = config.get('duplicate_rate', 0.005)
    n_dups = int(n_rows * dup_rate)
    if n_dups > 0:
        dup_rows = df.sample(n=n_dups, replace=True)
        df = pd.concat([df, dup_rows], ignore_index=True)
        print(f"  ⚠️  Injected {n_dups} duplicate rows")
    
    # 2. Inject nulls
    if nullable_columns:
        null_rate = config.get('null_rate_optional', 0.02)
        for col in nullable_columns:
            if col in df.columns:
                n_nulls = int(n_rows * null_rate)
                if n_nulls > 0 and len(df) > 0:
                    null_indices = np.random.choice(df.index, size=min(n_nulls, len(df)), replace=False)
                    df.loc[null_indices, col] = None
        print(f"  ⚠️  Injected nulls in {len(nullable_columns)} columns")
    
    # 3. Inject future dates
    if date_columns:
        future_rate = config.get('future_date_rate', 0.001)
        n_future = int(n_rows * future_rate)
        if n_future > 0:
            for col in date_columns:
                if col in df.columns and len(df) > 0:
                    future_indices = np.random.choice(df.index, size=min(n_future, len(df)), replace=False)
                    future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                    df.loc[future_indices, col] = future_date
            print(f"  ⚠️  Injected {n_future} future dates")
    
    return df


def print_generation_summary(source_name: str, n_rows: int, start_time: datetime):
    """Print summary of data generation"""
    elapsed = (datetime.now() - start_time).total_seconds()
    rows_per_sec = n_rows / elapsed if elapsed > 0 else 0
    
    print(f"\n✅ {source_name}")
    print(f"   Rows: {n_rows:,}")
    print(f"   Time: {elapsed:.1f}s ({rows_per_sec:,.0f} rows/sec)")
