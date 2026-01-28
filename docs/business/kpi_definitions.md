
# KPI Definitions — PSP Patient Journey Lakehouse

**Version:** 1.0  
**Last Updated:** 2025-01-27  
**Owner:** Revanth

---

## 1) Hero Table Grain

**Table:** `gold_psp_patient_journey`  
**Grain:** 1 row per **enrollment** (`enrollment_id`) per patient.  
A patient may have multiple enrollments over time.

### Keys
- `patient_id_hash` (de-identified stable key)
- `enrollment_id` (unique per enrollment episode)
- Optional linkage:
  - `primary_case_id` (e.g., PA case)
  - `payer_id`, `provider_id`, `product_id`

---

## 2) Source Milestones (Derived Dates)

All dates are stored in UTC date or timestamp columns.

**Required milestone columns (nullable):**
- `enrolled_ts`
- `bv_completed_ts`
- `pa_submitted_ts`
- `pa_approved_ts`
- `first_shipment_ts`

**Optional:**
- `pa_denied_ts`
- `appeal_submitted_ts`
- `copay_assistance_approved_ts`

---

## 3) Funnel KPIs (Conversion)

All funnel metrics are based on `enrollment_id` within the reporting period.

### Enrollment Count
**Definition:** count distinct `enrollment_id` where `date(enrolled_ts)` in period.

### BV Completed Rate
**BV Completed:** `bv_completed_ts IS NOT NULL`  
**Rate:** completed / enrolled

### PA Submitted Rate
**PA Submitted:** `pa_submitted_ts IS NOT NULL`  
**Rate:** submitted / enrolled

### PA Approval Rate
**Approved:** `pa_approved_ts IS NOT NULL` OR `pa_outcome = 'APPROVED'`  
**Denied:** `pa_denied_ts IS NOT NULL` OR `pa_outcome = 'DENIED'`  
**Rate:** approved / (approved + denied)  
**Note:** exclude pending from denominator.

### Shipped Rate
**Shipped:** `first_shipment_ts IS NOT NULL`  
**Rate:** shipped / enrolled

---

## 4) Time-to-Therapy (TtT)

### TtT Days
**Definition:**  
```sql
time_to_therapy_days = date(first_shipment_ts) - date(enrolled_ts)
```

**Eligibility:** only compute when both timestamps exist.  
**Quality rule:** negative values are invalid and must fail DQ.  
**Reporting:** median and p90 by payer/channel/program_type, etc.

---

## 5) Abandonment

### Abandoned Flag (default)
**Definition:** no shipment within **30 days** of enrollment.  
```sql
abandoned_flag = (first_shipment_ts IS NULL) 
                 AND (date_diff(current_date, date(enrolled_ts)) > 30)
```

### Abandonment within a reporting window
For historical reporting (so results don't change day to day), define a fixed cutoff:
```sql
abandoned_by_period_end = first_shipment_ts IS NULL 
                          AND date_diff(period_end_date, date(enrolled_ts)) > 30
```

---

## 6) Bottleneck / Stage Durations

All duration metrics are computed in **days** and are nullable if either boundary is missing.
```sql
-- Time to BV completion
time_to_bv_days = date(bv_completed_ts) - date(enrolled_ts)

-- Time from BV to PA submit (fallback to enrollment if no BV)
time_to_pa_submit_days = date(pa_submitted_ts) 
                         - coalesce(date(bv_completed_ts), date(enrolled_ts))

-- Time for PA decision (payer turnaround)
time_to_pa_approval_days = date(pa_approved_ts) - date(pa_submitted_ts)

-- Time from approval to shipment
time_from_approval_to_ship_days = date(first_shipment_ts) - date(pa_approved_ts)
```

---

## 7) Segmentation Dimensions (required in hero table)

Include these to enable real analysis:

| Dimension | Field | Example Values |
|-----------|-------|----------------|
| **Channel** | `channel` | hub, web, field, call_center |
| **Hub Vendor** | `hub_vendor` | Eversana, McKesson, Cigna |
| **Program Type** | `program_type` | copay, bridge, free_trial, PAP |
| **Payer** | `payer_id` | Aetna, UHC, BCBS |
| **Product** | `product_id` | YORVIPATH, SKYTROFA |
| **Indication** | `indication` | Hypoparathyroidism, Growth Hormone Deficiency |

---

## 8) Known Limitations / Assumptions

- **Data Lag:** Claims and shipment data may arrive late; SLA differs by source.
- **Optional Milestones:** Some enrollments may skip milestones (e.g., no PA required).
- **Status History:** The journey table uses "first achieved" milestone unless specified.
- **Reversals:** Status history may contain reversals; latest state is considered final.
- **De-identification:** `patient_id_hash` is SHA-256 hashed; cannot link to external systems.

---

## 9) Data Quality Rules

### Required Checks

**Enrollment-level:**
- ✅ `enrollment_id` must be unique
- ✅ `patient_id_hash` must not be null
- ✅ `enrolled_ts` must not be null
- ✅ `enrolled_ts` must be <= current_date (no future enrollments)

**Milestone Sequencing:**
- ✅ `bv_completed_ts >= enrolled_ts` (if not null)
- ✅ `pa_submitted_ts >= bv_completed_ts` (if both not null)
- ✅ `pa_approved_ts >= pa_submitted_ts` (if both not null)
- ✅ `first_shipment_ts >= enrolled_ts` (if not null)

**Calculated Metrics:**
- ✅ `time_to_therapy_days >= 0` (no negative durations)
- ✅ `time_to_bv_days >= 0`
- ✅ `time_to_pa_approval_days >= 0`
- ✅ `time_from_approval_to_ship_days >= 0`

**Referential Integrity:**
- ✅ `payer_id` exists in `dim_payer` (if not null)
- ✅ `product_id` exists in `dim_product`
- ✅ `provider_id` exists in `dim_provider` (if not null)

### Alert Thresholds

**Metric Anomalies:**
- ⚠️ Alert if median TtT > 30 days
- ⚠️ Alert if PA approval rate < 60%
- ⚠️ Alert if abandonment rate > 30%
- ⚠️ Alert if row count changes > 30% day-over-day

**Data Freshness:**
- ⚠️ Alert if max(enrolled_ts) > 48 hours old
- ⚠️ Alert if shipment data not updated in 36 hours

---

## 10) Usage Examples

### Time-to-Therapy by Payer (Median & P90)
```sql
SELECT 
    payer_id,
    COUNT(*) as n_shipped,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_to_therapy_days) as median_ttt,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_to_therapy_days) as p90_ttt
FROM gold_psp_patient_journey
WHERE first_shipment_ts IS NOT NULL
  AND time_to_therapy_days >= 0
GROUP BY payer_id
ORDER BY median_ttt DESC;
```

### Abandonment Rate by Channel
```sql
SELECT 
    channel,
    COUNT(*) as total_enrollments,
    SUM(CASE WHEN abandoned_flag THEN 1 ELSE 0 END) as abandoned_count,
    ROUND(100.0 * SUM(CASE WHEN abandoned_flag THEN 1 ELSE 0 END) / COUNT(*), 2) as abandonment_rate_pct
FROM gold_psp_patient_journey
WHERE date_diff(CURRENT_DATE, date(enrolled_ts)) > 30  -- Only mature enrollments
GROUP BY channel
ORDER BY abandonment_rate_pct DESC;
```

### Enrollment Funnel
```sql
SELECT 
    program_type,
    COUNT(*) as enrolled,
    SUM(CASE WHEN bv_completed_ts IS NOT NULL THEN 1 ELSE 0 END) as bv_completed,
    SUM(CASE WHEN pa_submitted_ts IS NOT NULL THEN 1 ELSE 0 END) as pa_submitted,
    SUM(CASE WHEN pa_approved_ts IS NOT NULL THEN 1 ELSE 0 END) as pa_approved,
    SUM(CASE WHEN first_shipment_ts IS NOT NULL THEN 1 ELSE 0 END) as shipped,
    -- Conversion rates
    ROUND(100.0 * SUM(CASE WHEN bv_completed_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as bv_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN pa_approved_ts IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN pa_submitted_ts IS NOT NULL THEN 1 ELSE 0 END), 0), 1) as pa_approval_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN first_shipment_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as shipped_rate_pct
FROM gold_psp_patient_journey
WHERE date(enrolled_ts) BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY program_type;
```

### Bottleneck Analysis (Where Are Delays Happening?)
```sql
SELECT 
    'Enrollment to BV' as stage,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_to_bv_days) as median_days,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_to_bv_days) as p90_days
FROM gold_psp_patient_journey
WHERE time_to_bv_days IS NOT NULL

UNION ALL

SELECT 
    'BV to PA Submit',
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_to_pa_submit_days),
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_to_pa_submit_days)
FROM gold_psp_patient_journey
WHERE time_to_pa_submit_days IS NOT NULL

UNION ALL

SELECT 
    'PA Submit to Approval',
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_to_pa_approval_days),
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_to_pa_approval_days)
FROM gold_psp_patient_journey
WHERE time_to_pa_approval_days IS NOT NULL

UNION ALL

SELECT 
    'Approval to Shipment',
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_from_approval_to_ship_days),
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_from_approval_to_ship_days)
FROM gold_psp_patient_journey
WHERE time_from_approval_to_ship_days IS NOT NULL;
```

---

## 11) Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-27 | Revanth | Initial KPI definitions |



---

## 12) References

- **Business Context:** [Problem Statement](../business/problem_statement.md)
- **Technical Schema:** [Hero Table DDL](../technical/hero_table_schema.md)
- **Data Sources:** [Source System Schemas](../technical/data_sources.md)

---

**This document is the single source of truth for all KPI calculations in the PSP lakehouse.**  
All dashboards, reports, and analytics **must** use these definitions.