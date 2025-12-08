# Data Quality Issues

This document catalogs systematic data quality issues identified in HMDA data through anomaly detection analysis (Isolation Forest). These issues should be addressed in the silver → gold pipeline transformation.

## Overview

Analysis using Isolation Forest anomaly detection on post-2018 HMDA data revealed several systematic reporting errors at the lender (LEI) level. Issues fall into three main categories:

1. **Unit/Scale Errors**: Values reported in wrong units (e.g., raw values instead of thousands, decimals instead of percentages)
2. **Invalid Codes**: Use of placeholder/invalid values (e.g., 8888 for discount points)
3. **Suspicious Patterns**: Unusual but potentially legitimate reporting patterns requiring verification

## Issues by Year and Type

### 2018 (Three-Year File)

#### Income: Raw Values Instead of Thousands
**Issue**: Income reported in raw dollars instead of thousands of dollars

**Affected LEIs**:
- 549300CCELEPUO4TOE73
- 2549001LVVJUGK9VA038
- 549300CKKPTDS03YHG30
- 5493000RRYPUX5O9MI08

**Fix**: Divide income values by 1000

#### Interest Rate: Raw Values Instead of Percentage
**Issue**: Interest rates reported as decimals (e.g., 0.045) instead of percentages (4.5)

**Affected LEIs**:
- 549300CCELEPUO4TOE73

**Fix**: Multiply interest_rate by 100

#### Discount Points: Invalid Placeholder Values
**Issue**: All discount points reported as 8888 (invalid placeholder code)

**Affected LEIs**:
- 549300JD2HS86SAFVA88

**Fix**: Set discount_points to NULL/missing for this LEI

---

### 2019 (Three-Year File)

#### CLTV: Decimal Instead of Percentage
**Issue**: Combined loan-to-value ratio reported as decimal (e.g., 0.80) instead of percentage (80)

**Affected LEIs**:
- 2549001UO7C3LB3SXA82
- MZJU01BGQ7J1KULQSB89
- 549300C8GOC4OYUV0Z32

**Fix**: Multiply combined_loan_to_value_ratio by 100

#### Loan Amount: Unusually Large Values
**Issue**: Loan amounts appear abnormally large (requires verification - may be legitimate multifamily)

**Affected LEIs**:
- MZJU01BGQ7J1KULQSB89
- B2S31CFVSWTN3FR00Q90

**Fix**: Verify if multifamily loans. If not, investigate further for potential unit errors.

---

### 2020 (Three-Year File)

#### Income: Raw Values Instead of Thousands
**Issue**: Income reported in raw dollars instead of thousands of dollars

**Affected LEIs**:
- 5493009TOEDMWVNG1442
- 54930057XF33SONJFP81 (partial - some but not all records)

**Fix**:
- 5493009TOEDMWVNG1442: Divide income by 1000 for all records
- 54930057XF33SONJFP81: Conditionally divide income by 1000 for affected records (requires threshold logic)

#### Loan Term: All One Month Terms
**Issue**: All loan terms reported as 1 month (highly suspicious)

**Affected LEIs**:
- 254900Z5QRSHW4Y8CR51

**Fix**: Set loan_term to NULL/missing for this LEI (data unreliable)

---

### 2022 (One-Year File)

#### Income: Monthly Raw Instead of Annual Rounded
**Issue**: Income reported as monthly raw dollars instead of annual rounded thousands

**Affected LEIs**:
- 549300G3FCPL48R4HU28
- 5493009TOEDMWVNG1442
- 984500DA79C1B97ACF16

**Fix**: Multiply income by 12 (annual), then divide by 1000 (thousands)

#### Loan Amount: Possibly Multiplied by 100
**Issue**: Loan amounts appear to be multiplied by 100 (e.g., $15M instead of $150K)

**Affected LEIs**:
- B2S31CFVSWTN3FR00Q90

**Fix**: Divide loan_amount by 100

#### Suspicious Pattern: Large 40-Year Loans to GNMA
**Issue**: All loans are large 40-year terms sent to GNMA (unusual but may be legitimate program)

**Affected LEIs**:
- 5493002W52M3SYLFEX32

**Fix**: Flag for review but do not automatically correct (may be legitimate)

---

## Implementation Strategy for Gold Pipeline

### Approach 1: LEI-Specific Corrections
Create a corrections mapping table with LEI, year, file_type, field, and correction logic:

```python
corrections = {
    ('549300CCELEPUO4TOE73', 2018, 'a'): {
        'income': lambda x: x / 1000,
        'interest_rate': lambda x: x * 100
    },
    # ... additional mappings
}
```

### Approach 2: Rule-Based Detection
Implement detection logic to identify similar issues in other LEIs:

```python
# Detect raw income (likely > 900 since values should be in 1000s)
if income > 900 and income < 10_000_000:
    income = income / 1000

# Detect decimal interest rates (likely < 1.0 since values should be %)
if interest_rate < 1.0 and interest_rate > 0:
    interest_rate = interest_rate * 100

# Detect decimal CLTV (likely < 2.0 since values should be %)
if cltv < 2.0 and cltv > 0:
    cltv = cltv * 100
```

### Approach 3: Hybrid
1. Apply LEI-specific corrections for known issues
2. Apply rule-based detection for remaining data
3. Flag borderline cases for manual review
4. Track all corrections in audit table

### Recommended Implementation

Use **Approach 3 (Hybrid)** with the following steps:

1. Create `data_quality_corrections.py` module with:
   - Known LEI correction mappings
   - Rule-based detection functions
   - Flagging logic for review

2. Add `gold_corrections` table to track applied corrections:
   - Fields: hmda_index, field_name, original_value, corrected_value, correction_type, correction_timestamp

3. Implement corrections in `build_gold_*()` functions:
   - Apply corrections during silver → gold transformation
   - Log all corrections to audit table
   - Add `dq_flag` field to gold layer indicating records with corrections

---

## Testing Requirements

Before deploying corrections:

1. **Unit tests**: Verify each correction function works as expected
2. **Integration tests**: Test full silver → gold pipeline with known bad LEIs
3. **Validation checks**: Ensure corrected values fall within expected ranges
4. **Sample review**: Manually review 100 random corrected records
5. **Distribution analysis**: Compare distributions before/after corrections

---

## Future Improvements

1. **Automated detection**: Extend Isolation Forest analysis to identify new problematic LEIs as new data arrives
2. **Cross-year validation**: Check if LEIs with issues in one year have similar problems in other years
3. **CFPB reporting**: Consider reporting systematic errors back to CFPB for data quality improvement
4. **Documentation**: Link each LEI to original loan records for verification

---

## References

**Analysis Source**: `examples/99_isolation_forest_example.ipynb`

**Detection Method**: Isolation Forest anomaly detection (contamination=0.0025) on 2020 originated loans with features:
- interest_rate, loan_amount, income
- combined_loan_to_value_ratio, debt_to_income_ratio
- discount_points, lender_credits
- loan_type, loan_purpose, purchaser_type, loan_term

**Next Steps**: Implement gold layer pipeline with corrections per strategy above
